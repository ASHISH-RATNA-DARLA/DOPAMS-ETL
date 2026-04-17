#!/usr/bin/env python3
"""
ETL Pipeline Main Orchestrator (Standalone Version)
Processes all APIs sequentially: Crimes -> Persons -> Property -> Interrogation
This version works when all files are in the same directory
"""
import sys
import os
import psycopg2
import requests
from datetime import datetime
from pathlib import Path

# Add current directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from config.database import get_db_config
from config.api_config import APIConfig
from utils.logger import setup_logger
from utils.date_utils import generate_date_chunks, generate_date_chunks_backwards
from utils.idempotency import IdempotencyChecker
from extract.crimes_extractor import CrimesExtractor
from extract.persons_extractor import PersonsExtractor
from extract.property_extractor import PropertyExtractor
from extract.interrogation_extractor import InterrogationExtractor
from extract.mo_seizures_extractor import MoSeizuresExtractor
from extract.chargesheets_extractor import ChargesheetsExtractor
from extract.fsl_case_property_extractor import FslCasePropertyExtractor
from load.files_loader import FilesLoader


class ETLPipeline:
    """Main ETL Pipeline orchestrator"""
    
    def __init__(self, log_file='logs/etl_pipeline.log'):
        """Initialize ETL pipeline"""
        self.logger = setup_logger('ETL_Pipeline', log_file=log_file)
        self.db_config = None
        self.api_config = None
        self.connection = None
        self.idempotency_checker = None
        self.loader = None
        
        # Statistics
        self.stats = {
            'crimes': {'processed': 0, 'inserted': 0, 'skipped': 0, 'errors': 0},
            'persons': {'processed': 0, 'inserted': 0, 'skipped': 0, 'errors': 0},
            'property': {'processed': 0, 'inserted': 0, 'skipped': 0, 'errors': 0},
            'interrogation': {'processed': 0, 'inserted': 0, 'skipped': 0, 'errors': 0},
            'mo_seizures': {'processed': 0, 'inserted': 0, 'skipped': 0, 'errors': 0},
            'chargesheets': {'processed': 0, 'inserted': 0, 'skipped': 0, 'errors': 0},
            'fsl_case_property': {'processed': 0, 'inserted': 0, 'skipped': 0, 'errors': 0}
        }
    
    def initialize(self):
        """Initialize database and API connections"""
        try:
            self.logger.info("="*80)
            self.logger.info("ETL PIPELINE INITIALIZATION")
            self.logger.info("="*80)
            
            # Load database config
            self.logger.info("Loading database configuration from .env...")
            self.db_config = get_db_config()
            self.logger.info("✓ Database configuration loaded")
            
            # Load API config - look for api-ref.txt in current directory
            api_config_path = Path('api-ref.txt')
            if not api_config_path.exists():
                # Try parent directory
                api_config_path = Path(__file__).parent.parent / 'api-ref.txt'
            
            self.logger.info(f"Loading API configuration from {api_config_path}...")
            self.api_config = APIConfig(str(api_config_path))
            self.logger.info(f"✓ API configuration loaded (Base URL: {self.api_config.base_url})")
            
            # Connect to database
            self.logger.info("Connecting to database...")
            self.connection = psycopg2.connect(**self.db_config)
            self.logger.info("✓ Database connection established")
            
            # Initialize idempotency checker and loader
            self.idempotency_checker = IdempotencyChecker(self.connection)
            self.loader = FilesLoader(self.connection, self.idempotency_checker, self.logger)
            
            self.logger.info("✓ ETL Pipeline initialized successfully")
            self.logger.info("="*80)
            
        except Exception as e:
            self.logger.error(f"✗ Initialization failed: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            raise
    
    def get_last_processed_date_per_api(self) -> dict:
        """
        Get the last processed date per API (source_type) for file ID extraction.
        
        This checks the maximum created_at date per source_type in the files table,
        which represents the last date when file IDs were extracted for each API.
        
        Returns:
            dict: Mapping of source_type to last processed date (YYYY-MM-DD format),
                  or None if no files have been processed for that source_type.
        """
        try:
            # Check if created_at column exists
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = 'files' AND column_name = 'created_at'
                """)
                has_created_at = cursor.fetchone() is not None
                
                if not has_created_at:
                    self.logger.warning("⚠️  created_at column does not exist in files table - cannot resume from last date")
                    return {}
                
                # Get maximum created_at date per source_type
                cursor.execute("""
                    SELECT 
                        source_type,
                        MAX(DATE(created_at)) as last_processed_date
                    FROM files
                    WHERE created_at IS NOT NULL
                    GROUP BY source_type
                """)
                results = cursor.fetchall()
                
                last_dates = {}
                for row in results:
                    source_type = row[0]
                    last_date = row[1]
                    if last_date:
                        # Convert date to string format
                        last_dates[source_type] = last_date.strftime('%Y-%m-%d') if hasattr(last_date, 'strftime') else str(last_date)
                
                return last_dates
                
        except Exception as e:
            self.logger.warning(f"⚠️  Could not determine last processed dates per API: {e}")
            return {}
    
    def _get_backwards_chunks(self, api_name: str, end_date: str = None, start_date: str = None):
        """
        Helper method to get backwards date chunks with resume logic.
        
        Args:
            api_name: Name of the API (for resume tracking)
            end_date: End date (default: today) - where we START processing
            start_date: Start date (default: 2022-01-01) - where we STOP processing
        
        Returns:
            List of (from_date, to_date) tuples, or None if all chunks already processed
        """
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
        if start_date is None:
            start_date = '2022-01-01'
        
        # Check for last processed date
        last_dates = self.get_last_processed_date_per_api()
        if api_name in last_dates:
            resume_date = last_dates[api_name]
            self.logger.info(f"📌 Found last processed date for {api_name}: {resume_date}")
            self.logger.info(f"   Will process from {end_date} backwards, stopping at {resume_date} (already processed)")
        else:
            self.logger.info(f"📌 No previous file IDs found for {api_name} - processing from {end_date} backwards to {start_date}")
        
        # Generate date chunks going BACKWARDS (most recent first)
        chunks = generate_date_chunks_backwards(end_date=end_date, start_date=start_date, chunk_days=5, overlap_days=1)
        self.logger.info(f"Processing {len(chunks)} date chunks BACKWARDS from {end_date} to {start_date} (most recent first)")
        
        # If we have a resume_date, skip chunks that are after it (already processed)
        if api_name in last_dates:
            resume_date = last_dates[api_name]
            resume_dt = datetime.strptime(resume_date, '%Y-%m-%d')
            # Filter out chunks where to_date > resume_date (already processed)
            chunks = [(f, t) for f, t in chunks if datetime.strptime(t, '%Y-%m-%d') <= resume_dt]
            if not chunks:
                self.logger.info(f"   All chunks after {resume_date} already processed, skipping")
                return None
            self.logger.info(f"   Filtered to {len(chunks)} chunks remaining (skipping chunks after {resume_date})")
        
        return chunks
    
    def process_crimes(self, start_date='2022-01-01', end_date=None):
        """Process Crimes API"""
        self.logger.info("")
        self.logger.info("="*80)
        self.logger.info("PROCESSING CRIMES API")
        self.logger.info("="*80)
        
        extractor = CrimesExtractor(self.api_config, self.logger)
        
        # Get backwards date chunks with resume logic
        chunks = self._get_backwards_chunks('crime', end_date=end_date, start_date=start_date)
        if chunks is None:
            return
        
        total_files = []
        
        for idx, (from_date, to_date) in enumerate(chunks, 1):
            self.logger.info(f"Processing chunk {idx}/{len(chunks)}: {from_date} to {to_date}")
            
            try:
                # Build API URL
                url = self.api_config.get_url('crimes', fromDate=from_date, toDate=to_date)
                
                # Fetch data
                data = extractor.fetch_data(url)
                
                # Extract files - pass from_date as api_date for records that don't have date fields
                files = extractor.extract_files(data, api_date=from_date)
                self.logger.info(f"  Extracted {len(files)} file records from chunk")
                
                total_files.extend(files)
                self.stats['crimes']['processed'] += len(files)
            
            except Exception as e:
                self.logger.error(f"  ✗ Error processing chunk {from_date} to {to_date}: {e}")
                import traceback
                self.logger.error(traceback.format_exc())
                self.stats['crimes']['errors'] += 1
                continue
        
        # Load files
        if total_files:
            self.logger.info(f"Loading {len(total_files)} crime file records...")
            load_stats = self.loader.load_files(total_files, skip_existing=True)
            self.stats['crimes']['inserted'] += load_stats['inserted']
            self.stats['crimes']['skipped'] += load_stats['skipped']
            self.stats['crimes']['errors'] += load_stats['errors']
        
        self.logger.info(f"✓ Crimes processing complete: {self.stats['crimes']['inserted']} inserted, "
                        f"{self.stats['crimes']['skipped']} skipped, {self.stats['crimes']['errors']} errors")
    
    def process_persons(self):
        """Process Persons API"""
        self.logger.info("")
        self.logger.info("="*80)
        self.logger.info("PROCESSING PERSONS API")
        self.logger.info("="*80)
        
        extractor = PersonsExtractor(self.api_config, self.logger)
        
        # Get all person IDs from database
        self.logger.info("Fetching person IDs from database...")
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT DISTINCT person_id FROM persons ORDER BY person_id")
                person_ids = [row[0] for row in cursor.fetchall()]
            
            self.logger.info(f"Found {len(person_ids)} person IDs to process")
        
        except Exception as e:
            self.logger.error(f"✗ Error fetching person IDs: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return
        
        total_files = []
        
        for idx, person_id in enumerate(person_ids, 1):
            if idx % 100 == 0:
                self.logger.info(f"Processing person {idx}/{len(person_ids)}: {person_id}")
            
            try:
                # Build API URL
                url = self.api_config.get_url('persons', person_id=person_id)
                
                # Fetch data
                data = extractor.fetch_data(url)
                
                # Extract files - persons API doesn't use date ranges, so api_date is None
                files = extractor.extract_files(data, person_id=person_id, api_date=None)
                
                total_files.extend(files)
                self.stats['persons']['processed'] += len(files)
            
            except requests.exceptions.HTTPError as e:
                # Handle HTTP errors (400, 404, etc.) - these are expected for invalid person IDs
                if e.response is not None and 400 <= e.response.status_code < 500:
                    # Client errors (4xx) - log at debug level to reduce noise
                    self.logger.debug(f"  ⚠ Person {person_id}: HTTP {e.response.status_code} - {e.response.reason}")
                else:
                    # Server errors (5xx) - log as error
                    self.logger.error(f"  ✗ Error processing person {person_id}: {e}")
                self.stats['persons']['errors'] += 1
                continue
            except Exception as e:
                # Other exceptions (network, timeout, etc.)
                error_msg = str(e)
                # Only log full error details every 100th error to reduce log noise
                if self.stats['persons']['errors'] % 100 == 0:
                    self.logger.warning(f"  ✗ Error processing person {person_id}: {error_msg}")
                else:
                    self.logger.debug(f"  ✗ Error processing person {person_id}: {error_msg}")
                self.stats['persons']['errors'] += 1
                continue
            
            # Load in batches (every 1000 records)
            if len(total_files) >= 1000:
                self.logger.info(f"Loading batch of {len(total_files)} person file records...")
                load_stats = self.loader.load_files(total_files, skip_existing=True)
                self.stats['persons']['inserted'] += load_stats['inserted']
                self.stats['persons']['skipped'] += load_stats['skipped']
                self.stats['persons']['errors'] += load_stats['errors']
                total_files = []
        
        # Load remaining files
        if total_files:
            self.logger.info(f"Loading final batch of {len(total_files)} person file records...")
            load_stats = self.loader.load_files(total_files, skip_existing=True)
            self.stats['persons']['inserted'] += load_stats['inserted']
            self.stats['persons']['skipped'] += load_stats['skipped']
            self.stats['persons']['errors'] += load_stats['errors']
        
        self.logger.info(f"✓ Persons processing complete: {self.stats['persons']['inserted']} inserted, "
                        f"{self.stats['persons']['skipped']} skipped, {self.stats['persons']['errors']} errors")
    
    def process_property(self, start_date='2022-01-01', end_date=None):
        """Process Property API"""
        self.logger.info("")
        self.logger.info("="*80)
        self.logger.info("PROCESSING PROPERTY API")
        self.logger.info("="*80)
        
        extractor = PropertyExtractor(self.api_config, self.logger)
        
        # Get backwards date chunks with resume logic
        chunks = self._get_backwards_chunks('property', end_date=end_date, start_date=start_date)
        if chunks is None:
            return
        
        total_files = []
        
        for idx, (from_date, to_date) in enumerate(chunks, 1):
            self.logger.info(f"Processing chunk {idx}/{len(chunks)}: {from_date} to {to_date}")
            
            try:
                # Build API URL
                url = self.api_config.get_url('property', fromDate=from_date, toDate=to_date)
                
                # Fetch data
                data = extractor.fetch_data(url)
                
                # Extract files - pass from_date as api_date for records that don't have date fields
                files = extractor.extract_files(data, api_date=from_date)
                self.logger.info(f"  Extracted {len(files)} file records from chunk")
                
                total_files.extend(files)
                self.stats['property']['processed'] += len(files)
            
            except Exception as e:
                self.logger.error(f"  ✗ Error processing chunk {from_date} to {to_date}: {e}")
                import traceback
                self.logger.error(traceback.format_exc())
                self.stats['property']['errors'] += 1
                continue
        
        # Load files
        if total_files:
            self.logger.info(f"Loading {len(total_files)} property file records...")
            load_stats = self.loader.load_files(total_files, skip_existing=True)
            self.stats['property']['inserted'] += load_stats['inserted']
            self.stats['property']['skipped'] += load_stats['skipped']
            self.stats['property']['errors'] += load_stats['errors']
        
        self.logger.info(f"✓ Property processing complete: {self.stats['property']['inserted']} inserted, "
                        f"{self.stats['property']['skipped']} skipped, {self.stats['property']['errors']} errors")
    
    def process_interrogation(self, start_date='2022-01-01', end_date=None):
        """Process Interrogation API"""
        self.logger.info("")
        self.logger.info("="*80)
        self.logger.info("PROCESSING INTERROGATION API")
        self.logger.info("="*80)
        
        extractor = InterrogationExtractor(self.api_config, self.logger)
        
        # Get backwards date chunks with resume logic
        chunks = self._get_backwards_chunks('interrogation', end_date=end_date, start_date=start_date)
        if chunks is None:
            return
        
        total_files = []
        
        for idx, (from_date, to_date) in enumerate(chunks, 1):
            self.logger.info(f"Processing chunk {idx}/{len(chunks)}: {from_date} to {to_date}")
            
            try:
                # Build API URL
                url = self.api_config.get_url('interrogation', fromDate=from_date, toDate=to_date)
                
                # Fetch data
                data = extractor.fetch_data(url)
                
                # Extract files - pass from_date as api_date for records that don't have date fields
                files = extractor.extract_files(data, api_date=from_date)
                self.logger.info(f"  Extracted {len(files)} file records from chunk")
                
                total_files.extend(files)
                self.stats['interrogation']['processed'] += len(files)
            
            except Exception as e:
                self.logger.error(f"  ✗ Error processing chunk {from_date} to {to_date}: {e}")
                import traceback
                self.logger.error(traceback.format_exc())
                self.stats['interrogation']['errors'] += 1
                continue
        
        # Load files
        if total_files:
            self.logger.info(f"Loading {len(total_files)} interrogation file records...")
            load_stats = self.loader.load_files(total_files, skip_existing=True)
            self.stats['interrogation']['inserted'] += load_stats['inserted']
            self.stats['interrogation']['skipped'] += load_stats['skipped']
            self.stats['interrogation']['errors'] += load_stats['errors']
        
        self.logger.info(f"✓ Interrogation processing complete: {self.stats['interrogation']['inserted']} inserted, "
                        f"{self.stats['interrogation']['skipped']} skipped, {self.stats['interrogation']['errors']} errors")
    
    def process_mo_seizures(self, start_date='2022-01-01', end_date=None):
        """Process MO Seizures API"""
        self.logger.info("")
        self.logger.info("="*80)
        self.logger.info("PROCESSING MO SEIZURES API")
        self.logger.info("="*80)
        
        extractor = MoSeizuresExtractor(self.api_config, self.logger)
        
        # Get backwards date chunks with resume logic
        chunks = self._get_backwards_chunks('mo_seizures', end_date=end_date, start_date=start_date)
        if chunks is None:
            return
        
        total_files = []
        
        for idx, (from_date, to_date) in enumerate(chunks, 1):
            self.logger.info(f"Processing chunk {idx}/{len(chunks)}: {from_date} to {to_date}")
            
            try:
                # Build API URL
                url = self.api_config.get_url('mo_seizures', fromDate=from_date, toDate=to_date)
                
                # Fetch data
                data = extractor.fetch_data(url)
                
                # Extract files - pass from_date as api_date for records that don't have date fields
                files = extractor.extract_files(data, api_date=from_date)
                self.logger.info(f"  Extracted {len(files)} file records from chunk")
                
                total_files.extend(files)
                self.stats['mo_seizures']['processed'] += len(files)
            
            except Exception as e:
                self.logger.error(f"  ✗ Error processing chunk {from_date} to {to_date}: {e}")
                import traceback
                self.logger.error(traceback.format_exc())
                self.stats['mo_seizures']['errors'] += 1
                continue
        
        # Load files
        if total_files:
            self.logger.info(f"Loading {len(total_files)} MO Seizures file records...")
            load_stats = self.loader.load_files(total_files, skip_existing=True)
            self.stats['mo_seizures']['inserted'] += load_stats['inserted']
            self.stats['mo_seizures']['skipped'] += load_stats['skipped']
            self.stats['mo_seizures']['errors'] += load_stats['errors']
        
        self.logger.info(f"✓ MO Seizures processing complete: {self.stats['mo_seizures']['inserted']} inserted, "
                        f"{self.stats['mo_seizures']['skipped']} skipped, {self.stats['mo_seizures']['errors']} errors")
    
    def process_chargesheets(self, start_date='2022-01-01', end_date=None):
        """Process Chargesheets API"""
        self.logger.info("")
        self.logger.info("="*80)
        self.logger.info("PROCESSING CHARGESHEETS API")
        self.logger.info("="*80)
        
        extractor = ChargesheetsExtractor(self.api_config, self.logger)
        
        # Get backwards date chunks with resume logic
        chunks = self._get_backwards_chunks('chargesheets', end_date=end_date, start_date=start_date)
        if chunks is None:
            return
        
        total_files = []
        
        for idx, (from_date, to_date) in enumerate(chunks, 1):
            self.logger.info(f"Processing chunk {idx}/{len(chunks)}: {from_date} to {to_date}")
            
            try:
                # Build API URL
                url = self.api_config.get_url('chargesheets', fromDate=from_date, toDate=to_date)
                
                # Fetch data
                data = extractor.fetch_data(url)
                
                # Extract files - pass from_date as api_date for records that don't have date fields
                files = extractor.extract_files(data, api_date=from_date)
                self.logger.info(f"  Extracted {len(files)} file records from chunk")
                
                total_files.extend(files)
                self.stats['chargesheets']['processed'] += len(files)
            
            except Exception as e:
                self.logger.error(f"  ✗ Error processing chunk {from_date} to {to_date}: {e}")
                import traceback
                self.logger.error(traceback.format_exc())
                self.stats['chargesheets']['errors'] += 1
                continue
        
        # Load files
        if total_files:
            self.logger.info(f"Loading {len(total_files)} Chargesheets file records...")
            load_stats = self.loader.load_files(total_files, skip_existing=True)
            self.stats['chargesheets']['inserted'] += load_stats['inserted']
            self.stats['chargesheets']['skipped'] += load_stats['skipped']
            self.stats['chargesheets']['errors'] += load_stats['errors']
        
        self.logger.info(f"✓ Chargesheets processing complete: {self.stats['chargesheets']['inserted']} inserted, "
                        f"{self.stats['chargesheets']['skipped']} skipped, {self.stats['chargesheets']['errors']} errors")
    
    def process_fsl_case_property(self, start_date='2022-01-01', end_date=None):
        """Process FSL Case Property API"""
        self.logger.info("")
        self.logger.info("="*80)
        self.logger.info("PROCESSING FSL CASE PROPERTY API")
        self.logger.info("="*80)
        
        extractor = FslCasePropertyExtractor(self.api_config, self.logger)
        
        # Get backwards date chunks with resume logic
        # Note: source_type in files table is 'case_property' not 'fsl_case_property'
        chunks = self._get_backwards_chunks('case_property', end_date=end_date, start_date=start_date)
        if chunks is None:
            return
        
        total_files = []
        
        for idx, (from_date, to_date) in enumerate(chunks, 1):
            self.logger.info(f"Processing chunk {idx}/{len(chunks)}: {from_date} to {to_date}")
            
            try:
                # Build API URL
                url = self.api_config.get_url('fsl_case_property', fromDate=from_date, toDate=to_date)
                
                # Fetch data
                data = extractor.fetch_data(url)
                
                # Extract files - pass from_date as api_date for records that don't have date fields
                files = extractor.extract_files(data, api_date=from_date)
                self.logger.info(f"  Extracted {len(files)} file records from chunk")
                
                total_files.extend(files)
                self.stats['fsl_case_property']['processed'] += len(files)
            
            except Exception as e:
                self.logger.error(f"  ✗ Error processing chunk {from_date} to {to_date}: {e}")
                import traceback
                self.logger.error(traceback.format_exc())
                self.stats['fsl_case_property']['errors'] += 1
                continue
        
        # Load files
        if total_files:
            self.logger.info(f"Loading {len(total_files)} FSL Case Property file records...")
            load_stats = self.loader.load_files(total_files, skip_existing=True)
            self.stats['fsl_case_property']['inserted'] += load_stats['inserted']
            self.stats['fsl_case_property']['skipped'] += load_stats['skipped']
            self.stats['fsl_case_property']['errors'] += load_stats['errors']
        
        self.logger.info(f"✓ FSL Case Property processing complete: {self.stats['fsl_case_property']['inserted']} inserted, "
                        f"{self.stats['fsl_case_property']['skipped']} skipped, {self.stats['fsl_case_property']['errors']} errors")
    
    def get_apis_to_process(self):
        """
        Get list of APIs to process from environment variable.
        
        Reads ETL_PROCESS_APIS from .env file.
        Format: comma-separated list, e.g., "mo_seizures,chargesheets,fsl_case_property"
        If not set or empty, processes all APIs.
        
        Returns:
            list: List of API names to process
        """
        apis_env = os.getenv('ETL_PROCESS_APIS', '').strip()
        
        if not apis_env:
            # Process all APIs if not specified
            return ['crimes', 'persons', 'property', 'interrogation', 'mo_seizures', 'chargesheets', 'fsl_case_property']
        
        # Parse comma-separated list
        apis = [api.strip().lower() for api in apis_env.split(',') if api.strip()]
        
        # Validate API names
        valid_apis = ['crimes', 'persons', 'property', 'interrogation', 'mo_seizures', 'chargesheets', 'fsl_case_property']
        invalid_apis = [api for api in apis if api not in valid_apis]
        
        if invalid_apis:
            self.logger.warning(f"⚠ Invalid API names in ETL_PROCESS_APIS: {invalid_apis}")
            self.logger.warning(f"  Valid APIs are: {', '.join(valid_apis)}")
            apis = [api for api in apis if api in valid_apis]
        
        if not apis:
            self.logger.warning("⚠ No valid APIs specified, processing all APIs")
            return ['crimes', 'persons', 'property', 'interrogation', 'mo_seizures', 'chargesheets', 'fsl_case_property']
        
        return apis
    
    def run(self):
        """Run complete ETL pipeline sequentially"""
        start_time = datetime.now()
        
        try:
            self.logger.info("")
            self.logger.info("="*80)
            self.logger.info("STARTING ETL PIPELINE")
            self.logger.info(f"Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            self.logger.info("="*80)
            
            # Initialize
            self.initialize()
            
            # Get list of APIs to process from environment
            apis_to_process = self.get_apis_to_process()
            self.logger.info(f"📋 APIs to process: {', '.join(apis_to_process)}")
            self.logger.info("")
            
            # Process APIs sequentially based on configuration
            if 'crimes' in apis_to_process:
                self.process_crimes()
            else:
                self.logger.info("⏭️  Skipping Crimes API (not in ETL_PROCESS_APIS)")
            
            if 'persons' in apis_to_process:
                self.process_persons()
            else:
                self.logger.info("⏭️  Skipping Persons API (not in ETL_PROCESS_APIS)")
            
            if 'property' in apis_to_process:
                self.process_property()
            else:
                self.logger.info("⏭️  Skipping Property API (not in ETL_PROCESS_APIS)")
            
            if 'interrogation' in apis_to_process:
                self.process_interrogation()
            else:
                self.logger.info("⏭️  Skipping Interrogation API (not in ETL_PROCESS_APIS)")
            
            if 'mo_seizures' in apis_to_process:
                self.process_mo_seizures()
            else:
                self.logger.info("⏭️  Skipping MO Seizures API (not in ETL_PROCESS_APIS)")
            
            if 'chargesheets' in apis_to_process:
                self.process_chargesheets()
            else:
                self.logger.info("⏭️  Skipping Chargesheets API (not in ETL_PROCESS_APIS)")
            
            if 'fsl_case_property' in apis_to_process:
                self.process_fsl_case_property()
            else:
                self.logger.info("⏭️  Skipping FSL Case Property API (not in ETL_PROCESS_APIS)")
            
            # Final statistics
            end_time = datetime.now()
            duration = end_time - start_time
            
            self.logger.info("")
            self.logger.info("="*80)
            self.logger.info("ETL PIPELINE COMPLETE")
            self.logger.info("="*80)
            self.logger.info("FINAL STATISTICS:")
            
            # Only show statistics for APIs that were processed
            apis_to_process = self.get_apis_to_process()
            
            if 'crimes' in apis_to_process:
                self.logger.info(f"  Crimes:     {self.stats['crimes']['inserted']} inserted, "
                               f"{self.stats['crimes']['skipped']} skipped, {self.stats['crimes']['errors']} errors")
            if 'persons' in apis_to_process:
                self.logger.info(f"  Persons:    {self.stats['persons']['inserted']} inserted, "
                               f"{self.stats['persons']['skipped']} skipped, {self.stats['persons']['errors']} errors")
            if 'property' in apis_to_process:
                self.logger.info(f"  Property:   {self.stats['property']['inserted']} inserted, "
                               f"{self.stats['property']['skipped']} skipped, {self.stats['property']['errors']} errors")
            if 'interrogation' in apis_to_process:
                self.logger.info(f"  Interrogation: {self.stats['interrogation']['inserted']} inserted, "
                               f"{self.stats['interrogation']['skipped']} skipped, {self.stats['interrogation']['errors']} errors")
            if 'mo_seizures' in apis_to_process:
                self.logger.info(f"  MO Seizures: {self.stats['mo_seizures']['inserted']} inserted, "
                               f"{self.stats['mo_seizures']['skipped']} skipped, {self.stats['mo_seizures']['errors']} errors")
            if 'chargesheets' in apis_to_process:
                self.logger.info(f"  Chargesheets: {self.stats['chargesheets']['inserted']} inserted, "
                               f"{self.stats['chargesheets']['skipped']} skipped, {self.stats['chargesheets']['errors']} errors")
            if 'fsl_case_property' in apis_to_process:
                self.logger.info(f"  FSL Case Property: {self.stats['fsl_case_property']['inserted']} inserted, "
                               f"{self.stats['fsl_case_property']['skipped']} skipped, {self.stats['fsl_case_property']['errors']} errors")
            self.logger.info(f"Duration: {duration}")
            self.logger.info(f"End Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            self.logger.info("="*80)
        
        except Exception as e:
            self.logger.error(f"✗ ETL Pipeline failed: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            raise
        
        finally:
            self.close()
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            self.logger.info("Database connection closed")


def main():
    """Main entry point"""
    pipeline = ETLPipeline()
    pipeline.run()


if __name__ == "__main__":
    main()


