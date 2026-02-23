#!/usr/bin/env python3
"""
DOPAMAS ETL Pipeline - Hierarchy API
Fetches hierarchy data in 5-day chunks and loads into PostgreSQL
"""

import sys
import time
import requests
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime, timedelta, timezone
from tqdm import tqdm
import logging
import colorlog
from typing import List, Dict, Optional, Tuple, Set
import json

from config import DB_CONFIG, API_CONFIG, ETL_CONFIG, LOG_CONFIG, TABLE_CONFIG

# IST timezone offset (UTC+05:30)
IST_OFFSET = timezone(timedelta(hours=5, minutes=30))

# Add TRACE level support (lower than DEBUG)
TRACE_LEVEL = 5
logging.addLevelName(TRACE_LEVEL, 'TRACE')

def trace(self, message, *args, **kws):
    """Custom trace method for logger"""
    if self.isEnabledFor(TRACE_LEVEL):
        self._log(TRACE_LEVEL, message, args, **kws)

logging.Logger.trace = trace

# Setup colored logging
handler = colorlog.StreamHandler()
handler.setFormatter(colorlog.ColoredFormatter(
    LOG_CONFIG['format'],
    datefmt=LOG_CONFIG['date_format'],
    log_colors={
        'TRACE': 'white',
        'DEBUG': 'cyan',
        'INFO': 'green',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'red,bg_white',
    }
))
logger = colorlog.getLogger()
logger.addHandler(handler)

# Handle TRACE level
log_level = LOG_CONFIG['level'].upper()
if log_level == 'TRACE':
    logger.setLevel(TRACE_LEVEL)
else:
    logger.setLevel(log_level)

# Target table (allows redirecting ETL runs to test tables)
HIERARCHY_TABLE = TABLE_CONFIG.get('hierarchy', 'hierarchy')


def parse_iso_date(date_str: str) -> datetime:
    """Parse ISO 8601 date string (with optional time component) to datetime."""
    if 'T' in date_str:
        return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
    return datetime.strptime(date_str, '%Y-%m-%d')


def get_yesterday_end_ist() -> str:
    """Get yesterday's date at 23:59:59 in IST (UTC+05:30) as ISO format string."""
    now_ist = datetime.now(IST_OFFSET)
    yesterday = now_ist - timedelta(days=1)
    yesterday_end = yesterday.replace(hour=23, minute=59, second=59, microsecond=0)
    return yesterday_end.isoformat()


class HierarchyETL:
    """ETL Pipeline for Hierarchy API"""
    
    def __init__(self):
        self.db_conn = None
        self.db_cursor = None
        self.stats = {
            'total_api_calls': 0,
            'total_hierarchy_fetched': 0,
            'total_hierarchy_inserted': 0,
            'total_hierarchy_updated': 0,
            'total_hierarchy_no_change': 0,  # Records that exist but no changes needed
            'total_hierarchy_failed': 0,  # Records that failed to process
            'total_hierarchy_skipped': 0,  # Records that were skipped
            'total_duplicates': 0,
            'failed_api_calls': 0,
            'errors': []
        }
        
        # Setup chunk-wise logging files
        self.setup_chunk_loggers()
    
    def setup_chunk_loggers(self):
        """Setup separate log files for API responses, DB operations, and duplicates"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # API response log file
        self.api_log_file = f'hierarchy_api_chunks_{timestamp}.log'
        self.api_log = open(self.api_log_file, 'w', encoding='utf-8')
        self.api_log.write(f"# Hierarchy API Chunk-wise Log\n")
        self.api_log.write(f"# Generated: {datetime.now().isoformat()}\n")
        self.api_log.write(f"# Date Range: {ETL_CONFIG['start_date']} to {ETL_CONFIG['end_date']}\n")
        self.api_log.write(f"# Chunk Size: {ETL_CONFIG['chunk_days']} days\n")
        self.api_log.write(f"{'='*80}\n\n")
        
        # Database operations log file
        self.db_log_file = f'hierarchy_db_chunks_{timestamp}.log'
        self.db_log = open(self.db_log_file, 'w', encoding='utf-8')
        self.db_log.write(f"# Hierarchy Database Operations Chunk-wise Log\n")
        self.db_log.write(f"# Generated: {datetime.now().isoformat()}\n")
        self.db_log.write(f"# Date Range: {ETL_CONFIG['start_date']} to {ETL_CONFIG['end_date']}\n")
        self.db_log.write(f"# Chunk Size: {ETL_CONFIG['chunk_days']} days\n")
        self.db_log.write(f"{'='*80}\n\n")
        
        # Duplicates log file
        self.duplicates_log_file = f'hierarchy_duplicates_{timestamp}.log'
        self.duplicates_log = open(self.duplicates_log_file, 'w', encoding='utf-8')
        self.duplicates_log.write(f"# Hierarchy Duplicates Log\n")
        self.duplicates_log.write(f"# Generated: {datetime.now().isoformat()}\n")
        self.duplicates_log.write(f"# Date Range: {ETL_CONFIG['start_date']} to {ETL_CONFIG['end_date']}\n")
        self.duplicates_log.write(f"# Chunk Size: {ETL_CONFIG['chunk_days']} days\n")
        self.duplicates_log.write(f"{'='*80}\n\n")
        
        logger.info(f"📝 API chunk log: {self.api_log_file}")
        logger.info(f"📝 DB chunk log: {self.db_log_file}")
        logger.info(f"📝 Duplicates log: {self.duplicates_log_file}")
    
    def close_chunk_loggers(self):
        """Close chunk log files"""
        if hasattr(self, 'api_log') and self.api_log:
            self.api_log.close()
        if hasattr(self, 'db_log') and self.db_log:
            self.db_log.close()
        if hasattr(self, 'duplicates_log') and self.duplicates_log:
            self.duplicates_log.close()
    
    def connect_db(self):
        """Connect to PostgreSQL database"""
        try:
            self.db_conn = psycopg2.connect(**DB_CONFIG)
            self.db_cursor = self.db_conn.cursor()
            logger.info(f"✅ Connected to database: {DB_CONFIG['database']}")
            return True
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            return False
    
    def close_db(self):
        """Close database connection"""
        if self.db_cursor:
            self.db_cursor.close()
        if self.db_conn:
            self.db_conn.close()
        logger.info("Database connection closed")
    
    def get_table_columns(self, table_name: str) -> Set[str]:
        """Get all column names from a table."""
        try:
            self.db_cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = %s
            """, (table_name,))
            return {row[0] for row in self.db_cursor.fetchall()}
        except Exception as e:
            logger.error(f"Error getting table columns for {table_name}: {e}")
            return set()
    
    def get_effective_start_date(self) -> str:
        """
        Get effective start date for ETL:
        - If table is empty: return 2022-01-01T00:00:00+05:30
        - If table has data: return max(date_created, date_modified) from table
        - BUT always ensure start date is at least 2022-01-01 (never go before this)
        """
        MIN_START_DATE = '2022-01-01T00:00:00+05:30'
        min_start_dt = parse_iso_date('2022-01-01T00:00:00+05:30')
        
        try:
            # Check if table has any data
            self.db_cursor.execute(f"SELECT COUNT(*) FROM {HIERARCHY_TABLE}")
            count = self.db_cursor.fetchone()[0]
            
            if count == 0:
                # New database, start from beginning
                logger.info("📊 Table is empty, starting from 2022-01-01")
                return MIN_START_DATE
            
            # Table has data, get max of date_created and date_modified
            # Only consider dates >= 2022-01-01 to avoid processing very old data
            self.db_cursor.execute(f"""
                SELECT GREATEST(
                    COALESCE(MAX(CASE WHEN date_created >= '2022-01-01'::timestamp THEN date_created END), '2022-01-01'::timestamp),
                    COALESCE(MAX(CASE WHEN date_modified >= '2022-01-01'::timestamp THEN date_modified END), '2022-01-01'::timestamp)
                ) as max_date
                FROM {HIERARCHY_TABLE}
            """)
            result = self.db_cursor.fetchone()
            if result and result[0]:
                max_date = result[0]
                # Convert to IST timezone if needed
                if isinstance(max_date, datetime):
                    if max_date.tzinfo is None:
                        max_date = max_date.replace(tzinfo=IST_OFFSET)
                    else:
                        max_date = max_date.astimezone(IST_OFFSET)
                    
                    # Ensure we never go before 2022-01-01
                    if max_date < min_start_dt:
                        logger.warning(f"⚠️  Max date ({max_date.isoformat()}) is before 2022-01-01, using 2022-01-01")
                        return MIN_START_DATE
                    
                    logger.info(f"📊 Table has data, starting from: {max_date.isoformat()}")
                    return max_date.isoformat()
            
            # Fallback to start date
            logger.warning("⚠️  Could not determine max date, using 2022-01-01")
            return MIN_START_DATE
            
        except Exception as e:
            logger.error(f"❌ Error getting effective start date: {e}")
            logger.warning("⚠️  Using default start date: 2022-01-01")
            return MIN_START_DATE
    
    def detect_new_fields(self, api_record: Dict, table_columns: Set[str]) -> Dict[str, str]:
        """
        Detect new fields in API response that don't exist in table.
        Returns dict mapping API field name to database column name (snake_case).
        """
        new_fields = {}
        
        # Map API field names to database column names
        # API uses UPPER_CASE, DB uses snake_case
        field_mapping = {
            'PS_CODE': 'ps_code',
            'PS_NAME': 'ps_name',
            'CIRCLE_CODE': 'circle_code',
            'CIRCLE_NAME': 'circle_name',
            'SDPO_CODE': 'sdpo_code',
            'SDPO_NAME': 'sdpo_name',
            'SUB_ZONE_CODE': 'sub_zone_code',
            'SUB_ZONE_NAME': 'sub_zone_name',
            'DIST_CODE': 'dist_code',
            'DIST_NAME': 'dist_name',
            'RANGE_CODE': 'range_code',
            'RANGE_NAME': 'range_name',
            'ZONE_CODE': 'zone_code',
            'ZONE_NAME': 'zone_name',
            'ADG_CODE': 'adg_code',
            'ADG_NAME': 'adg_name',
            'DATE_CREATED': 'date_created',
            'DATE_MODIFIED': 'date_modified'
        }
        
        for api_field, db_column in field_mapping.items():
            if api_field in api_record and db_column not in table_columns:
                new_fields[api_field] = db_column
        
        return new_fields
    
    def add_column_to_table(self, column_name: str, column_type: str = 'TEXT'):
        """Add a new column to the hierarchy table."""
        try:
            # Determine column type based on field name
            if 'date' in column_name.lower():
                column_type = 'TIMESTAMP'
            elif 'code' in column_name.lower():
                column_type = 'VARCHAR(20)'
            elif 'name' in column_name.lower():
                column_type = 'VARCHAR(255)'
            
            alter_sql = f"ALTER TABLE {HIERARCHY_TABLE} ADD COLUMN IF NOT EXISTS {column_name} {column_type}"
            self.db_cursor.execute(alter_sql)
            self.db_conn.commit()
            logger.info(f"✅ Added column {column_name} ({column_type}) to {HIERARCHY_TABLE}")
            return True
        except Exception as e:
            logger.error(f"❌ Error adding column {column_name}: {e}")
            self.db_conn.rollback()
            return False
    
    def update_existing_records_with_new_fields(self, new_fields: Dict[str, str], chunk_end_date: str):
        """
        Update existing records from start_date to chunk_end_date with new fields.
        For new fields, we need to fetch those records from API and update them.
        Since we can't easily fetch old records by date range from API for specific PS_CODEs,
        we'll update them to NULL initially, and they'll be updated when those records are processed in future runs.
        """
        if not new_fields:
            return
        
        try:
            # Parse chunk_end_date to datetime
            chunk_end_dt = parse_iso_date(chunk_end_date)
            if chunk_end_dt.tzinfo is None:
                chunk_end_dt = chunk_end_dt.replace(tzinfo=IST_OFFSET)
            
            start_date = '2022-01-01T00:00:00+05:30'
            start_dt = parse_iso_date(start_date)
            if start_dt.tzinfo is None:
                start_dt = start_dt.replace(tzinfo=IST_OFFSET)
            
            # For now, we'll set new fields to NULL for existing records
            # They will be updated when those records are processed in future ETL runs
            # This is a limitation: we can't easily fetch old records from API without knowing PS_CODEs
            logger.info(f"📝 New fields detected: {list(new_fields.keys())}")
            logger.info(f"   Note: Existing records will be updated when processed in future ETL runs")
            logger.info(f"   New fields are set to NULL for existing records until they are reprocessed")
            
            # Optionally, we could update all existing records to NULL for new fields
            # But this might not be necessary since they'll be updated when processed
            # Uncomment below if you want to explicitly set NULL for all existing records:
            # for db_column in new_fields.values():
            #     update_sql = f"""
            #         UPDATE {HIERARCHY_TABLE} 
            #         SET {db_column} = NULL
            #         WHERE (date_created IS NULL OR date_created <= %s)
            #           AND (date_modified IS NULL OR date_modified <= %s)
            #     """
            #     self.db_cursor.execute(update_sql, (chunk_end_dt, chunk_end_dt))
            #     self.db_conn.commit()
            
        except Exception as e:
            logger.error(f"❌ Error updating existing records: {e}")
    
    def generate_date_ranges(self, start_date: str, end_date: str, chunk_days: int = 5, overlap_days: int = 1) -> List[Tuple[str, str]]:
        """
        Generate date ranges in chunks with overlap to ensure no data is missed
        OVERLAP: Each chunk overlaps with the previous chunk by overlap_days to catch boundary records
        
        Args:
            start_date: Start date in ISO format (YYYY-MM-DDTHH:MM:SS) or YYYY-MM-DD
            end_date: End date in ISO format (YYYY-MM-DDTHH:MM:SS) or YYYY-MM-DD
            chunk_days: Number of days per chunk
            overlap_days: Number of days to overlap between chunks (default: 1 to ensure no data loss)
        
        Returns:
            List of (from_date, to_date) tuples in ISO format (YYYY-MM-DDTHH:MM:SS)
        """
        date_ranges = []
        # Parse ISO format dates
        current_date = parse_iso_date(start_date).date()
        end = parse_iso_date(end_date).date()
        
        while current_date <= end:
            chunk_end = current_date + timedelta(days=chunk_days - 1)
            if chunk_end > end:
                chunk_end = end
            
            # Convert to datetime with time
            start_datetime = datetime.combine(current_date, datetime.min.time())  # 00:00:00
            end_datetime = datetime.combine(chunk_end, datetime.max.time().replace(microsecond=0))  # 23:59:59
            
            # Format as ISO format
            date_ranges.append((
                start_datetime.strftime('%Y-%m-%dT%H:%M:%S'),
                end_datetime.strftime('%Y-%m-%dT%H:%M:%S')
            ))
            
            # If we've already reached or passed the end date, break
            if chunk_end >= end:
                break
            
            # Next chunk starts with overlap: current chunk end - overlap_days + 1
            # For overlap_days=1: next_start = chunk_end (same day, creating 1-day overlap)
            # This ensures: Last day of chunk N = First day of chunk N+1
            next_start = chunk_end - timedelta(days=overlap_days - 1)
            
            # Move to next chunk start
            current_date = next_start
        
        return date_ranges
    
    def fetch_hierarchy_api(self, from_date: str, to_date: str) -> Optional[List[Dict]]:
        """
        Fetch hierarchy data from API for given date range
        
        Args:
            from_date: Start datetime in ISO format (YYYY-MM-DDTHH:MM:SS)
            to_date: End datetime in ISO format (YYYY-MM-DDTHH:MM:SS)
        
        Returns:
            List of hierarchy records or None if failed
        """
        # Convert ISO datetime to date-only format (YYYY-MM-DD) for API compatibility
        from_date_only = from_date.split('T')[0] if 'T' in from_date else from_date
        to_date_only = to_date.split('T')[0] if 'T' in to_date else to_date
        
        url = API_CONFIG['hierarchy_url']
        params = {
            'fromDate': from_date_only,
            'toDate': to_date_only
        }
        headers = {
            'x-api-key': API_CONFIG['api_key']
        }
        
        for attempt in range(API_CONFIG['max_retries']):
            try:
                logger.debug(f"Fetching hierarchy: {from_date} to {to_date} (Attempt {attempt + 1})")
                logger.trace(f"API Request - URL: {url}, Params: {params}, Headers: {headers}")
                response = requests.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=API_CONFIG['timeout']
                )
                logger.trace(f"API Response - Status: {response.status_code}, Headers: {dict(response.headers)}")
                
                if response.status_code == 200:
                    data = response.json()
                    self.stats['total_api_calls'] += 1
                    
                    # Handle both single object and array responses
                    if data.get('status'):
                        hierarchy_data = data.get('data')
                        if hierarchy_data:
                            # If single object, convert to list
                            if isinstance(hierarchy_data, dict):
                                hierarchy_data = [hierarchy_data]
                            
                            # Extract ps_codes for logging
                            ps_codes = [record.get('PS_CODE') for record in hierarchy_data if record.get('PS_CODE')]
                            
                            # Log to API chunk file
                            self.log_api_chunk(from_date, to_date, len(hierarchy_data), ps_codes, hierarchy_data)
                            
                            logger.info(f"✅ Fetched {len(hierarchy_data)} hierarchy records for {from_date} to {to_date}")
                            logger.debug(f"📋 PS Codes from API: {ps_codes[:10]}{'...' if len(ps_codes) > 10 else ''}")
                            logger.trace(f"Full PS Codes list: {ps_codes}")
                            logger.trace(f"Sample record structure: {json.dumps(hierarchy_data[0] if hierarchy_data else {}, indent=2)}")
                            return hierarchy_data
                        else:
                            # Log empty response
                            self.log_api_chunk(from_date, to_date, 0, [], [])
                            logger.warning(f"⚠️  No hierarchy data found for {from_date} to {to_date}")
                            return []
                    else:
                        # Log failed status
                        self.log_api_chunk(from_date, to_date, 0, [], [], error="API returned status=false")
                        logger.warning(f"⚠️  API returned status=false for {from_date} to {to_date}")
                        return []
                
                elif response.status_code == 404:
                    # Log 404 response
                    self.log_api_chunk(from_date, to_date, 0, [], [], error="404 Not Found")
                    logger.warning(f"⚠️  No data found for {from_date} to {to_date}")
                    return []
                
                else:
                    logger.warning(f"API returned status code {response.status_code}, retrying...")
                    time.sleep(2 ** attempt)  # Exponential backoff
                    
            except requests.exceptions.Timeout:
                logger.warning(f"API timeout, retrying... (Attempt {attempt + 1})")
                time.sleep(2 ** attempt)
            except Exception as e:
                logger.error(f"API error: {e}")
                if attempt == API_CONFIG['max_retries'] - 1:
                    self.stats['failed_api_calls'] += 1
                    self.stats['errors'].append(f"{from_date} to {to_date}: {str(e)}")
                    self.log_api_chunk(from_date, to_date, 0, [], [], error=str(e))
                time.sleep(2 ** attempt)
        
        logger.error(f"❌ Failed to fetch hierarchy for {from_date} to {to_date} after {API_CONFIG['max_retries']} attempts")
        self.log_api_chunk(from_date, to_date, 0, [], [], error="Failed after max retries")
        return None
    
    def log_api_chunk(self, from_date: str, to_date: str, count: int, ps_codes: List[str], 
                     hierarchy_data: List[Dict], error: Optional[str] = None):
        """Log API response for a chunk"""
        chunk_info = {
            'chunk': f"{from_date} to {to_date}",
            'timestamp': datetime.now().isoformat(),
            'count': count,
            'ps_codes': ps_codes,
            'error': error
        }
        
        self.api_log.write(f"\n{'='*80}\n")
        self.api_log.write(f"CHUNK: {from_date} to {to_date}\n")
        self.api_log.write(f"Timestamp: {datetime.now().isoformat()}\n")
        self.api_log.write(f"{'-'*80}\n")
        
        if error:
            self.api_log.write(f"ERROR: {error}\n")
            self.api_log.write(f"Count: 0\n")
            self.api_log.write(f"PS Codes: []\n")
        else:
            self.api_log.write(f"Count: {count}\n")
            self.api_log.write(f"PS Codes ({len(ps_codes)}):\n")
            for i, ps_code in enumerate(ps_codes, 1):
                self.api_log.write(f"  {i}. {ps_code}\n")
            
            # Also write JSON format for easy parsing
            self.api_log.write(f"\nJSON Format:\n")
            self.api_log.write(json.dumps(chunk_info, indent=2, ensure_ascii=False))
            self.api_log.write(f"\n")
        
        self.api_log.flush()
    
    def transform_hierarchy(self, record_raw: Dict, table_columns: Set[str] = None) -> Dict:
        """
        Transform API response to database format
        Dynamically handles new fields from API
        
        Args:
            record_raw: Raw hierarchy data from API
            table_columns: Set of existing table columns (for dynamic field handling)
        
        Returns:
            Transformed hierarchy dict ready for database
        """
        logger.trace(f"Transforming hierarchy record: PS_CODE={record_raw.get('PS_CODE')}, PS_NAME={record_raw.get('PS_NAME')}")
        
        # Standard field mapping
        field_mapping = {
            'PS_CODE': 'ps_code',
            'PS_NAME': 'ps_name',
            'CIRCLE_CODE': 'circle_code',
            'CIRCLE_NAME': 'circle_name',
            'SDPO_CODE': 'sdpo_code',
            'SDPO_NAME': 'sdpo_name',
            'SUB_ZONE_CODE': 'sub_zone_code',
            'SUB_ZONE_NAME': 'sub_zone_name',
            'DIST_CODE': 'dist_code',
            'DIST_NAME': 'dist_name',
            'RANGE_CODE': 'range_code',
            'RANGE_NAME': 'range_name',
            'ZONE_CODE': 'zone_code',
            'ZONE_NAME': 'zone_name',
            'ADG_CODE': 'adg_code',
            'ADG_NAME': 'adg_name',
            'DATE_CREATED': 'date_created',
            'DATE_MODIFIED': 'date_modified'
        }
        
        transformed = {}
        
        # Transform known fields
        for api_field, db_field in field_mapping.items():
            value = record_raw.get(api_field)
            if value is not None and value != '':
                transformed[db_field] = value
            else:
                transformed[db_field] = None
        
        # Handle any new fields dynamically
        if table_columns:
            for api_field, value in record_raw.items():
                if api_field not in field_mapping:
                    # Convert to snake_case for database column name
                    db_field = api_field.lower().replace('_', '_')
                    # Only include if column exists in table (or will be added)
                    if db_field in table_columns or not table_columns:
                        transformed[db_field] = value if value not in (None, '') else None
        
        logger.trace(f"Transformed record: {json.dumps(transformed, indent=2, default=str)}")
        return transformed
    
    def hierarchy_exists(self, ps_code: str) -> bool:
        """Check if hierarchy record already exists in database"""
        logger.trace(f"Checking if PS_CODE exists in database: {ps_code}")
        self.db_cursor.execute(f"SELECT 1 FROM {HIERARCHY_TABLE} WHERE ps_code = %s", (ps_code,))
        exists = self.db_cursor.fetchone() is not None
        logger.trace(f"PS_CODE {ps_code} exists: {exists}")
        return exists
    
    def get_existing_hierarchy(self, ps_code: str) -> Optional[Dict]:
        """Get existing hierarchy record from database"""
        query = f"""
            SELECT ps_code, ps_name,
                   circle_code, circle_name,
                   sdpo_code, sdpo_name,
                   sub_zone_code, sub_zone_name,
                   dist_code, dist_name,
                   range_code, range_name,
                   zone_code, zone_name,
                   adg_code, adg_name,
                   date_created, date_modified
            FROM {HIERARCHY_TABLE}
            WHERE ps_code = %s
        """
        self.db_cursor.execute(query, (ps_code,))
        row = self.db_cursor.fetchone()
        if row:
            return {
                'ps_code': row[0],
                'ps_name': row[1],
                'circle_code': row[2],
                'circle_name': row[3],
                'sdpo_code': row[4],
                'sdpo_name': row[5],
                'sub_zone_code': row[6],
                'sub_zone_name': row[7],
                'dist_code': row[8],
                'dist_name': row[9],
                'range_code': row[10],
                'range_name': row[11],
                'zone_code': row[12],
                'zone_name': row[13],
                'adg_code': row[14],
                'adg_name': row[15],
                'date_created': row[16],
                'date_modified': row[17]
            }
        return None
    
    def insert_hierarchy(self, record: Dict, chunk_date_range: str = "", table_columns: Set[str] = None) -> Tuple[bool, str]:
        """
        Insert or update single hierarchy record into database
        
        Args:
            record: Transformed hierarchy dict
            chunk_date_range: Date range for chunk tracking
            table_columns: Set of existing table columns (for dynamic field handling)
        
        Returns:
            Tuple of (success: bool, operation: str) where operation is 'inserted', 'updated', or 'skipped'
        """
        try:
            if not record['ps_code']:
                logger.warning(f"⚠️  Hierarchy record missing PS_CODE, skipping")
                self.stats['total_hierarchy_failed'] += 1
                return False, 'skipped_missing_ps_code'
            
            # Check if hierarchy record already exists
            ps_code = record['ps_code']
            logger.trace(f"Processing hierarchy record: PS_CODE={ps_code}, PS_NAME={record.get('ps_name')}")
            
            if self.hierarchy_exists(ps_code):
                # Get existing record to compare
                existing = self.get_existing_hierarchy(ps_code)
                if not existing:
                    logger.warning(f"⚠️  PS_CODE {ps_code} exists check returned True but fetch returned None")
                    # Fall back to insert
                    existing = None
                
                if existing:
                    # Smart update: only update fields that need updating
                    # Rules:
                    # 1. If existing is NULL and new is not NULL → update
                    # 2. If existing is not NULL and new is NULL → keep existing (don't update to NULL)
                    # 3. If both are not NULL and different → update
                    # 4. If both are not NULL and same → skip update (no change needed)
                    
                    update_fields = []
                    update_values = []
                    changes = []
                    
                    # Define all fields to check (excluding ps_code which is the key)
                    fields_to_check = [
                        ('ps_name', 'PS_NAME'),
                        ('circle_code', 'CIRCLE_CODE'),
                        ('circle_name', 'CIRCLE_NAME'),
                        ('sdpo_code', 'SDPO_CODE'),
                        ('sdpo_name', 'SDPO_NAME'),
                        ('sub_zone_code', 'SUB_ZONE_CODE'),
                        ('sub_zone_name', 'SUB_ZONE_NAME'),
                        ('dist_code', 'DIST_CODE'),
                        ('dist_name', 'DIST_NAME'),
                        ('range_code', 'RANGE_CODE'),
                        ('range_name', 'RANGE_NAME'),
                        ('zone_code', 'ZONE_CODE'),
                        ('zone_name', 'ZONE_NAME'),
                        ('adg_code', 'ADG_CODE'),
                        ('adg_name', 'ADG_NAME'),
                        ('date_created', 'DATE_CREATED'),
                        ('date_modified', 'DATE_MODIFIED')
                    ]
                    
                    for db_field, api_field in fields_to_check:
                        existing_val = existing.get(db_field)
                        new_val = record.get(db_field)
                        
                        # Rule 1: Existing is NULL, new is not NULL → update
                        if existing_val is None and new_val is not None:
                            update_fields.append(f"{db_field} = %s")
                            update_values.append(new_val)
                            changes.append(f"{db_field}: NULL → {new_val}")
                            logger.trace(f"  Will update {db_field}: NULL → {new_val}")
                        
                        # Rule 2: Existing is not NULL, new is NULL → keep existing (skip)
                        elif existing_val is not None and new_val is None:
                            logger.trace(f"  Will keep existing {db_field}: {existing_val} (new value is NULL)")
                            # Don't add to update - preserve existing value
                        
                        # Rule 3 & 4: Both are not NULL
                        elif existing_val is not None and new_val is not None:
                            # Rule 3: Different → update
                            if existing_val != new_val:
                                update_fields.append(f"{db_field} = %s")
                                update_values.append(new_val)
                                changes.append(f"{db_field}: {existing_val} → {new_val}")
                                logger.trace(f"  Will update {db_field}: {existing_val} → {new_val}")
                            # Rule 4: Same → skip (no change)
                            else:
                                logger.trace(f"  No change for {db_field}: {existing_val}")
                        
                        # Both are NULL → no update needed
                        else:
                            logger.trace(f"  Both NULL for {db_field}, no update")
                    
                    # Only update if there are changes
                    if update_fields:
                        update_query = f"""
                            UPDATE {HIERARCHY_TABLE} SET
                                {', '.join(update_fields)}
                            WHERE ps_code = %s
                        """
                        update_values.append(ps_code)
                        self.db_cursor.execute(update_query, tuple(update_values))
                        self.stats['total_hierarchy_updated'] += 1
                        logger.debug(f"Updated hierarchy: {ps_code} ({len(changes)} fields changed)")
                        logger.trace(f"Changes: {', '.join(changes)}")
                        self.db_conn.commit()
                        logger.trace(f"Transaction committed for updated PS_CODE: {ps_code}")
                        return True, 'updated'
                    else:
                        # No changes needed
                        self.stats['total_hierarchy_no_change'] += 1
                        logger.trace(f"No changes needed for PS_CODE: {ps_code} (all fields match or preserved)")
                        return True, 'no_change'
                else:
                    # Exists check returned True but couldn't fetch - treat as new insert
                    logger.warning(f"⚠️  PS_CODE {ps_code} exists but couldn't fetch, treating as new insert")
                    # Fall through to insert logic
            else:
                # Insert new record - dynamically build query to handle new fields
                logger.trace(f"Inserting new hierarchy record: {ps_code}")
                
                # Get all fields from record that exist in table
                # We'll use the record keys, but filter by what columns exist
                if table_columns:
                    # Only include fields that exist in table
                    fields_to_insert = {k: v for k, v in record.items() if k in table_columns}
                else:
                    # If table_columns not provided, use all fields from record
                    fields_to_insert = record
                
                # Build dynamic INSERT query
                columns = list(fields_to_insert.keys())
                placeholders = ', '.join(['%s'] * len(columns))
                column_names = ', '.join(columns)
                
                insert_query = f"""
                    INSERT INTO {HIERARCHY_TABLE} ({column_names})
                    VALUES ({placeholders})
                """
                values = tuple(fields_to_insert.values())
                
                self.db_cursor.execute(insert_query, values)
                self.stats['total_hierarchy_inserted'] += 1
                logger.debug(f"Inserted hierarchy: {record['ps_code']}")
                logger.trace(f"Insert query executed for PS_CODE: {ps_code}")
                self.db_conn.commit()
                logger.trace(f"Transaction committed for inserted PS_CODE: {ps_code}")
                return True, 'inserted'
            
        except psycopg2.IntegrityError as e:
            self.db_conn.rollback()
            logger.warning(f"⚠️  Integrity error for hierarchy {record['ps_code']}: {e}")
            self.stats['total_hierarchy_skipped'] += 1
            return False, 'skipped_integrity_error'
        except Exception as e:
            self.db_conn.rollback()
            logger.error(f"❌ Error inserting hierarchy {record['ps_code']}: {e}")
            self.stats['errors'].append(f"Hierarchy {record['ps_code']}: {str(e)}")
            return False, 'skipped_error'
    
    def process_date_range(self, from_date: str, to_date: str, table_columns: Set[str] = None):
        """Process hierarchy data for a specific date range"""
        chunk_range = f"{from_date} to {to_date}"
        logger.info(f"📅 Processing: {chunk_range}")
        
        # Fetch hierarchy from API
        hierarchy_raw = self.fetch_hierarchy_api(from_date, to_date)
        
        if hierarchy_raw is None:
            logger.error(f"❌ Failed to fetch hierarchy for {chunk_range}")
            self.log_db_chunk(from_date, to_date, 0, [], [], [], [], [], error="API fetch failed")
            return
        
        if not hierarchy_raw:
            logger.info(f"ℹ️  No hierarchy data found for {chunk_range}")
            self.log_db_chunk(from_date, to_date, 0, [], [], [], [], [], error="No hierarchy in API response")
            return
        
        # Check for schema evolution if we got data
        if table_columns is not None and len(hierarchy_raw) > 0:
            # Check for new fields in first record
            new_fields = self.detect_new_fields(hierarchy_raw[0], table_columns)
            if new_fields:
                logger.info(f"🔍 New fields detected in API response: {list(new_fields.keys())}")
                # Add new columns to table
                for api_field, db_column in new_fields.items():
                    if self.add_column_to_table(db_column):
                        # Update table_columns set
                        table_columns.add(db_column)
                # Update existing records from start_date to current chunk end_date
                self.update_existing_records_with_new_fields(new_fields, to_date)
        
        # Transform and insert each hierarchy record
        self.stats['total_hierarchy_fetched'] += len(hierarchy_raw)
        logger.trace(f"Processing {len(hierarchy_raw)} hierarchy records for chunk {chunk_range}")
        
        # Track operations for this chunk
        inserted_ps_codes = []
        updated_ps_codes = []
        no_change_ps_codes = []  # Records that exist but no changes needed
        skipped_ps_codes = []
        skipped_reasons = {}
        duplicates_in_chunk = []
        
        # Track ps_codes seen in this chunk to detect duplicates within the chunk
        seen_ps_codes = {}
        
        logger.trace(f"Starting to process records for chunk: {chunk_range}")
        for idx, record_raw in enumerate(hierarchy_raw, 1):
            logger.trace(f"Processing record {idx}/{len(hierarchy_raw)}: {record_raw.get('PS_CODE')}")
            record = self.transform_hierarchy(record_raw, table_columns)
            ps_code = record['ps_code']
            
            if not ps_code:
                logger.warning(f"⚠️  Hierarchy record missing PS_CODE, skipping")
                self.stats['total_hierarchy_failed'] += 1
                skipped_ps_codes.append(None)
                if 'missing_ps_code' not in skipped_reasons:
                    skipped_reasons['missing_ps_code'] = []
                skipped_reasons['missing_ps_code'].append(None)
                continue
            
            # Check for duplicates within this chunk
            if ps_code in seen_ps_codes:
                duplicates_in_chunk.append({
                    'ps_code': ps_code,
                    'first_seen_in': seen_ps_codes[ps_code],
                    'duplicate_in': chunk_range
                })
                self.stats['total_duplicates'] += 1
                logger.warning(f"⚠️  Duplicate PS_CODE {ps_code} found in chunk {chunk_range}")
                logger.trace(f"Duplicate details - First seen: {seen_ps_codes[ps_code]}, Current: {chunk_range}")
            else:
                seen_ps_codes[ps_code] = chunk_range
                logger.trace(f"New PS_CODE seen: {ps_code} in chunk {chunk_range}")
            
            # Check if this ps_code was already processed in this chunk (duplicate)
            if ps_code in inserted_ps_codes or ps_code in updated_ps_codes or ps_code in no_change_ps_codes:
                # This is a duplicate within the chunk, skip processing
                logger.trace(f"Skipping duplicate PS_CODE {ps_code} - already processed in this chunk")
                continue
            
            success, operation = self.insert_hierarchy(record, chunk_range, table_columns)
            logger.trace(f"Operation result for PS_CODE {ps_code}: success={success}, operation={operation}")
            if success:
                if operation == 'inserted':
                    inserted_ps_codes.append(ps_code)
                    logger.trace(f"Added to inserted list: {ps_code}")
                elif operation == 'updated':
                    updated_ps_codes.append(ps_code)
                    logger.trace(f"Added to updated list: {ps_code}")
                elif operation == 'no_change':
                    no_change_ps_codes.append(ps_code)
                    logger.trace(f"Added to no_change list: {ps_code}")
            else:
                skipped_ps_codes.append(ps_code)
                if operation not in skipped_reasons:
                    skipped_reasons[operation] = []
                skipped_reasons[operation].append(ps_code)
                logger.trace(f"Added to skipped list: {ps_code}, reason: {operation}")
        
        # Log duplicates for this chunk
        if duplicates_in_chunk:
            logger.trace(f"Found {len(duplicates_in_chunk)} duplicates in chunk {chunk_range}")
            self.log_duplicates_chunk(from_date, to_date, duplicates_in_chunk)
        
        # Log database operations for this chunk
        logger.trace(f"Chunk summary - Inserted: {len(inserted_ps_codes)}, Updated: {len(updated_ps_codes)}, No Change: {len(no_change_ps_codes)}, Failed: {len(skipped_ps_codes)}, Duplicates: {len(duplicates_in_chunk)}")
        self.log_db_chunk(from_date, to_date, len(hierarchy_raw), inserted_ps_codes, 
                         updated_ps_codes, no_change_ps_codes, skipped_ps_codes, skipped_reasons)
        
        logger.info(f"✅ Completed: {chunk_range}")
        logger.info(f"   📊 Chunk Stats - Inserted: {len(inserted_ps_codes)}, Updated: {len(updated_ps_codes)}, "
                   f"No Change: {len(no_change_ps_codes)}, Failed: {len(skipped_ps_codes)}, Duplicates: {len(duplicates_in_chunk)}")
        logger.trace(f"Chunk processing complete for {chunk_range}")
    
    def log_duplicates_chunk(self, from_date: str, to_date: str, duplicates: List[Dict]):
        """Log duplicates found in a chunk"""
        chunk_info = {
            'chunk': f"{from_date} to {to_date}",
            'timestamp': datetime.now().isoformat(),
            'duplicate_count': len(duplicates),
            'duplicates': duplicates
        }
        
        self.duplicates_log.write(f"\n{'='*80}\n")
        self.duplicates_log.write(f"CHUNK: {from_date} to {to_date}\n")
        self.duplicates_log.write(f"Timestamp: {datetime.now().isoformat()}\n")
        self.duplicates_log.write(f"{'-'*80}\n")
        self.duplicates_log.write(f"Duplicate Count: {len(duplicates)}\n")
        self.duplicates_log.write(f"\nDuplicates:\n")
        for i, dup in enumerate(duplicates, 1):
            self.duplicates_log.write(f"  {i}. PS_CODE: {dup['ps_code']}\n")
            self.duplicates_log.write(f"     First seen in: {dup['first_seen_in']}\n")
            self.duplicates_log.write(f"     Duplicate in: {dup['duplicate_in']}\n")
        
        # Also write JSON format for easy parsing
        self.duplicates_log.write(f"\nJSON Format:\n")
        self.duplicates_log.write(json.dumps(chunk_info, indent=2, ensure_ascii=False))
        self.duplicates_log.write(f"\n")
        
        self.duplicates_log.flush()
    
    def log_db_chunk(self, from_date: str, to_date: str, total_fetched: int,
                    inserted_ps_codes: List[str], updated_ps_codes: List[str],
                    no_change_ps_codes: List[str], skipped_ps_codes: List[str], 
                    skipped_reasons: Dict, error: Optional[str] = None):
        """Log database operations for a chunk"""
        chunk_info = {
            'chunk': f"{from_date} to {to_date}",
            'timestamp': datetime.now().isoformat(),
            'total_fetched': total_fetched,
            'inserted_count': len(inserted_ps_codes),
            'inserted_ps_codes': inserted_ps_codes,
            'updated_count': len(updated_ps_codes),
            'updated_ps_codes': updated_ps_codes,
            'no_change_count': len(no_change_ps_codes),
            'no_change_ps_codes': no_change_ps_codes,
            'skipped_count': len(skipped_ps_codes),
            'skipped_ps_codes': skipped_ps_codes,
            'skipped_reasons': skipped_reasons,
            'error': error
        }
        
        self.db_log.write(f"\n{'='*80}\n")
        self.db_log.write(f"CHUNK: {from_date} to {to_date}\n")
        self.db_log.write(f"Timestamp: {datetime.now().isoformat()}\n")
        self.db_log.write(f"{'-'*80}\n")
        
        if error:
            self.db_log.write(f"ERROR: {error}\n")
        else:
            self.db_log.write(f"Total Fetched from API: {total_fetched}\n")
            self.db_log.write(f"\nINSERTED: {len(inserted_ps_codes)}\n")
            for i, ps_code in enumerate(inserted_ps_codes, 1):
                self.db_log.write(f"  {i}. {ps_code}\n")
            
            self.db_log.write(f"\nUPDATED: {len(updated_ps_codes)}\n")
            for i, ps_code in enumerate(updated_ps_codes, 1):
                self.db_log.write(f"  {i}. {ps_code}\n")
            
            self.db_log.write(f"\nNO CHANGE: {len(no_change_ps_codes)}\n")
            for i, ps_code in enumerate(no_change_ps_codes, 1):
                self.db_log.write(f"  {i}. {ps_code}\n")
            
            self.db_log.write(f"\nSKIPPED: {len(skipped_ps_codes)}\n")
            if skipped_reasons:
                for reason, ps_codes in skipped_reasons.items():
                    self.db_log.write(f"  Reason: {reason} ({len(ps_codes)})\n")
                    for i, ps_code in enumerate(ps_codes[:20], 1):  # Show first 20
                        self.db_log.write(f"    {i}. {ps_code}\n")
                    if len(ps_codes) > 20:
                        self.db_log.write(f"    ... and {len(ps_codes) - 20} more\n")
            
            # Also write JSON format for easy parsing
            self.db_log.write(f"\nJSON Format:\n")
            self.db_log.write(json.dumps(chunk_info, indent=2, ensure_ascii=False))
            self.db_log.write(f"\n")
        
        self.db_log.flush()
    
    def write_log_summaries(self):
        """Write summary sections to all log files"""
        # API log summary
        self.api_log.write(f"\n\n{'='*80}\n")
        self.api_log.write(f"SUMMARY\n")
        self.api_log.write(f"{'='*80}\n")
        self.api_log.write(f"Total API Calls: {self.stats['total_api_calls']}\n")
        self.api_log.write(f"Total Hierarchy Records Fetched: {self.stats['total_hierarchy_fetched']}\n")
        self.api_log.write(f"Failed API Calls: {self.stats['failed_api_calls']}\n")
        self.api_log.write(f"Total Chunks Processed: {self.stats['total_api_calls'] + self.stats['failed_api_calls']}\n")
        
        # DB log summary
        self.db_log.write(f"\n\n{'='*80}\n")
        self.db_log.write(f"SUMMARY\n")
        self.db_log.write(f"{'='*80}\n")
        self.db_log.write(f"Total Hierarchy Records Fetched from API: {self.stats['total_hierarchy_fetched']}\n")
        self.db_log.write(f"Total Hierarchy Records Inserted (New): {self.stats['total_hierarchy_inserted']}\n")
        self.db_log.write(f"Total Hierarchy Records Updated (Existing): {self.stats['total_hierarchy_updated']}\n")
        self.db_log.write(f"Total Hierarchy Records No Change: {self.stats['total_hierarchy_no_change']}\n")
        self.db_log.write(f"Total Hierarchy Records Skipped: {self.stats.get('total_hierarchy_skipped', 0)}\n")
        self.db_log.write(f"Total Operations (Inserted + Updated + No Change): {self.stats['total_hierarchy_inserted'] + self.stats['total_hierarchy_updated'] + self.stats['total_hierarchy_no_change']}\n")
        db_total = self.stats.get('db_total_count', self.stats['total_hierarchy_inserted'])
        self.db_log.write(f"Total Unique Hierarchy Records in Database: {db_total}\n")
        self.db_log.write(f"Note: Updated count includes duplicates (same ps_code in multiple chunks)\n")
        if self.stats['total_hierarchy_fetched'] > 0:
            coverage = ((self.stats['total_hierarchy_inserted'] + self.stats['total_hierarchy_updated'] + self.stats['total_hierarchy_no_change']) / self.stats['total_hierarchy_fetched']) * 100
            self.db_log.write(f"Coverage: {coverage:.2f}%\n")
        self.db_log.write(f"Errors: {len(self.stats['errors'])}\n")
        
        # Duplicates log summary
        self.duplicates_log.write(f"\n\n{'='*80}\n")
        self.duplicates_log.write(f"SUMMARY\n")
        self.duplicates_log.write(f"{'='*80}\n")
        self.duplicates_log.write(f"Total Duplicates Found: {self.stats['total_duplicates']}\n")
        self.duplicates_log.write(f"Note: Duplicates are PS_CODES that appear multiple times within the same chunk\n")
    
    def run(self):
        """Main ETL execution"""
        logger.info("=" * 80)
        logger.info("🚀 DOPAMAS ETL Pipeline - Hierarchy API")
        logger.info("=" * 80)
        
        # Calculate date range
        # Start date: Always 2022-01-01T00:00:00+05:30
        # End date: Yesterday at 23:59:59+05:30 (IST)
        fixed_start_date = '2022-01-01T00:00:00+05:30'
        calculated_end_date = get_yesterday_end_ist()
        
        logger.info(f"Fixed Start Date: {fixed_start_date}")
        logger.info(f"Calculated End Date: {calculated_end_date}")
        
        # Connect to database
        if not self.connect_db():
            logger.error("Failed to connect to database. Exiting.")
            return False
        
        try:
            # Get effective start date (check if table has data)
            effective_start_date = self.get_effective_start_date()
            logger.info(f"Effective Start Date: {effective_start_date}")
            
            # Get table columns for schema evolution
            table_columns = self.get_table_columns(HIERARCHY_TABLE)
            logger.debug(f"Existing table columns: {sorted(table_columns)}")
            
            # Generate date ranges with overlap to ensure no data is missed
            date_ranges = self.generate_date_ranges(
                effective_start_date,
                calculated_end_date,
                ETL_CONFIG['chunk_days'],
                ETL_CONFIG.get('chunk_overlap_days', 1)  # Default to 1 day overlap for safety
            )
            
            logger.info(f"Date Range: {effective_start_date} to {calculated_end_date}")
            overlap_days = ETL_CONFIG.get('chunk_overlap_days', 1)
            logger.info(f"Chunk Size: {ETL_CONFIG['chunk_days']} days (overlap: {overlap_days} day(s) to ensure no data loss)")
            logger.info("=" * 80)
            
            logger.info(f"📊 Total date ranges to process: {len(date_ranges)}")
            logger.trace(f"Generated date ranges: {date_ranges[:5]}{'...' if len(date_ranges) > 5 else ''} (showing first 5)")
            logger.info("")
            
            # Process each date range with progress bar
            for from_date, to_date in tqdm(date_ranges, desc="Processing date ranges", unit="range"):
                # Process the chunk (will check for schema evolution and process data)
                self.process_date_range(from_date, to_date, table_columns)
                time.sleep(1)  # Be nice to the API
            
            # Get database counts
            self.db_cursor.execute(f"SELECT COUNT(*) FROM {HIERARCHY_TABLE}")
            db_hierarchy_count = self.db_cursor.fetchone()[0]
            
            # Store for summary
            self.stats['db_total_count'] = db_hierarchy_count
            
            # Print final statistics
            logger.info("")
            logger.info("=" * 80)
            logger.info("📊 FINAL STATISTICS")
            logger.info("=" * 80)
            logger.info(f"📡 API CALLS:")
            logger.info(f"  Total API Calls:      {self.stats['total_api_calls']}")
            logger.info(f"  Failed API Calls:     {self.stats['failed_api_calls']}")
            logger.info(f"")
            logger.info(f"📥 FROM API:")
            logger.info(f"  Total Hierarchy Records Fetched: {self.stats['total_hierarchy_fetched']}")
            logger.info(f"")
            logger.info(f"💾 TO DATABASE:")
            logger.info(f"  Total Inserted (New): {self.stats['total_hierarchy_inserted']}")
            logger.info(f"  Total Updated:        {self.stats['total_hierarchy_updated']}")
            logger.info(f"  Total No Change:      {self.stats['total_hierarchy_no_change']}")
            logger.info(f"  Total Failed:          {self.stats['total_hierarchy_failed']}")
            logger.info(f"  Total in DB:                      {db_hierarchy_count}")
            logger.info(f"")
            logger.info(f"📊 COVERAGE:")
            if self.stats['total_hierarchy_fetched'] > 0:
                coverage = ((self.stats['total_hierarchy_inserted'] + self.stats['total_hierarchy_updated'] + self.stats['total_hierarchy_no_change']) / self.stats['total_hierarchy_fetched']) * 100
                logger.info(f"  API → DB Coverage:   {coverage:.2f}%")
            logger.info(f"")
            logger.info(f"🔄 DUPLICATES:")
            logger.info(f"  Total Duplicates Found:          {self.stats['total_duplicates']}")
            logger.info(f"")
            logger.info(f"Errors:               {len(self.stats['errors'])}")
            logger.info("=" * 80)
            
            if self.stats['errors']:
                logger.warning("⚠️  Errors encountered:")
                for error in self.stats['errors'][:10]:  # Show first 10 errors
                    logger.warning(f"  - {error}")
                if len(self.stats['errors']) > 10:
                    logger.warning(f"  ... and {len(self.stats['errors']) - 10} more")
            
            # Write summary to log files
            self.write_log_summaries()
            
            logger.info("✅ ETL Pipeline completed successfully!")
            logger.info(f"📝 API chunk log saved to: {self.api_log_file}")
            logger.info(f"📝 DB chunk log saved to: {self.db_log_file}")
            logger.info(f"📝 Duplicates log saved to: {self.duplicates_log_file}")
            return True
            
        except KeyboardInterrupt:
            logger.warning("\n⚠️  ETL interrupted by user")
            return False
        except Exception as e:
            logger.error(f"❌ ETL failed with error: {e}")
            import traceback
            traceback.print_exc()
            return False
        finally:
            self.close_chunk_loggers()
            self.close_db()


def main():
    """Main entry point"""
    etl = HierarchyETL()
    success = etl.run()
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()


