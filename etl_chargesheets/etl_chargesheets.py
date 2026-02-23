#!/usr/bin/env python3
"""
DOPAMAS ETL Pipeline - Chargesheets API
Fetches chargesheets data in 5-day chunks and loads into PostgreSQL
Handles 4 tables: chargesheets, chargesheet_files, chargesheet_acts, chargesheet_accused
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
import uuid

from config import DB_CONFIG, API_CONFIG, ETL_CONFIG, LOG_CONFIG, TABLE_CONFIG

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

# Ensure trace method is available on the logger instance (for colorlog compatibility)
if not hasattr(logger, 'trace') or not callable(getattr(logger, 'trace', None)):
    import types
    logger.trace = types.MethodType(trace, logger)

# Handle TRACE level
log_level = LOG_CONFIG['level'].upper()
if log_level == 'TRACE':
    logger.setLevel(TRACE_LEVEL)
else:
    logger.setLevel(log_level)

# Target tables (allows redirecting ETL into test tables)
CHARGESHEETS_TABLE = TABLE_CONFIG.get('chargesheets', 'chargesheets')
CHARGESHEET_FILES_TABLE = TABLE_CONFIG.get('chargesheet_files', 'chargesheet_files')
CHARGESHEET_ACTS_TABLE = TABLE_CONFIG.get('chargesheet_acts', 'chargesheet_acts')
CHARGESHEET_ACCUSED_TABLE = TABLE_CONFIG.get('chargesheet_accused', 'chargesheet_accused')
CRIMES_TABLE = TABLE_CONFIG.get('crimes', 'crimes')

# IST timezone offset (UTC+05:30)
IST_OFFSET = timezone(timedelta(hours=5, minutes=30))

def parse_iso_date(iso_date_str: str) -> datetime:
    """
    Parse ISO 8601 date string to datetime object
    Supports formats:
    - YYYY-MM-DDTHH:MM:SS+TZ:TZ (e.g., '2022-10-01T00:00:00+05:30')
    - YYYY-MM-DD (e.g., '2022-10-01') - defaults to 00:00:00 IST
    - YYYY-MM-DD HH:MM:SS (e.g., '2025-08-23 10:20:00') - defaults to IST
    
    Args:
        iso_date_str: ISO 8601 date string or date-time string
        
    Returns:
        datetime object with timezone info
    """
    try:
        # Try parsing as ISO format with timezone
        if 'T' in iso_date_str:
            # ISO format with time: 2022-10-01T00:00:00+05:30
            return datetime.fromisoformat(iso_date_str.replace('Z', '+00:00'))
        elif ' ' in iso_date_str and ':' in iso_date_str:
            # Format: YYYY-MM-DD HH:MM:SS (from API)
            dt = datetime.strptime(iso_date_str, '%Y-%m-%d %H:%M:%S')
            return dt.replace(tzinfo=IST_OFFSET)
        else:
            # Date only format: 2022-10-01 - default to 00:00:00 IST
            dt = datetime.strptime(iso_date_str, '%Y-%m-%d')
            return dt.replace(tzinfo=IST_OFFSET)
    except ValueError:
        # Fallback: try parsing as date only
        try:
            dt = datetime.strptime(iso_date_str.split('T')[0].split(' ')[0], '%Y-%m-%d')
            return dt.replace(tzinfo=IST_OFFSET)
        except:
            return None

def iso_to_date_only(iso_date_str: str) -> str:
    """
    Extract date part from ISO 8601 format string
    Converts '2022-10-01T00:00:00+05:30' to '2022-10-01'
    
    Args:
        iso_date_str: ISO 8601 date string
    
    Returns:
        Date string in YYYY-MM-DD format
    """
    if 'T' in iso_date_str:
        return iso_date_str.split('T')[0]
    return iso_date_str

def format_iso_date(dt: datetime, include_time: bool = True) -> str:
    """
    Format datetime to ISO 8601 string
    
    Args:
        dt: datetime object
        include_time: If True, includes time and timezone; if False, only date
    
    Returns:
        ISO 8601 formatted string
    """
    if include_time:
        # Ensure timezone info
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=IST_OFFSET)
        return dt.isoformat()
    else:
        return dt.strftime('%Y-%m-%d')


def get_yesterday_end_ist() -> str:
    """Get yesterday's date at 23:59:59 in IST (UTC+05:30) as ISO format string."""
    now_ist = datetime.now(IST_OFFSET)
    yesterday = now_ist - timedelta(days=1)
    yesterday_end = yesterday.replace(hour=23, minute=59, second=59, microsecond=0)
    return yesterday_end.isoformat()


class ChargesheetsETL:
    """ETL Pipeline for Chargesheets API"""
    
    def __init__(self):
        self.db_conn = None
        self.db_cursor = None
        self.stats = {
            'total_api_calls': 0,
            'total_chargesheets_fetched': 0,
            'total_chargesheets_inserted': 0,
            'total_chargesheets_updated': 0,
            'total_chargesheets_no_change': 0,
            'total_chargesheets_failed': 0,
            'total_chargesheets_failed_crime_id': 0,  # Chargesheets failed due to CRIME_ID not found
            'total_files_inserted': 0,
            'total_files_updated': 0,
            'total_acts_inserted': 0,
            'total_acts_updated': 0,
            'total_accused_inserted': 0,
            'total_accused_updated': 0,
            'total_duplicates': 0,
            'failed_api_calls': 0,
            'errors': []
        }
        
        # Setup chunk-wise logging files
        self.setup_chunk_loggers()
    
    def setup_chunk_loggers(self):
        """Setup separate log files for API responses, DB operations, and failed records"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Create logs directory if it doesn't exist
        import os
        os.makedirs('logs', exist_ok=True)
        
        # API response log file
        self.api_log_file = f'logs/chargesheets_api_chunks_{timestamp}.log'
        self.api_log = open(self.api_log_file, 'w', encoding='utf-8')
        self.api_log.write(f"# Chargesheets API Chunk-wise Log\n")
        self.api_log.write(f"# Generated: {datetime.now().isoformat()}\n")
        self.api_log.write(f"# Date Range (ISO 8601): {ETL_CONFIG['start_date']} to {ETL_CONFIG['end_date']}\n")
        overlap_days = ETL_CONFIG.get('chunk_overlap_days', 1)
        self.api_log.write(f"# Chunk Size: {ETL_CONFIG['chunk_days']} days (overlap: {overlap_days} day(s) between chunks)\n")
        self.api_log.write(f"# API Server Timezone: IST (UTC+05:30)\n")
        start_dt = parse_iso_date(ETL_CONFIG['start_date'])
        end_dt = parse_iso_date(ETL_CONFIG['end_date'])
        self.api_log.write(f"#   - Start: {format_iso_date(start_dt)} (IST)\n")
        self.api_log.write(f"#   - End: {format_iso_date(end_dt)} (IST)\n")
        self.api_log.write(f"# ETL Server Timezone: UTC\n")
        self.api_log.write(f"{'='*80}\n\n")
        
        # Database operations log file
        self.db_log_file = f'logs/chargesheets_db_chunks_{timestamp}.log'
        self.db_log = open(self.db_log_file, 'w', encoding='utf-8')
        self.db_log.write(f"# Chargesheets Database Operations Chunk-wise Log\n")
        self.db_log.write(f"# Generated: {datetime.now().isoformat()}\n")
        self.db_log.write(f"# Date Range (ISO 8601): {ETL_CONFIG['start_date']} to {ETL_CONFIG['end_date']}\n")
        overlap_days = ETL_CONFIG.get('chunk_overlap_days', 1)
        self.db_log.write(f"# Chunk Size: {ETL_CONFIG['chunk_days']} days (overlap: {overlap_days} day(s) between chunks)\n")
        self.db_log.write(f"# API Server Timezone: IST (UTC+05:30)\n")
        start_dt = parse_iso_date(ETL_CONFIG['start_date'])
        end_dt = parse_iso_date(ETL_CONFIG['end_date'])
        self.db_log.write(f"#   - Start: {format_iso_date(start_dt)} (IST)\n")
        self.db_log.write(f"#   - End: {format_iso_date(end_dt)} (IST)\n")
        self.db_log.write(f"{'='*80}\n\n")
        
        # Failed records log file
        self.failed_log_file = f'logs/chargesheets_failed_{timestamp}.log'
        self.failed_log = open(self.failed_log_file, 'w', encoding='utf-8')
        self.failed_log.write(f"# Chargesheets Failed Records Log\n")
        self.failed_log.write(f"# Generated: {datetime.now().isoformat()}\n")
        self.failed_log.write(f"# Records that failed to insert or update with reasons\n")
        self.failed_log.write(f"{'='*80}\n\n")
        
        # Duplicates log file
        self.duplicates_log_file = f'logs/chargesheets_duplicates_{timestamp}.log'
        self.duplicates_log = open(self.duplicates_log_file, 'w', encoding='utf-8')
        self.duplicates_log.write(f"# Chargesheets Duplicates Log\n")
        self.duplicates_log.write(f"# Generated: {datetime.now().isoformat()}\n")
        self.duplicates_log.write(f"# Duplicate records found within the same chunk\n")
        self.duplicates_log.write(f"{'='*80}\n\n")
        
        # Invalid crime_id log file (chargesheets with crime_id not found in crimes table)
        self.invalid_crime_id_log_file = f'logs/chargesheets_invalid_crime_id_{timestamp}.log'
        self.invalid_crime_id_log = open(self.invalid_crime_id_log_file, 'w', encoding='utf-8')
        self.invalid_crime_id_log.write(f"# Chargesheets Invalid CRIME_ID Log\n")
        self.invalid_crime_id_log.write(f"# Generated: {datetime.now().isoformat()}\n")
        self.invalid_crime_id_log.write(f"# Chargesheets that failed because CRIME_ID not found in crimes table\n")
        self.invalid_crime_id_log.write(f"# These records are SKIPPED and NOT inserted/updated\n")
        self.invalid_crime_id_log.write(f"{'='*80}\n\n")
        
        logger.info(f"📝 API chunk log: {self.api_log_file}")
        logger.info(f"📝 DB chunk log: {self.db_log_file}")
        logger.info(f"📝 Failed records log: {self.failed_log_file}")
        logger.info(f"📝 Duplicates log: {self.duplicates_log_file}")
        logger.info(f"📝 Invalid CRIME_ID log: {self.invalid_crime_id_log_file}")
    
    def close_chunk_loggers(self):
        """Close chunk log files"""
        if hasattr(self, 'api_log') and self.api_log:
            self.api_log.close()
        if hasattr(self, 'db_log') and self.db_log:
            self.db_log.close()
        if hasattr(self, 'failed_log') and self.failed_log:
            self.failed_log.close()
        if hasattr(self, 'duplicates_log') and self.duplicates_log:
            self.duplicates_log.close()
        if hasattr(self, 'invalid_crime_id_log') and self.invalid_crime_id_log:
            self.invalid_crime_id_log.close()
    
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
            # Rollback any previous failed transaction
            self.db_conn.rollback()
            self.db_cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = %s
            """, (table_name,))
            return {row[0] for row in self.db_cursor.fetchall()}
        except Exception as e:
            logger.error(f"Error getting table columns for {table_name}: {e}")
            self.db_conn.rollback()
            return set()
    
    def get_effective_start_date(self) -> str:
        """
        Get effective start date for ETL by checking all 4 chargesheet tables:
        - If all tables are empty: return 2022-01-01T00:00:00+05:30
        - If any table has data: return max(date_created, date_modified) across all tables
        - Uses overlap: goes back by overlap_days to ensure no data is missed
        """
        try:
            # Rollback any previous failed transaction
            self.db_conn.rollback()
            
            # Check all 4 tables with their specific date columns
            table_configs = [
                {
                    'table': CHARGESHEETS_TABLE,
                    'date_columns': ['date_created', 'date_modified'],  # chargesheets table uses date_created/date_modified
                    'name': 'chargesheets'
                },
                {
                    'table': CHARGESHEET_FILES_TABLE,
                    'date_columns': ['created_at'],  # related tables use created_at
                    'name': 'chargesheet_files'
                },
                {
                    'table': CHARGESHEET_ACTS_TABLE,
                    'date_columns': ['created_at'],
                    'name': 'chargesheet_acts'
                },
                {
                    'table': CHARGESHEET_ACCUSED_TABLE,
                    'date_columns': ['created_at'],
                    'name': 'chargesheet_accused'
                }
            ]
            
            max_dates = []
            
            for config in table_configs:
                table = config['table']
                date_columns = config['date_columns']
                table_name = config['name']
                
                try:
                    # Check if table exists and has any data
                    self.db_cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = self.db_cursor.fetchone()[0]
                    
                    if count > 0:
                        # Build query with appropriate date columns for this table
                        date_checks = []
                        for col in date_columns:
                            date_checks.append(f"COALESCE(MAX(CASE WHEN {col} >= '2022-01-01'::timestamp THEN {col} END), '2022-01-01'::timestamp)")
                        
                        query = f"""
                            SELECT GREATEST({', '.join(date_checks)})
                            FROM {table}
                        """
                        self.db_cursor.execute(query)
                        result = self.db_cursor.fetchone()
                        if result and result[0]:
                            max_dates.append(result[0])
                            logger.debug(f"📊 {table_name} table max date: {result[0]}")
                except Exception as e:
                    # Rollback on error and continue with next table
                    self.db_conn.rollback()
                    logger.warning(f"⚠️  Error checking {table_name} table: {e}")
                    continue
            
            if not max_dates:
                # All tables are empty, start from beginning
                logger.info("📊 All chargesheet tables are empty, starting from 2022-01-01")
                return '2022-01-01T00:00:00+05:30'
            
            # Get the maximum date across all tables
            max_date = max(max_dates)
            min_start_dt = parse_iso_date('2022-01-01T00:00:00+05:30')
            
            # Convert to IST timezone if needed
            if isinstance(max_date, datetime):
                if max_date.tzinfo is None:
                    max_date = max_date.replace(tzinfo=IST_OFFSET)
                else:
                    max_date = max_date.astimezone(IST_OFFSET)
                
                # Ensure we never go before 2022-01-01
                if max_date < min_start_dt:
                    logger.warning(f"⚠️  Max date ({max_date.isoformat()}) is before 2022-01-01, using 2022-01-01")
                    return '2022-01-01T00:00:00+05:30'
                
                # Apply overlap: go back by overlap_days to ensure no data is missed
                overlap_days = ETL_CONFIG.get('chunk_overlap_days', 1)
                effective_start = max_date - timedelta(days=overlap_days)
                
                # Ensure we never go before 2022-01-01
                if effective_start < min_start_dt:
                    effective_start = min_start_dt
                
                logger.info(f"📊 Chargesheet tables have data, latest date: {max_date.isoformat()}")
                logger.info(f"📊 Effective start date (with {overlap_days} day overlap): {effective_start.isoformat()}")
                return effective_start.isoformat()
            
            # Fallback to start date
            logger.warning("⚠️  Could not determine max date, using 2022-01-01")
            return '2022-01-01T00:00:00+05:30'
            
        except Exception as e:
            logger.error(f"❌ Error getting effective start date: {e}")
            self.db_conn.rollback()
            logger.warning("⚠️  Using default start date: 2022-01-01")
            return '2022-01-01T00:00:00+05:30'
    
    def detect_new_fields(self, api_record: Dict, table_columns: Set[str]) -> Dict[str, str]:
        """
        Detect new fields in API response that don't exist in chargesheets table.
        Returns dict mapping API field name to database column name (snake_case).
        API uses camelCase field names.
        """
        new_fields = {}
        
        # Map API field names (camelCase) to database column names (snake_case)
        field_mapping = {
            'crimeId': 'crime_id',
            'chargeSheetNo': 'chargesheet_no',
            'chargeSheetNoForIcjs': 'chargesheet_no_icjs',
            'chargeSheetDate': 'chargesheet_date',
            'chargeSheetType': 'chargesheet_type',
            'courtName': 'court_name',
            'isCcl': 'is_ccl',
            'isEsigned': 'is_esigned',
            'dateCreated': 'date_created',
            'dateModified': 'date_modified',
            # Also support uppercase for backward compatibility
            'CRIME_ID': 'crime_id',
            'CHARGESHEET_NO': 'chargesheet_no',
            'CHARGESHEET_NO_ICJS': 'chargesheet_no_icjs',
            'CHARGESHEET_DATE': 'chargesheet_date',
            'CHARGESHEET_TYPE': 'chargesheet_type',
            'COURT_NAME': 'court_name',
            'IS_CCL': 'is_ccl',
            'IS_ESIGNED': 'is_esigned',
            'DATE_CREATED': 'date_created',
            'DATE_MODIFIED': 'date_modified'
        }
        
        for api_field, db_column in field_mapping.items():
            if api_field in api_record and db_column not in table_columns:
                new_fields[api_field] = db_column
        
        return new_fields
    
    def add_column_to_table(self, column_name: str, column_type: str = 'TEXT'):
        """Add a new column to the chargesheets table."""
        try:
            # Determine column type based on field name
            if column_name == 'chargesheet_date' or 'date' in column_name.lower():
                column_type = 'TIMESTAMP'
            elif column_name == 'crime_id':
                column_type = 'VARCHAR(50)'
            elif column_name in ('chargesheet_no', 'chargesheet_no_icjs', 'chargesheet_type'):
                column_type = 'VARCHAR(50)'
            elif column_name in ('is_ccl', 'is_esigned'):
                column_type = 'BOOLEAN'
            elif column_name in ('court_name'):
                column_type = 'TEXT'
            else:
                column_type = 'TEXT'
            
            alter_sql = f"ALTER TABLE {CHARGESHEETS_TABLE} ADD COLUMN IF NOT EXISTS {column_name} {column_type}"
            self.db_cursor.execute(alter_sql)
            self.db_conn.commit()
            logger.info(f"✅ Added column {column_name} ({column_type}) to {CHARGESHEETS_TABLE}")
            return True
        except Exception as e:
            logger.error(f"❌ Error adding column {column_name}: {e}")
            self.db_conn.rollback()
            return False
    
    def update_existing_records_with_new_fields(self, new_fields: Dict[str, str], chunk_end_date: str):
        """
        Update existing records from start_date to chunk_end_date with new fields.
        For new fields, set to NULL (they will be updated when those records are processed).
        """
        if not new_fields:
            return
        
        try:
            logger.info(f"📝 New fields detected: {list(new_fields.keys())}")
            logger.info(f"   Note: Existing records will be updated when processed in future ETL runs")
            logger.info(f"   New fields are set to NULL for existing records until they are reprocessed")
        except Exception as e:
            logger.error(f"❌ Error updating existing records: {e}")
    
    def generate_date_ranges(self, start_date: str, end_date: str, chunk_days: int = 5, overlap_days: int = 1) -> List[Tuple[str, str]]:
        """
        Generate date ranges in chunks with overlap to ensure no data is missed
        OVERLAP: Each chunk overlaps with the previous chunk by overlap_days to catch boundary records
        
        Accepts ISO 8601 format dates (e.g., '2022-10-01T00:00:00+05:30')
        Returns date-only format (YYYY-MM-DD) for API compatibility
        
        API interprets dates as:
        - fromDate: YYYY-MM-DD 00:00:00 IST (start of day)
        - toDate: YYYY-MM-DD 23:59:59 IST (end of day)
        
        Example with chunk_days=5, overlap_days=1:
        - Chunk 1: 2022-10-01 to 2022-10-05 (2022-10-01 00:00:00 IST to 2022-10-05 23:59:59 IST)
        - Chunk 2: 2022-10-05 to 2022-10-10 (2022-10-05 00:00:00 IST to 2022-10-10 23:59:59 IST) - overlaps by 1 day
        - Chunk 3: 2022-10-10 to 2022-10-15 (2022-10-10 00:00:00 IST to 2022-10-15 23:59:59 IST) - overlaps by 1 day
        - Overlap ensures no records are missed at boundaries ✅
        
        Note: Duplicate records from overlap are handled by smart update logic (updates only if changed)
        
        Args:
            start_date: Start date in ISO 8601 format (e.g., '2022-10-01T00:00:00+05:30')
            end_date: End date in ISO 8601 format (e.g., '2025-11-18T23:59:59+05:30')
            chunk_days: Number of days per chunk
            overlap_days: Number of days to overlap between chunks (default: 1 to ensure no data loss)
        
        Returns:
            List of (from_date, to_date) tuples in YYYY-MM-DD format (for API compatibility)
        """
        date_ranges = []
        # Parse ISO format dates
        current_date = parse_iso_date(start_date)
        end = parse_iso_date(end_date)
        
        # Extract date part (without time) for comparison
        current_date_only = current_date.date()
        end_date_only = end.date()
        
        while current_date_only <= end_date_only:
            # Calculate chunk end: current_date + (chunk_days - 1) days
            chunk_end_date = current_date_only + timedelta(days=chunk_days - 1)
            if chunk_end_date > end_date_only:
                chunk_end_date = end_date_only
            
            # Return date-only format for API compatibility
            date_ranges.append((
                current_date_only.strftime('%Y-%m-%d'),
                chunk_end_date.strftime('%Y-%m-%d')
            ))
            
            # Next chunk starts with overlap
            next_start = chunk_end_date - timedelta(days=overlap_days - 1)
            
            # If we've already reached or passed the end date, break
            if chunk_end_date >= end_date_only:
                break
            
            # Move to next chunk start
            current_date_only = next_start
        
        return date_ranges
    
    def fetch_chargesheets_api(self, from_date: str, to_date: str) -> Optional[List[Dict]]:
        """
        Fetch chargesheets data from API for given date range
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
        
        Returns:
            List of chargesheet records or None if failed
        """
        # Use chargesheets_url from config (which reads from .env)
        url = API_CONFIG.get('chargesheets_url', f"{API_CONFIG['base_url']}/chargesheets")
        params = {
            'fromDate': from_date,
            'toDate': to_date
        }
        headers = {
            'x-api-key': API_CONFIG['api_key']
        }
        
        for attempt in range(API_CONFIG['max_retries']):
            try:
                logger.debug(f"Fetching chargesheets: {from_date} to {to_date} (Attempt {attempt + 1})")
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
                        chargesheets_data = data.get('data')
                        if chargesheets_data:
                            # If single object, convert to list
                            if isinstance(chargesheets_data, dict):
                                chargesheets_data = [chargesheets_data]
                            
                            # Extract crime_ids for logging (API uses camelCase: 'crimeId')
                            crime_ids = [d.get('crimeId') or d.get('CRIME_ID') for d in chargesheets_data if d.get('crimeId') or d.get('CRIME_ID')]
                            
                            # Log to API chunk file
                            self.log_api_chunk(from_date, to_date, len(chargesheets_data), crime_ids, chargesheets_data)
                            
                            logger.info(f"✅ Fetched {len(chargesheets_data)} chargesheet records for {from_date} to {to_date}")
                            logger.debug(f"📋 Crime IDs from API: {crime_ids[:10]}{'...' if len(crime_ids) > 10 else ''}")
                            logger.trace(f"Full Crime IDs list: {crime_ids}")
                            logger.trace(f"Sample chargesheet structure: {json.dumps(chargesheets_data[0] if chargesheets_data else {}, indent=2, default=str)}")
                            return chargesheets_data
                        else:
                            # Log empty response
                            self.log_api_chunk(from_date, to_date, 0, [], [])
                            logger.warning(f"⚠️  No chargesheet records found for {from_date} to {to_date}")
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
        
        logger.error(f"❌ Failed to fetch chargesheets for {from_date} to {to_date} after {API_CONFIG['max_retries']} attempts")
        self.log_api_chunk(from_date, to_date, 0, [], [], error="Failed after max retries")
        return None
    
    def log_api_chunk(self, from_date: str, to_date: str, count: int, crime_ids: List[str], 
                     chargesheets_data: List[Dict], error: Optional[str] = None):
        """Log API response for a chunk"""
        chunk_info = {
            'chunk': f"{from_date} to {to_date}",
            'timestamp': datetime.now().isoformat(),
            'count': count,
            'crime_ids': crime_ids,
            'error': error
        }
        
        self.api_log.write(f"\n{'='*80}\n")
        self.api_log.write(f"CHUNK: {from_date} to {to_date}\n")
        self.api_log.write(f"Timestamp: {datetime.now().isoformat()}\n")
        self.api_log.write(f"{'-'*80}\n")
        
        if error:
            self.api_log.write(f"ERROR: {error}\n")
            self.api_log.write(f"Count: 0\n")
            self.api_log.write(f"Crime IDs: []\n")
        else:
            self.api_log.write(f"Count: {count}\n")
            self.api_log.write(f"Crime IDs ({len(crime_ids)}):\n")
            for i, crime_id in enumerate(crime_ids, 1):
                self.api_log.write(f"  {i}. {crime_id}\n")
            
            # Also write JSON format for easy parsing
            self.api_log.write(f"\nJSON Format:\n")
            self.api_log.write(json.dumps(chunk_info, indent=2, ensure_ascii=False))
            self.api_log.write(f"\n")
        
        self.api_log.flush()
    
    def normalize_date_value(self, value):
        """
        Normalize date/timestamp values from API
        Converts empty strings to None (NULL) for PostgreSQL compatibility
        
        Args:
            value: Date/timestamp value from API (can be string, None, or empty string)
        
        Returns:
            Normalized value (None if empty string, otherwise parsed datetime or original value)
        """
        if value == "" or value is None:
            return None
        # Try to parse as datetime if it's a string
        if isinstance(value, str):
            try:
                return parse_iso_date(value)
            except:
                return value
        return value
    
    def normalize_boolean_value(self, value):
        """Normalize boolean values from API"""
        if value is None or value == "":
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in ('true', '1', 'yes', 'on')
        return bool(value)
    
    def transform_chargesheet(self, chargesheet_raw: Dict) -> Dict:
        """
        Transform API response to database format
        Dates are always taken from API (never use CURRENT_TIMESTAMP)
        Validates crime_id exists in crimes table
        API uses camelCase field names
        
        Args:
            chargesheet_raw: Raw chargesheet data from API (camelCase fields)
        
        Returns:
            Transformed chargesheet dict ready for database
        """
        logger.trace(f"Transforming chargesheet: crimeId={chargesheet_raw.get('crimeId')}")
        
        # Get crime_id from API and validate it exists in crimes table
        # API uses camelCase: 'crimeId'
        crime_id_str = chargesheet_raw.get('crimeId')
        crime_id_valid = None
        
        if crime_id_str:
            # Validate that crime_id exists in crimes table
            try:
                self.db_cursor.execute(f"SELECT crime_id FROM {CRIMES_TABLE} WHERE crime_id = %s", (crime_id_str,))
                result = self.db_cursor.fetchone()
                if result:
                    crime_id_valid = crime_id_str
                    logger.trace(f"CRIME_ID {crime_id_str} found in crimes table")
                else:
                    logger.trace(f"CRIME_ID {crime_id_str} not found in crimes table")
            except Exception as e:
                logger.error(f"Error validating crime_id {crime_id_str}: {e}")
                # Rollback transaction on error to allow continuation
                self.db_conn.rollback()
        
        # Handle uploadChargeSheet.fileId for files
        upload_charge_sheet = chargesheet_raw.get('uploadChargeSheet', {})
        file_id = upload_charge_sheet.get('fileId') if isinstance(upload_charge_sheet, dict) else None
        files_list = [{'fileId': file_id}] if file_id else []
        
        transformed = {
            'crime_id': crime_id_valid,  # Validated crime_id (None if not found in crimes table)
            'chargesheet_no': chargesheet_raw.get('chargeSheetNo'),
            'chargesheet_no_icjs': chargesheet_raw.get('chargeSheetNoForIcjs'),
            'chargesheet_date': self.normalize_date_value(chargesheet_raw.get('chargeSheetDate')),
            'chargesheet_type': chargesheet_raw.get('chargeSheetType'),
            'court_name': chargesheet_raw.get('courtName'),
            'is_ccl': self.normalize_boolean_value(chargesheet_raw.get('isCcl')),
            'is_esigned': self.normalize_boolean_value(chargesheet_raw.get('isEsigned')),
            'date_created': self.normalize_date_value(chargesheet_raw.get('dateCreated')),
            'date_modified': self.normalize_date_value(chargesheet_raw.get('dateModified')),
            # Store nested data for related tables (API uses camelCase)
            '_files': files_list,  # From uploadChargeSheet.fileId
            '_acts': chargesheet_raw.get('actsAndSections', []),  # API uses 'actsAndSections'
            '_accused': chargesheet_raw.get('accusedParticulars', []),  # API uses 'accusedParticulars'
            # Store original CRIME_ID string for validation logging
            '_original_crime_id': crime_id_str
        }
        logger.trace(f"Transformed chargesheet: {json.dumps({k: v for k, v in transformed.items() if k != '_original_crime_id'}, indent=2, default=str)}")
        return transformed
    
    def chargesheet_exists(self, crime_id: str, chargesheet_no: Optional[str], chargesheet_date: Optional[datetime]) -> bool:
        """Check if chargesheet already exists in database"""
        logger.trace(f"Checking if chargesheet exists: crime_id={crime_id}, chargesheet_no={chargesheet_no}, chargesheet_date={chargesheet_date}")
        
        # Use crime_id + chargesheet_no + chargesheet_date as unique identifier
        # If chargesheet_no is None, use only crime_id + chargesheet_date
        if chargesheet_no:
            query = f"""
                SELECT 1 FROM {CHARGESHEETS_TABLE} 
                WHERE crime_id = %s AND chargesheet_no = %s AND chargesheet_date = %s
            """
            self.db_cursor.execute(query, (crime_id, chargesheet_no, chargesheet_date))
        else:
            query = f"""
                SELECT 1 FROM {CHARGESHEETS_TABLE} 
                WHERE crime_id = %s AND chargesheet_no IS NULL AND chargesheet_date = %s
            """
            self.db_cursor.execute(query, (crime_id, chargesheet_date))
        
        exists = self.db_cursor.fetchone() is not None
        logger.trace(f"Chargesheet exists: {exists}")
        return exists
    
    def get_existing_chargesheet(self, crime_id: str, chargesheet_no: Optional[str], chargesheet_date: Optional[datetime]) -> Optional[Dict]:
        """Get existing chargesheet record from database"""
        if chargesheet_no:
            query = f"""
                SELECT id, crime_id, chargesheet_no, chargesheet_no_icjs, chargesheet_date, chargesheet_type,
                       court_name, is_ccl, is_esigned, date_created, date_modified
                FROM {CHARGESHEETS_TABLE}
                WHERE crime_id = %s AND chargesheet_no = %s AND chargesheet_date = %s
            """
            self.db_cursor.execute(query, (crime_id, chargesheet_no, chargesheet_date))
        else:
            query = f"""
                SELECT id, crime_id, chargesheet_no, chargesheet_no_icjs, chargesheet_date, chargesheet_type,
                       court_name, is_ccl, is_esigned, date_created, date_modified
                FROM {CHARGESHEETS_TABLE}
                WHERE crime_id = %s AND chargesheet_no IS NULL AND chargesheet_date = %s
            """
            self.db_cursor.execute(query, (crime_id, chargesheet_date))
        
        row = self.db_cursor.fetchone()
        if row:
            return {
                'id': row[0],
                'crime_id': row[1],
                'chargesheet_no': row[2],
                'chargesheet_no_icjs': row[3],
                'chargesheet_date': row[4],
                'chargesheet_type': row[5],
                'court_name': row[6],
                'is_ccl': row[7],
                'is_esigned': row[8],
                'date_created': row[9],
                'date_modified': row[10]
            }
        return None
    
    def log_failed_record(self, chargesheet: Dict, reason: str, error_details: str = ""):
        """Log a failed record to the failed records log file"""
        failed_info = {
            'crime_id': chargesheet.get('crime_id'),
            'chargesheet_no': chargesheet.get('chargesheet_no'),
            'chargesheet_date': chargesheet.get('chargesheet_date'),
            'reason': reason,
            'error_details': error_details,
            'timestamp': datetime.now().isoformat(),
            'chargesheet_data': chargesheet
        }
        
        self.failed_log.write(f"\n{'='*80}\n")
        self.failed_log.write(f"CRIME_ID: {chargesheet.get('crime_id')}\n")
        self.failed_log.write(f"CHARGESHEET_NO: {chargesheet.get('chargesheet_no')}\n")
        self.failed_log.write(f"CHARGESHEET_DATE: {chargesheet.get('chargesheet_date')}\n")
        self.failed_log.write(f"REASON: {reason}\n")
        if error_details:
            self.failed_log.write(f"ERROR: {error_details}\n")
        self.failed_log.write(f"Timestamp: {datetime.now().isoformat()}\n")
        self.failed_log.write(f"\nJSON Format:\n")
        self.failed_log.write(json.dumps(failed_info, indent=2, ensure_ascii=False, default=str))
        self.failed_log.write(f"\n")
        self.failed_log.flush()
    
    def log_invalid_crime_id(self, chargesheet: Dict, crime_id_str: str, chunk_range: str = ""):
        """Log a chargesheet that failed due to invalid CRIME_ID (not found in crimes table)"""
        failure_info = {
            'crime_id': crime_id_str,
            'chargesheet_no': chargesheet.get('chargesheet_no'),
            'chargesheet_date': chargesheet.get('chargesheet_date'),
            'chargesheet_type': chargesheet.get('chargesheet_type'),
            'chunk': chunk_range,
            'timestamp': datetime.now().isoformat(),
            'chargesheet_data': chargesheet
        }
        
        self.invalid_crime_id_log.write(f"\n{'='*80}\n")
        self.invalid_crime_id_log.write(f"CRIME_ID: {crime_id_str}\n")
        self.invalid_crime_id_log.write(f"CHARGESHEET_NO: {chargesheet.get('chargesheet_no')}\n")
        self.invalid_crime_id_log.write(f"CHARGESHEET_DATE: {chargesheet.get('chargesheet_date')}\n")
        self.invalid_crime_id_log.write(f"CHARGESHEET_TYPE: {chargesheet.get('chargesheet_type')}\n")
        self.invalid_crime_id_log.write(f"REASON: CRIME_ID not found in crimes table\n")
        self.invalid_crime_id_log.write(f"Chunk: {chunk_range}\n")
        self.invalid_crime_id_log.write(f"Timestamp: {datetime.now().isoformat()}\n")
        self.invalid_crime_id_log.write(f"\nJSON Format:\n")
        self.invalid_crime_id_log.write(json.dumps(failure_info, indent=2, ensure_ascii=False, default=str))
        self.invalid_crime_id_log.write(f"\n")
        self.invalid_crime_id_log.flush()
    
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
        self.duplicates_log.write(f"Note: These duplicates were PROCESSED (not skipped) to allow updates\n")
        self.duplicates_log.write(f"\nDuplicates:\n")
        for i, dup in enumerate(duplicates, 1):
            self.duplicates_log.write(f"  {i}. CRIME_ID: {dup['crime_id']}, CHARGESHEET_NO: {dup.get('chargesheet_no')}, CHARGESHEET_DATE: {dup.get('chargesheet_date')}\n")
            self.duplicates_log.write(f"     Occurrence: #{dup.get('occurrence', 'N/A')}\n")
            self.duplicates_log.write(f"     First seen in: {dup['first_seen_in']}\n")
            self.duplicates_log.write(f"     Duplicate in: {dup['duplicate_in']}\n")
        
        # Also write JSON format for easy parsing
        self.duplicates_log.write(f"\nJSON Format:\n")
        self.duplicates_log.write(json.dumps(chunk_info, indent=2, ensure_ascii=False))
        self.duplicates_log.write(f"\n")
        
        self.duplicates_log.flush()
    
    def insert_chargesheet(self, chargesheet: Dict, chunk_date_range: str = "") -> Tuple[bool, str, Optional[str]]:
        """
        Insert or update single chargesheet into database with smart update logic
        Also handles related tables: chargesheet_files, chargesheet_acts, chargesheet_accused
        Dates are always from API (never use CURRENT_TIMESTAMP)
        
        Returns:
            Tuple of (success: bool, operation: str, chargesheet_id: str) where operation is 'inserted', 'updated', 'no_change', or 'skipped'
            chargesheet_id is returned as string for psycopg2 compatibility
        """
        crime_id = chargesheet.get('crime_id')
        chargesheet_no = chargesheet.get('chargesheet_no')
        chargesheet_date = chargesheet.get('chargesheet_date')
        original_crime_id = chargesheet.get('_original_crime_id')
        
        # Validate crime_id exists in crimes table
        if not crime_id:
            reason = 'invalid_crime_id'
            error_details = f"CRIME_ID {original_crime_id} not found in crimes table"
            logger.warning(f"⚠️  {error_details}, skipping chargesheet")
            self.stats['total_chargesheets_failed'] += 1
            self.stats['total_chargesheets_failed_crime_id'] += 1
            self.log_failed_record(chargesheet, reason, error_details)
            self.log_invalid_crime_id(chargesheet, original_crime_id, chunk_date_range)
            return False, reason, None
        
        try:
            logger.trace(f"Processing chargesheet: crime_id={crime_id}, chargesheet_no={chargesheet_no}, chargesheet_date={chargesheet_date}")
            
            # Check if chargesheet already exists
            if self.chargesheet_exists(crime_id, chargesheet_no, chargesheet_date):
                # Get existing record to compare
                existing = self.get_existing_chargesheet(crime_id, chargesheet_no, chargesheet_date)
                if not existing:
                    logger.warning(f"⚠️  Chargesheet exists check returned True but fetch returned None")
                    existing = None
                
                if existing:
                    # Convert UUID to string (psycopg2 returns UUID objects from database)
                    chargesheet_id = str(existing['id']) if existing['id'] else None
                    if not chargesheet_id:
                        logger.warning(f"⚠️  Existing chargesheet has no id")
                        existing = None
                        chargesheet_id = None
                    
                    # Smart update: only update fields that need updating
                    update_fields = []
                    update_values = []
                    changes = []
                    
                    # Define all fields to check (excluding primary key: id)
                    fields_to_check = [
                        ('chargesheet_no_icjs', 'CHARGESHEET_NO_ICJS'),
                        ('chargesheet_type', 'CHARGESHEET_TYPE'),
                        ('court_name', 'COURT_NAME'),
                        ('is_ccl', 'IS_CCL'),
                        ('is_esigned', 'IS_ESIGNED'),
                        ('date_created', 'DATE_CREATED'),  # Always from API
                        ('date_modified', 'DATE_MODIFIED')  # Always from API
                    ]
                    
                    for db_field, api_field in fields_to_check:
                        existing_val = existing.get(db_field)
                        new_val = chargesheet.get(db_field)
                        
                        # Special handling for date fields - always use API value
                        if db_field in ('date_created', 'date_modified'):
                            if existing_val != new_val:
                                update_fields.append(f"{db_field} = %s")
                                update_values.append(new_val)
                                changes.append(f"{db_field}: {existing_val} → {new_val}")
                        else:
                            # Rule 1: Existing is NULL, new is not NULL → update
                            if existing_val is None and new_val is not None:
                                update_fields.append(f"{db_field} = %s")
                                update_values.append(new_val)
                                changes.append(f"{db_field}: NULL → {new_val}")
                            
                            # Rule 2: Existing is not NULL, new is NULL → keep existing (skip)
                            elif existing_val is not None and new_val is None:
                                logger.trace(f"  Will keep existing {db_field}: {existing_val} (new value is NULL)")
                            
                            # Rule 3 & 4: Both are not NULL
                            elif existing_val is not None and new_val is not None:
                                # Rule 3: Different → update
                                if existing_val != new_val:
                                    update_fields.append(f"{db_field} = %s")
                                    update_values.append(new_val)
                                    changes.append(f"{db_field}: {existing_val} → {new_val}")
                            
                            # Both are NULL → no update needed
                    
                    # Only update if there are changes
                    if update_fields:
                        update_query = f"""
                            UPDATE {CHARGESHEETS_TABLE} SET
                                {', '.join(update_fields)}
                            WHERE id = %s
                        """
                        update_values.append(chargesheet_id)  # chargesheet_id is already a string
                        self.db_cursor.execute(update_query, tuple(update_values))
                        self.stats['total_chargesheets_updated'] += 1
                        logger.debug(f"Updated chargesheet: id={chargesheet_id} ({len(changes)} fields changed)")
                        self.db_conn.commit()
                        operation = 'updated'
                    else:
                        self.stats['total_chargesheets_no_change'] += 1
                        logger.trace(f"No changes needed for chargesheet")
                        operation = 'no_change'
                    
                    # Process related tables (files, acts, accused)
                    self.process_related_tables(chargesheet_id, chargesheet)
                    
                    return True, operation, chargesheet_id
                else:
                    # Exists check returned True but couldn't fetch - treat as new insert
                    logger.warning(f"⚠️  Chargesheet exists but couldn't fetch, treating as new insert")
            else:
                # Insert new chargesheet
                logger.trace(f"Inserting new chargesheet: crime_id={crime_id}, chargesheet_no={chargesheet_no}")
                chargesheet_id = str(uuid.uuid4())  # Convert UUID to string for psycopg2
                
                insert_query = f"""
                    INSERT INTO {CHARGESHEETS_TABLE} (
                        id, crime_id, chargesheet_no, chargesheet_no_icjs, chargesheet_date,
                        chargesheet_type, court_name, is_ccl, is_esigned,
                        date_created, date_modified
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """
                self.db_cursor.execute(insert_query, (
                    chargesheet_id,  # chargesheet_id is now a string
                    crime_id,
                    chargesheet_no,
                    chargesheet.get('chargesheet_no_icjs'),
                    chargesheet_date,
                    chargesheet.get('chargesheet_type'),
                    chargesheet.get('court_name'),
                    chargesheet.get('is_ccl'),
                    chargesheet.get('is_esigned'),
                    chargesheet.get('date_created'),
                    chargesheet.get('date_modified')
                ))
                self.stats['total_chargesheets_inserted'] += 1
                logger.debug(f"Inserted chargesheet: id={chargesheet_id}")
                self.db_conn.commit()
                
                # Process related tables (files, acts, accused)
                self.process_related_tables(chargesheet_id, chargesheet)
                
                return True, 'inserted', chargesheet_id
            
        except psycopg2.IntegrityError as e:
            self.db_conn.rollback()
            reason = 'integrity_error'
            error_details = str(e)
            logger.warning(f"⚠️  Integrity error for chargesheet: {e}")
            self.stats['total_chargesheets_failed'] += 1
            self.log_failed_record(chargesheet, reason, error_details)
            return False, reason, None
        except Exception as e:
            self.db_conn.rollback()
            reason = 'error'
            error_details = str(e)
            logger.error(f"❌ Error inserting chargesheet: {e}")
            self.stats['total_chargesheets_failed'] += 1
            self.stats['errors'].append(f"Chargesheet crime_id={crime_id}: {str(e)}")
            self.log_failed_record(chargesheet, reason, error_details)
            return False, reason, None
    
    def process_related_tables(self, chargesheet_id: str, chargesheet: Dict):
        """Process related tables: files, acts, accused"""
        # Process files
        files = chargesheet.get('_files', [])
        for file_data in files:
            self.insert_chargesheet_file(chargesheet_id, file_data)
        
        # Process acts
        acts = chargesheet.get('_acts', [])
        for act_data in acts:
            self.insert_chargesheet_act(chargesheet_id, act_data)
        
        # Process accused
        accused_list = chargesheet.get('_accused', [])
        for accused_data in accused_list:
            self.insert_chargesheet_accused(chargesheet_id, accused_data)
    
    def insert_chargesheet_file(self, chargesheet_id: str, file_data: Dict):
        """Insert or update chargesheet file"""
        try:
            # API uses camelCase: 'fileId'
            file_id = file_data.get('fileId') or file_data.get('FILE_ID')
            created_at = self.normalize_date_value(file_data.get('createdAt') or file_data.get('CREATED_AT'))
            
            # Check if file already exists
            query = f"""
                SELECT id FROM {CHARGESHEET_FILES_TABLE}
                WHERE chargesheet_id = %s AND file_id = %s
            """
            self.db_cursor.execute(query, (chargesheet_id, file_id))  # chargesheet_id is already a string
            existing = self.db_cursor.fetchone()
            
            if existing:
                # Update existing file
                update_query = f"""
                    UPDATE {CHARGESHEET_FILES_TABLE} SET
                        created_at = %s
                    WHERE id = %s
                """
                self.db_cursor.execute(update_query, (created_at, existing[0]))
                self.stats['total_files_updated'] += 1
            else:
                # Insert new file
                insert_query = f"""
                    INSERT INTO {CHARGESHEET_FILES_TABLE} (
                        id, chargesheet_id, file_id, created_at
                    ) VALUES (
                        %s, %s, %s, %s
                    )
                """
                self.db_cursor.execute(insert_query, (
                    str(uuid.uuid4()),  # Convert UUID to string
                    chargesheet_id,  # chargesheet_id is already a string
                    file_id,
                    created_at
                ))
                self.stats['total_files_inserted'] += 1
            
            self.db_conn.commit()
        except Exception as e:
            logger.error(f"❌ Error processing chargesheet file: {e}")
            self.db_conn.rollback()
    
    def insert_chargesheet_act(self, chargesheet_id: str, act_data: Dict):
        """Insert or update chargesheet act"""
        try:
            # API uses camelCase and section is an array
            section_array = act_data.get('section', [])
            # Convert array to string (join with comma if array, otherwise use as-is)
            if isinstance(section_array, list):
                if section_array:
                    # Join array elements with comma, truncate to 50 chars if needed (VARCHAR(50) limit)
                    section = ', '.join(str(s) for s in section_array)[:50]
                else:
                    section = act_data.get('SECTION', '')
            elif section_array:
                # Already a string or other type
                section = str(section_array)[:50]
            else:
                # Fallback to uppercase field name
                section = act_data.get('SECTION', '')[:50]
            
            act_description = act_data.get('actDescription') or act_data.get('ACT_DESCRIPTION')
            rw_required = self.normalize_boolean_value(act_data.get('rwRequired') or act_data.get('RW_REQUIRED'))
            section_description = act_data.get('sectionDescription') or act_data.get('SECTION_DESCRIPTION')
            grave_particulars = act_data.get('graveParticulars') or act_data.get('GRAVE_PARTICULARS')
            created_at = self.normalize_date_value(act_data.get('createdAt') or act_data.get('CREATED_AT'))
            
            # Skip if section is empty
            if not section:
                logger.warning(f"⚠️  Skipping act with empty section for chargesheet_id={chargesheet_id}")
                return
            
            # Check if act already exists (by chargesheet_id + section)
            query = f"""
                SELECT id FROM {CHARGESHEET_ACTS_TABLE}
                WHERE chargesheet_id = %s AND section = %s
            """
            self.db_cursor.execute(query, (chargesheet_id, section))  # chargesheet_id is already a string
            existing = self.db_cursor.fetchone()
            
            if existing:
                # Update existing act
                update_query = f"""
                    UPDATE {CHARGESHEET_ACTS_TABLE} SET
                        act_description = %s, rw_required = %s, section_description = %s,
                        grave_particulars = %s, created_at = %s
                    WHERE id = %s
                """
                self.db_cursor.execute(update_query, (
                    act_description, rw_required, section_description,
                    grave_particulars, created_at, existing[0]
                ))
                self.stats['total_acts_updated'] += 1
            else:
                # Insert new act
                insert_query = f"""
                    INSERT INTO {CHARGESHEET_ACTS_TABLE} (
                        id, chargesheet_id, act_description, section, rw_required,
                        section_description, grave_particulars, created_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """
                self.db_cursor.execute(insert_query, (
                    str(uuid.uuid4()),  # Convert UUID to string
                    chargesheet_id,  # chargesheet_id is already a string
                    act_description,
                    section,
                    rw_required,
                    section_description,
                    grave_particulars,
                    created_at
                ))
                self.stats['total_acts_inserted'] += 1
            
            self.db_conn.commit()
        except Exception as e:
            logger.error(f"❌ Error processing chargesheet act: {e}")
            self.db_conn.rollback()
    
    def insert_chargesheet_accused(self, chargesheet_id: str, accused_data: Dict):
        """Insert or update chargesheet accused"""
        try:
            # API uses camelCase
            accused_person_id = accused_data.get('accusedPersonId') or accused_data.get('ACCUSED_PERSON_ID')
            charge_status = accused_data.get('chargeStatus') or accused_data.get('CHARGE_STATUS')
            requested_for_nbw = self.normalize_boolean_value(accused_data.get('requestedForNBW') or accused_data.get('REQUESTED_FOR_NBW'))
            reason_for_no_charge = accused_data.get('reasonForNoCharge') or accused_data.get('REASON_FOR_NO_CHARGE')
            # API doesn't seem to have isPersonMasterPresent, default to True
            is_person_master_present = self.normalize_boolean_value(accused_data.get('isPersonMasterPresent') or accused_data.get('IS_PERSON_MASTER_PRESENT', True))
            created_at = self.normalize_date_value(accused_data.get('createdAt') or accused_data.get('CREATED_AT'))
            
            # Check if accused already exists (by chargesheet_id + accused_person_id)
            query = f"""
                SELECT id FROM {CHARGESHEET_ACCUSED_TABLE}
                WHERE chargesheet_id = %s AND accused_person_id = %s
            """
            self.db_cursor.execute(query, (chargesheet_id, accused_person_id))  # chargesheet_id is already a string
            existing = self.db_cursor.fetchone()
            
            if existing:
                # Update existing accused
                update_query = f"""
                    UPDATE {CHARGESHEET_ACCUSED_TABLE} SET
                        charge_status = %s, requested_for_nbw = %s, reason_for_no_charge = %s,
                        is_person_master_present = %s, created_at = %s
                    WHERE id = %s
                """
                self.db_cursor.execute(update_query, (
                    charge_status, requested_for_nbw, reason_for_no_charge,
                    is_person_master_present, created_at, existing[0]
                ))
                self.stats['total_accused_updated'] += 1
            else:
                # Insert new accused
                insert_query = f"""
                    INSERT INTO {CHARGESHEET_ACCUSED_TABLE} (
                        id, chargesheet_id, accused_person_id, charge_status, requested_for_nbw,
                        reason_for_no_charge, is_person_master_present, created_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """
                self.db_cursor.execute(insert_query, (
                    str(uuid.uuid4()),  # Convert UUID to string
                    chargesheet_id,  # chargesheet_id is already a string
                    accused_person_id,
                    charge_status,
                    requested_for_nbw,
                    reason_for_no_charge,
                    is_person_master_present,
                    created_at
                ))
                self.stats['total_accused_inserted'] += 1
            
            self.db_conn.commit()
        except Exception as e:
            logger.error(f"❌ Error processing chargesheet accused: {e}")
            self.db_conn.rollback()
    
    def process_date_range(self, from_date: str, to_date: str, table_columns: Set[str] = None):
        """Process chargesheet records for a specific date range"""
        chunk_range = f"{from_date} to {to_date}"
        logger.info(f"📅 Processing: {chunk_range}")
        
        # Fetch chargesheets from API
        chargesheets_raw = self.fetch_chargesheets_api(from_date, to_date)
        
        if chargesheets_raw is None:
            logger.error(f"❌ Failed to fetch chargesheets for {chunk_range}")
            self.log_db_chunk(from_date, to_date, 0, [], [], [], [], {}, error="API fetch failed")
            return
        
        if not chargesheets_raw:
            logger.info(f"ℹ️  No chargesheet records found for {chunk_range}")
            self.log_db_chunk(from_date, to_date, 0, [], [], [], [], {}, error="No chargesheet records in API response")
            return
        
        # Check for schema evolution if we got data
        if table_columns is not None and len(chargesheets_raw) > 0:
            # Check for new fields in first record
            new_fields = self.detect_new_fields(chargesheets_raw[0], table_columns)
            if new_fields:
                logger.info(f"🔍 New fields detected in API response: {list(new_fields.keys())}")
                # Add new columns to table
                for api_field, db_column in new_fields.items():
                    if self.add_column_to_table(db_column):
                        # Update table_columns set
                        table_columns.add(db_column)
                # Update existing records from start_date to current chunk end_date
                self.update_existing_records_with_new_fields(new_fields, to_date)
        
        # Transform and insert each chargesheet
        self.stats['total_chargesheets_fetched'] += len(chargesheets_raw)
        logger.trace(f"Processing {len(chargesheets_raw)} chargesheet records for chunk {chunk_range}")
        
        # Track operations for this chunk
        inserted_keys = []
        updated_keys = []
        no_change_keys = []
        failed_keys = []
        failed_reasons = {}
        duplicates_in_chunk = []
        invalid_crime_ids_in_chunk = []
        
        # Track unique keys seen in this chunk to detect duplicates (for reporting only, not skipping)
        seen_keys = {}
        key_occurrences = {}
        
        logger.trace(f"Starting to process records for chunk: {chunk_range}")
        for idx, chargesheet_record in enumerate(chargesheets_raw, 1):
            logger.trace(f"Processing record {idx}/{len(chargesheets_raw)}: {chargesheet_record.get('crimeId')}")
            chargesheet = self.transform_chargesheet(chargesheet_record)
            crime_id = chargesheet.get('crime_id')
            chargesheet_no = chargesheet.get('chargesheet_no')
            chargesheet_date = chargesheet.get('chargesheet_date')
            original_crime_id = chargesheet.get('_original_crime_id')
            
            # Check if crime_id is valid (exists in crimes table)
            if not crime_id:
                logger.warning(f"⚠️  Chargesheet with CRIME_ID {original_crime_id} not found in crimes table, skipping")
                self.stats['total_chargesheets_failed'] += 1
                self.stats['total_chargesheets_failed_crime_id'] += 1
                failed_keys.append(f"{original_crime_id}:{chargesheet_no}:{chargesheet_date}")
                reason = 'invalid_crime_id'
                if reason not in failed_reasons:
                    failed_reasons[reason] = []
                failed_reasons[reason].append(original_crime_id)
                invalid_crime_ids_in_chunk.append({
                    'crime_id': original_crime_id,
                    'chargesheet_no': chargesheet_no,
                    'chargesheet_date': chargesheet_date
                })
                self.log_invalid_crime_id(chargesheet, original_crime_id, chunk_range)
                continue
            
            # Create unique key for tracking duplicates
            unique_key = f"{crime_id}:{chargesheet_no}:{chargesheet_date}"
            
            # Track occurrences for duplicate reporting (but don't skip - process all)
            if unique_key in seen_keys:
                # This is a duplicate occurrence - track it but still process
                occurrence_count = key_occurrences.get(unique_key, 1) + 1
                key_occurrences[unique_key] = occurrence_count
                
                duplicates_in_chunk.append({
                    'crime_id': crime_id,
                    'chargesheet_no': chargesheet_no,
                    'chargesheet_date': chargesheet_date,
                    'occurrence': occurrence_count,
                    'first_seen_in': seen_keys[unique_key],
                    'duplicate_in': chunk_range
                })
                self.stats['total_duplicates'] += 1
                logger.info(f"⚠️  Duplicate chargesheet found in chunk {chunk_range} (occurrence #{occurrence_count}) - Will process to update record")
                logger.trace(f"Duplicate details - First seen: {seen_keys[unique_key]}, Current occurrence: {occurrence_count}")
            else:
                seen_keys[unique_key] = chunk_range
                key_occurrences[unique_key] = 1
                logger.trace(f"New chargesheet key seen: {unique_key} in chunk {chunk_range}")
            
            # IMPORTANT: Process ALL records, even duplicates
            # If same key appears multiple times, each occurrence might have updated data
            # The smart update logic will handle whether to actually update or not
            success, operation, chargesheet_id = self.insert_chargesheet(chargesheet, chunk_range)
            logger.trace(f"Operation result for chargesheet: success={success}, operation={operation}")
            if success:
                if operation == 'inserted':
                    # Only add to list if first occurrence (to avoid duplicate entries in log)
                    if unique_key not in inserted_keys:
                        inserted_keys.append(unique_key)
                    logger.trace(f"Added to inserted list: {unique_key}")
                elif operation == 'updated':
                    # Track all updates (even if same key updated multiple times)
                    updated_keys.append(unique_key)
                    logger.trace(f"Added to updated list: {unique_key} (occurrence #{key_occurrences.get(unique_key, 1)})")
                elif operation == 'no_change':
                    # Only add to list if first occurrence
                    if unique_key not in no_change_keys:
                        no_change_keys.append(unique_key)
                    logger.trace(f"Added to no_change list: {unique_key}")
            else:
                failed_keys.append(unique_key)
                if operation not in failed_reasons:
                    failed_reasons[operation] = []
                failed_reasons[operation].append(unique_key)
                logger.trace(f"Added to failed list: {unique_key}, reason: {operation}")
        
        # Log duplicates for this chunk (for reporting, but they were all processed)
        if duplicates_in_chunk:
            logger.info(f"📊 Found {len(duplicates_in_chunk)} duplicate occurrences in chunk {chunk_range} - All were processed for potential updates")
            logger.trace(f"Duplicate details: {duplicates_in_chunk}")
            self.log_duplicates_chunk(from_date, to_date, duplicates_in_chunk)
        
        # Log invalid crime_ids for this chunk
        if invalid_crime_ids_in_chunk:
            logger.warning(f"⚠️  Found {len(invalid_crime_ids_in_chunk)} chargesheet records with invalid CRIME_IDs in chunk {chunk_range}")
            # Extract unique CRIME_IDs
            unique_crime_ids = list(set([f['crime_id'] for f in invalid_crime_ids_in_chunk if f.get('crime_id')]))
            logger.warning(f"   Invalid CRIME_IDs: {unique_crime_ids}")
        
        # Log database operations for this chunk
        logger.trace(f"Chunk summary - Inserted: {len(inserted_keys)}, Updated: {len(updated_keys)}, No Change: {len(no_change_keys)}, Failed: {len(failed_keys)}, Duplicates: {len(duplicates_in_chunk)}, Invalid CRIME_IDs: {len(invalid_crime_ids_in_chunk)}")
        self.log_db_chunk(from_date, to_date, len(chargesheets_raw), inserted_keys, updated_keys, 
                         no_change_keys, failed_keys, failed_reasons)
        
        logger.info(f"✅ Completed: {chunk_range} - Inserted: {len(inserted_keys)}, Updated: {len(updated_keys)}, No Change: {len(no_change_keys)}, Failed: {len(failed_keys)}, Duplicates: {len(duplicates_in_chunk)}, Invalid CRIME_IDs: {len(invalid_crime_ids_in_chunk)}")
        logger.trace(f"Chunk processing complete for {chunk_range}")
    
    def log_db_chunk(self, from_date: str, to_date: str, total_fetched: int,
                    inserted_keys: List[str], updated_keys: List[str], no_change_keys: List[str],
                    failed_keys: List[str], failed_reasons: Dict, error: Optional[str] = None):
        """Log database operations for a chunk"""
        chunk_info = {
            'chunk': f"{from_date} to {to_date}",
            'timestamp': datetime.now().isoformat(),
            'total_fetched': total_fetched,
            'inserted_count': len(inserted_keys),
            'inserted_keys': inserted_keys,
            'updated_count': len(updated_keys),
            'updated_keys': updated_keys,
            'no_change_count': len(no_change_keys),
            'no_change_keys': no_change_keys,
            'failed_count': len(failed_keys),
            'failed_keys': failed_keys,
            'failed_reasons': failed_reasons,
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
            self.db_log.write(f"\nINSERTED: {len(inserted_keys)}\n")
            for i, key in enumerate(inserted_keys, 1):
                self.db_log.write(f"  {i}. {key}\n")
            
            self.db_log.write(f"\nUPDATED: {len(updated_keys)}\n")
            for i, key in enumerate(updated_keys, 1):
                self.db_log.write(f"  {i}. {key}\n")
            
            self.db_log.write(f"\nNO CHANGE: {len(no_change_keys)}\n")
            for i, key in enumerate(no_change_keys, 1):
                self.db_log.write(f"  {i}. {key}\n")
            
            self.db_log.write(f"\nFAILED: {len(failed_keys)}\n")
            if failed_reasons:
                for reason, keys in failed_reasons.items():
                    self.db_log.write(f"  Reason: {reason} ({len(keys)})\n")
                    for i, key in enumerate(keys[:20], 1):  # Show first 20
                        self.db_log.write(f"    {i}. {key}\n")
                    if len(keys) > 20:
                        self.db_log.write(f"    ... and {len(keys) - 20} more\n")
            
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
        self.api_log.write(f"Total Chargesheets Fetched: {self.stats['total_chargesheets_fetched']}\n")
        self.api_log.write(f"Failed API Calls: {self.stats['failed_api_calls']}\n")
        self.api_log.write(f"Total Chunks Processed: {self.stats['total_api_calls'] + self.stats['failed_api_calls']}\n")
        
        # DB log summary
        self.db_log.write(f"\n\n{'='*80}\n")
        self.db_log.write(f"SUMMARY\n")
        self.db_log.write(f"{'='*80}\n")
        self.db_log.write(f"Total Chargesheets Fetched from API: {self.stats['total_chargesheets_fetched']}\n")
        self.db_log.write(f"Total Chargesheets Inserted (New): {self.stats['total_chargesheets_inserted']}\n")
        self.db_log.write(f"Total Chargesheets Updated (Existing): {self.stats['total_chargesheets_updated']}\n")
        self.db_log.write(f"Total Chargesheets No Change: {self.stats['total_chargesheets_no_change']}\n")
        self.db_log.write(f"Total Chargesheets Failed: {self.stats['total_chargesheets_failed']}\n")
        self.db_log.write(f"  - Failed due to Invalid CRIME_ID: {self.stats['total_chargesheets_failed_crime_id']}\n")
        self.db_log.write(f"Total Chargesheets Duplicates (Processed): {self.stats['total_duplicates']}\n")
        self.db_log.write(f"Total Files Inserted: {self.stats['total_files_inserted']}\n")
        self.db_log.write(f"Total Files Updated: {self.stats['total_files_updated']}\n")
        self.db_log.write(f"Total Acts Inserted: {self.stats['total_acts_inserted']}\n")
        self.db_log.write(f"Total Acts Updated: {self.stats['total_acts_updated']}\n")
        self.db_log.write(f"Total Accused Inserted: {self.stats['total_accused_inserted']}\n")
        self.db_log.write(f"Total Accused Updated: {self.stats['total_accused_updated']}\n")
        self.db_log.write(f"Total Operations (Inserted + Updated + No Change): {self.stats['total_chargesheets_inserted'] + self.stats['total_chargesheets_updated'] + self.stats['total_chargesheets_no_change']}\n")
        db_total = self.stats.get('db_total_count', self.stats['total_chargesheets_inserted'])
        self.db_log.write(f"Total Unique Chargesheets in Database: {db_total}\n")
        self.db_log.write(f"Note: Updated count includes multiple updates (same key in multiple chunks or same chunk)\n")
        self.db_log.write(f"Note: Duplicates are records that appear multiple times within the same chunk - ALL are processed for updates\n")
        if self.stats['total_chargesheets_fetched'] > 0:
            coverage = ((self.stats['total_chargesheets_inserted'] + self.stats['total_chargesheets_updated'] + self.stats['total_chargesheets_no_change']) / self.stats['total_chargesheets_fetched']) * 100
            self.db_log.write(f"Coverage: {coverage:.2f}%\n")
        self.db_log.write(f"Errors: {len(self.stats['errors'])}\n")
        
        # Failed records log summary
        self.failed_log.write(f"\n\n{'='*80}\n")
        self.failed_log.write(f"SUMMARY\n")
        self.failed_log.write(f"{'='*80}\n")
        self.failed_log.write(f"Total Failed Records: {self.stats['total_chargesheets_failed']}\n")
        self.failed_log.write(f"Note: Failed records are those that could not be inserted or updated\n")
        self.failed_log.write(f"Check individual entries above for specific reasons\n")
        
        # Invalid CRIME_ID log summary
        self.invalid_crime_id_log.write(f"\n\n{'='*80}\n")
        self.invalid_crime_id_log.write(f"SUMMARY\n")
        self.invalid_crime_id_log.write(f"{'='*80}\n")
        self.invalid_crime_id_log.write(f"Total Chargesheets SKIPPED Due to Invalid CRIME_ID: {self.stats['total_chargesheets_failed_crime_id']}\n")
        self.invalid_crime_id_log.write(f"\n")
        self.invalid_crime_id_log.write(f"Note: These chargesheet records were SKIPPED because CRIME_ID was not found in crimes table.\n")
        self.invalid_crime_id_log.write(f"      CRIME_ID is a required foreign key, so these records cannot be processed.\n")
        self.invalid_crime_id_log.write(f"      Please ensure these CRIME_IDs are loaded in the crimes table first.\n")
        
        # Duplicates log summary
        self.duplicates_log.write(f"\n\n{'='*80}\n")
        self.duplicates_log.write(f"SUMMARY\n")
        self.duplicates_log.write(f"{'='*80}\n")
        self.duplicates_log.write(f"Total Duplicate Occurrences Found: {self.stats['total_duplicates']}\n")
        self.duplicates_log.write(f"Note: Duplicates are records that appear multiple times within the same chunk\n")
        self.duplicates_log.write(f"IMPORTANT: All duplicates are PROCESSED (not skipped) to allow updates\n")
        self.duplicates_log.write(f"If the same key appears multiple times, each occurrence is processed\n")
        self.duplicates_log.write(f"The smart update logic will determine if actual updates are needed\n")
    
    def run(self):
        """Main ETL execution"""
        logger.info("=" * 80)
        logger.info("🚀 DOPAMAS ETL Pipeline - Chargesheets API")
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
            # Get effective start date (check if all 4 tables have data)
            # This will rollback any failed transactions first
            effective_start_date = self.get_effective_start_date()
            logger.info(f"Effective Start Date: {effective_start_date}")
            
            # Get table columns for schema evolution
            # This will also rollback any failed transactions first
            table_columns = self.get_table_columns(CHARGESHEETS_TABLE)
            if table_columns:
                logger.debug(f"Existing table columns: {sorted(table_columns)}")
            else:
                logger.info("📊 No existing columns found (table may be empty or new), will detect schema from API")
            
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
            start_dt = parse_iso_date(effective_start_date)
            end_dt = parse_iso_date(calculated_end_date)
            logger.info(f"ℹ️  API Server Timezone: IST (UTC+05:30)")
            logger.info(f"ℹ️  Date Range: {format_iso_date(start_dt)} to {format_iso_date(end_dt)}")
            logger.info(f"ℹ️  ETL Server Timezone: UTC")
            logger.info("")
            
            # Process each date range with progress bar
            for from_date, to_date in tqdm(date_ranges, desc="Processing date ranges", unit="range"):
                # Process the chunk (will check for schema evolution and process data)
                self.process_date_range(from_date, to_date, table_columns)
                time.sleep(1)  # Be nice to the API
            
            # Get database counts
            self.db_cursor.execute(f"SELECT COUNT(*) FROM {CHARGESHEETS_TABLE}")
            db_chargesheets_count = self.db_cursor.fetchone()[0]
            
            # Store for summary
            self.stats['db_total_count'] = db_chargesheets_count
            
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
            logger.info(f"  Total Chargesheets Fetched: {self.stats['total_chargesheets_fetched']}")
            logger.info(f"")
            logger.info(f"💾 TO DATABASE:")
            logger.info(f"  Total Inserted (New): {self.stats['total_chargesheets_inserted']}")
            logger.info(f"  Total Updated:        {self.stats['total_chargesheets_updated']}")
            logger.info(f"  Total No Change:      {self.stats['total_chargesheets_no_change']}")
            logger.info(f"  Total Failed:         {self.stats['total_chargesheets_failed']}")
            logger.info(f"    - Invalid CRIME_ID:   {self.stats['total_chargesheets_failed_crime_id']}")
            logger.info(f"  Total in DB:          {db_chargesheets_count}")
            logger.info(f"")
            logger.info(f"📎 RELATED TABLES:")
            logger.info(f"  Files - Inserted: {self.stats['total_files_inserted']}, Updated: {self.stats['total_files_updated']}")
            logger.info(f"  Acts - Inserted: {self.stats['total_acts_inserted']}, Updated: {self.stats['total_acts_updated']}")
            logger.info(f"  Accused - Inserted: {self.stats['total_accused_inserted']}, Updated: {self.stats['total_accused_updated']}")
            logger.info(f"")
            logger.info(f"🔄 DUPLICATES:")
            logger.info(f"  Total Duplicate Occurrences (Processed): {self.stats['total_duplicates']}")
            logger.info(f"  Note: All duplicates are processed to allow updates")
            logger.info(f"")
            logger.info(f"⚠️  INVALID CRIME_ID:")
            logger.info(f"  Chargesheets SKIPPED Due to Invalid CRIME_ID: {self.stats['total_chargesheets_failed_crime_id']}")
            logger.info(f"    Check logs/chargesheets_invalid_crime_id_*.log for details")
            logger.info(f"")
            logger.info(f"📊 COVERAGE:")
            if self.stats['total_chargesheets_fetched'] > 0:
                coverage = ((self.stats['total_chargesheets_inserted'] + self.stats['total_chargesheets_updated'] + self.stats['total_chargesheets_no_change']) / self.stats['total_chargesheets_fetched']) * 100
                logger.info(f"  API → DB Coverage:   {coverage:.2f}%")
            logger.info(f"")
            logger.info(f"📈 SUMMARY:")
            logger.info(f"  Total from API:       {self.stats['total_chargesheets_fetched']}")
            logger.info(f"  Inserted + Updated:   {self.stats['total_chargesheets_inserted'] + self.stats['total_chargesheets_updated']}")
            logger.info(f"  Duplicate Occurrences: {self.stats['total_duplicates']} (all processed)")
            logger.info(f"  Failed:               {self.stats['total_chargesheets_failed']}")
            logger.info(f"")
            logger.info(f"💡 NOTE:")
            logger.info(f"  - Same chargesheet key can appear multiple times in API response")
            logger.info(f"  - Each occurrence is processed to capture any data updates")
            logger.info(f"  - Smart update logic ensures only changed fields are updated")
            logger.info(f"  - Related tables (files, acts, accused) are processed automatically")
            logger.info(f"  - CRIME_ID must exist in crimes table (foreign key validation)")
            logger.info(f"  - Invalid CRIME_IDs are logged separately for review")
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
            logger.info(f"📝 Failed records log saved to: {self.failed_log_file}")
            logger.info(f"📝 Invalid CRIME_ID log saved to: {self.invalid_crime_id_log_file}")
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
    etl = ChargesheetsETL()
    success = etl.run()
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()


