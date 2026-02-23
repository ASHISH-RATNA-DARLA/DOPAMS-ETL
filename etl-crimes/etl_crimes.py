#!/usr/bin/env python3
"""
DOPAMAS ETL Pipeline - Crimes API
Fetches crime data in 5-day chunks and loads into PostgreSQL
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
# The trace method was attached to logging.Logger class, but we ensure it's bound to this instance
if not hasattr(logger, 'trace') or not callable(getattr(logger, 'trace', None)):
    # Bind the trace method to this logger instance
    import types
    logger.trace = types.MethodType(trace, logger)

# Handle TRACE level
log_level = LOG_CONFIG['level'].upper()
if log_level == 'TRACE':
    logger.setLevel(TRACE_LEVEL)
else:
    logger.setLevel(log_level)

# Target tables (allows redirecting ETL into test tables)
CRIMES_TABLE = TABLE_CONFIG.get('crimes', 'crimes')
HIERARCHY_TABLE = TABLE_CONFIG.get('hierarchy', 'hierarchy')

# IST timezone offset (UTC+05:30)
IST_OFFSET = timezone(timedelta(hours=5, minutes=30))

def parse_iso_date(iso_date_str: str) -> datetime:
    """
    Parse ISO 8601 date string to datetime object
    Supports formats:
    - YYYY-MM-DDTHH:MM:SS+TZ:TZ (e.g., '2022-10-01T00:00:00+05:30')
    - YYYY-MM-DD (e.g., '2022-10-01') - defaults to 00:00:00 IST
    
    Args:
        iso_date_str: ISO 8601 date string
        
    Returns:
        datetime object with timezone info
    """
    try:
        # Try parsing as ISO format with timezone
        if 'T' in iso_date_str:
            # ISO format with time: 2022-10-01T00:00:00+05:30
            return datetime.fromisoformat(iso_date_str.replace('Z', '+00:00'))
        else:
            # Date only format: 2022-10-01 - default to 00:00:00 IST
            dt = datetime.strptime(iso_date_str, '%Y-%m-%d')
            return dt.replace(tzinfo=IST_OFFSET)
    except ValueError:
        # Fallback: try parsing as date only
        dt = datetime.strptime(iso_date_str.split('T')[0], '%Y-%m-%d')
        return dt.replace(tzinfo=IST_OFFSET)

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


class CrimesETL:
    """ETL Pipeline for Crimes API"""
    
    def __init__(self):
        self.db_conn = None
        self.db_cursor = None
        self.stats = {
            'total_api_calls': 0,
            'total_crimes_fetched': 0,
            'total_crimes_inserted': 0,
            'total_crimes_updated': 0,
            'total_crimes_no_change': 0,  # Records that exist but no changes needed
            'total_crimes_skipped': 0,
            'total_crimes_failed': 0,  # Records that failed to insert/update
            'total_crimes_failed_ps_code': 0,  # Crimes failed due to PS_CODE not found
            'total_duplicates': 0,  # Duplicate crime_ids found within chunks
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
        self.api_log_file = f'logs/crimes_api_chunks_{timestamp}.log'
        self.api_log = open(self.api_log_file, 'w', encoding='utf-8')
        self.api_log.write(f"# Crimes API Chunk-wise Log\n")
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
        self.db_log_file = f'logs/crimes_db_chunks_{timestamp}.log'
        self.db_log = open(self.db_log_file, 'w', encoding='utf-8')
        self.db_log.write(f"# Crimes Database Operations Chunk-wise Log\n")
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
        
        # Failed records log file (records that couldn't be inserted/updated)
        self.failed_log_file = f'logs/crimes_failed_{timestamp}.log'
        self.failed_log = open(self.failed_log_file, 'w', encoding='utf-8')
        self.failed_log.write(f"# Crimes Failed Records Log\n")
        self.failed_log.write(f"# Generated: {datetime.now().isoformat()}\n")
        self.failed_log.write(f"# Records that failed to insert or update with reasons\n")
        self.failed_log.write(f"{'='*80}\n\n")
        
        # Duplicates log file (duplicate crime_ids found within chunks)
        self.duplicates_log_file = f'logs/crimes_duplicates_{timestamp}.log'
        self.duplicates_log = open(self.duplicates_log_file, 'w', encoding='utf-8')
        self.duplicates_log.write(f"# Crimes Duplicates Log\n")
        self.duplicates_log.write(f"# Generated: {datetime.now().isoformat()}\n")
        self.duplicates_log.write(f"# Duplicate CRIME_IDs found within the same chunk\n")
        self.duplicates_log.write(f"{'='*80}\n\n")
        
        # PS_CODE failures log file (crimes that failed due to missing PS_CODE in hierarchy)
        self.ps_code_failures_log_file = f'logs/crimes_ps_code_failures_{timestamp}.log'
        self.ps_code_failures_log = open(self.ps_code_failures_log_file, 'w', encoding='utf-8')
        self.ps_code_failures_log.write(f"# Crimes PS_CODE Failures Log\n")
        self.ps_code_failures_log.write(f"# Generated: {datetime.now().isoformat()}\n")
        self.ps_code_failures_log.write(f"# Crimes that failed to insert/update because PS_CODE not found in hierarchy table\n")
        self.ps_code_failures_log.write(f"{'='*80}\n\n")
        
        logger.info(f"📝 API chunk log: {self.api_log_file}")
        logger.info(f"📝 DB chunk log: {self.db_log_file}")
        logger.info(f"📝 Failed records log: {self.failed_log_file}")
        logger.info(f"📝 Duplicates log: {self.duplicates_log_file}")
        logger.info(f"📝 PS_CODE failures log: {self.ps_code_failures_log_file}")
    
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
        if hasattr(self, 'ps_code_failures_log') and self.ps_code_failures_log:
            self.ps_code_failures_log.close()
    
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
        """
        try:
            # Check if table has any data
            self.db_cursor.execute(f"SELECT COUNT(*) FROM {CRIMES_TABLE}")
            count = self.db_cursor.fetchone()[0]
            
            if count == 0:
                # New database, start from beginning
                logger.info("📊 Table is empty, starting from 2022-01-01")
                return '2022-01-01T00:00:00+05:30'
            
            # Table has data, get max of date_created and date_modified
            # Only consider dates >= 2022-01-01 to avoid processing very old data
            MIN_START_DATE = '2022-01-01T00:00:00+05:30'
            min_start_dt = parse_iso_date('2022-01-01T00:00:00+05:30')
            
            self.db_cursor.execute(f"""
                SELECT GREATEST(
                    COALESCE(MAX(CASE WHEN date_created >= '2022-01-01'::timestamp THEN date_created END), '2022-01-01'::timestamp),
                    COALESCE(MAX(CASE WHEN date_modified >= '2022-01-01'::timestamp THEN date_modified END), '2022-01-01'::timestamp)
                ) as max_date
                FROM {CRIMES_TABLE}
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
            return '2022-01-01T00:00:00+05:30'
            
        except Exception as e:
            logger.error(f"❌ Error getting effective start date: {e}")
            logger.warning("⚠️  Using default start date: 2022-01-01")
            return '2022-01-01T00:00:00+05:30'
    
    def detect_new_fields(self, api_record: Dict, table_columns: Set[str]) -> Dict[str, str]:
        """
        Detect new fields in API response that don't exist in table.
        Returns dict mapping API field name to database column name (snake_case).
        """
        new_fields = {}
        
        # Map API field names to database column names
        field_mapping = {
            'CRIME_ID': 'crime_id',
            'PS_CODE': 'ps_code',
            'FIR_NUM': 'fir_num',
            'FIR_REG_NUM': 'fir_reg_num',
            'FIR_TYPE': 'fir_type',
            'ACTS_SECTIONS': 'acts_sections',
            'FIR_DATE': 'fir_date',
            'CASE_STATUS': 'case_status',
            'MAJOR_HEAD': 'major_head',
            'MINOR_HEAD': 'minor_head',
            'CRIME_TYPE': 'crime_type',
            'IO_NAME': 'io_name',
            'IO_RANK': 'io_rank',
            'BRIEF_FACTS': 'brief_facts',
            'FIR_COPY': 'fir_copy',  # FIR copy identifier
            'DATE_CREATED': 'date_created',
            'DATE_MODIFIED': 'date_modified'
        }
        
        for api_field, db_column in field_mapping.items():
            if api_field in api_record and db_column not in table_columns:
                new_fields[api_field] = db_column
        
        return new_fields
    
    def add_column_to_table(self, column_name: str, column_type: str = 'TEXT'):
        """Add a new column to the crimes table."""
        try:
            # Determine column type based on field name
            if 'date' in column_name.lower():
                column_type = 'TIMESTAMP'
            elif 'id' in column_name.lower() or 'code' in column_name.lower():
                column_type = 'VARCHAR(50)'
            elif column_name in ('brief_facts', 'acts_sections'):
                column_type = 'TEXT'
            else:
                column_type = 'VARCHAR(255)'
            
            alter_sql = f"ALTER TABLE {CRIMES_TABLE} ADD COLUMN IF NOT EXISTS {column_name} {column_type}"
            self.db_cursor.execute(alter_sql)
            self.db_conn.commit()
            logger.info(f"✅ Added column {column_name} ({column_type}) to {CRIMES_TABLE}")
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
            # Example: If current_date = 2022-10-01 and chunk_days = 5:
            #   chunk_end = 2022-10-01 + 4 days = 2022-10-05 (5 days total: 1,2,3,4,5)
            chunk_end_date = current_date_only + timedelta(days=chunk_days - 1)
            if chunk_end_date > end_date_only:
                chunk_end_date = end_date_only
            
            # Return date-only format for API compatibility
            date_ranges.append((
                current_date_only.strftime('%Y-%m-%d'),
                chunk_end_date.strftime('%Y-%m-%d')
            ))
            
            # Next chunk starts with overlap: current chunk end - overlap_days + 1
            # This ensures the last overlap_days of current chunk are included in next chunk
            # Example: If chunk_end = 2022-10-05 and overlap_days = 1:
            #   next chunk starts at 2022-10-05 - 1 + 1 = 2022-10-05 (includes day 5 in both chunks)
            # Example: If chunk_end = 2022-10-05 and overlap_days = 2:
            #   next chunk starts at 2022-10-05 - 2 + 1 = 2022-10-04 (includes days 4,5 in both chunks)
            next_start = chunk_end_date - timedelta(days=overlap_days - 1)
            
            # If we've already reached or passed the end date, break
            if chunk_end_date >= end_date_only:
                break
            
            # Move to next chunk start
            current_date_only = next_start
        
        return date_ranges
    
    def fetch_crimes_api(self, from_date: str, to_date: str) -> Optional[Dict]:
        """
        Fetch crimes from API for given date range
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
        
        Returns:
            API response dict or None if failed
        """
        url = f"{API_CONFIG['base_url']}/crimes"
        params = {
            'fromDate': from_date,
            'toDate': to_date
        }
        headers = {
            'x-api-key': API_CONFIG['api_key']
        }
        
        for attempt in range(API_CONFIG['max_retries']):
            try:
                logger.debug(f"Fetching crimes: {from_date} to {to_date} (Attempt {attempt + 1})")
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
                        crime_data = data.get('data')
                        if crime_data:
                            # If single object, convert to list
                            if isinstance(crime_data, dict):
                                crime_data = [crime_data]
                            
                            # Extract crime_ids for logging
                            crime_ids = [crime.get('CRIME_ID') for crime in crime_data if crime.get('CRIME_ID')]
                            
                            # Log to API chunk file
                            self.log_api_chunk(from_date, to_date, len(crime_data), crime_ids, crime_data)
                            
                            logger.info(f"✅ Fetched {len(crime_data)} crimes for {from_date} to {to_date}")
                            logger.debug(f"📋 Crime IDs from API: {crime_ids[:10]}{'...' if len(crime_ids) > 10 else ''}")
                            logger.trace(f"Full Crime IDs list: {crime_ids}")
                            logger.trace(f"Sample crime structure: {json.dumps(crime_data[0] if crime_data else {}, indent=2, default=str)}")
                            return crime_data
                        else:
                            # Log empty response
                            self.log_api_chunk(from_date, to_date, 0, [], [])
                            logger.warning(f"⚠️  No crimes found for {from_date} to {to_date}")
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
        
        logger.error(f"❌ Failed to fetch crimes for {from_date} to {to_date} after {API_CONFIG['max_retries']} attempts")
        self.log_api_chunk(from_date, to_date, 0, [], [], error="Failed after max retries")
        return None
    
    def log_api_chunk(self, from_date: str, to_date: str, count: int, crime_ids: List[str], 
                     crime_data: List[Dict], error: Optional[str] = None):
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
    
    def transform_crime(self, crime_raw: Dict) -> Dict:
        """
        Transform API response to database format
        Dates are always taken from API (never use CURRENT_TIMESTAMP)
        
        Args:
            crime_raw: Raw crime data from API
        
        Returns:
            Transformed crime dict ready for database
        """
        logger.trace(f"Transforming crime: CRIME_ID={crime_raw.get('CRIME_ID')}, FIR_NUM={crime_raw.get('FIR_NUM')}")
        transformed = {
            'crime_id': crime_raw.get('CRIME_ID'),
            'ps_code': crime_raw.get('PS_CODE'),
            'fir_num': crime_raw.get('FIR_NUM'),
            'fir_reg_num': crime_raw.get('FIR_REG_NUM'),
            'fir_type': crime_raw.get('FIR_TYPE'),
            'acts_sections': crime_raw.get('ACTS_SECTIONS'),
            'fir_date': crime_raw.get('FIR_DATE'),
            'case_status': crime_raw.get('CASE_STATUS'),
            'major_head': crime_raw.get('MAJOR_HEAD'),
            'minor_head': crime_raw.get('MINOR_HEAD'),
            'crime_type': crime_raw.get('CRIME_TYPE'),
            'io_name': crime_raw.get('IO_NAME'),
            'io_rank': crime_raw.get('IO_RANK'),
            'brief_facts': crime_raw.get('BRIEF_FACTS'),
            'fir_copy': crime_raw.get('FIR_COPY'),  # FIR copy identifier from API
            # Dates are always from API (never use CURRENT_TIMESTAMP)
            # If API doesn't provide dates, they will be NULL
            'date_created': crime_raw.get('DATE_CREATED') or None,
            'date_modified': crime_raw.get('DATE_MODIFIED') or None
        }
        logger.trace(f"Transformed crime: {json.dumps(transformed, indent=2, default=str)}")
        return transformed
    
    def crime_exists(self, crime_id: str) -> bool:
        """Check if crime already exists in database"""
        logger.trace(f"Checking if CRIME_ID exists in database: {crime_id}")
        self.db_cursor.execute(f"SELECT 1 FROM {CRIMES_TABLE} WHERE crime_id = %s", (crime_id,))
        exists = self.db_cursor.fetchone() is not None
        logger.trace(f"CRIME_ID {crime_id} exists: {exists}")
        return exists
    
    def get_existing_crime(self, crime_id: str) -> Optional[Dict]:
        """Get existing crime record from database"""
        query = f"""
            SELECT crime_id, ps_code, fir_num, fir_reg_num, fir_type,
                   acts_sections, fir_date, case_status, major_head, minor_head,
                   crime_type, io_name, io_rank, brief_facts, fir_copy,
                   date_created, date_modified
            FROM {CRIMES_TABLE}
            WHERE crime_id = %s
        """
        self.db_cursor.execute(query, (crime_id,))
        row = self.db_cursor.fetchone()
        if row:
            return {
                'crime_id': row[0],
                'ps_code': row[1],
                'fir_num': row[2],
                'fir_reg_num': row[3],
                'fir_type': row[4],
                'acts_sections': row[5],
                'fir_date': row[6],
                'case_status': row[7],
                'major_head': row[8],
                'minor_head': row[9],
                'crime_type': row[10],
                'io_name': row[11],
                'io_rank': row[12],
                'brief_facts': row[13],
                'fir_copy': row[14],
                'date_created': row[15],
                'date_modified': row[16]
            }
        return None
    
    def log_failed_record(self, crime: Dict, reason: str, error_details: str = ""):
        """Log a failed record to the failed records log file"""
        failed_info = {
            'crime_id': crime.get('crime_id'),
            'fir_num': crime.get('fir_num'),
            'ps_code': crime.get('ps_code'),
            'reason': reason,
            'error_details': error_details,
            'timestamp': datetime.now().isoformat(),
            'crime_data': crime
        }
        
        self.failed_log.write(f"\n{'='*80}\n")
        self.failed_log.write(f"CRIME_ID: {crime.get('crime_id')}\n")
        self.failed_log.write(f"FIR_NUM: {crime.get('fir_num')}\n")
        self.failed_log.write(f"PS_CODE: {crime.get('ps_code')}\n")
        self.failed_log.write(f"REASON: {reason}\n")
        if error_details:
            self.failed_log.write(f"ERROR: {error_details}\n")
        self.failed_log.write(f"Timestamp: {datetime.now().isoformat()}\n")
        self.failed_log.write(f"\nJSON Format:\n")
        self.failed_log.write(json.dumps(failed_info, indent=2, ensure_ascii=False, default=str))
        self.failed_log.write(f"\n")
        self.failed_log.flush()
    
    def log_ps_code_failure(self, crime: Dict, ps_code: str, chunk_range: str = ""):
        """Log a crime that failed due to missing PS_CODE in hierarchy"""
        failure_info = {
            'crime_id': crime.get('crime_id'),
            'fir_num': crime.get('fir_num'),
            'ps_code': ps_code,
            'chunk': chunk_range,
            'timestamp': datetime.now().isoformat(),
            'crime_data': crime
        }
        
        self.ps_code_failures_log.write(f"\n{'='*80}\n")
        self.ps_code_failures_log.write(f"CRIME_ID: {crime.get('crime_id')}\n")
        self.ps_code_failures_log.write(f"FIR_NUM: {crime.get('fir_num')}\n")
        self.ps_code_failures_log.write(f"PS_CODE: {ps_code}\n")
        self.ps_code_failures_log.write(f"REASON: PS_CODE not found in hierarchy table\n")
        self.ps_code_failures_log.write(f"Chunk: {chunk_range}\n")
        self.ps_code_failures_log.write(f"Timestamp: {datetime.now().isoformat()}\n")
        self.ps_code_failures_log.write(f"\nJSON Format:\n")
        self.ps_code_failures_log.write(json.dumps(failure_info, indent=2, ensure_ascii=False, default=str))
        self.ps_code_failures_log.write(f"\n")
        self.ps_code_failures_log.flush()
    
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
            self.duplicates_log.write(f"  {i}. CRIME_ID: {dup['crime_id']}\n")
            self.duplicates_log.write(f"     FIR_NUM: {dup.get('fir_num', 'N/A')}\n")
            self.duplicates_log.write(f"     PS_CODE: {dup.get('ps_code', 'N/A')}\n")
            self.duplicates_log.write(f"     Occurrence: #{dup.get('occurrence', 'N/A')}\n")
            self.duplicates_log.write(f"     First seen in: {dup['first_seen_in']}\n")
            self.duplicates_log.write(f"     Duplicate in: {dup['duplicate_in']}\n")
        
        # Also write JSON format for easy parsing
        self.duplicates_log.write(f"\nJSON Format:\n")
        self.duplicates_log.write(json.dumps(chunk_info, indent=2, ensure_ascii=False))
        self.duplicates_log.write(f"\n")
        
        self.duplicates_log.flush()
    
    def insert_crime(self, crime: Dict, chunk_date_range: str = "") -> Tuple[bool, str]:
        """
        Insert or update single crime into database with smart update logic
        Dates are always from API (never use CURRENT_TIMESTAMP)
        
        Behavior:
        - NEW DATA: If crime_id doesn't exist → INSERT (creates new record)
        - EXISTING DATA: If crime_id exists → UPDATE (updates only changed fields)
        - Smart Update: Only updates fields that have changed, preserves existing values if API sends NULL
        
        Date Handling:
        - date_created and date_modified are always taken from API
        - If API provides dates, they are used (even if different from existing)
        - If API doesn't provide dates, they remain NULL
        
        Args:
            crime: Transformed crime dict
            chunk_date_range: Date range for chunk tracking
        
        Returns:
            Tuple of (success: bool, operation: str) where operation is 'inserted', 'updated', 'no_change', or 'skipped'
        """
        crime_id = crime.get('crime_id')
        if not crime_id:
            reason = 'missing_crime_id'
            error_details = "Crime record missing CRIME_ID"
            logger.warning(f"⚠️  {error_details}")
            self.stats['total_crimes_failed'] += 1
            self.log_failed_record(crime, reason, error_details)
            return False, reason
        
        try:
            logger.trace(f"Processing crime: CRIME_ID={crime_id}, FIR_NUM={crime.get('fir_num')}, PS_CODE={crime.get('ps_code')}")
            
            # Check if PS_CODE exists in hierarchy
            if crime.get('ps_code'):
                self.db_cursor.execute(f"SELECT 1 FROM {HIERARCHY_TABLE} WHERE ps_code = %s", (crime['ps_code'],))
                if not self.db_cursor.fetchone():
                    reason = 'ps_code_not_found'
                    error_details = f"PS_CODE {crime['ps_code']} not found in hierarchy table"
                    logger.warning(f"⚠️  {error_details}, skipping crime {crime_id}")
                    self.stats['total_crimes_failed'] += 1
                    self.log_failed_record(crime, reason, error_details)
                    return False, reason
            else:
                reason = 'missing_ps_code'
                error_details = "Crime record missing PS_CODE"
                logger.warning(f"⚠️  {error_details}, skipping crime {crime_id}")
                self.stats['total_crimes_failed'] += 1
                self.log_failed_record(crime, reason, error_details)
                return False, reason
            
            # Check if crime already exists
            if self.crime_exists(crime_id):
                # Get existing record to compare
                existing = self.get_existing_crime(crime_id)
                if not existing:
                    logger.warning(f"⚠️  CRIME_ID {crime_id} exists check returned True but fetch returned None")
                    # Fall back to insert
                    existing = None
                
                if existing:
                    # Smart update: only update fields that need updating
                    # Rules:
                    # 1. If existing is NULL and new is not NULL → update
                    # 2. If existing is not NULL and new is NULL → keep existing (don't update to NULL)
                    # 3. If both are not NULL and different → update
                    # 4. If both are not NULL and same → skip update (no change needed)
                    # Special: date_created and date_modified always from API (even if NULL)
                    
                    update_fields = []
                    update_values = []
                    changes = []
                    
                    # Define all fields to check (excluding crime_id which is the key)
                    fields_to_check = [
                        ('ps_code', 'PS_CODE'),
                        ('fir_num', 'FIR_NUM'),
                        ('fir_reg_num', 'FIR_REG_NUM'),
                        ('fir_type', 'FIR_TYPE'),
                        ('acts_sections', 'ACTS_SECTIONS'),
                        ('fir_date', 'FIR_DATE'),
                        ('case_status', 'CASE_STATUS'),
                        ('major_head', 'MAJOR_HEAD'),
                        ('minor_head', 'MINOR_HEAD'),
                        ('crime_type', 'CRIME_TYPE'),
                        ('io_name', 'IO_NAME'),
                        ('io_rank', 'IO_RANK'),
                        ('brief_facts', 'BRIEF_FACTS'),
                        ('fir_copy', 'FIR_COPY'),  # FIR copy identifier
                        ('date_created', 'DATE_CREATED'),  # Always from API
                        ('date_modified', 'DATE_MODIFIED')  # Always from API
                    ]
                    
                    for db_field, api_field in fields_to_check:
                        existing_val = existing.get(db_field)
                        new_val = crime.get(db_field)
                        
                        # Special handling for date fields - always use API value
                        if db_field in ('date_created', 'date_modified'):
                            # Always update date fields from API (even if NULL)
                            if existing_val != new_val:
                                update_fields.append(f"{db_field} = %s")
                                update_values.append(new_val)
                                changes.append(f"{db_field}: {existing_val} → {new_val}")
                                logger.trace(f"  Will update {db_field}: {existing_val} → {new_val} (API date)")
                        else:
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
                            UPDATE {CRIMES_TABLE} SET
                                {', '.join(update_fields)}
                            WHERE crime_id = %s
                        """
                        update_values.append(crime_id)
                        self.db_cursor.execute(update_query, tuple(update_values))
                        self.stats['total_crimes_updated'] += 1
                        logger.debug(f"Updated crime: {crime_id} ({len(changes)} fields changed)")
                        logger.trace(f"Changes: {', '.join(changes)}")
                        self.db_conn.commit()
                        logger.trace(f"Transaction committed for updated CRIME_ID: {crime_id}")
                        return True, 'updated'
                    else:
                        # No changes needed
                        self.stats['total_crimes_no_change'] += 1
                        logger.trace(f"No changes needed for CRIME_ID: {crime_id} (all fields match or preserved)")
                        return True, 'no_change'
                else:
                    # Exists check returned True but couldn't fetch - treat as new insert
                    logger.warning(f"⚠️  CRIME_ID {crime_id} exists but couldn't fetch, treating as new insert")
                    # Fall through to insert logic
            else:
                # Insert new crime
                logger.trace(f"Inserting new crime: {crime_id}")
                insert_query = f"""
                    INSERT INTO {CRIMES_TABLE} (
                        crime_id, ps_code, fir_num, fir_reg_num, fir_type,
                        acts_sections, fir_date, case_status, major_head, minor_head,
                        crime_type, io_name, io_rank, brief_facts, fir_copy,
                        date_created, date_modified
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """
                self.db_cursor.execute(insert_query, (
                    crime['crime_id'],
                    crime['ps_code'],
                    crime['fir_num'],
                    crime['fir_reg_num'],
                    crime['fir_type'],
                    crime['acts_sections'],
                    crime['fir_date'],
                    crime['case_status'],
                    crime['major_head'],
                    crime['minor_head'],
                    crime['crime_type'],
                    crime['io_name'],
                    crime['io_rank'],
                    crime['brief_facts'],
                    crime['fir_copy'],  # From API (or NULL)
                    crime['date_created'],  # From API (or NULL)
                    crime['date_modified']  # From API (or NULL)
                ))
                self.stats['total_crimes_inserted'] += 1
                logger.debug(f"Inserted crime: {crime_id}")
                logger.trace(f"Insert query executed for CRIME_ID: {crime_id}")
                self.db_conn.commit()
                logger.trace(f"Transaction committed for inserted CRIME_ID: {crime_id}")
                return True, 'inserted'
            
        except psycopg2.IntegrityError as e:
            self.db_conn.rollback()
            reason = 'integrity_error'
            error_details = str(e)
            logger.warning(f"⚠️  Integrity error for crime {crime_id}: {e}")
            self.stats['total_crimes_failed'] += 1
            self.log_failed_record(crime, reason, error_details)
            return False, reason
        except Exception as e:
            self.db_conn.rollback()
            reason = 'error'
            error_details = str(e)
            logger.error(f"❌ Error inserting crime {crime_id}: {e}")
            self.stats['total_crimes_failed'] += 1
            self.stats['errors'].append(f"Crime {crime_id}: {str(e)}")
            self.log_failed_record(crime, reason, error_details)
            return False, reason
    
    def process_date_range(self, from_date: str, to_date: str, table_columns: Set[str] = None):
        """Process crimes for a specific date range"""
        chunk_range = f"{from_date} to {to_date}"
        logger.info(f"📅 Processing: {chunk_range}")
        
        # Fetch crimes from API
        crimes_raw = self.fetch_crimes_api(from_date, to_date)
        
        if crimes_raw is None:
            logger.error(f"❌ Failed to fetch crimes for {chunk_range}")
            self.log_db_chunk(from_date, to_date, 0, [], [], [], [], [], error="API fetch failed")
            return
        
        if not crimes_raw:
            logger.info(f"ℹ️  No crimes found for {chunk_range}")
            self.log_db_chunk(from_date, to_date, 0, [], [], [], [], [], error="No crimes in API response")
            return
        
        # Check for schema evolution if we got data
        if table_columns is not None and len(crimes_raw) > 0:
            # Check for new fields in first record
            new_fields = self.detect_new_fields(crimes_raw[0], table_columns)
            if new_fields:
                logger.info(f"🔍 New fields detected in API response: {list(new_fields.keys())}")
                # Add new columns to table
                for api_field, db_column in new_fields.items():
                    if self.add_column_to_table(db_column):
                        # Update table_columns set
                        table_columns.add(db_column)
                # Update existing records from start_date to current chunk end_date
                self.update_existing_records_with_new_fields(new_fields, to_date)
        
        # Transform and insert each crime
        self.stats['total_crimes_fetched'] += len(crimes_raw)
        logger.trace(f"Processing {len(crimes_raw)} crimes for chunk {chunk_range}")
        
        # Track operations for this chunk
        inserted_ids = []
        updated_ids = []
        no_change_ids = []  # Records that exist but no changes needed
        failed_ids = []  # Records that failed to insert/update
        failed_reasons = {}
        duplicates_in_chunk = []
        ps_code_failures_in_chunk = []  # Track PS_CODE failures for this chunk
        
        # Track crime_ids seen in this chunk to detect duplicates (for reporting only, not skipping)
        seen_crime_ids = {}
        crime_id_occurrences = {}  # Track how many times each crime_id appears
        
        logger.trace(f"Starting to process records for chunk: {chunk_range}")
        for idx, crime_raw in enumerate(crimes_raw, 1):
            logger.trace(f"Processing record {idx}/{len(crimes_raw)}: {crime_raw.get('CRIME_ID')}")
            crime = self.transform_crime(crime_raw)
            crime_id = crime.get('crime_id')
            
            if not crime_id:
                logger.warning(f"⚠️  Crime missing CRIME_ID, skipping")
                self.stats['total_crimes_failed'] += 1
                failed_ids.append(None)
                reason = 'missing_crime_id'
                if reason not in failed_reasons:
                    failed_reasons[reason] = []
                failed_reasons[reason].append(None)
                continue
            
            # Track occurrences for duplicate reporting (but don't skip - process all)
            if crime_id in seen_crime_ids:
                # This is a duplicate occurrence - track it but still process
                occurrence_count = crime_id_occurrences.get(crime_id, 1) + 1
                crime_id_occurrences[crime_id] = occurrence_count
                
                duplicates_in_chunk.append({
                    'crime_id': crime_id,
                    'fir_num': crime.get('fir_num'),
                    'ps_code': crime.get('ps_code'),
                    'occurrence': occurrence_count,
                    'first_seen_in': seen_crime_ids[crime_id],
                    'duplicate_in': chunk_range
                })
                self.stats['total_duplicates'] += 1
                logger.info(f"⚠️  Duplicate CRIME_ID {crime_id} found in chunk {chunk_range} (occurrence #{occurrence_count}) - Will process to update record")
                logger.trace(f"Duplicate details - First seen: {seen_crime_ids[crime_id]}, Current occurrence: {occurrence_count}")
            else:
                seen_crime_ids[crime_id] = chunk_range
                crime_id_occurrences[crime_id] = 1
                logger.trace(f"New CRIME_ID seen: {crime_id} in chunk {chunk_range}")
            
            # IMPORTANT: Process ALL records, even duplicates
            # If same crime_id appears multiple times, each occurrence might have updated data
            # The smart update logic will handle whether to actually update or not
            success, operation = self.insert_crime(crime, chunk_range)
            logger.trace(f"Operation result for CRIME_ID {crime_id}: success={success}, operation={operation}")
            if success:
                if operation == 'inserted':
                    # Only add to list if first occurrence (to avoid duplicate entries in log)
                    if crime_id not in inserted_ids:
                        inserted_ids.append(crime_id)
                    logger.trace(f"Added to inserted list: {crime_id}")
                elif operation == 'updated':
                    # Track all updates (even if same crime_id updated multiple times)
                    updated_ids.append(crime_id)
                    logger.trace(f"Added to updated list: {crime_id} (occurrence #{crime_id_occurrences.get(crime_id, 1)})")
                elif operation == 'no_change':
                    # Only add to list if first occurrence
                    if crime_id not in no_change_ids:
                        no_change_ids.append(crime_id)
                    logger.trace(f"Added to no_change list: {crime_id}")
            else:
                failed_ids.append(crime_id)
                if operation not in failed_reasons:
                    failed_reasons[operation] = []
                failed_reasons[operation].append(crime_id)
                logger.trace(f"Added to failed list: {crime_id}, reason: {operation}")
                
                # Track PS_CODE failures separately
                if operation == 'ps_code_not_found':
                    ps_code_failures_in_chunk.append({
                        'crime_id': crime_id,
                        'ps_code': crime.get('ps_code'),
                        'fir_num': crime.get('fir_num')
                    })
        
        # Log duplicates for this chunk (for reporting, but they were all processed)
        if duplicates_in_chunk:
            logger.info(f"📊 Found {len(duplicates_in_chunk)} duplicate occurrences in chunk {chunk_range} - All were processed for potential updates")
            logger.trace(f"Duplicate details: {duplicates_in_chunk}")
            self.log_duplicates_chunk(from_date, to_date, duplicates_in_chunk)
        
        # Log PS_CODE failures for this chunk
        if ps_code_failures_in_chunk:
            logger.warning(f"⚠️  Found {len(ps_code_failures_in_chunk)} crimes with missing PS_CODEs in chunk {chunk_range}")
            # Extract unique PS_CODEs
            unique_ps_codes = list(set([f['ps_code'] for f in ps_code_failures_in_chunk if f.get('ps_code')]))
            logger.warning(f"   Missing PS_CODEs: {unique_ps_codes}")
        
        # Log database operations for this chunk
        logger.trace(f"Chunk summary - Inserted: {len(inserted_ids)}, Updated: {len(updated_ids)}, No Change: {len(no_change_ids)}, Failed: {len(failed_ids)}, Duplicates: {len(duplicates_in_chunk)}, PS_CODE Failures: {len(ps_code_failures_in_chunk)}")
        self.log_db_chunk(from_date, to_date, len(crimes_raw), inserted_ids, updated_ids, 
                         no_change_ids, failed_ids, failed_reasons)
        
        logger.info(f"✅ Completed: {chunk_range} - Inserted: {len(inserted_ids)}, Updated: {len(updated_ids)}, No Change: {len(no_change_ids)}, Failed: {len(failed_ids)}, Duplicates: {len(duplicates_in_chunk)}, PS_CODE Failures: {len(ps_code_failures_in_chunk)}")
        logger.trace(f"Chunk processing complete for {chunk_range}")
    
    def log_db_chunk(self, from_date: str, to_date: str, total_fetched: int,
                    inserted_ids: List[str], updated_ids: List[str], no_change_ids: List[str],
                    failed_ids: List[str], failed_reasons: Dict, error: Optional[str] = None):
        """Log database operations for a chunk"""
        chunk_info = {
            'chunk': f"{from_date} to {to_date}",
            'timestamp': datetime.now().isoformat(),
            'total_fetched': total_fetched,
            'inserted_count': len(inserted_ids),
            'inserted_ids': inserted_ids,
            'updated_count': len(updated_ids),
            'updated_ids': updated_ids,
            'no_change_count': len(no_change_ids),
            'no_change_ids': no_change_ids,
            'failed_count': len(failed_ids),
            'failed_ids': failed_ids,
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
            self.db_log.write(f"\nINSERTED: {len(inserted_ids)}\n")
            for i, crime_id in enumerate(inserted_ids, 1):
                self.db_log.write(f"  {i}. {crime_id}\n")
            
            self.db_log.write(f"\nUPDATED: {len(updated_ids)}\n")
            for i, crime_id in enumerate(updated_ids, 1):
                self.db_log.write(f"  {i}. {crime_id}\n")
            
            self.db_log.write(f"\nNO CHANGE: {len(no_change_ids)}\n")
            for i, crime_id in enumerate(no_change_ids, 1):
                self.db_log.write(f"  {i}. {crime_id}\n")
            
            self.db_log.write(f"\nFAILED: {len(failed_ids)}\n")
            if failed_reasons:
                for reason, ids in failed_reasons.items():
                    self.db_log.write(f"  Reason: {reason} ({len(ids)})\n")
                    for i, crime_id in enumerate(ids[:20], 1):  # Show first 20
                        self.db_log.write(f"    {i}. {crime_id}\n")
                    if len(ids) > 20:
                        self.db_log.write(f"    ... and {len(ids) - 20} more\n")
            
            # Also write JSON format for easy parsing
            self.db_log.write(f"\nJSON Format:\n")
            self.db_log.write(json.dumps(chunk_info, indent=2, ensure_ascii=False))
            self.db_log.write(f"\n")
        
        self.db_log.flush()
    
    def write_log_summaries(self):
        """Write summary sections to both log files"""
        # API log summary
        self.api_log.write(f"\n\n{'='*80}\n")
        self.api_log.write(f"SUMMARY\n")
        self.api_log.write(f"{'='*80}\n")
        self.api_log.write(f"Total API Calls: {self.stats['total_api_calls']}\n")
        self.api_log.write(f"Total Crimes Fetched: {self.stats['total_crimes_fetched']}\n")
        self.api_log.write(f"Failed API Calls: {self.stats['failed_api_calls']}\n")
        self.api_log.write(f"Total Chunks Processed: {self.stats['total_api_calls'] + self.stats['failed_api_calls']}\n")
        
        # DB log summary
        self.db_log.write(f"\n\n{'='*80}\n")
        self.db_log.write(f"SUMMARY\n")
        self.db_log.write(f"{'='*80}\n")
        self.db_log.write(f"Total Crimes Fetched from API: {self.stats['total_crimes_fetched']}\n")
        self.db_log.write(f"Total Crimes Inserted (New): {self.stats['total_crimes_inserted']}\n")
        self.db_log.write(f"Total Crimes Updated (Existing): {self.stats['total_crimes_updated']}\n")
        self.db_log.write(f"Total Crimes No Change: {self.stats['total_crimes_no_change']}\n")
        self.db_log.write(f"Total Crimes Failed: {self.stats['total_crimes_failed']}\n")
        self.db_log.write(f"  - Failed due to Missing PS_CODE: {self.stats['total_crimes_failed_ps_code']}\n")
        self.db_log.write(f"Total Crimes Duplicates (Processed): {self.stats['total_duplicates']}\n")
        self.db_log.write(f"Total Operations (Inserted + Updated + No Change): {self.stats['total_crimes_inserted'] + self.stats['total_crimes_updated'] + self.stats['total_crimes_no_change']}\n")
        db_total = self.stats.get('db_total_count', self.stats['total_crimes_inserted'])
        self.db_log.write(f"Total Unique Crimes in Database: {db_total}\n")
        self.db_log.write(f"Note: Updated count includes multiple updates (same crime_id in multiple chunks or same chunk)\n")
        self.db_log.write(f"Note: Duplicates are CRIME_IDs that appear multiple times within the same chunk - ALL are processed for updates\n")
        if self.stats['total_crimes_fetched'] > 0:
            coverage = ((self.stats['total_crimes_inserted'] + self.stats['total_crimes_updated'] + self.stats['total_crimes_no_change']) / self.stats['total_crimes_fetched']) * 100
            self.db_log.write(f"Coverage: {coverage:.2f}%\n")
        self.db_log.write(f"Errors: {len(self.stats['errors'])}\n")
        
        # Failed records log summary
        self.failed_log.write(f"\n\n{'='*80}\n")
        self.failed_log.write(f"SUMMARY\n")
        self.failed_log.write(f"{'='*80}\n")
        self.failed_log.write(f"Total Failed Records: {self.stats['total_crimes_failed']}\n")
        self.failed_log.write(f"Note: Failed records are those that could not be inserted or updated\n")
        self.failed_log.write(f"Check individual entries above for specific reasons\n")
        
        # Duplicates log summary
        self.duplicates_log.write(f"\n\n{'='*80}\n")
        self.duplicates_log.write(f"SUMMARY\n")
        self.duplicates_log.write(f"{'='*80}\n")
        self.duplicates_log.write(f"Total Duplicate Occurrences Found: {self.stats['total_duplicates']}\n")
        self.duplicates_log.write(f"Note: Duplicates are CRIME_IDs that appear multiple times within the same chunk\n")
        self.duplicates_log.write(f"IMPORTANT: All duplicates are PROCESSED (not skipped) to allow updates\n")
        self.duplicates_log.write(f"If the same crime_id appears multiple times, each occurrence is processed\n")
        self.duplicates_log.write(f"The smart update logic will determine if actual updates are needed\n")
        
        # PS_CODE failures log summary
        self.ps_code_failures_log.write(f"\n\n{'='*80}\n")
        self.ps_code_failures_log.write(f"SUMMARY\n")
        self.ps_code_failures_log.write(f"{'='*80}\n")
        self.ps_code_failures_log.write(f"Total Crimes Failed Due to Missing PS_CODE: {self.stats['total_crimes_failed_ps_code']}\n")
        self.ps_code_failures_log.write(f"\n")
        self.ps_code_failures_log.write(f"Note: These crimes could not be inserted/updated because their PS_CODE\n")
        self.ps_code_failures_log.write(f"      was not found in the hierarchy table. Please ensure these PS_CODEs\n")
        self.ps_code_failures_log.write(f"      are loaded in the hierarchy table first.\n")
    
    def run(self):
        """Main ETL execution"""
        logger.info("=" * 80)
        logger.info("🚀 DOPAMAS ETL Pipeline - Crimes API")
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
            table_columns = self.get_table_columns(CRIMES_TABLE)
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
            self.db_cursor.execute(f"SELECT COUNT(*) FROM {CRIMES_TABLE}")
            db_crimes_count = self.db_cursor.fetchone()[0]
            
            # Store for summary
            self.stats['db_total_count'] = db_crimes_count
            
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
            logger.info(f"  Total Crimes Fetched: {self.stats['total_crimes_fetched']}")
            logger.info(f"")
            logger.info(f"💾 TO DATABASE:")
            logger.info(f"  Total Inserted (New): {self.stats['total_crimes_inserted']}")
            logger.info(f"  Total Updated:        {self.stats['total_crimes_updated']}")
            logger.info(f"  Total No Change:      {self.stats['total_crimes_no_change']}")
            logger.info(f"  Total Failed:         {self.stats['total_crimes_failed']}")
            logger.info(f"    - Missing PS_CODE:   {self.stats['total_crimes_failed_ps_code']}")
            logger.info(f"  Total in DB:          {db_crimes_count}")
            logger.info(f"")
            logger.info(f"🔄 DUPLICATES:")
            logger.info(f"  Total Duplicate Occurrences (Processed): {self.stats['total_duplicates']}")
            logger.info(f"  Note: All duplicates are processed to allow updates")
            logger.info(f"")
            logger.info(f"⚠️  PS_CODE FAILURES:")
            logger.info(f"  Crimes Failed Due to Missing PS_CODE: {self.stats['total_crimes_failed_ps_code']}")
            logger.info(f"  Check logs/crimes_ps_code_failures_*.log for details")
            logger.info(f"")
            logger.info(f"📊 COVERAGE:")
            if self.stats['total_crimes_fetched'] > 0:
                coverage = ((self.stats['total_crimes_inserted'] + self.stats['total_crimes_updated'] + self.stats['total_crimes_no_change']) / self.stats['total_crimes_fetched']) * 100
                logger.info(f"  API → DB Coverage:   {coverage:.2f}%")
            logger.info(f"")
            logger.info(f"📈 SUMMARY:")
            logger.info(f"  Total from API:       {self.stats['total_crimes_fetched']}")
            logger.info(f"  Inserted + Updated:   {self.stats['total_crimes_inserted'] + self.stats['total_crimes_updated']}")
            logger.info(f"  Duplicate Occurrences: {self.stats['total_duplicates']} (all processed)")
            logger.info(f"  Failed:               {self.stats['total_crimes_failed']}")
            logger.info(f"")
            logger.info(f"💡 NOTE:")
            logger.info(f"  - Same crime_id can appear multiple times in API response")
            logger.info(f"  - Each occurrence is processed to capture any data updates")
            logger.info(f"  - Smart update logic ensures only changed fields are updated")
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
            logger.info(f"📝 Duplicates log saved to: {self.duplicates_log_file}")
            logger.info(f"📝 PS_CODE failures log saved to: {self.ps_code_failures_log_file}")
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
    etl = CrimesETL()
    success = etl.run()
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()


