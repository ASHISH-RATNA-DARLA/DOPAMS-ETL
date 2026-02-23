#!/usr/bin/env python3
"""
DOPAMAS ETL Pipeline - Arrests API
Fetches arrests data in 5-day chunks and loads into PostgreSQL
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
ARRESTS_TABLE = TABLE_CONFIG.get('arrests', 'arrests')
CRIMES_TABLE = TABLE_CONFIG.get('crimes', 'crimes')
PERSONS_TABLE = TABLE_CONFIG.get('persons', 'persons')
ACCUSED_TABLE = TABLE_CONFIG.get('accused', 'accused')

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


class ArrestsETL:
    """ETL Pipeline for Arrests API"""
    
    def __init__(self):
        self.db_conn = None
        self.db_cursor = None
        self.stats = {
            'total_api_calls': 0,
            'total_arrests_fetched': 0,
            'total_arrests_inserted': 0,
            'total_arrests_updated': 0,
            'total_arrests_no_change': 0,  # Records that exist but no changes needed
            'total_arrests_failed': 0,  # Records that failed to insert/update
            'total_arrests_failed_crime_id': 0,  # Arrests failed due to CRIME_ID not found
            'total_arrests_failed_person_id': 0,  # Arrests failed due to PERSON_ID not found
            'total_duplicates': 0,  # Duplicate records found within chunks
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
        self.api_log_file = f'logs/arrests_api_chunks_{timestamp}.log'
        self.api_log = open(self.api_log_file, 'w', encoding='utf-8')
        self.api_log.write(f"# Arrests API Chunk-wise Log\n")
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
        self.db_log_file = f'logs/arrests_db_chunks_{timestamp}.log'
        self.db_log = open(self.db_log_file, 'w', encoding='utf-8')
        self.db_log.write(f"# Arrests Database Operations Chunk-wise Log\n")
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
        self.failed_log_file = f'logs/arrests_failed_{timestamp}.log'
        self.failed_log = open(self.failed_log_file, 'w', encoding='utf-8')
        self.failed_log.write(f"# Arrests Failed Records Log\n")
        self.failed_log.write(f"# Generated: {datetime.now().isoformat()}\n")
        self.failed_log.write(f"# Records that failed to insert or update with reasons\n")
        self.failed_log.write(f"{'='*80}\n\n")
        
        # Invalid IDs log file (arrests with invalid crime_id - these are skipped)
        self.invalid_ids_log_file = f'logs/arrests_invalid_ids_{timestamp}.log'
        self.invalid_ids_log = open(self.invalid_ids_log_file, 'w', encoding='utf-8')
        self.invalid_ids_log.write(f"# Arrests Invalid IDs Log (CRIME_ID only - these records are SKIPPED)\n")
        self.invalid_ids_log.write(f"# Generated: {datetime.now().isoformat()}\n")
        self.invalid_ids_log.write(f"# Arrests that failed because CRIME_ID not found in crimes table\n")
        self.invalid_ids_log.write(f"# These records are SKIPPED and NOT inserted/updated\n")
        self.invalid_ids_log.write(f"{'='*80}\n\n")
        
        # Invalid person_id log file (person_id not found but record is still processed with NULL)
        self.invalid_person_id_log_file = f'logs/arrests_invalid_person_id_{timestamp}.log'
        self.invalid_person_id_log = open(self.invalid_person_id_log_file, 'w', encoding='utf-8')
        self.invalid_person_id_log.write(f"# Arrests Invalid PERSON_ID Log\n")
        self.invalid_person_id_log.write(f"# Generated: {datetime.now().isoformat()}\n")
        self.invalid_person_id_log.write(f"# Arrests where PERSON_ID from API was not found in persons table\n")
        self.invalid_person_id_log.write(f"# These records are PROCESSED with person_id = NULL\n")
        self.invalid_person_id_log.write(f"# The record is still inserted/updated, but person_id is set to NULL\n")
        self.invalid_person_id_log.write(f"{'='*80}\n\n")
        
        # Duplicates log file (duplicate records found within chunks)
        self.duplicates_log_file = f'logs/arrests_duplicates_{timestamp}.log'
        self.duplicates_log = open(self.duplicates_log_file, 'w', encoding='utf-8')
        self.duplicates_log.write(f"# Arrests Duplicates Log\n")
        self.duplicates_log.write(f"# Generated: {datetime.now().isoformat()}\n")
        self.duplicates_log.write(f"# Duplicate records found within the same chunk\n")
        self.duplicates_log.write(f"{'='*80}\n\n")
        
        logger.info(f"📝 API chunk log: {self.api_log_file}")
        logger.info(f"📝 DB chunk log: {self.db_log_file}")
        logger.info(f"📝 Failed records log: {self.failed_log_file}")
        logger.info(f"📝 Invalid IDs log (CRIME_ID only - skipped): {self.invalid_ids_log_file}")
        logger.info(f"📝 Invalid PERSON_ID log (processed with NULL): {self.invalid_person_id_log_file}")
        logger.info(f"📝 Duplicates log: {self.duplicates_log_file}")
    
    def close_chunk_loggers(self):
        """Close chunk log files"""
        if hasattr(self, 'api_log') and self.api_log:
            self.api_log.close()
        if hasattr(self, 'db_log') and self.db_log:
            self.db_log.close()
        if hasattr(self, 'failed_log') and self.failed_log:
            self.failed_log.close()
        if hasattr(self, 'invalid_ids_log') and self.invalid_ids_log:
            self.invalid_ids_log.close()
        if hasattr(self, 'invalid_person_id_log') and self.invalid_person_id_log:
            self.invalid_person_id_log.close()
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
        """
        try:
            # Check if table has any data
            self.db_cursor.execute(f"SELECT COUNT(*) FROM {ARRESTS_TABLE}")
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
                FROM {ARRESTS_TABLE}
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
            'PERSON_ID': 'person_id',
            'ACCUSED_SEQ_NO': 'accused_seq_no',
            'ACCUSED_CODE': 'accused_code',
            'ACCUSED_TYPE': 'accused_type',
            'IS_ARRESTED': 'is_arrested',
            'ARRESTED_DATE': 'arrested_date',
            'IS_41A_CRPC': 'is_41a_crpc',
            'IS_41A_EXPLAIN_SUBMITTED': 'is_41a_explain_submitted',
            'DATE_OF_ISSUE_41A': 'date_of_issue_41a',
            'IS_CCL': 'is_ccl',
            'IS_APPREHENDED': 'is_apprehended',
            'IS_ABSCONDING': 'is_absconding',
            'IS_DIED': 'is_died',
            'DATE_CREATED': 'date_created',
            'DATE_MODIFIED': 'date_modified'
        }
        
        for api_field, db_column in field_mapping.items():
            if api_field in api_record and db_column not in table_columns:
                new_fields[api_field] = db_column
        
        return new_fields
    
    def add_column_to_table(self, column_name: str, column_type: str = 'TEXT'):
        """Add a new column to the arrests table."""
        try:
            # Determine column type based on field name
            if 'date' in column_name.lower() or 'at' in column_name.lower():
                if column_name == 'date_of_issue_41a':
                    column_type = 'DATE'
                else:
                    column_type = 'TIMESTAMPTZ'
            elif column_name in ('crime_id', 'person_id'):
                column_type = 'VARCHAR(50)'  # Matches foreign key types
            elif 'id' in column_name.lower():
                column_type = 'VARCHAR(50)'  # Most IDs are VARCHAR in this schema
            elif column_name.startswith('is_'):
                column_type = 'BOOLEAN'
            elif column_name in ('accused_seq_no', 'accused_code', 'accused_type'):
                column_type = 'TEXT'
            else:
                column_type = 'TEXT'
            
            alter_sql = f"ALTER TABLE {ARRESTS_TABLE} ADD COLUMN IF NOT EXISTS {column_name} {column_type}"
            self.db_cursor.execute(alter_sql)
            self.db_conn.commit()
            logger.info(f"✅ Added column {column_name} ({column_type}) to {ARRESTS_TABLE}")
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
    
    def fetch_arrests_api(self, from_date: str, to_date: str) -> Optional[List[Dict]]:
        """
        Fetch arrests data from API for given date range
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
        
        Returns:
            List of arrests records or None if failed
        """
        # Use arrests_url from config (which reads from .env)
        url = API_CONFIG.get('arrests_url', f"{API_CONFIG['base_url']}/arrests")
        params = {
            'fromDate': from_date,
            'toDate': to_date
        }
        headers = {
            'x-api-key': API_CONFIG['api_key']
        }
        
        for attempt in range(API_CONFIG['max_retries']):
            try:
                logger.debug(f"Fetching arrests: {from_date} to {to_date} (Attempt {attempt + 1})")
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
                        arrests_data = data.get('data')
                        if arrests_data:
                            # If single object, convert to list
                            if isinstance(arrests_data, dict):
                                arrests_data = [arrests_data]
                            
                            # Extract crime_ids for logging
                            crime_ids = [d.get('CRIME_ID') for d in arrests_data if d.get('CRIME_ID')]
                            
                            # Log to API chunk file
                            self.log_api_chunk(from_date, to_date, len(arrests_data), crime_ids, arrests_data)
                            
                            logger.info(f"✅ Fetched {len(arrests_data)} arrests records for {from_date} to {to_date}")
                            logger.debug(f"📋 Crime IDs from API: {crime_ids[:10]}{'...' if len(crime_ids) > 10 else ''}")
                            logger.trace(f"Full Crime IDs list: {crime_ids}")
                            logger.trace(f"Sample arrests structure: {json.dumps(arrests_data[0] if arrests_data else {}, indent=2, default=str)}")
                            return arrests_data
                        else:
                            # Log empty response
                            self.log_api_chunk(from_date, to_date, 0, [], [])
                            logger.warning(f"⚠️  No arrests records found for {from_date} to {to_date}")
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
        
        logger.error(f"❌ Failed to fetch arrests for {from_date} to {to_date} after {API_CONFIG['max_retries']} attempts")
        self.log_api_chunk(from_date, to_date, 0, [], [], error="Failed after max retries")
        return None
    
    def log_api_chunk(self, from_date: str, to_date: str, count: int, crime_ids: List[str], 
                     arrests_data: List[Dict], error: Optional[str] = None):
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
            Normalized value (None if empty string, otherwise original value)
        """
        if value == "" or value is None:
            return None
        return value
    
    def transform_arrests(self, arrests_raw: Dict) -> Dict:
        """
        Transform API response to database format
        Dates are always taken from API (never use CURRENT_TIMESTAMP)
        Validates crime_id and person_id exist in respective tables
        
        Args:
            arrests_raw: Raw arrests data from API
        
        Returns:
            Transformed arrests dict ready for database
        """
        logger.trace(f"Transforming arrests: CRIME_ID={arrests_raw.get('CRIME_ID')}, PERSON_ID={arrests_raw.get('PERSON_ID')}")
        
        # Validate crime_id (required)
        crime_id_str = arrests_raw.get('CRIME_ID')
        crime_id_valid = None
        
        if crime_id_str:
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
                self.db_conn.rollback()
        
        # Validate person_id (optional - only if provided)
        person_id_str = arrests_raw.get('PERSON_ID')
        person_id_valid = None
        
        if person_id_str:
            try:
                self.db_cursor.execute(f"SELECT person_id FROM {PERSONS_TABLE} WHERE person_id = %s", (person_id_str,))
                result = self.db_cursor.fetchone()
                if result:
                    person_id_valid = person_id_str
                    logger.trace(f"PERSON_ID {person_id_str} found in persons table")
                else:
                    logger.trace(f"PERSON_ID {person_id_str} not found in persons table")
            except Exception as e:
                logger.error(f"Error validating person_id {person_id_str}: {e}")
                self.db_conn.rollback()
        
        # Normalize date/timestamp fields (convert empty strings to None)
        transformed = {
            'crime_id': crime_id_valid,  # VARCHAR foreign key to crimes.crime_id (required)
            'person_id': person_id_valid,  # VARCHAR foreign key to persons.person_id (optional)
            'accused_seq_no': arrests_raw.get('ACCUSED_SEQ_NO'),
            'accused_code': arrests_raw.get('ACCUSED_CODE'),
            'accused_type': arrests_raw.get('ACCUSED_TYPE'),
            'is_arrested': arrests_raw.get('IS_ARRESTED'),
            'arrested_date': self.normalize_date_value(arrests_raw.get('ARRESTED_DATE')),  # TIMESTAMPTZ
            'is_41a_crpc': arrests_raw.get('IS_41A_CRPC'),
            'is_41a_explain_submitted': arrests_raw.get('IS_41A_EXPLAIN_SUBMITTED'),
            'date_of_issue_41a': self.normalize_date_value(arrests_raw.get('DATE_OF_ISSUE_41A')),  # DATE
            'is_ccl': arrests_raw.get('IS_CCL'),
            'is_apprehended': arrests_raw.get('IS_APPREHENDED'),
            'is_absconding': arrests_raw.get('IS_ABSCONDING'),
            'is_died': arrests_raw.get('IS_DIED'),
            # Dates are always from API (never use CURRENT_TIMESTAMP)
            # Normalize to convert empty strings to None
            'date_created': self.normalize_date_value(arrests_raw.get('DATE_CREATED')),  # TIMESTAMPTZ
            'date_modified': self.normalize_date_value(arrests_raw.get('DATE_MODIFIED')),  # TIMESTAMPTZ
            # Store original IDs for validation logging
            '_original_crime_id': crime_id_str,
            '_original_person_id': person_id_str
        }
        logger.trace(f"Transformed arrests: {json.dumps({k: v for k, v in transformed.items() if not k.startswith('_original')}, indent=2, default=str)}")
        return transformed
    
    def arrests_exists(self, crime_id: str, accused_seq_no: str) -> bool:
        """Check if arrests record already exists in database (based on unique constraint)"""
        logger.trace(f"Checking if arrests exists: crime_id={crime_id}, accused_seq_no={accused_seq_no}")
        query = f"""
            SELECT 1 FROM {ARRESTS_TABLE} 
            WHERE crime_id = %s AND accused_seq_no = %s
        """
        self.db_cursor.execute(query, (crime_id, accused_seq_no))
        exists = self.db_cursor.fetchone() is not None
        logger.trace(f"Arrests exists: {exists}")
        return exists
    
    def get_existing_arrests(self, crime_id: str, accused_seq_no: str) -> Optional[Dict]:
        """Get existing arrests record from database"""
        query = f"""
            SELECT crime_id, person_id, accused_seq_no, accused_code, accused_type,
                   is_arrested, arrested_date, is_41a_crpc, is_41a_explain_submitted,
                   date_of_issue_41a, is_ccl, is_apprehended, is_absconding, is_died,
                   date_created, date_modified
            FROM {ARRESTS_TABLE}
            WHERE crime_id = %s AND accused_seq_no = %s
        """
        self.db_cursor.execute(query, (crime_id, accused_seq_no))
        row = self.db_cursor.fetchone()
        if row:
            return {
                'crime_id': row[0],
                'person_id': row[1],
                'accused_seq_no': row[2],
                'accused_code': row[3],
                'accused_type': row[4],
                'is_arrested': row[5],
                'arrested_date': row[6],
                'is_41a_crpc': row[7],
                'is_41a_explain_submitted': row[8],
                'date_of_issue_41a': row[9],
                'is_ccl': row[10],
                'is_apprehended': row[11],
                'is_absconding': row[12],
                'is_died': row[13],
                'date_created': row[14],
                'date_modified': row[15]
            }
        return None
    
    def log_failed_record(self, arrests: Dict, reason: str, error_details: str = ""):
        """Log a failed record to the failed records log file"""
        failed_info = {
            'crime_id': arrests.get('crime_id'),
            'person_id': arrests.get('person_id'),
            'accused_seq_no': arrests.get('accused_seq_no'),
            'reason': reason,
            'error_details': error_details,
            'timestamp': datetime.now().isoformat(),
            'arrests_data': arrests
        }
        
        self.failed_log.write(f"\n{'='*80}\n")
        self.failed_log.write(f"CRIME_ID: {arrests.get('crime_id')}\n")
        self.failed_log.write(f"PERSON_ID: {arrests.get('person_id')}\n")
        self.failed_log.write(f"ACCUSED_SEQ_NO: {arrests.get('accused_seq_no')}\n")
        self.failed_log.write(f"REASON: {reason}\n")
        if error_details:
            self.failed_log.write(f"ERROR: {error_details}\n")
        self.failed_log.write(f"Timestamp: {datetime.now().isoformat()}\n")
        self.failed_log.write(f"\nJSON Format:\n")
        self.failed_log.write(json.dumps(failed_info, indent=2, ensure_ascii=False, default=str))
        self.failed_log.write(f"\n")
        self.failed_log.flush()
    
    def log_invalid_ids(self, arrests: Dict, invalid_ids: Dict, chunk_range: str = ""):
        """
        Log arrests that failed due to invalid IDs (crime_id only - these are skipped)
        
        Args:
            arrests: Transformed arrests dict
            invalid_ids: Dict with keys 'crime_id', 'person_id' indicating which are invalid
            chunk_range: Date range for chunk tracking
        """
        failure_info = {
            'crime_id': arrests.get('_original_crime_id'),
            'person_id': arrests.get('_original_person_id'),
            'accused_seq_no': arrests.get('accused_seq_no'),
            'invalid_crime_id': invalid_ids.get('crime_id', False),
            'invalid_person_id': invalid_ids.get('person_id', False),
            'chunk': chunk_range,
            'timestamp': datetime.now().isoformat(),
            'arrests_data': arrests
        }
        
        # Build reason string
        reasons = []
        if invalid_ids.get('crime_id'):
            reasons.append('CRIME_ID not found in crimes table')
        if invalid_ids.get('person_id'):
            reasons.append('PERSON_ID not found in persons table')
        reason_str = '; '.join(reasons) if reasons else 'Unknown'
        
        self.invalid_ids_log.write(f"\n{'='*80}\n")
        self.invalid_ids_log.write(f"CRIME_ID: {arrests.get('_original_crime_id')}\n")
        self.invalid_ids_log.write(f"PERSON_ID: {arrests.get('_original_person_id')}\n")
        self.invalid_ids_log.write(f"ACCUSED_SEQ_NO: {arrests.get('accused_seq_no')}\n")
        self.invalid_ids_log.write(f"REASON: {reason_str}\n")
        self.invalid_ids_log.write(f"Chunk: {chunk_range}\n")
        self.invalid_ids_log.write(f"Timestamp: {datetime.now().isoformat()}\n")
        self.invalid_ids_log.write(f"\nJSON Format:\n")
        self.invalid_ids_log.write(json.dumps(failure_info, indent=2, ensure_ascii=False, default=str))
        self.invalid_ids_log.write(f"\n")
        self.invalid_ids_log.flush()
    
    def log_invalid_person_id(self, arrests: Dict, original_person_id: str, chunk_range: str = ""):
        """
        Log arrests where PERSON_ID from API was not found in persons table
        These records are still processed with person_id = NULL
        
        Args:
            arrests: Transformed arrests dict
            original_person_id: Original PERSON_ID from API
            chunk_range: Date range for chunk tracking
        """
        failure_info = {
            'crime_id': arrests.get('_original_crime_id'),
            'person_id': original_person_id,
            'accused_seq_no': arrests.get('accused_seq_no'),
            'chunk': chunk_range,
            'timestamp': datetime.now().isoformat(),
            'note': 'Record is still processed with person_id = NULL',
            'arrests_data': arrests
        }
        
        self.invalid_person_id_log.write(f"\n{'='*80}\n")
        self.invalid_person_id_log.write(f"CRIME_ID: {arrests.get('_original_crime_id')}\n")
        self.invalid_person_id_log.write(f"PERSON_ID (from API): {original_person_id}\n")
        self.invalid_person_id_log.write(f"ACCUSED_SEQ_NO: {arrests.get('accused_seq_no')}\n")
        self.invalid_person_id_log.write(f"REASON: PERSON_ID not found in persons table\n")
        self.invalid_person_id_log.write(f"ACTION: Record processed with person_id = NULL\n")
        self.invalid_person_id_log.write(f"Chunk: {chunk_range}\n")
        self.invalid_person_id_log.write(f"Timestamp: {datetime.now().isoformat()}\n")
        self.invalid_person_id_log.write(f"\nJSON Format:\n")
        self.invalid_person_id_log.write(json.dumps(failure_info, indent=2, ensure_ascii=False, default=str))
        self.invalid_person_id_log.write(f"\n")
        self.invalid_person_id_log.flush()
    
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
            self.duplicates_log.write(f"  {i}. CRIME_ID: {dup['crime_id']}, ACCUSED_SEQ_NO: {dup.get('accused_seq_no')}\n")
            self.duplicates_log.write(f"     Occurrence: #{dup.get('occurrence', 'N/A')}\n")
            self.duplicates_log.write(f"     First seen in: {dup['first_seen_in']}\n")
            self.duplicates_log.write(f"     Duplicate in: {dup['duplicate_in']}\n")
        
        # Also write JSON format for easy parsing
        self.duplicates_log.write(f"\nJSON Format:\n")
        self.duplicates_log.write(json.dumps(chunk_info, indent=2, ensure_ascii=False))
        self.duplicates_log.write(f"\n")
        
        self.duplicates_log.flush()
    
    def insert_arrests(self, arrests: Dict, chunk_date_range: str = "") -> Tuple[bool, str]:
        """
        Insert or update single arrests record into database with smart update logic
        Dates are always from API (never use CURRENT_TIMESTAMP)
        
        Behavior:
        - NEW DATA: If (crime_id, accused_seq_no) doesn't exist → INSERT
        - EXISTING DATA: If exists → UPDATE (updates only changed fields)
        - Smart Update: Only updates fields that have changed, preserves existing values if API sends NULL
        
        Date Handling:
        - date_created and date_modified are always taken from API
        - If API provides dates, they are used (even if different from existing)
        - If API doesn't provide dates, they remain NULL
        
        Args:
            arrests: Transformed arrests dict
            chunk_date_range: Date range for chunk tracking
        
        Returns:
            Tuple of (success: bool, operation: str) where operation is 'inserted', 'updated', 'no_change', or 'skipped'
        """
        crime_id = arrests.get('crime_id')
        person_id = arrests.get('person_id')
        accused_seq_no = arrests.get('accused_seq_no')
        original_crime_id = arrests.get('_original_crime_id')
        original_person_id = arrests.get('_original_person_id')
        
        # Validate IDs and track which ones are invalid
        invalid_ids = {}
        has_invalid_ids = False
        
        # Validate crime_id (required)
        if not crime_id:
            invalid_ids['crime_id'] = True
            has_invalid_ids = True
        
        # Validate person_id (optional - only if provided in API)
        # If API provides PERSON_ID but it's not found, log to separate file and process with NULL
        if original_person_id and not person_id:
            invalid_ids['person_id'] = True
            logger.warning(f"⚠️  PERSON_ID {original_person_id} not found in persons table, will set to NULL and process record")
            self.stats['total_arrests_failed_person_id'] += 1
            # Set person_id to None explicitly
            arrests['person_id'] = None
            # Log to separate file for invalid person_id
            self.log_invalid_person_id(arrests, original_person_id, chunk_date_range)
        
        # Only skip if crime_id is invalid (required field)
        # person_id and accused_id are optional, so we process the record even if they're invalid
        if invalid_ids.get('crime_id'):
            reason = 'invalid_ids'
            error_details = f"Invalid IDs: {invalid_ids}"
            logger.warning(f"⚠️  {error_details}, skipping arrests")
            self.stats['total_arrests_failed'] += 1
            self.stats['total_arrests_failed_crime_id'] += 1
            if invalid_ids.get('person_id'):
                self.stats['total_arrests_failed_person_id'] += 1
            self.log_failed_record(arrests, reason, error_details)
            self.log_invalid_ids(arrests, invalid_ids, chunk_date_range)
            return False, reason
        
        if not accused_seq_no:
            reason = 'missing_accused_seq_no'
            error_details = "Arrests record missing ACCUSED_SEQ_NO"
            logger.warning(f"⚠️  {error_details}")
            self.stats['total_arrests_failed'] += 1
            self.log_failed_record(arrests, reason, error_details)
            return False, reason
        
        try:
            logger.trace(f"Processing arrests: crime_id={crime_id}, accused_seq_no={accused_seq_no}")
            
            # Check if arrests already exists (based on unique constraint)
            if self.arrests_exists(crime_id, accused_seq_no):
                # Get existing record to compare
                existing = self.get_existing_arrests(crime_id, accused_seq_no)
                if not existing:
                    logger.warning(f"⚠️  Arrests exists check returned True but fetch returned None")
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
                    
                    # Define all fields to check (excluding unique key fields: crime_id, accused_seq_no)
                    fields_to_check = [
                        ('person_id', 'PERSON_ID'),
                        ('accused_code', 'ACCUSED_CODE'),
                        ('accused_type', 'ACCUSED_TYPE'),
                        ('is_arrested', 'IS_ARRESTED'),
                        ('arrested_date', 'ARRESTED_DATE'),
                        ('is_41a_crpc', 'IS_41A_CRPC'),
                        ('is_41a_explain_submitted', 'IS_41A_EXPLAIN_SUBMITTED'),
                        ('date_of_issue_41a', 'DATE_OF_ISSUE_41A'),
                        ('is_ccl', 'IS_CCL'),
                        ('is_apprehended', 'IS_APPREHENDED'),
                        ('is_absconding', 'IS_ABSCONDING'),
                        ('is_died', 'IS_DIED'),
                        ('date_created', 'DATE_CREATED'),  # Always from API
                        ('date_modified', 'DATE_MODIFIED')  # Always from API
                    ]
                    
                    for db_field, api_field in fields_to_check:
                        existing_val = existing.get(db_field)
                        new_val = arrests.get(db_field)
                        
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
                            UPDATE {ARRESTS_TABLE} SET
                                {', '.join(update_fields)}
                            WHERE crime_id = %s AND accused_seq_no = %s
                        """
                        update_values.extend([crime_id, accused_seq_no])
                        self.db_cursor.execute(update_query, tuple(update_values))
                        self.stats['total_arrests_updated'] += 1
                        logger.debug(f"Updated arrests: crime_id={crime_id}, accused_seq_no={accused_seq_no} ({len(changes)} fields changed)")
                        logger.trace(f"Changes: {', '.join(changes)}")
                        self.db_conn.commit()
                        logger.trace(f"Transaction committed for updated arrests")
                        return True, 'updated'
                    else:
                        # No changes needed
                        self.stats['total_arrests_no_change'] += 1
                        logger.trace(f"No changes needed for arrests (all fields match or preserved)")
                        return True, 'no_change'
                else:
                    # Exists check returned True but couldn't fetch - treat as new insert
                    logger.warning(f"⚠️  Arrests exists but couldn't fetch, treating as new insert")
                    # Fall through to insert logic
            else:
                # Insert new arrests
                logger.trace(f"Inserting new arrests: crime_id={crime_id}, accused_seq_no={accused_seq_no}")
                insert_query = f"""
                    INSERT INTO {ARRESTS_TABLE} (
                        crime_id, person_id, accused_seq_no, accused_code, accused_type,
                        is_arrested, arrested_date, is_41a_crpc, is_41a_explain_submitted,
                        date_of_issue_41a, is_ccl, is_apprehended, is_absconding, is_died,
                        date_created, date_modified
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """
                self.db_cursor.execute(insert_query, (
                    crime_id,
                    person_id,  # Can be NULL
                    accused_seq_no,
                    arrests.get('accused_code'),
                    arrests.get('accused_type'),
                    arrests.get('is_arrested'),
                    arrests.get('arrested_date'),
                    arrests.get('is_41a_crpc'),
                    arrests.get('is_41a_explain_submitted'),
                    arrests.get('date_of_issue_41a'),
                    arrests.get('is_ccl'),
                    arrests.get('is_apprehended'),
                    arrests.get('is_absconding'),
                    arrests.get('is_died'),
                    arrests.get('date_created'),  # From API (or NULL)
                    arrests.get('date_modified')  # From API (or NULL)
                ))
                self.stats['total_arrests_inserted'] += 1
                logger.debug(f"Inserted arrests: crime_id={crime_id}, accused_seq_no={accused_seq_no}")
                logger.trace(f"Insert query executed for arrests")
                self.db_conn.commit()
                logger.trace(f"Transaction committed for inserted arrests")
                return True, 'inserted'
            
        except psycopg2.IntegrityError as e:
            self.db_conn.rollback()
            reason = 'integrity_error'
            error_details = str(e)
            logger.warning(f"⚠️  Integrity error for arrests: {e}")
            self.stats['total_arrests_failed'] += 1
            self.log_failed_record(arrests, reason, error_details)
            return False, reason
        except Exception as e:
            self.db_conn.rollback()
            reason = 'error'
            error_details = str(e)
            logger.error(f"❌ Error inserting arrests: {e}")
            self.stats['total_arrests_failed'] += 1
            self.stats['errors'].append(f"Arrests crime_id={crime_id}: {str(e)}")
            self.log_failed_record(arrests, reason, error_details)
            return False, reason
    
    def process_date_range(self, from_date: str, to_date: str, table_columns: Set[str] = None):
        """Process arrests records for a specific date range"""
        chunk_range = f"{from_date} to {to_date}"
        logger.info(f"📅 Processing: {chunk_range}")
        
        # Fetch arrests from API
        arrests_raw = self.fetch_arrests_api(from_date, to_date)
        
        if arrests_raw is None:
            logger.error(f"❌ Failed to fetch arrests for {chunk_range}")
            self.log_db_chunk(from_date, to_date, 0, [], [], [], [], [], error="API fetch failed")
            return
        
        if not arrests_raw:
            logger.info(f"ℹ️  No arrests records found for {chunk_range}")
            self.log_db_chunk(from_date, to_date, 0, [], [], [], [], [], error="No arrests records in API response")
            return
        
        # Check for schema evolution if we got data
        if table_columns is not None and len(arrests_raw) > 0:
            # Check for new fields in first record
            new_fields = self.detect_new_fields(arrests_raw[0], table_columns)
            if new_fields:
                logger.info(f"🔍 New fields detected in API response: {list(new_fields.keys())}")
                # Add new columns to table
                for api_field, db_column in new_fields.items():
                    if self.add_column_to_table(db_column):
                        # Update table_columns set
                        table_columns.add(db_column)
                # Update existing records from start_date to current chunk end_date
                self.update_existing_records_with_new_fields(new_fields, to_date)
        
        # Transform and insert each arrests
        self.stats['total_arrests_fetched'] += len(arrests_raw)
        logger.trace(f"Processing {len(arrests_raw)} arrests records for chunk {chunk_range}")
        
        # Track operations for this chunk
        inserted_keys = []
        updated_keys = []
        no_change_keys = []
        failed_keys = []
        failed_reasons = {}
        duplicates_in_chunk = []
        invalid_ids_in_chunk = []
        
        # Track unique keys seen in this chunk to detect duplicates (for reporting only, not skipping)
        seen_keys = {}
        key_occurrences = {}
        
        logger.trace(f"Starting to process records for chunk: {chunk_range}")
        for idx, arrests_record in enumerate(arrests_raw, 1):
            logger.trace(f"Processing record {idx}/{len(arrests_raw)}: {arrests_record.get('CRIME_ID')}")
            arrests = self.transform_arrests(arrests_record)
            crime_id = arrests.get('crime_id')
            person_id = arrests.get('person_id')
            accused_seq_no = arrests.get('accused_seq_no')
            original_crime_id = arrests.get('_original_crime_id')
            original_person_id = arrests.get('_original_person_id')
            
            # Check if any IDs are invalid
            invalid_ids = {}
            if not crime_id:
                invalid_ids['crime_id'] = True
            if original_person_id and not person_id:
                invalid_ids['person_id'] = True
            
            # Only skip if crime_id is invalid (required field)
            # person_id and accused_id are optional, so we process the record even if they're invalid
            if invalid_ids.get('crime_id'):
                logger.warning(f"⚠️  Arrests with invalid CRIME_ID: {original_crime_id}, skipping")
                self.stats['total_arrests_failed'] += 1
                self.stats['total_arrests_failed_crime_id'] += 1
                failed_keys.append(f"{original_crime_id}:{accused_seq_no}")
                reason = 'invalid_ids'
                if reason not in failed_reasons:
                    failed_reasons[reason] = []
                failed_reasons[reason].append(f"{original_crime_id}:{accused_seq_no}")
                invalid_ids_in_chunk.append({
                    'crime_id': original_crime_id,
                    'person_id': original_person_id,
                    'accused_seq_no': accused_seq_no,
                    'invalid_ids': invalid_ids
                })
                self.log_invalid_ids(arrests, invalid_ids, chunk_range)
                continue
            
            # Log warnings for invalid optional IDs but continue processing
            if invalid_ids.get('person_id'):
                logger.warning(f"⚠️  PERSON_ID {original_person_id} not found, will set to NULL and process record")
                self.stats['total_arrests_failed_person_id'] += 1
                arrests['person_id'] = None
                invalid_ids_in_chunk.append({
                    'crime_id': original_crime_id,
                    'person_id': original_person_id,
                    'accused_seq_no': accused_seq_no,
                    'invalid_ids': invalid_ids
                })
                # Log to separate file for invalid person_id
                self.log_invalid_person_id(arrests, original_person_id, chunk_range)
            
            # Create unique key for tracking duplicates (based on unique constraint)
            unique_key = f"{crime_id}:{accused_seq_no}"
            
            # Track occurrences for duplicate reporting (but don't skip - process all)
            if unique_key in seen_keys:
                # This is a duplicate occurrence - track it but still process
                occurrence_count = key_occurrences.get(unique_key, 1) + 1
                key_occurrences[unique_key] = occurrence_count
                
                duplicates_in_chunk.append({
                    'crime_id': crime_id,
                    'accused_seq_no': accused_seq_no,
                    'occurrence': occurrence_count,
                    'first_seen_in': seen_keys[unique_key],
                    'duplicate_in': chunk_range
                })
                self.stats['total_duplicates'] += 1
                logger.info(f"⚠️  Duplicate arrests found in chunk {chunk_range} (occurrence #{occurrence_count}) - Will process to update record")
                logger.trace(f"Duplicate details - First seen: {seen_keys[unique_key]}, Current occurrence: {occurrence_count}")
            else:
                seen_keys[unique_key] = chunk_range
                key_occurrences[unique_key] = 1
                logger.trace(f"New arrests key seen: {unique_key} in chunk {chunk_range}")
            
            # IMPORTANT: Process ALL records, even duplicates
            # If same key appears multiple times, each occurrence might have updated data
            # The smart update logic will handle whether to actually update or not
            success, operation = self.insert_arrests(arrests, chunk_range)
            logger.trace(f"Operation result for arrests: success={success}, operation={operation}")
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
        
        # Log invalid IDs for this chunk
        if invalid_ids_in_chunk:
            logger.warning(f"⚠️  Found {len(invalid_ids_in_chunk)} arrests records with invalid IDs in chunk {chunk_range}")
            # Extract unique invalid IDs
            invalid_crime_ids = list(set([f['crime_id'] for f in invalid_ids_in_chunk if f.get('invalid_ids', {}).get('crime_id')]))
            invalid_person_ids = list(set([f['person_id'] for f in invalid_ids_in_chunk if f.get('invalid_ids', {}).get('person_id')]))
            if invalid_crime_ids:
                logger.warning(f"   Invalid CRIME_IDs: {invalid_crime_ids}")
            if invalid_person_ids:
                logger.warning(f"   Invalid PERSON_IDs: {invalid_person_ids}")
        
        # Log database operations for this chunk
        logger.trace(f"Chunk summary - Inserted: {len(inserted_keys)}, Updated: {len(updated_keys)}, No Change: {len(no_change_keys)}, Failed: {len(failed_keys)}, Duplicates: {len(duplicates_in_chunk)}, Invalid IDs: {len(invalid_ids_in_chunk)}")
        self.log_db_chunk(from_date, to_date, len(arrests_raw), inserted_keys, updated_keys, 
                         no_change_keys, failed_keys, failed_reasons)
        
        logger.info(f"✅ Completed: {chunk_range} - Inserted: {len(inserted_keys)}, Updated: {len(updated_keys)}, No Change: {len(no_change_keys)}, Failed: {len(failed_keys)}, Duplicates: {len(duplicates_in_chunk)}, Invalid IDs: {len(invalid_ids_in_chunk)}")
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
        self.api_log.write(f"Total Arrests Fetched: {self.stats['total_arrests_fetched']}\n")
        self.api_log.write(f"Failed API Calls: {self.stats['failed_api_calls']}\n")
        self.api_log.write(f"Total Chunks Processed: {self.stats['total_api_calls'] + self.stats['failed_api_calls']}\n")
        
        # DB log summary
        self.db_log.write(f"\n\n{'='*80}\n")
        self.db_log.write(f"SUMMARY\n")
        self.db_log.write(f"{'='*80}\n")
        self.db_log.write(f"Total Arrests Fetched from API: {self.stats['total_arrests_fetched']}\n")
        self.db_log.write(f"Total Arrests Inserted (New): {self.stats['total_arrests_inserted']}\n")
        self.db_log.write(f"Total Arrests Updated (Existing): {self.stats['total_arrests_updated']}\n")
        self.db_log.write(f"Total Arrests No Change: {self.stats['total_arrests_no_change']}\n")
        self.db_log.write(f"Total Arrests Failed: {self.stats['total_arrests_failed']}\n")
        self.db_log.write(f"  - Failed due to Invalid CRIME_ID (SKIPPED): {self.stats['total_arrests_failed_crime_id']}\n")
        self.db_log.write(f"  - Invalid PERSON_ID (Processed with NULL): {self.stats['total_arrests_failed_person_id']}\n")
        self.db_log.write(f"Note: PERSON_ID and ACCUSED_ID are optional. Records with invalid values are still processed with NULL.\n")
        self.db_log.write(f"Total Arrests Duplicates (Processed): {self.stats['total_duplicates']}\n")
        self.db_log.write(f"Total Operations (Inserted + Updated + No Change): {self.stats['total_arrests_inserted'] + self.stats['total_arrests_updated'] + self.stats['total_arrests_no_change']}\n")
        db_total = self.stats.get('db_total_count', self.stats['total_arrests_inserted'])
        self.db_log.write(f"Total Unique Arrests in Database: {db_total}\n")
        self.db_log.write(f"Note: Updated count includes multiple updates (same key in multiple chunks or same chunk)\n")
        self.db_log.write(f"Note: Duplicates are records that appear multiple times within the same chunk - ALL are processed for updates\n")
        if self.stats['total_arrests_fetched'] > 0:
            coverage = ((self.stats['total_arrests_inserted'] + self.stats['total_arrests_updated'] + self.stats['total_arrests_no_change']) / self.stats['total_arrests_fetched']) * 100
            self.db_log.write(f"Coverage: {coverage:.2f}%\n")
        self.db_log.write(f"Errors: {len(self.stats['errors'])}\n")
        
        # Failed records log summary
        self.failed_log.write(f"\n\n{'='*80}\n")
        self.failed_log.write(f"SUMMARY\n")
        self.failed_log.write(f"{'='*80}\n")
        self.failed_log.write(f"Total Failed Records: {self.stats['total_arrests_failed']}\n")
        self.failed_log.write(f"Note: Failed records are those that could not be inserted or updated\n")
        self.failed_log.write(f"Check individual entries above for specific reasons\n")
        
        # Invalid IDs log summary (CRIME_ID only - these are skipped)
        self.invalid_ids_log.write(f"\n\n{'='*80}\n")
        self.invalid_ids_log.write(f"SUMMARY\n")
        self.invalid_ids_log.write(f"{'='*80}\n")
        self.invalid_ids_log.write(f"Total Arrests SKIPPED Due to Invalid CRIME_ID: {self.stats['total_arrests_failed_crime_id']}\n")
        self.invalid_ids_log.write(f"\n")
        self.invalid_ids_log.write(f"Note: These arrests records were SKIPPED because CRIME_ID was not found in crimes table.\n")
        self.invalid_ids_log.write(f"      CRIME_ID is a required field, so these records cannot be processed.\n")
        self.invalid_ids_log.write(f"      Please ensure these CRIME_IDs are loaded in the crimes table first.\n")
        
        # Invalid PERSON_ID log summary (processed with NULL)
        self.invalid_person_id_log.write(f"\n\n{'='*80}\n")
        self.invalid_person_id_log.write(f"SUMMARY\n")
        self.invalid_person_id_log.write(f"{'='*80}\n")
        self.invalid_person_id_log.write(f"Total Arrests with Invalid PERSON_ID (Processed with NULL): {self.stats['total_arrests_failed_person_id']}\n")
        self.invalid_person_id_log.write(f"\n")
        self.invalid_person_id_log.write(f"Note: These arrests records were PROCESSED with person_id = NULL because\n")
        self.invalid_person_id_log.write(f"      PERSON_ID from API was not found in persons table.\n")
        self.invalid_person_id_log.write(f"      PERSON_ID is optional, so records are still inserted/updated with person_id = NULL.\n")
        self.invalid_person_id_log.write(f"      Please ensure these PERSON_IDs are loaded in the persons table if needed.\n")
        
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
        logger.info("🚀 DOPAMAS ETL Pipeline - Arrests API")
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
            table_columns = self.get_table_columns(ARRESTS_TABLE)
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
            self.db_cursor.execute(f"SELECT COUNT(*) FROM {ARRESTS_TABLE}")
            db_arrests_count = self.db_cursor.fetchone()[0]
            
            # Store for summary
            self.stats['db_total_count'] = db_arrests_count
            
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
            logger.info(f"  Total Arrests Fetched: {self.stats['total_arrests_fetched']}")
            logger.info(f"")
            logger.info(f"💾 TO DATABASE:")
            logger.info(f"  Total Inserted (New): {self.stats['total_arrests_inserted']}")
            logger.info(f"  Total Updated:        {self.stats['total_arrests_updated']}")
            logger.info(f"  Total No Change:      {self.stats['total_arrests_no_change']}")
            logger.info(f"  Total Failed:         {self.stats['total_arrests_failed']}")
            logger.info(f"    - Invalid CRIME_ID:   {self.stats['total_arrests_failed_crime_id']}")
            logger.info(f"    - Invalid PERSON_ID:  {self.stats['total_arrests_failed_person_id']}")
            logger.info(f"  Total in DB:          {db_arrests_count}")
            logger.info(f"")
            logger.info(f"🔄 DUPLICATES:")
            logger.info(f"  Total Duplicate Occurrences (Processed): {self.stats['total_duplicates']}")
            logger.info(f"  Note: All duplicates are processed to allow updates")
            logger.info(f"")
            logger.info(f"⚠️  INVALID IDs:")
            logger.info(f"  Arrests SKIPPED Due to Invalid CRIME_ID: {self.stats['total_arrests_failed_crime_id']}")
            logger.info(f"    Check logs/arrests_invalid_ids_*.log for details")
            logger.info(f"  Arrests with Invalid PERSON_ID (Processed with NULL): {self.stats['total_arrests_failed_person_id']}")
            logger.info(f"    Check logs/arrests_invalid_person_id_*.log for details")
            logger.info(f"")
            logger.info(f"📊 COVERAGE:")
            if self.stats['total_arrests_fetched'] > 0:
                coverage = ((self.stats['total_arrests_inserted'] + self.stats['total_arrests_updated'] + self.stats['total_arrests_no_change']) / self.stats['total_arrests_fetched']) * 100
                logger.info(f"  API → DB Coverage:   {coverage:.2f}%")
            logger.info(f"")
            logger.info(f"📈 SUMMARY:")
            logger.info(f"  Total from API:       {self.stats['total_arrests_fetched']}")
            logger.info(f"  Inserted + Updated:   {self.stats['total_arrests_inserted'] + self.stats['total_arrests_updated']}")
            logger.info(f"  Duplicate Occurrences: {self.stats['total_duplicates']} (all processed)")
            logger.info(f"  Failed:               {self.stats['total_arrests_failed']}")
            logger.info(f"")
            logger.info(f"💡 NOTE:")
            logger.info(f"  - Same arrests key can appear multiple times in API response")
            logger.info(f"  - Each occurrence is processed to capture any data updates")
            logger.info(f"  - Smart update logic ensures only changed fields are updated")
            logger.info(f"  - Invalid IDs (any combination) are logged separately for review")
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
            logger.info(f"📝 Invalid IDs log (CRIME_ID only - skipped) saved to: {self.invalid_ids_log_file}")
            logger.info(f"📝 Invalid PERSON_ID log (processed with NULL) saved to: {self.invalid_person_id_log_file}")
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
    etl = ArrestsETL()
    success = etl.run()
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()


