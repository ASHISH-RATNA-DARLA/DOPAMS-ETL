#!/usr/bin/env python3
"""
DOPAMAS ETL Pipeline - FSL Case Property API
Fetches FSL case property data in 5-day chunks and loads into PostgreSQL
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
FSL_CASE_PROPERTY_TABLE = TABLE_CONFIG.get('fsl_case_property', 'fsl_case_property')
FSL_CASE_PROPERTY_MEDIA_TABLE = TABLE_CONFIG.get('fsl_case_property_media', 'fsl_case_property_media')
CRIMES_TABLE = TABLE_CONFIG.get('crimes', 'crimes')

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


class FSLCasePropertyETL:
    """ETL Pipeline for FSL Case Property API"""
    
    def __init__(self):
        self.db_conn = None
        self.db_cursor = None
        self.stats = {
            'total_api_calls': 0,
            'total_records_fetched': 0,
            'total_records_inserted': 0,
            'total_records_updated': 0,
            'total_records_no_change': 0,  # Records that exist but no changes needed
            'total_records_failed': 0,  # Records that failed to insert/update
            'total_records_failed_crime_id': 0,  # Records failed due to CRIME_ID not found
            'total_media_inserted': 0,  # Media files inserted
            'total_media_updated': 0,  # Media files updated
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
        self.api_log_file = f'logs/fsl_case_property_api_chunks_{timestamp}.log'
        self.api_log = open(self.api_log_file, 'w', encoding='utf-8')
        self.api_log.write(f"# FSL Case Property API Chunk-wise Log\n")
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
        self.db_log_file = f'logs/fsl_case_property_db_chunks_{timestamp}.log'
        self.db_log = open(self.db_log_file, 'w', encoding='utf-8')
        self.db_log.write(f"# FSL Case Property Database Operations Chunk-wise Log\n")
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
        self.failed_log_file = f'logs/fsl_case_property_failed_{timestamp}.log'
        self.failed_log = open(self.failed_log_file, 'w', encoding='utf-8')
        self.failed_log.write(f"# FSL Case Property Failed Records Log\n")
        self.failed_log.write(f"# Generated: {datetime.now().isoformat()}\n")
        self.failed_log.write(f"# Records that failed to insert or update with reasons\n")
        self.failed_log.write(f"{'='*80}\n\n")
        
        # Invalid crime_id log file (records with crime_id not found in crimes table)
        self.invalid_crime_id_log_file = f'logs/fsl_case_property_invalid_crime_id_{timestamp}.log'
        self.invalid_crime_id_log = open(self.invalid_crime_id_log_file, 'w', encoding='utf-8')
        self.invalid_crime_id_log.write(f"# FSL Case Property Invalid CRIME_ID Log\n")
        self.invalid_crime_id_log.write(f"# Generated: {datetime.now().isoformat()}\n")
        self.invalid_crime_id_log.write(f"# Records that failed because CRIME_ID not found in crimes table\n")
        self.invalid_crime_id_log.write(f"{'='*80}\n\n")
        
        # Duplicates log file (duplicate records found within chunks)
        self.duplicates_log_file = f'logs/fsl_case_property_duplicates_{timestamp}.log'
        self.duplicates_log = open(self.duplicates_log_file, 'w', encoding='utf-8')
        self.duplicates_log.write(f"# FSL Case Property Duplicates Log\n")
        self.duplicates_log.write(f"# Generated: {datetime.now().isoformat()}\n")
        self.duplicates_log.write(f"# Duplicate records found within the same chunk\n")
        self.duplicates_log.write(f"{'='*80}\n\n")
        
        logger.info(f"📝 API chunk log: {self.api_log_file}")
        logger.info(f"📝 DB chunk log: {self.db_log_file}")
        logger.info(f"📝 Failed records log: {self.failed_log_file}")
        logger.info(f"📝 Invalid CRIME_ID log: {self.invalid_crime_id_log_file}")
        logger.info(f"📝 Duplicates log: {self.duplicates_log_file}")
    
    def close_chunk_loggers(self):
        """Close chunk log files"""
        if hasattr(self, 'api_log') and self.api_log:
            self.api_log.close()
        if hasattr(self, 'db_log') and self.db_log:
            self.db_log.close()
        if hasattr(self, 'failed_log') and self.failed_log:
            self.failed_log.close()
        if hasattr(self, 'invalid_crime_id_log') and self.invalid_crime_id_log:
            self.invalid_crime_id_log.close()
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
            self.db_cursor.execute(f"SELECT COUNT(*) FROM {FSL_CASE_PROPERTY_TABLE}")
            count = self.db_cursor.fetchone()[0]
            logger.debug(f"Table {FSL_CASE_PROPERTY_TABLE} has {count} records")
            
            if count == 0:
                # Table is empty (truncated or new), start from beginning
                logger.info("📊 Table is empty (truncated or new), starting from 2022-01-01")
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
                FROM {FSL_CASE_PROPERTY_TABLE}
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
        
        # Map API field names to database column names (based on schema)
        field_mapping = {
            'CASE_PROPERTY_ID': 'case_property_id',
            'CASE_TYPE': 'case_type',
            'CRIME_ID': 'crime_id',
            'MO_ID': 'mo_id',
            'STATUS': 'status',
            'SEND_DATE': 'send_date',
            'FSL_DATE': 'fsl_date',
            'DATE_DISPOSAL': 'date_disposal',
            'RELEASE_DATE': 'release_date',
            'RETURN_DATE': 'return_date',
            'DATE_CUSTODY': 'date_custody',
            'DATE_SENT_TO_EXPERT': 'date_sent_to_expert',
            'COURT_ORDER_DATE': 'court_order_date',
            'FORWARDING_THROUGH': 'forwarding_through',
            'COURT_NAME': 'court_name',
            'FSL_COURT_NAME': 'fsl_court_name',
            'CPR_COURT_NAME': 'cpr_court_name',
            'COURT_ORDER_NUMBER': 'court_order_number',
            'FSL_NO': 'fsl_no',
            'FSL_REQUEST_ID': 'fsl_request_id',
            'REPORT_RECEIVED': 'report_received',
            'OPINION': 'opinion',
            'OPINION_FURNISHED': 'opinion_furnished',
            'STRENGTH_OF_EVIDENCE': 'strength_of_evidence',
            'EXPERT_TYPE': 'expert_type',
            'OTHER_EXPERT_TYPE': 'other_expert_type',
            'CPR_NO': 'cpr_no',
            'DIRECTION_BY_COURT': 'direction_by_court',
            'DETAILS_DISPOSAL': 'details_disposal',
            'PLACE_DISPOSAL': 'place_disposal',
            'RELEASE_ORDER_NO': 'release_order_no',
            'PLACE_CUSTODY': 'place_custody',
            'ASSIGN_CUSTODY': 'assign_custody',
            'PROPERTY_RECEIVED_BACK': 'property_received_back',
            'DATE_CREATED': 'date_created',
            'DATE_MODIFIED': 'date_modified'
        }
        
        for api_field, db_column in field_mapping.items():
            if api_field in api_record and db_column not in table_columns:
                new_fields[api_field] = db_column
        
        return new_fields
    
    def add_column_to_table(self, column_name: str, column_type: str = 'TEXT'):
        """Add a new column to the fsl_case_property table."""
        try:
            # Determine column type based on field name
            if 'date' in column_name.lower() or 'at' in column_name.lower():
                column_type = 'TIMESTAMPTZ'
            elif column_name == 'case_property_id':
                column_type = 'VARCHAR(255)'  # MongoDB ObjectId (24 hex characters)
            elif column_name == 'crime_id':
                column_type = 'VARCHAR(50)'  # Matches crimes.crime_id type
            elif column_name in ('mo_id', 'fsl_no', 'fsl_request_id', 'cpr_no', 'release_order_no', 'court_order_number'):
                column_type = 'VARCHAR(255)'
            elif 'id' in column_name.lower():
                column_type = 'VARCHAR(50)'  # Most IDs are VARCHAR in this schema
            elif column_name in ('report_received', 'property_received_back'):
                column_type = 'BOOLEAN DEFAULT FALSE'
            elif column_name in ('opinion', 'direction_by_court', 'details_disposal'):
                column_type = 'TEXT'
            elif column_name in ('court_name', 'fsl_court_name', 'cpr_court_name', 'place_disposal', 'place_custody'):
                column_type = 'VARCHAR(500)'
            elif column_name in ('case_type', 'status', 'forwarding_through', 'opinion_furnished', 'strength_of_evidence', 
                               'expert_type', 'other_expert_type', 'assign_custody'):
                column_type = 'VARCHAR(255)'
            else:
                column_type = 'TEXT'
            
            alter_sql = f"ALTER TABLE {FSL_CASE_PROPERTY_TABLE} ADD COLUMN IF NOT EXISTS {column_name} {column_type}"
            self.db_cursor.execute(alter_sql)
            self.db_conn.commit()
            logger.info(f"✅ Added column {column_name} ({column_type}) to {FSL_CASE_PROPERTY_TABLE}")
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
    
    def fetch_fsl_case_property_api(self, from_date: str, to_date: str) -> Optional[List[Dict]]:
        """
        Fetch FSL case property data from API for given date range
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
        
        Returns:
            List of FSL case property records or None if failed
        """
        # Use fsl_case_property_url from config (which reads from .env)
        url = API_CONFIG.get('fsl_case_property_url', f"{API_CONFIG['base_url']}/case-property")
        params = {
            'fromDate': from_date,
            'toDate': to_date
        }
        headers = {
            'x-api-key': API_CONFIG['api_key']
        }
        
        for attempt in range(API_CONFIG['max_retries']):
            try:
                logger.debug(f"Fetching FSL case property: {from_date} to {to_date} (Attempt {attempt + 1})")
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
                        case_property_data = data.get('data')
                        if case_property_data:
                            # If single object, convert to list
                            if isinstance(case_property_data, dict):
                                case_property_data = [case_property_data]
                            
                            # Extract crime_ids for logging
                            crime_ids = [d.get('CRIME_ID') for d in case_property_data if d.get('CRIME_ID')]
                            
                            # Log to API chunk file
                            self.log_api_chunk(from_date, to_date, len(case_property_data), crime_ids, case_property_data)
                            
                            logger.info(f"✅ Fetched {len(case_property_data)} FSL case property records for {from_date} to {to_date}")
                            logger.debug(f"📋 Crime IDs from API: {crime_ids[:10]}{'...' if len(crime_ids) > 10 else ''}")
                            logger.trace(f"Full Crime IDs list: {crime_ids}")
                            logger.trace(f"Sample case property structure: {json.dumps(case_property_data[0] if case_property_data else {}, indent=2, default=str)}")
                            return case_property_data
                        else:
                            # Log empty response
                            self.log_api_chunk(from_date, to_date, 0, [], [])
                            logger.warning(f"⚠️  No FSL case property records found for {from_date} to {to_date}")
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
        
        logger.error(f"❌ Failed to fetch FSL case property for {from_date} to {to_date} after {API_CONFIG['max_retries']} attempts")
        self.log_api_chunk(from_date, to_date, 0, [], [], error="Failed after max retries")
        return None
    
    def log_api_chunk(self, from_date: str, to_date: str, count: int, crime_ids: List[str], 
                     case_property_data: List[Dict], error: Optional[str] = None):
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
    
    def transform_fsl_case_property(self, case_property_raw: Dict) -> Dict:
        """
        Transform API response to database format
        Dates are always taken from API (never use CURRENT_TIMESTAMP)
        
        Args:
            case_property_raw: Raw FSL case property data from API
        
        Returns:
            Transformed case property dict ready for database
        """
        import uuid
        
        logger.trace(f"Transforming FSL case property: CASE_PROPERTY_ID={case_property_raw.get('CASE_PROPERTY_ID')}, CRIME_ID={case_property_raw.get('CRIME_ID')}")
        
        # Helper function to normalize values (convert empty strings to None)
        def normalize_value(value):
            """Convert empty strings to None for all fields"""
            if value is None:
                return None
            if isinstance(value, str):
                value = value.strip()
                if value == '' or value == 'null' or value.lower() == 'null':
                    return None
            return value
        
        # Get case_property_id (MongoDB ObjectId - 24 hex characters, store as string)
        case_property_id = case_property_raw.get('CASE_PROPERTY_ID')
        if not case_property_id:
            logger.warning(f"Missing CASE_PROPERTY_ID in record")
            return {}  # Return empty dict if essential ID is missing
        
        # Get crime_id - normalize it (will be validated by database FK constraint)
        crime_id_str = normalize_value(case_property_raw.get('CRIME_ID'))
        
        # Optional: Check if crime_id exists (for logging only, not for skipping)
        if crime_id_str:
            try:
                self.db_cursor.execute(f"SELECT crime_id FROM {CRIMES_TABLE} WHERE crime_id = %s", (crime_id_str,))
                result = self.db_cursor.fetchone()
                if result:
                    logger.trace(f"CRIME_ID {crime_id_str} found in crimes table")
                else:
                    logger.warning(f"⚠️  CRIME_ID {crime_id_str} not found in crimes table - will attempt insert (DB will enforce FK constraint)")
            except Exception as e:
                logger.error(f"Error checking crime_id {crime_id_str}: {e}")
                # Rollback transaction on error to allow continuation
                self.db_conn.rollback()
        
        # Helper function to parse boolean
        def parse_bool(value):
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.lower() in ('true', '1', 'yes', 't')
            return bool(value) if value is not None else False
        
        transformed = {
            'case_property_id': case_property_id,  # NOT NULL - required
            'case_type': normalize_value(case_property_raw.get('CASE_TYPE')),
            'crime_id': crime_id_str if crime_id_str else None,  # NOT NULL - use original value, let DB handle FK constraint
            'mo_id': normalize_value(case_property_raw.get('MO_ID')),
            'status': normalize_value(case_property_raw.get('STATUS')),
            # Dates - normalize empty strings to None (all can be NULL)
            'send_date': normalize_value(case_property_raw.get('SEND_DATE')),
            'fsl_date': normalize_value(case_property_raw.get('FSL_DATE')),
            'date_disposal': normalize_value(case_property_raw.get('DATE_DISPOSAL')),
            'release_date': normalize_value(case_property_raw.get('RELEASE_DATE')),
            'return_date': normalize_value(case_property_raw.get('RETURN_DATE')),
            'date_custody': normalize_value(case_property_raw.get('DATE_CUSTODY')),
            'date_sent_to_expert': normalize_value(case_property_raw.get('DATE_SENT_TO_EXPERT')),
            'court_order_date': normalize_value(case_property_raw.get('COURT_ORDER_DATE')),
            # Court Information (all can be NULL)
            'forwarding_through': normalize_value(case_property_raw.get('FORWARDING_THROUGH')),
            'court_name': normalize_value(case_property_raw.get('COURT_NAME')),
            'fsl_court_name': normalize_value(case_property_raw.get('FSL_COURT_NAME')),
            'cpr_court_name': normalize_value(case_property_raw.get('CPR_COURT_NAME')),
            'court_order_number': normalize_value(case_property_raw.get('COURT_ORDER_NUMBER')),
            # FSL Information (all can be NULL)
            'fsl_no': normalize_value(case_property_raw.get('FSL_NO')),
            'fsl_request_id': normalize_value(case_property_raw.get('FSL_REQUEST_ID')),
            'report_received': parse_bool(case_property_raw.get('REPORT_RECEIVED')) if case_property_raw.get('REPORT_RECEIVED') is not None else None,
            'opinion': normalize_value(case_property_raw.get('OPINION')),
            'opinion_furnished': normalize_value(case_property_raw.get('OPINION_FURNISHED')),
            'strength_of_evidence': normalize_value(case_property_raw.get('STRENGTH_OF_EVIDENCE')),
            'expert_type': normalize_value(case_property_raw.get('EXPERT_TYPE')),
            'other_expert_type': normalize_value(case_property_raw.get('OTHER_EXPERT_TYPE')),
            # Disposal Information (all can be NULL)
            'cpr_no': normalize_value(case_property_raw.get('CPR_NO')),
            'direction_by_court': normalize_value(case_property_raw.get('DIRECTION_BY_COURT')),
            'details_disposal': normalize_value(case_property_raw.get('DETAILS_DISPOSAL')),
            'place_disposal': normalize_value(case_property_raw.get('PLACE_DISPOSAL')),
            # Release Information (all can be NULL)
            'release_order_no': normalize_value(case_property_raw.get('RELEASE_ORDER_NO')),
            # Custody Information (all can be NULL)
            'place_custody': normalize_value(case_property_raw.get('PLACE_CUSTODY')),
            'assign_custody': normalize_value(case_property_raw.get('ASSIGN_CUSTODY')),
            # Property Status (can be NULL)
            'property_received_back': parse_bool(case_property_raw.get('PROPERTY_RECEIVED_BACK')) if case_property_raw.get('PROPERTY_RECEIVED_BACK') is not None else None,
            # Dates are always from API (never use CURRENT_TIMESTAMP)
            # If API doesn't provide dates, they will be NULL
            'date_created': normalize_value(case_property_raw.get('DATE_CREATED')),
            'date_modified': normalize_value(case_property_raw.get('DATE_MODIFIED')),
            # Store original CRIME_ID string for validation
            '_original_crime_id': crime_id_str,
            # Store media files if present
            '_media_files': case_property_raw.get('MEDIA_FILES', []) or case_property_raw.get('MEDIA', []) or []
        }
        logger.trace(f"Transformed case property: {json.dumps({k: v for k, v in transformed.items() if k not in ('_original_crime_id', '_media_files')}, indent=2, default=str)}")
        return transformed
    
    def case_property_exists(self, case_property_id) -> bool:
        """Check if case property already exists in database (based on primary key)"""
        if not case_property_id:
            return False
        logger.trace(f"Checking if case property exists: case_property_id={case_property_id}")
        query = f"""
            SELECT 1 FROM {FSL_CASE_PROPERTY_TABLE} 
            WHERE case_property_id = %s
        """
        self.db_cursor.execute(query, (case_property_id,))
        exists = self.db_cursor.fetchone() is not None
        logger.trace(f"Case property exists: {exists}")
        return exists
    
    def get_existing_case_property(self, case_property_id) -> Optional[Dict]:
        """Get existing case property record from database"""
        if not case_property_id:
            return None
        query = f"""
            SELECT case_property_id, case_type, crime_id, mo_id, status,
                   send_date, fsl_date, date_disposal, release_date, return_date,
                   date_custody, date_sent_to_expert, court_order_date,
                   forwarding_through, court_name, fsl_court_name, cpr_court_name,
                   court_order_number, fsl_no, fsl_request_id, report_received,
                   opinion, opinion_furnished, strength_of_evidence, expert_type,
                   other_expert_type, cpr_no, direction_by_court, details_disposal,
                   place_disposal, release_order_no, place_custody, assign_custody,
                   property_received_back, date_created, date_modified
            FROM {FSL_CASE_PROPERTY_TABLE}
            WHERE case_property_id = %s
        """
        self.db_cursor.execute(query, (case_property_id,))
        row = self.db_cursor.fetchone()
        if row:
            return {
                'case_property_id': row[0],
                'case_type': row[1],
                'crime_id': row[2],
                'mo_id': row[3],
                'status': row[4],
                'send_date': row[5],
                'fsl_date': row[6],
                'date_disposal': row[7],
                'release_date': row[8],
                'return_date': row[9],
                'date_custody': row[10],
                'date_sent_to_expert': row[11],
                'court_order_date': row[12],
                'forwarding_through': row[13],
                'court_name': row[14],
                'fsl_court_name': row[15],
                'cpr_court_name': row[16],
                'court_order_number': row[17],
                'fsl_no': row[18],
                'fsl_request_id': row[19],
                'report_received': row[20],
                'opinion': row[21],
                'opinion_furnished': row[22],
                'strength_of_evidence': row[23],
                'expert_type': row[24],
                'other_expert_type': row[25],
                'cpr_no': row[26],
                'direction_by_court': row[27],
                'details_disposal': row[28],
                'place_disposal': row[29],
                'release_order_no': row[30],
                'place_custody': row[31],
                'assign_custody': row[32],
                'property_received_back': row[33],
                'date_created': row[34],
                'date_modified': row[35]
            }
        return None
    
    def log_failed_record(self, case_property: Dict, reason: str, error_details: str = ""):
        """Log a failed record to the failed records log file"""
        failed_info = {
            'case_property_id': case_property.get('case_property_id'),
            'crime_id': case_property.get('crime_id'),
            'fsl_no': case_property.get('fsl_no'),
            'mo_id': case_property.get('mo_id'),
            'reason': reason,
            'error_details': error_details,
            'timestamp': datetime.now().isoformat(),
            'case_property_data': case_property
        }
        
        self.failed_log.write(f"\n{'='*80}\n")
        self.failed_log.write(f"CASE_PROPERTY_ID: {case_property.get('case_property_id')}\n")
        self.failed_log.write(f"CRIME_ID: {case_property.get('crime_id')}\n")
        self.failed_log.write(f"FSL_NO: {case_property.get('fsl_no')}\n")
        self.failed_log.write(f"MO_ID: {case_property.get('mo_id')}\n")
        self.failed_log.write(f"REASON: {reason}\n")
        if error_details:
            self.failed_log.write(f"ERROR: {error_details}\n")
        self.failed_log.write(f"Timestamp: {datetime.now().isoformat()}\n")
        self.failed_log.write(f"\nJSON Format:\n")
        self.failed_log.write(json.dumps(failed_info, indent=2, ensure_ascii=False, default=str))
        self.failed_log.write(f"\n")
        self.failed_log.flush()
    
    def log_invalid_crime_id(self, case_property: Dict, crime_id_str: str, chunk_range: str = ""):
        """Log a case property that failed due to invalid CRIME_ID (not found in crimes table)"""
        failure_info = {
            'case_property_id': case_property.get('case_property_id'),
            'crime_id': crime_id_str,
            'fsl_no': case_property.get('fsl_no'),
            'mo_id': case_property.get('mo_id'),
            'chunk': chunk_range,
            'timestamp': datetime.now().isoformat(),
            'case_property_data': case_property
        }
        
        self.invalid_crime_id_log.write(f"\n{'='*80}\n")
        self.invalid_crime_id_log.write(f"CASE_PROPERTY_ID: {case_property.get('case_property_id')}\n")
        self.invalid_crime_id_log.write(f"CRIME_ID: {crime_id_str}\n")
        self.invalid_crime_id_log.write(f"FSL_NO: {case_property.get('fsl_no')}\n")
        self.invalid_crime_id_log.write(f"MO_ID: {case_property.get('mo_id')}\n")
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
            self.duplicates_log.write(f"  {i}. CASE_PROPERTY_ID: {dup.get('case_property_id')}, CRIME_ID: {dup.get('crime_id')}, FSL_NO: {dup.get('fsl_no')}\n")
            self.duplicates_log.write(f"     Occurrence: #{dup.get('occurrence', 'N/A')}\n")
            self.duplicates_log.write(f"     First seen in: {dup['first_seen_in']}\n")
            self.duplicates_log.write(f"     Duplicate in: {dup['duplicate_in']}\n")
        
        # Also write JSON format for easy parsing
        self.duplicates_log.write(f"\nJSON Format:\n")
        self.duplicates_log.write(json.dumps(chunk_info, indent=2, ensure_ascii=False))
        self.duplicates_log.write(f"\n")
        
        self.duplicates_log.flush()
    
    def insert_media_files(self, case_property_id, media_files: List) -> int:
        """
        Insert or update media files for a case property
        Deletes existing media and inserts new ones (simple replace strategy)
        
        Args:
            case_property_id: MongoDB ObjectId (string) of the case property
            media_files: List of media file IDs (can be strings, dicts with file_id, or UUIDs)
        
        Returns:
            Number of media files inserted
        """
        if not case_property_id or not media_files:
            return 0
        
        import uuid
        
        try:
            # Delete existing media files for this case property
            self.db_cursor.execute(
                f"DELETE FROM {FSL_CASE_PROPERTY_MEDIA_TABLE} WHERE case_property_id = %s",
                (case_property_id,)
            )
            
            inserted_count = 0
            for media_item in media_files:
                # Handle different media file formats
                if isinstance(media_item, dict):
                    file_id = media_item.get('file_id') or media_item.get('FILE_ID') or media_item.get('id') or media_item.get('ID')
                    media_id_str = media_item.get('media_id') or media_item.get('MEDIA_ID')
                elif isinstance(media_item, str):
                    file_id = media_item if media_item.strip() else None
                    media_id_str = None
                else:
                    file_id = str(media_item) if media_item else None
                    media_id_str = None
                
                # Normalize file_id (empty strings to None - file_id can be NULL)
                if file_id:
                    file_id = file_id.strip() if isinstance(file_id, str) else file_id
                    if file_id == '':
                        file_id = None
                
                # Generate UUID for media_id (NOT NULL - always required)
                if media_id_str:
                    try:
                        media_id = uuid.UUID(media_id_str) if isinstance(media_id_str, str) else media_id_str
                    except (ValueError, TypeError):
                        media_id = uuid.uuid4()
                else:
                    media_id = uuid.uuid4()
                
                # Convert UUID to string for psycopg2 (PostgreSQL will cast it to UUID type)
                media_id_str_for_db = str(media_id)
                
                # Insert media file (file_id can be NULL, media_id is NOT NULL)
                self.db_cursor.execute(
                    f"INSERT INTO {FSL_CASE_PROPERTY_MEDIA_TABLE} (media_id, case_property_id, file_id) VALUES (%s::uuid, %s, %s)",
                    (media_id_str_for_db, case_property_id, file_id)
                )
                inserted_count += 1
            
            if inserted_count > 0:
                logger.trace(f"Inserted {inserted_count} media files for case_property_id={case_property_id}")
            
            return inserted_count
            
        except Exception as e:
            logger.error(f"Error inserting media files: {e}")
            return 0
    
    def insert_fsl_case_property(self, case_property: Dict, chunk_date_range: str = "") -> Tuple[bool, str]:
        """
        Insert or update single FSL case property into database with smart update logic
        Dates are always from API (never use CURRENT_TIMESTAMP)
        
        Behavior:
        - NEW DATA: If case_property_id doesn't exist → INSERT
        - EXISTING DATA: If exists → UPDATE (updates only changed fields)
        - Smart Update: Only updates fields that have changed, preserves existing values if API sends NULL
        - Media Files: Always replaced (delete old, insert new)
        
        Date Handling:
        - date_created and date_modified are always taken from API
        - If API provides dates, they are used (even if different from existing)
        - If API doesn't provide dates, they remain NULL
        
        Args:
            case_property: Transformed case property dict
            chunk_date_range: Date range for chunk tracking
        
        Returns:
            Tuple of (success: bool, operation: str) where operation is 'inserted', 'updated', 'no_change', or 'skipped'
        """
        case_property_id = case_property.get('case_property_id')
        crime_id = case_property.get('crime_id')
        original_crime_id = case_property.get('_original_crime_id')
        media_files = case_property.get('_media_files', [])
        
        # Validate case_property_id exists
        if not case_property_id:
            reason = 'missing_case_property_id'
            error_details = "Case property record missing CASE_PROPERTY_ID"
            logger.warning(f"⚠️  {error_details}")
            self.stats['total_records_failed'] += 1
            self.log_failed_record(case_property, reason, error_details)
            return False, reason
        
        # Validate crime_id is provided (NOT NULL constraint)
        if not crime_id:
            reason = 'missing_crime_id'
            error_details = f"CRIME_ID is required (NOT NULL) but was not provided"
            logger.warning(f"⚠️  {error_details}, skipping case property")
            self.stats['total_records_failed'] += 1
            self.stats['total_records_failed_crime_id'] += 1
            self.log_failed_record(case_property, reason, error_details)
            self.log_invalid_crime_id(case_property, original_crime_id or 'NULL', chunk_date_range)
            return False, reason
        
        try:
            logger.trace(f"Processing case property: case_property_id={case_property_id}, crime_id={crime_id}")
            
            # Check if case property already exists (based on primary key)
            if self.case_property_exists(case_property_id):
                # Get existing record to compare
                existing = self.get_existing_case_property(case_property_id)
                if not existing:
                    logger.warning(f"⚠️  Case property exists check returned True but fetch returned None")
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
                    
                    # Define all fields to check (excluding primary key)
                    fields_to_check = [
                        ('case_type',), ('crime_id',), ('mo_id',), ('status',),
                        ('send_date',), ('fsl_date',), ('date_disposal',), ('release_date',),
                        ('return_date',), ('date_custody',), ('date_sent_to_expert',), ('court_order_date',),
                        ('forwarding_through',), ('court_name',), ('fsl_court_name',), ('cpr_court_name',),
                        ('court_order_number',), ('fsl_no',), ('fsl_request_id',), ('report_received',),
                        ('opinion',), ('opinion_furnished',), ('strength_of_evidence',), ('expert_type',),
                        ('other_expert_type',), ('cpr_no',), ('direction_by_court',), ('details_disposal',),
                        ('place_disposal',), ('release_order_no',), ('place_custody',), ('assign_custody',),
                        ('property_received_back',), ('date_created',), ('date_modified',)
                    ]
                    
                    for db_field in fields_to_check:
                        db_field = db_field[0] if isinstance(db_field, tuple) else db_field
                        existing_val = existing.get(db_field)
                        new_val = case_property.get(db_field)
                        
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
                            UPDATE {FSL_CASE_PROPERTY_TABLE} SET
                                {', '.join(update_fields)}
                            WHERE case_property_id = %s
                        """
                        update_values.append(case_property_id)
                        self.db_cursor.execute(update_query, tuple(update_values))
                        self.stats['total_records_updated'] += 1
                        logger.debug(f"Updated case property: case_property_id={case_property_id} ({len(changes)} fields changed)")
                        logger.trace(f"Changes: {', '.join(changes)}")
                        
                        # Handle media files (always replace)
                        media_count = self.insert_media_files(case_property_id, media_files)
                        if media_count > 0:
                            self.stats['total_media_updated'] += media_count
                        
                        self.db_conn.commit()
                        logger.trace(f"Transaction committed for updated case property")
                        return True, 'updated'
                    else:
                        # No changes needed, but still update media files if they changed
                        media_count = self.insert_media_files(case_property_id, media_files)
                        if media_count > 0:
                            self.stats['total_media_updated'] += media_count
                            self.db_conn.commit()
                            return True, 'updated'  # Media updated even if main record didn't change
                        
                        self.stats['total_records_no_change'] += 1
                        logger.trace(f"No changes needed for case property (all fields match or preserved)")
                        return True, 'no_change'
                else:
                    # Exists check returned True but couldn't fetch - treat as new insert
                    logger.warning(f"⚠️  Case property exists but couldn't fetch, treating as new insert")
                    # Fall through to insert logic
            else:
                # Insert new case property
                logger.trace(f"Inserting new case property: case_property_id={case_property_id}, crime_id={crime_id}")
                insert_query = f"""
                    INSERT INTO {FSL_CASE_PROPERTY_TABLE} (
                        case_property_id, case_type, crime_id, mo_id, status,
                        send_date, fsl_date, date_disposal, release_date, return_date,
                        date_custody, date_sent_to_expert, court_order_date,
                        forwarding_through, court_name, fsl_court_name, cpr_court_name,
                        court_order_number, fsl_no, fsl_request_id, report_received,
                        opinion, opinion_furnished, strength_of_evidence, expert_type,
                        other_expert_type, cpr_no, direction_by_court, details_disposal,
                        place_disposal, release_order_no, place_custody, assign_custody,
                        property_received_back, date_created, date_modified
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """
                self.db_cursor.execute(insert_query, (
                    case_property.get('case_property_id'),
                    case_property.get('case_type'),
                    case_property.get('crime_id'),
                    case_property.get('mo_id'),
                    case_property.get('status'),
                    case_property.get('send_date'),
                    case_property.get('fsl_date'),
                    case_property.get('date_disposal'),
                    case_property.get('release_date'),
                    case_property.get('return_date'),
                    case_property.get('date_custody'),
                    case_property.get('date_sent_to_expert'),
                    case_property.get('court_order_date'),
                    case_property.get('forwarding_through'),
                    case_property.get('court_name'),
                    case_property.get('fsl_court_name'),
                    case_property.get('cpr_court_name'),
                    case_property.get('court_order_number'),
                    case_property.get('fsl_no'),
                    case_property.get('fsl_request_id'),
                    case_property.get('report_received'),
                    case_property.get('opinion'),
                    case_property.get('opinion_furnished'),
                    case_property.get('strength_of_evidence'),
                    case_property.get('expert_type'),
                    case_property.get('other_expert_type'),
                    case_property.get('cpr_no'),
                    case_property.get('direction_by_court'),
                    case_property.get('details_disposal'),
                    case_property.get('place_disposal'),
                    case_property.get('release_order_no'),
                    case_property.get('place_custody'),
                    case_property.get('assign_custody'),
                    case_property.get('property_received_back'),
                    case_property.get('date_created'),  # From API (or NULL)
                    case_property.get('date_modified')  # From API (or NULL)
                ))
                self.stats['total_records_inserted'] += 1
                
                # Insert media files
                media_count = self.insert_media_files(case_property_id, media_files)
                if media_count > 0:
                    self.stats['total_media_inserted'] += media_count
                
                logger.debug(f"Inserted case property: case_property_id={case_property_id}, crime_id={crime_id}")
                logger.trace(f"Insert query executed for case property")
                self.db_conn.commit()
                logger.trace(f"Transaction committed for inserted case property")
                return True, 'inserted'
            
        except psycopg2.IntegrityError as e:
            self.db_conn.rollback()
            reason = 'integrity_error'
            error_details = str(e)
            logger.warning(f"⚠️  Integrity error for case property: {e}")
            self.stats['total_records_failed'] += 1
            self.log_failed_record(case_property, reason, error_details)
            return False, reason
        except Exception as e:
            self.db_conn.rollback()
            reason = 'error'
            error_details = str(e)
            logger.error(f"❌ Error inserting case property: {e}")
            self.stats['total_records_failed'] += 1
            self.stats['errors'].append(f"Case property case_property_id={case_property_id}: {str(e)}")
            self.log_failed_record(case_property, reason, error_details)
            return False, reason
    
    def process_date_range(self, from_date: str, to_date: str, table_columns: Set[str] = None):
        """Process FSL case property records for a specific date range"""
        chunk_range = f"{from_date} to {to_date}"
        logger.info(f"📅 Processing: {chunk_range}")
        
        # Fetch FSL case property from API
        case_property_raw = self.fetch_fsl_case_property_api(from_date, to_date)
        
        if case_property_raw is None:
            logger.error(f"❌ Failed to fetch FSL case property for {chunk_range}")
            self.log_db_chunk(from_date, to_date, 0, [], [], [], [], [], error="API fetch failed")
            return
        
        if not case_property_raw:
            logger.info(f"ℹ️  No FSL case property records found for {chunk_range}")
            self.log_db_chunk(from_date, to_date, 0, [], [], [], [], [], error="No case property records in API response")
            return
        
        # Check for schema evolution if we got data
        if table_columns is not None and len(case_property_raw) > 0:
            # Check for new fields in first record
            new_fields = self.detect_new_fields(case_property_raw[0], table_columns)
            if new_fields:
                logger.info(f"🔍 New fields detected in API response: {list(new_fields.keys())}")
                # Add new columns to table
                for api_field, db_column in new_fields.items():
                    if self.add_column_to_table(db_column):
                        # Update table_columns set
                        table_columns.add(db_column)
                # Update existing records from start_date to current chunk end_date
                self.update_existing_records_with_new_fields(new_fields, to_date)
        
        # Transform and insert each case property
        self.stats['total_records_fetched'] += len(case_property_raw)
        logger.trace(f"Processing {len(case_property_raw)} FSL case property records for chunk {chunk_range}")
        
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
        for idx, case_property_record in enumerate(case_property_raw, 1):
            logger.trace(f"Processing record {idx}/{len(case_property_raw)}: CASE_PROPERTY_ID={case_property_record.get('CASE_PROPERTY_ID')}, CRIME_ID={case_property_record.get('CRIME_ID')}")
            case_property = self.transform_fsl_case_property(case_property_record)
            
            # Check if transform returned empty (missing required fields)
            if not case_property:
                logger.warning(f"⚠️  Transform returned empty for record {idx}, skipping")
                self.stats['total_records_failed'] += 1
                failed_keys.append(f"record_{idx}")
                reason = 'transform_failed'
                if reason not in failed_reasons:
                    failed_reasons[reason] = []
                failed_reasons[reason].append(f"record_{idx}")
                continue
            
            case_property_id = case_property.get('case_property_id')
            crime_id = case_property.get('crime_id')
            original_crime_id = case_property.get('_original_crime_id')
            
            # Check if crime_id is provided (NOT NULL constraint)
            if not crime_id:
                logger.warning(f"⚠️  Case property missing CRIME_ID (required NOT NULL), skipping")
                self.stats['total_records_failed'] += 1
                self.stats['total_records_failed_crime_id'] += 1
                failed_keys.append(f"{case_property_id or 'unknown'}")
                reason = 'missing_crime_id'
                if reason not in failed_reasons:
                    failed_reasons[reason] = []
                failed_reasons[reason].append(original_crime_id or 'NULL')
                invalid_crime_ids_in_chunk.append({
                    'case_property_id': case_property_id,
                    'crime_id': original_crime_id or 'NULL',
                    'fsl_no': case_property.get('fsl_no')
                })
                self.log_invalid_crime_id(case_property, original_crime_id or 'NULL', chunk_range)
                continue
            
            # Create unique key for tracking duplicates (use case_property_id)
            unique_key = str(case_property_id) if case_property_id else f"unknown_{idx}"
            
            # Track occurrences for duplicate reporting (but don't skip - process all)
            if unique_key in seen_keys:
                # This is a duplicate occurrence - track it but still process
                occurrence_count = key_occurrences.get(unique_key, 1) + 1
                key_occurrences[unique_key] = occurrence_count
                
                duplicates_in_chunk.append({
                    'case_property_id': case_property_id,
                    'crime_id': crime_id,
                    'fsl_no': case_property.get('fsl_no'),
                    'occurrence': occurrence_count,
                    'first_seen_in': seen_keys[unique_key],
                    'duplicate_in': chunk_range
                })
                self.stats['total_duplicates'] += 1
                logger.info(f"⚠️  Duplicate case property found in chunk {chunk_range} (occurrence #{occurrence_count}) - Will process to update record")
                logger.trace(f"Duplicate details - First seen: {seen_keys[unique_key]}, Current occurrence: {occurrence_count}")
            else:
                seen_keys[unique_key] = chunk_range
                key_occurrences[unique_key] = 1
                logger.trace(f"New case property key seen: {unique_key} in chunk {chunk_range}")
            
            # IMPORTANT: Process ALL records, even duplicates
            # If same key appears multiple times, each occurrence might have updated data
            # The smart update logic will handle whether to actually update or not
            success, operation = self.insert_fsl_case_property(case_property, chunk_range)
            logger.trace(f"Operation result for case property: success={success}, operation={operation}")
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
            logger.warning(f"⚠️  Found {len(invalid_crime_ids_in_chunk)} case property records with invalid CRIME_IDs in chunk {chunk_range}")
            # Extract unique CRIME_IDs
            unique_crime_ids = list(set([f['crime_id'] for f in invalid_crime_ids_in_chunk if f.get('crime_id')]))
            logger.warning(f"   Invalid CRIME_IDs: {unique_crime_ids}")
        
        # Log database operations for this chunk
        logger.trace(f"Chunk summary - Inserted: {len(inserted_keys)}, Updated: {len(updated_keys)}, No Change: {len(no_change_keys)}, Failed: {len(failed_keys)}, Duplicates: {len(duplicates_in_chunk)}, Invalid CRIME_IDs: {len(invalid_crime_ids_in_chunk)}")
        self.log_db_chunk(from_date, to_date, len(case_property_raw), inserted_keys, updated_keys, 
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
        self.api_log.write(f"Total Records Fetched: {self.stats['total_records_fetched']}\n")
        self.api_log.write(f"Failed API Calls: {self.stats['failed_api_calls']}\n")
        self.api_log.write(f"Total Chunks Processed: {self.stats['total_api_calls'] + self.stats['failed_api_calls']}\n")
        
        # DB log summary
        self.db_log.write(f"\n\n{'='*80}\n")
        self.db_log.write(f"SUMMARY\n")
        self.db_log.write(f"{'='*80}\n")
        self.db_log.write(f"Total Records Fetched from API: {self.stats['total_records_fetched']}\n")
        self.db_log.write(f"Total Records Inserted (New): {self.stats['total_records_inserted']}\n")
        self.db_log.write(f"Total Records Updated (Existing): {self.stats['total_records_updated']}\n")
        self.db_log.write(f"Total Records No Change: {self.stats['total_records_no_change']}\n")
        self.db_log.write(f"Total Records Failed: {self.stats['total_records_failed']}\n")
        self.db_log.write(f"  - Failed due to Invalid CRIME_ID: {self.stats['total_records_failed_crime_id']}\n")
        self.db_log.write(f"Total Records Duplicates (Processed): {self.stats['total_duplicates']}\n")
        self.db_log.write(f"Total Operations (Inserted + Updated + No Change): {self.stats['total_records_inserted'] + self.stats['total_records_updated'] + self.stats['total_records_no_change']}\n")
        db_total = self.stats.get('db_total_count', self.stats['total_records_inserted'])
        self.db_log.write(f"Total Unique Records in Database: {db_total}\n")
        self.db_log.write(f"Note: Updated count includes multiple updates (same key in multiple chunks or same chunk)\n")
        self.db_log.write(f"Note: Duplicates are records that appear multiple times within the same chunk - ALL are processed for updates\n")
        if self.stats['total_records_fetched'] > 0:
            coverage = ((self.stats['total_records_inserted'] + self.stats['total_records_updated'] + self.stats['total_records_no_change']) / self.stats['total_records_fetched']) * 100
            self.db_log.write(f"Coverage: {coverage:.2f}%\n")
        self.db_log.write(f"Errors: {len(self.stats['errors'])}\n")
        
        # Failed records log summary
        self.failed_log.write(f"\n\n{'='*80}\n")
        self.failed_log.write(f"SUMMARY\n")
        self.failed_log.write(f"{'='*80}\n")
        self.failed_log.write(f"Total Failed Records: {self.stats['total_records_failed']}\n")
        self.failed_log.write(f"Note: Failed records are those that could not be inserted or updated\n")
        self.failed_log.write(f"Check individual entries above for specific reasons\n")
        
        # Invalid CRIME_ID log summary
        self.invalid_crime_id_log.write(f"\n\n{'='*80}\n")
        self.invalid_crime_id_log.write(f"SUMMARY\n")
        self.invalid_crime_id_log.write(f"{'='*80}\n")
        self.invalid_crime_id_log.write(f"Total Records Failed Due to Invalid CRIME_ID: {self.stats['total_records_failed_crime_id']}\n")
        self.invalid_crime_id_log.write(f"\n")
        self.invalid_crime_id_log.write(f"Note: These case property records could not be inserted/updated because their CRIME_ID\n")
        self.invalid_crime_id_log.write(f"      was not found in the crimes table. Please ensure these crimes are loaded\n")
        self.invalid_crime_id_log.write(f"      in the crimes table first.\n")
        
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
        logger.info("🚀 DOPAMAS ETL Pipeline - FSL Case Property API")
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
            table_columns = self.get_table_columns(FSL_CASE_PROPERTY_TABLE)
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
            self.db_cursor.execute(f"SELECT COUNT(*) FROM {FSL_CASE_PROPERTY_TABLE}")
            db_records_count = self.db_cursor.fetchone()[0]
            
            # Store for summary
            self.stats['db_total_count'] = db_records_count
            
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
            logger.info(f"  Total Records Fetched: {self.stats['total_records_fetched']}")
            logger.info(f"")
            logger.info(f"💾 TO DATABASE:")
            logger.info(f"  Total Inserted (New): {self.stats['total_records_inserted']}")
            logger.info(f"  Total Updated:        {self.stats['total_records_updated']}")
            logger.info(f"  Total No Change:      {self.stats['total_records_no_change']}")
            logger.info(f"  Total Failed:         {self.stats['total_records_failed']}")
            logger.info(f"    - Invalid CRIME_ID:   {self.stats['total_records_failed_crime_id']}")
            logger.info(f"  Total in DB:          {db_records_count}")
            logger.info(f"")
            logger.info(f"🔄 DUPLICATES:")
            logger.info(f"  Total Duplicate Occurrences (Processed): {self.stats['total_duplicates']}")
            logger.info(f"  Note: All duplicates are processed to allow updates")
            logger.info(f"")
            logger.info(f"⚠️  INVALID CRIME_ID:")
            logger.info(f"  Records Failed Due to Invalid CRIME_ID: {self.stats['total_records_failed_crime_id']}")
            logger.info(f"  Check logs/fsl_case_property_invalid_crime_id_*.log for details")
            logger.info(f"")
            logger.info(f"📊 COVERAGE:")
            if self.stats['total_records_fetched'] > 0:
                coverage = ((self.stats['total_records_inserted'] + self.stats['total_records_updated'] + self.stats['total_records_no_change']) / self.stats['total_records_fetched']) * 100
                logger.info(f"  API → DB Coverage:   {coverage:.2f}%")
            logger.info(f"")
            logger.info(f"📈 SUMMARY:")
            logger.info(f"  Total from API:       {self.stats['total_records_fetched']}")
            logger.info(f"  Inserted + Updated:   {self.stats['total_records_inserted'] + self.stats['total_records_updated']}")
            logger.info(f"  Duplicate Occurrences: {self.stats['total_duplicates']} (all processed)")
            logger.info(f"  Failed:               {self.stats['total_records_failed']}")
            logger.info(f"")
            logger.info(f"💡 NOTE:")
            logger.info(f"  - Same case property can appear multiple times in API response")
            logger.info(f"  - Each occurrence is processed to capture any data updates")
            logger.info(f"  - Smart update logic ensures only changed fields are updated")
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
    etl = FSLCasePropertyETL()
    success = etl.run()
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()


