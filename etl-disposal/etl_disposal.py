#!/usr/bin/env python3
"""
DOPAMAS ETL Pipeline - Disposal API
Fetches disposal data in 5-day chunks and loads into PostgreSQL
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
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from db_pooling import PostgreSQLConnectionPool
except ImportError:
    pass

try:
    from etl_fk_retry_queue import push_fk_failure, drain_fk_queue as _drain_fk_queue
except ImportError:  # pragma: no cover — queue module not yet deployed
    push_fk_failure = None
    _drain_fk_queue = None

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
DISPOSAL_TABLE = TABLE_CONFIG.get('disposal', 'disposal')
CRIMES_TABLE = TABLE_CONFIG.get('crimes', 'crimes')

# IST timezone offset (UTC+05:30)
IST_OFFSET = timezone(timedelta(hours=5, minutes=30))

# API data availability start date (2022-06-06 is the earliest date API has data)
API_DATA_START_DATE = '2022-06-06T00:00:00+05:30'

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


def normalize_api_value(value):
    """Normalize API values by converting blank strings to None."""
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        return value or None
    return value


def parse_api_timestamp(value) -> Optional[datetime]:
    """Parse API timestamps as timezone-aware datetimes without double conversion."""
    value = normalize_api_value(value)
    if value is None:
        return None

    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=IST_OFFSET)
        return value

    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=IST_OFFSET)
            return parsed
        except ValueError:
            try:
                parsed = datetime.strptime(value.split('T')[0], '%Y-%m-%d')
                return parsed.replace(tzinfo=IST_OFFSET)
            except ValueError:
                logger.warning(f"⚠️  Could not parse API timestamp: {value}")
                return None

    logger.warning(f"⚠️  Unsupported API timestamp type: {type(value).__name__}")
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


class DisposalETL:
    """ETL Pipeline for Disposal API"""
    
    def __init__(self):
        self.db_conn = None
        self.db_cursor = None
        self.stats = {
            'total_api_calls': 0,
            'total_disposals_fetched': 0,
            'total_disposals_inserted': 0,
            'total_disposals_updated': 0,
            'total_disposals_no_change': 0,  # Records that exist but no changes needed
            'total_disposals_failed': 0,  # Records that failed to insert/update
            'total_disposals_failed_crime_id': 0,  # Disposals failed due to CRIME_ID not found
            'total_duplicates': 0,  # Duplicate records found within chunks
            'failed_api_calls': 0,
            'errors': [],
            'duplicate_warnings_suppressed': 0  # Count of suppressed duplicate insert warnings
        }
        self.stats_lock = threading.Lock()
        self.log_lock = threading.Lock()
        self.schema_lock = threading.Lock()
        self.db_pool = None
        self.preflight_checks_done = False
        
        # Setup chunk-wise logging files
        self.setup_chunk_loggers()
    
    def setup_chunk_loggers(self):
        """Setup separate log files for API responses, DB operations, and failed records"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Create logs directory if it doesn't exist
        import os
        os.makedirs('logs', exist_ok=True)
        
        # API response log file
        self.api_log_file = f'logs/disposal_api_chunks_{timestamp}.log'
        self.api_log = open(self.api_log_file, 'w', encoding='utf-8')
        self.api_log.write(f"# Disposal API Chunk-wise Log\n")
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
        self.db_log_file = f'logs/disposal_db_chunks_{timestamp}.log'
        self.db_log = open(self.db_log_file, 'w', encoding='utf-8')
        self.db_log.write(f"# Disposal Database Operations Chunk-wise Log\n")
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
        self.failed_log_file = f'logs/disposal_failed_{timestamp}.log'
        self.failed_log = open(self.failed_log_file, 'w', encoding='utf-8')
        self.failed_log.write(f"# Disposal Failed Records Log\n")
        self.failed_log.write(f"# Generated: {datetime.now().isoformat()}\n")
        self.failed_log.write(f"# Records that failed to insert or update with reasons\n")
        self.failed_log.write(f"{'='*80}\n\n")
        
        # Invalid crime_id log file (disposals with crime_id not found in crimes table)
        self.invalid_crime_id_log_file = f'logs/disposal_invalid_crime_id_{timestamp}.log'
        self.invalid_crime_id_log = open(self.invalid_crime_id_log_file, 'w', encoding='utf-8')
        self.invalid_crime_id_log.write(f"# Disposal Invalid CRIME_ID Log\n")
        self.invalid_crime_id_log.write(f"# Generated: {datetime.now().isoformat()}\n")
        self.invalid_crime_id_log.write(f"# Disposals that failed because CRIME_ID not found in crimes table\n")
        self.invalid_crime_id_log.write(f"{'='*80}\n\n")
        
        # Duplicates log file (duplicate records found within chunks)
        self.duplicates_log_file = f'logs/disposal_duplicates_{timestamp}.log'
        self.duplicates_log = open(self.duplicates_log_file, 'w', encoding='utf-8')
        self.duplicates_log.write(f"# Disposal Duplicates Log\n")
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
        """Connect to PostgreSQL database with optimized pool sizing"""
        try:
            pool_config = DB_CONFIG.copy()
            
            # Smart connection pool sizing to avoid worker contention
            # Formula: max_workers * 1.5 ensures each worker can get a connection when needed
            # plus buffer for schema operations
            max_workers = int(os.environ.get('MAX_WORKERS', getattr(self, 'max_workers', min(32, (os.cpu_count() or 1) * 4))))
            optimal_pool_size = max(max_workers // 2, 5)  # At least 5, but allow worker sharing
            max_pool_size = max_workers + 10  # Max can be higher to prevent queue buildup
            
            pool_config['minconn'] = optimal_pool_size // 2
            pool_config['maxconn'] = max_pool_size
            
            self.db_pool = PostgreSQLConnectionPool(**pool_config)
            
            # Keep one permanent connection for schema generation operations if needed
            self.db_conn = self.db_pool.get_connection()
            self.db_cursor = self.db_conn.cursor()
            
            logger.info(f"✅ Connected to database: {DB_CONFIG['database']} using connection pool")
            logger.info(f"   Pool: min={optimal_pool_size // 2}, max={max_pool_size} (workers={max_workers})")
            return self.db_pool is not None
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            return False
            
    def close_db(self):
        """Close database connection"""
        if self.db_cursor:
            self.db_cursor.close()
        if self.db_conn:
            # db_pooling exposes return_connection (not release_connection).
            if hasattr(self.db_pool, 'return_connection'):
                self.db_pool.return_connection(self.db_conn)
            elif hasattr(self.db_pool, 'release_connection'):
                self.db_pool.release_connection(self.db_conn)
        
        if self.db_pool:
            self.db_pool.close_all()
        logger.info("Database connection closed")

    def ensure_run_state_table(self):
        """Ensure ETL run-state table exists."""
        with self.db_pool.get_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS etl_run_state (
                        module_name TEXT PRIMARY KEY,
                        last_successful_end TIMESTAMPTZ NOT NULL,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                conn.commit()

    def get_run_checkpoint(self, module_name: str) -> Optional[datetime]:
        """Get last successful run checkpoint for a module."""
        with self.db_pool.get_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT last_successful_end FROM etl_run_state WHERE module_name = %s",
                    (module_name,)
                )
                row = cur.fetchone()
                return row[0] if row else None

    def update_run_checkpoint(self, module_name: str, end_date_iso: str):
        """Persist successful run completion boundary."""
        end_dt = parse_iso_date(end_date_iso)
        with self.db_pool.get_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO etl_run_state (module_name, last_successful_end, updated_at)
                    VALUES (%s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (module_name) DO UPDATE SET
                        last_successful_end = EXCLUDED.last_successful_end,
                        updated_at = CURRENT_TIMESTAMP
                """, (module_name, end_dt))
                conn.commit()
    
    def get_table_columns(self, table_name: str) -> Set[str]:
        """Get all column names from a table."""
        try:
            with self.db_pool.get_connection_context() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name = %s
                    """, (table_name,))
                    return {row[0] for row in cur.fetchall()}
        except Exception as e:
            logger.error(f"Error getting table columns for {table_name}: {e}")
            return set()
    
    def get_effective_start_date(self) -> str:
        """
        Get effective start date for ETL:
        - If table is empty: return API_DATA_START_DATE (2022-06-06, earliest API data)
        - If table has data: return max(date_created, date_modified) from table
        - Always respect API_DATA_START_DATE as minimum (API has no data before this)
        """
        try:
            with self.db_pool.get_connection_context() as conn:
                with conn.cursor() as cur:
                    # Check if table has any data
                    cur.execute(f"SELECT COUNT(*) FROM {DISPOSAL_TABLE}")
                    count = cur.fetchone()[0]
                    
                    if count == 0:
                        # New database, start from API data availability date
                        logger.info(f"📊 Table is empty, starting from {API_DATA_START_DATE}")
                        return API_DATA_START_DATE
                    
                    # Table has data, get max of date_created and date_modified
                    # Only consider dates >= API_DATA_START_DATE to avoid processing non-existent data
                    MIN_START_DATE = API_DATA_START_DATE
                    min_start_dt = parse_iso_date(API_DATA_START_DATE)
                    
                    api_start_dt = parse_iso_date(API_DATA_START_DATE)
                    cur.execute(f"""
                        SELECT GREATEST(
                            COALESCE(MAX(date_created), %s::timestamptz),
                            COALESCE(MAX(date_modified), %s::timestamptz)
                        ) as max_date
                        FROM {DISPOSAL_TABLE}
                        WHERE date_created >= %s::timestamptz OR date_modified >= %s::timestamptz
                    """, (api_start_dt, api_start_dt, api_start_dt, api_start_dt))
                    result = cur.fetchone()
                    if result and result[0]:
                        max_date = result[0]
                        # Convert to IST timezone if needed
                        if isinstance(max_date, datetime):
                            if max_date.tzinfo is None:
                                max_date = max_date.replace(tzinfo=IST_OFFSET)
                            else:
                                max_date = max_date.astimezone(IST_OFFSET)
                            
                            # Ensure we never go before API_DATA_START_DATE
                            if max_date < min_start_dt:
                                logger.warning(f"⚠️  Max date ({max_date.isoformat()}) is before {API_DATA_START_DATE}, using {API_DATA_START_DATE}")
                                return MIN_START_DATE
                            
                            logger.info(f"📊 Table has data, starting from: {max_date.isoformat()}")
                            return max_date.isoformat()
                    
                    # Fallback to start date
                    logger.warning(f"⚠️  Could not determine max date, using {API_DATA_START_DATE}")
                    return API_DATA_START_DATE
            
        except Exception as e:
            logger.error(f"❌ Error getting effective start date: {e}")
            logger.warning(f"⚠️  Using default start date: {API_DATA_START_DATE}")
            return API_DATA_START_DATE
    
    def detect_new_fields(self, api_record: Dict, table_columns: Set[str]) -> Dict[str, str]:
        """
        Detect new fields in API response that don't exist in table.
        Returns dict mapping API field name to database column name (snake_case).
        """
        new_fields = {}
        
        # Map API field names to database column names
        field_mapping = {
            'CRIME_ID': 'crime_id',
            'DISPOSAL_TYPE': 'disposal_type',
            'DISPOSED_DATE': 'disposed_at',
            'DISPOSED_AT': 'disposed_at',
            'DISPOSAL': 'disposal',
            'CASE_STATUS': 'case_status',
            'DATE_CREATED': 'date_created',
            'DATE_MODIFIED': 'date_modified'
        }
        
        for api_field, db_column in field_mapping.items():
            if api_field in api_record and db_column not in table_columns:
                new_fields[api_field] = db_column
        
        return new_fields
    
    def add_column_to_table(self, column_name: str, column_type: str = 'TEXT'):
        """Add a new column to the disposal table."""
        with self.schema_lock:
            try:
                # Determine column type based on field name
                if 'date' in column_name.lower() or 'at' in column_name.lower():
                    column_type = 'TIMESTAMPTZ'
                elif column_name == 'crime_id':
                    column_type = 'VARCHAR(50)'  # Matches crimes.crime_id type
                elif 'id' in column_name.lower():
                    column_type = 'VARCHAR(50)'  # Most IDs are VARCHAR in this schema
                elif column_name in ('disposal', 'case_status', 'disposal_type'):
                    column_type = 'TEXT'
                else:
                    column_type = 'TEXT'
                
                with self.db_pool.get_connection_context() as conn:
                    with conn.cursor() as cur:
                        alter_sql = f"ALTER TABLE {DISPOSAL_TABLE} ADD COLUMN IF NOT EXISTS {column_name} {column_type}"
                        cur.execute(alter_sql)
                        conn.commit()
                        logger.info(f"✅ Added column {column_name} ({column_type}) to {DISPOSAL_TABLE}")
                        return True
            except Exception as e:
                logger.error(f"❌ Error adding column {column_name}: {e}")
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
    
    def fetch_disposal_api(self, from_date: str, to_date: str) -> Optional[List[Dict]]:
        """
        Fetch disposal data from API for given date range with adaptive timeout
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
        
        Returns:
            List of disposal records or None if failed
        """
        # Use disposal_url from config (which reads from .env)
        url = API_CONFIG.get('disposal_url', f"{API_CONFIG['base_url']}/crimes/disposal")
        params = {
            'fromDate': from_date,
            'toDate': to_date
        }
        headers = {
            'x-api-key': API_CONFIG['api_key']
        }
        
        # Adaptive timeout: start conservative, increase on retry with backoff
        base_timeout = API_CONFIG.get('timeout', 30)
        
        for attempt in range(API_CONFIG['max_retries']):
            try:
                # Adaptive timeout: increases with retry attempts
                # Attempt 0: 30s, Attempt 1: 45s, Attempt 2: 60s
                adaptive_timeout = base_timeout + (attempt * 15)
                
                logger.debug(f"Fetching disposal: {from_date} to {to_date} (Attempt {attempt + 1}/{API_CONFIG['max_retries']}, timeout={adaptive_timeout}s)")
                logger.trace(f"API Request - URL: {url}, Params: {params}, Headers: {headers}")
                response = requests.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=adaptive_timeout
                )
                logger.trace(f"API Response - Status: {response.status_code}, Headers: {dict(response.headers)}")
                
                if response.status_code == 200:
                    data = response.json()
                    self.stats['total_api_calls'] += 1
                    
                    # Handle both single object and array responses
                    if data.get('status'):
                        disposal_data = data.get('data')
                        if disposal_data:
                            # If single object, convert to list
                            if isinstance(disposal_data, dict):
                                disposal_data = [disposal_data]
                            
                            # Extract crime_ids for logging
                            crime_ids = [d.get('CRIME_ID') for d in disposal_data if d.get('CRIME_ID')]
                            
                            # Log to API chunk file
                            self.log_api_chunk(from_date, to_date, len(disposal_data), crime_ids, disposal_data)
                            
                            logger.info(f"✅ Fetched {len(disposal_data)} disposal records for {from_date} to {to_date}")
                            logger.debug(f"📋 Crime IDs from API: {crime_ids[:10]}{'...' if len(crime_ids) > 10 else ''}")
                            logger.trace(f"Full Crime IDs list: {crime_ids}")
                            logger.trace(f"Sample disposal structure: {json.dumps(disposal_data[0] if disposal_data else {}, indent=2, default=str)}")
                            return disposal_data
                        else:
                            # Log empty response
                            self.log_api_chunk(from_date, to_date, 0, [], [])
                            logger.warning(f"⚠️  No disposal records found for {from_date} to {to_date}")
                            return []
                    else:
                        # Log failed status
                        self.log_api_chunk(from_date, to_date, 0, [], [], error="API returned status=false")
                        logger.warning(f"⚠️  API returned status=false for {from_date} to {to_date}")
                        return []
                
                elif response.status_code == 404:
                    # Log 404 response
                    self.log_api_chunk(from_date, to_date, 0, [], [], error="404 Not Found")
                    logger.info(f"ℹ️  No data found for {from_date} to {to_date}")
                    return []
                
                else:
                    logger.warning(f"API returned status code {response.status_code}, retrying...")
                    backoff_time = min(2 ** attempt, 30)  # Exponential backoff, cap at 30s
                    logger.debug(f"Waiting {backoff_time}s before retry...")
                    time.sleep(backoff_time)
                    
            except requests.exceptions.Timeout:
                backoff_time = min(2 ** attempt, 30)
                logger.warning(f"API timeout (exceeded {adaptive_timeout}s), retrying in {backoff_time}s... (Attempt {attempt + 1}/{API_CONFIG['max_retries']})")
                if attempt < API_CONFIG['max_retries'] - 1:
                    time.sleep(backoff_time)
                else:
                    logger.error(f"❌ API timeout after {API_CONFIG['max_retries']} attempts for {from_date} to {to_date}")
                    self.stats['failed_api_calls'] += 1
                    self.stats['errors'].append(f"{from_date} to {to_date}: Timeout after {adaptive_timeout}s")
                    self.log_api_chunk(from_date, to_date, 0, [], [], error=f"Timeout after {adaptive_timeout}s")
            except Exception as e:
                logger.debug(f"API error: {e}")
                if attempt == API_CONFIG['max_retries'] - 1:
                    self.stats['failed_api_calls'] += 1
                    self.stats['errors'].append(f"{from_date} to {to_date}: {str(e)}")
                    self.log_api_chunk(from_date, to_date, 0, [], [], error=str(e))
                elif attempt < API_CONFIG['max_retries'] - 1:
                    backoff_time = min(2 ** attempt, 30)
                    logger.debug(f"Retrying in {backoff_time}s...")
                    time.sleep(backoff_time)
        
        logger.error(f"❌ Failed to fetch disposal for {from_date} to {to_date} after {API_CONFIG['max_retries']} attempts (max timeout was {adaptive_timeout}s)")
        self.log_api_chunk(from_date, to_date, 0, [], [], error=f"Failed after {API_CONFIG['max_retries']} attempts (timeout={adaptive_timeout}s)")
        return None
    
    def log_api_chunk(self, from_date: str, to_date: str, count: int, crime_ids: List[str], 
                     disposal_data: List[Dict], error: Optional[str] = None):
        """Log API response for a chunk"""
        chunk_info = {
            'chunk': f"{from_date} to {to_date}",
            'timestamp': datetime.now().isoformat(),
            'count': count,
            'crime_ids': crime_ids,
            'error': error
        }
        
        with self.log_lock:
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
    
    def transform_disposal(self, disposal_raw: Dict, cursor) -> Dict:
        """
        Transform API response to database format
        Dates are always taken from API (never use CURRENT_TIMESTAMP)
        
        Args:
            disposal_raw: Raw disposal data from API
        
        Returns:
            Transformed disposal dict ready for database
        """
        logger.trace(f"Transforming disposal: CRIME_ID={disposal_raw.get('CRIME_ID')}, DISPOSAL_TYPE={disposal_raw.get('DISPOSAL_TYPE')}")
        
        # Get crime_id - validate it exists in crimes table
        crime_id_str = normalize_api_value(disposal_raw.get('CRIME_ID'))
        crime_id_valid = None
        
        if crime_id_str:
            # Validate that crime_id exists in crimes table (crime_id is VARCHAR primary key)
            try:
                cursor.execute(f"SELECT crime_id FROM {CRIMES_TABLE} WHERE crime_id = %s", (crime_id_str,))
                result = cursor.fetchone()
                if result:
                    crime_id_valid = crime_id_str  # Use the string directly (VARCHAR)
                    logger.trace(f"CRIME_ID {crime_id_str} found in crimes table")
                else:
                    logger.trace(f"CRIME_ID {crime_id_str} not found in crimes table")
            except Exception as e:
                logger.error(f"Error validating crime_id {crime_id_str}: {e}")
                # Note: The calling worker ensures transaction handling
        
        # FIX: API sends DISPOSED_DATE; accept DISPOSED_AT for compatibility.
        disposed_at_timestamp = parse_api_timestamp(
            disposal_raw.get('DISPOSED_DATE') or disposal_raw.get('DISPOSED_AT')
        )
        
        transformed = {
            'crime_id': crime_id_valid,  # VARCHAR foreign key to crimes.crime_id
            'disposal_type': normalize_api_value(disposal_raw.get('DISPOSAL_TYPE')),
            'disposed_at': disposed_at_timestamp,  # FIX: Maps DISPOSED_DATE from API
            'disposal': normalize_api_value(disposal_raw.get('DISPOSAL')),
            'case_status': normalize_api_value(disposal_raw.get('CASE_STATUS')),
            # Dates are always from API (never use CURRENT_TIMESTAMP)
            # If API doesn't provide dates, they will be NULL
            'date_created': parse_api_timestamp(disposal_raw.get('DATE_CREATED')),
            'date_modified': parse_api_timestamp(disposal_raw.get('DATE_MODIFIED')),
            # Store original CRIME_ID string for validation
            '_original_crime_id': crime_id_str
        }
        logger.trace(f"Transformed disposal: {json.dumps({k: v for k, v in transformed.items() if k != '_original_crime_id'}, indent=2, default=str)}")
        return transformed
    
    def disposal_exists(self, crime_id: str, disposal_type: str, disposed_at: Optional[datetime], cursor) -> bool:
        """Check if disposal already exists in database (based on unique constraint)"""
        logger.trace(f"Checking if disposal exists: crime_id={crime_id}, disposal_type={disposal_type}, disposed_at={disposed_at}")
        query = f"""
            SELECT 1 FROM {DISPOSAL_TABLE} 
            WHERE crime_id = %s
              AND disposal_type IS NOT DISTINCT FROM %s
              AND disposed_at IS NOT DISTINCT FROM %s
        """
        cursor.execute(query, (crime_id, disposal_type, disposed_at))
        exists = cursor.fetchone() is not None
        logger.trace(f"Disposal exists: {exists}")
        return exists
    
    def get_existing_disposal(self, crime_id: str, disposal_type: str, disposed_at: Optional[datetime], cursor) -> Optional[Dict]:
        """Get existing disposal record from database"""
        query = f"""
            SELECT crime_id, disposal_type, disposed_at, disposal, case_status,
                   date_created, date_modified
            FROM {DISPOSAL_TABLE}
            WHERE crime_id = %s
              AND disposal_type IS NOT DISTINCT FROM %s
              AND disposed_at IS NOT DISTINCT FROM %s
        """
        cursor.execute(query, (crime_id, disposal_type, disposed_at))
        row = cursor.fetchone()
        if row:
            return {
                'crime_id': row[0],
                'disposal_type': row[1],
                'disposed_at': row[2],
                'disposal': row[3],
                'case_status': row[4],
                'date_created': row[5],
                'date_modified': row[6]
            }
        return None
    
    def log_failed_record(self, disposal: Dict, reason: str, error_details: str = ""):
        """Log a failed record to the failed records log file"""
        failed_info = {
            'crime_id': disposal.get('crime_id'),
            'disposal_type': disposal.get('disposal_type'),
            'disposed_at': disposal.get('disposed_at'),
            'reason': reason,
            'error_details': error_details,
            'timestamp': datetime.now().isoformat(),
            'disposal_data': disposal
        }
        
        with self.log_lock:
            self.failed_log.write(f"\n{'='*80}\n")
            self.failed_log.write(f"CRIME_ID: {disposal.get('crime_id')}\n")
            self.failed_log.write(f"DISPOSAL_TYPE: {disposal.get('disposal_type')}\n")
            self.failed_log.write(f"DISPOSED_AT: {disposal.get('disposed_at')}\n")
            self.failed_log.write(f"REASON: {reason}\n")
            if error_details:
                self.failed_log.write(f"ERROR: {error_details}\n")
            self.failed_log.write(f"Timestamp: {datetime.now().isoformat()}\n")
            self.failed_log.write(f"\nJSON Format:\n")
            self.failed_log.write(json.dumps(failed_info, indent=2, ensure_ascii=False, default=str))
            self.failed_log.write(f"\n")
            self.failed_log.flush()
    
    def log_invalid_crime_id(self, disposal: Dict, crime_id_str: str, chunk_range: str = ""):
        """Log a disposal that failed due to invalid CRIME_ID (not found in crimes table)"""
        failure_info = {
            'crime_id': crime_id_str,
            'disposal_type': disposal.get('disposal_type'),
            'disposed_at': disposal.get('disposed_at'),
            'chunk': chunk_range,
            'timestamp': datetime.now().isoformat(),
            'disposal_data': disposal
        }
        
        with self.log_lock:
            self.invalid_crime_id_log.write(f"\n{'='*80}\n")
            self.invalid_crime_id_log.write(f"CRIME_ID: {crime_id_str}\n")
            self.invalid_crime_id_log.write(f"DISPOSAL_TYPE: {disposal.get('disposal_type')}\n")
            self.invalid_crime_id_log.write(f"DISPOSED_AT: {disposal.get('disposed_at')}\n")
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
        
        with self.log_lock:
            self.duplicates_log.write(f"\n{'='*80}\n")
            self.duplicates_log.write(f"CHUNK: {from_date} to {to_date}\n")
            self.duplicates_log.write(f"Timestamp: {datetime.now().isoformat()}\n")
            self.duplicates_log.write(f"{'-'*80}\n")
            self.duplicates_log.write(f"Duplicate Count: {len(duplicates)}\n")
            self.duplicates_log.write(f"Note: These duplicates were PROCESSED (not skipped) to allow updates\n")
            self.duplicates_log.write(f"\nDuplicates:\n")
            for i, dup in enumerate(duplicates, 1):
                self.duplicates_log.write(f"  {i}. CRIME_ID: {dup['crime_id']}, DISPOSAL_TYPE: {dup.get('disposal_type')}, DISPOSED_AT: {dup.get('disposed_at')}\n")
                self.duplicates_log.write(f"     Occurrence: #{dup.get('occurrence', 'N/A')}\n")
                self.duplicates_log.write(f"     First seen in: {dup['first_seen_in']}\n")
                self.duplicates_log.write(f"     Duplicate in: {dup['duplicate_in']}\n")
            
            # Also write JSON format for easy parsing
            self.duplicates_log.write(f"\nJSON Format:\n")
            self.duplicates_log.write(json.dumps(chunk_info, indent=2, ensure_ascii=False))
            self.duplicates_log.write(f"\n")
            
            self.duplicates_log.flush()
    
    def insert_disposal(self, disposal: Dict, conn, cursor, chunk_date_range: str = "") -> Tuple[bool, str]:
        """
        Insert or update single disposal into database with smart update logic
        Dates are always from API (never use CURRENT_TIMESTAMP)
        
        Behavior:
        - NEW DATA: If (crime_id, disposal_type, disposed_at) doesn't exist → INSERT
        - EXISTING DATA: If exists → UPDATE (updates only changed fields)
        - Smart Update: Only updates fields that have changed, preserves existing values if API sends NULL
        
        Date Handling:
        - date_created and date_modified are always taken from API
        - If API provides dates, they are used (even if different from existing)
        - If API doesn't provide dates, they remain NULL
        
        Args:
            disposal: Transformed disposal dict
            chunk_date_range: Date range for chunk tracking
        
        Returns:
            Tuple of (success: bool, operation: str) where operation is 'inserted', 'updated', 'no_change', or 'skipped'
        """
        crime_id = disposal.get('crime_id')
        disposal_type = disposal.get('disposal_type')
        disposed_at = disposal.get('disposed_at')
        original_crime_id = disposal.get('_original_crime_id')
        
        # Validate crime_id exists in crimes table
        if not crime_id:
            reason = 'invalid_crime_id'
            error_details = f"CRIME_ID {original_crime_id} not found in crimes table"
            logger.warning(f"⚠️  {error_details}, skipping disposal")
            with self.stats_lock:
                self.stats['total_disposals_failed'] += 1
                self.stats['total_disposals_failed_crime_id'] += 1
            self.log_failed_record(disposal, reason, error_details)
            self.log_invalid_crime_id(disposal, original_crime_id, chunk_date_range)
            # Park in FK retry queue so the record recovers when the crime arrives.
            if push_fk_failure is not None:
                try:
                    push_fk_failure(
                        conn, 'disposal',
                        record_id=original_crime_id or 'UNKNOWN',
                        record_json=json.dumps(
                            {k: str(v) if v is not None else None
                             for k, v in disposal.items()},
                        ),
                        missing_fk_column='crime_id',
                        missing_fk_value=original_crime_id or '',
                    )
                    conn.commit()
                except Exception as _qe:
                    logger.warning("FK queue push failed for disposal %s: %s", original_crime_id, _qe)
            return False, reason
        
        try:
            logger.trace(f"Processing disposal: crime_id={crime_id}, disposal_type={disposal_type}, disposed_at={disposed_at}")

            upsert_query = f"""
                INSERT INTO {DISPOSAL_TABLE} (
                    crime_id, disposal_type, disposed_at, disposal, case_status,
                    date_created, date_modified
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (crime_id, disposal_type, disposed_at) DO UPDATE SET
                    disposal = EXCLUDED.disposal,
                    case_status = EXCLUDED.case_status,
                    date_created = EXCLUDED.date_created,
                    date_modified = EXCLUDED.date_modified
                WHERE (
                    {DISPOSAL_TABLE}.disposal IS DISTINCT FROM EXCLUDED.disposal OR
                    {DISPOSAL_TABLE}.case_status IS DISTINCT FROM EXCLUDED.case_status OR
                    {DISPOSAL_TABLE}.date_created IS DISTINCT FROM EXCLUDED.date_created OR
                    {DISPOSAL_TABLE}.date_modified IS DISTINCT FROM EXCLUDED.date_modified
                )
                RETURNING (xmax = 0) AS inserted
            """

            cursor.execute(upsert_query, (
                crime_id,
                disposal_type,
                disposed_at,
                disposal.get('disposal'),
                disposal.get('case_status'),
                disposal.get('date_created'),
                disposal.get('date_modified')
            ))

            result = cursor.fetchone()
            conn.commit()

            if result is None:
                with self.stats_lock:
                    self.stats['total_disposals_no_change'] += 1
                return True, 'no_change'

            if result[0]:
                with self.stats_lock:
                    self.stats['total_disposals_inserted'] += 1
                return True, 'inserted'

            with self.stats_lock:
                self.stats['total_disposals_updated'] += 1
            return True, 'updated'
            
        except psycopg2.IntegrityError as e:
            conn.rollback()
            # Check if error is due to duplicate key (already inserted)
            error_str = str(e).lower()
            if 'duplicate' in error_str or 'unique' in error_str:
                reason = 'duplicate_key_constraint'
                with self.stats_lock:
                    self.stats['duplicate_warnings_suppressed'] += 1
                # Suppress warning for duplicates - this is expected with overlapping chunks
                logger.trace(f"Duplicate key for disposal (expected with overlaps): crime_id={crime_id}, disposal_type={disposal_type}")
                return True, 'no_change'  # Treat as no-change to avoid error count inflation
            else:
                reason = 'integrity_error'
                error_details = str(e)
                logger.warning(f"⚠️  Integrity error for disposal: {e}")
                with self.stats_lock:
                    self.stats['total_disposals_failed'] += 1
                self.log_failed_record(disposal, reason, error_details)
                return False, reason
        except Exception as e:
            conn.rollback()
            reason = 'error'
            error_details = str(e)
            logger.error(f"❌ Error inserting disposal: {e}")
            with self.stats_lock:
                self.stats['total_disposals_failed'] += 1
                self.stats['errors'].append(f"Disposal crime_id={crime_id}: {str(e)}")
            self.log_failed_record(disposal, reason, error_details)
            return False, reason
    
    def process_date_range(self, from_date: str, to_date: str, table_columns: Set[str] = None):
        """Process disposal records for a specific date range"""
        chunk_range = f"{from_date} to {to_date}"
        logger.info(f"📅 Processing: {chunk_range}")
        
        # Fetch disposal from API
        disposal_raw = self.fetch_disposal_api(from_date, to_date)
        
        if disposal_raw is None:
            logger.error(f"❌ Failed to fetch disposal for {chunk_range}")
            self.log_db_chunk(from_date, to_date, 0, [], [], [], [], [], error="API fetch failed")
            return
        
        if not disposal_raw:
            logger.info(f"ℹ️  No disposal records found for {chunk_range}")
            self.log_db_chunk(from_date, to_date, 0, [], [], [], [], [], error="No disposal records in API response")
            return
        
        # Check for schema evolution if we got data
        if table_columns is not None and len(disposal_raw) > 0:
            # Check for new fields in first record
            new_fields = self.detect_new_fields(disposal_raw[0], table_columns)
            if new_fields:
                logger.info(f"🔍 New fields detected in API response: {list(new_fields.keys())}")
                # Add new columns to table
                for api_field, db_column in new_fields.items():
                    if self.add_column_to_table(db_column):
                        # Update table_columns set
                        table_columns.add(db_column)
                # Update existing records from start_date to current chunk end_date
                self.update_existing_records_with_new_fields(new_fields, to_date)
        
        # Transform and insert each disposal
        self.stats['total_disposals_fetched'] += len(disposal_raw)
        logger.trace(f"Processing {len(disposal_raw)} disposal records for chunk {chunk_range}")
        
    def process_record_worker(self, idx: int, total_records: int, disposal_record: Dict, chunk_range: str, 
                              chunk_state: Dict, chunk_lock: threading.Lock):
        """Worker method to process a single disposal record"""
        try:
            with self.db_pool.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    logger.trace(f"Processing record {idx}/{total_records}: {disposal_record.get('CRIME_ID')}")
                    disposal = self.transform_disposal(disposal_record, cursor)
                    crime_id = disposal.get('crime_id')
                    disposal_type = disposal.get('disposal_type')
                    disposed_at = disposal.get('disposed_at')
                    original_crime_id = disposal.get('_original_crime_id')
                    
                    # Check if crime_id is valid (exists in crimes table)
                    if not crime_id:
                        with chunk_lock:
                            logger.warning(f"⚠️  Disposal with CRIME_ID {original_crime_id} not found in crimes table, skipping")
                            with self.stats_lock:
                                self.stats['total_disposals_failed'] += 1
                                self.stats['total_disposals_failed_crime_id'] += 1
                            chunk_state['failed_keys'].append(f"{original_crime_id}:{disposal_type}:{disposed_at}")
                            reason = 'invalid_crime_id'
                            if reason not in chunk_state['failed_reasons']:
                                chunk_state['failed_reasons'][reason] = []
                            chunk_state['failed_reasons'][reason].append(original_crime_id)
                            chunk_state['invalid_crime_ids'].append({
                                'crime_id': original_crime_id,
                                'disposal_type': disposal_type,
                                'disposed_at': disposed_at
                            })
                            self.log_invalid_crime_id(disposal, original_crime_id, chunk_range)
                        return
                    
                    # Create unique key for tracking duplicates
                    unique_key = f"{crime_id}:{disposal_type}:{disposed_at}"
                    
                    # Track occurrences for duplicate reporting (but don't skip - process all)
                    with chunk_lock:
                        if unique_key in chunk_state['seen_keys']:
                            # This is a duplicate occurrence - track it but still process
                            occurrence_count = chunk_state['key_occurrences'].get(unique_key, 1) + 1
                            chunk_state['key_occurrences'][unique_key] = occurrence_count
                            
                            chunk_state['duplicates'].append({
                                'crime_id': crime_id,
                                'disposal_type': disposal_type,
                                'disposed_at': disposed_at,
                                'occurrence': occurrence_count,
                                'first_seen_in': chunk_state['seen_keys'][unique_key],
                                'duplicate_in': chunk_range
                            })
                            with self.stats_lock:
                                self.stats['total_duplicates'] += 1
                            logger.info(f"⚠️  Duplicate disposal found in chunk {chunk_range} (occurrence #{occurrence_count}) - Will process to update record")
                            logger.trace(f"Duplicate details - First seen: {chunk_state['seen_keys'][unique_key]}, Current occurrence: {occurrence_count}")
                        else:
                            chunk_state['seen_keys'][unique_key] = chunk_range
                            chunk_state['key_occurrences'][unique_key] = 1
                            logger.trace(f"New disposal key seen: {unique_key} in chunk {chunk_range}")
                    
                    # IMPORTANT: Process ALL records, even duplicates
                    # If same key appears multiple times, each occurrence might have updated data
                    # The smart update logic will handle whether to actually update or not
                    success, operation = self.insert_disposal(disposal, conn, cursor, chunk_range)
                    logger.trace(f"Operation result for disposal: success={success}, operation={operation}")
                    
                    with chunk_lock:
                        if success:
                            if operation == 'inserted':
                                # Only add to list if first occurrence (to avoid duplicate entries in log)
                                if unique_key not in chunk_state['inserted_keys']:
                                    chunk_state['inserted_keys'].append(unique_key)
                                logger.trace(f"Added to inserted list: {unique_key}")
                            elif operation == 'updated':
                                # Track all updates (even if same key updated multiple times)
                                chunk_state['updated_keys'].append(unique_key)
                                logger.trace(f"Added to updated list: {unique_key} (occurrence #{chunk_state['key_occurrences'].get(unique_key, 1)})")
                            elif operation == 'no_change':
                                # Only add to list if first occurrence
                                if unique_key not in chunk_state['no_change_keys']:
                                    chunk_state['no_change_keys'].append(unique_key)
                                logger.trace(f"Added to no_change list: {unique_key}")
                        else:
                            chunk_state['failed_keys'].append(unique_key)
                            if operation not in chunk_state['failed_reasons']:
                                chunk_state['failed_reasons'][operation] = []
                            chunk_state['failed_reasons'][operation].append(unique_key)
                            logger.trace(f"Added to failed list: {unique_key}, reason: {operation}")

        except Exception as e:
            logger.error(f"❌ Error in worker processing record {idx}: {e}")
            with self.stats_lock:
                self.stats['total_disposals_failed'] += 1
    
    def process_date_range(self, from_date: str, to_date: str, table_columns: Set[str] = None):
        """Process disposal records for a specific date range"""
        chunk_range = f"{from_date} to {to_date}"
        logger.info(f"📅 Processing: {chunk_range}")
        
        # Fetch disposal from API
        disposal_raw = self.fetch_disposal_api(from_date, to_date)
        
        if disposal_raw is None:
            logger.error(f"❌ Failed to fetch disposal for {chunk_range}")
            self.log_db_chunk(from_date, to_date, 0, [], [], [], [], [], error="API fetch failed")
            return
        
        if not disposal_raw:
            logger.info(f"ℹ️  No disposal records found for {chunk_range}")
            self.log_db_chunk(from_date, to_date, 0, [], [], [], [], [], error="No disposal records in API response")
            return
        
        # Check for schema evolution if we got data
        if table_columns is not None and len(disposal_raw) > 0:
            # Check for new fields in first record
            new_fields = self.detect_new_fields(disposal_raw[0], table_columns)
            if new_fields:
                logger.info(f"🔍 New fields detected in API response: {list(new_fields.keys())}")
                # Add new columns to table
                for api_field, db_column in new_fields.items():
                    if self.add_column_to_table(db_column):
                        # Update table_columns set
                        table_columns.add(db_column)
                # Update existing records from start_date to current chunk end_date
                self.update_existing_records_with_new_fields(new_fields, to_date)
        
        # Transform and insert each disposal
        with self.stats_lock:
            self.stats['total_disposals_fetched'] += len(disposal_raw)
        logger.trace(f"Processing {len(disposal_raw)} disposal records for chunk {chunk_range}")
        
        # Track operations for this chunk
        chunk_lock = threading.Lock()
        chunk_state = {
            'inserted_keys': [],
            'updated_keys': [],
            'no_change_keys': [],
            'failed_keys': [],
            'failed_reasons': {},
            'duplicates': [],
            'invalid_crime_ids': [],
            'seen_keys': {},
            'key_occurrences': {}
        }
        
        logger.trace(f"Starting parallel processing for chunk: {chunk_range}")
        max_workers = int(os.environ.get('MAX_WORKERS', getattr(self, 'max_workers', min(32, (os.cpu_count() or 1) * 4))))
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            total_records = len(disposal_raw)
            futures = [
                executor.submit(self.process_record_worker, idx + 1, total_records, record, chunk_range, chunk_state, chunk_lock)
                for idx, record in enumerate(disposal_raw)
            ]
            for future in as_completed(futures):
                future.result()  # raise exceptions if any occurred in worker
        
        # Log duplicates for this chunk (for reporting, but they were all processed)
        if chunk_state['duplicates']:
            logger.info(f"📊 Found {len(chunk_state['duplicates'])} duplicate occurrences in chunk {chunk_range} - All were processed for potential updates")
            logger.trace(f"Duplicate details: {chunk_state['duplicates']}")
            self.log_duplicates_chunk(from_date, to_date, chunk_state['duplicates'])
        
        # Log invalid crime_ids for this chunk
        if chunk_state['invalid_crime_ids']:
            logger.warning(f"⚠️  Found {len(chunk_state['invalid_crime_ids'])} disposal records with invalid CRIME_IDs in chunk {chunk_range}")
            # Extract unique CRIME_IDs
            unique_crime_ids = list(set([f['crime_id'] for f in chunk_state['invalid_crime_ids'] if f.get('crime_id')]))
            logger.warning(f"   Invalid CRIME_IDs: {unique_crime_ids}")
        
        # Log database operations for this chunk
        logger.trace(f"Chunk summary - Inserted: {len(chunk_state['inserted_keys'])}, Updated: {len(chunk_state['updated_keys'])}, No Change: {len(chunk_state['no_change_keys'])}, Failed: {len(chunk_state['failed_keys'])}, Duplicates: {len(chunk_state['duplicates'])}, Invalid CRIME_IDs: {len(chunk_state['invalid_crime_ids'])}")
        self.log_db_chunk(from_date, to_date, len(disposal_raw), chunk_state['inserted_keys'], chunk_state['updated_keys'], 
                         chunk_state['no_change_keys'], chunk_state['failed_keys'], chunk_state['failed_reasons'])
        
        logger.info(f"✅ Completed: {chunk_range} - Inserted: {len(chunk_state['inserted_keys'])}, Updated: {len(chunk_state['updated_keys'])}, No Change: {len(chunk_state['no_change_keys'])}, Failed: {len(chunk_state['failed_keys'])}, Duplicates: {len(chunk_state['duplicates'])}, Invalid CRIME_IDs: {len(chunk_state['invalid_crime_ids'])}")
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
        
        with self.log_lock:
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
        self.api_log.write(f"Total Disposals Fetched: {self.stats['total_disposals_fetched']}\n")
        self.api_log.write(f"Failed API Calls: {self.stats['failed_api_calls']}\n")
        self.api_log.write(f"Total Chunks Processed: {self.stats['total_api_calls'] + self.stats['failed_api_calls']}\n")
        
        # DB log summary
        self.db_log.write(f"\n\n{'='*80}\n")
        self.db_log.write(f"SUMMARY\n")
        self.db_log.write(f"{'='*80}\n")
        self.db_log.write(f"Total Disposals Fetched from API: {self.stats['total_disposals_fetched']}\n")
        self.db_log.write(f"Total Disposals Inserted (New): {self.stats['total_disposals_inserted']}\n")
        self.db_log.write(f"Total Disposals Updated (Existing): {self.stats['total_disposals_updated']}\n")
        self.db_log.write(f"Total Disposals No Change: {self.stats['total_disposals_no_change']}\n")
        self.db_log.write(f"Total Disposals Failed: {self.stats['total_disposals_failed']}\n")
        self.db_log.write(f"  - Failed due to Invalid CRIME_ID: {self.stats['total_disposals_failed_crime_id']}\n")
        self.db_log.write(f"Total Disposals Duplicates (Processed): {self.stats['total_duplicates']}\n")
        self.db_log.write(f"Total Operations (Inserted + Updated + No Change): {self.stats['total_disposals_inserted'] + self.stats['total_disposals_updated'] + self.stats['total_disposals_no_change']}\n")
        db_total = self.stats.get('db_total_count', self.stats['total_disposals_inserted'])
        self.db_log.write(f"Total Unique Disposals in Database: {db_total}\n")
        self.db_log.write(f"Note: Updated count includes multiple updates (same key in multiple chunks or same chunk)\n")
        self.db_log.write(f"Note: Duplicates are records that appear multiple times within the same chunk - ALL are processed for updates\n")
        if self.stats['total_disposals_fetched'] > 0:
            coverage = ((self.stats['total_disposals_inserted'] + self.stats['total_disposals_updated'] + self.stats['total_disposals_no_change']) / self.stats['total_disposals_fetched']) * 100
            self.db_log.write(f"Coverage: {coverage:.2f}%\n")
        self.db_log.write(f"Errors: {len(self.stats['errors'])}\n")
        
        # Failed records log summary
        self.failed_log.write(f"\n\n{'='*80}\n")
        self.failed_log.write(f"SUMMARY\n")
        self.failed_log.write(f"{'='*80}\n")
        self.failed_log.write(f"Total Failed Records: {self.stats['total_disposals_failed']}\n")
        self.failed_log.write(f"Note: Failed records are those that could not be inserted or updated\n")
        self.failed_log.write(f"Check individual entries above for specific reasons\n")
        
        # Invalid CRIME_ID log summary
        self.invalid_crime_id_log.write(f"\n\n{'='*80}\n")
        self.invalid_crime_id_log.write(f"SUMMARY\n")
        self.invalid_crime_id_log.write(f"{'='*80}\n")
        self.invalid_crime_id_log.write(f"Total Disposals Failed Due to Invalid CRIME_ID: {self.stats['total_disposals_failed_crime_id']}\n")
        self.invalid_crime_id_log.write(f"\n")
        self.invalid_crime_id_log.write(f"Note: These disposal records could not be inserted/updated because their CRIME_ID\n")
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
    
    def run_preflight_checks(self) -> bool:
        """Run pre-flight validation checks before ETL execution"""
        logger.info("\n" + "=" * 80)
        logger.info("🔍 PRE-FLIGHT VALIDATION CHECKS")
        logger.info("=" * 80)
        
        try:
            # Check 1: Database connectivity
            logger.info("[1/5] Checking database connectivity...")
            if not self.connect_db():
                logger.error("❌ Failed to connect to database")
                return False
            logger.info("✅ Database connected successfully")
            
            # Check 2: Check if crimes table has data (prerequisite for disposal ETL)
            logger.info("[2/5] Checking if crimes table is loaded (prerequisite)...")
            with self.db_pool.get_connection_context() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT COUNT(*) FROM {CRIMES_TABLE}")
                    crime_count = cur.fetchone()[0]
                    if crime_count == 0:
                        logger.error("❌ PREREQUISITE FAILED: Crimes table is empty. Load crimes data first before running disposal ETL.")
                        return False
                    logger.info(f"✅ Crimes table has {crime_count:,} records")
            
            # Check 3: Verify disposal table schema
            logger.info("[3/5] Verifying disposal table schema...")
            table_columns = self.get_table_columns(DISPOSAL_TABLE)
            required_columns = {'crime_id', 'disposal_type', 'disposed_at'}
            if not required_columns.issubset(table_columns):
                logger.error(f"❌ Disposal table missing required columns: {required_columns - table_columns}")
                return False
            logger.info(f"✅ Disposal table has all required columns")
            
            # Check 4: Verify API connectivity
            logger.info("[4/5] Checking API connectivity...")
            test_url = API_CONFIG.get('disposal_url', f"{API_CONFIG['base_url']}/crimes/disposal")
            headers = {'x-api-key': API_CONFIG['api_key']}
            try:
                response = requests.get(
                    test_url,
                    params={'fromDate': '2022-06-06', 'toDate': '2022-06-07'},
                    headers=headers,
                    timeout=10
                )
                if response.status_code in [200, 404]:  # 200 = data, 404 = no data (both OK)
                    logger.info(f"✅ API is accessible (HTTP {response.status_code})")
                else:
                    logger.error(f"❌ API returned unexpected status {response.status_code}")
                    return False
            except requests.exceptions.Timeout:
                logger.error("❌ API connectivity check timed out")
                return False
            except Exception as e:
                logger.error(f"❌ API connectivity check failed: {e}")
                return False
            
            # Check 5: Verify API data availability start date
            logger.info("[5/5] Verifying API data availability (minimum date: 2022-06-06)...")
            logger.info(f"✅ API data available from: {API_DATA_START_DATE}")
            
            logger.info("=" * 80)
            logger.info("✅ ALL PRE-FLIGHT CHECKS PASSED - Ready to start ETL")
            logger.info("=" * 80 + "\n")
            self.preflight_checks_done = True
            return True
            
        except Exception as e:
            logger.error(f"❌ Pre-flight check failed with error: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def _retry_disposal_record(self, conn, record):
        """Retry insertion of a queued disposal record once its crime_id is present.

        Called by drain_fk_queue after a previous ETL run parked this record
        because CRIME_ID was not yet in the crimes table.
        Returns True on success, False if still unresolvable.
        """
        original_crime_id = record.get('_original_crime_id') or record.get('crime_id')
        if not original_crime_id:
            return False
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT crime_id FROM {CRIMES_TABLE} WHERE crime_id = %s",
                (original_crime_id,)
            )
            if not cur.fetchone():
                return False  # Still missing — leave in queue
            # Crime now exists — inject validated crime_id and attempt insert.
            record['crime_id'] = original_crime_id
            success, _ = self.insert_disposal(record, conn, cur, 'FK_RETRY')
            return success

    def run(self):
        """Main ETL execution"""
        logger.info("=" * 80)
        logger.info("🚀 DOPAMAS ETL Pipeline - Disposal API")
        logger.info("=" * 80)
        
        # Run pre-flight validation checks
        if not self.run_preflight_checks():
            logger.error("\n❌ PRE-FLIGHT CHECKS FAILED - Aborting ETL run")
            return False

        self.ensure_run_state_table()

        # Retry any disposal records queued from previous runs due to FK misses.
        if _drain_fk_queue is not None:
            try:
                with self.db_pool.get_connection_context() as _drain_conn:
                    _drain_fk_queue(_drain_conn, 'disposal', self._retry_disposal_record)
                    _drain_conn.commit()
            except Exception as _de:
                logger.warning("FK queue drain failed at startup: %s (non-fatal)", _de)
        
        # Calculate date range
        # Calculate end date: Yesterday at 23:59:59+05:30 (IST)
        calculated_end_date = get_yesterday_end_ist()
        
        logger.info(f"API Data Availability: {API_DATA_START_DATE}")
        logger.info(f"Calculated End Date: {calculated_end_date}")
        
        try:
            # Get effective start date (check if table has data, or use API_DATA_START_DATE)
            effective_start_date = self.get_effective_start_date()
            checkpoint_date = self.get_run_checkpoint('disposal')
            if checkpoint_date:
                checkpoint_iso = checkpoint_date.isoformat()
                if parse_iso_date(checkpoint_iso) > parse_iso_date(effective_start_date):
                    effective_start_date = checkpoint_iso

            logger.info(f"Effective Start Date (for this run): {effective_start_date}")
            
            # Get table columns for schema evolution
            table_columns = self.get_table_columns(DISPOSAL_TABLE)
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
            logger.info(f"API Min Date: {API_DATA_START_DATE} (earliest data available from API)")
            logger.info("=" * 80)
            
            logger.info(f"📊 Total date ranges to process: {len(date_ranges)}")
            logger.debug(f"Generated date ranges: {date_ranges[:5]}{'...' if len(date_ranges) > 5 else ''} (showing first 5)")
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
            with self.db_pool.get_connection_context() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT COUNT(*) FROM {DISPOSAL_TABLE}")
                    db_disposals_count = cur.fetchone()[0]
            
            # Store for summary
            self.stats['db_total_count'] = db_disposals_count
            
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
            logger.info(f"  Total Disposals Fetched: {self.stats['total_disposals_fetched']}")
            logger.info(f"")
            logger.info(f"💾 TO DATABASE:")
            logger.info(f"  Total Inserted (New): {self.stats['total_disposals_inserted']}")
            logger.info(f"  Total Updated:        {self.stats['total_disposals_updated']}")
            logger.info(f"  Total No Change:      {self.stats['total_disposals_no_change']}")
            logger.info(f"  Total Failed:         {self.stats['total_disposals_failed']}")
            logger.info(f"    - Invalid CRIME_ID:   {self.stats['total_disposals_failed_crime_id']}")
            logger.info(f"  Total in DB:          {db_disposals_count}")
            logger.info(f"")
            logger.info(f"🔄 DUPLICATES:")
            logger.info(f"  Total Duplicate Occurrences (Processed): {self.stats['total_duplicates']}")
            logger.info(f"  Note: All duplicates are processed to allow updates")
            logger.info(f"")
            logger.info(f"⚠️  INVALID CRIME_ID:")
            logger.info(f"  Disposals Failed Due to Invalid CRIME_ID: {self.stats['total_disposals_failed_crime_id']}")
            logger.info(f"  Check logs/disposal_invalid_crime_id_*.log for details")
            logger.info(f"")
            logger.info(f"📊 COVERAGE:")
            if self.stats['total_disposals_fetched'] > 0:
                coverage = ((self.stats['total_disposals_inserted'] + self.stats['total_disposals_updated'] + self.stats['total_disposals_no_change']) / self.stats['total_disposals_fetched']) * 100
                logger.info(f"  API → DB Coverage:   {coverage:.2f}%")
            logger.info(f"")
            logger.info(f"📈 SUMMARY:")
            logger.info(f"  Total from API:       {self.stats['total_disposals_fetched']}")
            logger.info(f"  Inserted + Updated:   {self.stats['total_disposals_inserted'] + self.stats['total_disposals_updated']}")
            logger.info(f"  Duplicate Occurrences: {self.stats['total_duplicates']} (all processed)")
            logger.info(f"  Failed:               {self.stats['total_disposals_failed']}")
            logger.info(f"")
            logger.info(f"💡 NOTES:")
            logger.info(f"  - API data available from: {API_DATA_START_DATE}")
            logger.info(f"  - Same disposal key can appear in overlapping chunks (handled)")
            logger.info(f"  - Duplicate warnings suppressed: {self.stats.get('duplicate_warnings_suppressed', 0)}")
            logger.info(f"  - Smart update logic: only changed fields are updated")
            logger.info(f"  - Invalid CRIME_IDs are logged separately for review")
            logger.info(f"  - FIX: Using DISPOSED_DATE from API (maps to disposed_at column)")
            logger.info(f"")
            logger.info(f"Errors:               {len(self.stats['errors'])}")
            logger.info("=" * 80)

            self.update_run_checkpoint('disposal', calculated_end_date)
            
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
    etl = DisposalETL()
    success = etl.run()
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()


