#!/usr/bin/env python3
"""
DOPAMAS ETL Pipeline - Interrogation Reports (IR) API
Fetches IR data in date-range chunks with overlap and loads into normalized PostgreSQL tables
"""

import sys
import os
import time
import requests
import psycopg2
from psycopg2.extras import Json, execute_values
from psycopg2 import errors as psycopg2_errors
from datetime import datetime, timedelta
from tqdm import tqdm
import logging
import colorlog
import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional, Tuple, Any, Set
from datetime import timezone, timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from db_pooling import PostgreSQLConnectionPool, compute_safe_workers
except ImportError:
    pass

from config import DB_CONFIG, API_CONFIG, ETL_CONFIG, LOG_CONFIG, TABLE_CONFIG

# IST timezone offset (UTC+05:30)
IST_OFFSET = timezone(timedelta(hours=5, minutes=30))

# Setup colored logging
handler = colorlog.StreamHandler()
handler.setFormatter(colorlog.ColoredFormatter(
    LOG_CONFIG['format'],
    datefmt=LOG_CONFIG['date_format'],
    log_colors={
        'DEBUG': 'cyan',
        'INFO': 'green',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'red,bg_white',
    }
))
logger = colorlog.getLogger()
logger.addHandler(handler)
logger.setLevel(LOG_CONFIG['level'])

# Target tables (allows redirecting ETL runs to test tables)
IR_TABLE = TABLE_CONFIG.get('interrogation_reports', 'interrogation_reports')
IR_FAMILY_HISTORY_TABLE = TABLE_CONFIG.get('ir_family_history', 'ir_family_history')
IR_LOCAL_CONTACTS_TABLE = TABLE_CONFIG.get('ir_local_contacts', 'ir_local_contacts')
IR_REGULAR_HABITS_TABLE = TABLE_CONFIG.get('ir_regular_habits', 'ir_regular_habits')
IR_TYPES_OF_DRUGS_TABLE = TABLE_CONFIG.get('ir_types_of_drugs', 'ir_types_of_drugs')
IR_SIM_DETAILS_TABLE = TABLE_CONFIG.get('ir_sim_details', 'ir_sim_details')
IR_FINANCIAL_HISTORY_TABLE = TABLE_CONFIG.get('ir_financial_history', 'ir_financial_history')
IR_CONSUMER_DETAILS_TABLE = TABLE_CONFIG.get('ir_consumer_details', 'ir_consumer_details')
IR_MODUS_OPERANDI_TABLE = TABLE_CONFIG.get('ir_modus_operandi', 'ir_modus_operandi')
IR_PREVIOUS_OFFENCES_TABLE = TABLE_CONFIG.get('ir_previous_offences_confessed', 'ir_previous_offences_confessed')
IR_DEFENCE_COUNSEL_TABLE = TABLE_CONFIG.get('ir_defence_counsel', 'ir_defence_counsel')
IR_ASSOCIATE_DETAILS_TABLE = TABLE_CONFIG.get('ir_associate_details', 'ir_associate_details')
IR_SHELTER_TABLE = TABLE_CONFIG.get('ir_shelter', 'ir_shelter')
IR_MEDIA_TABLE = TABLE_CONFIG.get('ir_media', 'ir_media')
IR_INTERROGATION_REPORT_REFS_TABLE = TABLE_CONFIG.get('ir_interrogation_report_refs', 'ir_interrogation_report_refs')
IR_DOPAMS_LINKS_TABLE = TABLE_CONFIG.get('ir_dopams_links', 'ir_dopams_links')
IR_INDULGANCE_BEFORE_OFFENCE_TABLE = TABLE_CONFIG.get('ir_indulgance_before_offence', 'ir_indulgance_before_offence')
IR_PROPERTY_DISPOSAL_TABLE = TABLE_CONFIG.get('ir_property_disposal', 'ir_property_disposal')
IR_REGULARIZATION_TRANSIT_WARRANTS_TABLE = TABLE_CONFIG.get('ir_regularization_transit_warrants', 'ir_regularization_transit_warrants')
IR_EXECUTION_OF_NBW_TABLE = TABLE_CONFIG.get('ir_execution_of_nbw', 'ir_execution_of_nbw')
IR_PENDING_NBW_TABLE = TABLE_CONFIG.get('ir_pending_nbw', 'ir_pending_nbw')
IR_SURETIES_TABLE = TABLE_CONFIG.get('ir_sureties', 'ir_sureties')
IR_JAIL_SENTENCE_TABLE = TABLE_CONFIG.get('ir_jail_sentence', 'ir_jail_sentence')
IR_NEW_GANG_FORMATION_TABLE = TABLE_CONFIG.get('ir_new_gang_formation', 'ir_new_gang_formation')
IR_CONVICTION_ACQUITTAL_TABLE = TABLE_CONFIG.get('ir_conviction_acquittal', 'ir_conviction_acquittal')
CRIMES_TABLE = TABLE_CONFIG.get('crimes', 'crimes')
PENDING_FK_TABLE = 'ir_pending_fk'

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


def parse_timestamp(ts_string: Optional[str]) -> Optional[datetime]:
    """Parse ISO timestamp string and normalize timezone-aware values to UTC."""
    if not ts_string:
        return None
    try:
        ts_string = ts_string.replace('Z', '+00:00')
        dt = datetime.fromisoformat(ts_string)
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc)
        return dt.replace(tzinfo=None)
    except Exception as e:
        logger.debug(f"Failed to parse timestamp '{ts_string}': {e}")
        return None


def parse_date(date_string: Optional[str]) -> Optional[datetime]:
    """Parse date string to date object."""
    if not date_string:
        return None
    try:
        return datetime.fromisoformat(date_string.replace('Z', '+00:00')).date()
    except Exception as e:
        logger.debug(f"Failed to parse date '{date_string}': {e}")
        return None


def normalize_person_id(person_id):
    """Normalize person_id: treat empty strings as None."""
    if person_id and isinstance(person_id, str) and person_id.strip():
        return person_id.strip()
    return None


def truncate_string(value: Optional[str], max_length: int) -> Optional[str]:
    """Truncate string to max_length if it exceeds the limit."""
    if value is None:
        return None
    if isinstance(value, str) and len(value) > max_length:
        return value[:max_length]
    return value


class InterrogationReportsETL:
    """ETL Pipeline for Interrogation Reports API"""
    
    def __init__(self):
        self.db_pool = None
        self.crime_ids = set()
        self.stats_lock = threading.Lock()
        self.schema_lock = threading.Lock()
        self.stats = {
            'total_api_calls': 0,
            'total_ir_fetched': 0,
            'total_ir_inserted': 0,
            'total_ir_updated': 0,
            'total_ir_no_change': 0,  # Records that exist but no changes needed (unchanged)
            'total_ir_failed': 0,  # Records that failed to process
            'total_pending_fk': 0,
            'total_retried_ok': 0,
            'total_retried_still_missing': 0,
            'failed_api_calls': 0,
            'errors': []
        }
    
    def connect_db(self):
        """Connect to PostgreSQL database using connection pool"""
        try:
            max_workers = int(os.environ.get('MAX_WORKERS', min(32, (os.cpu_count() or 1) * 4)))
            self.db_pool = PostgreSQLConnectionPool(
                minconn=5,
                maxconn=max_workers + 5
            )
            logger.info(f"✅ Connected to connection pool (maxconn={max_workers + 5})")
            return True
        except Exception as e:
            logger.error(f"❌ Database connection pool failed: {e}")
            return False
    
    def close_db(self):
        """Close database connection pool"""
        if self.db_pool:
            self.db_pool.close_all()
        logger.info("Database connection closed")

    def ensure_pending_table(self):
        """Create the pending FK retry table."""
        try:
            with self.db_pool.get_connection_context() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        CREATE TABLE IF NOT EXISTS {PENDING_FK_TABLE} (
                            id SERIAL PRIMARY KEY,
                            ir_id VARCHAR(50) NOT NULL,
                            crime_id VARCHAR(50) NOT NULL,
                            raw_data JSONB NOT NULL,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            retry_count INTEGER DEFAULT 0,
                            last_retry_at TIMESTAMP,
                            resolved BOOLEAN DEFAULT FALSE,
                            resolved_at TIMESTAMP
                        )
                    """)
                    cur.execute(f"""
                        CREATE UNIQUE INDEX IF NOT EXISTS idx_pending_fk_ir_id
                        ON {PENDING_FK_TABLE}(ir_id) WHERE NOT resolved
                    """)
                    conn.commit()
            logger.info(f"✅ Pending FK retry table ready: {PENDING_FK_TABLE}")
        except Exception as e:
            logger.error(f"❌ Failed to create pending FK table: {e}")
            raise

    def ensure_schema_exists(self):
        """Apply schema migrations from INTERROGATION_REPORTS_FIX.sql (9 new tables + fixes)."""
        try:
            # Get the directory where this script lives
            script_dir = os.path.dirname(os.path.abspath(__file__))
            sql_file_path = os.path.join(script_dir, '..', 'INTERROGATION_REPORTS_FIX.sql')
            
            # If not found, try alternate paths
            if not os.path.exists(sql_file_path):
                sql_file_path = os.path.join(os.getcwd(), 'INTERROGATION_REPORTS_FIX.sql')
            
            if not os.path.exists(sql_file_path):
                logger.warning(f"⚠️  Schema migration file not found at {sql_file_path}. Skipping schema setup.")
                logger.warning("    Ensure INTERROGATION_REPORTS_FIX.sql exists or run: psql < INTERROGATION_REPORTS_FIX.sql manually")
                return True  # Non-fatal; schema may already exist
            
            logger.info(f"📋 Applying schema migrations from {sql_file_path}...")
            
            # Read the SQL file
            with open(sql_file_path, 'r') as f:
                sql_content = f.read()
            
            # Split by semicolon, filter comments and empty statements
            statements = []
            current_stmt = []
            for line in sql_content.split('\n'):
                # Skip comment lines
                if line.strip().startswith('--'):
                    continue
                current_stmt.append(line)
                if ';' in line:
                    stmt = '\n'.join(current_stmt).strip()
                    if stmt and not stmt.startswith('--'):
                        statements.append(stmt)
                    current_stmt = []
            
            if current_stmt:
                stmt = '\n'.join(current_stmt).strip()
                if stmt and not stmt.startswith('--'):
                    statements.append(stmt)
            
            # Execute each statement
            executed_count = 0
            with self.db_pool.get_connection_context() as conn:
                with conn.cursor() as cur:
                    for stmt in statements:
                        if stmt.strip():
                            try:
                                cur.execute(stmt)
                                executed_count += 1
                            except Exception as e:
                                # Log but continue (CREATE IF NOT EXISTS shouldn't fail)
                                if 'already exists' not in str(e).lower():
                                    logger.debug(f"  Statement execution note: {e}")
                    conn.commit()
            
            logger.info(f"✅ Schema migrations applied: {executed_count} statements executed")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to ensure schema exists: {e}")
            # Non-fatal - tables may already exist
            return True

    def load_crime_ids(self) -> bool:
        """Load all crime IDs into an in-memory set for O(1) lookups."""
        logger.info("⏳ Loading crime IDs into memory...")
        try:
            with self.db_pool.get_connection_context() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT crime_id FROM {CRIMES_TABLE} WHERE crime_id IS NOT NULL")
                    rows = cur.fetchall()
                    self.crime_ids = {row[0] for row in rows}
                    logger.info(f"✅ Loaded {len(self.crime_ids)} crime IDs into memory.")
                    return True
        except Exception as e:
            logger.error(f"❌ Failed to load crime IDs: {e}")
            self.crime_ids = set()
            return False

    def queue_pending_fk(self, record_raw: Dict, crime_id: str, conn, cursor):
        """Insert an IR record into the pending FK retry queue."""
        ir_id = record_raw.get('INTERROGATION_REPORT_ID', 'unknown')
        try:
            cursor.execute(f"""
                INSERT INTO {PENDING_FK_TABLE} (ir_id, crime_id, raw_data)
                VALUES (%s, %s, %s)
                ON CONFLICT (ir_id) WHERE NOT resolved
                DO UPDATE SET
                    raw_data = EXCLUDED.raw_data,
                    retry_count = {PENDING_FK_TABLE}.retry_count
            """, (ir_id, crime_id, json.dumps(record_raw, default=str)))
            conn.commit()
            with self.stats_lock:
                self.stats['total_pending_fk'] += 1
            logger.debug(f"Queued IR {ir_id} (crime_id={crime_id}) for FK retry")
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to queue pending FK for IR {ir_id}: {e}")

    def retry_pending_fk(self):
        """Retry all unresolved pending FK records."""
        logger.info("")
        logger.info("=" * 80)
        logger.info("🔄 Retrying pending FK records...")

        try:
            with self.db_pool.get_connection_context() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        SELECT id, ir_id, crime_id, raw_data, retry_count
                        FROM {PENDING_FK_TABLE}
                        WHERE resolved = FALSE
                        ORDER BY created_at
                    """)
                    pending_rows = cur.fetchall()

            if not pending_rows:
                logger.info("ℹ️  No pending FK records to retry")
                return

            logger.info(f"📊 Found {len(pending_rows)} pending FK records to retry")

            resolved_count = 0
            still_missing = 0

            for row_id, ir_id, crime_id, raw_data, retry_count in pending_rows:
                try:
                    with self.db_pool.get_connection_context() as conn:
                        with conn.cursor() as cur:
                            if crime_id in self.crime_ids:
                                success = self.process_ir_record(raw_data, conn, cur)
                                if success:
                                    conn.commit()
                                cur.execute(f"""
                                    UPDATE {PENDING_FK_TABLE}
                                    SET resolved = TRUE, resolved_at = CURRENT_TIMESTAMP,
                                        last_retry_at = CURRENT_TIMESTAMP, retry_count = %s
                                    WHERE id = %s
                                """, (retry_count + 1, row_id))
                                conn.commit()
                                resolved_count += 1
                                logger.debug(f"✅ Resolved pending IR {ir_id}")
                            else:
                                cur.execute(f"""
                                    UPDATE {PENDING_FK_TABLE}
                                    SET last_retry_at = CURRENT_TIMESTAMP, retry_count = %s
                                    WHERE id = %s
                                """, (retry_count + 1, row_id))
                                conn.commit()
                                still_missing += 1
                except Exception as e:
                    logger.error(f"Error retrying pending IR {ir_id}: {e}")
                    still_missing += 1

            with self.stats_lock:
                self.stats['total_retried_ok'] = resolved_count
                self.stats['total_retried_still_missing'] = still_missing

            logger.info(f"🔄 Retry complete: {resolved_count} resolved, {still_missing} still missing crime_id")

        except Exception as e:
            logger.error(f"❌ Error during pending FK retry: {e}")
    
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
        - If table is empty: return 2022-01-01T00:00:00+05:30
        - If table has data: return max(date_created, date_modified) from table
        """
        try:
            with self.db_pool.get_connection_context() as conn:
                with conn.cursor() as cur:
                    # Check if table has any data
                    cur.execute(f"SELECT COUNT(*) FROM {IR_TABLE}")
                    count = cur.fetchone()[0]
                    
                    if count == 0:
                        # New database, start from beginning
                        logger.info("📊 Table is empty, starting from 2022-01-01")
                        return '2022-01-01T00:00:00+05:30'
                    
                    # Table has data, get max of date_created and date_modified
                    # Only consider dates >= 2022-01-01 to avoid processing very old data
                    MIN_START_DATE = '2022-01-01T00:00:00+05:30'
                    min_start_dt = parse_iso_date('2022-01-01T00:00:00+05:30')
                    
                    cur.execute(f"""
                        SELECT GREATEST(
                            COALESCE(MAX(CASE WHEN date_created >= '2022-01-01'::timestamp THEN date_created END), '2022-01-01'::timestamp),
                            COALESCE(MAX(CASE WHEN date_modified >= '2022-01-01'::timestamp THEN date_modified END), '2022-01-01'::timestamp)
                        ) as max_date
                        FROM {IR_TABLE}
                    """)
                    result = cur.fetchone()
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
        Note: IR has complex nested structure, so we focus on top-level fields.
        """
        new_fields = {}
        
        # Map API field names to database column names (main table fields)
        # Note: IR has many nested structures, so we check top-level fields
        top_level_fields = [
            'INTERROGATION_REPORT_ID', 'CRIME_ID', 'PERSON_ID',
            'DATE_CREATED', 'DATE_MODIFIED', 'OTHER_REGULAR_HABITS',
            'OTHER_INDULGENCE_BEFORE_OFFENCE', 'TIME_SINCE_MODUS_OPERANDI'
        ]
        
        for api_field in top_level_fields:
            if api_field in api_record:
                # Convert to snake_case
                db_column = api_field.lower()
                if db_column not in table_columns:
                    new_fields[api_field] = db_column
        
        return new_fields
    
    def add_column_to_table(self, column_name: str, column_type: str = 'TEXT'):
        """Add a new column to the interrogation_reports table."""
        with self.schema_lock:
            try:
                # Determine column type based on field name
                if 'date' in column_name.lower():
                    column_type = 'TIMESTAMP'
                elif 'id' in column_name.lower():
                    column_type = 'VARCHAR(50)'
                elif column_name in ('other_regular_habits', 'other_indulgence_before_offence', 'time_since_modus_operandi'):
                    column_type = 'TEXT'
                else:
                    column_type = 'VARCHAR(255)'
                
                with self.db_pool.get_connection_context() as conn:
                    with conn.cursor() as cur:
                        alter_sql = f"ALTER TABLE {IR_TABLE} ADD COLUMN IF NOT EXISTS {column_name} {column_type}"
                        cur.execute(alter_sql)
                        conn.commit()
                        logger.info(f"✅ Added column {column_name} ({column_type}) to {IR_TABLE}")
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
        """
        date_ranges = []
        current_date = parse_iso_date(start_date).date()
        end = parse_iso_date(end_date).date()
        
        while current_date <= end:
            chunk_end = current_date + timedelta(days=chunk_days - 1)
            if chunk_end > end:
                chunk_end = end
            
            date_ranges.append((
                current_date.strftime('%Y-%m-%d'),
                chunk_end.strftime('%Y-%m-%d')
            ))
            
            if chunk_end >= end:
                break
            
            next_start = chunk_end - timedelta(days=overlap_days - 1)
            current_date = next_start
    
        return date_ranges

    def fetch_ir_data_from_api(self, from_date: str, to_date: str) -> Optional[List[Dict[str, Any]]]:
        """
        Fetch IR data from API for given date range
        
        Args:
            from_date: Start datetime in ISO format (YYYY-MM-DDTHH:MM:SS)
            to_date: End datetime in ISO format (YYYY-MM-DDTHH:MM:SS)
        
        Returns:
            List of IR records or None if failed
        """
        # API uses query parameters: /interrogation-reports/v1?fromDate=YYYY-MM-DD&toDate=YYYY-MM-DD
        # Convert ISO datetime to date-only format (YYYY-MM-DD) for API compatibility
        # The API expects date-only format, not ISO format with time
        from_date_only = from_date.split('T')[0] if 'T' in from_date else from_date
        to_date_only = to_date.split('T')[0] if 'T' in to_date else to_date
        
        url = API_CONFIG['ir_url']
        params = {
            'fromDate': from_date_only,  # Date-only format (YYYY-MM-DD)
            'toDate': to_date_only       # Date-only format (YYYY-MM-DD)
        }
        headers = {
            'x-api-key': API_CONFIG['api_key']
        }
        
        for attempt in range(API_CONFIG['max_retries']):
            try:
                logger.debug(f"Fetching IR data: {from_date} to {to_date} (Attempt {attempt + 1})")
                logger.debug(f"API URL: {url}")
                logger.debug(f"API Params: {params}")
                response = requests.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=API_CONFIG['timeout']
                )
                logger.debug(f"Response status: {response.status_code}")
                logger.debug(f"Response URL: {response.url}")
                
                if response.status_code == 200:
                    data = response.json()
                    with self.stats_lock:
                        self.stats['total_api_calls'] += 1
                    
                    # Log the actual response for debugging
                    logger.debug(f"API Response URL: {response.url}")
                    logger.debug(f"API Response status field: {data.get('status')}")
                    
                    if data.get('status'):
                        records = data.get('data', [])
                        if records:
                            # Ensure it's a list
                            if isinstance(records, dict):
                                records = [records]
                            logger.info(f"✅ Fetched {len(records)} IR records for {from_date} to {to_date}")
                            return records
                        else:
                            logger.warning(f"⚠️  No IR records found for {from_date} to {to_date}")
                            return []
                    else:
                        # Log error details from API
                        error_info = data.get('error', [])
                        if error_info:
                            logger.error(f"⚠️  API returned status=false for {from_date} to {to_date}")
                            logger.error(f"API Error details: {json.dumps(error_info, indent=2)}")
                        else:
                            logger.warning(f"⚠️  API returned status=false for {from_date} to {to_date}: {data}")
                        return []
                
                elif response.status_code == 404:
                    logger.warning(f"⚠️  No data found for {from_date} to {to_date} (404)")
                    return []
                
                else:
                    # Log error response body for debugging
                    try:
                        error_data = response.json()
                        logger.error(f"API returned status code {response.status_code}")
                        logger.error(f"Error response: {json.dumps(error_data, indent=2)}")
                    except:
                        logger.error(f"API returned status code {response.status_code}")
                        logger.error(f"Error response text: {response.text[:500]}")
                    logger.warning(f"Retrying... (Attempt {attempt + 1})")
                    time.sleep(2 ** attempt)  # Exponential backoff
                    
            except requests.exceptions.Timeout:
                logger.warning(f"API timeout, retrying... (Attempt {attempt + 1})")
                time.sleep(2 ** attempt)
            except Exception as e:
                logger.error(f"API error: {e}")
                if attempt == API_CONFIG['max_retries'] - 1:
                    with self.stats_lock:
                        self.stats['failed_api_calls'] += 1
                        self.stats['errors'].append(f"{from_date} to {to_date}: {str(e)}")
                time.sleep(2 ** attempt)
        
        logger.error(f"❌ Failed to fetch IR data for {from_date} to {to_date} after {API_CONFIG['max_retries']} attempts")
        return None

    def get_existing_ir_record(self, ir_id: str, cursor) -> Optional[Dict[str, Any]]:
        """Get existing IR record from database with a snapshot used for fallback comparison."""
        cursor.execute(
            f"""
            SELECT
                interrogation_report_id,
                date_created,
                date_modified,
                crime_id,
                person_id,
                other_regular_habits,
                other_indulgence_before_offence,
                time_since_modus_operandi,
                is_in_jail,
                is_on_bail,
                is_absconding,
                is_normal_life,
                is_rehabilitated,
                is_dead,
                is_facing_trial,
                date_of_bail
            FROM {IR_TABLE}
            WHERE interrogation_report_id = %s
            """,
            (ir_id,)
        )
        result = cursor.fetchone()
        if result:
            return {
                'interrogation_report_id': result[0],
                'date_created': result[1],
                'date_modified': result[2],
                'snapshot': {
                    'crime_id': result[3],
                    'person_id': result[4],
                    'other_regular_habits': result[5],
                    'other_indulgence_before_offence': result[6],
                    'time_since_modus_operandi': result[7],
                    'is_in_jail': result[8],
                    'is_on_bail': result[9],
                    'is_absconding': result[10],
                    'is_normal_life': result[11],
                    'is_rehabilitated': result[12],
                    'is_dead': result[13],
                    'is_facing_trial': result[14],
                    'date_of_bail': result[15]
                }
            }
        return None

    def _build_main_snapshot_from_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Build a comparable snapshot from API payload for fallback updates when DATE_MODIFIED is missing."""
        pw = record.get('PRESENT_WHEREABOUTS', {})
        in_jail = pw.get('IN_JAIL', {})
        on_bail = pw.get('ON_BAIL', {})
        absconding = pw.get('ABSCONDING', {})
        normal_life = pw.get('NORMAL_LIFE', {})
        rehabilitated = pw.get('REHABILITATED', {})
        dead = pw.get('DEAD', {})
        facing_trial = pw.get('FACING_TRIAL', {})

        return {
            'crime_id': record.get('CRIME_ID'),
            'person_id': normalize_person_id(record.get('PERSON_ID')),
            'other_regular_habits': record.get('OTHER_REGULAR_HABITS'),
            'other_indulgence_before_offence': (
                record.get('OTHER_INDULGENCE_BEFORE_OFFENCE')
                if record.get('OTHER_INDULGENCE_BEFORE_OFFENCE') is not None
                else record.get('OTHER_INDULGANCE_BEFORE_OFFENCE')
            ),
            'time_since_modus_operandi': record.get('TIME_SINCE_MODUS_OPERANDI'),
            'is_in_jail': in_jail.get('IS_IN_JAIL'),
            'is_on_bail': on_bail.get('IS_ON_BAIL'),
            'is_absconding': absconding.get('IS_ABSCONDING'),
            'is_normal_life': normal_life.get('IS_NORMAL_LIFE'),
            'is_rehabilitated': rehabilitated.get('IS_REHABILITATED'),
            'is_dead': dead.get('IS_DEAD'),
            'is_facing_trial': facing_trial.get('IS_FACING_TRIAL'),
            'date_of_bail': parse_date(on_bail.get('DATE_OF_BAIL')) if on_bail.get('DATE_OF_BAIL') else None
        }

    def should_update_record(self, existing: Dict[str, Any], record: Dict[str, Any]) -> bool:
        """Determine if record should be updated using DATE_MODIFIED or fallback field-diff when missing."""
        if not existing:
            return False

        new_date_modified = parse_timestamp(record.get('DATE_MODIFIED'))
        if new_date_modified:
            existing_modified = existing.get('date_modified')
            if not existing_modified:
                return True
            if isinstance(existing_modified, datetime) and existing_modified.tzinfo is not None:
                existing_modified = existing_modified.replace(tzinfo=None)
            if new_date_modified.tzinfo is not None:
                new_date_modified = new_date_modified.replace(tzinfo=None)
            return new_date_modified > existing_modified

        # Fallback comparison when DATE_MODIFIED is missing.
        existing_snapshot = existing.get('snapshot', {})
        new_snapshot = self._build_main_snapshot_from_record(record)
        has_diff = existing_snapshot != new_snapshot
        if has_diff:
            logger.debug(
                "DATE_MODIFIED missing for IR %s; fallback field-diff detected change.",
                record.get('INTERROGATION_REPORT_ID')
            )
        return has_diff

    def delete_related_records(self, ir_id: str, cursor):
        """Delete all related records for an IR before re-inserting."""
        tables = [
            IR_FAMILY_HISTORY_TABLE,
            IR_LOCAL_CONTACTS_TABLE,
            IR_REGULAR_HABITS_TABLE,
            IR_TYPES_OF_DRUGS_TABLE,
            IR_SIM_DETAILS_TABLE,
            IR_FINANCIAL_HISTORY_TABLE,
            IR_CONSUMER_DETAILS_TABLE,
            IR_MODUS_OPERANDI_TABLE,
            IR_PREVIOUS_OFFENCES_TABLE,
            IR_DEFENCE_COUNSEL_TABLE,
            IR_ASSOCIATE_DETAILS_TABLE,
            IR_SHELTER_TABLE,
            IR_MEDIA_TABLE,
            IR_INTERROGATION_REPORT_REFS_TABLE,
            IR_DOPAMS_LINKS_TABLE,
            IR_INDULGANCE_BEFORE_OFFENCE_TABLE,
            IR_PROPERTY_DISPOSAL_TABLE,
            IR_REGULARIZATION_TRANSIT_WARRANTS_TABLE,
            IR_EXECUTION_OF_NBW_TABLE,
            IR_PENDING_NBW_TABLE,
            IR_SURETIES_TABLE,
            IR_JAIL_SENTENCE_TABLE,
            IR_NEW_GANG_FORMATION_TABLE,
            IR_CONVICTION_ACQUITTAL_TABLE
        ]
        
        for table in tables:
            cursor.execute(f"DELETE FROM {table} WHERE interrogation_report_id = %s", (ir_id,))

    def insert_main_record(self, record: Dict[str, Any], cursor, is_update: bool = False):
        """Insert or update main interrogation_reports record."""
        pf = record.get('PHYSICAL_FEATURES', {})
        sep = record.get('SOCIO_ECONOMIC_PROFILE', {})
        coo = record.get('COMMISSION_OF_OFFENCE', {})
        soas = record.get('SHARE_OF_AMOUNT_SPENT', {})
        pw = record.get('PRESENT_WHEREABOUTS', {})
        
        in_jail = pw.get('IN_JAIL', {})
        on_bail = pw.get('ON_BAIL', {})
        absconding = pw.get('ABSCONDING', {})
        normal_life = pw.get('NORMAL_LIFE', {})
        rehabilitated = pw.get('REHABILITATED', {})
        dead = pw.get('DEAD', {})
        facing_trial = pw.get('FACING_TRIAL', {})
        
        # Handle LANGUAGE_OR_DIALECT array
        lang_dialect = pf.get('LANGUAGE_OR_DIALECT', [])
        if not isinstance(lang_dialect, list):
            lang_dialect = []
        
        main_values = (
            record.get('INTERROGATION_REPORT_ID'),
            record.get('CRIME_ID'),
            normalize_person_id(record.get('PERSON_ID')),
        pf.get('BEARD'),
        pf.get('BUILD'),
        pf.get('BURN_MARKS'),
        pf.get('COLOR'),
        pf.get('DEFORMITIES_OR_PECULIARITIES'),
        pf.get('DEFORMITIES'),
        pf.get('EAR'),
        pf.get('EYES'),
        pf.get('FACE'),
        pf.get('HAIR'),
        pf.get('HEIGHT'),
        pf.get('IDENTIFICATION_MARKS'),
        lang_dialect,
        pf.get('LEUCODERMA'),
        pf.get('MOLE'),
        pf.get('MUSTACHE'),
        pf.get('NOSE'),
        pf.get('SCAR'),
        pf.get('TATTOO'),
        pf.get('TEETH'),
        sep.get('LIVING_STATUS'),
        sep.get('MARITAL_STATUS'),
        sep.get('EDUCATION'),
        sep.get('OCCUPATION'),
        sep.get('INCOME_GROUP'),
        coo.get('OFFENCE_TIME'),
        coo.get('OTHER_OFFENCE_TIME'),
        soas.get('SHARE_OF_AMOUNT_SPENT'),
        soas.get('OTHER_SHARE_OF_AMOUNT_SPENT'),
        soas.get('REMARKS'),
        in_jail.get('IS_IN_JAIL'),
        in_jail.get('FROM_WHERE_SENT_IN_JAIL'),
        in_jail.get('CRIME_NUM'),
        in_jail.get('DIST_UNIT'),
        on_bail.get('IS_ON_BAIL'),
        on_bail.get('FROM_WHERE_SENT_ON_BAIL'),
        on_bail.get('CRIME_NUM'),
        parse_date(on_bail.get('DATE_OF_BAIL')) if on_bail.get('DATE_OF_BAIL') else None,
        absconding.get('IS_ABSCONDING'),
        absconding.get('WANTED_IN_POLICE_STATION'),
        absconding.get('CRIME_NUM'),
        normal_life.get('IS_NORMAL_LIFE'),
        normal_life.get('EKING_LIVELIHOOD_BY_LABOR_WORK'),
        rehabilitated.get('IS_REHABILITATED'),
        rehabilitated.get('REHABILITATION_DETAILS'),
        dead.get('IS_DEAD'),
        dead.get('DEATH_DETAILS'),
        facing_trial.get('IS_FACING_TRIAL'),
        facing_trial.get('PS_NAME'),
        facing_trial.get('CRIME_NUM'),
            record.get('OTHER_REGULAR_HABITS'),
            record.get('OTHER_INDULGENCE_BEFORE_OFFENCE') if record.get('OTHER_INDULGENCE_BEFORE_OFFENCE') is not None else record.get('OTHER_INDULGANCE_BEFORE_OFFENCE'),
            record.get('TIME_SINCE_MODUS_OPERANDI'),
        parse_timestamp(record.get('DATE_CREATED')),
        parse_timestamp(record.get('DATE_MODIFIED'))
    )
    
        if is_update:
            # Update existing record
            update_sql = f"""
                UPDATE {IR_TABLE} SET
                    crime_id = %s, person_id = %s,
                    physical_beard = %s, physical_build = %s, physical_burn_marks = %s, physical_color = %s,
                    physical_deformities_or_peculiarities = %s, physical_deformities = %s, physical_ear = %s,
                    physical_eyes = %s, physical_face = %s, physical_hair = %s, physical_height = %s,
                    physical_identification_marks = %s, physical_language_or_dialect = %s,
                    physical_leucoderma = %s, physical_mole = %s, physical_mustache = %s, physical_nose = %s,
                    physical_scar = %s, physical_tattoo = %s, physical_teeth = %s,
                    socio_living_status = %s, socio_marital_status = %s, socio_education = %s,
                    socio_occupation = %s, socio_income_group = %s,
                    offence_time = %s, other_offence_time = %s,
                    share_of_amount_spent = %s, other_share_of_amount_spent = %s, share_remarks = %s,
                    is_in_jail = %s, from_where_sent_in_jail = %s, in_jail_crime_num = %s, in_jail_dist_unit = %s,
                    is_on_bail = %s, from_where_sent_on_bail = %s, on_bail_crime_num = %s, date_of_bail = %s,
                    is_absconding = %s, wanted_in_police_station = %s, absconding_crime_num = %s,
                    is_normal_life = %s, eking_livelihood_by_labor_work = %s,
                    is_rehabilitated = %s, rehabilitation_details = %s,
                    is_dead = %s, death_details = %s,
                    is_facing_trial = %s, facing_trial_ps_name = %s, facing_trial_crime_num = %s,
                    other_regular_habits = %s, other_indulgence_before_offence = %s,
                    time_since_modus_operandi = %s,
                    date_created = %s, date_modified = %s
                WHERE interrogation_report_id = %s
            """
            cursor.execute(update_sql, main_values[1:] + (main_values[0],))
        else:
            # Insert new record
            insert_sql = f"""
                INSERT INTO {IR_TABLE} (
                    interrogation_report_id, crime_id, person_id,
                    physical_beard, physical_build, physical_burn_marks, physical_color,
                    physical_deformities_or_peculiarities, physical_deformities, physical_ear,
                    physical_eyes, physical_face, physical_hair, physical_height,
                    physical_identification_marks, physical_language_or_dialect,
                    physical_leucoderma, physical_mole, physical_mustache, physical_nose,
                    physical_scar, physical_tattoo, physical_teeth,
                    socio_living_status, socio_marital_status, socio_education,
                    socio_occupation, socio_income_group,
                    offence_time, other_offence_time,
                    share_of_amount_spent, other_share_of_amount_spent, share_remarks,
                    is_in_jail, from_where_sent_in_jail, in_jail_crime_num, in_jail_dist_unit,
                    is_on_bail, from_where_sent_on_bail, on_bail_crime_num, date_of_bail,
                    is_absconding, wanted_in_police_station, absconding_crime_num,
                    is_normal_life, eking_livelihood_by_labor_work,
                    is_rehabilitated, rehabilitation_details,
                    is_dead, death_details,
                    is_facing_trial, facing_trial_ps_name, facing_trial_crime_num,
                    other_regular_habits, other_indulgence_before_offence,
                    time_since_modus_operandi,
                    date_created, date_modified
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """
            cursor.execute(insert_sql, main_values)
    
    def insert_related_records(self, record: Dict[str, Any], cursor):
        """Insert all related records for an IR. Person_id is optional - all data is inserted."""
        ir_id = record.get('INTERROGATION_REPORT_ID')
        
        # 1. Family History
        family_history = record.get('FAMILY_HISTORY', [])
        if family_history:
            try:
                family_values = []
                for fh in family_history:
                    fh_person_id = normalize_person_id(fh.get('PERSON_ID'))
                    family_values.append((
                        ir_id, fh_person_id, fh.get('RELATION'),
                        fh.get('FAMILY_MEMBER_PECULIARITY'), fh.get('CRIMINAL_BACKGROUND', False),
                        fh.get('IS_ALIVE', True), fh.get('FAMILY_STAY_TOGETHER', True)
                    ))
                
                if family_values:
                    execute_values(
                        cursor,
                        f"""INSERT INTO {IR_FAMILY_HISTORY_TABLE} 
                           (interrogation_report_id, person_id, relation, family_member_peculiarity,
                            criminal_background, is_alive, family_stay_together)
                           VALUES %s""",
                        family_values
                    )
            except Exception as e:
                logger.warning(f"Failed to insert family_history for {ir_id}: {e}")
                raise Exception(f"family_history: {str(e)}")
        
        # 2. Local Contacts
        local_contacts = record.get('LOCAL_CONTACTS', [])
        if local_contacts:
            try:
                contact_values = []
                for lc in local_contacts:
                    lc_person_id = normalize_person_id(lc.get('PERSON_ID'))
                    contact_values.append((
                        ir_id, lc_person_id, lc.get('TOWN'),
                        lc.get('ADDRESS'), lc.get('JURISDICTION_PS')
                    ))
                
                if contact_values:
                    execute_values(
                        cursor,
                        f"""INSERT INTO {IR_LOCAL_CONTACTS_TABLE} 
                           (interrogation_report_id, person_id, town, address, jurisdiction_ps)
                           VALUES %s""",
                        contact_values
                    )
            except Exception as e:
                logger.warning(f"Failed to insert local_contacts for {ir_id}: {e}")
                raise Exception(f"local_contacts: {str(e)}")
        
        # 3. Regular Habits
        regular_habits = record.get('REGULAR_HABITS', [])
        if regular_habits:
            habit_values = [(ir_id, habit) for habit in regular_habits if habit]
            if habit_values:
                execute_values(
                    cursor,
                    f"""INSERT INTO {IR_REGULAR_HABITS_TABLE} (interrogation_report_id, habit)
                       VALUES %s ON CONFLICT DO NOTHING""",
                    habit_values
                )
        
        # 4. Types of Drugs
        types_of_drugs = record.get('TYPES_OF_DRUGS', [])
        if types_of_drugs:
            try:
                drug_values = []
                for td in types_of_drugs:
                    supplier_id = normalize_person_id(td.get('SUPPLIER_PERSON_ID'))
                    receiver_id = normalize_person_id(td.get('RECEIVERS_PERSON_ID'))
                    purchase_amount = td.get('PURCHASE_AMOUNT_IN_INR')
                    if purchase_amount is None:
                        purchase_amount = td.get('PURCHASE_AMOUN_IN_INR')
                    drug_values.append((
                        ir_id, td.get('TYPE_OF_DRUG'), td.get('QUANTITY'),
                        purchase_amount, td.get('MODE_OF_PAYMENT'),
                        td.get('MODE_OF_TRANSPORT'), supplier_id, receiver_id
                    ))
                
                if drug_values:
                    execute_values(
                        cursor,
                        f"""INSERT INTO {IR_TYPES_OF_DRUGS_TABLE} 
                           (interrogation_report_id, type_of_drug, quantity, purchase_amount_in_inr,
                            mode_of_payment, mode_of_transport, supplier_person_id, receivers_person_id)
                           VALUES %s""",
                        drug_values
                    )
            except Exception as e:
                logger.warning(f"Failed to insert types_of_drugs for {ir_id}: {e}")
                raise Exception(f"types_of_drugs: {str(e)}")
        
        # 5. SIM Details
        sim_details = record.get('SIM_DETAILS', [])
        if sim_details:
            sim_values = []
            for sd in sim_details:
                sim_person_id = normalize_person_id(sd.get('PERSON_ID'))
                sim_values.append((
                    ir_id, sd.get('PHONE_NUMBER'), sd.get('SDR'),
                    sd.get('IMEI'), sd.get('TRUE_CALLER_NAME'), sim_person_id
                ))
            
            if sim_values:
                execute_values(
                    cursor,
                    f"""INSERT INTO {IR_SIM_DETAILS_TABLE} 
                       (interrogation_report_id, phone_number, sdr, imei, true_caller_name, person_id)
                       VALUES %s""",
                    sim_values
                )
        
        # 6. Financial History
        financial_history = record.get('FINANCIAL_HISTORY', [])
        if financial_history:
            try:
                financial_values = []
                for fh in financial_history:
                    account_holder_id = normalize_person_id(fh.get('ACCOUNT_HOLDER_PERSON_ID'))
                    financial_values.append((
                        ir_id, account_holder_id, fh.get('PAN_NO'),
                        fh.get('UPI_ID'), fh.get('NAME_OF_BANK'), fh.get('ACCOUNT_NUMBER'),
                        fh.get('BRANCH_NAME'), fh.get('IFSC_CODE'),
                        fh.get('IMMOVABLE_PROPERTY_ACQUIRED'), fh.get('MOVABLE_PROPERTY_ACQUIRED')
                    ))
                
                if financial_values:
                    execute_values(
                        cursor,
                        f"""INSERT INTO {IR_FINANCIAL_HISTORY_TABLE} 
                           (interrogation_report_id, account_holder_person_id, pan_no, upi_id,
                            name_of_bank, account_number, branch_name, ifsc_code,
                            immovable_property_acquired, movable_property_acquired)
                           VALUES %s""",
                        financial_values
                    )
            except Exception as e:
                logger.warning(f"Failed to insert financial_history for {ir_id}: {e}")
                raise Exception(f"financial_history: {str(e)}")
        
        # 7. Consumer Details
        consumer_details = record.get('CONSUMER_DETAILS', [])
        if consumer_details:
            consumer_values = []
            for cd in consumer_details:
                consumer_person_id = normalize_person_id(cd.get('CONSUMER_PERSON_ID'))
                consumer_values.append((
                    ir_id, consumer_person_id, cd.get('PLACE_OF_CONSUMPTION'),
                    cd.get('OTHER_SOURCES'), cd.get('OTHER_SOURCES_PHONE_NO'),
                    cd.get('AADHAR_CARD_NUMBER'), cd.get('AADHAR_CARD_NUMBER_PHONE_NO')
                ))
            
            if consumer_values:
                execute_values(
                    cursor,
                    f"""INSERT INTO {IR_CONSUMER_DETAILS_TABLE} 
                       (interrogation_report_id, consumer_person_id, place_of_consumption,
                        other_sources, other_sources_phone_no, aadhar_card_number, aadhar_card_number_phone_no)
                       VALUES %s""",
                    consumer_values
                )
        
        # 8. Modus Operandi
        modus_operandi = record.get('MODUS_OPERANDI', [])
        if modus_operandi:
            mo_values = [
                (ir_id, mo.get('CRIME_HEAD'), mo.get('CRIME_SUB_HEAD'),
                 mo.get('MODUS_OPERANDI'))
                for mo in modus_operandi
            ]
            execute_values(
                cursor,
                f"""INSERT INTO {IR_MODUS_OPERANDI_TABLE} 
                   (interrogation_report_id, crime_head, crime_sub_head, modus_operandi)
                   VALUES %s""",
                mo_values
            )
        
        # 9. Previous Offences Confessed
        previous_offences = record.get('PREVIOUS_OFFENCES_CONFESSED', [])
        if previous_offences:
            try:
                po_values = [
                    (
                        ir_id,
                        parse_date(po.get('ARREST_DATE')) if po.get('ARREST_DATE') else None,
                        po.get('ARRESTED_BY'),
                        po.get('ARREST_PLACE'),
                        po.get('CRIME_NUM'),
                        po.get('DIST_UNIT_DIVISION'),
                        po.get('GANG_MEMBER'),
                        po.get('INTERROGATED_BY'),
                        po.get('LAW_SECTION'),
                        po.get('OTHERS_IDENTIFY'),
                        po.get('PROPERTY_RECOVERED'),
                        po.get('PROPERTY_STOLEN'),
                        po.get('PS_CODE'),
                        po.get('REMARKS'),
                        po.get('CONVICTION_STATUS'),
                        po.get('BAIL_STATUS'),
                        po.get('COURT_NAME'),
                        po.get('JUDGE_NAME')
                    )
                    for po in previous_offences
                ]
                execute_values(
                    cursor,
                    f"""INSERT INTO {IR_PREVIOUS_OFFENCES_TABLE} 
                       (interrogation_report_id, arrest_date, arrested_by, arrest_place, crime_num,
                        dist_unit_division, gang_member, interrogated_by, law_section,
                        others_identify, property_recovered, property_stolen, ps_code, remarks,
                        conviction_status, bail_status, court_name, judge_name)
                       VALUES %s""",
                    po_values
                )
            except Exception as e:
                logger.warning(f"Failed to insert previous_offences_confessed for {ir_id}: {e}")
                # Don't fail the entire record if related data insertion fails
                # The main record is already inserted, so we log and continue
                logger.debug(f"Continuing despite previous_offences_confessed error for {ir_id}")
        
        # 10. Defence Counsel
        defence_counsel = record.get('DEFENCE_COUNSEL', [])
        if defence_counsel:
            try:
                dc_values = []
                for dc in defence_counsel:
                    dc_person_id = normalize_person_id(dc.get('DEFENCE_COUNSEL_PERSON_ID'))
                    dc_values.append((
                        ir_id, dc.get('DIST_DIVISION'), dc.get('PS_CODE'), dc.get('CRIME_NUM'),
                        dc.get('LAW_SECTION'), dc.get('SC_CC_NUM'), dc.get('DEFENCE_COUNSEL_ADDRESS'),
                        dc.get('DEFENCE_COUNSEL_PHONE'), dc.get('ASSISTANCE'), dc_person_id
                    ))
                
                if dc_values:
                    execute_values(
                        cursor,
                        f"""INSERT INTO {IR_DEFENCE_COUNSEL_TABLE} 
                           (interrogation_report_id, dist_division, ps_code, crime_num, law_section,
                            sc_cc_num, defence_counsel_address, defence_counsel_phone, assistance, defence_counsel_person_id)
                           VALUES %s""",
                        dc_values
                    )
            except Exception as e:
                logger.warning(f"Failed to insert defence_counsel for {ir_id}: {e}")
                raise Exception(f"defence_counsel: {str(e)}")
        
        # 11. Associate Details
        associate_details = record.get('ASSOCIATE_DETAILS', [])
        if associate_details:
            try:
                assoc_values = []
                for ad in associate_details:
                    ad_person_id = normalize_person_id(ad.get('PERSON_ID'))
                    assoc_values.append((
                        ir_id, ad_person_id, ad.get('GANG'), ad.get('RELATION')
                    ))
                
                if assoc_values:
                    execute_values(
                        cursor,
                        f"""INSERT INTO {IR_ASSOCIATE_DETAILS_TABLE} 
                           (interrogation_report_id, person_id, gang, relation)
                           VALUES %s""",
                        assoc_values
                    )
            except Exception as e:
                logger.warning(f"Failed to insert associate_details for {ir_id}: {e}")
                raise Exception(f"associate_details: {str(e)}")
        
        # 12. Shelter
        shelter = record.get('SHELTER', [])
        if shelter:
            shelter_values = [
                (ir_id, sh.get('PREPARATION_OF_OFFENCE'), sh.get('AFTER_OFFENCE'),
                 sh.get('REGULAR_RESIDENCY'), sh.get('REMARKS'), sh.get('OTHER_REGULAR_RESIDENCY'))
                for sh in shelter
            ]
            execute_values(
                cursor,
                f"""INSERT INTO {IR_SHELTER_TABLE} 
                   (interrogation_report_id, preparation_of_offence, after_offence,
                    regular_residency, remarks, other_regular_residency)
                   VALUES %s""",
                shelter_values
            )
        
        # 13. Media
        media = record.get('MEDIA', [])
        if media:
            media_values = [(ir_id, media_id) for media_id in media if media_id]
            if media_values:
                execute_values(
                    cursor,
                    f"""INSERT INTO {IR_MEDIA_TABLE} (interrogation_report_id, media_id)
                       VALUES %s ON CONFLICT DO NOTHING""",
                    media_values
                )
        
        # 14. Interrogation Report Refs
        interrogation_report = record.get('INTERROGATION_REPORT', [])
        if interrogation_report:
            ir_ref_values = [(ir_id, ref_id) for ref_id in interrogation_report if ref_id]
            if ir_ref_values:
                execute_values(
                    cursor,
                    f"""INSERT INTO {IR_INTERROGATION_REPORT_REFS_TABLE} (interrogation_report_id, report_ref_id)
                       VALUES %s ON CONFLICT DO NOTHING""",
                    ir_ref_values
                )
        
        # 15. DOPAMS Links
        dopams_links = record.get('DOPAMS_LINKS', [])
        if dopams_links:
            dopams_values = [
                (ir_id, dl.get('PHONE_NUMBER'),
                 dl.get('DOPAMS_DATA') if isinstance(dl.get('DOPAMS_DATA'), list) else [])
                for dl in dopams_links
            ]
            execute_values(
                cursor,
                f"""INSERT INTO {IR_DOPAMS_LINKS_TABLE} (interrogation_report_id, phone_number, dopams_data)
                   VALUES %s""",
                dopams_values
            )

        # 16. INDULGANCE_BEFORE_OFFENCE
        indulgance_before_offence = record.get('INDULGANCE_BEFORE_OFFENCE', [])
        if indulgance_before_offence:
            ind_values = [(ir_id, value) for value in indulgance_before_offence if value]
            if ind_values:
                execute_values(
                    cursor,
                    f"""INSERT INTO {IR_INDULGANCE_BEFORE_OFFENCE_TABLE} (interrogation_report_id, indulgance)
                       VALUES %s""",
                    ind_values
                )

        # 17. PROPERTY_DISPOSAL
        property_disposal = record.get('PROPERTY_DISPOSAL', [])
        if property_disposal:
            pd_values = [
                (
                    ir_id,
                    pd.get('MODE_OF_DISPOSAL'),
                    pd.get('BUYER_NAME'),
                    pd.get('SOLD_AMOUNT_IN_INR'),
                    pd.get('LOCATION_OF_DISPOSAL'),
                    parse_date(pd.get('DATE_OF_DISPOSAL')) if pd.get('DATE_OF_DISPOSAL') else None,
                    pd.get('REMARKS')
                )
                for pd in property_disposal
            ]
            execute_values(
                cursor,
                f"""INSERT INTO {IR_PROPERTY_DISPOSAL_TABLE}
                   (interrogation_report_id, mode_of_disposal, buyer_name, sold_amount_in_inr,
                    location_of_disposal, date_of_disposal, remarks)
                   VALUES %s""",
                pd_values
            )

        # 18. REGULARIZATION_OF_TRANSIT_WARRANTS
        regularization_transit = record.get('REGULARIZATION_OF_TRANSIT_WARRANTS', [])
        if regularization_transit:
            rtw_values = [
                (
                    ir_id,
                    row.get('WARRANT_NUMBER'),
                    row.get('WARRANT_TYPE'),
                    parse_date(row.get('ISSUED_DATE')) if row.get('ISSUED_DATE') else None,
                    row.get('JURISDICTION_PS'),
                    row.get('CRIME_NUM'),
                    row.get('STATUS'),
                    row.get('REMARKS')
                )
                for row in regularization_transit
            ]
            execute_values(
                cursor,
                f"""INSERT INTO {IR_REGULARIZATION_TRANSIT_WARRANTS_TABLE}
                   (interrogation_report_id, warrant_number, warrant_type, issued_date,
                    jurisdiction_ps, crime_num, status, remarks)
                   VALUES %s""",
                rtw_values
            )

        # 19. EXECUTION_OF_NBW
        execution_of_nbw = record.get('EXECUTION_OF_NBW', [])
        if execution_of_nbw:
            enbw_values = [
                (
                    ir_id,
                    row.get('NBW_NUMBER'),
                    parse_date(row.get('ISSUED_DATE')) if row.get('ISSUED_DATE') else None,
                    parse_date(row.get('EXECUTED_DATE')) if row.get('EXECUTED_DATE') else None,
                    row.get('JURISDICTION_PS'),
                    row.get('CRIME_NUM'),
                    row.get('EXECUTED_BY'),
                    row.get('PLACE_OF_EXECUTION'),
                    row.get('REMARKS')
                )
                for row in execution_of_nbw
            ]
            execute_values(
                cursor,
                f"""INSERT INTO {IR_EXECUTION_OF_NBW_TABLE}
                   (interrogation_report_id, nbw_number, issued_date, executed_date,
                    jurisdiction_ps, crime_num, executed_by, place_of_execution, remarks)
                   VALUES %s""",
                enbw_values
            )

        # 20. PENDING_NBW
        pending_nbw = record.get('PENDING_NBW', [])
        if pending_nbw:
            pnbw_values = [
                (
                    ir_id,
                    row.get('NBW_NUMBER'),
                    parse_date(row.get('ISSUED_DATE')) if row.get('ISSUED_DATE') else None,
                    row.get('JURISDICTION_PS'),
                    row.get('CRIME_NUM'),
                    row.get('REASON_FOR_PENDING'),
                    parse_date(row.get('EXPECTED_EXECUTION_DATE')) if row.get('EXPECTED_EXECUTION_DATE') else None,
                    row.get('REMARKS')
                )
                for row in pending_nbw
            ]
            execute_values(
                cursor,
                f"""INSERT INTO {IR_PENDING_NBW_TABLE}
                   (interrogation_report_id, nbw_number, issued_date, jurisdiction_ps,
                    crime_num, reason_for_pending, expected_execution_date, remarks)
                   VALUES %s""",
                pnbw_values
            )

        # 21. SURETIES
        sureties = record.get('SURETIES', [])
        if sureties:
            sur_values = [
                (
                    ir_id,
                    normalize_person_id(row.get('SURETY_PERSON_ID')),
                    row.get('SURETY_NAME'),
                    row.get('RELATION_TO_ACCUSED'),
                    row.get('OCCUPATION'),
                    row.get('AADHAR_NUMBER'),
                    row.get('PAN_NUMBER'),
                    row.get('HOUSE_NO'),
                    row.get('STREET_ROAD_NO'),
                    row.get('LOCALITY_VILLAGE'),
                    row.get('AREA_MANDAL'),
                    row.get('DISTRICT'),
                    row.get('STATE_UT'),
                    row.get('PIN_CODE'),
                    row.get('PHONE_NUMBER'),
                    row.get('SURETY_AMOUNT_IN_INR'),
                    parse_date(row.get('DATE_OF_SURETY')) if row.get('DATE_OF_SURETY') else None,
                    row.get('REMARKS')
                )
                for row in sureties
            ]
            execute_values(
                cursor,
                f"""INSERT INTO {IR_SURETIES_TABLE}
                   (interrogation_report_id, surety_person_id, surety_name, relation_to_accused,
                    occupation, aadhar_number, pan_number, house_no, street_road_no,
                    locality_village, area_mandal, district, state_ut, pin_code,
                    phone_number, surety_amount_in_inr, date_of_surety, remarks)
                   VALUES %s""",
                sur_values
            )

        # 22. JAIL_SENTENCE
        jail_sentence = record.get('JAIL_SENTENCE', [])
        if jail_sentence:
            js_values = [
                (
                    ir_id,
                    row.get('CRIME_NUM'),
                    row.get('JURISDICTION_PS'),
                    row.get('LAW_SECTION'),
                    row.get('SENTENCE_TYPE'),
                    row.get('SENTENCE_DURATION_IN_MONTHS'),
                    parse_date(row.get('SENTENCE_START_DATE')) if row.get('SENTENCE_START_DATE') else None,
                    parse_date(row.get('SENTENCE_END_DATE')) if row.get('SENTENCE_END_DATE') else None,
                    row.get('SENTENCE_AMOUNT_IN_INR'),
                    row.get('JAIL_NAME'),
                    parse_date(row.get('DATE_OF_JAIL_ENTRY')) if row.get('DATE_OF_JAIL_ENTRY') else None,
                    parse_date(row.get('DATE_OF_JAIL_RELEASE')) if row.get('DATE_OF_JAIL_RELEASE') else None,
                    row.get('REMARKS')
                )
                for row in jail_sentence
            ]
            execute_values(
                cursor,
                f"""INSERT INTO {IR_JAIL_SENTENCE_TABLE}
                   (interrogation_report_id, crime_num, jurisdiction_ps, law_section,
                    sentence_type, sentence_duration_in_months, sentence_start_date,
                    sentence_end_date, sentence_amount_in_inr, jail_name,
                    date_of_jail_entry, date_of_jail_release, remarks)
                   VALUES %s""",
                js_values
            )

        # 23. NEW_GANG_FORMATION
        new_gang_formation = record.get('NEW_GANG_FORMATION', [])
        if new_gang_formation:
            ngf_values = [
                (
                    ir_id,
                    row.get('GANG_NAME'),
                    parse_date(row.get('GANG_FORMATION_DATE')) if row.get('GANG_FORMATION_DATE') else None,
                    row.get('NUMBER_OF_MEMBERS'),
                    row.get('LEADER_NAME'),
                    normalize_person_id(row.get('LEADER_PERSON_ID')),
                    row.get('GANG_OBJECTIVE'),
                    row.get('CRIMINAL_HISTORY'),
                    row.get('JURISDICTION_PS'),
                    row.get('ACTIVE'),
                    row.get('REMARKS')
                )
                for row in new_gang_formation
            ]
            execute_values(
                cursor,
                f"""INSERT INTO {IR_NEW_GANG_FORMATION_TABLE}
                   (interrogation_report_id, gang_name, gang_formation_date, number_of_members,
                    leader_name, leader_person_id, gang_objective, criminal_history,
                    jurisdiction_ps, active, remarks)
                   VALUES %s""",
                ngf_values
            )

        # 24. CONVICTION_ACQUITTAL
        conviction_acquittal = record.get('CONVICTION_ACQUITTAL', [])
        if conviction_acquittal:
            ca_values = [
                (
                    ir_id,
                    row.get('CRIME_NUM'),
                    row.get('JURISDICTION_PS'),
                    row.get('COURT_NAME'),
                    row.get('JUDGE_NAME'),
                    row.get('LAW_SECTION'),
                    row.get('VERDICT'),
                    parse_date(row.get('VERDICT_DATE')) if row.get('VERDICT_DATE') else None,
                    row.get('REASON_IF_ACQUITTED'),
                    row.get('CONVICTION_REMARKS'),
                    row.get('FINE_AMOUNT_IN_INR'),
                    row.get('SENTENCE_IF_CONVICTED'),
                    row.get('APPEAL_STATUS'),
                    row.get('APPEAL_COURT')
                )
                for row in conviction_acquittal
            ]
            execute_values(
                cursor,
                f"""INSERT INTO {IR_CONVICTION_ACQUITTAL_TABLE}
                   (interrogation_report_id, crime_num, jurisdiction_ps, court_name,
                    judge_name, law_section, verdict, verdict_date,
                    reason_if_acquitted, conviction_remarks, fine_amount_in_inr,
                    sentence_if_convicted, appeal_status, appeal_court)
                   VALUES %s""",
                ca_values
            )

    def process_ir_record(self, record: Dict[str, Any], conn, cursor) -> bool:
        """
        Process a single IR record (insert or update).
        
        Args:
            record: IR record dictionary
        
        Returns:
            True if successful, False otherwise
        """
        ir_id = record.get('INTERROGATION_REPORT_ID')
        crime_id = record.get('CRIME_ID')
        
        if not ir_id:
            logger.warning("Record missing INTERROGATION_REPORT_ID, skipping")
            with self.stats_lock:
                self.stats['total_ir_failed'] += 1
            return False

        if crime_id and crime_id not in self.crime_ids:
            self.queue_pending_fk(record, crime_id, conn, cursor)
            logger.debug(f"⏳ IR {ir_id}: crime_id {crime_id} not in crimes table — queued for retry")
            return False
        
        try:
            # Check if record exists
            existing = self.get_existing_ir_record(ir_id, cursor)
            
            if existing:
                # Check if update is needed
                if self.should_update_record(existing, record):
                    logger.debug(f"Updating record: {ir_id}")
                    # Delete related records before re-inserting
                    self.delete_related_records(ir_id, cursor)
                    # Update main record
                    try:
                        self.insert_main_record(record, cursor, is_update=True)
                    except psycopg2_errors.ForeignKeyViolation as fk_error:
                        error_str = str(fk_error)
                        if 'crime_id' in error_str.lower():
                            logger.warning(f"Record {ir_id}: crime_id {crime_id} foreign key violation. Crime must exist in crimes table.")
                            with self.stats_lock:
                                self.stats['total_ir_failed'] += 1
                                self.stats['errors'].append(f"IR {ir_id}: crime_id {crime_id} not found in crimes table")
                            # Rollback immediately to clear aborted transaction state
                            try:
                                conn.rollback()
                            except:
                                pass
                            return False
                        raise
                    # Re-insert related records (atomic requirement: fail whole record on dependent insert error)
                    try:
                        self.insert_related_records(record, cursor)
                    except Exception as e:
                        logger.error(f"Dependent insert failed for {ir_id}; rolling back record: {e}")
                        conn.rollback()
                        with self.stats_lock:
                            self.stats['total_ir_failed'] += 1
                            self.stats['errors'].append(f"IR {ir_id}: dependent insert failed ({e})")
                        return False
                    with self.stats_lock:
                        self.stats['total_ir_updated'] += 1
                    return True
                else:
                    logger.debug(f"Record {ir_id} is up-to-date, skipping")
                    with self.stats_lock:
                        self.stats['total_ir_no_change'] += 1
                    return True
            else:
                # New record
                logger.debug(f"Inserting new record: {ir_id}")
                try:
                    self.insert_main_record(record, cursor, is_update=False)
                except psycopg2_errors.ForeignKeyViolation as fk_error:
                    error_str = str(fk_error)
                    if 'crime_id' in error_str.lower():
                        logger.warning(f"Record {ir_id}: crime_id {crime_id} foreign key violation. Crime must exist in crimes table.")
                        with self.stats_lock:
                            self.stats['total_ir_failed'] += 1
                            self.stats['errors'].append(f"IR {ir_id}: crime_id {crime_id} not found in crimes table")
                        # Rollback immediately to clear aborted transaction state
                        try:
                            conn.rollback()
                        except:
                            pass
                        return False
                    raise
                # Insert related records (atomic requirement: fail whole record on dependent insert error)
                try:
                    self.insert_related_records(record, cursor)
                except Exception as e:
                    logger.error(f"Dependent insert failed for {ir_id}; rolling back record: {e}")
                    conn.rollback()
                    with self.stats_lock:
                        self.stats['total_ir_failed'] += 1
                        self.stats['errors'].append(f"IR {ir_id}: dependent insert failed ({e})")
                    return False
                with self.stats_lock:
                    self.stats['total_ir_inserted'] += 1
                return True
            
        except psycopg2_errors.ForeignKeyViolation as e:
            # Handle foreign key constraint violations gracefully
            error_str = str(e)
            if 'crime_id' in error_str.lower():
                logger.warning(f"Record {ir_id}: crime_id {crime_id} foreign key violation. Crime must exist in crimes table.")
                with self.stats_lock:
                    self.stats['total_ir_failed'] += 1
                    self.stats['errors'].append(f"IR {ir_id}: crime_id {crime_id} not found in crimes table")
                # Rollback immediately to clear aborted transaction state
                try:
                    conn.rollback()
                except:
                    pass
                return False
            else:
                logger.error(f"Foreign key violation for record {ir_id}: {e}")
                with self.stats_lock:
                    self.stats['errors'].append(f"IR {ir_id}: {str(e)}")
                # Rollback before re-raising
                try:
                    conn.rollback()
                except:
                    pass
                raise
        except Exception as e:
            logger.error(f"Error processing record {ir_id}: {e}")
            with self.stats_lock:
                self.stats['errors'].append(f"IR {ir_id}: {str(e)}")
            raise
    
    def process_date_range(self, from_date: str, to_date: str, table_columns: Set[str] = None):
        """Process IR records for a specific date range"""
        logger.info(f"📅 Processing: {from_date} to {to_date}")
        
        # Initialize chunk-level statistics
        chunk_stats = {
            'inserted': 0,
            'updated': 0,
            'no_change': 0,
            'failed': 0
        }
        
        # Store initial stats to calculate chunk differences
        initial_inserted = self.stats['total_ir_inserted']
        initial_updated = self.stats['total_ir_updated']
        initial_no_change = self.stats['total_ir_no_change']
        initial_failed = self.stats['total_ir_failed']
        
        # Fetch IR records from API
        records = self.fetch_ir_data_from_api(from_date, to_date)
        
        if records is None:
            logger.error(f"❌ Failed to fetch IR records for {from_date} to {to_date}")
            chunk_stats['failed'] = 1  # API call failed
            self.stats['total_ir_failed'] += 1
            return
        
        if not records:
            logger.info(f"ℹ️  No IR records found for {from_date} to {to_date} - continuing to next chunk")
            return
        
        # Check for schema evolution if we got data
        if table_columns is not None and len(records) > 0:
            # Check for new fields in first record
            new_fields = self.detect_new_fields(records[0], table_columns)
            if new_fields:
                logger.info(f"🔍 New fields detected in API response: {list(new_fields.keys())}")
                # Add new columns to table
                for api_field, db_column in new_fields.items():
                    if self.add_column_to_table(db_column):
                        # Update table_columns set
                        table_columns.add(db_column)
                # Update existing records from start_date to current chunk end_date
                self.update_existing_records_with_new_fields(new_fields, to_date)
        
        # Process each record
        with self.stats_lock:
            self.stats['total_ir_fetched'] += len(records)
            
        def process_record_worker(record):
            try:
                with self.db_pool.get_connection_context() as conn:
                    with conn.cursor() as cur:
                        result = self.process_ir_record(record, conn, cur)
                        if result:
                            conn.commit()
            except Exception as e:
                logger.error(f"Worker thread error: {e}")
                with self.stats_lock:
                    self.stats['total_ir_failed'] += 1
        
        requested_workers = int(os.environ.get('MAX_WORKERS', min(32, (os.cpu_count() or 1) * 4)))
        max_workers = compute_safe_workers(self.db_pool, requested_workers)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            list(executor.map(process_record_worker, records))
        
        with self.stats_lock:
            # Calculate chunk statistics
            chunk_stats['inserted'] = self.stats['total_ir_inserted'] - initial_inserted
            chunk_stats['updated'] = self.stats['total_ir_updated'] - initial_updated
            chunk_stats['no_change'] = self.stats['total_ir_no_change'] - initial_no_change
            chunk_stats['failed'] = self.stats['total_ir_failed'] - initial_failed
        
        # Log chunk statistics
        logger.info(f"✅ Completed: {from_date} to {to_date}")
        logger.info(f"   📊 Chunk Stats - Inserted: {chunk_stats['inserted']}, Updated: {chunk_stats['updated']}, "
                   f"No Change: {chunk_stats['no_change']}, Failed: {chunk_stats['failed']}")
    
    def run(self):
        """Main ETL execution"""
        logger.info("=" * 80)
        logger.info("🚀 DOPAMAS ETL Pipeline - Interrogation Reports API")
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
            # Ensure pending table exists
            self.ensure_pending_table()
            
            # Ensure schema migrations are applied (9 new tables + fixes)
            self.ensure_schema_exists()
            
            # Load crime IDs into memory
            self.load_crime_ids()

            # Get effective start date (check if table has data)
            effective_start_date = self.get_effective_start_date()
            logger.info(f"Effective Start Date: {effective_start_date}")
            
            # Get table columns for schema evolution
            table_columns = self.get_table_columns(IR_TABLE)
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
            logger.info("")
            
            # Process each date range with progress bar
            for from_date, to_date in tqdm(date_ranges, desc="Processing date ranges", unit="range"):
                # Process the chunk (will check for schema evolution and process data)
                self.process_date_range(from_date, to_date, table_columns)
                time.sleep(1)  # Be nice to the API
            
            # Retry pending FK records
            self.retry_pending_fk()

            # Get database counts
            with self.db_pool.get_connection_context() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT COUNT(*) FROM {IR_TABLE}")
                    db_ir_count = cur.fetchone()[0]
                    cur.execute(f"SELECT COUNT(*) FROM {PENDING_FK_TABLE} WHERE resolved = FALSE")
                    pending_count = cur.fetchone()[0]
            
            # Print final statistics
            logger.info("")
            logger.info("=" * 80)
            logger.info("📊 FINAL STATISTICS")
            logger.info("=" * 80)
            logger.info(f"📡 API CALLS:")
            logger.info(f"  Total API Calls:          {self.stats['total_api_calls']}")
            logger.info(f"  Failed API Calls:         {self.stats['failed_api_calls']}")
            logger.info(f"")
            logger.info(f"📥 FROM API:")
            logger.info(f"  Total IR Records Fetched: {self.stats['total_ir_fetched']}")
            logger.info(f"")
            logger.info(f"💾 TO DATABASE:")
            logger.info(f"  Total Inserted (New):     {self.stats['total_ir_inserted']}")
            logger.info(f"  Total Updated:            {self.stats['total_ir_updated']}")
            logger.info(f"  Total No Change:          {self.stats['total_ir_no_change']}")
            logger.info(f"  Total Failed:             {self.stats['total_ir_failed']}")
            logger.info(f"  Total in DB:              {db_ir_count}")
            logger.info(f"")
            logger.info(f"⏳ PENDING FK RETRY QUEUE:")
            logger.info(f"  Queued (missing crime_id): {self.stats['total_pending_fk']}")
            logger.info(f"  Retried → Resolved:        {self.stats['total_retried_ok']}")
            logger.info(f"  Retried → Still Missing:   {self.stats['total_retried_still_missing']}")
            logger.info(f"  Remaining in Queue:        {pending_count}")
            logger.info(f"")
            logger.info(f"📊 COVERAGE:")
            if self.stats['total_ir_fetched'] > 0:
                coverage = ((self.stats['total_ir_inserted'] + self.stats['total_ir_updated']) / self.stats['total_ir_fetched']) * 100
                logger.info(f"  API → DB Coverage:       {coverage:.2f}%")
            logger.info(f"")
            logger.info(f"❌ Errors:                   {len(self.stats['errors'])}")
            logger.info("=" * 80)
            
            if self.stats['errors']:
                logger.warning("⚠️  Errors encountered:")
                for error in self.stats['errors'][:10]:  # Show first 10 errors
                    logger.warning(f"  - {error}")
                if len(self.stats['errors']) > 10:
                    logger.warning(f"  ... and {len(self.stats['errors']) - 10} more")
            
            logger.info("✅ ETL Pipeline completed successfully!")
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
            self.close_db()


def main():
    """Main entry point"""
    etl = InterrogationReportsETL()
    success = etl.run()
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()

