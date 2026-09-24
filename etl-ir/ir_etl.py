#!/usr/bin/env python3
"""
DOPAMAS ETL Pipeline - Interrogation Reports (IR) API
Fetches IR data in date-range chunks with overlap and loads into normalized PostgreSQL tables
"""

import sys
import os
import time
import random
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
import uuid

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from db_pooling import PostgreSQLConnectionPool, compute_safe_workers
except ImportError:
    pass
from env_utils import get_etl_run_id

try:
    from etl_fk_retry_queue import push_fk_failure, drain_fk_queue as _drain_fk_queue
except ImportError:
    push_fk_failure = None
    _drain_fk_queue = None


from config import DB_CONFIG, API_CONFIG, ETL_CONFIG, LOG_CONFIG, TABLE_CONFIG

# CCTNS V2 source-provenance constants (see migrations/2026-09-23_add_cctns_provenance_columns.sql)
SOURCE_SYSTEM = 'CCTNS_V2'
SOURCE_ENDPOINT = '/interrogation-reports/v1/'
ETL_RUN_ID = get_etl_run_id()

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
CRIMES_TABLE = TABLE_CONFIG.get('crimes', 'crimes')

def parse_iso_date(date_str: str) -> datetime:
    """Parse ISO 8601 date string (with optional time component) to datetime."""
    if 'T' in date_str or ' ' in date_str:
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


def _clean(value):
    """Blank-string -> None, otherwise pass through."""
    if isinstance(value, str):
        stripped = value.strip()
        return stripped if stripped else None
    return value


def _pluck(items: List[Dict], *keys, cast=None) -> List:
    """Extract one field (first matching key) from each dict in items, in order.
    Non-dict items are skipped. Used to build the parallel-array columns for
    IR sub-entities with a confirmed stable schema (see cctns-v2_schema.sql)."""
    out = []
    for item in (items or []):
        if not isinstance(item, dict):
            continue
        val = None
        for k in keys:
            val = item.get(k)
            if val is not None:
                break
        val = _clean(val)
        if cast is not None and val is not None:
            try:
                val = cast(val)
            except (TypeError, ValueError):
                pass
        out.append(val)
    return out


def build_ir_arrays(record: Dict[str, Any]) -> Dict[str, List]:
    """
    Build the parallel-array columns for the 11 IR sub-entities with a
    confirmed stable field schema from real captured CCTNS V2 responses
    (see cctns-v2_schema.sql / cctns-v2_schema_mapping_report.md for the
    field-level evidence). Each source array item becomes one position
    across all of that group's parallel arrays.
    """
    family_history = record.get('FAMILY_HISTORY') or []
    associate_details = record.get('ASSOCIATE_DETAILS') or []
    local_contacts = record.get('LOCAL_CONTACTS') or []
    modus_operandi = record.get('MODUS_OPERANDI') or []
    shelter = record.get('SHELTER') or []
    dopams_links = record.get('DOPAMS_LINKS') or []
    types_of_drugs = record.get('TYPES_OF_DRUGS') or []
    consumer_details = record.get('CONSUMER_DETAILS') or []
    financial_history = record.get('FINANCIAL_HISTORY') or []
    sim_details = record.get('SIM_DETAILS') or []
    regular_habits = record.get('REGULAR_HABITS') or []

    dopams_phone_numbers = _pluck(dopams_links, 'PHONE_NUMBER')
    dopams_data_joined = []
    for item in dopams_links:
        if not isinstance(item, dict):
            continue
        data = item.get('DOPAMS_DATA')
        if isinstance(data, list):
            dopams_data_joined.append(','.join(str(_clean(x)) for x in data if _clean(x)) or None)
        else:
            dopams_data_joined.append(_clean(data))

    return {
        'family_history_person_ids': _pluck(family_history, 'PERSON_ID'),
        'family_history_relations': _pluck(family_history, 'RELATION'),
        'family_history_peculiarities': _pluck(family_history, 'FAMILY_MEMBER_PECULIARITY'),
        'family_history_criminal_background': _pluck(family_history, 'CRIMINAL_BACKGROUND'),
        'family_history_is_alive': _pluck(family_history, 'IS_ALIVE'),
        'family_history_stay_together': _pluck(family_history, 'FAMILY_STAY_TOGETHER'),

        'associate_person_ids': _pluck(associate_details, 'PERSON_ID'),
        'associate_gangs': _pluck(associate_details, 'GANG'),
        'associate_relations': _pluck(associate_details, 'RELATION'),

        'local_contact_person_ids': _pluck(local_contacts, 'PERSON_ID'),
        'local_contact_towns': _pluck(local_contacts, 'TOWN'),
        'local_contact_addresses': _pluck(local_contacts, 'ADDRESS'),
        'local_contact_jurisdiction_ps': _pluck(local_contacts, 'JURISDICTION_PS'),

        'mo_crime_heads': _pluck(modus_operandi, 'CRIME_HEAD'),
        'mo_crime_sub_heads': _pluck(modus_operandi, 'CRIME_SUB_HEAD'),
        'mo_descriptions': _pluck(modus_operandi, 'MODUS_OPERANDI'),

        'shelter_preparation_of_offence': _pluck(shelter, 'PREPARATION_OF_OFFENCE'),
        'shelter_after_offence': _pluck(shelter, 'AFTER_OFFENCE'),
        'shelter_regular_residency': _pluck(shelter, 'REGULAR_RESIDENCY'),
        'shelter_remarks': _pluck(shelter, 'REMARKS'),
        'shelter_other_regular_residency': _pluck(shelter, 'OTHER_REGULAR_RESIDENCY'),

        'dopams_link_phone_numbers': dopams_phone_numbers,
        'dopams_link_data': dopams_data_joined,

        'drug_types': _pluck(types_of_drugs, 'TYPE_OF_DRUG'),
        'drug_quantities': _pluck(types_of_drugs, 'QUANTITY'),
        'drug_purchase_amounts_inr': _pluck(types_of_drugs, 'PURCHASE_AMOUN_IN_INR', 'PURCHASE_AMOUNT_IN_INR'),
        'drug_modes_of_payment': _pluck(types_of_drugs, 'MODE_OF_PAYMENT'),
        'drug_modes_of_transport': _pluck(types_of_drugs, 'MODE_OF_TRANSPORT'),
        'drug_supplier_person_ids': _pluck(types_of_drugs, 'SUPPLIER_PERSON_ID'),
        'drug_receiver_person_ids': _pluck(types_of_drugs, 'RECEIVERS_PERSON_ID'),

        'consumer_person_ids': _pluck(consumer_details, 'CONSUMER_PERSON_ID'),
        'consumer_places_of_consumption': _pluck(consumer_details, 'PLACE_OF_CONSUMPTION'),
        'consumer_other_sources': _pluck(consumer_details, 'OTHER_SOURCES'),
        'consumer_other_sources_phone_nos': _pluck(consumer_details, 'OTHER_SOURCES_PHONE_NO'),
        'consumer_aadhar_numbers': _pluck(consumer_details, 'AADHAR_CARD_NUMBER'),
        'consumer_aadhar_phone_nos': _pluck(consumer_details, 'AADHAR_CARD_NUMBER_PHONE_NO'),

        'financial_account_holder_person_ids': _pluck(financial_history, 'ACCOUNT_HOLDER_PERSON_ID'),
        'financial_pan_nos': _pluck(financial_history, 'PAN_NO'),
        'financial_upi_ids': _pluck(financial_history, 'UPI_ID'),
        'financial_bank_names': _pluck(financial_history, 'NAME_OF_BANK'),
        'financial_account_numbers': _pluck(financial_history, 'ACCOUNT_NUMBER'),
        'financial_branch_names': _pluck(financial_history, 'BRANCH_NAME'),
        'financial_ifsc_codes': _pluck(financial_history, 'IFSC_CODE'),
        'financial_immovable_property': _pluck(financial_history, 'IMMOVABLE_PROPERTY_ACQUIRED'),
        'financial_movable_property': _pluck(financial_history, 'MOVABLE_PROPERTY_ACQUIRED'),

        'sim_phone_numbers': _pluck(sim_details, 'PHONE_NUMBER'),
        'sim_sdrs': _pluck(sim_details, 'SDR'),
        'sim_imeis': _pluck(sim_details, 'IMEI'),
        'sim_true_caller_names': _pluck(sim_details, 'TRUE_CALLER_NAME'),
        'sim_person_ids': _pluck(sim_details, 'PERSON_ID'),

        'regular_habits': [h for h in (regular_habits if isinstance(regular_habits, list) else []) if _clean(h)],
    }


IR_ARRAY_COLUMNS = [
    'family_history_person_ids', 'family_history_relations', 'family_history_peculiarities',
    'family_history_criminal_background', 'family_history_is_alive', 'family_history_stay_together',
    'associate_person_ids', 'associate_gangs', 'associate_relations',
    'local_contact_person_ids', 'local_contact_towns', 'local_contact_addresses', 'local_contact_jurisdiction_ps',
    'mo_crime_heads', 'mo_crime_sub_heads', 'mo_descriptions',
    'shelter_preparation_of_offence', 'shelter_after_offence', 'shelter_regular_residency',
    'shelter_remarks', 'shelter_other_regular_residency',
    'dopams_link_phone_numbers', 'dopams_link_data',
    'drug_types', 'drug_quantities', 'drug_purchase_amounts_inr', 'drug_modes_of_payment',
    'drug_modes_of_transport', 'drug_supplier_person_ids', 'drug_receiver_person_ids',
    'consumer_person_ids', 'consumer_places_of_consumption', 'consumer_other_sources',
    'consumer_other_sources_phone_nos', 'consumer_aadhar_numbers', 'consumer_aadhar_phone_nos',
    'financial_account_holder_person_ids', 'financial_pan_nos', 'financial_upi_ids',
    'financial_bank_names', 'financial_account_numbers', 'financial_branch_names',
    'financial_ifsc_codes', 'financial_immovable_property', 'financial_movable_property',
    'sim_phone_numbers', 'sim_sdrs', 'sim_imeis', 'sim_true_caller_names', 'sim_person_ids',
    'regular_habits',
]

# The 10 IR sub-entities never observed populated in any captured CCTNS V2
# response (see cctns-v2_schema.sql for the documented Case-5 reasoning).
# Stored as JSONB, keyed by their column name == lowercased API field name.
IR_JSONB_COLUMNS = {
    'conviction_acquittal': 'CONVICTION_ACQUITTAL',
    'defence_counsel': 'DEFENCE_COUNSEL',
    'execution_of_nbw': 'EXECUTION_OF_NBW',
    'jail_sentence': 'JAIL_SENTENCE',
    'new_gang_formation': 'NEW_GANG_FORMATION',
    'pending_nbw': 'PENDING_NBW',
    'previous_offences_confessed': 'PREVIOUS_OFFENCES_CONFESSED',
    'property_disposal': 'PROPERTY_DISPOSAL',
    'regularization_transit_warrants': 'REGULARIZATION_OF_TRANSIT_WARRANTS',
    'sureties': 'SURETIES',
}


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
            'total_ir_no_change': 0,
            'total_ir_failed': 0,
            'total_pending_fk': 0,
            'total_retried_ok': 0,
            'total_retried_still_missing': 0,
            'failed_api_calls': 0,
            'errors': []
        }
        # Thread-local stats for reduced lock contention
        self._thread_local_stats = threading.local()
    
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

    def queue_pending_fk(self, ir_raw: Dict, crime_id: str, conn, cursor):
        ir_id = ir_raw.get('INTERROGATION_REPORT_ID', 'unknown')
        if push_fk_failure:
            push_fk_failure(conn, source_table='interrogation_reports', record_id=ir_id,
                            record_json=json.dumps(ir_raw, default=str),
                            missing_fk_column='crime_id', missing_fk_value=crime_id)
            with self.stats_lock:
                self.stats['total_pending_fk'] += 1

    def _retry_ir_record(self, conn, record: Dict) -> bool:
        """Retry insertion of a queued IR record once its crime_id is present.

        Called by drain_fk_queue, which already deserializes the JSONB
        record_json column into a dict before calling this. Returns True on
        success, False if still unresolvable.
        """
        try:
            raw_data = record
            crime_id = raw_data.get('CRIME_ID')
            if crime_id not in self.crime_ids:
                return False
            
            with conn.cursor() as cur:
                success = self.insert_main_record(raw_data, cur)
            return success
        except Exception as e:
            logger.error(f"Error retrying IR record: {e}")
            return False

    def retry_pending_fk(self):
        if _drain_fk_queue:
            with self.db_pool.get_connection_context() as conn:
                _drain_fk_queue(conn, 'interrogation_reports', self._retry_ir_record)
    
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
        force_start = os.environ.get('FORCE_START_DATE')
        if force_start:
            logger.info(f"⚠️  FORCE_START_DATE override: {force_start}")
            return force_start
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
    
    def generate_date_ranges(self, start_date: str, end_date: str, chunk_days: int = 10, overlap_days: int = 0) -> List[Tuple[str, str]]:
        """
        Generate date ranges in larger chunks with no overlap for efficiency.
        Larger chunks (10 days) reduce API calls by 50% vs 5-day chunks.
        No overlap is safe with ordered timestamps.
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

            current_date = chunk_end + timedelta(days=1)

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

                elif response.status_code == 429:
                    # Rate limited — honor Retry-After header or use backoff with jitter
                    retry_after = response.headers.get('Retry-After')
                    wait_time = float(retry_after) if retry_after else min(60, 2 ** attempt + random.uniform(0, 1))
                    logger.warning(f"⚠️  API rate limited (429), waiting {wait_time:.1f}s before retry (attempt {attempt + 1})")
                    time.sleep(wait_time)

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
                    time.sleep(2 ** attempt + random.uniform(0, 0.5))  # Exponential backoff with jitter
                    
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

    def insert_main_record(self, record: Dict[str, Any], cursor, is_update: bool = False):
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

        lang_dialect = pf.get('LANGUAGE_OR_DIALECT', [])
        if not isinstance(lang_dialect, list):
            lang_dialect = []

        ir_id = record.get('INTERROGATION_REPORT_ID')

        # MEDIA[] and INTERROGATION_REPORT[] are file/document reference UUIDs,
        # not business data -- both live in file_media_bookkeeping, keyed by
        # parent_id=ir_id. Delete-then-insert on every (re)load so stale rows
        # from a prior, longer version of either array don't linger.
        now_utc = datetime.now(timezone.utc)
        cursor.execute(
            "DELETE FROM file_media_bookkeeping WHERE source_type = 'interrogation' "
            "AND source_field IN ('MEDIA', 'INTERROGATION_REPORT') AND parent_id = %s",
            (ir_id,)
        )
        for source_field, api_field in (('MEDIA', 'MEDIA'), ('INTERROGATION_REPORT', 'INTERROGATION_REPORT')):
            items = record.get(api_field, [])
            if not isinstance(items, list):
                continue
            values = [
                (ir_id, idx, file_id, SOURCE_SYSTEM, SOURCE_ENDPOINT, now_utc, ETL_RUN_ID)
                for idx, file_id in enumerate(items) if file_id
            ]
            if values:
                execute_values(
                    cursor,
                    """INSERT INTO file_media_bookkeeping
                           (parent_id, file_index, file_id, source_system, source_endpoint, fetched_at, etl_run_id,
                            source_type, source_field)
                       VALUES %s ON CONFLICT DO NOTHING""",
                    values,
                    template=f"(%s, %s, %s::uuid, %s, %s, %s, %s, 'interrogation', '{source_field}')"
                )

        # INDULGANCE_BEFORE_OFFENCE is a mixed-type API field: an empty array
        # in most records, a plain string in the rest -- never a real
        # multi-item array. Coerce to a nullable scalar.
        indulgance_raw = record.get('INDULGANCE_BEFORE_OFFENCE')
        indulgance_value = indulgance_raw if isinstance(indulgance_raw, str) and indulgance_raw.strip() else None

        array_values = build_ir_arrays(record)

        base_columns = [
            'interrogation_report_id', 'crime_id', 'person_id',
            'physical_beard', 'physical_build', 'physical_burn_marks', 'physical_color',
            'physical_deformities_or_peculiarities', 'physical_deformities', 'physical_ear',
            'physical_eyes', 'physical_face', 'physical_hair', 'physical_height',
            'physical_identification_marks', 'physical_language_or_dialect',
            'physical_leucoderma', 'physical_mole', 'physical_mustache', 'physical_nose',
            'physical_scar', 'physical_tattoo', 'physical_teeth',
            'socio_living_status', 'socio_marital_status', 'socio_education',
            'socio_occupation', 'socio_income_group',
            'offence_time', 'other_offence_time',
            'share_of_amount_spent', 'other_share_of_amount_spent', 'share_remarks',
            'is_in_jail', 'from_where_sent_in_jail', 'in_jail_crime_num', 'in_jail_dist_unit',
            'is_on_bail', 'from_where_sent_on_bail', 'on_bail_crime_num', 'date_of_bail',
            'is_absconding', 'wanted_in_police_station', 'absconding_crime_num',
            'is_normal_life', 'eking_livelihood_by_labor_work',
            'is_rehabilitated', 'rehabilitation_details',
            'is_dead', 'death_details',
            'is_facing_trial', 'facing_trial_ps_name', 'facing_trial_crime_num',
            'other_regular_habits', 'other_indulgence_before_offence', 'indulgance_before_offence',
            'time_since_modus_operandi',
            'date_created', 'date_modified', 'source_system', 'source_endpoint', 'fetched_at', 'etl_run_id',
        ]
        base_values = [
            ir_id,
            record.get('CRIME_ID'),
            normalize_person_id(record.get('PERSON_ID')),
            pf.get('BEARD'), pf.get('BUILD'), pf.get('BURN_MARKS'), pf.get('COLOR'),
            pf.get('DEFORMITIES_OR_PECULIARITIES'), pf.get('DEFORMITIES'), pf.get('EAR'),
            pf.get('EYES'), pf.get('FACE'), pf.get('HAIR'), pf.get('HEIGHT'),
            pf.get('IDENTIFICATION_MARKS'), lang_dialect,
            pf.get('LEUCODERMA'), pf.get('MOLE'), pf.get('MUSTACHE'), pf.get('NOSE'),
            pf.get('SCAR'), pf.get('TATTOO'), pf.get('TEETH'),
            sep.get('LIVING_STATUS'), sep.get('MARITAL_STATUS'), sep.get('EDUCATION'),
            sep.get('OCCUPATION'), sep.get('INCOME_GROUP'),
            coo.get('OFFENCE_TIME'), coo.get('OTHER_OFFENCE_TIME'),
            soas.get('SHARE_OF_AMOUNT_SPENT'), soas.get('OTHER_SHARE_OF_AMOUNT_SPENT'), soas.get('REMARKS'),
            in_jail.get('IS_IN_JAIL'), in_jail.get('FROM_WHERE_SENT'), in_jail.get('CRIME_NUM'), in_jail.get('DIST_UNIT'),
            on_bail.get('IS_ON_BAIL'), on_bail.get('FROM_WHERE_SENT'), on_bail.get('CRIME_NUM'),
            parse_iso_date(on_bail.get('DATE_OF_BAIL')) if on_bail.get('DATE_OF_BAIL') else None,
            absconding.get('IS_ABSCONDING'), absconding.get('WANTED_IN_POLICE_STATION'), absconding.get('CRIME_NUM'),
            normal_life.get('IS_NORMAL_LIFE'), normal_life.get('EKING_LIVELIHOOD_BY_LABOR_WORK'),
            rehabilitated.get('IS_REHABILITATED'), rehabilitated.get('REHABILITATION_DETAILS'),
            dead.get('IS_DEAD'), dead.get('DEATH_DETAILS'),
            facing_trial.get('IS_FACING_TRIAL'), facing_trial.get('PS_NAME'), facing_trial.get('CRIME_NUM'),
            record.get('OTHER_REGULAR_HABITS'),
            record.get('OTHER_INDULGENCE_BEFORE_OFFENCE') if record.get('OTHER_INDULGENCE_BEFORE_OFFENCE') is not None else record.get('OTHER_INDULGANCE_BEFORE_OFFENCE'),
            indulgance_value,
            record.get('TIME_SINCE_MODUS_OPERANDI'),
            parse_timestamp(record.get('DATE_CREATED')),
            parse_timestamp(record.get('DATE_MODIFIED')),
            SOURCE_SYSTEM, SOURCE_ENDPOINT, now_utc, ETL_RUN_ID,
        ]

        array_columns = list(IR_ARRAY_COLUMNS)
        array_col_values = [array_values[c] for c in array_columns]

        jsonb_columns = list(IR_JSONB_COLUMNS.keys())
        jsonb_values = [json.dumps(record.get(IR_JSONB_COLUMNS[c], [])) for c in jsonb_columns]

        all_columns = base_columns + array_columns + jsonb_columns
        all_values = base_values + array_col_values + jsonb_values

        placeholders = []
        for c in all_columns:
            placeholders.append('%s::jsonb' if c in IR_JSONB_COLUMNS else '%s')

        update_set = ',\n                '.join(
            f"{c} = EXCLUDED.{c}" for c in all_columns if c != 'interrogation_report_id'
        )

        query = f"""
            INSERT INTO {IR_TABLE} (
                {', '.join(all_columns)}
            ) VALUES (
                {', '.join(placeholders)}
            )
            ON CONFLICT (interrogation_report_id) DO UPDATE SET
                {update_set}
        """
        cursor.execute(query, tuple(all_values))
        return True

    def process_date_range(self, from_date: str, to_date: str, table_columns: Set[str] = None):
        """Fetch and load IR records for one date-range chunk."""
        chunk_range = f"{from_date} to {to_date}"
        logger.info(f"📅 Processing: {chunk_range}")

        records = self.fetch_ir_data_from_api(from_date, to_date)
        if records is None:
            logger.error(f"❌ Failed to fetch IR data for {chunk_range}")
            with self.stats_lock:
                self.stats['total_ir_failed'] += 1
            return
        if not records:
            logger.info(f"ℹ️  No IR records found for {chunk_range}")
            return

        with self.stats_lock:
            self.stats['total_ir_fetched'] += len(records)

        # Schema evolution: detect any new top-level fields the API started sending.
        if table_columns is not None and records:
            new_fields = self.detect_new_fields(records[0], table_columns)
            if new_fields:
                logger.info(f"🔍 New fields detected in API response: {list(new_fields.keys())}")
                for api_field, db_column in new_fields.items():
                    if self.add_column_to_table(db_column):
                        table_columns.add(db_column)
                self.update_existing_records_with_new_fields(new_fields, to_date)

        for record in records:
            ir_id = record.get('INTERROGATION_REPORT_ID')
            crime_id = record.get('CRIME_ID')
            if not ir_id:
                logger.warning("⚠️  IR record missing INTERROGATION_REPORT_ID, skipping")
                with self.stats_lock:
                    self.stats['total_ir_failed'] += 1
                continue

            try:
                with self.db_pool.get_connection_context() as conn:
                    with conn.cursor() as cur:
                        if crime_id not in self.crime_ids:
                            self.queue_pending_fk(record, crime_id, conn, cur)
                            conn.commit()
                            logger.debug(f"⏳ IR {ir_id}: crime_id {crime_id} not in crimes table — queued for retry")
                            continue

                        existing = self.get_existing_ir_record(ir_id, cur)
                        if existing and not self.should_update_record(existing, record):
                            with self.stats_lock:
                                self.stats['total_ir_no_change'] += 1
                            continue

                        self.insert_main_record(record, cur, is_update=bool(existing))
                        conn.commit()
                        with self.stats_lock:
                            if existing:
                                self.stats['total_ir_updated'] += 1
                            else:
                                self.stats['total_ir_inserted'] += 1
            except Exception as e:
                logger.error(f"❌ Error processing IR {ir_id}: {e}")
                with self.stats_lock:
                    self.stats['total_ir_failed'] += 1
                    self.stats['errors'].append(f"IR {ir_id}: {str(e)}")

        logger.info(f"✅ Completed: {chunk_range}")

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
            
            # Ensure schema migrations are applied (9 new tables + fixes)
            
            # Load crime IDs into memory
            self.load_crime_ids()

            # Get effective start date (check if table has data)
            effective_start_date = self.get_effective_start_date()
            logger.info(f"Effective Start Date: {effective_start_date}")
            
            # Get table columns for schema evolution
            table_columns = self.get_table_columns(IR_TABLE)
            logger.debug(f"Existing table columns: {sorted(table_columns)}")
            
            # Generate date ranges with NO overlap (more efficient)
            # API has 7-day limit on date ranges, so use 7 days (vs original 5)
            # No overlap = fewer redundant API calls
            date_ranges = self.generate_date_ranges(
                effective_start_date,
                calculated_end_date,
                chunk_days=7,   # API limit: max 7 days per call
                overlap_days=0  # Removed overlap for efficiency
            )

            logger.info(f"Date Range: {effective_start_date} to {calculated_end_date}")
            logger.info(f"Chunk Size: 7 days (API limit, no overlap for efficiency)")
            logger.info("=" * 80)

            logger.info(f"📊 Total date ranges to process: {len(date_ranges)}")
            logger.info(f"⚡ Optimization: Parallel API calls with 3-5 concurrent requests")
            logger.info("")

            # Process date ranges with parallel API calls
            if len(date_ranges) > 0:
                # Use ThreadPoolExecutor for concurrent API requests
                # Default: 8 workers (from .env), can override with MAX_API_WORKERS env var
                # Optimized: 4 → 8 reduces execution time by 30-40% (1319s → 800-950s)
                max_api_workers = int(os.environ.get('MAX_API_WORKERS', 8))
                max_api_workers = min(max_api_workers, len(date_ranges))  # Don't exceed number of ranges
                logger.info(f"⚡ Using {max_api_workers} parallel API workers (optimized from 4)")

                with ThreadPoolExecutor(max_workers=max_api_workers) as api_executor:
                    # Submit all API calls
                    futures = {}
                    for from_date, to_date in date_ranges:
                        future = api_executor.submit(self.process_date_range, from_date, to_date, table_columns)
                        futures[future] = (from_date, to_date)

                    # Process results as they complete (not in order)
                    with tqdm(total=len(date_ranges), desc="Processing date ranges", unit="range") as pbar:
                        for future in as_completed(futures):
                            from_date, to_date = futures[future]
                            try:
                                future.result()
                            except Exception as e:
                                logger.error(f"Error processing {from_date} to {to_date}: {e}")
                                with self.stats_lock:
                                    self.stats['failed_api_calls'] += 1
                            pbar.update(1)
            
            # Retry pending FK records
            self.retry_pending_fk()

            # Get database counts
            with self.db_pool.get_connection_context() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT COUNT(*) FROM {IR_TABLE}")
                    db_ir_count = cur.fetchone()[0]
                    cur.execute("SELECT COUNT(*) FROM etl_bookkeeping WHERE kind = 'fk_retry' AND module_name = 'interrogation_reports' AND resolved = FALSE")
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
    from db_pooling import PostgreSQLConnectionPool
    pool = PostgreSQLConnectionPool()
    pool.reset()

    etl = InterrogationReportsETL()
    success = etl.run()
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()

