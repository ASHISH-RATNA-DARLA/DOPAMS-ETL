#!/usr/bin/env python3
"""
DOPAMAS ETL Pipeline - Property Details API
Fetches property/seizure data in date-range chunks with overlap and loads into PostgreSQL
"""

import sys
import os
import time
import requests
import psycopg2
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from psycopg2.extras import Json
from decimal import Decimal, InvalidOperation
from datetime import datetime, timedelta
from tqdm import tqdm
import logging
import colorlog
from typing import List, Dict, Optional, Tuple, Set
from datetime import timezone, timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from db_pooling import PostgreSQLConnectionPool, compute_safe_workers
except ImportError:
    pass

import json

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
PROPERTIES_TABLE = TABLE_CONFIG.get('properties', 'properties')
CRIMES_TABLE = TABLE_CONFIG.get('crimes', 'crimes')
PENDING_FK_TABLE = 'properties_pending_fk'
PROPERTY_ADDITIONAL_DETAILS_TABLE = TABLE_CONFIG.get('property_additional_details', 'property_additional_details')
PROPERTY_MEDIA_TABLE = TABLE_CONFIG.get('property_media', 'property_media')


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


class PropertiesETL:
    """ETL Pipeline for Property Details API"""
    
    def __init__(self):
        self.db_pool = None
        self.crime_ids = set()
        self.stats_lock = threading.Lock()
        self.schema_lock = threading.Lock()
        self.has_property_additional_details_table = False
        self.has_property_media_table = False
        self.stats = {
            'total_api_calls': 0,
            'total_properties_fetched': 0,
            'total_properties_inserted': 0,
            'total_properties_updated': 0,
            'total_properties_no_change': 0,  # Records that exist but no changes needed
            'total_properties_failed': 0,  # Records that failed to process
            'total_pending_fk': 0,  # Records queued due to missing crime_id
            'total_retried_ok': 0,  # Pending records resolved on retry
            'total_retried_still_missing': 0,  # Pending records still missing crime_id after retry
            'failed_api_calls': 0,
            'errors': []
        }

    def table_exists(self, table_name: str) -> bool:
        """Check whether a table exists in current schema search path."""
        try:
            with self.db_pool.get_connection_context() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT EXISTS (
                            SELECT 1
                            FROM information_schema.tables
                            WHERE table_name = %s
                        )
                    """, (table_name,))
                    row = cur.fetchone()
                    return bool(row and row[0])
        except Exception as e:
            logger.warning(f"Could not verify table existence for {table_name}: {e}")
            return False
    
    def connect_db(self):
        """Connect to PostgreSQL database using db_pool"""
        try:
            pool_config = DB_CONFIG.copy()
            max_workers = int(os.environ.get('MAX_WORKERS', min(32, (os.cpu_count() or 1) * 4)))
            pool_config['minconn'] = 1
            pool_config['maxconn'] = max_workers + 5
            
            self.db_pool = PostgreSQLConnectionPool(**pool_config)
            logger.info(f"✅ Connected to connection pool (maxconn={pool_config['maxconn']})")
            return True
        except Exception as e:
            logger.error(f"❌ Database connection pool failed: {e}")
            return False
    
    def close_db(self):
        """Close database connection"""
        if self.db_pool:
            self.db_pool.close_all()
        logger.info("Database connection closed")

    def ensure_run_state_table(self):
        """Ensure ETL run-state table exists."""
        with self.db_pool.get_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
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
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT last_successful_end FROM etl_run_state WHERE module_name = %s",
                    (module_name,)
                )
                row = cursor.fetchone()
                return row[0] if row else None

    def update_run_checkpoint(self, module_name: str, end_date_iso: str):
        """Persist successful run completion boundary."""
        end_dt = parse_iso_date(end_date_iso)
        with self.db_pool.get_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO etl_run_state (module_name, last_successful_end, updated_at)
                    VALUES (%s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (module_name) DO UPDATE SET
                        last_successful_end = EXCLUDED.last_successful_end,
                        updated_at = CURRENT_TIMESTAMP
                """, (module_name, end_dt))
                conn.commit()
    
    def ensure_pending_table(self):
        """Create the properties_pending_fk table if it doesn't exist."""
        try:
            with self.db_pool.get_connection_context() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        CREATE TABLE IF NOT EXISTS {PENDING_FK_TABLE} (
                            id SERIAL PRIMARY KEY,
                            property_id VARCHAR(50) NOT NULL,
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
                        CREATE UNIQUE INDEX IF NOT EXISTS idx_pending_fk_property_id
                        ON {PENDING_FK_TABLE}(property_id) WHERE NOT resolved
                    """)
                    conn.commit()
            logger.info(f"✅ Pending FK retry table ready: {PENDING_FK_TABLE}")
        except Exception as e:
            logger.error(f"❌ Failed to create pending FK table: {e}")
            raise

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

    def queue_pending_fk(self, property_raw: Dict, crime_id: str, conn, cursor):
        """Insert a property record into the pending FK retry queue."""
        property_id = property_raw.get('PROPERTY_ID', 'unknown')
        try:
            cursor.execute(f"""
                INSERT INTO {PENDING_FK_TABLE} (property_id, crime_id, raw_data)
                VALUES (%s, %s, %s)
                ON CONFLICT (property_id) WHERE NOT resolved
                DO UPDATE SET
                    raw_data = EXCLUDED.raw_data,
                    retry_count = {PENDING_FK_TABLE}.retry_count  -- keep existing count
            """, (property_id, crime_id, json.dumps(property_raw, default=str)))
            conn.commit()
            with self.stats_lock:
                self.stats['total_pending_fk'] += 1
            logger.debug(f"Queued property {property_id} (crime_id={crime_id}) for FK retry")
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to queue pending FK for property {property_id}: {e}")

    def retry_pending_fk(self):
        """
        Retry all unresolved pending FK records.
        For each, check if crime_id now exists in crimes. If so, process normally.
        """
        logger.info("")
        logger.info("=" * 80)
        logger.info("🔄 Retrying pending FK records...")

        try:
            # Fetch all unresolved pending records
            with self.db_pool.get_connection_context() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        SELECT id, property_id, crime_id, raw_data, retry_count
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

            for row_id, property_id, crime_id, raw_data, retry_count in pending_rows:
                try:
                    with self.db_pool.get_connection_context() as conn:
                        with conn.cursor() as cur:
                            if crime_id in self.crime_ids:
                                # Crime now exists — process the property
                                prop = self.transform_property(raw_data)
                                success = self.insert_property(prop, conn, cur)
                                if success:
                                    conn.commit()
                                # Mark as resolved regardless (avoid infinite re-inserts on data issues)
                                cur.execute(f"""
                                    UPDATE {PENDING_FK_TABLE}
                                    SET resolved = TRUE, resolved_at = CURRENT_TIMESTAMP,
                                        last_retry_at = CURRENT_TIMESTAMP, retry_count = %s
                                    WHERE id = %s
                                """, (retry_count + 1, row_id))
                                conn.commit()
                                resolved_count += 1
                                logger.debug(f"✅ Resolved pending property {property_id}")
                            else:
                                # Still missing — bump retry count
                                cur.execute(f"""
                                    UPDATE {PENDING_FK_TABLE}
                                    SET last_retry_at = CURRENT_TIMESTAMP, retry_count = %s
                                    WHERE id = %s
                                """, (retry_count + 1, row_id))
                                conn.commit()
                                still_missing += 1
                except Exception as e:
                    logger.error(f"Error retrying pending property {property_id}: {e}")
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
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name = %s
                    """, (table_name,))
                    return {row[0] for row in cursor.fetchall()}
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
                with conn.cursor() as cursor:
                    # Check if table has any data
                    cursor.execute(f"SELECT COUNT(*) FROM {PROPERTIES_TABLE}")
                    count = cursor.fetchone()[0]
                    
                    if count == 0:
                        # New database, start from beginning
                        logger.info("📊 Table is empty, starting from 2022-01-01")
                        return '2022-01-01T00:00:00+05:30'
                    
                    # Table has data, get max of date_created and date_modified
                    # Only consider dates >= 2022-01-01 to avoid processing very old data
                    MIN_START_DATE = '2022-01-01T00:00:00+05:30'
                    min_start_dt = parse_iso_date('2022-01-01T00:00:00+05:30')
                    
                    cursor.execute(f"""
                        SELECT GREATEST(
                            COALESCE(MAX(CASE WHEN date_created >= '2022-01-01'::timestamp THEN date_created END), '2022-01-01'::timestamp),
                            COALESCE(MAX(CASE WHEN date_modified >= '2022-01-01'::timestamp THEN date_modified END), '2022-01-01'::timestamp)
                        ) as max_date
                        FROM {PROPERTIES_TABLE}
                    """)
                    result = cursor.fetchone()
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
            'PROPERTY_ID': 'property_id',
            'CRIME_ID': 'crime_id',
            'CASE_PROPERTY_ID': 'case_property_id',
            'PROPERTY_STATUS': 'property_status',
            'RECOVERED_FROM': 'recovered_from',
            'PLACE_OF_RECOVERY': 'place_of_recovery',
            'DATE_OF_SEIZURE': 'date_of_seizure',
            'NATURE': 'nature',
            'BELONGS': 'belongs',
            'ESTIMATE_VALUE': 'estimate_value',
            'RECOVERED_VALUE': 'recovered_value',
            'PARTICULAR_OF_PROPERTY': 'particular_of_property',
            'CATEGORY': 'category',
            'ADDITIONAL_DETAILS': 'additional_details',
            'MEDIA': 'media',
            'DATE_CREATED': 'date_created',
            'DATE_MODIFIED': 'date_modified'
        }
        
        for api_field, db_column in field_mapping.items():
            if api_field in api_record and db_column not in table_columns:
                new_fields[api_field] = db_column
        
        return new_fields
    
    def add_column_to_table(self, column_name: str, column_type: str = 'TEXT'):
        """Add a new column to the properties table."""
        try:
            with self.schema_lock:
                with self.db_pool.get_connection_context() as conn:
                    with conn.cursor() as cursor:
                        # Determine column type based on field name
                        if 'date' in column_name.lower():
                            column_type = 'TIMESTAMP'
                        elif 'id' in column_name.lower() or 'code' in column_name.lower():
                            column_type = 'VARCHAR(50)'
                        elif column_name in ('additional_details', 'media'):
                            column_type = 'JSONB'
                        elif column_name in ('particular_of_property', 'nature'):
                            column_type = 'TEXT'
                        elif 'value' in column_name.lower():
                            column_type = 'NUMERIC'
                        else:
                            column_type = 'VARCHAR(255)'
                        
                        alter_sql = f"ALTER TABLE {PROPERTIES_TABLE} ADD COLUMN IF NOT EXISTS {column_name} {column_type}"
                        cursor.execute(alter_sql)
                        conn.commit()
                        logger.info(f"✅ Added column {column_name} ({column_type}) to {PROPERTIES_TABLE}")
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
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            chunk_days: Number of days per chunk
            overlap_days: Number of days to overlap between chunks (default: 1 to ensure no data loss)
        
        Returns:
            List of (from_date, to_date) tuples in YYYY-MM-DD format
        """
        date_ranges = []
        current_date = parse_iso_date(start_date).date()
        end = parse_iso_date(end_date).date()
        
        while current_date <= end:
            chunk_end = current_date + timedelta(days=chunk_days - 1)
            if chunk_end > end:
                chunk_end = end
            
            # Always add the range, even if it's less than chunk_days (handles partial chunks)
            date_ranges.append((
                current_date.strftime('%Y-%m-%d'),
                chunk_end.strftime('%Y-%m-%d')
            ))
            
            # Next chunk starts with overlap: current chunk end - overlap_days + 1
            # This ensures the last overlap_days of current chunk are included in next chunk
            # Example: If chunk_end = 2022-10-05 and overlap_days = 1:
            #   next chunk starts at 2022-10-05 - 1 + 1 = 2022-10-05 (includes day 5 in both chunks)
            next_start = chunk_end - timedelta(days=overlap_days - 1)
            
            # If we've already reached or passed the end date, break
            if chunk_end >= end:
                break
            
            # Move to next chunk start
            current_date = next_start
        
        return date_ranges
    
    def fetch_properties_api(self, from_date: str, to_date: str) -> Optional[List[Dict]]:
        """
        Fetch properties from API for given date range
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
        
        Returns:
            List of property dicts or None if failed
        """
        url = f"{API_CONFIG['base_url']}/property-details"
        params = {
            'fromDate': from_date,
            'toDate': to_date
        }
        headers = {
            'x-api-key': API_CONFIG['api_key']
        }
        
        for attempt in range(API_CONFIG['max_retries']):
            try:
                logger.debug(f"Fetching properties: {from_date} to {to_date} (Attempt {attempt + 1})")
                response = requests.get(
                    url,
                    params=params,
                    headers=headers
                )
                
                if response.status_code == 200:
                    data = response.json()
                    with self.stats_lock:
                        self.stats['total_api_calls'] += 1
                    
                    if data.get('status'):
                        property_data = data.get('data')
                        if property_data:
                            # Ensure it's a list
                            if isinstance(property_data, dict):
                                property_data = [property_data]
                            logger.info(f"✅ Fetched {len(property_data)} properties for {from_date} to {to_date}")
                            return property_data
                        else:
                            logger.warning(f"⚠️  No properties found for {from_date} to {to_date}")
                            return []
                    else:
                        logger.warning(f"⚠️  API returned status=false for {from_date} to {to_date}")
                        return []
                
                elif response.status_code == 404:
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
                    with self.stats_lock:
                        self.stats['failed_api_calls'] += 1
                        self.stats['errors'].append(f"{from_date} to {to_date}: {str(e)}")
                time.sleep(2 ** attempt)
        
        logger.error(f"❌ Failed to fetch properties for {from_date} to {to_date} after {API_CONFIG['max_retries']} attempts")
        return None
    
    def parse_date_field(self, date_value) -> Optional[datetime]:
        """
        Parse date field from API response
        Handles ISO 8601 format, empty strings, None, and already-parsed datetime objects
        
        Args:
            date_value: Date value from API (string, datetime, or None)
        
        Returns:
            datetime object or None
        """
        if date_value is None:
            return None
        
        # If already a datetime object, return as is
        if isinstance(date_value, datetime):
            return date_value
        
        # If not a string, try to convert
        if not isinstance(date_value, str):
            return None
        
        # Handle empty strings
        if not date_value.strip():
            return None
        
        try:
            # Try parsing ISO format (handles 'Z' timezone)
            return datetime.fromisoformat(date_value.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            # If ISO parsing fails, try other common formats
            try:
                # Try YYYY-MM-DD format
                return datetime.strptime(date_value, '%Y-%m-%d').replace(tzinfo=IST_OFFSET)
            except (ValueError, AttributeError):
                # If all parsing fails, return None
                logger.debug(f"Could not parse date: {date_value}")
                return None

    def normalize_blank(self, value):
        """Normalize blank strings to None, leave other values unchanged."""
        if isinstance(value, str):
            cleaned = value.strip()
            return cleaned if cleaned else None
        return value

    def coerce_numeric(self, value, field_name: str) -> Optional[Decimal]:
        """Safely coerce numeric API values to Decimal or None."""
        value = self.normalize_blank(value)
        if value is None:
            return None
        if isinstance(value, Decimal):
            return value
        if isinstance(value, bool):
            logger.warning(f"Unexpected boolean for numeric field {field_name}: {value}")
            return None
        try:
            return Decimal(str(value))
        except (InvalidOperation, ValueError, TypeError):
            logger.warning(f"Could not coerce numeric field {field_name}: {value}")
            return None

    def coerce_bool(self, value) -> Optional[bool]:
        """Coerce API boolean-like values to bool or None."""
        value = self.normalize_blank(value)
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in ('true', 't', '1', 'yes', 'y'):
                return True
            if lowered in ('false', 'f', '0', 'no', 'n'):
                return False
        logger.warning(f"Could not coerce boolean value: {value}")
        return None

    def normalize_json_value(self, value):
        """Recursively normalize JSON values (blank strings to null)."""
        if isinstance(value, dict):
            return {k: self.normalize_json_value(v) for k, v in value.items()}
        if isinstance(value, list):
            return [self.normalize_json_value(v) for v in value]
        return self.normalize_blank(value)

    def normalize_additional_details(self, details_raw) -> Optional[Dict]:
        """
        Normalize ADDITIONAL_DETAILS payload.
        - Preserve all keys
        - Convert blank strings to null
        - Coerce known numeric/boolean keys
        - Preserve explicit null from API
        """
        if details_raw is None:
            return None

        if not isinstance(details_raw, dict):
            logger.warning("ADDITIONAL_DETAILS is not an object; storing as wrapped raw value")
            return {'_raw': self.normalize_json_value(details_raw)}

        normalized = self.normalize_json_value(details_raw)

        if 'WEIGHT' in normalized:
            weight_value = self.coerce_numeric(normalized.get('WEIGHT'), 'ADDITIONAL_DETAILS.WEIGHT')
            normalized['WEIGHT'] = float(weight_value) if weight_value is not None else None

        bool_keys = [
            'WHETHER_NOTICE',
            'WHETHER_LAB',
            'WHETHER_ACCUSED',
            'WHETHER_DRUG_SYNDICATE',
            'WHETHER_TRAFFICKER',
            'WHETHER_CARRIER',
            'WHETHER_ADDICT',
            'WHETHER_PEDDLER',
            'WHETHER_DETAINED',
            'WHETHER_EMERGENCY',
            'WHETHER_INTERROGATION',
        ]
        for key in bool_keys:
            if key in normalized:
                normalized[key] = self.coerce_bool(normalized.get(key))

        return normalized

    def normalize_media(self, media_raw) -> Optional[List]:
        """
        Normalize MEDIA array while preserving payload shape.
        Returns None when API explicitly sends null.
        """
        if media_raw is None:
            return None
        if not isinstance(media_raw, list):
            logger.warning("MEDIA is not an array; coercing to empty array")
            return []
        return [self.normalize_json_value(item) for item in media_raw]

    def to_jsonb_param(self, value):
        """Return psycopg2 JSON wrapper only for non-null values."""
        return Json(value) if value is not None else None

    def parse_media_item(self, media_item) -> Tuple[Optional[str], Optional[str], Optional[Dict]]:
        """Extract media_file_id and media_url from a media entry while keeping full payload."""
        if media_item is None:
            return None, None, None

        if isinstance(media_item, str):
            media_file_id = self.normalize_blank(media_item)
            return media_file_id, None, {'media_file_id': media_file_id}

        if isinstance(media_item, dict):
            media_file_id = (
                self.normalize_blank(media_item.get('fileId'))
                or self.normalize_blank(media_item.get('file_id'))
                or self.normalize_blank(media_item.get('mediaFileId'))
                or self.normalize_blank(media_item.get('MEDIA_FILE_ID'))
                or self.normalize_blank(media_item.get('id'))
            )
            media_url = (
                self.normalize_blank(media_item.get('url'))
                or self.normalize_blank(media_item.get('mediaUrl'))
                or self.normalize_blank(media_item.get('MEDIA_URL'))
                or self.normalize_blank(media_item.get('fileUrl'))
                or self.normalize_blank(media_item.get('file_url'))
            )
            return media_file_id, media_url, media_item

        return None, None, {'_raw': media_item}

    def upsert_property_additional_details(
        self,
        property_id: str,
        additional_details: Optional[Dict],
        date_created: Optional[datetime],
        date_modified: Optional[datetime],
        cursor,
    ):
        """
        Keep property_additional_details in overwrite mode.
        If source is null, remove child row to avoid stale data.
        """
        if not self.has_property_additional_details_table:
            return

        if additional_details is None:
            cursor.execute(
                f"DELETE FROM {PROPERTY_ADDITIONAL_DETAILS_TABLE} WHERE property_id = %s",
                (property_id,),
            )
            return

        cursor.execute(
            f"""
                INSERT INTO {PROPERTY_ADDITIONAL_DETAILS_TABLE}
                    (property_id, additional_details, date_created, date_modified)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (property_id)
                DO UPDATE SET
                    additional_details = EXCLUDED.additional_details,
                    date_created = EXCLUDED.date_created,
                    date_modified = EXCLUDED.date_modified
            """,
            (
                property_id,
                self.to_jsonb_param(additional_details),
                date_created,
                date_modified,
            ),
        )

    def replace_property_media(
        self,
        property_id: str,
        media: Optional[List],
        date_created: Optional[datetime],
        date_modified: Optional[datetime],
        cursor,
    ):
        """
        Keep property_media in overwrite mode.
        Always delete previous rows first to avoid stale child records.
        """
        if not self.has_property_media_table:
            return

        cursor.execute(
            f"DELETE FROM {PROPERTY_MEDIA_TABLE} WHERE property_id = %s",
            (property_id,),
        )

        if not media:
            return

        insert_sql = f"""
            INSERT INTO {PROPERTY_MEDIA_TABLE}
                (property_id, media_index, media_file_id, media_url, media_payload, date_created, date_modified)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (property_id, media_index)
            DO UPDATE SET
                media_file_id = EXCLUDED.media_file_id,
                media_url = EXCLUDED.media_url,
                media_payload = EXCLUDED.media_payload,
                date_created = EXCLUDED.date_created,
                date_modified = EXCLUDED.date_modified
        """

        for idx, media_item in enumerate(media):
            media_file_id, media_url, media_payload = self.parse_media_item(media_item)
            cursor.execute(
                insert_sql,
                (
                    property_id,
                    idx,
                    media_file_id,
                    media_url,
                    self.to_jsonb_param(media_payload),
                    date_created,
                    date_modified,
                ),
            )
    
    def transform_property(self, property_raw: Dict) -> Dict:
        """
        Transform API response to database format
        Dates are always taken from API (never use CURRENT_TIMESTAMP)
        
        Args:
            property_raw: Raw property data from API
        
        Returns:
            Transformed property dict ready for database
        """
        # Parse date_of_seizure (handle ISO format, empty strings, None)
        date_of_seizure = self.parse_date_field(property_raw.get('DATE_OF_SEIZURE'))
        
        # Parse date_created and date_modified (handle ISO format, empty strings, None)
        date_created = self.parse_date_field(property_raw.get('DATE_CREATED'))
        date_modified = self.parse_date_field(property_raw.get('DATE_MODIFIED'))
        
        additional_details = self.normalize_additional_details(property_raw.get('ADDITIONAL_DETAILS'))
        media = self.normalize_media(property_raw.get('MEDIA'))
        
        return {
            'property_id': property_raw.get('PROPERTY_ID'),
            'crime_id': property_raw.get('CRIME_ID'),
            'case_property_id': property_raw.get('CASE_PROPERTY_ID'),
            'property_status': property_raw.get('PROPERTY_STATUS'),
            'recovered_from': self.normalize_blank(property_raw.get('RECOVERED_FROM')),
            'place_of_recovery': self.normalize_blank(property_raw.get('PLACE_OF_RECOVERY')),
            'date_of_seizure': date_of_seizure,
            'nature': property_raw.get('NATURE'),
            'belongs': property_raw.get('BELONGS'),
            'estimate_value': self.coerce_numeric(property_raw.get('ESTIMATE_VALUE'), 'ESTIMATE_VALUE'),
            'recovered_value': self.coerce_numeric(property_raw.get('RECOVERED_VALUE'), 'RECOVERED_VALUE'),
            'particular_of_property': property_raw.get('PARTICULAR_OF_PROPERTY'),
            'category': property_raw.get('CATEGORY'),
            'additional_details': additional_details,
            'media': media,
            'date_created': date_created,  # Parsed from API (or NULL)
            'date_modified': date_modified  # Parsed from API (or NULL)
        }
    
    def property_exists(self, property_id: str, cursor) -> bool:
        """Check if property already exists in database"""
        cursor.execute(f"SELECT 1 FROM {PROPERTIES_TABLE} WHERE property_id = %s", (property_id,))
        return cursor.fetchone() is not None
    
    def insert_property(self, prop: Dict, conn, cursor) -> bool:
        """
        Insert or update single property in database
        
        Args:
            prop: Transformed property dict
            conn: Database connection
            cursor: Database cursor
            
        Returns:
            True if successful, False otherwise
        """
        try:
            upsert_query = f"""
                INSERT INTO {PROPERTIES_TABLE} (
                    property_id, crime_id, case_property_id, property_status,
                    recovered_from, place_of_recovery, date_of_seizure, nature,
                    belongs, estimate_value, recovered_value, particular_of_property,
                    category, additional_details, media,
                    date_created, date_modified
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (property_id) DO UPDATE SET
                    crime_id = EXCLUDED.crime_id,
                    case_property_id = EXCLUDED.case_property_id,
                    property_status = EXCLUDED.property_status,
                    recovered_from = EXCLUDED.recovered_from,
                    place_of_recovery = EXCLUDED.place_of_recovery,
                    date_of_seizure = EXCLUDED.date_of_seizure,
                    nature = EXCLUDED.nature,
                    belongs = EXCLUDED.belongs,
                    estimate_value = EXCLUDED.estimate_value,
                    recovered_value = EXCLUDED.recovered_value,
                    particular_of_property = EXCLUDED.particular_of_property,
                    category = EXCLUDED.category,
                    additional_details = EXCLUDED.additional_details,
                    media = EXCLUDED.media,
                    date_created = EXCLUDED.date_created,
                    date_modified = EXCLUDED.date_modified
                WHERE (
                    {PROPERTIES_TABLE}.crime_id IS DISTINCT FROM EXCLUDED.crime_id OR
                    {PROPERTIES_TABLE}.case_property_id IS DISTINCT FROM EXCLUDED.case_property_id OR
                    {PROPERTIES_TABLE}.property_status IS DISTINCT FROM EXCLUDED.property_status OR
                    {PROPERTIES_TABLE}.recovered_from IS DISTINCT FROM EXCLUDED.recovered_from OR
                    {PROPERTIES_TABLE}.place_of_recovery IS DISTINCT FROM EXCLUDED.place_of_recovery OR
                    {PROPERTIES_TABLE}.date_of_seizure IS DISTINCT FROM EXCLUDED.date_of_seizure OR
                    {PROPERTIES_TABLE}.nature IS DISTINCT FROM EXCLUDED.nature OR
                    {PROPERTIES_TABLE}.belongs IS DISTINCT FROM EXCLUDED.belongs OR
                    {PROPERTIES_TABLE}.estimate_value IS DISTINCT FROM EXCLUDED.estimate_value OR
                    {PROPERTIES_TABLE}.recovered_value IS DISTINCT FROM EXCLUDED.recovered_value OR
                    {PROPERTIES_TABLE}.particular_of_property IS DISTINCT FROM EXCLUDED.particular_of_property OR
                    {PROPERTIES_TABLE}.category IS DISTINCT FROM EXCLUDED.category OR
                    {PROPERTIES_TABLE}.additional_details IS DISTINCT FROM EXCLUDED.additional_details OR
                    {PROPERTIES_TABLE}.media IS DISTINCT FROM EXCLUDED.media OR
                    {PROPERTIES_TABLE}.date_created IS DISTINCT FROM EXCLUDED.date_created OR
                    {PROPERTIES_TABLE}.date_modified IS DISTINCT FROM EXCLUDED.date_modified
                )
                RETURNING (xmax = 0) AS inserted
            """

            cursor.execute(upsert_query, (
                prop['property_id'],
                prop['crime_id'],
                prop['case_property_id'],
                prop['property_status'],
                prop['recovered_from'],
                prop['place_of_recovery'],
                prop['date_of_seizure'],
                prop['nature'],
                prop['belongs'],
                prop['estimate_value'],
                prop['recovered_value'],
                prop['particular_of_property'],
                prop['category'],
                self.to_jsonb_param(prop['additional_details']),
                self.to_jsonb_param(prop['media']),
                prop['date_created'],
                prop['date_modified']
            ))

            upsert_result = cursor.fetchone()

            self.upsert_property_additional_details(
                prop['property_id'],
                prop['additional_details'],
                prop['date_created'],
                prop['date_modified'],
                cursor,
            )
            self.replace_property_media(
                prop['property_id'],
                prop['media'],
                prop['date_created'],
                prop['date_modified'],
                cursor,
            )

            if upsert_result is None:
                with self.stats_lock:
                    self.stats['total_properties_no_change'] += 1
            elif upsert_result[0]:
                with self.stats_lock:
                    self.stats['total_properties_inserted'] += 1
                logger.debug(f"Inserted property: {prop['property_id']}")
            else:
                with self.stats_lock:
                    self.stats['total_properties_updated'] += 1
                logger.debug(f"Updated property: {prop['property_id']}")
            
            # The commit action for db pool is moved to process_date_range process_prop function 
            return True
            
        except psycopg2.IntegrityError as e:
            conn.rollback()
            logger.warning(f"⚠️  Integrity error for property {prop['property_id']}: {e}")
            with self.stats_lock:
                self.stats['total_properties_failed'] += 1
            return False
        except Exception as e:
            conn.rollback()
            logger.error(f"❌ Error inserting property {prop['property_id']}: {e}")
            with self.stats_lock:
                self.stats['errors'].append(f"Property {prop['property_id']}: {str(e)}")
            return False
    
    def process_date_range(self, from_date: str, to_date: str, table_columns: Set[str] = None):
        """Process properties for a specific date range"""
        logger.info(f"📅 Processing: {from_date} to {to_date}")
        
        # Initialize chunk-level statistics
        chunk_stats = {
            'inserted': 0,
            'updated': 0,
            'no_change': 0,
            'failed': 0
        }
        
        with self.stats_lock:
            # Store initial stats to calculate chunk differences
            initial_inserted = self.stats['total_properties_inserted']
            initial_updated = self.stats['total_properties_updated']
            initial_no_change = self.stats['total_properties_no_change']
            initial_failed = self.stats['total_properties_failed']
        
        # Fetch properties from API
        properties_raw = self.fetch_properties_api(from_date, to_date)
        
        if properties_raw is None:
            logger.error(f"❌ Failed to fetch properties for {from_date} to {to_date}")
            chunk_stats['failed'] = 1  # API call failed
            with self.stats_lock:
                self.stats['total_properties_failed'] += 1
            return
        
        if not properties_raw:
            logger.info(f"ℹ️  No properties found for {from_date} to {to_date} - continuing to next chunk")
            return
        
        # Check for schema evolution if we got data
        if table_columns is not None and len(properties_raw) > 0:
            # Check for new fields in first record
            new_fields = self.detect_new_fields(properties_raw[0], table_columns)
            if new_fields:
                logger.info(f"🔍 New fields detected in API response: {list(new_fields.keys())}")
                # Add new columns to table
                for api_field, db_column in new_fields.items():
                    if self.add_column_to_table(db_column):
                        # Update table_columns set
                        table_columns.add(db_column)
                # Update existing records from start_date to current chunk end_date
                self.update_existing_records_with_new_fields(new_fields, to_date)
        
        # Transform and insert each property
        with self.stats_lock:
            self.stats['total_properties_fetched'] += len(properties_raw)
        
        def process_prop(property_raw):
            try:
                prop = self.transform_property(property_raw)
                if not prop['property_id']:
                    logger.warning(f"⚠️  Property missing PROPERTY_ID, skipping")
                    with self.stats_lock:
                        self.stats['total_properties_failed'] += 1
                    return

                crime_id = prop.get('crime_id')
                with self.db_pool.get_connection_context() as conn:
                    with conn.cursor() as cur:
                        # Pre-validate: does crime_id exist in crimes table?
                        if crime_id and crime_id not in self.crime_ids:
                            # Queue for retry instead of letting FK blow up
                            self.queue_pending_fk(property_raw, crime_id, conn, cur)
                            logger.debug(
                                f"⏳ Property {prop['property_id']}: crime_id {crime_id} "
                                f"not in crimes table — queued for retry"
                            )
                            return

                        self.insert_property(prop, conn, cur)
                        conn.commit()
            except Exception as e:
                logger.error(f"Error in process_prop: {e}")
                with self.stats_lock:
                    self.stats['total_properties_failed'] += 1
        
        requested_workers = int(os.environ.get('MAX_WORKERS', min(32, (os.cpu_count() or 1) * 4)))
        max_workers = compute_safe_workers(self.db_pool, requested_workers)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            list(executor.map(process_prop, properties_raw))
        
        with self.stats_lock:
            # Calculate chunk statistics
            chunk_stats['inserted'] = self.stats['total_properties_inserted'] - initial_inserted
            chunk_stats['updated'] = self.stats['total_properties_updated'] - initial_updated
            chunk_stats['no_change'] = self.stats['total_properties_no_change'] - initial_no_change
            chunk_stats['failed'] = self.stats['total_properties_failed'] - initial_failed
        
        # Log chunk statistics
        logger.info(f"✅ Completed: {from_date} to {to_date}")
        logger.info(f"   📊 Chunk Stats - Inserted: {chunk_stats['inserted']}, Updated: {chunk_stats['updated']}, "
                   f"No Change: {chunk_stats['no_change']}, Failed: {chunk_stats['failed']}")
    
    def run(self):
        """Main ETL execution"""
        logger.info("=" * 80)
        logger.info("🚀 DOPAMAS ETL Pipeline - Property Details API")
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
            self.ensure_run_state_table()

            # Ensure the pending FK retry queue table exists
            self.ensure_pending_table()

            self.has_property_additional_details_table = self.table_exists(PROPERTY_ADDITIONAL_DETAILS_TABLE)
            self.has_property_media_table = self.table_exists(PROPERTY_MEDIA_TABLE)
            if self.has_property_additional_details_table:
                logger.info(f"✅ Child table detected: {PROPERTY_ADDITIONAL_DETAILS_TABLE}")
            else:
                logger.warning(
                    f"⚠️  Child table missing: {PROPERTY_ADDITIONAL_DETAILS_TABLE} "
                    f"(run migration to enable normalized ADDITIONAL_DETAILS sync)"
                )
            if self.has_property_media_table:
                logger.info(f"✅ Child table detected: {PROPERTY_MEDIA_TABLE}")
            else:
                logger.warning(
                    f"⚠️  Child table missing: {PROPERTY_MEDIA_TABLE} "
                    f"(run migration to enable normalized MEDIA sync)"
                )
            
            # Load crime IDs into memory
            self.load_crime_ids()

            # Get effective start date (check if table has data)
            effective_start_date = self.get_effective_start_date()
            checkpoint_date = self.get_run_checkpoint('properties')
            if checkpoint_date:
                checkpoint_iso = checkpoint_date.isoformat()
                if parse_iso_date(checkpoint_iso) > parse_iso_date(effective_start_date):
                    effective_start_date = checkpoint_iso

            logger.info(f"Effective Start Date: {effective_start_date}")
            
            # Get table columns for schema evolution
            table_columns = self.get_table_columns(PROPERTIES_TABLE)
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
            
            # Retry pending FK records (crime_id may now exist after earlier ETLs)
            self.retry_pending_fk()

            # Get database counts
            with self.db_pool.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"SELECT COUNT(*) FROM {PROPERTIES_TABLE}")
                    db_properties_count = cursor.fetchone()[0]
                    cursor.execute(f"SELECT COUNT(*) FROM {PENDING_FK_TABLE} WHERE resolved = FALSE")
                    pending_count = cursor.fetchone()[0]
            
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
            logger.info(f"  Total Properties Fetched: {self.stats['total_properties_fetched']}")
            logger.info(f"")
            logger.info(f"💾 TO DATABASE:")
            logger.info(f"  Total Inserted (New):     {self.stats['total_properties_inserted']}")
            logger.info(f"  Total Updated:            {self.stats['total_properties_updated']}")
            logger.info(f"  Total No Change:          {self.stats['total_properties_no_change']}")
            logger.info(f"  Total Failed:             {self.stats['total_properties_failed']}")
            logger.info(f"  Total in DB:              {db_properties_count}")
            logger.info(f"")
            logger.info(f"⏳ PENDING FK RETRY QUEUE:")
            logger.info(f"  Queued (missing crime_id): {self.stats['total_pending_fk']}")
            logger.info(f"  Retried → Resolved:        {self.stats['total_retried_ok']}")
            logger.info(f"  Retried → Still Missing:   {self.stats['total_retried_still_missing']}")
            logger.info(f"  Remaining in Queue:        {pending_count}")
            logger.info(f"")
            logger.info(f"📊 COVERAGE:")
            if self.stats['total_properties_fetched'] > 0:
                coverage = ((self.stats['total_properties_inserted'] + self.stats['total_properties_updated']) / self.stats['total_properties_fetched']) * 100
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

            self.update_run_checkpoint('properties', calculated_end_date)
            
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
    etl = PropertiesETL()
    success = etl.run()
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()

