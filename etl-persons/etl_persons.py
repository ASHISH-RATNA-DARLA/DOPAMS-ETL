#!/usr/bin/env python3
"""
DOPAMAS ETL Pipeline - Person Details API
Incrementally fetches person details for PERSON_IDs from recently updated accused records
Only processes person_ids from accused records that were added/updated since last run
"""

import sys
import os
import time
import re
import json
import requests
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime, timezone, timedelta
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import colorlog
from typing import Dict, Optional, List, Set, Tuple, Any

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_pooling import PostgreSQLConnectionPool, compute_safe_workers

from config import DB_CONFIG, API_CONFIG, LOG_CONFIG, TABLE_CONFIG, PERSON_GENDER_CONFIG

# IST timezone offset (UTC+05:30)
IST_OFFSET = timezone(timedelta(hours=5, minutes=30))


def parse_iso_date(iso_date_str: str) -> datetime:
    """Parse ISO 8601 or YYYY-MM-DD date string to datetime."""
    if 'T' in iso_date_str:
        return datetime.fromisoformat(iso_date_str.replace('Z', '+00:00'))
    dt = datetime.strptime(iso_date_str, '%Y-%m-%d')
    return dt.replace(tzinfo=IST_OFFSET)


def get_yesterday_end_ist() -> str:
    """Get yesterday 23:59:59 in IST as ISO-8601 string."""
    now_ist = datetime.now(IST_OFFSET)
    yesterday = now_ist - timedelta(days=1)
    return yesterday.replace(hour=23, minute=59, second=59, microsecond=0).isoformat()

# Get table names from config (with fallback to defaults)
ACCUSED_TABLE = TABLE_CONFIG.get('accused', 'accused')
PERSONS_TABLE = TABLE_CONFIG.get('persons', 'persons')

# Setup logging
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


class PersonsETL:
    def __init__(self):
        self.db_pool = None
        self.person_gender_infer_on_unknown = bool(PERSON_GENDER_CONFIG.get('infer_on_unknown', False))
        self.person_gender_inference_threshold = float(PERSON_GENDER_CONFIG.get('inference_threshold', 0.8))
        self.person_gender_dry_run = bool(PERSON_GENDER_CONFIG.get('dry_run', False))
        self.person_gender_preserve_valid_api = bool(PERSON_GENDER_CONFIG.get('preserve_valid_api', True))
        self.stats = {
            'person_ids': 0,
            'api_calls': 0,
            'failed_api_calls': 0,
            'inserted': 0,
            'updated': 0,
            'no_change': 0,  # Records that exist but no changes needed
            'no_data': 0,   # API returned 404 / no data (dead person_id)
            'failed': 0,  # Records that failed to process
            'errors': 0,
            'dry_run_changes': 0,
            'dry_run_no_change': 0,
            'dry_run_inserts': 0
        }
        self.stats_lock = threading.Lock()
        self.schema_lock = threading.Lock()
        self.dry_run_lock = threading.Lock()

        self.dry_run_log_file = None
        if self.person_gender_dry_run:
            os.makedirs('logs', exist_ok=True)
            ts = datetime.now().strftime('%Y%m%d_%H%M%S')
            self.dry_run_log_file = f"logs/person_gender_dry_run_{ts}.jsonl"
            logger.warning(f"⚠️  PERSON_GENDER_DRY_RUN enabled. No DB writes will be made. Dry-run log: {self.dry_run_log_file}")

        self.invalid_name_exact = {
            'absconding', 'unknown', 'unknown person', 'unknown persons',
            'not known', 'name not known', 'unidentified', 'dead body',
            'na', 'n a', 'n/a', 'nil', 'none', 'not available',
            'no name', 'accused', 'suspect', 'person'
        }
        self.placeholder_tokens = {
            'absconding', 'unknown', 'unidentified', 'na', 'n/a',
            'nil', 'none', 'dead', 'body', 'accused', 'suspect', 'person'
        }
        self.gender_map = {
            'male': 'Male',
            'm': 'Male',
            'man': 'Male',
            'boy': 'Male',
            'female': 'Female',
            'f': 'Female',
            'woman': 'Female',
            'girl': 'Female',
            'transgender': 'Transgender',
            'trans gender': 'Transgender',
            'third gender': 'Transgender',
            'trans': 'Transgender',
            'tg': 'Transgender',
            'unknown': 'Unknown',
            'unk': 'Unknown',
            'n/a': 'Unknown',
            'na': 'Unknown',
            'not known': 'Unknown',
            'not available': 'Unknown',
            '': 'Unknown'
        }
        self.name_gender_rule_map = {
            'ramesh': 'Male', 'rajesh': 'Male', 'suresh': 'Male', 'mahesh': 'Male',
            'rahul': 'Male', 'vijay': 'Male', 'kiran': 'Male', 'arun': 'Male',
            'sita': 'Female', 'laxmi': 'Female', 'lakshmi': 'Female', 'kavitha': 'Female',
            'kavita': 'Female', 'sunita': 'Female', 'anjali': 'Female', 'pooja': 'Female'
        }

    def _normalize_space(self, value: str) -> str:
        return ' '.join(value.strip().split())

    def _canonical_name_for_match(self, value: str) -> str:
        lowered = value.lower().strip()
        lowered = re.sub(r'[^a-z0-9\s]+', ' ', lowered)
        return self._normalize_space(lowered)

    def _normalize_person_name(self, raw_name: Optional[str], personal: Dict) -> Optional[str]:
        if raw_name is None:
            name_part = self._normalize_space(str(personal.get('NAME') or ''))
            surname_part = self._normalize_space(str(personal.get('SURNAME') or ''))
            joined = self._normalize_space(f"{name_part} {surname_part}".strip())
            return joined if joined else None
        text = self._normalize_space(str(raw_name))
        if not text:
            return None
        return text

    def _is_valid_person_name(self, clean_name: Optional[str]) -> bool:
        if clean_name is None:
            return False
        lowered = clean_name.lower().strip()
        if not lowered:
            return False

        canonical = self._canonical_name_for_match(lowered)
        if canonical in self.invalid_name_exact:
            return False

        alpha_chars = [ch for ch in lowered if ch.isalpha()]
        if len(alpha_chars) < 2:
            return False
        alpha_ratio = len(alpha_chars) / max(len(lowered), 1)
        if alpha_ratio < 0.35:
            return False

        tokens = [t for t in re.split(r'[^a-z]+', canonical) if t]
        if tokens and all(token in self.placeholder_tokens for token in tokens):
            return False
        if re.search(r'\b(name\s+not\s+known|unknown\s+person(s)?|absconding\s+accused|dead\s+body|unidentified)\b', canonical):
            return False
        return True

    def _normalize_api_gender(self, raw_gender: Optional[str]) -> Optional[str]:
        if raw_gender is None:
            return 'Unknown'
        value = self._normalize_space(str(raw_gender)).strip().lower()
        mapped = self.gender_map.get(value)
        if mapped:
            return mapped
        compact = self._normalize_space(value.replace('-', ' '))
        mapped = self.gender_map.get(compact)
        if mapped:
            return mapped
        return None

    def _infer_gender_from_name(self, clean_name: Optional[str]) -> Tuple[Optional[str], float, str]:
        if not clean_name:
            return None, 0.0, 'heuristic'

        tokens = [token for token in re.findall(r"[A-Za-z]+", clean_name.lower()) if len(token) > 1]
        if not tokens:
            return None, 0.0, 'heuristic'

        first = tokens[0]
        rule_match = self.name_gender_rule_map.get(first)
        if rule_match:
            return rule_match, 0.9, 'rule'

        female_suffixes = ('a', 'i', 'ya', 'ika', 'ita')
        male_suffixes = ('esh', 'endra', 'kumar', 'raj', 'veer')
        if first.endswith(female_suffixes):
            return 'Female', 0.8, 'heuristic'
        if first.endswith(male_suffixes):
            return 'Male', 0.8, 'heuristic'

        return None, 0.0, 'heuristic'

    def _normalize_phone_numbers(self, raw_phone: Any) -> List[str]:
        """Normalize phone payloads into a de-duplicated list preserving source order."""
        collected: List[str] = []

        def collect(value: Any):
            if value is None:
                return
            if isinstance(value, dict):
                for item in value.values():
                    collect(item)
                return
            if isinstance(value, (list, tuple, set)):
                for item in value:
                    collect(item)
                return

            text = self._normalize_space(str(value))
            if not text:
                return
            for chunk in re.split(r'[\n,;|/]+', text):
                normalized = self._normalize_space(chunk)
                if normalized:
                    collected.append(normalized)

        collect(raw_phone)

        invalid_tokens = {'na', 'n/a', 'none', 'null', 'not available', 'unknown', '-'}
        deduped: List[str] = []
        seen: Set[str] = set()
        for value in collected:
            lowered = value.lower()
            if lowered in invalid_tokens:
                continue
            if value in seen:
                continue
            seen.add(value)
            deduped.append(value)
        return deduped

    def _resolve_gender(self, clean_name: Optional[str], api_gender_raw: Optional[str]) -> Tuple[str, float, str]:
        normalized_api = self._normalize_api_gender(api_gender_raw)

        # Source-priority protection: never override a valid API gender when enabled.
        if self.person_gender_preserve_valid_api and normalized_api in ('Male', 'Female', 'Transgender'):
            return normalized_api, 1.0, 'api'

        is_name_valid = self._is_valid_person_name(clean_name)
        if not is_name_valid:
            return 'Unknown', 0.0, 'invalid_name'

        if normalized_api in ('Male', 'Female', 'Transgender'):
            return normalized_api, 1.0, 'api'

        if normalized_api == 'Unknown':
            if not self.person_gender_infer_on_unknown:
                return 'Unknown', 1.0, 'api'
            inferred_gender, confidence, source = self._infer_gender_from_name(clean_name)
            if inferred_gender and confidence >= self.person_gender_inference_threshold:
                return inferred_gender, confidence, source
            return 'Unknown', confidence, source

        # Invalid raw gender value from API.
        inferred_gender, confidence, source = self._infer_gender_from_name(clean_name)
        if inferred_gender and confidence >= self.person_gender_inference_threshold:
            return inferred_gender, confidence, source
        return 'Unknown', confidence, source

    def connect_db(self):
        try:
            max_workers = int(os.environ.get('MAX_WORKERS', min(32, (os.cpu_count() or 1) * 4)))
            self.db_pool = PostgreSQLConnectionPool(
                min_conn=1,
                max_conn=max_workers + 5, 
                **DB_CONFIG
            )
            logger.info(f"✅ Connected to database: {DB_CONFIG['database']} (Pool max: {self.db_pool.max_conn})")
            return True
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            return False

    def close_db(self):
        if self.db_pool:
            self.db_pool.close_all()
        logger.info("Database connection pool closed")

    def ensure_run_state_table(self):
        """Ensure ETL run-state table exists."""
        with self.db_pool.get_connection_context() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS etl_run_state (
                    module_name TEXT PRIMARY KEY,
                    last_successful_end TIMESTAMPTZ NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()

    def get_run_checkpoint(self, module_name: str) -> Optional[datetime]:
        """Read last successful checkpoint for a module."""
        with self.db_pool.get_connection_context() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT last_successful_end FROM etl_run_state WHERE module_name = %s",
                (module_name,)
            )
            row = cursor.fetchone()
            return row[0] if row else None

    def update_run_checkpoint(self, module_name: str, end_date_iso: str):
        """Persist successful run end boundary for resume safety."""
        end_dt = parse_iso_date(end_date_iso)
        with self.db_pool.get_connection_context() as conn:
            cursor = conn.cursor()
            cursor.execute("""
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
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = %s
                """, (table_name,))
                return {row[0] for row in cursor.fetchall()}
        except Exception as e:
            logger.error(f"Error getting table columns for {table_name}: {e}")
            return set()
    
    def detect_new_fields(self, api_record: Dict, table_columns: Set[str]) -> Dict[str, str]:
        """
        Detect new fields in API response that don't exist in table.
        Returns dict mapping API field name to database column name (snake_case).
        """
        new_fields = {}
        
        # Map API field names to database column names
        # Personal Details mapping
        personal_fields = {
            'NAME': 'name',
            'SURNAME': 'surname',
            'ALIAS': 'alias',
            'FULL_NAME': 'full_name',
            'RELATION_TYPE': 'relation_type',
            'RELATIVE_NAME': 'relative_name',
            'GENDER': 'gender',
            'IS_DIED': 'is_died',
            'DATE_OF_BIRTH': 'date_of_birth',
            'AGE': 'age',
            'OCCUPATION': 'occupation',
            'EDUCATION_QUALIFICATION': 'education_qualification',
            'CASTE': 'caste',
            'SUB_CASTE': 'sub_caste',
            'RELIGION': 'religion',
            'NATIONALITY': 'nationality',
            'DESIGNATION': 'designation',
            'PLACE_OF_WORK': 'place_of_work'
        }
        
        # Present Address mapping
        present_address_fields = {
            'HOUSE_NO': 'present_house_no',
            'STREET_ROAD_NO': 'present_street_road_no',
            'WARD_COLONY': 'present_ward_colony',
            'LANDMARK_MILESTONE': 'present_landmark_milestone',
            'LOCALITY_VILLAGE': 'present_locality_village',
            'AREA_MANDAL': 'present_area_mandal',
            'DISTRICT': 'present_district',
            'STATE_UT': 'present_state_ut',
            'COUNTRY': 'present_country',
            'RESIDENCY_TYPE': 'present_residency_type',
            'PIN_CODE': 'present_pin_code',
            'JURISDICTION_PS': 'present_jurisdiction_ps'
        }
        
        # Permanent Address mapping
        permanent_address_fields = {
            'HOUSE_NO': 'permanent_house_no',
            'STREET_ROAD_NO': 'permanent_street_road_no',
            'WARD_COLONY': 'permanent_ward_colony',
            'LANDMARK_MILESTONE': 'permanent_landmark_milestone',
            'LOCALITY_VILLAGE': 'permanent_locality_village',
            'AREA_MANDAL': 'permanent_area_mandal',
            'DISTRICT': 'permanent_district',
            'STATE_UT': 'permanent_state_ut',
            'COUNTRY': 'permanent_country',
            'RESIDENCY_TYPE': 'permanent_residency_type',
            'PIN_CODE': 'permanent_pin_code',
            'JURISDICTION_PS': 'permanent_jurisdiction_ps'
        }
        
        # Contact Details mapping
        contact_fields = {
            'PHONE_NUMBER': 'phone_number',
            'COUNTRY_CODE': 'country_code',
            'EMAIL_ID': 'email_id'
        }
        
        # Top-level fields
        top_level_fields = {
            'PERSON_ID': 'person_id',
            'DATE_CREATED': 'date_created',
            'DATE_MODIFIED': 'date_modified'
        }
        
        # Check top-level fields
        for api_field, db_column in top_level_fields.items():
            if api_field in api_record and db_column not in table_columns:
                new_fields[api_field] = db_column
        
        # Check Personal Details
        personal = api_record.get('PERSONAL_DETAILS', {})
        if isinstance(personal, dict):
            for api_field, db_column in personal_fields.items():
                if api_field in personal and db_column not in table_columns:
                    new_fields[f'PERSONAL_DETAILS.{api_field}'] = db_column
        
        # Check Present Address
        present = api_record.get('PRESENT_ADDRESS', {})
        if isinstance(present, dict):
            for api_field, db_column in present_address_fields.items():
                if api_field in present and db_column not in table_columns:
                    new_fields[f'PRESENT_ADDRESS.{api_field}'] = db_column
        
        # Check Permanent Address
        permanent = api_record.get('PERMANENT_ADDRESS', {})
        if isinstance(permanent, dict):
            for api_field, db_column in permanent_address_fields.items():
                if api_field in permanent and db_column not in table_columns:
                    new_fields[f'PERMANENT_ADDRESS.{api_field}'] = db_column
        
        # Check Contact Details
        contact = api_record.get('CONTACT_DETAILS', {})
        if isinstance(contact, dict):
            for api_field, db_column in contact_fields.items():
                if api_field in contact and db_column not in table_columns:
                    new_fields[f'CONTACT_DETAILS.{api_field}'] = db_column
        
        return new_fields
    
    def add_column_to_table(self, column_name: str, column_type: str = 'TEXT'):
        """Add a new column to the persons table."""
        with self.schema_lock:
            try:
                explicit_type = (column_type or '').strip().upper()
                if explicit_type and explicit_type != 'TEXT':
                    column_type = explicit_type
                else:
                    # Determine column type based on field name
                    if 'date' in column_name.lower():
                        column_type = 'DATE'
                    elif column_name == 'age':
                        column_type = 'INTEGER'
                    elif column_name in ('is_died',):
                        column_type = 'BOOLEAN'
                    elif column_name in ('name', 'surname', 'alias', 'full_name', 'raw_full_name', 'occupation', 
                                        'education_qualification', 'designation', 'place_of_work'):
                        column_type = 'VARCHAR(500)'
                    elif column_name in ('relation_type', 'gender', 'caste', 'sub_caste', 'religion', 
                                        'nationality', 'residency_type', 'country_code', 'gender_source'):
                        column_type = 'VARCHAR(100)'
                    elif column_name == 'gender_confidence':
                        column_type = 'NUMERIC(4,3)'
                    elif 'pin_code' in column_name.lower() or 'jurisdiction_ps' in column_name.lower():
                        column_type = 'VARCHAR(20)'
                    elif 'phone_number' in column_name.lower() or 'email_id' in column_name.lower():
                        column_type = 'VARCHAR(255)'
                    elif 'house_no' in column_name.lower() or 'street' in column_name.lower() or \
                         'ward' in column_name.lower() or 'landmark' in column_name.lower() or \
                         'locality' in column_name.lower() or 'area' in column_name.lower() or \
                         'district' in column_name.lower() or 'state' in column_name.lower() or \
                         'country' in column_name.lower():
                        column_type = 'VARCHAR(255)'
                    else:
                        column_type = 'VARCHAR(255)'
                
                with self.db_pool.get_connection_context() as conn:
                    cursor = conn.cursor()
                    alter_sql = f"ALTER TABLE {PERSONS_TABLE} ADD COLUMN IF NOT EXISTS {column_name} {column_type}"
                    cursor.execute(alter_sql)
                    conn.commit()
                logger.info(f"✅ Added column {column_name} ({column_type}) to {PERSONS_TABLE}")
                return True
            except Exception as e:
                logger.error(f"❌ Error adding column {column_name}: {e}")
                return False

    def ensure_person_enrichment_columns(self, table_columns: Set[str]) -> Set[str]:
        """Ensure required enrichment columns exist in persons table."""
        required_columns = {
            'raw_full_name': 'VARCHAR(500)',
            'gender_confidence': 'NUMERIC(4,3)',
            'gender_source': 'VARCHAR(20)',
            'phone_numbers': 'TEXT'
        }
        for column_name, column_type in required_columns.items():
            if column_name not in table_columns:
                if self.add_column_to_table(column_name, column_type):
                    table_columns.add(column_name)
        return table_columns

    def apply_person_enrichment(self, person_id: str, raw_full_name: Optional[str],
                                gender_confidence: Optional[float], gender_source: Optional[str],
                                phone_numbers: Optional[str],
                                table_columns: Set[str], cursor):
        """Apply enrichment fields that are not part of the base API mapping."""
        updates = []
        values = []

        if 'raw_full_name' in table_columns:
            updates.append('raw_full_name = COALESCE(%s, raw_full_name)')
            values.append(raw_full_name)

        if 'gender_confidence' in table_columns:
            updates.append('gender_confidence = COALESCE(%s, gender_confidence)')
            values.append(gender_confidence)

        if 'gender_source' in table_columns:
            updates.append('gender_source = COALESCE(%s, gender_source)')
            values.append(gender_source)

        if 'phone_numbers' in table_columns:
            updates.append('phone_numbers = COALESCE(%s, phone_numbers)')
            values.append(phone_numbers)

        if not updates:
            return

        values.append(person_id)
        cursor.execute(
            f"""
                UPDATE {PERSONS_TABLE}
                SET {', '.join(updates)}
                WHERE person_id = %s
            """,
            tuple(values)
        )

    def _normalize_confidence(self, value: Optional[Any]) -> Optional[float]:
        if value is None:
            return None
        try:
            return round(float(value), 3)
        except Exception:
            return None

    def log_dry_run_change(self, person_id: str, operation: str, changes: Dict[str, Dict[str, Any]],
                           preview: Dict[str, Any]):
        if not self.dry_run_log_file:
            return
        payload = {
            'timestamp': datetime.now(tz=IST_OFFSET).isoformat(),
            'person_id': person_id,
            'operation': operation,
            'changes': changes,
            'preview': preview,
        }
        with self.dry_run_lock:
            with open(self.dry_run_log_file, 'a', encoding='utf-8') as handle:
                handle.write(json.dumps(payload, ensure_ascii=True) + '\n')

    def handle_dry_run(self, person_id: str, clean_full_name: Optional[str], raw_full_name_value: Optional[str],
                       resolved_gender: Optional[str], gender_confidence: Optional[float],
                       gender_source: Optional[str], phone_numbers_value: Optional[str],
                       table_columns: Set[str], cursor):
        select_columns = ['full_name', 'gender']
        if 'raw_full_name' in table_columns:
            select_columns.append('raw_full_name')
        if 'gender_confidence' in table_columns:
            select_columns.append('gender_confidence')
        if 'gender_source' in table_columns:
            select_columns.append('gender_source')
        if 'phone_numbers' in table_columns:
            select_columns.append('phone_numbers')

        cursor.execute(
            f"""
                SELECT {', '.join(select_columns)}
                FROM {PERSONS_TABLE}
                WHERE person_id = %s
            """,
            (person_id,)
        )
        row = cursor.fetchone()

        new_conf = self._normalize_confidence(gender_confidence)
        if row is None:
            preview = {
                'full_name': clean_full_name,
                'raw_full_name': raw_full_name_value,
                'gender': resolved_gender,
                'gender_confidence': new_conf,
                'gender_source': gender_source,
                'phone_numbers': phone_numbers_value,
            }
            self.log_dry_run_change(person_id, 'insert', {}, preview)
            with self.stats_lock:
                self.stats['dry_run_inserts'] += 1
            return

        idx = {name: i for i, name in enumerate(select_columns)}
        old_full_name = row[idx['full_name']] if 'full_name' in idx else None
        old_gender = row[idx['gender']] if 'gender' in idx else None
        old_raw_full_name = row[idx['raw_full_name']] if 'raw_full_name' in idx else None
        old_conf = row[idx['gender_confidence']] if 'gender_confidence' in idx else None
        old_source = row[idx['gender_source']] if 'gender_source' in idx else None
        old_phone_numbers = row[idx['phone_numbers']] if 'phone_numbers' in idx else None
        old_conf = self._normalize_confidence(old_conf)
        changes: Dict[str, Dict[str, Any]] = {}

        if clean_full_name is not None and clean_full_name != old_full_name:
            changes['full_name'] = {'old': old_full_name, 'new': clean_full_name}
        if raw_full_name_value is not None and raw_full_name_value != old_raw_full_name:
            changes['raw_full_name'] = {'old': old_raw_full_name, 'new': raw_full_name_value}
        if resolved_gender is not None and resolved_gender != old_gender:
            changes['gender'] = {'old': old_gender, 'new': resolved_gender}
        if new_conf is not None and new_conf != old_conf:
            changes['gender_confidence'] = {'old': old_conf, 'new': new_conf}
        if gender_source is not None and gender_source != old_source:
            changes['gender_source'] = {'old': old_source, 'new': gender_source}
        if phone_numbers_value is not None and phone_numbers_value != old_phone_numbers:
            changes['phone_numbers'] = {'old': old_phone_numbers, 'new': phone_numbers_value}

        if changes:
            preview = {
                'full_name': clean_full_name,
                'raw_full_name': raw_full_name_value,
                'gender': resolved_gender,
                'gender_confidence': new_conf,
                'gender_source': gender_source,
                'phone_numbers': phone_numbers_value,
            }
            self.log_dry_run_change(person_id, 'update', changes, preview)
            with self.stats_lock:
                self.stats['dry_run_changes'] += 1
        else:
            with self.stats_lock:
                self.stats['dry_run_no_change'] += 1
    
    def update_existing_records_with_new_fields(self, new_fields: Dict[str, str]):
        """
        Update existing records with new fields.
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
    
    def update_new_fields(self, person_id: str, p: Dict, personal: Dict, present: Dict, 
                         permanent: Dict, contact: Dict, table_columns: Set[str], cursor):
        """
        Update any new fields that were added via schema evolution.
        This handles fields that exist in table_columns but are not in the standard field list.
        """
        if not table_columns:
            return
        
        # Standard fields that are already handled in the main UPDATE/INSERT
        standard_fields = {
            'person_id', 'name', 'surname', 'alias', 'full_name', 'relation_type', 'relative_name',
            'gender', 'is_died', 'date_of_birth', 'age', 'occupation', 'education_qualification',
            'caste', 'sub_caste', 'religion', 'nationality', 'designation', 'place_of_work',
            'raw_full_name', 'gender_confidence', 'gender_source',
            'present_house_no', 'present_street_road_no', 'present_ward_colony',
            'present_landmark_milestone', 'present_locality_village', 'present_area_mandal',
            'present_district', 'present_state_ut', 'present_country', 'present_residency_type',
            'present_pin_code', 'present_jurisdiction_ps',
            'permanent_house_no', 'permanent_street_road_no', 'permanent_ward_colony',
            'permanent_landmark_milestone', 'permanent_locality_village', 'permanent_area_mandal',
            'permanent_district', 'permanent_state_ut', 'permanent_country', 'permanent_residency_type',
            'permanent_pin_code', 'permanent_jurisdiction_ps',
            'phone_number', 'phone_numbers', 'country_code', 'email_id', 'date_created', 'date_modified'
        }
        
        # Find new fields (exist in table but not in standard fields)
        new_fields_to_update = {}
        
        # Check for new fields in API response that exist in table_columns
        # Map API field paths to database column names
        field_mapping = {
            'PERSON_ID': ('person_id', p.get('PERSON_ID')),
            'DATE_CREATED': ('date_created', p.get('DATE_CREATED')),
            'DATE_MODIFIED': ('date_modified', p.get('DATE_MODIFIED')),
        }
        
        # Personal details
        personal_mapping = {
            'NAME': 'name', 'SURNAME': 'surname', 'ALIAS': 'alias', 'FULL_NAME': 'full_name',
            'RELATION_TYPE': 'relation_type', 'RELATIVE_NAME': 'relative_name', 'GENDER': 'gender',
            'IS_DIED': 'is_died', 'DATE_OF_BIRTH': 'date_of_birth', 'AGE': 'age',
            'OCCUPATION': 'occupation', 'EDUCATION_QUALIFICATION': 'education_qualification',
            'CASTE': 'caste', 'SUB_CASTE': 'sub_caste', 'RELIGION': 'religion',
            'NATIONALITY': 'nationality', 'DESIGNATION': 'designation', 'PLACE_OF_WORK': 'place_of_work'
        }
        
        for api_field, db_column in personal_mapping.items():
            if db_column in table_columns and db_column not in standard_fields:
                value = personal.get(api_field)
                if api_field == 'AGE':
                    # Handle age normalization
                    try:
                        if value is None:
                            value = None
                        elif isinstance(value, (int, float)):
                            value = int(value)
                        elif isinstance(value, str):
                            s = value.strip()
                            value = int(s) if s.isdigit() else None
                    except Exception:
                        value = None
                elif api_field == 'DATE_OF_BIRTH':
                    value = value  # Keep as is, will be handled by NULLIF in SQL if needed
                elif api_field in ('NAME', 'SURNAME', 'ALIAS', 'FULL_NAME', 'OCCUPATION', 
                                  'EDUCATION_QUALIFICATION', 'DESIGNATION', 'PLACE_OF_WORK'):
                    value = self.truncate_string(value, 500, db_column)
                elif api_field in ('RELATION_TYPE', 'GENDER', 'CASTE', 'SUB_CASTE', 'RELIGION', 
                                  'NATIONALITY'):
                    value = self.truncate_string(value, 100, db_column)
                else:
                    value = self.truncate_string(value, 255, db_column)
                new_fields_to_update[db_column] = value
        
        # Present address fields
        present_mapping = {
            'HOUSE_NO': 'present_house_no', 'STREET_ROAD_NO': 'present_street_road_no',
            'WARD_COLONY': 'present_ward_colony', 'LANDMARK_MILESTONE': 'present_landmark_milestone',
            'LOCALITY_VILLAGE': 'present_locality_village', 'AREA_MANDAL': 'present_area_mandal',
            'DISTRICT': 'present_district', 'STATE_UT': 'present_state_ut', 'COUNTRY': 'present_country',
            'RESIDENCY_TYPE': 'present_residency_type', 'PIN_CODE': 'present_pin_code',
            'JURISDICTION_PS': 'present_jurisdiction_ps'
        }
        
        for api_field, db_column in present_mapping.items():
            if db_column in table_columns and db_column not in standard_fields:
                value = present.get(api_field)
                if 'pin_code' in db_column or 'jurisdiction_ps' in db_column:
                    value = self.truncate_string(value, 20, db_column)
                else:
                    value = self.truncate_string(value, 255, db_column)
                new_fields_to_update[db_column] = value
        
        # Permanent address fields
        permanent_mapping = {
            'HOUSE_NO': 'permanent_house_no', 'STREET_ROAD_NO': 'permanent_street_road_no',
            'WARD_COLONY': 'permanent_ward_colony', 'LANDMARK_MILESTONE': 'permanent_landmark_milestone',
            'LOCALITY_VILLAGE': 'permanent_locality_village', 'AREA_MANDAL': 'permanent_area_mandal',
            'DISTRICT': 'permanent_district', 'STATE_UT': 'permanent_state_ut', 'COUNTRY': 'permanent_country',
            'RESIDENCY_TYPE': 'permanent_residency_type', 'PIN_CODE': 'permanent_pin_code',
            'JURISDICTION_PS': 'permanent_jurisdiction_ps'
        }
        
        for api_field, db_column in permanent_mapping.items():
            if db_column in table_columns and db_column not in standard_fields:
                value = permanent.get(api_field)
                if 'pin_code' in db_column or 'jurisdiction_ps' in db_column:
                    value = self.truncate_string(value, 20, db_column)
                else:
                    value = self.truncate_string(value, 255, db_column)
                new_fields_to_update[db_column] = value
        
        # Contact fields
        contact_mapping = {
            'PHONE_NUMBER': 'phone_number', 'COUNTRY_CODE': 'country_code', 'EMAIL_ID': 'email_id'
        }
        
        for api_field, db_column in contact_mapping.items():
            if db_column in table_columns and db_column not in standard_fields:
                value = contact.get(api_field)
                if db_column == 'phone_number' or db_column == 'email_id':
                    value = self.truncate_string(value, 255, db_column)
                else:
                    value = self.truncate_string(value, 10, db_column)
                new_fields_to_update[db_column] = value
        
        # If there are new fields to update, execute an UPDATE statement
        if new_fields_to_update:
            try:
                set_clauses = [f"{col} = COALESCE(%s, {col})" for col in new_fields_to_update.keys()]
                update_values_list = list(new_fields_to_update.values()) + [person_id]
                
                update_query = f"""
                    UPDATE {PERSONS_TABLE} SET {', '.join(set_clauses)}
                    WHERE person_id = %s
                """
                cursor.execute(update_query, update_values_list)
                logger.debug(f"Updated new fields for person {person_id}: {list(new_fields_to_update.keys())}")
            except Exception as e:
                logger.warning(f"⚠️  Error updating new fields for person {person_id}: {e}")
                # Don't fail the whole operation, just log the warning

    def get_last_processed_date(self) -> Optional[datetime]:
        """
        Get the last processed date from persons table.
        Returns max(date_created, date_modified) or None if table is empty.
        """
        try:
            with self.db_pool.get_connection_context() as conn:
                cursor = conn.cursor()
                # Check if table has any data
                cursor.execute(f"SELECT COUNT(*) FROM {PERSONS_TABLE}")
                count = cursor.fetchone()[0]
                
                if count == 0:
                    # New database, return None to process all
                    logger.info("📊 Persons table is empty, will process all person_ids from accused table")
                    return None
                
                # Table has data, get max of date_created and date_modified
                # Only consider dates >= 2022-01-01 to avoid processing very old data
                MIN_START_DATE = datetime(2022, 1, 1, tzinfo=IST_OFFSET)
                
                cursor.execute(f"""
                    SELECT GREATEST(
                        COALESCE(MAX(CASE WHEN date_created >= '2022-01-01'::timestamp THEN date_created END), '2022-01-01'::timestamp),
                        COALESCE(MAX(CASE WHEN date_modified >= '2022-01-01'::timestamp THEN date_modified END), '2022-01-01'::timestamp)
                    ) as max_date
                    FROM {PERSONS_TABLE}
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
                        if max_date < MIN_START_DATE:
                            logger.warning(f"⚠️  Max date ({max_date.isoformat()}) is before 2022-01-01, using 2022-01-01")
                            return MIN_START_DATE
                        
                        logger.info(f"📊 Persons table has data, last processed date: {max_date.isoformat()}")
                        return max_date
                
                # Fallback to start date
                logger.warning("⚠️  Could not determine max date, using 2022-01-01")
                return MIN_START_DATE
            
        except Exception as e:
            logger.error(f"❌ Error getting last processed date: {e}")
            logger.warning("⚠️  Using default start date: 2022-01-01")
            return datetime(2022, 1, 1, tzinfo=IST_OFFSET)
    
    def get_person_ids(self) -> List[str]:
        """
        Get person IDs from accused records that were recently added/updated.
        Only processes person_ids from accused records where date_created or date_modified
        is after the last processed date from persons table.
        """
        # Get last processed date from persons table
        last_date = self.get_last_processed_date()
        
        with self.db_pool.get_connection_context() as conn:
            cursor = conn.cursor()
            if last_date is None:
                # First run - process all person_ids from accused table
                logger.info("🔄 First run: Processing all person_ids from accused table")
                cursor.execute(f"""
                    SELECT DISTINCT person_id 
                    FROM {ACCUSED_TABLE} 
                    WHERE person_id IS NOT NULL
                """)
            else:
                # Incremental run - process person_ids from:
                #   1. Accused records updated after last_date (incremental)
                #   2. Stub persons (name IS NULL) — created by accused ETL but never populated,
                #      regardless of accused.date_created/date_modified (which may be NULL or old)
                logger.info(f"🔄 Incremental run: Processing person_ids from accused records updated after {last_date.isoformat()} + any unpopulated stubs")
                cursor.execute(f"""
                    SELECT DISTINCT a.person_id 
                    FROM {ACCUSED_TABLE} a
                    LEFT JOIN {PERSONS_TABLE} p ON a.person_id = p.person_id
                    WHERE a.person_id IS NOT NULL
                    AND (
                        -- Stub person: row exists but core identity fields are all NULL
                        (p.person_id IS NOT NULL AND p.name IS NULL AND p.gender IS NULL
                         AND p.date_of_birth IS NULL AND p.nationality IS NULL)
                        -- Person row doesn't exist at all yet
                        OR p.person_id IS NULL
                        -- Accused record updated after last persons run
                        OR a.date_created >= %s 
                        OR a.date_modified >= %s
                    )
                """, (last_date, last_date))
            
            rows = [r[0] for r in cursor.fetchall()]
        
        with self.stats_lock:
            self.stats['person_ids'] = len(rows)
        
        if last_date is None:
            logger.info(f"📊 Found {len(rows)} person_ids to process (first run - all records)")
        else:
            logger.info(f"📊 Found {len(rows)} person_ids to process (incremental - updated after {last_date.isoformat()} + stubs)")
        
        return rows

    def get_person_ids_for_window(self, from_date: str, to_date: str) -> List[str]:
        """
        Get distinct person IDs from accused records changed in a date window.
        Date window is inclusive and uses accused.date_created/date_modified.
        """
        from_dt = parse_iso_date(from_date)
        to_dt = parse_iso_date(to_date)

        with self.db_pool.get_connection_context() as conn:
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT DISTINCT a.person_id
                FROM {ACCUSED_TABLE} a
                WHERE a.person_id IS NOT NULL
                  AND (
                    (a.date_created IS NOT NULL AND a.date_created >= %s AND a.date_created <= %s)
                    OR
                    (a.date_modified IS NOT NULL AND a.date_modified >= %s AND a.date_modified <= %s)
                  )
            """, (from_dt, to_dt, from_dt, to_dt))

            return [r[0] for r in cursor.fetchall()]

    def generate_date_ranges(self, start_date: str, end_date: str, chunk_days: int = 5, overlap_days: int = 1) -> List[Tuple[str, str]]:
        """Generate YYYY-MM-DD window ranges in chunks with overlap."""
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

            next_start = chunk_end - timedelta(days=overlap_days - 1)
            if chunk_end >= end:
                break
            current_date = next_start

        return date_ranges

    def fetch_person_api(self, person_id: str, from_date: str, to_date: str) -> Optional[Dict]:
        url = f"{API_CONFIG['base_url']}/person-details/{person_id}"
        params = {
            'fromDate': from_date,
            'toDate': to_date
        }
        headers = {'x-api-key': API_CONFIG['api_key']}
        
        for attempt in range(API_CONFIG['max_retries']):
            try:
                logger.debug(f"Fetching person details for {person_id} (Attempt {attempt + 1})")
                resp = requests.get(url, params=params, headers=headers, timeout=API_CONFIG['timeout'])
                
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get('status') and data.get('data'):
                        with self.stats_lock:
                            self.stats['api_calls'] += 1
                        return data['data']
                    else:
                        # API returned 200 but no valid data
                        logger.warning(f"API returned 200 but no valid data for person {person_id}")
                        with self.stats_lock:
                            self.stats['no_data'] += 1
                        return None
                elif resp.status_code == 404:
                    # Person not found - don't retry
                    logger.debug(f"Person {person_id} not found (404)")
                    with self.stats_lock:
                        self.stats['no_data'] += 1
                    return None
                elif resp.status_code == 400:
                    # Specific logic for 400: Bad Request (Likely dead/expunged ID) - skip instantly
                    logger.error(f"API rejected person_id {person_id} (400 Bad Request). Skipping.")
                    with self.stats_lock:
                        self.stats['no_data'] += 1
                    return None
                else:
                    # Other status codes - retry
                    logger.warning(f"API returned status code {resp.status_code}... waiting 60s before retrying")
                    if attempt < API_CONFIG['max_retries'] - 1:
                        time.sleep(60)
                    
            except requests.exceptions.Timeout:
                logger.warning(f"API timeout for person {person_id}, retrying... (Attempt {attempt + 1}/{API_CONFIG['max_retries']})")
                if attempt < API_CONFIG['max_retries'] - 1:
                    time.sleep(2 ** attempt)
            except requests.exceptions.ConnectionError as e:
                logger.warning(f"API connection error for person {person_id}, retrying... (Attempt {attempt + 1}/{API_CONFIG['max_retries']}): {e}")
                if attempt < API_CONFIG['max_retries'] - 1:
                    time.sleep(2 ** attempt)
            except Exception as e:
                logger.error(f"API error for person {person_id}: {e}")
                if attempt == API_CONFIG['max_retries'] - 1:
                    with self.stats_lock:
                        self.stats['failed_api_calls'] += 1
                if attempt < API_CONFIG['max_retries'] - 1:
                    time.sleep(2 ** attempt)
        
        logger.error(f"❌ Failed to fetch person {person_id} after {API_CONFIG['max_retries']} attempts")
        with self.stats_lock:
            self.stats['failed_api_calls'] += 1
        return None

    def truncate_string(self, value: Optional[str], max_length: int = 100, field_name: str = "") -> Optional[str]:
        """Truncate string to max_length, logging if truncation occurs. Empty strings become None."""
        if value is None:
            return None
        if isinstance(value, str) and value.strip() == '':
            return None
        if not isinstance(value, str):
            return str(value)[:max_length] if len(str(value)) > max_length else str(value)
        if len(value) > max_length:
            logger.warning(f"⚠️  Truncating {field_name} from {len(value)} to {max_length} chars for person {getattr(self, '_current_person_id', 'unknown')}")
            return value[:max_length]
        return value

    def upsert_person(self, d: Dict, table_columns: Set[str], conn, cursor):
        p = d or {}
        personal = p.get('PERSONAL_DETAILS') or {}
        present = p.get('PRESENT_ADDRESS') or {}
        permanent = p.get('PERMANENT_ADDRESS') or {}
        contact = p.get('CONTACT_DETAILS') or {}
        
        # Store person_id for truncation logging
        person_id = p.get('PERSON_ID')
        self._current_person_id = person_id
        
        # Extract date fields from API (API dates only, NULL if not provided - no current timestamps)
        date_created = p.get('DATE_CREATED') or None
        date_modified = p.get('DATE_MODIFIED') or None

        # Normalize age to integer or None so we never cast strings like '25' with NULLIF
        raw_age = personal.get('AGE')
        age_value = None
        try:
            if raw_age is None:
                age_value = None
            elif isinstance(raw_age, (int, float)):
                age_value = int(raw_age)
            elif isinstance(raw_age, str):
                s = raw_age.strip()
                age_value = int(s) if s.isdigit() else None
            else:
                age_value = None
        except Exception:
            age_value = None

        api_full_name = personal.get('FULL_NAME')
        raw_full_name_value = self.truncate_string(api_full_name, 500, 'raw_full_name')
        clean_full_name = self._normalize_person_name(api_full_name, personal)
        clean_full_name = self.truncate_string(clean_full_name, 500, 'full_name')
        resolved_gender, gender_confidence, gender_source = self._resolve_gender(clean_full_name, personal.get('GENDER'))
        resolved_gender = self.truncate_string(resolved_gender, 20, 'gender')
        gender_source = self.truncate_string(gender_source, 20, 'gender_source')
        normalized_phone_numbers = self._normalize_phone_numbers(contact.get('PHONE_NUMBER'))
        primary_phone = self.truncate_string(normalized_phone_numbers[0], 20, 'phone_number') if normalized_phone_numbers else None
        phone_numbers_value = self.truncate_string(' | '.join(normalized_phone_numbers), 500, 'phone_numbers') if normalized_phone_numbers else None

        try:
            if self.person_gender_dry_run:
                self.handle_dry_run(
                    person_id=person_id,
                    clean_full_name=clean_full_name,
                    raw_full_name_value=raw_full_name_value,
                    resolved_gender=resolved_gender,
                    gender_confidence=gender_confidence,
                    gender_source=gender_source,
                    phone_numbers_value=phone_numbers_value,
                    table_columns=table_columns,
                    cursor=cursor
                )
                return

            cursor.execute(f"SELECT 1 FROM {PERSONS_TABLE} WHERE person_id = %s", (p.get('PERSON_ID'),))
            exists = cursor.fetchone() is not None

            if exists:
                # Use COALESCE so existing non-NULL values are never overwritten by API nulls
                cursor.execute(
                    f"""
                    UPDATE {PERSONS_TABLE} SET
                        name=COALESCE(%s, name),
                        surname=COALESCE(%s, surname),
                        alias=COALESCE(%s, alias),
                        full_name=COALESCE(%s, full_name),
                        relation_type=COALESCE(%s, relation_type),
                        relative_name=COALESCE(%s, relative_name),
                        gender=COALESCE(%s, gender),
                        is_died=COALESCE(%s, is_died),
                        date_of_birth=COALESCE(NULLIF(%s,'')::date, date_of_birth),
                        age=COALESCE(%s, age),
                        occupation=COALESCE(%s, occupation),
                        education_qualification=COALESCE(%s, education_qualification),
                        caste=COALESCE(%s, caste),
                        sub_caste=COALESCE(%s, sub_caste),
                        religion=COALESCE(%s, religion),
                        nationality=COALESCE(%s, nationality),
                        designation=COALESCE(%s, designation),
                        place_of_work=COALESCE(%s, place_of_work),
                        present_house_no=COALESCE(%s, present_house_no),
                        present_street_road_no=COALESCE(%s, present_street_road_no),
                        present_ward_colony=COALESCE(%s, present_ward_colony),
                        present_landmark_milestone=COALESCE(%s, present_landmark_milestone),
                        present_locality_village=COALESCE(%s, present_locality_village),
                        present_area_mandal=COALESCE(%s, present_area_mandal),
                        present_district=COALESCE(%s, present_district),
                        present_state_ut=COALESCE(%s, present_state_ut),
                        present_country=COALESCE(%s, present_country),
                        present_residency_type=COALESCE(%s, present_residency_type),
                        present_pin_code=COALESCE(%s, present_pin_code),
                        present_jurisdiction_ps=COALESCE(%s, present_jurisdiction_ps),
                        permanent_house_no=COALESCE(%s, permanent_house_no),
                        permanent_street_road_no=COALESCE(%s, permanent_street_road_no),
                        permanent_ward_colony=COALESCE(%s, permanent_ward_colony),
                        permanent_landmark_milestone=COALESCE(%s, permanent_landmark_milestone),
                        permanent_locality_village=COALESCE(%s, permanent_locality_village),
                        permanent_area_mandal=COALESCE(%s, permanent_area_mandal),
                        permanent_district=COALESCE(%s, permanent_district),
                        permanent_state_ut=COALESCE(%s, permanent_state_ut),
                        permanent_country=COALESCE(%s, permanent_country),
                        permanent_residency_type=COALESCE(%s, permanent_residency_type),
                        permanent_pin_code=COALESCE(%s, permanent_pin_code),
                        permanent_jurisdiction_ps=COALESCE(%s, permanent_jurisdiction_ps),
                        phone_number=COALESCE(%s, phone_number),
                        country_code=COALESCE(%s, country_code),
                        email_id=COALESCE(%s, email_id),
                        date_created=COALESCE(%s, date_created),
                        date_modified=COALESCE(%s, date_modified)
                    WHERE person_id=%s
                    """,
                    (
                        self.truncate_string(personal.get('NAME'), 255, 'name'),
                        self.truncate_string(personal.get('SURNAME'), 255, 'surname'),
                        self.truncate_string(personal.get('ALIAS'), 255, 'alias'),
                        clean_full_name,
                        self.truncate_string(personal.get('RELATION_TYPE'), 50, 'relation_type'),
                        self.truncate_string(personal.get('RELATIVE_NAME'), 255, 'relative_name'),
                        resolved_gender,
                        personal.get('IS_DIED'),
                        personal.get('DATE_OF_BIRTH'), age_value,
                        self.truncate_string(personal.get('OCCUPATION'), 255, 'occupation'),
                        self.truncate_string(personal.get('EDUCATION_QUALIFICATION'), 255, 'education_qualification'),
                        self.truncate_string(personal.get('CASTE'), 100, 'caste'),
                        self.truncate_string(personal.get('SUB_CASTE'), 100, 'sub_caste'),
                        self.truncate_string(personal.get('RELIGION'), 100, 'religion'),
                        self.truncate_string(personal.get('NATIONALITY'), 100, 'nationality'),
                        self.truncate_string(personal.get('DESIGNATION'), 255, 'designation'),
                        self.truncate_string(personal.get('PLACE_OF_WORK'), 500, 'place_of_work'),
                        self.truncate_string(present.get('HOUSE_NO'), 255, 'present_house_no'),
                        self.truncate_string(present.get('STREET_ROAD_NO'), 255, 'present_street_road_no'),
                        self.truncate_string(present.get('WARD_COLONY'), 255, 'present_ward_colony'),
                        self.truncate_string(present.get('LANDMARK_MILESTONE'), 255, 'present_landmark_milestone'),
                        self.truncate_string(present.get('LOCALITY_VILLAGE'), 255, 'present_locality_village'),
                        self.truncate_string(present.get('AREA_MANDAL'), 255, 'present_area_mandal'),
                        self.truncate_string(present.get('DISTRICT'), 255, 'present_district'),
                        self.truncate_string(present.get('STATE_UT'), 255, 'present_state_ut'),
                        self.truncate_string(present.get('COUNTRY'), 255, 'present_country'),
                        self.truncate_string(present.get('RESIDENCY_TYPE'), 100, 'present_residency_type'),
                        self.truncate_string(present.get('PIN_CODE'), 20, 'present_pin_code'),
                        self.truncate_string(present.get('JURISDICTION_PS'), 20, 'present_jurisdiction_ps'),
                        self.truncate_string(permanent.get('HOUSE_NO'), 255, 'permanent_house_no'),
                        self.truncate_string(permanent.get('STREET_ROAD_NO'), 255, 'permanent_street_road_no'),
                        self.truncate_string(permanent.get('WARD_COLONY'), 255, 'permanent_ward_colony'),
                        self.truncate_string(permanent.get('LANDMARK_MILESTONE'), 255, 'permanent_landmark_milestone'),
                        self.truncate_string(permanent.get('LOCALITY_VILLAGE'), 255, 'permanent_locality_village'),
                        self.truncate_string(permanent.get('AREA_MANDAL'), 255, 'permanent_area_mandal'),
                        self.truncate_string(permanent.get('DISTRICT'), 255, 'permanent_district'),
                        self.truncate_string(permanent.get('STATE_UT'), 255, 'permanent_state_ut'),
                        self.truncate_string(permanent.get('COUNTRY'), 255, 'permanent_country'),
                        self.truncate_string(permanent.get('RESIDENCY_TYPE'), 100, 'permanent_residency_type'),
                        self.truncate_string(permanent.get('PIN_CODE'), 20, 'permanent_pin_code'),
                        self.truncate_string(permanent.get('JURISDICTION_PS'), 20, 'permanent_jurisdiction_ps'),
                        primary_phone,
                        self.truncate_string(contact.get('COUNTRY_CODE'), 10, 'country_code'),
                        self.truncate_string(contact.get('EMAIL_ID'), 255, 'email_id'),
                        date_created, date_modified,
                        person_id
                    )
                )

                self.apply_person_enrichment(
                    person_id=person_id,
                    raw_full_name=raw_full_name_value,
                    gender_confidence=gender_confidence,
                    gender_source=gender_source,
                    phone_numbers=phone_numbers_value,
                    table_columns=table_columns,
                    cursor=cursor
                )
                with self.stats_lock:
                    self.stats['updated'] += 1
                
                # Update any new fields that were added via schema evolution
                if table_columns:
                    self.update_new_fields(person_id, p, personal, present, permanent, contact, table_columns, cursor)
            else:
                cursor.execute(
                    f"""
                    INSERT INTO {PERSONS_TABLE} (
                        person_id, name, surname, alias, full_name,
                        relation_type, relative_name, gender, is_died,
                        date_of_birth, age, occupation,
                        education_qualification, caste, sub_caste, religion,
                        nationality, designation, place_of_work,
                        present_house_no, present_street_road_no, present_ward_colony,
                        present_landmark_milestone, present_locality_village, present_area_mandal,
                        present_district, present_state_ut, present_country, present_residency_type,
                        present_pin_code, present_jurisdiction_ps,
                        permanent_house_no, permanent_street_road_no, permanent_ward_colony,
                        permanent_landmark_milestone, permanent_locality_village, permanent_area_mandal,
                        permanent_district, permanent_state_ut, permanent_country, permanent_residency_type,
                        permanent_pin_code, permanent_jurisdiction_ps,
                        phone_number, country_code, email_id,
                        date_created, date_modified
                    ) VALUES (
                        %s,%s,%s,%s,%s,
                        %s,%s,%s,%s,
                        NULLIF(%s,'')::date,%s,%s,
                        %s,%s,%s,%s,
                        %s,%s,%s,
                        %s,%s,%s,
                        %s,%s,%s,
                        %s,%s,%s,%s,
                        %s,%s,
                        %s,%s,%s,
                        %s,%s,%s,
                        %s,%s,%s,%s,
                        %s,%s,
                        %s,%s,%s,
                        %s, %s
                    )
                    """,
                    (
                        person_id,
                        self.truncate_string(personal.get('NAME'), 255, 'name'),
                        self.truncate_string(personal.get('SURNAME'), 255, 'surname'),
                        self.truncate_string(personal.get('ALIAS'), 255, 'alias'),
                        clean_full_name,
                        self.truncate_string(personal.get('RELATION_TYPE'), 50, 'relation_type'),
                        self.truncate_string(personal.get('RELATIVE_NAME'), 255, 'relative_name'),
                        resolved_gender,
                        personal.get('IS_DIED'),
                        personal.get('DATE_OF_BIRTH'), age_value,
                        self.truncate_string(personal.get('OCCUPATION'), 255, 'occupation'),
                        self.truncate_string(personal.get('EDUCATION_QUALIFICATION'), 255, 'education_qualification'),
                        self.truncate_string(personal.get('CASTE'), 100, 'caste'),
                        self.truncate_string(personal.get('SUB_CASTE'), 100, 'sub_caste'),
                        self.truncate_string(personal.get('RELIGION'), 100, 'religion'),
                        self.truncate_string(personal.get('NATIONALITY'), 100, 'nationality'),
                        self.truncate_string(personal.get('DESIGNATION'), 255, 'designation'),
                        self.truncate_string(personal.get('PLACE_OF_WORK'), 500, 'place_of_work'),
                        self.truncate_string(present.get('HOUSE_NO'), 255, 'present_house_no'),
                        self.truncate_string(present.get('STREET_ROAD_NO'), 255, 'present_street_road_no'),
                        self.truncate_string(present.get('WARD_COLONY'), 255, 'present_ward_colony'),
                        self.truncate_string(present.get('LANDMARK_MILESTONE'), 255, 'present_landmark_milestone'),
                        self.truncate_string(present.get('LOCALITY_VILLAGE'), 255, 'present_locality_village'),
                        self.truncate_string(present.get('AREA_MANDAL'), 255, 'present_area_mandal'),
                        self.truncate_string(present.get('DISTRICT'), 255, 'present_district'),
                        self.truncate_string(present.get('STATE_UT'), 255, 'present_state_ut'),
                        self.truncate_string(present.get('COUNTRY'), 255, 'present_country'),
                        self.truncate_string(present.get('RESIDENCY_TYPE'), 100, 'present_residency_type'),
                        self.truncate_string(present.get('PIN_CODE'), 20, 'present_pin_code'),
                        self.truncate_string(present.get('JURISDICTION_PS'), 20, 'present_jurisdiction_ps'),
                        self.truncate_string(permanent.get('HOUSE_NO'), 255, 'permanent_house_no'),
                        self.truncate_string(permanent.get('STREET_ROAD_NO'), 255, 'permanent_street_road_no'),
                        self.truncate_string(permanent.get('WARD_COLONY'), 255, 'permanent_ward_colony'),
                        self.truncate_string(permanent.get('LANDMARK_MILESTONE'), 255, 'permanent_landmark_milestone'),
                        self.truncate_string(permanent.get('LOCALITY_VILLAGE'), 255, 'permanent_locality_village'),
                        self.truncate_string(permanent.get('AREA_MANDAL'), 255, 'permanent_area_mandal'),
                        self.truncate_string(permanent.get('DISTRICT'), 255, 'permanent_district'),
                        self.truncate_string(permanent.get('STATE_UT'), 255, 'permanent_state_ut'),
                        self.truncate_string(permanent.get('COUNTRY'), 255, 'permanent_country'),
                        self.truncate_string(permanent.get('RESIDENCY_TYPE'), 100, 'permanent_residency_type'),
                        self.truncate_string(permanent.get('PIN_CODE'), 20, 'permanent_pin_code'),
                        self.truncate_string(permanent.get('JURISDICTION_PS'), 20, 'permanent_jurisdiction_ps'),
                        primary_phone,
                        self.truncate_string(contact.get('COUNTRY_CODE'), 10, 'country_code'),
                        self.truncate_string(contact.get('EMAIL_ID'), 255, 'email_id'),
                        date_created, date_modified
                    )
                )

                self.apply_person_enrichment(
                    person_id=person_id,
                    raw_full_name=raw_full_name_value,
                    gender_confidence=gender_confidence,
                    gender_source=gender_source,
                    phone_numbers=phone_numbers_value,
                    table_columns=table_columns,
                    cursor=cursor
                )
                with self.stats_lock:
                    self.stats['inserted'] += 1
                
                # Update any new fields that were added via schema evolution (for new inserts, new fields will be NULL initially)
                # They'll be updated when the person is reprocessed in future runs
                if table_columns:
                    self.update_new_fields(person_id, p, personal, present, permanent, contact, table_columns, cursor)

            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"❌ Error upserting person {p.get('PERSON_ID')}: {e}")
            with self.stats_lock:
                self.stats['failed'] += 1
                self.stats['errors'] += 1

    def run(self):
        logger.info("=" * 80)
        logger.info("🚀 DOPAMAS ETL Pipeline - Person Details API")
        logger.info("=" * 80)
        logger.info("ℹ️  This process incrementally fetches person details for PERSON_IDs")
        logger.info("ℹ️  Only processes person_ids from recently updated accused records")
        if self.person_gender_dry_run:
            logger.warning("⚠️  DRY RUN MODE ENABLED: writes are disabled and would-change rows are logged only")
            if self.dry_run_log_file:
                logger.warning(f"⚠️  Dry-run change log: {self.dry_run_log_file}")
        logger.info("=" * 80)
        
        if not self.connect_db():
            return False
        try:
            self.ensure_run_state_table()

            # Get table columns for schema evolution
            table_columns = self.get_table_columns(PERSONS_TABLE)
            if self.person_gender_dry_run:
                missing = [c for c in ('raw_full_name', 'gender_confidence', 'gender_source', 'phone_numbers') if c not in table_columns]
                if missing:
                    logger.warning(
                        f"⚠️  Dry-run detected missing enrichment columns {missing}. "
                        "Preview output will omit comparisons for missing columns."
                    )
            else:
                table_columns = self.ensure_person_enrichment_columns(table_columns)
            logger.debug(f"Existing table columns: {sorted(table_columns)}")
            
            last_date = self.get_last_processed_date()
            checkpoint_date = self.get_run_checkpoint('persons')
            resume_boundary = last_date
            if checkpoint_date and (resume_boundary is None or checkpoint_date > resume_boundary):
                resume_boundary = checkpoint_date

            effective_start_date = resume_boundary.isoformat() if resume_boundary else '2022-01-01T00:00:00+05:30'
            calculated_end_date = get_yesterday_end_ist()

            chunk_days = int(os.environ.get('CHUNK_DAYS', '5'))
            overlap_days = int(os.environ.get('CHUNK_OVERLAP_DAYS', '1'))

            date_ranges = self.generate_date_ranges(
                effective_start_date,
                calculated_end_date,
                chunk_days,
                overlap_days
            )

            logger.info(f"Date Range: {effective_start_date} to {calculated_end_date}")
            logger.info(f"Chunk Size: {chunk_days} days (overlap: {overlap_days} day(s))")

            if not date_ranges:
                logger.info("ℹ️  No date ranges to process. Nothing to do.")
                return True

            processed_person_ids = set()
            
            batch_size = 100  # Log batch stats every 100 records
            first_record_processed = False
            
            def process_person(pid, table_columns, from_date, to_date):
                nonlocal first_record_processed
                data = self.fetch_person_api(pid, from_date, to_date)
                if data:
                    # Check for schema evolution on first record
                    if not first_record_processed and table_columns is not None and not self.person_gender_dry_run:
                        with self.schema_lock:
                            # Double check in case another thread already did it
                            if not first_record_processed:
                                new_fields = self.detect_new_fields(data, table_columns)
                                if new_fields:
                                    logger.info(f"🔍 New fields detected in API response: {list(new_fields.keys())}")
                                    # Add new columns to table
                                    for api_field, db_column in new_fields.items():
                                        if self.add_column_to_table(db_column):
                                            # Update table_columns set
                                            table_columns.add(db_column)
                                    # Update existing records with new fields
                                    self.update_existing_records_with_new_fields(new_fields)
                                first_record_processed = True
                    
                    db_retry_attempts = int(os.environ.get('DB_WRITE_MAX_RETRIES', '3'))
                    for db_attempt in range(db_retry_attempts):
                        try:
                            with self.db_pool.get_connection_context() as conn:
                                cursor = conn.cursor()
                                self.upsert_person(data, table_columns, conn, cursor)
                            break
                        except (psycopg2.OperationalError, psycopg2.InterfaceError) as db_err:
                            if db_attempt == db_retry_attempts - 1:
                                raise
                            logger.warning(
                                f"Transient DB error for person {pid}, retrying "
                                f"({db_attempt + 1}/{db_retry_attempts}): {db_err}"
                            )
                            time.sleep(2 ** db_attempt)
                else:
                    with self.stats_lock:
                        self.stats['no_data'] += 1

            requested_workers = int(os.environ.get('MAX_WORKERS', min(32, (os.cpu_count() or 1) * 4)))
            max_workers = compute_safe_workers(self.db_pool, requested_workers)

            for from_date, to_date in tqdm(date_ranges, desc="Processing date ranges", unit="range"):
                window_person_ids = self.get_person_ids_for_window(from_date, to_date)
                window_person_ids = [pid for pid in window_person_ids if pid not in processed_person_ids]

                if not window_person_ids:
                    continue

                for pid in window_person_ids:
                    processed_person_ids.add(pid)

                with self.stats_lock:
                    self.stats['person_ids'] += len(window_person_ids)

                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = {
                        executor.submit(process_person, pid, table_columns, from_date, to_date): pid
                        for pid in window_person_ids
                    }

                    with tqdm(total=len(window_person_ids), desc=f"Persons {from_date} to {to_date}", unit="person", leave=False) as pbar:
                        for idx, future in enumerate(as_completed(futures), 1):
                            pid = futures[future]
                            try:
                                future.result()
                            except Exception as e:
                                logger.error(f"Error processing person {pid}: {e}")
                                with self.stats_lock:
                                    self.stats['failed'] += 1
                                    self.stats['errors'] += 1

                            pbar.update(1)
                            if idx % batch_size == 0:
                                with self.stats_lock:
                                    logger.info(
                                        f"   📊 Progress ({from_date} to {to_date}): {idx}/{len(window_person_ids)} - "
                                        f"Inserted: {self.stats['inserted']}, Updated: {self.stats['updated']}, "
                                        f"Failed: {self.stats['failed']}"
                                    )

            # Get database counts
            with self.db_pool.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM {PERSONS_TABLE}")
                db_persons_count = cursor.fetchone()[0]
            
            logger.info("")
            logger.info("=" * 80)
            logger.info("📊 FINAL STATISTICS")
            logger.info("=" * 80)
            logger.info(f"📡 API CALLS:")
            logger.info(f"  Total API Calls:         {self.stats['api_calls']}")
            logger.info(f"  Failed API Calls:         {self.stats['failed_api_calls']}")
            logger.info(f"")
            logger.info(f"📥 FROM DATABASE (Person IDs from Accused):")
            logger.info(f"  Person IDs to Process:    {self.stats['person_ids']}")
            logger.info(f"")
            logger.info(f"💾 TO DATABASE:")
            logger.info(f"  Total Inserted (New):     {self.stats['inserted']}")
            logger.info(f"  Total Updated:            {self.stats['updated']}")
            logger.info(f"  Total No Change:          {self.stats['no_change']}")
            logger.info(f"  Total No Data (404/dead): {self.stats['no_data']}")
            logger.info(f"  Total Failed:              {self.stats['failed']}")
            logger.info(f"  Total in DB:               {db_persons_count}")
            logger.info(f"")
            logger.info(f"📊 COVERAGE:")
            if self.stats['person_ids'] > 0:
                coverage = ((self.stats['inserted'] + self.stats['updated']) / self.stats['person_ids']) * 100
                logger.info(f"  Processed → DB Coverage: {coverage:.2f}%")
            logger.info(f"")
            logger.info(f"❌ Errors:                  {self.stats['errors']}")
            if self.person_gender_dry_run:
                logger.info("")
                logger.info("🧪 DRY-RUN RESULTS:")
                logger.info(f"  Would Insert:             {self.stats['dry_run_inserts']}")
                logger.info(f"  Would Update:             {self.stats['dry_run_changes']}")
                logger.info(f"  No Change:                {self.stats['dry_run_no_change']}")
            else:
                self.update_run_checkpoint('persons', calculated_end_date)
            logger.info("=" * 80)
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
    etl = PersonsETL()
    success = etl.run()
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
