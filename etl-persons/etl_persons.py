#!/usr/bin/env python3
"""
DOPAMAS ETL Pipeline - Person Details API
Incrementally fetches person details for PERSON_IDs from recently updated accused records
Only processes person_ids from accused records that were added/updated since last run
"""

import sys
import time
import requests
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime, timezone, timedelta
from tqdm import tqdm
import colorlog
from typing import Dict, Optional, List, Set

from config import DB_CONFIG, API_CONFIG, LOG_CONFIG, TABLE_CONFIG

# IST timezone offset (UTC+05:30)
IST_OFFSET = timezone(timedelta(hours=5, minutes=30))

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
        self.db_conn = None
        self.db_cursor = None
        self.stats = {
            'person_ids': 0,
            'api_calls': 0,
            'failed_api_calls': 0,
            'inserted': 0,
            'updated': 0,
            'no_change': 0,  # Records that exist but no changes needed
            'failed': 0,  # Records that failed to process
            'errors': 0
        }

    def connect_db(self):
        try:
            self.db_conn = psycopg2.connect(**DB_CONFIG)
            self.db_cursor = self.db_conn.cursor()
            logger.info(f"✅ Connected to database: {DB_CONFIG['database']}")
            return True
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            return False

    def close_db(self):
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
        try:
            # Determine column type based on field name
            if 'date' in column_name.lower():
                column_type = 'DATE'
            elif column_name == 'age':
                column_type = 'INTEGER'
            elif column_name in ('is_died',):
                column_type = 'BOOLEAN'
            elif column_name in ('name', 'surname', 'alias', 'full_name', 'occupation', 
                                'education_qualification', 'designation', 'place_of_work'):
                column_type = 'VARCHAR(500)'
            elif column_name in ('relation_type', 'gender', 'caste', 'sub_caste', 'religion', 
                                'nationality', 'residency_type', 'country_code'):
                column_type = 'VARCHAR(100)'
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
            
            alter_sql = f"ALTER TABLE {PERSONS_TABLE} ADD COLUMN IF NOT EXISTS {column_name} {column_type}"
            self.db_cursor.execute(alter_sql)
            self.db_conn.commit()
            logger.info(f"✅ Added column {column_name} ({column_type}) to {PERSONS_TABLE}")
            return True
        except Exception as e:
            logger.error(f"❌ Error adding column {column_name}: {e}")
            self.db_conn.rollback()
            return False
    
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
                         permanent: Dict, contact: Dict, table_columns: Set[str]):
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
            'present_house_no', 'present_street_road_no', 'present_ward_colony',
            'present_landmark_milestone', 'present_locality_village', 'present_area_mandal',
            'present_district', 'present_state_ut', 'present_country', 'present_residency_type',
            'present_pin_code', 'present_jurisdiction_ps',
            'permanent_house_no', 'permanent_street_road_no', 'permanent_ward_colony',
            'permanent_landmark_milestone', 'permanent_locality_village', 'permanent_area_mandal',
            'permanent_district', 'permanent_state_ut', 'permanent_country', 'permanent_residency_type',
            'permanent_pin_code', 'permanent_jurisdiction_ps',
            'phone_number', 'country_code', 'email_id', 'date_created', 'date_modified'
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
                set_clauses = [f"{col} = %s" for col in new_fields_to_update.keys()]
                update_values_list = list(new_fields_to_update.values()) + [person_id]
                
                update_query = f"""
                    UPDATE {PERSONS_TABLE} SET {', '.join(set_clauses)}
                    WHERE person_id = %s
                """
                self.db_cursor.execute(update_query, update_values_list)
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
            # Check if table has any data
            self.db_cursor.execute(f"SELECT COUNT(*) FROM {PERSONS_TABLE}")
            count = self.db_cursor.fetchone()[0]
            
            if count == 0:
                # New database, return None to process all
                logger.info("📊 Persons table is empty, will process all person_ids from accused table")
                return None
            
            # Table has data, get max of date_created and date_modified
            # Only consider dates >= 2022-01-01 to avoid processing very old data
            MIN_START_DATE = datetime(2022, 1, 1, tzinfo=IST_OFFSET)
            
            self.db_cursor.execute(f"""
                SELECT GREATEST(
                    COALESCE(MAX(CASE WHEN date_created >= '2022-01-01'::timestamp THEN date_created END), '2022-01-01'::timestamp),
                    COALESCE(MAX(CASE WHEN date_modified >= '2022-01-01'::timestamp THEN date_modified END), '2022-01-01'::timestamp)
                ) as max_date
                FROM {PERSONS_TABLE}
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
        
        if last_date is None:
            # First run - process all person_ids from accused table
            logger.info("🔄 First run: Processing all person_ids from accused table")
            self.db_cursor.execute(f"""
                SELECT DISTINCT person_id 
                FROM {ACCUSED_TABLE} 
                WHERE person_id IS NOT NULL
            """)
        else:
            # Incremental run - only process person_ids from recently updated accused records
            logger.info(f"🔄 Incremental run: Processing person_ids from accused records updated after {last_date.isoformat()}")
            self.db_cursor.execute(f"""
                SELECT DISTINCT person_id 
                FROM {ACCUSED_TABLE} 
                WHERE person_id IS NOT NULL
                AND (
                    date_created >= %s OR 
                    date_modified >= %s
                )
            """, (last_date, last_date))
        
        rows = [r[0] for r in self.db_cursor.fetchall()]
        self.stats['person_ids'] = len(rows)
        
        if last_date is None:
            logger.info(f"📊 Found {len(rows)} person_ids to process (first run - all records)")
        else:
            logger.info(f"📊 Found {len(rows)} person_ids to process (incremental - updated after {last_date.isoformat()})")
        
        return rows

    def fetch_person_api(self, person_id: str) -> Optional[Dict]:
        url = f"{API_CONFIG['base_url']}/person-details/{person_id}"
        params = {
            'fromDate': '2022-01-01',  # API requires dates but seems ignored for details
            'toDate': '2025-12-31'
        }
        headers = {'x-api-key': API_CONFIG['api_key']}
        
        for attempt in range(API_CONFIG['max_retries']):
            try:
                logger.debug(f"Fetching person details for {person_id} (Attempt {attempt + 1})")
                resp = requests.get(url, params=params, headers=headers, timeout=API_CONFIG['timeout'])
                
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get('status') and data.get('data'):
                        self.stats['api_calls'] += 1
                        return data['data']
                    else:
                        # API returned 200 but no valid data
                        logger.warning(f"API returned 200 but no valid data for person {person_id}")
                        return None
                elif resp.status_code == 404:
                    # Person not found - don't retry
                    logger.debug(f"Person {person_id} not found (404)")
                    return None
                elif resp.status_code == 400:
                    # Specific logic for 400: Wait 60s and retry (up to max_retries)
                    logger.warning(f"API returned status code 400 for person {person_id}, waiting 60s before retrying...")
                    if attempt < API_CONFIG['max_retries'] - 1:
                        time.sleep(60)
                else:
                    # Other status codes - retry
                    logger.warning(f"API returned status code {resp.status_code} for person {person_id}, retrying...")
                    if attempt < API_CONFIG['max_retries'] - 1:
                        time.sleep(2 ** attempt)
                    
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
                    self.stats['failed_api_calls'] += 1
                if attempt < API_CONFIG['max_retries'] - 1:
                    time.sleep(2 ** attempt)
        
        logger.error(f"❌ Failed to fetch person {person_id} after {API_CONFIG['max_retries']} attempts")
        self.stats['failed_api_calls'] += 1
        return None

    def truncate_string(self, value: Optional[str], max_length: int = 100, field_name: str = "") -> Optional[str]:
        """Truncate string to max_length, logging if truncation occurs"""
        if value is None:
            return None
        if not isinstance(value, str):
            return str(value)[:max_length] if len(str(value)) > max_length else str(value)
        if len(value) > max_length:
            logger.warning(f"⚠️  Truncating {field_name} from {len(value)} to {max_length} chars for person {getattr(self, '_current_person_id', 'unknown')}")
            return value[:max_length]
        return value

    def upsert_person(self, d: Dict, table_columns: Set[str] = None):
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

        try:
            self.db_cursor.execute(f"SELECT 1 FROM {PERSONS_TABLE} WHERE person_id = %s", (p.get('PERSON_ID'),))
            exists = self.db_cursor.fetchone() is not None

            if exists:
                self.db_cursor.execute(
                    f"""
                    UPDATE {PERSONS_TABLE} SET
                        name=%s, surname=%s, alias=%s, full_name=%s,
                        relation_type=%s, relative_name=%s, gender=%s, is_died=%s,
                        date_of_birth=NULLIF(%s,'')::date, age=%s, occupation=%s,
                        education_qualification=%s, caste=%s, sub_caste=%s, religion=%s,
                        nationality=%s, designation=%s, place_of_work=%s,
                        present_house_no=%s, present_street_road_no=%s, present_ward_colony=%s,
                        present_landmark_milestone=%s, present_locality_village=%s, present_area_mandal=%s,
                        present_district=%s, present_state_ut=%s, present_country=%s, present_residency_type=%s,
                        present_pin_code=%s, present_jurisdiction_ps=%s,
                        permanent_house_no=%s, permanent_street_road_no=%s, permanent_ward_colony=%s,
                        permanent_landmark_milestone=%s, permanent_locality_village=%s, permanent_area_mandal=%s,
                        permanent_district=%s, permanent_state_ut=%s, permanent_country=%s, permanent_residency_type=%s,
                        permanent_pin_code=%s, permanent_jurisdiction_ps=%s,
                        phone_number=%s, country_code=%s, email_id=%s,
                        date_created=%s, date_modified=%s
                    WHERE person_id=%s
                    """,
                    (
                        self.truncate_string(personal.get('NAME'), 255, 'name'),
                        self.truncate_string(personal.get('SURNAME'), 255, 'surname'),
                        self.truncate_string(personal.get('ALIAS'), 255, 'alias'),
                        self.truncate_string(personal.get('FULL_NAME'), 500, 'full_name'),
                        self.truncate_string(personal.get('RELATION_TYPE'), 50, 'relation_type'),
                        self.truncate_string(personal.get('RELATIVE_NAME'), 255, 'relative_name'),
                        self.truncate_string(personal.get('GENDER'), 20, 'gender'),
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
                        self.truncate_string(contact.get('PHONE_NUMBER'), 20, 'phone_number'),
                        self.truncate_string(contact.get('COUNTRY_CODE'), 10, 'country_code'),
                        self.truncate_string(contact.get('EMAIL_ID'), 255, 'email_id'),
                        date_created, date_modified,
                        person_id
                    )
                )
                self.stats['updated'] += 1
                
                # Update any new fields that were added via schema evolution
                if table_columns:
                    self.update_new_fields(person_id, p, personal, present, permanent, contact, table_columns)
            else:
                self.db_cursor.execute(
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
                        self.truncate_string(personal.get('FULL_NAME'), 500, 'full_name'),
                        self.truncate_string(personal.get('RELATION_TYPE'), 50, 'relation_type'),
                        self.truncate_string(personal.get('RELATIVE_NAME'), 255, 'relative_name'),
                        self.truncate_string(personal.get('GENDER'), 20, 'gender'),
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
                        self.truncate_string(contact.get('PHONE_NUMBER'), 20, 'phone_number'),
                        self.truncate_string(contact.get('COUNTRY_CODE'), 10, 'country_code'),
                        self.truncate_string(contact.get('EMAIL_ID'), 255, 'email_id'),
                        date_created, date_modified
                    )
                )
                self.stats['inserted'] += 1
                
                # Update any new fields that were added via schema evolution (for new inserts, new fields will be NULL initially)
                # They'll be updated when the person is reprocessed in future runs
                if table_columns:
                    self.update_new_fields(person_id, p, personal, present, permanent, contact, table_columns)

            self.db_conn.commit()
        except Exception as e:
            self.db_conn.rollback()
            logger.error(f"❌ Error upserting person {p.get('PERSON_ID')}: {e}")
            self.stats['failed'] += 1
            self.stats['errors'] += 1

    def run(self):
        logger.info("=" * 80)
        logger.info("🚀 DOPAMAS ETL Pipeline - Person Details API")
        logger.info("=" * 80)
        logger.info("ℹ️  This process incrementally fetches person details for PERSON_IDs")
        logger.info("ℹ️  Only processes person_ids from recently updated accused records")
        logger.info("=" * 80)
        
        if not self.connect_db():
            return False
        try:
            # Get table columns for schema evolution
            table_columns = self.get_table_columns(PERSONS_TABLE)
            logger.debug(f"Existing table columns: {sorted(table_columns)}")
            
            person_ids = self.get_person_ids()
            logger.info(f"📊 Person IDs to process: {len(person_ids)}")
            
            if not person_ids:
                logger.info("ℹ️  No new/updated person IDs found in accused table. Nothing to process.")
                logger.info("ℹ️  All person records are up to date!")
                return True
            
            batch_size = 100  # Log batch stats every 100 records
            first_record_processed = False
            
            for idx, pid in enumerate(tqdm(person_ids, desc="Processing persons", unit="person"), 1):
                # Store initial stats for batch calculation
                if idx % batch_size == 1:
                    batch_start_inserted = self.stats['inserted']
                    batch_start_updated = self.stats['updated']
                    batch_start_no_change = self.stats['no_change']
                    batch_start_failed = self.stats['failed']
                
                data = self.fetch_person_api(pid)
                if data:
                    # Check for schema evolution on first record
                    if not first_record_processed and table_columns is not None:
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
                    
                    self.upsert_person(data, table_columns)
                else:
                    self.stats['failed'] += 1
                    self.stats['failed_api_calls'] += 1
                
                # Log batch statistics every batch_size records
                if idx % batch_size == 0:
                    batch_inserted = self.stats['inserted'] - batch_start_inserted
                    batch_updated = self.stats['updated'] - batch_start_updated
                    batch_no_change = self.stats['no_change'] - batch_start_no_change
                    batch_failed = self.stats['failed'] - batch_start_failed
                    logger.info(f"   📊 Batch Stats (Records {idx-batch_size+1}-{idx}) - "
                               f"Inserted: {batch_inserted}, Updated: {batch_updated}, "
                               f"No Change: {batch_no_change}, Failed: {batch_failed}")
                
                time.sleep(0.2)

            # Get database counts
            self.db_cursor.execute(f"SELECT COUNT(*) FROM {PERSONS_TABLE}")
            db_persons_count = self.db_cursor.fetchone()[0]
            
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
            logger.info(f"  Total Failed:              {self.stats['failed']}")
            logger.info(f"  Total in DB:               {db_persons_count}")
            logger.info(f"")
            logger.info(f"📊 COVERAGE:")
            if self.stats['person_ids'] > 0:
                coverage = ((self.stats['inserted'] + self.stats['updated']) / self.stats['person_ids']) * 100
                logger.info(f"  Processed → DB Coverage: {coverage:.2f}%")
            logger.info(f"")
            logger.info(f"❌ Errors:                  {self.stats['errors']}")
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
