"""
ETL Pipeline for MongoDB to PostgreSQL Data Migration
Migrates FIR records from MongoDB to PostgreSQL with proper transformations and logging
"""

import os
import sys
import logging
import csv
import json
import hashlib
import secrets
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple
from decimal import Decimal
import uuid
from difflib import SequenceMatcher

import pymongo
from pymongo import MongoClient
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
from psycopg2.pool import ThreadedConnectionPool
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'etl_migration_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class ETLMigration:
    """Main ETL Migration Class"""
    
    def __init__(self):
        """Initialize connections and load reference data"""
        self.mongo_client = None
        self.mongo_db = None
        self.pg_pool = None
        self.state_districts = {}
        self.unmapped_fields = []
        self.stats = {
            'total_records': 0,
            'crimes_inserted': 0,
            'accused_inserted': 0,
            'persons_inserted': 0,
            'brief_facts_drugs_inserted': 0,
            'interrogation_reports_inserted': 0,
            'hierarchy_created': 0,
            'errors': 0,
            'warnings': 0
        }
        
        # Load state-districts mapping
        self._load_state_districts()
        
    def _load_state_districts(self):
        """Load state-districts CSV for state lookup"""
        try:
            csv_path = 'state-districts.csv'
            if not os.path.exists(csv_path):
                logger.warning(f"State-districts CSV not found at {csv_path}")
                return
                
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    district = row.get('district', '').strip()
                    state = row.get('state', '').strip()
                    if district and state:
                        self.state_districts[district.lower()] = state
                        
            logger.info(f"Loaded {len(self.state_districts)} state-district mappings")
        except Exception as e:
            logger.error(f"Error loading state-districts CSV: {e}")
            
    def connect_mongodb(self):
        """Connect to MongoDB"""
        try:
            mongo_uri = os.getenv('MONGO_URI')
            mongo_db_name = os.getenv('MONGO_DB_NAME')
            
            if not mongo_uri or not mongo_db_name:
                raise ValueError("MONGO_URI and MONGO_DB_NAME must be set in .env")
                
            self.mongo_client = MongoClient(mongo_uri)
            self.mongo_db = self.mongo_client[mongo_db_name]
            
            # Test connection
            self.mongo_client.admin.command('ping')
            logger.info(f"Connected to MongoDB: {mongo_db_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            return False
            
    def connect_postgresql(self):
        """Connect to PostgreSQL"""
        try:
            pg_config = {
                'host': os.getenv('POSTGRES_HOST'),
                'port': os.getenv('POSTGRES_PORT'),
                'database': os.getenv('POSTGRES_DB'),
                'user': os.getenv('POSTGRES_USER'),
                'password': os.getenv('POSTGRES_PASSWORD')
            }
            
            if not all([pg_config['host'], pg_config['database'], pg_config['user'], pg_config['password']]):
                raise ValueError("PostgreSQL connection details must be set in .env")
                
            self.pg_pool = ThreadedConnectionPool(
                minconn=1,
                maxconn=10,
                **pg_config
            )
            
            # Test connection
            conn = self.pg_pool.getconn()
            conn.close()
            self.pg_pool.putconn(conn)
            
            logger.info(f"Connected to PostgreSQL: {pg_config['database']}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            return False
            
    def get_pg_connection(self):
        """Get PostgreSQL connection from pool"""
        return self.pg_pool.getconn()
        
    def return_pg_connection(self, conn):
        """Return connection to pool"""
        self.pg_pool.putconn(conn)
        
    def similarity(self, a: str, b: str) -> float:
        """Calculate similarity ratio between two strings"""
        if not a or not b:
            return 0.0
        return SequenceMatcher(None, a.lower().strip(), b.lower().strip()).ratio()
        
    def find_or_create_ps_code(self, ps_name: str, conn) -> Optional[str]:
        """Find PS code from hierarchy table or create new one"""
        if not ps_name:
            logger.warning("PS name is empty, cannot find or create PS code")
            return None
            
        cursor = conn.cursor()
        
        try:
            # First try exact match
            cursor.execute(
                "SELECT ps_code FROM hierarchy WHERE LOWER(TRIM(ps_name)) = LOWER(TRIM(%s))",
                (ps_name,)
            )
            result = cursor.fetchone()
            if result:
                logger.debug(f"Found exact PS match: {ps_name} -> {result[0]}")
                return result[0]
                
            # Try fuzzy matching
            cursor.execute("SELECT ps_code, ps_name FROM hierarchy")
            all_ps = cursor.fetchall()
            
            best_match = None
            best_ratio = 0.0
            
            for ps_code, existing_ps_name in all_ps:
                ratio = self.similarity(ps_name, existing_ps_name)
                if ratio > best_ratio:
                    best_ratio = ratio
                    best_match = (ps_code, existing_ps_name)
                    
            # If similarity is above 0.8, use it
            if best_ratio >= 0.8:
                logger.info(f"Found fuzzy PS match: {ps_name} -> {best_match[0]} (similarity: {best_ratio:.2f})")
                return best_match[0]
                
            # Create new PS code
            logger.info(f"Creating new PS code for: {ps_name}")
            
            # Generate 4-digit code (starting from 9000 to identify as migrated data)
            cursor.execute("SELECT MAX(CAST(ps_code AS INTEGER)) FROM hierarchy WHERE ps_code ~ '^[0-9]+$'")
            max_code = cursor.fetchone()[0]
            
            if max_code and max_code >= 9000:
                new_code = str(int(max_code) + 1).zfill(4)
            else:
                new_code = '9000'  # Start from 9000 for migrated data
                
            # Insert new hierarchy record
            now = datetime.now()
            cursor.execute("""
                INSERT INTO hierarchy (
                    ps_code, ps_name, date_created, date_modified
                ) VALUES (%s, %s, %s, %s)
            """, (new_code, ps_name, now, now))
            
            conn.commit()
            self.stats['hierarchy_created'] += 1
            logger.info(f"Created new PS code: {new_code} for PS: {ps_name}")
            
            return new_code
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error finding/creating PS code for {ps_name}: {e}")
            return None
        finally:
            cursor.close()
            
    def get_state_from_district(self, district: str) -> Optional[str]:
        """Get state name from district using state-districts CSV"""
        if not district:
            return None
        return self.state_districts.get(district.lower().strip())
        
    def convert_height_to_feet(self, height_cm: Optional[Any], height_feet: Optional[Any]) -> Optional[str]:
        """Convert height to feet format"""
        # Priority: use feet if available, otherwise convert from cm
        if height_feet:
            try:
                # If it's already in feet format, return as is
                if isinstance(height_feet, (int, float)):
                    return f"{height_feet:.2f}"
                return str(height_feet)
            except:
                pass
                
        if height_cm:
            try:
                cm_value = float(height_cm)
                feet_value = cm_value / 30.48  # Convert cm to feet
                return f"{feet_value:.2f}"
            except:
                pass
                
        return None
        
    def convert_weight_to_kg(self, weight_gm: Optional[Any], weight_kg: Optional[Any]) -> Optional[float]:
        """Convert weight to kg"""
        # Priority: use kg if available, otherwise convert from grams
        if weight_kg:
            try:
                return float(weight_kg)
            except:
                pass
                
        if weight_gm:
            try:
                gm_value = float(weight_gm)
                kg_value = gm_value / 1000.0  # Convert grams to kg
                return kg_value
            except:
                pass
                
        return None
        
    def get_unmapped_fields(self, mongo_record: Dict[str, Any]) -> Dict[str, Any]:
        """Extract fields that are not mapped to PostgreSQL"""
        # Define all mapped fields
        mapped_fields = {
            '_id', 'FIR_REG_NUM', 'ACT_SEC', 'FIR_NO', 'FIR_STATUS', 'FROM_DT',
            'MAJOR_HEAD', 'MINOR_HEAD', 'PS', 'REG_DT',
            'ACCUSED_NAME', 'ACCUSED_OCCUPATION', 'AGE', 'CASTE', 'DISTRICT',
            'FATHER_NAME', 'GENDER', 'MOBILE_1', 'NATIONALITY',
            'BEARD_TYPE', 'BUILD_TYPE', 'EARS_MISSING', 'EYE_COLOR', 'FACE_TYPE',
            'HAIR_COLOR', 'HEIGHT_FROM_CM', 'HEIGHT_LL_FEET', 'NOSE_TYPE',
            'OTHER_IDENTIFY_MARKS', 'TEETH_TYPE',
            'DRUG_DESC', 'DRUG_PARTICULARS', 'DRUG_PLACE_TYPE', 'DRUG_STATUS',
            'DRUG_TYPE', 'ESTIMATED_VALUE', 'PACKETS_COUNT', 'PAKING_MAKING_DESC',
            'WEIGHT_GM', 'WEIGHT_KG'
        }
        
        # Add all INT_ fields
        for key in mongo_record.keys():
            if key.startswith('INT_'):
                mapped_fields.add(key)
                
        # Extract unmapped fields
        unmapped = {}
        for key, value in mongo_record.items():
            if key not in mapped_fields and not key.startswith('_metadata'):
                unmapped[key] = value
                
        return unmapped
        
    def insert_crime(self, mongo_record: Dict[str, Any], ps_code: str, conn) -> Tuple[Optional[str], bool]:
        """Insert crime record into PostgreSQL"""
        cursor = conn.cursor()
        
        try:
            # Convert MongoDB _id to string (crimes table uses VARCHAR for crime_id)
            mongo_id = mongo_record.get('_id')
            if mongo_id:
                # Convert ObjectId to string if it's an ObjectId object
                if hasattr(mongo_id, '__str__'):
                    crime_id = str(mongo_id)
                else:
                    crime_id = str(mongo_id)
            else:
                # Generate a string ID if _id is missing
                crime_id = str(uuid.uuid4())
                
            # Map fields
            fir_reg_num = mongo_record.get('FIR_REG_NUM')
            acts_sections = mongo_record.get('ACT_SEC')
            fir_num = mongo_record.get('FIR_NO')
            case_status = mongo_record.get('FIR_STATUS')
            from_dt = mongo_record.get('FROM_DT')  # FROM_DT should map to date_created
            major_head = mongo_record.get('MAJOR_HEAD')
            minor_head = mongo_record.get('MINOR_HEAD')
            reg_dt = mongo_record.get('REG_DT')  # REG_DT should map to fir_date
            
            # Parse dates
            # FROM_DT maps to date_created
            date_created = None
            if from_dt:
                try:
                    if isinstance(from_dt, str):
                        date_created = datetime.fromisoformat(from_dt.replace('Z', '+00:00'))
                    else:
                        date_created = from_dt
                except:
                    date_created = datetime.now()
            else:
                date_created = datetime.now()
                
            # REG_DT maps to fir_date
            fir_date_parsed = None
            if reg_dt:
                try:
                    if isinstance(reg_dt, str):
                        fir_date_parsed = datetime.fromisoformat(reg_dt.replace('Z', '+00:00'))
                    else:
                        fir_date_parsed = reg_dt
                except:
                    pass
                    
            now = datetime.now()
            
            cursor.execute("""
                INSERT INTO crimes (
                    crime_id, ps_code, fir_num, fir_reg_num, acts_sections,
                    fir_date, case_status, major_head, minor_head,
                    date_created, date_modified
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (crime_id) DO NOTHING
                RETURNING crime_id
            """, (
                crime_id, ps_code, fir_num, fir_reg_num, acts_sections,
                fir_date_parsed, case_status, major_head, minor_head,
                date_created, now
            ))
            
            result = cursor.fetchone()
            if result:
                conn.commit()
                self.stats['crimes_inserted'] += 1
                logger.info(f"Inserted crime: {crime_id} (FIR: {fir_num})")
                return (crime_id, True)  # Return (crime_id, is_new)
            else:
                logger.info(f"Crime {crime_id} already exists, will skip person/accused insertion")
                return (crime_id, False)  # Return (crime_id, is_new)
                
        except Exception as e:
            conn.rollback()
            logger.error(f"Error inserting crime: {e}")
            self.stats['errors'] += 1
            return (None, False)
        finally:
            cursor.close()
            
    def insert_person(self, mongo_record: Dict[str, Any], conn, commit: bool = True) -> Optional[str]:
        """Insert or update person record
        
        Args:
            mongo_record: MongoDB record
            conn: PostgreSQL connection
            commit: Whether to commit the transaction (default: True)
                   Set to False if you want to commit after accused insertion
        """
        cursor = conn.cursor()
        
        try:
            # Generate person_id as string (persons table uses VARCHAR, 24-char hex format like MongoDB ObjectId)
            # Generate a 24-character hex string to match existing format
            person_id = secrets.token_hex(12)  # 12 bytes = 24 hex characters
            
            # Map fields
            name = mongo_record.get('ACCUSED_NAME')
            occupation = mongo_record.get('ACCUSED_OCCUPATION')
            age = mongo_record.get('AGE')
            caste = mongo_record.get('CASTE')
            district = mongo_record.get('DISTRICT')
            father_name = mongo_record.get('FATHER_NAME')
            gender = mongo_record.get('GENDER')
            mobile = mongo_record.get('MOBILE_1')
            nationality = mongo_record.get('NATIONALITY')
            
            # Get state from district
            state = self.get_state_from_district(district) if district else None
            
            # Set relation type if father name exists
            relation_type = 'Father' if father_name else None
            
            now = datetime.now()
            
            cursor.execute("""
                INSERT INTO persons (
                    person_id, name, occupation, age, caste, permanent_district,
                    permanent_state_ut, relative_name, relation_type, gender,
                    phone_number, nationality, date_created, date_modified
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING person_id
            """, (
                person_id, name, occupation, age, caste, district,
                state, father_name, relation_type, gender,
                mobile, nationality, now, now
            ))
            
            result = cursor.fetchone()
            if result:
                if commit:
                    conn.commit()
                self.stats['persons_inserted'] += 1
                logger.info(f"Inserted person: {person_id} (Name: {name})")
                return person_id
            else:
                return None
                
        except Exception as e:
            if commit:
                conn.rollback()
            logger.error(f"Error inserting person: {e}")
            self.stats['errors'] += 1
            return None
        finally:
            cursor.close()
            
    def insert_accused(self, mongo_record: Dict[str, Any], crime_id: str, 
                      person_id: str, conn, commit: bool = True) -> Optional[str]:
        """Insert accused record
        
        Args:
            mongo_record: MongoDB record
            crime_id: Crime ID
            person_id: Person ID
            conn: PostgreSQL connection
            commit: Whether to commit the transaction (default: True)
                   Set to False if you want to commit after both person and accused are inserted
        """
        cursor = conn.cursor()
        
        try:
            # Generate accused_id as string (accused table uses VARCHAR, 24-char hex format)
            # Generate a 24-character hex string to match existing format
            accused_id = secrets.token_hex(12)  # 12 bytes = 24 hex characters
            
            # Map physical attributes
            beard = mongo_record.get('BEARD_TYPE')
            build = mongo_record.get('BUILD_TYPE')
            ear = mongo_record.get('EARS_MISSING')
            eyes = mongo_record.get('EYE_COLOR')
            face = mongo_record.get('FACE_TYPE')
            hair = mongo_record.get('HAIR_COLOR')
            nose = mongo_record.get('NOSE_TYPE')
            mole = mongo_record.get('OTHER_IDENTIFY_MARKS')
            teeth = mongo_record.get('TEETH_TYPE')
            
            # Convert height
            height = self.convert_height_to_feet(
                mongo_record.get('HEIGHT_FROM_CM'),
                mongo_record.get('HEIGHT_LL_FEET')
            )
            
            # Set accused_code to "A1" (as per requirement)
            accused_code = "A1"
            
            # Set type to "Accused" (as per requirement)
            accused_type = "Accused"
            
            now = datetime.now()
            
            cursor.execute("""
                INSERT INTO accused (
                    accused_id, crime_id, person_id, accused_code, type, beard, build, ear, eyes,
                    face, hair, height, nose, mole, teeth, date_created, date_modified
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING accused_id
            """, (
                accused_id, crime_id, person_id, accused_code, accused_type, beard, build, ear, eyes,
                face, hair, height, nose, mole, teeth, now, now
            ))
            
            result = cursor.fetchone()
            if result:
                if commit:
                    conn.commit()
                self.stats['accused_inserted'] += 1
                logger.info(f"Inserted accused: {accused_id}")
                return accused_id
            else:
                return None
                
        except Exception as e:
            if commit:
                conn.rollback()
            logger.error(f"Error inserting accused: {e}")
            self.stats['errors'] += 1
            return None
        finally:
            cursor.close()
            
    def insert_brief_facts_drugs(self, mongo_record: Dict[str, Any], 
                                 crime_id: str, conn) -> Optional[str]:
        """Insert brief_facts_drugs record"""
        cursor = conn.cursor()
        
        try:
            # Generate UUID for id and convert to string (PostgreSQL UUID type accepts string format)
            drug_id = str(uuid.uuid4())
            
            # Map fields
            drug_name = mongo_record.get('DRUG_TYPE')
            drug_category = mongo_record.get('DRUG_STATUS')
            total_quantity = mongo_record.get('DRUG_PARTICULARS')
            supply_chain = mongo_record.get('DRUG_DESC')
            source_location = mongo_record.get('DRUG_PLACE_TYPE')
            street_value = mongo_record.get('ESTIMATED_VALUE')
            packet_count = mongo_record.get('PACKETS_COUNT')
            packaging_details = mongo_record.get('PAKING_MAKING_DESC')
            
            # Convert weight to kg
            quantity_numeric = self.convert_weight_to_kg(
                mongo_record.get('WEIGHT_GM'),
                mongo_record.get('WEIGHT_KG')
            )
            quantity_unit = 'kg' if quantity_numeric else None
            
            now = datetime.now()
            
            cursor.execute("""
                INSERT INTO brief_facts_drugs (
                    id, crime_id, drug_name, drug_category, total_quantity,
                    quantity_numeric, quantity_unit, supply_chain, source_location,
                    street_value, number_of_packets, packaging_details,
                    date_created, date_modified
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                drug_id, crime_id, drug_name, drug_category, total_quantity,
                quantity_numeric, quantity_unit, supply_chain, source_location,
                street_value, packet_count, packaging_details, now, now
            ))
            
            result = cursor.fetchone()
            if result:
                conn.commit()
                self.stats['brief_facts_drugs_inserted'] += 1
                logger.info(f"Inserted brief_facts_drugs: {drug_id}")
                return drug_id
            else:
                return None
                
        except Exception as e:
            conn.rollback()
            logger.error(f"Error inserting brief_facts_drugs: {e}")
            self.stats['errors'] += 1
            return None
        finally:
            cursor.close()
            
    def insert_interrogation_report(self, mongo_record: Dict[str, Any], 
                                   crime_id: str, conn) -> Optional[str]:
        """Insert old_interrogation_report record"""
        cursor = conn.cursor()
        
        try:
            # Generate UUID for interrogation_report_id and convert to string (PostgreSQL UUID type accepts string format)
            interrogation_report_id = str(uuid.uuid4())
            
            # Map all interrogation fields
            now = datetime.now()
            
            cursor.execute("""
                INSERT INTO old_interragation_report (
                    interrogation_report_id, crime_id,
                    int_aunt_address, int_aunt_mobile_no, int_aunt_name, int_aunt_occupation, int_relation_type_aunt,
                    int_brother_address, int_brother_mobile_no, int_brother_name, int_brother_occupation, int_relation_type_brother,
                    int_daughter_address, int_daughter_mobile_no, int_daughter_name, int_daughter_occupation, int_relation_type_daughter,
                    int_father_address, int_father_mobile_no, int_father_name, int_father_occupation,
                    int_fil_address, int_fil_mobile_no, int_fil_name, int_fil_occupation, int_relation_type_fil,
                    int_friend_address, int_friend_mobile_no, int_friend_name, int_friend_occupation, int_relation_type_friend,
                    int_mil_address, int_mil_mobile_no, int_mil_name, int_mil_occupation, int_relation_type_mil,
                    int_mother_address, int_mother_mobile_no, int_mother_name, int_mother_occupation, int_relation_type_mother,
                    int_sister_address, int_sister_mobile_no, int_sister_name, int_sister_occupation, int_relation_type_sister,
                    int_son_address, int_son_mobile_no, int_son_name, int_son_occupation, int_relation_type_son,
                    int_uncle_address, int_uncle_mobile_no, int_uncle_name, int_uncle_occupation, int_relation_type_uncle,
                    int_wife_address, int_wife_mobile_no, int_wife_name, int_wife_occupation, int_relation_type_wife
                ) VALUES (
                    %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s
                )
                RETURNING interrogation_report_id
            """, (
                interrogation_report_id, crime_id,
                mongo_record.get('INT_AUNT_ADDRESS'), mongo_record.get('INT_AUNT_MOBILE_NO'),
                mongo_record.get('INT_AUNT_NAME'), mongo_record.get('INT_AUNT_OCCUPATION'),
                mongo_record.get('INT_RELATION_TYPE_AUNT'),
                mongo_record.get('INT_BROTHER_ADDRESS'), mongo_record.get('INT_BROTHER_MOBILE_NO'),
                mongo_record.get('INT_BROTHER_NAME'), mongo_record.get('INT_BROTHER_OCCUPATION'),
                mongo_record.get('INT_RELATION_TYPE_BROTHER'),
                mongo_record.get('INT_DAUGHTER_ADDRESS'), mongo_record.get('INT_DAUGHTER_MOBILE_NO'),
                mongo_record.get('INT_DAUGHTER_NAME'), mongo_record.get('INT_DAUGHTER_OCCUPATION'),
                mongo_record.get('INT_RELATION_TYPE_DAUGHTER'),
                mongo_record.get('INT_FATHER_ADDRESS'), mongo_record.get('INT_FATHER_MOBILE_NO'),
                mongo_record.get('INT_FATHER_NAME'), mongo_record.get('INT_FATHER_OCCUPATION'),
                mongo_record.get('INT_FIL_ADDRESS'), mongo_record.get('INT_FIL_MOBILE_NO'),
                mongo_record.get('INT_FIL_NAME'), mongo_record.get('INT_FIL_OCCUPATION'),
                mongo_record.get('INT_RELATION_TYPE_FIL'),
                mongo_record.get('INT_FRIEND_ADDRESS'), mongo_record.get('INT_FRIEND_MOBILE_NO'),
                mongo_record.get('INT_FRIEND_NAME'), mongo_record.get('INT_FRIEND_OCCUPATION'),
                mongo_record.get('INT_RELATION_TYPE_FRIEND'),
                mongo_record.get('INT_MIL_ADDRESS'), mongo_record.get('INT_MIL_MOBILE_NO'),
                mongo_record.get('INT_MIL_NAME'), mongo_record.get('INT_MIL_OCCUPATION'),
                mongo_record.get('INT_RELATION_TYPE_MIL'),
                mongo_record.get('INT_MOTHER_ADDRESS'), mongo_record.get('INT_MOTHER_MOBILE_NO'),
                mongo_record.get('INT_MOTHER_NAME'), mongo_record.get('INT_MOTHER_OCCUPATION'),
                mongo_record.get('INT_RELATION_TYPE_MOTHER'),
                mongo_record.get('INT_SISTER_ADDRESS'), mongo_record.get('INT_SISTER_MOBILE_NO'),
                mongo_record.get('INT_SISTER_NAME'), mongo_record.get('INT_SISTER_OCCUPATION'),
                mongo_record.get('INT_RELATION_TYPE_SISTER'),
                mongo_record.get('INT_SON_ADDRESS'), mongo_record.get('INT_SON_MOBILE_NO'),
                mongo_record.get('INT_SON_NAME'), mongo_record.get('INT_SON_OCCUPATION'),
                mongo_record.get('INT_RELATION_TYPE_SON'),
                mongo_record.get('INT_UNCLE_ADDRESS'), mongo_record.get('INT_UNCLE_MOBILE_NO'),
                mongo_record.get('INT_UNCLE_NAME'), mongo_record.get('INT_UNCLE_OCCUPATION'),
                mongo_record.get('INT_RELATION_TYPE_UNCLE'),
                mongo_record.get('INT_WIFE_ADDRESS'), mongo_record.get('INT_WIFE_MOBILE_NO'),
                mongo_record.get('INT_WIFE_NAME'), mongo_record.get('INT_WIFE_OCCUPATION'),
                mongo_record.get('INT_RELATION_TYPE_WIFE')
            ))
            
            result = cursor.fetchone()
            if result:
                conn.commit()
                self.stats['interrogation_reports_inserted'] += 1
                logger.info(f"Inserted interrogation_report: {interrogation_report_id}")
                return interrogation_report_id
            else:
                return None
                
        except Exception as e:
            conn.rollback()
            logger.error(f"Error inserting interrogation_report: {e}")
            self.stats['errors'] += 1
            return None
        finally:
            cursor.close()
            
    def process_record(self, mongo_record: Dict[str, Any]) -> bool:
        """Process a single MongoDB record"""
        conn = None
        record_id = str(mongo_record.get('_id', 'unknown'))
        
        try:
            conn = self.get_pg_connection()
            
            # Get PS code
            ps_name = mongo_record.get('PS')
            ps_code = self.find_or_create_ps_code(ps_name, conn)
            
            if not ps_code:
                logger.warning(f"Record {record_id}: Could not find or create PS code, skipping")
                self.stats['warnings'] += 1
                return False
                
            # Insert crime
            crime_result = self.insert_crime(mongo_record, ps_code, conn)
            if not crime_result or not crime_result[0]:
                logger.warning(f"Record {record_id}: Failed to insert crime, skipping")
                self.stats['warnings'] += 1
                return False
                
            crime_id, is_new_crime = crime_result
            
            # Check if accused records already exist for this crime
            # If crime exists but no accused records, we still need to insert them
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT COUNT(*) FROM accused WHERE crime_id = %s", (crime_id,))
                accused_count = cursor.fetchone()[0]
            except Exception as e:
                logger.error(f"Error checking accused records: {e}")
                accused_count = 0
            finally:
                cursor.close()
            
            # Check if drug records already exist (check before early return)
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT COUNT(*) FROM brief_facts_drugs WHERE crime_id = %s", (crime_id,))
                drug_count = cursor.fetchone()[0]
            except Exception as e:
                logger.error(f"Error checking drug records: {e}")
                drug_count = 0
            finally:
                cursor.close()
                
            # Check if interrogation records already exist (check before early return)
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT COUNT(*) FROM old_interragation_report WHERE crime_id = %s", (crime_id,))
                int_count = cursor.fetchone()[0]
            except Exception as e:
                logger.error(f"Error checking interrogation records: {e}")
                int_count = 0
            finally:
                cursor.close()
            
            # Check what data exists in MongoDB
            has_drug_data = any([
                mongo_record.get('DRUG_TYPE'),
                mongo_record.get('WEIGHT_GM'),
                mongo_record.get('WEIGHT_KG')
            ])
            has_int_data = any([key.startswith('INT_') for key in mongo_record.keys()])
            
            # If crime exists AND accused records exist AND (drugs don't exist in MongoDB OR already in DB) AND (interrogation doesn't exist in MongoDB OR already in DB), skip everything
            drugs_complete = not has_drug_data or drug_count > 0
            interrogation_complete = not has_int_data or int_count > 0
            
            if not is_new_crime and accused_count > 0 and drugs_complete and interrogation_complete:
                logger.info(f"Record {record_id}: Crime {crime_id} and all related records already exist, skipping")
                return True  # Return True because everything exists, skip
            
            # If crime exists AND accused records exist, skip person/accused but still check drugs/interrogation
            if not is_new_crime and accused_count > 0:
                logger.info(f"Record {record_id}: Crime {crime_id} and accused records exist, skipping person/accused but checking drugs/interrogation")
                # Continue to insert drugs/interrogation if missing
            else:
                # If crime exists but no accused records, we need to insert them
                if not is_new_crime and accused_count == 0:
                    logger.info(f"Record {record_id}: Crime {crime_id} exists but no accused records found, will insert missing records")
                    
                # Insert person (only if accused doesn't exist)
                # Don't commit yet - we'll commit only if accused insertion succeeds
                person_id = self.insert_person(mongo_record, conn, commit=False)
                if not person_id:
                    logger.warning(f"Record {record_id}: Failed to insert person, skipping")
                    conn.rollback()  # Rollback any partial changes
                    self.stats['warnings'] += 1
                    return False
                    
                # Insert accused (only if accused doesn't exist)
                # Don't commit yet - we'll commit only if both person and accused succeed
                accused_id = self.insert_accused(mongo_record, crime_id, person_id, conn, commit=False)
                if not accused_id:
                    logger.warning(f"Record {record_id}: Failed to insert accused, rolling back person insertion")
                    conn.rollback()  # Rollback person insertion since accused failed
                    self.stats['warnings'] += 1
                    # Decrement persons_inserted counter since we're rolling back
                    if self.stats['persons_inserted'] > 0:
                        self.stats['persons_inserted'] -= 1
                    return False
                
                # Both person and accused inserted successfully, commit the transaction
                conn.commit()
                logger.info(f"Record {record_id}: Successfully inserted person {person_id} and accused {accused_id}")
                
            # Insert brief_facts_drugs (if drug-related fields exist and not already inserted)
            if has_drug_data and drug_count == 0:
                logger.info(f"Record {record_id}: Inserting missing brief_facts_drugs for crime {crime_id}")
                self.insert_brief_facts_drugs(mongo_record, crime_id, conn)
            elif has_drug_data and drug_count > 0:
                logger.info(f"Record {record_id}: brief_facts_drugs already exists for crime {crime_id}, skipping")
                    
            # Insert interrogation report (if any INT_ fields exist and not already inserted)
            if has_int_data and int_count == 0:
                logger.info(f"Record {record_id}: Inserting missing interrogation_report for crime {crime_id}")
                self.insert_interrogation_report(mongo_record, crime_id, conn)
            elif has_int_data and int_count > 0:
                logger.info(f"Record {record_id}: interrogation_report already exists for crime {crime_id}, skipping")
                
            # Track unmapped fields
            unmapped = self.get_unmapped_fields(mongo_record)
            if unmapped:
                row = {
                    'record_id': record_id,
                    'crime_id': str(crime_id)
                }
                row.update(unmapped)
                self.unmapped_fields.append(row)
                
            logger.info(f"Successfully processed record {record_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error processing record {record_id}: {e}", exc_info=True)
            self.stats['errors'] += 1
            return False
        finally:
            if conn:
                self.return_pg_connection(conn)
                
    def save_unmapped_fields(self):
        """Save unmapped fields to CSV"""
        if not self.unmapped_fields:
            logger.info("No unmapped fields to save")
            return
            
        filename = f'unmapped_fields_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        
        try:
            # Collect all unique field names
            all_field_names = set(['record_id', 'crime_id'])
            for item in self.unmapped_fields:
                all_field_names.update(item.keys())
            
            field_names = ['record_id', 'crime_id'] + sorted([f for f in all_field_names if f not in ['record_id', 'crime_id']])
            
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=field_names)
                writer.writeheader()
                writer.writerows(self.unmapped_fields)
                
            logger.info(f"Saved {len(self.unmapped_fields)} records with unmapped fields to {filename}")
            logger.info(f"Unmapped fields found: {len(field_names) - 2} unique fields")
        except Exception as e:
            logger.error(f"Error saving unmapped fields: {e}")
            
    def run(self):
        """Run the ETL migration"""
        logger.info("=" * 80)
        logger.info("Starting ETL Migration Process")
        logger.info("=" * 80)
        
        # Connect to databases
        if not self.connect_mongodb():
            logger.error("Failed to connect to MongoDB, aborting")
            return False
            
        if not self.connect_postgresql():
            logger.error("Failed to connect to PostgreSQL, aborting")
            return False
            
        try:
            # Get collection name
            collection_name = os.getenv('MONGO_COLLECTION_NAME')
            collection = self.mongo_db[collection_name]
            
            # Get total count
            total_count = collection.count_documents({})
            self.stats['total_records'] = total_count
            logger.info(f"Total records to process: {total_count}")
            
            # Process records one by one
            processed = 0
            for record in collection.find({}):
                processed += 1
                logger.info(f"Processing record {processed}/{total_count}")
                self.process_record(record)
                
                if processed % 100 == 0:
                    logger.info(f"Progress: {processed}/{total_count} records processed")
                    self.print_stats()
                    
            # Save unmapped fields
            self.save_unmapped_fields()
            
            # Print final stats
            logger.info("=" * 80)
            logger.info("ETL Migration Completed")
            logger.info("=" * 80)
            self.print_stats()
            
            return True
            
        except Exception as e:
            logger.error(f"Error during migration: {e}", exc_info=True)
            return False
        finally:
            # Close connections
            if self.mongo_client:
                self.mongo_client.close()
            if self.pg_pool:
                self.pg_pool.closeall()
                
    def print_stats(self):
        """Print migration statistics"""
        logger.info("Migration Statistics:")
        logger.info(f"  Total Records: {self.stats['total_records']}")
        logger.info(f"  Crimes Inserted: {self.stats['crimes_inserted']}")
        logger.info(f"  Accused Inserted: {self.stats['accused_inserted']}")
        logger.info(f"  Persons Inserted: {self.stats['persons_inserted']}")
        logger.info(f"  Brief Facts Drugs Inserted: {self.stats['brief_facts_drugs_inserted']}")
        logger.info(f"  Interrogation Reports Inserted: {self.stats['interrogation_reports_inserted']}")
        logger.info(f"  Hierarchy Records Created: {self.stats['hierarchy_created']}")
        logger.info(f"  Errors: {self.stats['errors']}")
        logger.info(f"  Warnings: {self.stats['warnings']}")
        logger.info(f"  Unmapped Fields Records: {len(self.unmapped_fields)}")


if __name__ == '__main__':
    migration = ETLMigration()
    migration.run()


