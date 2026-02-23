"""
Schema Reference - Exact PostgreSQL Schema from Your Database
THIS IS THE SOURCE OF TRUTH for all column names and data types
"""

from typing import Dict, List
from dataclasses import dataclass, field

@dataclass
class ColumnInfo:
    """Column information"""
    name: str
    data_type: str
    max_length: int = None
    is_nullable: bool = True
    default: str = None

@dataclass
class TableSchema:
    """Table schema with all columns"""
    name: str
    columns: List[ColumnInfo] = field(default_factory=list)
    primary_key: str = None
    description: str = None

# ============================================================================
# ACTUAL SCHEMA FROM YOUR DATABASE (Source of Truth)
# ============================================================================

POSTGRESQL_SCHEMA = {
    'accused': TableSchema(
        name='accused',
        primary_key='accused_id',
        description='Accused person details with physical features',
        columns=[
            ColumnInfo('accused_id', 'character varying', 50, False),
            ColumnInfo('crime_id', 'character varying', 50, False),
            ColumnInfo('person_id', 'character varying', 50, False),
            ColumnInfo('accused_code', 'character varying', 20, False),
            ColumnInfo('type', 'character varying', 50, True),
            ColumnInfo('seq_num', 'character varying', 50, True),
            ColumnInfo('is_ccl', 'boolean', None, True),
            ColumnInfo('beard', 'character varying', 100, True),
            ColumnInfo('build', 'character varying', 100, True),
            ColumnInfo('color', 'character varying', 100, True),
            ColumnInfo('ear', 'character varying', 100, True),
            ColumnInfo('eyes', 'character varying', 100, True),
            ColumnInfo('face', 'character varying', 100, True),
            ColumnInfo('hair', 'character varying', 100, True),
            ColumnInfo('height', 'character varying', 100, True),
            ColumnInfo('leucoderma', 'character varying', 100, True),
            ColumnInfo('mole', 'character varying', 100, True),
            ColumnInfo('mustache', 'character varying', 100, True),
            ColumnInfo('nose', 'character varying', 100, True),
            ColumnInfo('teeth', 'character varying', 100, True),
            ColumnInfo('physical_features_embedding', 'vector', None, True),
            ColumnInfo('date_created', 'timestamp without time zone', None, True),
            ColumnInfo('date_modified', 'timestamp without time zone', None, True),
            ColumnInfo('case_count', 'integer', None, True),
        ]
    ),
    
    'brief_facts_accused': TableSchema(
        name='brief_facts_accused',
        primary_key='bf_accused_id',
        description='Brief facts about accused persons',
        columns=[
            ColumnInfo('bf_accused_id', 'uuid', None, False),
            ColumnInfo('crime_id', 'character varying', 50, False),
            ColumnInfo('accused_id', 'character varying', 50, True),
            ColumnInfo('person_id', 'character varying', 50, True),
            ColumnInfo('person_code', 'character varying', 50, True),
            ColumnInfo('seq_num', 'character varying', 50, True),
            ColumnInfo('full_name', 'character varying', 500, True),
            ColumnInfo('alias_name', 'character varying', 255, True),
            ColumnInfo('age', 'integer', None, True),
            ColumnInfo('gender', 'character varying', 20, True),
            ColumnInfo('occupation', 'character varying', 255, True),
            ColumnInfo('address', 'text', None, True),
            ColumnInfo('phone_numbers', 'character varying', 255, True),
            ColumnInfo('role_in_crime', 'text', None, True),
            ColumnInfo('key_details', 'text', None, True),
            ColumnInfo('accused_type', 'character varying', 40, True),
            ColumnInfo('status', 'character varying', 40, True),
            ColumnInfo('is_ccl', 'boolean', None, True),
            ColumnInfo('source_person_fields', 'jsonb', None, True),
            ColumnInfo('source_accused_fields', 'jsonb', None, True),
            ColumnInfo('source_summary_fields', 'jsonb', None, True),
            ColumnInfo('date_created', 'timestamp without time zone', None, True),
            ColumnInfo('date_modified', 'timestamp without time zone', None, True),
        ]
    ),
    
    'brief_facts_crime_summaries': TableSchema(
        name='brief_facts_crime_summaries',
        primary_key='crime_id',
        description='AI-generated crime summaries',
        columns=[
            ColumnInfo('crime_id', 'character varying', None, False),
            ColumnInfo('summary_text', 'text', None, False),
            ColumnInfo('summary_json', 'jsonb', None, True),
            ColumnInfo('word_count', 'integer', None, True),
            ColumnInfo('processing_time_seconds', 'numeric', None, True),
            ColumnInfo('model_name', 'character varying', None, True),
            ColumnInfo('date_created', 'timestamp without time zone', None, True),
            ColumnInfo('date_modified', 'timestamp without time zone', None, True),
        ]
    ),
    
    'brief_facts_drugs': TableSchema(
        name='brief_facts_drugs',
        primary_key='id',
        description='Drug-related crime details',
        columns=[
            ColumnInfo('id', 'integer', None, False),
            ColumnInfo('crime_id', 'character varying', None, False),
            ColumnInfo('drug_name', 'character varying', None, False),
            ColumnInfo('scientific_name', 'character varying', None, True),
            ColumnInfo('brand_name', 'character varying', None, True),
            ColumnInfo('drug_category', 'character varying', None, True),
            ColumnInfo('drug_schedule', 'character varying', None, True),
            ColumnInfo('total_quantity', 'character varying', None, True),
            ColumnInfo('quantity_numeric', 'numeric', None, True),
            ColumnInfo('quantity_unit', 'character varying', None, True),
            ColumnInfo('number_of_packets', 'character varying', None, True),
            ColumnInfo('weight_breakdown', 'text', None, True),
            ColumnInfo('packaging_details', 'character varying', None, True),
            ColumnInfo('source_location', 'character varying', None, True),
            ColumnInfo('destination', 'character varying', None, True),
            ColumnInfo('transport_method', 'character varying', None, True),
            ColumnInfo('supply_chain', 'text', None, True),
            ColumnInfo('seizure_location', 'character varying', None, True),
            ColumnInfo('seizure_time', 'character varying', None, True),
            ColumnInfo('seizure_method', 'character varying', None, True),
            ColumnInfo('seizure_officer', 'character varying', None, True),
            ColumnInfo('commercial_quantity', 'character varying', None, True),
            ColumnInfo('is_commercial', 'boolean', None, True),
            ColumnInfo('street_value', 'character varying', None, True),
            ColumnInfo('street_value_numeric', 'numeric', None, True),
            ColumnInfo('purity', 'character varying', None, True),
            ColumnInfo('date_created', 'timestamp without time zone', None, True),
            ColumnInfo('date_modified', 'timestamp without time zone', None, True),
            ColumnInfo('seizure_worth', 'integer', None, True),
        ]
    ),
    
    'crimes': TableSchema(
        name='crimes',
        primary_key='crime_id',
        description='Main crime/FIR records',
        columns=[
            ColumnInfo('crime_id', 'character varying', 50, False),
            ColumnInfo('ps_code', 'character varying', 20, False),
            ColumnInfo('fir_num', 'character varying', 50, False),
            ColumnInfo('fir_reg_num', 'character varying', 50, False),
            ColumnInfo('fir_type', 'character varying', 50, True),
            ColumnInfo('acts_sections', 'text', None, True),
            ColumnInfo('fir_date', 'timestamp without time zone', None, True),
            ColumnInfo('case_status', 'character varying', 100, True),
            ColumnInfo('major_head', 'character varying', 100, True),
            ColumnInfo('minor_head', 'character varying', 255, True),
            ColumnInfo('crime_type', 'character varying', 100, True),
            ColumnInfo('io_name', 'character varying', 255, True),
            ColumnInfo('io_rank', 'character varying', 100, True),
            ColumnInfo('brief_facts', 'text', None, True),
            ColumnInfo('brief_facts_embedding', 'vector', None, True),
            ColumnInfo('crime_pattern_embedding', 'vector', None, True),
            ColumnInfo('date_created', 'timestamp without time zone', None, True),
            ColumnInfo('date_modified', 'timestamp without time zone', None, True),
            ColumnInfo('class_classification', 'character varying', 50, True),
        ]
    ),
    
    'hierarchy': TableSchema(
        name='hierarchy',
        primary_key='ps_code',
        description='Police station hierarchy (district, zone, circle, etc.)',
        columns=[
            ColumnInfo('ps_code', 'character varying', 20, False),
            ColumnInfo('ps_name', 'character varying', 255, False),
            ColumnInfo('circle_code', 'character varying', 20, True),
            ColumnInfo('circle_name', 'character varying', 255, True),
            ColumnInfo('sdpo_code', 'character varying', 20, True),
            ColumnInfo('sdpo_name', 'character varying', 255, True),
            ColumnInfo('sub_zone_code', 'character varying', 20, True),
            ColumnInfo('sub_zone_name', 'character varying', 255, True),
            ColumnInfo('dist_code', 'character varying', 20, True),
            ColumnInfo('dist_name', 'character varying', 255, True),
            ColumnInfo('range_code', 'character varying', 20, True),
            ColumnInfo('range_name', 'character varying', 255, True),
            ColumnInfo('zone_code', 'character varying', 20, True),
            ColumnInfo('zone_name', 'character varying', 255, True),
            ColumnInfo('adg_code', 'character varying', 20, True),
            ColumnInfo('adg_name', 'character varying', 255, True),
            ColumnInfo('created_at', 'timestamp without time zone', None, True),
            ColumnInfo('updated_at', 'timestamp without time zone', None, True),
        ]
    ),
    
    'persons': TableSchema(
        name='persons',
        primary_key='person_id',
        description='Person master data with full details',
        columns=[
            ColumnInfo('person_id', 'character varying', 50, False),
            ColumnInfo('name', 'character varying', 255, True),
            ColumnInfo('surname', 'character varying', 255, True),
            ColumnInfo('alias', 'character varying', 255, True),
            ColumnInfo('full_name', 'character varying', 500, True),
            ColumnInfo('relation_type', 'character varying', 50, True),
            ColumnInfo('relative_name', 'character varying', 255, True),
            ColumnInfo('gender', 'character varying', 20, True),
            ColumnInfo('is_died', 'boolean', None, True),
            ColumnInfo('date_of_birth', 'date', None, True),
            ColumnInfo('age', 'integer', None, True),
            ColumnInfo('occupation', 'character varying', 255, True),
            ColumnInfo('education_qualification', 'character varying', 255, True),
            ColumnInfo('caste', 'character varying', 255, True),
            ColumnInfo('sub_caste', 'character varying', 255, True),
            ColumnInfo('religion', 'character varying', 255, True),
            ColumnInfo('nationality', 'character varying', 255, True),
            ColumnInfo('designation', 'character varying', 255, True),
            ColumnInfo('place_of_work', 'character varying', 500, True),
            # Present address
            ColumnInfo('present_house_no', 'character varying', 255, True),
            ColumnInfo('present_street_road_no', 'character varying', 255, True),
            ColumnInfo('present_ward_colony', 'character varying', 255, True),
            ColumnInfo('present_landmark_milestone', 'character varying', 255, True),
            ColumnInfo('present_locality_village', 'character varying', 255, True),
            ColumnInfo('present_area_mandal', 'character varying', 255, True),
            ColumnInfo('present_district', 'character varying', 255, True),
            ColumnInfo('present_state_ut', 'character varying', 255, True),
            ColumnInfo('present_country', 'character varying', 255, True),
            ColumnInfo('present_residency_type', 'character varying', 255, True),
            ColumnInfo('present_pin_code', 'character varying', 20, True),
            ColumnInfo('present_jurisdiction_ps', 'character varying', 20, True),
            # Permanent address
            ColumnInfo('permanent_house_no', 'character varying', 255, True),
            ColumnInfo('permanent_street_road_no', 'character varying', 255, True),
            ColumnInfo('permanent_ward_colony', 'character varying', 255, True),
            ColumnInfo('permanent_landmark_milestone', 'character varying', 255, True),
            ColumnInfo('permanent_locality_village', 'character varying', 255, True),
            ColumnInfo('permanent_area_mandal', 'character varying', 255, True),
            ColumnInfo('permanent_district', 'character varying', 255, True),
            ColumnInfo('permanent_state_ut', 'character varying', 255, True),
            ColumnInfo('permanent_country', 'character varying', 255, True),
            ColumnInfo('permanent_residency_type', 'character varying', 255, True),
            ColumnInfo('permanent_pin_code', 'character varying', 20, True),
            ColumnInfo('permanent_jurisdiction_ps', 'character varying', 20, True),
            # Contact
            ColumnInfo('phone_number', 'character varying', 20, True),
            ColumnInfo('country_code', 'character varying', 10, True),
            ColumnInfo('email_id', 'character varying', 255, True),
            # Embeddings
            ColumnInfo('name_embedding', 'vector', None, True),
            ColumnInfo('profile_embedding', 'vector', None, True),
            # Audit
            ColumnInfo('date_created', 'timestamp without time zone', None, True),
            ColumnInfo('date_modified', 'timestamp without time zone', None, True),
            ColumnInfo('domicile_classification', 'character varying', 50, True),
        ]
    ),
    
    'brief_facts_crime_summaries': TableSchema(
        name='brief_facts_crime_summaries',
        primary_key='crime_id',
        description='AI-generated crime summaries',
        columns=[
            ColumnInfo('crime_id', 'character varying', None, False),
            ColumnInfo('summary_text', 'text', None, False),
            ColumnInfo('summary_json', 'jsonb', None, True),
            ColumnInfo('word_count', 'integer', None, True),
            ColumnInfo('processing_time_seconds', 'numeric', None, True),
            ColumnInfo('model_name', 'character varying', None, True),
            ColumnInfo('date_created', 'timestamp without time zone', None, True),
            ColumnInfo('date_modified', 'timestamp without time zone', None, True),
        ]
    ),
    
    'brief_facts_drugs': TableSchema(
        name='brief_facts_drugs',
        primary_key='id',
        description='Drug seizure details',
        columns=[
            ColumnInfo('id', 'integer', None, False),
            ColumnInfo('crime_id', 'character varying', None, False),
            ColumnInfo('drug_name', 'character varying', None, False),
            ColumnInfo('scientific_name', 'character varying', None, True),
            ColumnInfo('brand_name', 'character varying', None, True),
            ColumnInfo('drug_category', 'character varying', None, True),
            ColumnInfo('drug_schedule', 'character varying', None, True),
            ColumnInfo('total_quantity', 'character varying', None, True),
            ColumnInfo('quantity_numeric', 'numeric', None, True),
            ColumnInfo('quantity_unit', 'character varying', None, True),
            ColumnInfo('number_of_packets', 'character varying', None, True),
            ColumnInfo('weight_breakdown', 'text', None, True),
            ColumnInfo('packaging_details', 'character varying', None, True),
            ColumnInfo('source_location', 'character varying', None, True),
            ColumnInfo('destination', 'character varying', None, True),
            ColumnInfo('transport_method', 'character varying', None, True),
            ColumnInfo('supply_chain', 'text', None, True),
            ColumnInfo('seizure_location', 'character varying', None, True),
            ColumnInfo('seizure_time', 'character varying', None, True),
            ColumnInfo('seizure_method', 'character varying', None, True),
            ColumnInfo('seizure_officer', 'character varying', None, True),
            ColumnInfo('commercial_quantity', 'character varying', None, True),
            ColumnInfo('is_commercial', 'boolean', None, True),
            ColumnInfo('street_value', 'character varying', None, True),
            ColumnInfo('street_value_numeric', 'numeric', None, True),
            ColumnInfo('purity', 'character varying', None, True),
            ColumnInfo('date_created', 'timestamp without time zone', None, True),
            ColumnInfo('date_modified', 'timestamp without time zone', None, True),
            ColumnInfo('seizure_worth', 'integer', None, True),
        ]
    ),
    
    'crimes': TableSchema(
        name='crimes',
        primary_key='crime_id',
        description='Main crime/FIR records',
        columns=[
            ColumnInfo('crime_id', 'character varying', 50, False),
            ColumnInfo('ps_code', 'character varying', 20, False),
            ColumnInfo('fir_num', 'character varying', 50, False),
            ColumnInfo('fir_reg_num', 'character varying', 50, False),
            ColumnInfo('fir_type', 'character varying', 50, True),
            ColumnInfo('acts_sections', 'text', None, True),
            ColumnInfo('fir_date', 'timestamp without time zone', None, True),
            ColumnInfo('case_status', 'character varying', 100, True),
            ColumnInfo('major_head', 'character varying', 100, True),
            ColumnInfo('minor_head', 'character varying', 255, True),
            ColumnInfo('crime_type', 'character varying', 100, True),
            ColumnInfo('io_name', 'character varying', 255, True),
            ColumnInfo('io_rank', 'character varying', 100, True),
            ColumnInfo('brief_facts', 'text', None, True),
            ColumnInfo('brief_facts_embedding', 'vector', None, True),
            ColumnInfo('crime_pattern_embedding', 'vector', None, True),
            ColumnInfo('date_created', 'timestamp without time zone', None, True),
            ColumnInfo('date_modified', 'timestamp without time zone', None, True),
            ColumnInfo('class_classification', 'character varying', 50, True),
        ]
    ),
    
    'hierarchy': TableSchema(
        name='hierarchy',
        primary_key='ps_code',
        description='Police station hierarchy',
        columns=[
            ColumnInfo('ps_code', 'character varying', 20, False),
            ColumnInfo('ps_name', 'character varying', 255, False),
            ColumnInfo('circle_code', 'character varying', 20, True),
            ColumnInfo('circle_name', 'character varying', 255, True),
            ColumnInfo('sdpo_code', 'character varying', 20, True),
            ColumnInfo('sdpo_name', 'character varying', 255, True),
            ColumnInfo('sub_zone_code', 'character varying', 20, True),
            ColumnInfo('sub_zone_name', 'character varying', 255, True),
            ColumnInfo('dist_code', 'character varying', 20, True),
            ColumnInfo('dist_name', 'character varying', 255, True),
            ColumnInfo('range_code', 'character varying', 20, True),
            ColumnInfo('range_name', 'character varying', 255, True),
            ColumnInfo('zone_code', 'character varying', 20, True),
            ColumnInfo('zone_name', 'character varying', 255, True),
            ColumnInfo('adg_code', 'character varying', 20, True),
            ColumnInfo('adg_name', 'character varying', 255, True),
            ColumnInfo('created_at', 'timestamp without time zone', None, True),
            ColumnInfo('updated_at', 'timestamp without time zone', None, True),
        ]
    ),
    
    'properties': TableSchema(
        name='properties',
        primary_key='property_id',
        description='Seized/recovered properties',
        columns=[
            ColumnInfo('property_id', 'character varying', 50, False),
            ColumnInfo('crime_id', 'character varying', 50, False),
            ColumnInfo('case_property_id', 'character varying', 50, True),
            ColumnInfo('property_status', 'character varying', 100, True),
            ColumnInfo('recovered_from', 'character varying', 255, True),
            ColumnInfo('place_of_recovery', 'text', None, True),
            ColumnInfo('date_of_seizure', 'timestamp without time zone', None, True),
            ColumnInfo('nature', 'character varying', 255, True),
            ColumnInfo('belongs', 'character varying', 100, True),
            ColumnInfo('estimate_value', 'numeric', None, True),
            ColumnInfo('recovered_value', 'numeric', None, True),
            ColumnInfo('particular_of_property', 'text', None, True),
            ColumnInfo('category', 'character varying', 100, True),
            ColumnInfo('additional_details', 'jsonb', None, True),
            ColumnInfo('media', 'jsonb', None, True),
            ColumnInfo('date_created', 'timestamp without time zone', None, True),
            ColumnInfo('date_modified', 'timestamp without time zone', None, True),
            ColumnInfo('description_embedding', 'vector', None, True),
            ColumnInfo('property_profile_embedding', 'vector', None, True),
        ]
    ),
}

# ============================================================================
# COLUMN NAME MAPPINGS FOR ENTITY SEARCHES
# ============================================================================

COLUMN_MAPPINGS = {
    'phone_number': {
        'persons': ['phone_number'],
        'brief_facts_accused': ['phone_numbers'],
    },
    'email': {
        'persons': ['email_id'],
    },
    'name': {
        'persons': ['name', 'surname', 'full_name', 'alias'],
        'brief_facts_accused': ['full_name', 'alias_name'],
        'crimes': ['io_name'],
        'hierarchy': ['ps_name', 'circle_name', 'dist_name', 'zone_name'],
    },
    'crime_id': {
        'crimes': ['crime_id'],
        'accused': ['crime_id'],
        'brief_facts_accused': ['crime_id'],
        'brief_facts_crime_summaries': ['crime_id'],
        'brief_facts_drugs': ['crime_id'],
        'properties': ['crime_id'],
    },
    'person_id': {
        'persons': ['person_id'],
        'accused': ['person_id'],
        'brief_facts_accused': ['person_id'],
    },
    'status': {
        'crimes': ['case_status'],
        'brief_facts_accused': ['status'],
        'properties': ['property_status'],
    },
    'address': {
        'persons': [
            'present_house_no', 'present_street_road_no', 'present_ward_colony',
            'present_locality_village', 'present_district', 'present_state_ut',
            'permanent_house_no', 'permanent_district', 'permanent_state_ut'
        ],
        'brief_facts_accused': ['address'],
    },
    'district': {
        'persons': ['present_district', 'permanent_district'],
        'hierarchy': ['dist_name', 'dist_code'],
    },
    'date': {
        'crimes': ['fir_date', 'date_created'],
        'persons': ['date_of_birth', 'date_created'],
        'properties': ['date_of_seizure', 'date_created'],
        'brief_facts_drugs': ['date_created'],
    },
}

# ============================================================================
# IMPORTANT TABLES FOR DIFFERENT QUERIES
# ============================================================================

QUERY_TABLE_HINTS = {
    'person_search': ['persons', 'brief_facts_accused'],
    'crime_search': ['crimes', 'brief_facts_crime_summaries'],
    'drug_search': ['brief_facts_drugs'],
    'property_search': ['properties'],
    'location_search': ['hierarchy', 'persons', 'crimes'],
    'count_crimes': ['crimes'],
    'count_persons': ['persons'],
}

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_table_columns(table_name: str) -> List[str]:
    """Get all column names for a table"""
    if table_name in POSTGRESQL_SCHEMA:
        return [col.name for col in POSTGRESQL_SCHEMA[table_name].columns]
    return []

def get_searchable_columns(table_name: str) -> List[str]:
    """Get columns suitable for searching (text, varchar)"""
    if table_name not in POSTGRESQL_SCHEMA:
        return []
    
    searchable_types = ['character varying', 'text']
    return [
        col.name for col in POSTGRESQL_SCHEMA[table_name].columns
        if col.data_type in searchable_types
    ]

def get_columns_for_entity_type(entity_type: str) -> Dict[str, List[str]]:
    """Get which columns to search for a specific entity type"""
    if entity_type in COLUMN_MAPPINGS:
        return COLUMN_MAPPINGS[entity_type]
    return {}

def get_tables_for_query_type(query_type: str) -> List[str]:
    """Get recommended tables for a query type"""
    return QUERY_TABLE_HINTS.get(query_type, [])

def format_schema_for_llm(tables: List[str] = None) -> str:
    """Format schema in a way LLM can understand"""
    if tables is None:
        tables = list(POSTGRESQL_SCHEMA.keys())
    
    schema_parts = []
    for table_name in tables:
        if table_name not in POSTGRESQL_SCHEMA:
            continue
        
        table = POSTGRESQL_SCHEMA[table_name]
        schema_parts.append(f"\nTable: {table_name}")
        if table.description:
            schema_parts.append(f"Description: {table.description}")
        
        # Show important columns (limit to top 10-15)
        important_cols = [col for col in table.columns if not col.name.endswith('_embedding')][:15]
        for col in important_cols:
            nullable = "" if col.is_nullable else " (required)"
            schema_parts.append(f"  - {col.name}: {col.data_type}{nullable}")
    
    return "\n".join(schema_parts)

# ============================================================================
# EXAMPLE QUERIES FOR REFERENCE
# ============================================================================

# ============================================================================
# MONGODB SCHEMA (V1 Data - from your actual document)
# ============================================================================

# NOTE: All MongoDB fields are UPPERCASE!
MONGODB_SCHEMA = {
    'fir_records': {
        'collection_name': 'fir_records',
        'description': 'V1 Data - Legacy FIR records (ALL UPPERCASE FIELDS!)',
        'important_fields': {
            # IDs and References
            '_id': 'MongoDB ObjectID',
            'FIR_REG_NUM': 'FIR Registration Number (e.g., 2029019150001)',
            'FIR_NO': 'FIR Number (e.g., 1/2015)',
            'YEAR': 'Year of FIR',
            
            # Person Details
            'ACCUSED_NAME': 'Accused person name',
            'FATHER_NAME': 'Father name',
            'AGE': 'Age (integer)',
            'GENDER': 'Gender (Male/Female)',
            'NATIONALITY': 'Nationality',
            'CASTE': 'Caste',
            'ACCUSED_OCCUPATION': 'Occupation',
            
            # Contact Information (ACTUAL!)
            'MOBILE_1': 'Mobile number field',
            'TELEPHONE_RESIDENCE': 'Landline',
            
            # Location
            'DISTRICT': 'District name',
            'PS': 'Police Station name',
            
            # Crime Details
            'MAJOR_HEAD': 'Major crime category (e.g., Narcotics)',
            'MINOR_HEAD': 'Minor crime category',
            'ACT_SEC': 'Acts and sections',
            'FIR_STATUS': 'FIR status (e.g., Abated, Pending)',
            
            # Dates
            'REG_DT': 'Registration date (MongoDB date)',
            'PS_RECV_INFORM_DT': 'PS received information date',
            'FROM_DT': 'From date',
            'TO_DT': 'To date',
            
            # Physical Features
            'HEIGHT_FROM_CM': 'Height in cm',
            'HEIGHT_LL_FEET': 'Height in feet',
            'BUILD_TYPE': 'Build type',
            'COMPLEXION_TYPE': 'Complexion',
            'FACE_TYPE': 'Face type',
            'HAIR_STYLE': 'Hair style',
            'HAIR_COLOR': 'Hair color',
            'EYE_TYPE': 'Eye type',
            'EYE_COLOR': 'Eye color',
            'NOSE_TYPE': 'Nose type',
            'BEARD_TYPE': 'Beard type',
            'LIPS_TYPE': 'Lips type',
            'TEETH_TYPE': 'Teeth type',
            'EARS_TYPE_CD': 'Ears type',
            
            # Relationship Data (Many fields!)
            'INT_FATHER_NAME': 'Father name',
            'INT_FATHER_MOBILE_NO': 'Father mobile',
            'INT_FATHER_ADDRESS': 'Father address',
            'INT_MOTHER_NAME': 'Mother name',
            'INT_MOTHER_MOBILE_NO': 'Mother mobile',
            'INT_WIFE_NAME': 'Wife name',
            'INT_WIFE_MOBILE_NO': 'Wife mobile',
            'INT_BROTHER_NAME': 'Brother name',
            'INT_SISTER_NAME': 'Sister name',
            'INT_SON_NAME': 'Son name',
            'INT_DAUGHTER_NAME': 'Daughter name',
            'INT_FRIEND_NAME': 'Friend name',
            'INT_UNCLE_NAME': 'Uncle name',
            'INT_AUNT_NAME': 'Aunt name',
            # ... and many more relationship fields
            
            # Drug Related (if applicable)
            'DRUG_TYPE': 'Drug type',
            'DRUG_DESC': 'Drug description',
            'DRUG_PARTICULARS': 'Drug details',
            'DRUG_STATUS': 'Drug status',
            'WEIGHT_KG': 'Weight in kg',
            'WEIGHT_GM': 'Weight in grams',
            'PACKETS_COUNT': 'Number of packets',
            'ESTIMATED_VALUE': 'Estimated value',
            
            # Metadata
            '_metadata': 'Import metadata (timestamp, source, etc.)',
        },
        'sample_queries': {
            'count': '{"collection": "fir_records", "query": {}}',
            'by_district': '{"collection": "fir_records", "query": {"DISTRICT": "Sangareddy"}}',
            'by_name': '{"collection": "fir_records", "query": {"ACCUSED_NAME": {"$regex": "Rathod", "$options": "i"}}}',
            'by_mobile': '{"collection": "fir_records", "query": {"MOBILE_1": "9876543210"}}',
            'by_year': '{"collection": "fir_records", "query": {"YEAR": "2015"}}',
            'recent': '{"collection": "fir_records", "pipeline": [{"$sort": {"REG_DT": -1}}, {"$limit": 10}]}',
        }
    }
}

# ============================================================================
# MONGODB COLUMN MAPPINGS (ACTUAL FIELD NAMES - ALL UPPERCASE!)
# ============================================================================

MONGODB_FIELD_MAPPINGS = {
    'mobile_number': ['MOBILE_1', 'INT_FATHER_MOBILE_NO', 'INT_MOTHER_MOBILE_NO', 'INT_WIFE_MOBILE_NO'],
    'email': ['EMAIL'],  # If exists
    'name': ['ACCUSED_NAME', 'INT_FATHER_NAME', 'INT_MOTHER_NAME', 'INT_WIFE_NAME'],
    'fir_number': ['FIR_NO', 'FIR_REG_NUM'],
    'district': ['DISTRICT'],
    'police_station': ['PS'],
    'status': ['FIR_STATUS'],
    'crime_type': ['MAJOR_HEAD', 'MINOR_HEAD'],
    'age': ['AGE'],
    'gender': ['GENDER'],
    'occupation': ['ACCUSED_OCCUPATION'],
    'caste': ['CASTE'],
    'nationality': ['NATIONALITY'],
    'date': ['REG_DT', 'PS_RECV_INFORM_DT', 'FROM_DT', 'TO_DT'],
}

# ============================================================================
# EXAMPLE QUERIES
# ============================================================================

EXAMPLE_QUERIES = {
    # PostgreSQL
    'count_crimes': "SELECT COUNT(*) FROM crimes",
    'recent_crimes': "SELECT crime_id, fir_num, crime_type, fir_date FROM crimes ORDER BY fir_date DESC LIMIT 10",
    'find_person_by_phone': "SELECT person_id, full_name, phone_number, email_id FROM persons WHERE phone_number = '9876543210'",
    'find_person_by_name': "SELECT person_id, full_name, phone_number, present_district FROM persons WHERE full_name ILIKE '%John%'",
    'crimes_by_status': "SELECT case_status, COUNT(*) FROM crimes GROUP BY case_status",
    'drugs_by_type': "SELECT drug_category, COUNT(*), SUM(seizure_worth) FROM brief_facts_drugs GROUP BY drug_category",
    
    # MongoDB (ACTUAL field names - UPPERCASE!)
    'mongo_count': '{"collection": "fir_records", "query": {}}',
    'mongo_by_district': '{"collection": "fir_records", "query": {"DISTRICT": "Sangareddy"}}',
    'mongo_by_name': '{"collection": "fir_records", "query": {"ACCUSED_NAME": {"$regex": "Rathod", "$options": "i"}}}',
    'mongo_by_mobile': '{"collection": "fir_records", "query": {"MOBILE_1": "9876543210"}}',
    'mongo_recent': '{"collection": "fir_records", "pipeline": [{"$sort": {"REG_DT": -1}}, {"$limit": 10}]}',
}


