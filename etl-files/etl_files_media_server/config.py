"""
Configuration file for DOPAMAS ETL Pipeline
"""
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database Configuration
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'database': os.getenv('POSTGRES_DB', 'dopamasuprddb'),
    'user': os.getenv('POSTGRES_USER', 'dopamasprd_ur'),
    'password': os.getenv('POSTGRES_PASSWORD', ''),
    'port': int(os.getenv('POSTGRES_PORT', 5432))
}

# API Configuration
# API1 (original - port 3000): crimes, persons, property, interrogation
API1_BASE_URL = os.getenv('DOPAMAS_API_URL', 'http://103.164.200.184:3000/api/DOPAMS')

# API2 (new - port 3001): mo_seizures, chargesheets, fsl_case_property
API2_HOST = os.getenv('API2_URL', '103.164.200.184')
API2_PORT = os.getenv('API2_PORT', '3001')
API2_BASE_URL = f"http://{API2_HOST}:{API2_PORT}/api/DOPAMS"

API_CONFIG = {
    'base_url': API1_BASE_URL,  # Default to API1 for backward compatibility
    'api1_base_url': API1_BASE_URL,
    'api2_base_url': API2_BASE_URL,
    'api_key': os.getenv('DOPAMAS_API_KEY', 'c4127def-da76-4d8d-ad3d-159cea0206a0'),
    'timeout': int(os.getenv('API_TIMEOUT', 30)),
    'max_retries': int(os.getenv('API_MAX_RETRIES', 3)),
    
    # API1 Endpoints (port 3000)
    'crimes_url': f"{API1_BASE_URL}/crimes",
    'accused_url': f"{API1_BASE_URL}/accused",
    'persons_url': f"{API1_BASE_URL}/person-details",
    'hierarchy_url': f"{API1_BASE_URL}/master-data/hierarchy",
    'ir_url': f"{API1_BASE_URL}/interrogation-reports/v1/",
    'files_url': f"{API1_BASE_URL}/files",
    
    # API2 Endpoints (port 3001)
    'mo_seizures_url': f"{API2_BASE_URL}/mo-seizures",
    'chargesheets_url': f"{API2_BASE_URL}/chargesheets",
    'fsl_case_property_url': f"{API2_BASE_URL}/case-property"
}

# ETL Configuration
# Date range is now calculated dynamically:
#   - Start date: Always 2022-01-01T00:00:00+05:30 (hardcoded in ETL scripts)
#   - End date: Yesterday at 23:59:59+05:30 IST (calculated dynamically each run)
#   - For existing databases: ETL checks max(date_created, date_modified) and resumes from there
#   - For new databases: ETL starts from 2022-01-01
# Chunks are 5 days with 1-day overlap to ensure no data loss

ETL_CONFIG = {
    # NOTE: start_date and end_date are kept for backward compatibility and log headers only
    # Actual date range is calculated dynamically in each ETL script:
    #   - Fixed start: 2022-01-01T00:00:00+05:30 (hardcoded in ETL scripts)
    #   - Dynamic end: Yesterday at 23:59:59+05:30 IST (calculated each run)
    #   - For existing databases: ETL checks max(date_created, date_modified) and resumes from there
    'start_date': '2022-01-01T00:00:00+05:30',  # Reference date for log headers (1st January 2022, 00:00:00 IST)
    'end_date': '2025-12-31T23:59:59+05:30',    # Placeholder for log headers (not used in actual processing)
    
    'chunk_days': 5,  # Fetch 5 days at a time
    'chunk_overlap_days': int(os.getenv('CHUNK_OVERLAP_DAYS', '1')),  # Overlap between chunks to ensure no data is missed (default: 1 day)
    'batch_size': 100,  # Insert batch size
    'enable_embeddings': os.getenv('ENABLE_EMBEDDINGS', 'false').lower() == 'true'
}

# Embedding Configuration
EMBEDDING_CONFIG = {
    'model_name': os.getenv('EMBEDDING_MODEL', 'all-MiniLM-L6-v2'),
    'brief_facts_model': 'all-mpnet-base-v2',  # Better for long text
    'pattern_model': 'all-MiniLM-L6-v2',  # Faster for shorter patterns
    'batch_size': 32
}

# Logging Configuration
LOG_CONFIG = {
    'level': os.getenv('LOG_LEVEL', 'INFO'),
    'format': '%(log_color)s%(asctime)s - %(levelname)s - %(message)s',
    'date_format': '%Y-%m-%d %H:%M:%S'
}

# Table configuration (allows redirecting ETL runs to test tables)
def _table_name(env_key: str, default: str) -> str:
    """Return override table name; fall back to default if env unset or empty."""
    value = os.getenv(env_key, '').strip()
    return value or default

TABLE_CONFIG = {
    'crimes': _table_name('CRIMES_TABLE', 'crimes'),
    'accused': _table_name('ACCUSED_TABLE', 'accused'),
    'persons': _table_name('PERSONS_TABLE', 'persons'),
    'hierarchy': _table_name('HIERARCHY_TABLE', 'hierarchy'),
    'properties': _table_name('PROPERTIES_TABLE', 'properties'),
    # Interrogation Reports (IR) tables
    'interrogation_reports': _table_name('IR_TABLE', 'interrogation_reports'),
    'ir_family_history': _table_name('IR_FAMILY_HISTORY_TABLE', 'ir_family_history'),
    'ir_local_contacts': _table_name('IR_LOCAL_CONTACTS_TABLE', 'ir_local_contacts'),
    'ir_regular_habits': _table_name('IR_REGULAR_HABITS_TABLE', 'ir_regular_habits'),
    'ir_types_of_drugs': _table_name('IR_TYPES_OF_DRUGS_TABLE', 'ir_types_of_drugs'),
    'ir_sim_details': _table_name('IR_SIM_DETAILS_TABLE', 'ir_sim_details'),
    'ir_financial_history': _table_name('IR_FINANCIAL_HISTORY_TABLE', 'ir_financial_history'),
    'ir_consumer_details': _table_name('IR_CONSUMER_DETAILS_TABLE', 'ir_consumer_details'),
    'ir_modus_operandi': _table_name('IR_MODUS_OPERANDI_TABLE', 'ir_modus_operandi'),
    'ir_previous_offences_confessed': _table_name('IR_PREVIOUS_OFFENCES_TABLE', 'ir_previous_offences_confessed'),
    'ir_defence_counsel': _table_name('IR_DEFENCE_COUNSEL_TABLE', 'ir_defence_counsel'),
    'ir_associate_details': _table_name('IR_ASSOCIATE_DETAILS_TABLE', 'ir_associate_details'),
    'ir_shelter': _table_name('IR_SHELTER_TABLE', 'ir_shelter'),
    'ir_media': _table_name('IR_MEDIA_TABLE', 'ir_media'),
    'ir_interrogation_report_refs': _table_name('IR_INTERROGATION_REPORT_REFS_TABLE', 'ir_interrogation_report_refs'),
    'ir_dopams_links': _table_name('IR_DOPAMS_LINKS_TABLE', 'ir_dopams_links'),
}
