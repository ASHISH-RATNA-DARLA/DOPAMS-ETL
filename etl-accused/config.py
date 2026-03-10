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
    'host': os.getenv('POSTGRES_HOST'),
    'database': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'port': int(os.getenv('POSTGRES_PORT'))
}

# API Configuration
API_CONFIG = {
    'base_url': os.getenv('DOPAMAS_API_URL'),
    'api_key': os.getenv('DOPAMAS_API_KEY'),
    'timeout': int(os.getenv('API_TIMEOUT')),
    'max_retries': int(os.getenv('API_MAX_RETRIES')),
    
    # API Endpoints
    'crimes_url': f"{os.getenv('DOPAMAS_API_URL')}/crimes",
    'accused_url': f"{os.getenv('DOPAMAS_API_URL')}/accused",
    'persons_url': f"{os.getenv('DOPAMAS_API_URL')}/person-details",
    'hierarchy_url': f"{os.getenv('DOPAMAS_API_URL')}/master-data/hierarchy",
    'ir_url': f"{os.getenv('DOPAMAS_API_URL')}/interrogation-reports/v1/"
}

# ETL Configuration
# Date range can be configured via environment variables:
#   - ACCUSED_START_DATE: Override start date (format: YYYY-MM-DDTHH:MM:SS+HH:MM or YYYY-MM-DD)
#   - ACCUSED_END_DATE: Override end date (format: YYYY-MM-DDTHH:MM:SS+HH:MM or YYYY-MM-DD)
#   - If not set, ETL behavior depends on run mode:
#     * RUN_MODE=1 (Incremental): Uses max(date_created, date_modified) from existing data, defaults to DEFAULT_START_DATE
#     * RUN_MODE=0 (Full Reset): Uses DEFAULT_START_DATE to yesterday
# Chunks are 5 days with 1-day overlap to ensure no data loss

# Default date for full reset or initial runs (can be overridden with ACCUSED_START_DATE)
DEFAULT_START_DATE = os.getenv('ACCUSED_START_DATE', '2022-01-01T00:00:00+05:30')

ETL_CONFIG = {
    # NOTE: start_date and end_date can be overridden via environment variables:
    #   - ACCUSED_START_DATE: Override the start date (useful for testing or partial re-runs)
    #   - ACCUSED_END_DATE: Override the end date
    'start_date': DEFAULT_START_DATE,           # Can be overridden via ACCUSED_START_DATE env var
    'end_date': os.getenv('ACCUSED_END_DATE', '2025-12-31T23:59:59+05:30'),    # Can be overridden via ACCUSED_END_DATE env var
    
    'chunk_days': 5,  # Fetch 5 days at a time
    'chunk_overlap_days': int(os.getenv('CHUNK_OVERLAP_DAYS')),  # Overlap between chunks to ensure no data is missed
    'batch_size': 250,  # Insert batch size (balanced for memory and connection efficiency)
    'enable_embeddings': os.getenv('ENABLE_EMBEDDINGS') == 'true'
}

# Embedding Configuration
EMBEDDING_CONFIG = {
    'model_name': os.getenv('EMBEDDING_MODEL'),
    'brief_facts_model': 'all-mpnet-base-v2',  # Better for long text
    'pattern_model': 'all-MiniLM-L6-v2',  # Faster for shorter patterns
    'batch_size': 32
}

# Logging Configuration
LOG_CONFIG = {
    'level': os.getenv('LOG_LEVEL'),
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


