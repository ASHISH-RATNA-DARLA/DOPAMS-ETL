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
    'port': int(os.getenv('POSTGRES_PORT')),
    # Connection timeout and keepalive settings for connection stability
    # Keepalive helps maintain connections during long API calls and network issues
    'connect_timeout': int(os.getenv('DB_CONNECT_TIMEOUT')),  # Connection timeout in seconds
    'keepalives': int(os.getenv('DB_KEEPALIVES')),  # Enable TCP keepalive (1 = enabled)
    'keepalives_idle': int(os.getenv('DB_KEEPALIVES_IDLE')),  # Seconds of inactivity before sending keepalive
    'keepalives_interval': int(os.getenv('DB_KEEPALIVES_INTERVAL')),  # Seconds between keepalive packets
    'keepalives_count': int(os.getenv('DB_KEEPALIVES_COUNT')),  # Number of keepalive packets before considering connection dead
}

# API Configuration
# Helper function to get API endpoint URL
def get_api_endpoint(endpoint_key: str, default_path: str = '') -> str:
    """
    Get API endpoint URL from environment variables.
    Supports:
    - DOPAMAS_API_URL (default base URL)
    - DOPAMAS_API_URL2 (alternative base URL, e.g., for different port)
    - {ENDPOINT_KEY}_API_ENDPOINT (endpoint path, e.g., DISPOSAL_API_ENDPOINT=/crimes/disposal)
    - {ENDPOINT_KEY}_API_BASE_URL (optional base URL override for this endpoint)
    
    Args:
        endpoint_key: Key name (e.g., 'disposal', 'crimes', 'arrests')
        default_path: Default path if not found in env (for backward compatibility)
    
    Returns:
        Full API URL
    """
    # Check for endpoint-specific base URL override (e.g., DISPOSAL_API_BASE_URL)
    base_url_override = os.getenv(f'{endpoint_key.upper()}_API_BASE_URL', '').strip()
    
    # Use override if provided, otherwise check for DOPAMAS_API_URL2, then default to DOPAMAS_API_URL
    if base_url_override:
        base_url = base_url_override
    else:
        # Check for alternative base URL (e.g., DOPAMAS_API_URL2 for port 3001)
        api_url2 = os.getenv('DOPAMAS_API_URL2', '').strip()
        base_url = api_url2 if api_url2 else os.getenv('DOPAMAS_API_URL')
    
    # Get endpoint path from env (e.g., DISPOSAL_API_ENDPOINT=/crimes/disposal)
    endpoint_path = os.getenv(f'{endpoint_key.upper()}_API_ENDPOINT', '').strip()
    
    # If no endpoint path in env, use default_path (for backward compatibility)
    if not endpoint_path:
        endpoint_path = default_path
    
    # Ensure endpoint_path starts with /
    if endpoint_path and not endpoint_path.startswith('/'):
        endpoint_path = '/' + endpoint_path
    
    # Combine base URL and endpoint path
    return f"{base_url.rstrip('/')}{endpoint_path}"

API_CONFIG = {
    'base_url': os.getenv('DOPAMAS_API_URL'),
    'api_key': os.getenv('DOPAMAS_API_KEY'),
    'timeout': int(os.getenv('API_TIMEOUT')),
    'max_retries': int(os.getenv('API_MAX_RETRIES')),
    
    # API Endpoints - using helper function for flexibility
    # Backward compatibility: if env vars not set, use defaults
    'crimes_url': get_api_endpoint('crimes', '/crimes'),
    'accused_url': get_api_endpoint('accused', '/accused'),
    'persons_url': get_api_endpoint('persons', '/person-details'),
    'hierarchy_url': get_api_endpoint('hierarchy', '/master-data/hierarchy'),
    'ir_url': get_api_endpoint('ir', '/interrogation-reports/v1/'),
    'files_url': get_api_endpoint('files', '/files'),
    # New endpoints
    'disposal_url': get_api_endpoint('disposal', '/crimes/disposal'),
    'arrests_url': get_api_endpoint('arrests', '/arrests'),
    'seizures_url': get_api_endpoint('seizures', '/mo-seizures'),
    'chargesheets_url': get_api_endpoint('chargesheets', '/chargesheets'),
    'update_chargesheets_url': get_api_endpoint('update_chargesheets', '/update-chargesheets'),
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
    'chunk_overlap_days': int(os.getenv('CHUNK_OVERLAP_DAYS')),  # Overlap between chunks to ensure no data is missed
    'batch_size': 100,  # Insert batch size
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
    'disposal': _table_name('DISPOSAL_TABLE', 'disposal'),
    'arrests': _table_name('ARRESTS_TABLE', 'arrests'),
    'mo_seizures': _table_name('MO_SEIZURES_TABLE', 'mo_seizures'),
    'chargesheets': _table_name('CHARGESHEETS_TABLE', 'chargesheets'),
    'chargesheet_files': _table_name('CHARGESHEET_FILES_TABLE', 'chargesheet_files'),
    'chargesheet_acts': _table_name('CHARGESHEET_ACTS_TABLE', 'chargesheet_acts'),
    'chargesheet_accused': _table_name('CHARGESHEET_ACCUSED_TABLE', 'chargesheet_accused'),
    'update_chargesheet': _table_name('UPDATE_CHARGESHEET_TABLE', 'charge_sheet_updates'),
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


