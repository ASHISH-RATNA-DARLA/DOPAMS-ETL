"""Configuration file for DOPAMAS ETL Pipeline."""

import sys
from pathlib import Path

# Ensure the repo root (where env_utils.py lives) is on sys.path so this
# module can be imported regardless of the working directory.
_REPO_ROOT = Path(__file__).resolve().parents[2]  # config.py -> etl_files_media_server/ -> etl-files/ -> repo root
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from env_utils import (
    get_bool_env,
    get_int_env,
    load_repo_environment,
    resolve_api_base_url,
    resolve_db_config,
    resolve_table_name,
)

load_repo_environment()

DB_CONFIG = resolve_db_config()

API1_BASE_URL = resolve_api_base_url('DOPAMAS_API_URL', 'API1_BASE_URL')
API2_BASE_URL = resolve_api_base_url('DOPAMAS_API_URL2', 'API2_BASE_URL')
if not API2_BASE_URL:
    api2_host = resolve_api_base_url('API2_URL')
    api2_port = resolve_api_base_url('API2_PORT')
    if api2_host and api2_port:
        API2_BASE_URL = f"http://{api2_host}:{api2_port}/api/DOPAMS"

API_CONFIG = {
    'base_url': API1_BASE_URL,
    'api1_base_url': API1_BASE_URL,
    'api2_base_url': API2_BASE_URL,
    'api_key': resolve_api_base_url('DOPAMAS_API_KEY'),
    'timeout': get_int_env('API_TIMEOUT', 180),
    'max_retries': get_int_env('API_MAX_RETRIES', 5),
    'crimes_url': f"{API1_BASE_URL}/crimes",
    'accused_url': f"{API1_BASE_URL}/accused",
    'persons_url': f"{API1_BASE_URL}/person-details",
    'hierarchy_url': f"{API1_BASE_URL}/master-data/hierarchy",
    'ir_url': f"{API1_BASE_URL}/interrogation-reports/v1/",
    'files_url': f"{API1_BASE_URL}/files",
    'mo_seizures_url': f"{API2_BASE_URL}/mo-seizures",
    'chargesheets_url': f"{API2_BASE_URL}/chargesheets",
    'fsl_case_property_url': f"{API2_BASE_URL}/case-property",
}

ETL_CONFIG = {
    'start_date': '2022-01-01T00:00:00+05:30',
    'end_date': '2025-12-31T23:59:59+05:30',
    'chunk_days': 5,
    'chunk_overlap_days': get_int_env('CHUNK_OVERLAP_DAYS', 1),
    'batch_size': 100,
    'enable_embeddings': get_bool_env('ENABLE_EMBEDDINGS', False),
}

EMBEDDING_CONFIG = {
    'model_name': resolve_api_base_url('EMBEDDING_MODEL'),
    'brief_facts_model': 'all-mpnet-base-v2',
    'pattern_model': 'all-MiniLM-L6-v2',
    'batch_size': 32,
}

LOG_CONFIG = {
    'level': resolve_api_base_url('LOG_LEVEL', default='INFO'),
    'format': '%(log_color)s%(asctime)s - %(levelname)s - %(message)s',
    'date_format': '%Y-%m-%d %H:%M:%S',
}


def _table_name(env_key: str, default: str) -> str:
    return resolve_table_name(env_key, default)


TABLE_CONFIG = {
    'crimes': _table_name('CRIMES_TABLE', 'crimes'),
    'accused': _table_name('ACCUSED_TABLE', 'accused'),
    'persons': _table_name('PERSONS_TABLE', 'persons'),
    'hierarchy': _table_name('HIERARCHY_TABLE', 'hierarchy'),
    'properties': _table_name('PROPERTIES_TABLE', 'properties'),
    'disposal': _table_name('DISPOSAL_TABLE', 'disposal'),
    'interrogation_reports': _table_name('IR_TABLE', 'interrogation_reports'),
    'ir_media': _table_name('IR_MEDIA_TABLE', 'ir_media'),
}