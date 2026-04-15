"""Configuration file for DOPAMAS ETL Pipeline"""

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

API_CONFIG = {
    'base_url': resolve_api_base_url('DOPAMAS_API_URL'),
    'api_key': resolve_api_base_url('DOPAMAS_API_KEY'),
    'timeout': get_int_env('API_TIMEOUT', 180),
    'max_retries': get_int_env('API_MAX_RETRIES', 5),
    'crimes_url': f"{resolve_api_base_url('DOPAMAS_API_URL')}/crimes",
    'accused_url': f"{resolve_api_base_url('DOPAMAS_API_URL')}/accused",
    'persons_url': f"{resolve_api_base_url('DOPAMAS_API_URL')}/person-details",
    'hierarchy_url': f"{resolve_api_base_url('DOPAMAS_API_URL')}/master-data/hierarchy",
    'ir_url': f"{resolve_api_base_url('DOPAMAS_API_URL')}/interrogation-reports/v1/",
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
