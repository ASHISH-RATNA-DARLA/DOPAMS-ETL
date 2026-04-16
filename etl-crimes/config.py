"""Configuration file for DOPAMAS ETL Pipeline"""

from datetime import datetime, timedelta, timezone
from env_utils import (
    get_bool_env,
    get_int_env,
    load_repo_environment,
    resolve_api_base_url,
    resolve_db_config,
    resolve_table_name,
)

load_repo_environment()

# Calculate yesterday's end time in IST (UTC+05:30)
IST_OFFSET = timezone(timedelta(hours=5, minutes=30))
now_ist = datetime.now(IST_OFFSET)
yesterday_end = (now_ist - timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=0)

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

# Determine end_date based on backfill completion status
# After backfill completes, use dynamic yesterday's end
# During backfill, use fixed date to prevent gaps if pipeline fails mid-backfill
# Check if backfill is complete by looking for master_etl_state checkpoint
def get_etl_end_date():
    """
    Returns end_date for ETL:
    - During backfill: Fixed to 2026-04-16 23:59:59 (prevents gaps if pipeline fails)
    - After backfill: Dynamic to yesterday's end (24hr rolling window)

    Master checkpoint 'master_etl_backfill_complete' must exist in etl_run_state
    and only updates after ALL 28 ETL steps complete successfully.
    """
    try:
        # Try to read master backfill completion state
        from db_pooling import PostgreSQLConnectionPool
        db_pool = PostgreSQLConnectionPool(DB_CONFIG)
        with db_pool.get_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT last_successful_end FROM etl_run_state WHERE module_name = %s",
                    ('master_etl_backfill_complete',)
                )
                result = cur.fetchone()
                if result:
                    # Backfill completed - use dynamic yesterday's end for daily runs
                    return yesterday_end.isoformat()
    except Exception:
        pass

    # Backfill not yet complete - use fixed date (2026-04-16) to prevent gaps
    return '2026-04-16T23:59:59+05:30'

ETL_CONFIG = {
    'start_date': '2022-06-01T00:00:00+05:30',
    'end_date': get_etl_end_date(),  # Fixed during backfill, dynamic after
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
