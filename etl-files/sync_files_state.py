#!/usr/bin/env python3
"""
Sync Script: Database vs Filesystem
Checks for files marked as downloaded in DB but missing on disk.
Resets is_downloaded = FALSE for missing files to allow re-download.
"""

import os
import logging
import psycopg2
from pathlib import Path
from env_utils import first_env, load_repo_environment, resolve_db_config, resolve_table_name

SCRIPT_DIR = Path(__file__).resolve().parent

LOADED_ENV_FILE = load_repo_environment(extra_candidates=[SCRIPT_DIR / ".env.server", SCRIPT_DIR / ".env"])
_DB_RESOLVED = resolve_db_config()
DB_CONFIG = {
    "host": _DB_RESOLVED["host"],
    "port": _DB_RESOLVED["port"],
    "database": _DB_RESOLVED["dbname"],
    "user": _DB_RESOLVED["user"],
    "password": _DB_RESOLVED["password"],
}

BASE_MEDIA_PATH = first_env("FILES_MEDIA_BASE_PATH", default="/mnt/shared-etl-files")
FILES_TABLE = resolve_table_name("FILES_TABLE", "files")

# Setup logger for sync script
_sync_logger = None

def get_sync_logger():
    """Get or create logger for sync operations."""
    global _sync_logger
    if _sync_logger is None:
        _sync_logger = logging.getLogger("etl-files-sync")
        if not _sync_logger.handlers:
            _sync_logger.setLevel(logging.INFO)
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            _sync_logger.addHandler(handler)
    return _sync_logger


def validate_db_config(logger=None):
    if logger is None:
        logger = get_sync_logger()
    
    required = {
        "DB_HOST": DB_CONFIG["host"],
        "DB_NAME": DB_CONFIG["database"],
        "DB_USER": DB_CONFIG["user"],
        "DB_PASSWORD": DB_CONFIG["password"],
        "DB_PORT": DB_CONFIG["port"],
    }

    missing = [key for key, value in required.items() if value in (None, "")]
    if not missing:
        return True

    logger.error("Missing required database configuration.")
    logger.error(f"Missing variables: {', '.join(missing)}")
    if LOADED_ENV_FILE:
        logger.error(f"Loaded env file: {LOADED_ENV_FILE}")
    else:
        logger.error("No repo env file was loaded. Falling back to ambient environment.")
    logger.error("You can also set DOPAMS_ENV_FILE=/absolute/path/to/.env.server before running.")
    return False

def map_destination_subdir(source_type, source_field):
    source_type = (source_type or "").lower()
    source_field = (source_field or "").upper()

    mapping = {
        ("crime", "FIR_COPY"): "crimes",
        ("crime", "MEDIA"): "crimes",
        ("person", "IDENTITY_DETAILS"): os.path.join("person", "identitydetails"),
        ("person", "MEDIA"): os.path.join("person", "media"),
        ("property", "MEDIA"): "property",
        ("interrogation", "MEDIA"): os.path.join("interrogations", "media"),
        ("interrogation", "INTERROGATION_REPORT"): os.path.join("interrogations", "interrogationreport"),
        ("interrogation", "DOPAMS_DATA"): os.path.join("interrogations", "dopamsdata"),
        ("mo_seizures", "MO_MEDIA"): "mo_seizures",
        ("chargesheets", "UPLOADCHARGESHEET"): "chargesheets",
        ("case_property", "MEDIA"): "fsl_case_property"
    }
    return mapping.get((source_type, source_field))

def run_sync(logger=None):
    """
    Sync database file status with filesystem.
    Checks for files marked as downloaded but missing on disk.
    Resets is_downloaded = FALSE for missing files to allow re-download.
    """
    if logger is None:
        logger = get_sync_logger()
    
    if not validate_db_config(logger):
        return False

    logger.info(f"Connecting to database {DB_CONFIG['dbname']} at {DB_CONFIG['host']}...")
    if LOADED_ENV_FILE:
        logger.info(f"Loaded environment from {LOADED_ENV_FILE}")

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        return False

    logger.info(f"Base media path: {BASE_MEDIA_PATH}")
    
    # Fetch all files marked as downloaded
    sql = f"""
        SELECT id, source_type, source_field, file_id, file_url
        FROM {FILES_TABLE}
        WHERE is_downloaded = TRUE AND file_id IS NOT NULL
    """
    cursor.execute(sql)
    rows = cursor.fetchall()
    
    logger.info(f"Found {len(rows)} records marked as is_downloaded = TRUE")
    
    missing_count = 0
    fixed_count = 0
    
    for row in rows:
        db_id, source_type, source_field, file_id, file_url = row
        
        subdir = map_destination_subdir(source_type, source_field)
        if not subdir:
            continue
            
        # Determine extension from file_url or fallback
        ext = ".pdf"
        if file_url and "." in file_url.split("/")[-1]:
            ext = "." + file_url.split("/")[-1].split(".")[-1]
            
        file_path = os.path.join(BASE_MEDIA_PATH, subdir, f"{file_id}{ext}")
        
        if not os.path.exists(file_path):
            missing_count += 1
            # logger.debug(f"Missing: {file_path}")
            
            # Reset status in DB
            update_sql = f"""
                UPDATE {FILES_TABLE}
                SET is_downloaded = FALSE,
                    download_error = 'Sync: File missing on disk at expected path'
                WHERE id = %s
            """
            cursor.execute(update_sql, (db_id,))
            fixed_count += 1
            
            if fixed_count % 100 == 0:
                conn.commit()
                logger.info(f"Fixed {fixed_count} records...")

    conn.commit()
    logger.info("")
    logger.info("=" * 80)
    logger.info("📊 FILES SYNC VALIDATION SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total records checked: {len(rows)}")
    logger.info(f"Missing from disk: {missing_count}")
    logger.info(f"DB records reset for re-download: {fixed_count}")
    logger.info("=" * 80)
    
    cursor.close()
    conn.close()
    
    return True

if __name__ == "__main__":
    run_sync()
