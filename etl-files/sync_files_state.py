#!/usr/bin/env python3
"""
Sync Script: Database vs Filesystem
Checks for files marked as downloaded in DB but missing on disk.
Resets is_downloaded = FALSE for missing files to allow re-download.
"""

import os
import psycopg2
from pathlib import Path
from dotenv import load_dotenv

# Load configuration
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent


def load_environment():
    """
    Load environment variables from the most likely repo-level env files.

    Search order:
    1. Explicit file from DOPAMS_ENV_FILE
    2. Repo root .env.server
    3. Repo root .env
    4. Script directory .env.server
    5. Script directory .env
    """
    env_override = os.getenv("DOPAMS_ENV_FILE")
    candidates = []

    if env_override:
        candidates.append(Path(env_override).expanduser())

    candidates.extend(
        [
            REPO_ROOT / ".env.server",
            REPO_ROOT / ".env",
            SCRIPT_DIR / ".env.server",
            SCRIPT_DIR / ".env",
        ]
    )

    checked_paths = []
    seen_paths = set()

    for candidate in candidates:
        candidate = candidate.resolve()
        candidate_str = str(candidate)
        if candidate_str in seen_paths:
            continue

        seen_paths.add(candidate_str)
        checked_paths.append(candidate_str)

        if candidate.is_file():
            load_dotenv(candidate_str, override=False)
            return candidate_str, checked_paths

    return None, checked_paths


LOADED_ENV_FILE, CHECKED_ENV_PATHS = load_environment()


def get_postgres_port():
    port_value = os.getenv("POSTGRES_PORT", "5432")
    try:
        return int(port_value)
    except (TypeError, ValueError):
        return None


DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": get_postgres_port(),
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

BASE_MEDIA_PATH = os.getenv("FILES_MEDIA_BASE_PATH", "/mnt/shared-etl-files")
FILES_TABLE = os.getenv("FILES_TABLE", "files")


def validate_db_config():
    required = {
        "POSTGRES_HOST": DB_CONFIG["host"],
        "POSTGRES_DB": DB_CONFIG["database"],
        "POSTGRES_USER": DB_CONFIG["user"],
        "POSTGRES_PASSWORD": DB_CONFIG["password"],
        "POSTGRES_PORT": DB_CONFIG["port"],
    }

    missing = [key for key, value in required.items() if value in (None, "")]
    if not missing:
        return True

    print("Missing required database configuration.")
    print(f"Missing variables: {', '.join(missing)}")
    if LOADED_ENV_FILE:
        print(f"Loaded env file: {LOADED_ENV_FILE}")
    else:
        print("No env file was loaded.")
    print("Checked env paths:")
    for env_path in CHECKED_ENV_PATHS:
        print(f"  - {env_path}")
    print("You can also set DOPAMS_ENV_FILE=/absolute/path/to/.env.server before running.")
    return False

def map_destination_subdir(source_type, source_field):
    source_type = (source_type or "").lower()
    source_field = (source_field or "").upper()

    mapping = {
        ("crime", "FIR_COPY"): "crimes",
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

def run_sync():
    if not validate_db_config():
        return

    print(f"Connecting to database {DB_CONFIG['database']} at {DB_CONFIG['host']}...")
    if LOADED_ENV_FILE:
        print(f"Loaded environment from {LOADED_ENV_FILE}")

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return

    print(f"Base media path: {BASE_MEDIA_PATH}")
    
    # Fetch all files marked as downloaded
    sql = f"""
        SELECT id, source_type, source_field, file_id, file_url
        FROM {FILES_TABLE}
        WHERE is_downloaded = TRUE AND file_id IS NOT NULL
    """
    cursor.execute(sql)
    rows = cursor.fetchall()
    
    print(f"Found {len(rows)} records marked as is_downloaded = TRUE")
    
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
            # print(f"Missing: {file_path}")
            
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
                print(f"Fixed {fixed_count} records...")

    conn.commit()
    print(f"\nSync complete:")
    print(f"Total checked: {len(rows)}")
    print(f"Total missing: {missing_count}")
    print(f"Total DB records reset: {fixed_count}")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    run_sync()
