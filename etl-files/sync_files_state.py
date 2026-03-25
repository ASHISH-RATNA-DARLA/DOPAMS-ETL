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
load_dotenv(".env.server")

DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST'),
    'port': os.getenv('POSTGRES_PORT', 5432),
    'database': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD')
}

BASE_MEDIA_PATH = os.getenv("FILES_MEDIA_BASE_PATH", "/mnt/shared-etl-files")
FILES_TABLE = os.getenv("FILES_TABLE", "files")

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
    print(f"Connecting to database {DB_CONFIG['database']} at {DB_CONFIG['host']}...")
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
