#!/usr/bin/env python3
"""
DOPAMAS ETL - Files State Synchronizer

This script audits the filesystem and synchronizes the 'files' table.
It identifies records marked as is_downloaded=TRUE where the file is 
actually missing from the disk and updates the database accordingly.
"""

import os
import sys
import logging
import psycopg2
from pathlib import Path

# Add parent directory to path for config imports
CURRENT_DIR = Path(__file__).resolve().parent
PARENT_DIR = CURRENT_DIR.parent
if str(PARENT_DIR) not in sys.path:
    sys.path.insert(0, str(PARENT_DIR))

from config import DB_CONFIG

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("sync-files-state")

# Constants
FILES_TABLE = os.getenv("FILES_TABLE", "files")
BASE_MEDIA_PATH = os.getenv("FILES_MEDIA_BASE_PATH", "/mnt/shared-etl-files")

def get_expected_path(file_id, source_type, source_field):
    """Matches the build_destination_path logic in main.py"""
    mapping = {
        ("crime", "FIR_COPY"): "crimes",
        ("person", "IDENTITY_DETAILS"): "person/identitydetails",
        ("person", "MEDIA"): "person/media",
        ("property", "MEDIA"): "property",
        ("interrogation", "MEDIA"): "interrogations/media",
        ("interrogation", "INTERROGATION_REPORT"): "interrogations/interrogationreport",
        ("interrogation", "DOPAMS_DATA"): "interrogations/dopamsdata",
    }
    
    sub_dir = mapping.get((source_type, source_field))
    if not sub_dir:
        return None
        
    # We don't know the extension without the API, so we check for common ones
    # or just check if any file with {file_id}.* exists in that directory
    base_dir = os.path.join(BASE_MEDIA_PATH, sub_dir)
    if not os.path.exists(base_dir):
        return None
        
    for f in os.listdir(base_dir):
        if f.startswith(f"{file_id}."):
            return os.path.join(base_dir, f)
            
    return None

def sync():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        logger.info(f"🔍 Fetching records marked as downloaded from {FILES_TABLE}...")
        cur.execute(f"SELECT file_id, source_type, source_field FROM {FILES_TABLE} WHERE is_downloaded IS TRUE")
        rows = cur.fetchall()
        
        total = len(rows)
        missing = 0
        fixed = 0
        
        logger.info(f"📊 Found {total} records to verify on disk.")
        
        for file_id, source_type, source_field in rows:
            path = get_expected_path(file_id, source_type, source_field)
            
            if not path or not os.path.exists(path):
                missing += 1
                logger.warning(f"❌ Missing file on disk: {file_id} ({source_type}/{source_field})")
                
                # Update DB to reflect reality
                cur.execute(f"""
                    UPDATE {FILES_TABLE} 
                    SET is_downloaded = FALSE, 
                        download_error = 'Sync: File missing on disk during audit'
                    WHERE file_id = %s
                """, (file_id,))
                fixed += 1
                
                if fixed % 100 == 0:
                    conn.commit()
                    logger.info(f"📝 Committed {fixed} fixes...")
        
        conn.commit()
        logger.info("=" * 50)
        logger.info(f"✅ Audit Complete")
        logger.info(f"   - Total checked: {total}")
        logger.info(f"   - Missing on disk: {missing}")
        logger.info(f"   - Database updated: {fixed}")
        logger.info("=" * 50)
        
    except Exception as e:
        logger.error(f"💥 Error during sync: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    sync()
