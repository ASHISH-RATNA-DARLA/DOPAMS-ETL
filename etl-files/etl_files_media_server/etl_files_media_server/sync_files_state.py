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
from concurrent.futures import ThreadPoolExecutor, as_completed

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
WORKER_COUNT = int(os.getenv("SYNC_WORKER_COUNT", "32"))

_DIR_CACHE = {}

def is_file_missing(file_id, source_type, source_field):
    """Checks if a file exists on disk efficiently using a directory cache."""
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
        return True # Missing
        
    base_dir = os.path.join(BASE_MEDIA_PATH, sub_dir)
    if not os.path.exists(base_dir):
        return True # Missing
        
    # Lazy load directory cache to prevent O(N^2) os.listdir() calls
    if base_dir not in _DIR_CACHE:
        try:
            _DIR_CACHE[base_dir] = set(os.listdir(base_dir))
        except Exception:
            _DIR_CACHE[base_dir] = set()
            
    # Check if any file starts with {file_id}.
    prefix = f"{file_id}."
    for f in _DIR_CACHE[base_dir]:
        if f.startswith(prefix):
            return False # Found
            
    return True # Missing

def sync():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        logger.info(f"🔍 Fetching records marked as downloaded from {FILES_TABLE}...")
        cur.execute(f"SELECT file_id, source_type, source_field FROM {FILES_TABLE} WHERE is_downloaded IS TRUE")
        rows = cur.fetchall()
        
        total = len(rows)
        if total == 0:
            logger.info("✅ No downloaded files to verify.")
            return

        logger.info(f"📊 Found {total} records to verify on disk. Scanning with {WORKER_COUNT} workers...")
        
        missing_ids = []
        processed = 0

        with ThreadPoolExecutor(max_workers=WORKER_COUNT) as executor:
            # Map futures to file_id
            futures = {
                executor.submit(is_file_missing, row[0], row[1], row[2]): row[0]
                for row in rows
            }
            
            for future in as_completed(futures):
                file_id = futures[future]
                processed += 1
                
                if processed % 1000 == 0:
                    logger.info(f"   ... verified {processed}/{total} files ...")
                
                try:
                    is_missing = future.result()
                    if is_missing:
                        missing_ids.append(file_id)
                except Exception as e:
                    logger.error(f"Error checking file_id {file_id}: {e}")
        
        missing_count = len(missing_ids)
        
        if missing_count > 0:
            logger.warning(f"❌ Found {missing_count} missing files on disk. Updating database in batch...")
            
            # Batch update the database for maximum performance
            cur.execute(f"""
                UPDATE {FILES_TABLE} 
                SET is_downloaded = FALSE, 
                    download_error = 'Sync: File missing on disk during audit'
                WHERE file_id = ANY(%s)
            """, (missing_ids,))
            
            conn.commit()
            logger.info(f"📝 Database updated successfully for {missing_count} missing files.")
        else:
            logger.info("✅ All files are present on disk.")
            
        logger.info("=" * 50)
        logger.info(f"✅ Audit Complete")
        logger.info(f"   - Total checked: {total}")
        logger.info(f"   - Missing on disk: {missing_count}")
        logger.info("=" * 50)
        
    except Exception as e:
        logger.error(f"💥 Error during sync: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    sync()
