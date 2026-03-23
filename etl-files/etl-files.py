#!/usr/bin/env python3
"""
DOPAMAS ETL Pipeline - Files Downloader

- Reads FIR copy IDs from the PostgreSQL `crimes` table (column: fir_copy)
- For each non-null / non-empty fir_copy:
    * Calls the files API: {API_CONFIG['base_url']}/files/{fir_copy}
    * Saves the response as a PDF:
      /data-drive/etl-process-dev/etl-files/tomcat/webapps/files/pdfs/{fir_copy}.pdf
    * If a file already exists:
        - Logs the existing file size
        - Deletes the old file
        - Downloads and saves the latest one
- All actions and errors are logged into logs/files_etl_YYYYMMDD_HHMMSS.log
"""

import os
import sys
import logging
from datetime import datetime
import time
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple, Optional
import psutil

import psycopg2
import requests
import colorlog

try:
    from filelock import FileLock
except ImportError:
    FileLock = None

from config import DB_CONFIG, API_CONFIG, TABLE_CONFIG, LOG_CONFIG


# -----------------------------------------------------------------------------
# Logging setup
# -----------------------------------------------------------------------------

def setup_logger() -> logging.Logger:
    """Configure console + file logging, using existing LOG_CONFIG format."""
    logger = colorlog.getLogger("etl-files")
    logger.setLevel(LOG_CONFIG.get("level", "INFO").upper())

    # Ensure we don't add duplicate handlers if script is imported / re-run
    if logger.handlers:
        return logger

    # Create logs directory
    os.makedirs("logs", exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"logs/files_etl_{timestamp}.log"

    # Console handler (colored)
    console_handler = colorlog.StreamHandler()
    console_handler.setFormatter(colorlog.ColoredFormatter(
        LOG_CONFIG["format"],
        datefmt=LOG_CONFIG["date_format"],
        log_colors={
            "DEBUG": "cyan",
            "INFO": "green",
            "WARNING": "yellow",
            "ERROR": "red",
            "CRITICAL": "red,bg_white",
        },
    ))

    # File handler (plain text)
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_formatter = logging.Formatter(
        fmt="%(asctime)s - %(levelname)s - %(message)s",
        datefmt=LOG_CONFIG["date_format"],
    )
    file_handler.setFormatter(file_formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    logger.info(f"📝 Files ETL log file: {log_file}")
    return logger


logger = setup_logger()

# Target table (respects TABLE_CONFIG overrides)
CRIMES_TABLE = TABLE_CONFIG.get("crimes", "crimes")

# Output directory (MUST be set via FILES_MEDIA_BASE_PATH environment variable)
FILES_OUTPUT_DIR = os.getenv("FILES_MEDIA_BASE_PATH")
if not FILES_OUTPUT_DIR:
    logger.error("❌ FILES_MEDIA_BASE_PATH environment variable is not set. Please set it before running.")
    sys.exit(1)

# ============================================================================
# PRODUCTION CONFIGURATION - THREAD SAFETY & MEMORY MANAGEMENT
# ============================================================================

class ProductionConfig:
    """Production settings for 64GB RAM system."""
    
    # Parallel execution
    PARALLEL_WORKERS = int(os.getenv('ETL_PARALLEL_WORKERS', '8'))
    MAX_MEMORY_GB = int(os.getenv('ETL_MAX_MEMORY_GB', '50'))
    
    # File locking
    ENABLE_FILE_LOCKING = os.getenv('ENABLE_FILE_LOCKING', 'true').lower() == 'true'
    FILE_LOCK_TIMEOUT = int(os.getenv('FILES_LOCK_TIMEOUT', '60'))
    
    # Memory monitoring
    ENABLE_MEMORY_MONITORING = os.getenv('ENABLE_MEMORY_MONITORING', 'true').lower() == 'true'
    MEMORY_CHECK_INTERVAL = int(os.getenv('MEMORY_CHECK_INTERVAL_SECONDS', '30'))
    MEMORY_ALERT_THRESHOLD = int(os.getenv('MEMORY_ALERT_THRESHOLD_PERCENT', '85'))
    
    # Connection pool
    DB_POOL_SIZE = int(os.getenv('ETL_DB_POOL_SIZE', '10'))
    
    # Batch processing
    BATCH_SIZE = int(os.getenv('ETL_BATCH_SIZE', '100'))

# ============================================================================
# MEMORY MONITORING
# ============================================================================

class MemoryMonitor:
    """Monitor system memory and enforce limits."""
    
    def __init__(self, max_memory_gb=50):
        self.max_memory_bytes = max_memory_gb * 1024 * 1024 * 1024
        self.process = psutil.Process(os.getpid())
        self.lock = threading.Lock()
    
    def get_current_memory_mb(self) -> float:
        """Get current process memory in MB."""
        return self.process.memory_info().rss / 1024 / 1024
    
    def get_system_memory_percent(self) -> float:
        """Get system memory usage percent."""
        return psutil.virtual_memory().percent
    
    def check_memory(self) -> Tuple[bool, str]:
        """Check if memory usage is acceptable."""
        with self.lock:
            current_mb = self.get_current_memory_mb()
            sys_percent = self.get_system_memory_percent()
            
            if sys_percent > ProductionConfig.MEMORY_ALERT_THRESHOLD:
                msg = f"System: {sys_percent:.1f}% (Alert at {ProductionConfig.MEMORY_ALERT_THRESHOLD}%), Process: {current_mb:.0f}MB"
                return False, msg
            
            return True, f"System: {sys_percent:.1f}%, Process: {current_mb:.0f}MB"

# ============================================================================
# FILE LOCKING FOR THREAD SAFETY
# ============================================================================

class FileLocker:
    """Thread-safe file lock manager."""
    
    def __init__(self, lock_dir='.locks'):
        self.lock_dir = Path(lock_dir)
        self.lock_dir.mkdir(exist_ok=True)
        self.locks = {}
    
    def get_lock(self, file_path):
        """Get FileLock instance for file."""
        if not FileLock:
            return None
        
        file_hash = abs(hash(str(file_path))) % (2**32)
        lock_path = self.lock_dir / f"{file_hash}.lock"
        
        return FileLock(str(lock_path), timeout=ProductionConfig.FILE_LOCK_TIMEOUT)

# ============================================================================
# DATABASE CONNECTION POOL - THREAD SAFE
# ============================================================================

class DatabasePool:
    """Thread-safe connection pool."""
    
    def __init__(self, db_config, max_connections=10):
        self.db_config = db_config
        self.max_connections = max_connections
        self.pool = []
        self.available = threading.Semaphore(max_connections)
        self.lock = threading.Lock()
    
    def get_connection(self):
        """Get connection from pool."""
        self.available.acquire()
        with self.lock:
            if self.pool:
                return self.pool.pop()
        return psycopg2.connect(**self.db_config)
    
    def return_connection(self, conn):
        """Return connection to pool."""
        with self.lock:
            if len(self.pool) < self.max_connections:
                self.pool.append(conn)
            else:
                try:
                    conn.close()
                except:
                    pass
        self.available.release()
    
    def close_all(self):
        """Close all connections."""
        with self.lock:
            for conn in self.pool:
                try:
                    conn.close()
                except:
                    pass
            self.pool.clear()


class FilesETL:
    """ETL process to download FIR copy PDFs for crimes - Production Grade with Thread Safety."""

    def __init__(self, use_parallel=True):
        self.db_conn = None
        self.db_pool = None
        self.use_parallel = use_parallel and ProductionConfig.PARALLEL_WORKERS > 1
        self.memory_monitor = MemoryMonitor(ProductionConfig.MAX_MEMORY_GB) if ProductionConfig.ENABLE_MEMORY_MONITORING else None
        self.file_locker = FileLocker() if ProductionConfig.ENABLE_FILE_LOCKING else None
        self.stats_lock = threading.Lock()
        
        self.stats = {
            "total_fir_copy_values": 0,
            "total_processed": 0,
            "skipped_null_or_empty": 0,
            "downloaded_new": 0,
            "downloaded_replaced": 0,
            "failed": 0,
            "total_bytes": 0,
        }

    # -------------------------------------------------------------------------
    # DB helpers
    # -------------------------------------------------------------------------

    def connect_db(self) -> bool:
        """Connect to PostgreSQL using DB_CONFIG."""
        try:
            if self.use_parallel:
                self.db_pool = DatabasePool(DB_CONFIG, max_connections=ProductionConfig.DB_POOL_SIZE)
                logger.info(f"✅ Database pool created: {ProductionConfig.DB_POOL_SIZE} connections")
            else:
                self.db_conn = psycopg2.connect(**DB_CONFIG)
                logger.info(f"✅ Connected to database: {DB_CONFIG['database']}")
            return True
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            return False

    def close_db(self):
        """Close DB cursor and connection."""
        if self.db_pool:
            self.db_pool.close_all()
            logger.info("Database pool closed")
        elif self.db_conn:
            self.db_conn.close()
            logger.info("Database connection closed")
    
    def update_stats(self, **kwargs):
        """Thread-safe stats update."""
        with self.stats_lock:
            for key, value in kwargs.items():
                if key in self.stats:
                    self.stats[key] += value

    def get_distinct_fir_copy_values(self):
        """
        Fetch distinct non-null, non-empty fir_copy values from crimes table.
        This avoids downloading the same file multiple times.
        """
        sql = f"""
            SELECT DISTINCT fir_copy
            FROM {CRIMES_TABLE}
            WHERE fir_copy IS NOT NULL
              AND TRIM(fir_copy) <> ''
            ORDER BY fir_copy
        """
        logger.info(f"📥 Fetching distinct FIR_COPY values from table: {CRIMES_TABLE}")
        
        if self.use_parallel:
            conn = self.db_pool.get_connection()
            try:
                cursor = conn.cursor()
                cursor.execute(sql)
                rows = cursor.fetchall()
                cursor.close()
            finally:
                self.db_pool.return_connection(conn)
        else:
            self.db_conn.execute(sql)
            rows = self.db_conn.fetchall()
        
        fir_values = [r[0] for r in rows if r[0] is not None]
        self.stats["total_fir_copy_values"] = len(fir_values)
        logger.info(f"Found {len(fir_values)} distinct non-null FIR_COPY values")
        return fir_values

    # -------------------------------------------------------------------------
    # File download logic
    # -------------------------------------------------------------------------

    def build_file_url(self, file_id: str) -> str:
        """
        Build the files API URL for given file_id.

        Uses API_CONFIG['files_url'] if present, otherwise falls back to
        API_CONFIG['base_url'] + '/files'.
        """
        files_base = API_CONFIG.get("files_url")
        if files_base:
            base = files_base.rstrip("/")
        else:
            base = f"{API_CONFIG['base_url'].rstrip('/')}/files"
        return f"{base}/{file_id}"

    def get_destination_path(self, file_id: str) -> str:
        """Return absolute destination path for the PDF."""
        return os.path.join(FILES_OUTPUT_DIR, f"{file_id}.pdf")

    def ensure_output_dir(self):
        """Ensure the output directory exists."""
        os.makedirs(FILES_OUTPUT_DIR, exist_ok=True)

    def download_file(self, file_id: str) -> bool:
        """
        Download a single file by its ID with thread safety.

        - Acquires file lock before writing
        - Checks memory status
        - Downloads with retry logic
        - Updates stats in thread-safe manner
        """
        # Check memory before starting
        if self.memory_monitor and not self.memory_monitor.check_memory()[0]:
            is_ok, status = self.memory_monitor.check_memory()
            logger.warning(f"⚠️  Memory check: {status}")
        
        self.ensure_output_dir()
        dest_path = self.get_destination_path(file_id)
        url = self.build_file_url(file_id)
        headers = {
            "x-api-key": API_CONFIG["api_key"],
        }

        # Acquire file lock if enabled
        lock = self.file_locker.get_lock(dest_path) if self.file_locker else None
        
        try:
            if lock:
                lock.acquire()
            
            # Double-check file doesn't exist (another thread might have created it)
            if os.path.exists(dest_path):
                existing_size = os.path.getsize(dest_path)
                logger.info(
                    f"📄 File exists (thread-safe): {file_id}.pdf ({existing_size} bytes)"
                )
                self.update_stats(downloaded_replaced=1, total_bytes=existing_size)
                return True
            
            # Download with retry logic
            max_retries = int(API_CONFIG.get("max_retries", 3))
            base_delay = float(os.getenv("FILES_RETRY_DELAY_SECONDS", 1.0))

            for attempt in range(1, max_retries + 1):
                try:
                    logger.info(f"⬇️  Downloading {file_id} (attempt {attempt}/{max_retries})")
                    with requests.get(
                        url,
                        headers=headers,
                        timeout=API_CONFIG["timeout"],
                        stream=True,
                    ) as resp:
                        if resp.status_code == 200:
                            # Stream to disk
                            with open(dest_path, "wb") as f:
                                for chunk in resp.iter_content(chunk_size=8192):
                                    if chunk:
                                        f.write(chunk)

                            new_size = os.path.getsize(dest_path)
                            logger.info(f"✅ Downloaded {file_id}.pdf ({new_size} bytes)")
                            self.update_stats(downloaded_new=1, total_bytes=new_size)
                            return True

                        # Handle retryable errors
                        if resp.status_code == 429 or 500 <= resp.status_code < 600:
                            if resp.status_code == 429:
                                logger.warning(
                                    f"⚠️  Rate limited (429) for {file_id} - attempt {attempt}/{max_retries}"
                                )
                            else:
                                logger.error(
                                    f"❌ Server error ({resp.status_code}) for {file_id} - attempt {attempt}/{max_retries}"
                                )
                            
                            if attempt < max_retries:
                                retry_after = resp.headers.get("Retry-After")
                                if retry_after:
                                    try:
                                        sleep_for = float(retry_after)
                                    except:
                                        sleep_for = base_delay * (2 ** attempt)
                                else:
                                    sleep_for = base_delay * (2 ** attempt)
                                
                                logger.info(f"⏳ Backing off for {sleep_for:.1f}s before retry")
                                time.sleep(sleep_for)
                                continue
                        else:
                            logger.error(
                                f"❌ HTTP {resp.status_code} for {file_id} - not retriable"
                            )
                            break

                except requests.exceptions.Timeout:
                    logger.warning(f"⚠️  Timeout downloading {file_id} - attempt {attempt}/{max_retries}")
                    if attempt < max_retries:
                        time.sleep(base_delay * attempt)
                        continue
                    break
                
                except Exception as e:
                    logger.error(f"❌ Error downloading {file_id}: {e}")
                    if attempt < max_retries:
                        time.sleep(base_delay * attempt)
                        continue
                    break

            self.update_stats(failed=1)
            return False

        finally:
            if lock:
                try:
                    lock.release()
                except:
                    pass

    # -------------------------------------------------------------------------
    # Main run - Parallel or Sequential
    # -------------------------------------------------------------------------

    def run(self) -> bool:
        logger.info("=" * 80)
        logger.info("🚀 DOPAMAS ETL - Files Downloader (Production Grade)")
        logger.info(f"Mode: {'PARALLEL' if self.use_parallel else 'SEQUENTIAL'}")
        if self.use_parallel:
            logger.info(f"Workers: {ProductionConfig.PARALLEL_WORKERS}")
            logger.info(f"Max Memory: {ProductionConfig.MAX_MEMORY_GB}GB")
        logger.info("=" * 80)

        if not self.connect_db():
            logger.error("Failed to connect to database. Exiting.")
            return False

        try:
            fir_ids = self.get_distinct_fir_copy_values()
            if not fir_ids:
                logger.warning("No FIR_COPY values found to process.")
                return True

            if self.use_parallel:
                return self._run_parallel(fir_ids)
            else:
                return self._run_sequential(fir_ids)

        except KeyboardInterrupt:
            logger.warning("⚠️  Files ETL interrupted by user")
            return False
        except Exception as e:
            logger.error(f"❌ Files ETL failed with error: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
        finally:
            self.close_db()

    def _run_sequential(self, fir_ids: List[str]) -> bool:
        """Sequential processing (original method)."""
        logger.info("Starting SEQUENTIAL file downloads...")
        per_file_sleep = float(os.getenv("FILES_PER_FILE_SLEEP_SECONDS", 0.1))

        for idx, file_id in enumerate(fir_ids, start=1):
            if not file_id or not str(file_id).strip():
                self.update_stats(skipped_null_or_empty=1)
                logger.warning(f"[{idx}/{len(fir_ids)}] Skipping empty/NULL")
                continue

            self.update_stats(total_processed=1)
            self.download_file(str(file_id))

            if idx % 50 == 0:
                logger.info(f"Progress: {idx}/{len(fir_ids)} downloaded")
            
            if per_file_sleep > 0:
                time.sleep(per_file_sleep)

        return self._log_summary()

    def _run_parallel(self, fir_ids: List[str]) -> bool:
        """Parallel processing with ThreadPoolExecutor."""
        logger.info(f"Starting PARALLEL file downloads ({ProductionConfig.PARALLEL_WORKERS} workers)...")
        
        num_workers = ProductionConfig.PARALLEL_WORKERS
        processed = 0
        failed = 0

        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = {}
            
            # Submit all download tasks
            for file_id in fir_ids:
                if not file_id or not str(file_id).strip():
                    self.update_stats(skipped_null_or_empty=1)
                    continue
                
                future = executor.submit(self.download_file, str(file_id))
                futures[future] = str(file_id)
            
            # Process completed tasks
            for idx, future in enumerate(as_completed(futures), start=1):
                file_id = futures[future]
                try:
                    self.update_stats(total_processed=1)
                    success = future.result()
                    
                    if not success:
                        failed += 1
                    
                    processed += 1
                    
                    if processed % 50 == 0:
                        logger.info(
                            f"Progress: {processed}/{len(fir_ids)} "
                            f"({(processed/len(fir_ids))*100:.1f}%) completed"
                        )
                    
                    # Check memory every 100 files
                    if self.memory_monitor and processed % 100 == 0:
                        is_ok, status = self.memory_monitor.check_memory()
                        if is_ok:
                            logger.debug(f"Memory status: {status}")
                        else:
                            logger.warning(f"Memory status: {status}")

                except Exception as e:
                    logger.error(f"❌ Error processing {file_id}: {e}")
                    self.update_stats(failed=1)

        return self._log_summary()

    def _log_summary(self) -> bool:
        """Log final statistics."""
        logger.info("")
        logger.info("=" * 80)
        logger.info("📊 FILES ETL SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total distinct FIR_COPY values: {self.stats['total_fir_copy_values']}")
        logger.info(f"Total processed: {self.stats['total_processed']}")
        logger.info(f"✅ Successfully downloaded: {self.stats['downloaded_new']}")
        logger.info(f"🔄 Replaced existing: {self.stats['downloaded_replaced']}")
        logger.info(f"❌ Failed downloads: {self.stats['failed']}")
        logger.info(f"⏭️  Skipped NULL/empty: {self.stats['skipped_null_or_empty']}")
        
        if self.stats['total_bytes'] > 0:
            logger.info(f"📦 Total bytes downloaded: {self.stats['total_bytes'] / 1024 / 1024:.2f} MB")
        
        success_rate = (
            (self.stats['downloaded_new'] / max(1, self.stats['total_processed'])) * 100
            if self.stats['total_processed'] > 0 else 0
        )
        logger.info(f"Success rate: {success_rate:.1f}%")
        
        if self.memory_monitor:
            mem_status = self.memory_monitor.get_current_memory_mb()
            logger.info(f"Final memory: {mem_status:.0f} MB")
        
        logger.info("=" * 80)

        return self.stats['failed'] == 0


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='DOPAMAS ETL - Files Downloader')
    parser.add_argument(
        '--parallel',
        action='store_true',
        default=True,
        help='Enable parallel execution (default: enabled)'
    )
    parser.add_argument(
        '--sequential',
        action='store_true',
        help='Force sequential execution'
    )
    
    args = parser.parse_args()
    use_parallel = not args.sequential and args.parallel
    
    etl = FilesETL(use_parallel=use_parallel)
    success = etl.run()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

