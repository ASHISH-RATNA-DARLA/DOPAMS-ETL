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

import psycopg2
import requests
import colorlog

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

# Output directory (fixed path, but can be overridden by env if needed)
FILES_OUTPUT_DIR = os.getenv(
    "FILES_OUTPUT_DIR",
    "/data-drive/etl-process-dev/etl-files/tomcat/webapps/files/pdfs",
)


class FilesETL:
    """ETL process to download FIR copy PDFs for crimes."""

    def __init__(self):
        self.db_conn = None
        self.db_cursor = None
        self.stats = {
            "total_fir_copy_values": 0,
            "total_processed": 0,
            "skipped_null_or_empty": 0,
            "downloaded_new": 0,
            "downloaded_replaced": 0,
            "failed": 0,
        }

    # -------------------------------------------------------------------------
    # DB helpers
    # -------------------------------------------------------------------------

    def connect_db(self) -> bool:
        """Connect to PostgreSQL using DB_CONFIG."""
        try:
            self.db_conn = psycopg2.connect(**DB_CONFIG)
            self.db_cursor = self.db_conn.cursor()
            logger.info(f"✅ Connected to database: {DB_CONFIG['database']}")
            return True
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            return False

    def close_db(self):
        """Close DB cursor and connection."""
        if self.db_cursor:
            self.db_cursor.close()
        if self.db_conn:
            self.db_conn.close()
        logger.info("Database connection closed")

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
        self.db_cursor.execute(sql)
        rows = self.db_cursor.fetchall()
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
        Download a single file by its ID.

        - If destination file exists:
            * Log existing size
            * Delete it
        - Download latest content and save as {file_id}.pdf
        - Log final file size
        """
        self.ensure_output_dir()
        dest_path = self.get_destination_path(file_id)
        url = self.build_file_url(file_id)
        headers = {
            "x-api-key": API_CONFIG["api_key"],
        }

        # If file exists, log size and delete
        if os.path.exists(dest_path):
            existing_size = os.path.getsize(dest_path)
            logger.info(
                f"📄 Existing file for FIR_COPY={file_id}: "
                f"path={dest_path}, size={existing_size} bytes"
            )
            try:
                os.remove(dest_path)
                logger.info(f"🧹 Deleted existing file: {dest_path}")
            except Exception as e:
                logger.error(
                    f"❌ Failed to delete existing file for FIR_COPY={file_id}: {e}"
                )
                self.stats["failed"] += 1
                return False
            replace = True
        else:
            logger.info(
                f"📄 No existing file for FIR_COPY={file_id}, will download new file"
            )
            replace = False

        # Download latest file with simple retry & backoff (handles 429 / timeouts)
        max_retries = int(API_CONFIG.get("max_retries", 3))
        base_delay = float(os.getenv("FILES_RETRY_DELAY_SECONDS"))

        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"⬇️  Downloading FIR_COPY={file_id} from {url} (attempt {attempt}/{max_retries})")
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
                        logger.info(
                            f"✅ Downloaded FIR_COPY={file_id} to {dest_path}, "
                            f"size={new_size} bytes"
                        )

                        if replace:
                            self.stats["downloaded_replaced"] += 1
                        else:
                            self.stats["downloaded_new"] += 1

                        return True

                    # Handle rate limiting (429) and transient errors with backoff
                    if resp.status_code == 429 or 500 <= resp.status_code < 600:
                        # 429 is expected and handled gracefully, so log as WARNING
                        # 5xx errors are more serious, so log as ERROR
                        if resp.status_code == 429:
                            logger.warning(
                                f"⚠️  HTTP 429 (Rate Limited) for FIR_COPY={file_id} "
                                f"(attempt {attempt}/{max_retries}) - will retry after backoff"
                            )
                        else:
                            logger.error(
                                f"❌ HTTP {resp.status_code} while downloading FIR_COPY={file_id} "
                                f"(attempt {attempt}/{max_retries})"
                            )
                        if attempt < max_retries:
                            # Check for Retry-After header (some APIs provide this)
                            retry_after = resp.headers.get("Retry-After")
                            if retry_after:
                                try:
                                    sleep_for = float(retry_after)
                                    logger.info(f"⏳ API requested wait: {sleep_for:.1f}s (Retry-After header)")
                                except ValueError:
                                    sleep_for = base_delay * attempt * 2  # Longer backoff for 429
                            elif resp.status_code == 429:
                                # For 429, use longer exponential backoff
                                sleep_for = base_delay * attempt * 2
                            else:
                                # For 5xx errors, standard backoff
                                sleep_for = base_delay * attempt
                            
                            logger.info(f"⏳ Backing off for {sleep_for:.1f}s before retrying FIR_COPY={file_id}")
                            time.sleep(sleep_for)
                            continue
                    else:
                        # Non-retriable status codes
                        logger.error(
                            f"❌ HTTP {resp.status_code} while downloading FIR_COPY={file_id} "
                            f"- not retriable"
                        )
                        break

            except requests.exceptions.Timeout:
                logger.error(
                    f"❌ Timeout while downloading FIR_COPY={file_id} "
                    f"(attempt {attempt}/{max_retries})"
                )
                if attempt < max_retries:
                    sleep_for = base_delay * attempt
                    logger.info(f"⏳ Backing off for {sleep_for:.1f}s before retrying FIR_COPY={file_id}")
                    time.sleep(sleep_for)
                    continue
            except Exception as e:
                logger.error(
                    f"❌ Error while downloading FIR_COPY={file_id} "
                    f"(attempt {attempt}/{max_retries}): {e}"
                )
                # For unexpected errors, don't hammer retries too aggressively
                if attempt < max_retries:
                    sleep_for = base_delay * attempt
                    logger.info(f"⏳ Backing off for {sleep_for:.1f}s before retrying FIR_COPY={file_id}")
                    time.sleep(sleep_for)
                    continue

            # If we reach here, this attempt failed and is not retriable (or last attempt)
            break

        self.stats["failed"] += 1
        return False

    # -------------------------------------------------------------------------
    # Main run
    # -------------------------------------------------------------------------

    def run(self) -> bool:
        logger.info("=" * 80)
        logger.info("🚀 DOPAMAS ETL - Files Downloader (FIR_COPY PDFs)")
        logger.info("=" * 80)

        if not self.connect_db():
            logger.error("Failed to connect to database. Exiting.")
            return False

        try:
            fir_ids = self.get_distinct_fir_copy_values()
            if not fir_ids:
                logger.warning("No FIR_COPY values found to process.")
                return True

            per_file_sleep = float(os.getenv("FILES_PER_FILE_SLEEP_SECONDS"))

            for idx, file_id in enumerate(fir_ids, start=1):
                if not file_id or not str(file_id).strip():
                    self.stats["skipped_null_or_empty"] += 1
                    logger.warning(
                        f"[{idx}/{len(fir_ids)}] Skipping empty/NULL FIR_COPY value"
                    )
                    continue

                logger.info(
                    f"[{idx}/{len(fir_ids)}] Processing FIR_COPY={file_id}"
                )
                self.stats["total_processed"] += 1
                self.download_file(str(file_id))

                # Small delay between requests to avoid hammering the API (configurable)
                if per_file_sleep > 0:
                    time.sleep(per_file_sleep)

            # Final summary
            logger.info("")
            logger.info("=" * 80)
            logger.info("📊 FILES ETL SUMMARY")
            logger.info("=" * 80)
            logger.info(f"Total distinct FIR_COPY values: {self.stats['total_fir_copy_values']}")
            logger.info(f"Total processed (non-null/non-empty): {self.stats['total_processed']}")
            logger.info(f"Downloaded new files: {self.stats['downloaded_new']}")
            logger.info(f"Replaced existing files: {self.stats['downloaded_replaced']}")
            logger.info(f"Failed downloads: {self.stats['failed']}")
            logger.info(f"Skipped NULL/empty FIR_COPY: {self.stats['skipped_null_or_empty']}")
            logger.info("=" * 80)

            return True

        except KeyboardInterrupt:
            logger.warning("⚠️  Files ETL interrupted by user")
            return False
        except Exception as e:
            logger.error(f"❌ Files ETL failed with error: {e}")
            return False
        finally:
            self.close_db()


def main():
    etl = FilesETL()
    success = etl.run()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

