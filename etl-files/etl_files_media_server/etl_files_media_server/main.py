#!/usr/bin/env python3
"""
DOPAMAS ETL - Files Media Server Downloader

Process name: etl_files_media_server

Reads file metadata from the `files` table and downloads actual files
from the DOPAMAS files API into the Tomcat media server folder structure.

Key behavior:
- Uses DB_CONFIG from config.py (same .env-driven connection).
- Uses only the files API endpoint (API_CONFIG['files_url'] or base_url + /files).
- Table name is taken from env: FILES_TABLE (default: "files").
- For each row with a non-null file_id, determines destination folder based on:
    * source_type: crime, person, property, interrogation
    * source_field: FIR_COPY, MEDIA, INTERROGATION_REPORT, DOPAMS_DATA, IDENTITY_DETAILS
- Saves files into:
    base: /mnt/shared-etl-files  (from FILES_MEDIA_BASE_PATH env var)
    crime/FIR_COPY                  -> crimes/
    person/IDENTITY_DETAILS         -> person/identitydetails/
    person/MEDIA                    -> person/media/
    property/MEDIA                  -> property/
    interrogation/MEDIA             -> interrogations/media/
    interrogation/INTERROGATION_REPORT -> interrogations/interrogationreport/
    interrogation/DOPAMS_DATA       -> interrogations/dopamsdata/
- File name: {file_id}.{ext}, where ext is derived from Content-Type (fallback: .pdf).
- Idempotency on disk: if file already exists, it is skipped.
- Files API rate limit: 10 requests per minute (enforced by sleeping between requests).

FIX LOG:
- BUG FIX: download_attempts was being incremented TWICE on a successful download:
    once at the top of download_single_file() and again inside _mark_as_downloaded(success=True).
    Fixed by removing the increment from _mark_as_downloaded entirely.
    download_attempts is now only incremented once, at the start of each attempt, accurately
    reflecting the real number of attempts made.
"""

from __future__ import annotations

import os
import sys
import time
import logging
import argparse
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

import psycopg2
import requests
import colorlog
from env_utils import first_env, get_bool_env, get_int_env

# Allow running this file directly as a script as well as via `python -m`.
CURRENT_DIR = Path(__file__).resolve().parent
PARENT_DIR = CURRENT_DIR.parent
PARENT_DIR_STR = str(PARENT_DIR)
if PARENT_DIR_STR not in sys.path:
    sys.path.insert(0, PARENT_DIR_STR)

from config import DB_CONFIG, API_CONFIG, LOG_CONFIG


# -----------------------------------------------------------------------------
# Constants & configuration
# -----------------------------------------------------------------------------

# Table name for files metadata
FILES_TABLE = first_env("FILES_TABLE", default="files")

# Base path on the Tomcat media server - ALWAYS read from env
BASE_MEDIA_PATH = first_env("FILES_MEDIA_BASE_PATH", default="/mnt/shared-etl-files")

# Files API rate limit: reduced to 5 requests per minute to avoid connection blocking
FILES_API_MAX_RPM = 5
SECONDS_PER_REQUEST = 60.0 / FILES_API_MAX_RPM  # 12.0 seconds

# Hard cap to avoid retrying permanently bad file_ids forever across runs.
MAX_TOTAL_ATTEMPTS = get_int_env("FILES_MAX_TOTAL_ATTEMPTS", 5)

# By default, do not requeue terminal failures (PERMANENT:*).
RETRY_PERMANENT_ERRORS = get_bool_env("FILES_RETRY_PERMANENT_ERRORS", False)

# -----------------------------------------------------------------------------
# Logging setup
# -----------------------------------------------------------------------------

def setup_logger() -> logging.Logger:
    """Configure console + file logging for etl_files_media_server."""
    logger = colorlog.getLogger("etl-files-media-server")
    logger.setLevel(LOG_CONFIG.get("level", "INFO").upper())

    if logger.handlers:
        return logger

    os.makedirs("logs", exist_ok=True)
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    log_file = f"logs/files_media_server_etl_{timestamp}.log"

    console_handler = colorlog.StreamHandler()
    console_handler.setFormatter(
        colorlog.ColoredFormatter(
            LOG_CONFIG["format"],
            datefmt=LOG_CONFIG["date_format"],
            log_colors={
                "DEBUG": "cyan",
                "INFO": "green",
                "WARNING": "yellow",
                "ERROR": "red",
                "CRITICAL": "red,bg_white",
            },
        )
    )

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_formatter = logging.Formatter(
        fmt="%(asctime)s - %(levelname)s - %(message)s",
        datefmt=LOG_CONFIG["date_format"],
    )
    file_handler.setFormatter(file_formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    logger.info(f"📝 Files Media Server ETL log file: {log_file}")
    return logger


logger = setup_logger()


# -----------------------------------------------------------------------------
# Helper functions
# -----------------------------------------------------------------------------

def build_files_base_url() -> str:
    """
    Determine the base URL for the files API.
    Uses API_CONFIG['files_url'] if present, otherwise falls back to
    API_CONFIG['base_url'] + '/files'.
    """
    files_base = API_CONFIG.get("files_url")
    if files_base:
        return files_base.rstrip("/")
    return f"{API_CONFIG['base_url'].rstrip('/')}/files"


FILES_BASE_URL = build_files_base_url()


def map_destination_subdir(source_type: str, source_field: str) -> Optional[str]:
    """
    Map (source_type, source_field) to a relative subdirectory under BASE_MEDIA_PATH.
    Returns None if the combination is unsupported and should be skipped.

    NOTE: source_field is normalised to UPPER CASE before comparison, so the DB
    enum value 'uploadChargeSheet' is correctly matched as 'UPLOADCHARGESHEET'.
    """
    source_type = (source_type or "").lower()
    source_field = (source_field or "").upper()

    if source_type == "crime" and source_field == "FIR_COPY":
        return "crimes"

    # Backward-compatibility: some historical rows use crime/MEDIA.
    if source_type == "crime" and source_field == "MEDIA":
        return "crimes"

    if source_type == "person" and source_field == "IDENTITY_DETAILS":
        return os.path.join("person", "identitydetails")

    if source_type == "person" and source_field == "MEDIA":
        return os.path.join("person", "media")

    if source_type == "property" and source_field == "MEDIA":
        return "property"

    if source_type == "interrogation" and source_field == "MEDIA":
        return os.path.join("interrogations", "media")

    if source_type == "interrogation" and source_field == "INTERROGATION_REPORT":
        return os.path.join("interrogations", "interrogationreport")

    if source_type == "interrogation" and source_field == "DOPAMS_DATA":
        return os.path.join("interrogations", "dopamsdata")

    if source_type == "mo_seizures" and source_field == "MO_MEDIA":
        return "mo_seizures"

    # DB enum stores 'uploadChargeSheet'; .upper() makes this 'UPLOADCHARGESHEET'
    if source_type == "chargesheets" and source_field == "UPLOADCHARGESHEET":
        return "chargesheets"

    if source_type == "case_property" and source_field == "MEDIA":
        return "fsl_case_property"

    return None


def extension_from_response(resp: requests.Response) -> str:
    """
    Determine file extension from HTTP response.
    Priority: Content-Type header → Content-Disposition filename → fallback '.pdf'
    """
    content_type = resp.headers.get("Content-Type", "").split(";")[0].strip().lower()
    if content_type:
        if content_type == "application/pdf":
            return ".pdf"
        if content_type in ("image/jpeg", "image/jpg"):
            return ".jpg"
        if content_type == "image/png":
            return ".png"
        if content_type == "image/gif":
            return ".gif"
        if content_type in ("video/mp4", "application/mp4"):
            return ".mp4"
        if content_type == "image/webp":
            return ".webp"

    cd = resp.headers.get("Content-Disposition", "")
    if "filename=" in cd:
        filename_part = cd.split("filename=", 1)[1].strip().strip('"').strip("'")
        if "." in filename_part:
            ext = "." + filename_part.rsplit(".", 1)[1]
            if len(ext) <= 10:
                return ext

    return ".pdf"


def ensure_directory(path: str) -> None:
    """
    Ensure a directory exists and has correct permissions.
    Creates directory with permissions 775 so both owner and group can write.
    """
    try:
        if os.path.exists(path):
            if os.access(path, os.W_OK):
                return
            else:
                try:
                    os.chmod(path, 0o775)
                    if not os.access(path, os.W_OK):
                        raise PermissionError(f"Directory {path} exists but is not writable even after chmod")
                except PermissionError:
                    logger.error(f"❌ Permission denied: Directory {path} exists but is not writable")
                    logger.error(f"   Directory owner: {os.stat(path).st_uid}")
                    logger.error(f"   Current user: {os.getuid()}")
                    logger.error(f"   Current group: {os.getgid()}")
                    logger.error(f"   Solution: Ensure user is in tomcat group and directory has group write permissions")
                    raise
        else:
            os.makedirs(path, mode=0o775, exist_ok=True)
            if os.path.exists(path):
                try:
                    os.chmod(path, 0o775)
                except PermissionError:
                    if not os.access(path, os.W_OK):
                        raise
    except PermissionError as e:
        logger.error(f"❌ Permission denied creating directory {path}: {e}")
        logger.error(f"   Directory owner: {os.stat(path).st_uid if os.path.exists(path) else 'N/A'}")
        logger.error(f"   Current user: {os.getuid()}")
        logger.error(f"   Current group: {os.getgid()}")
        logger.error(f"   Solution: Add your user to tomcat group or run with sudo")
        raise
    except Exception as e:
        logger.warning(f"⚠️  Could not set directory permissions for {path}: {e}")
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)


def build_destination_path(file_id: str, source_type: str, source_field: str, resp: requests.Response) -> Optional[str]:
    """
    Build the absolute destination path for a file.
    Returns None if the (source_type, source_field) combination is unsupported.
    """
    subdir = map_destination_subdir(source_type, source_field)
    if subdir is None:
        return None

    ext = extension_from_response(resp)
    dest_dir = os.path.join(BASE_MEDIA_PATH, subdir)
    ensure_directory(dest_dir)
    return os.path.join(dest_dir, f"{file_id}{ext}")


# -----------------------------------------------------------------------------
# Core ETL class
# -----------------------------------------------------------------------------

class FilesMediaServerETL:
    """ETL process to download files from API and save to Tomcat media folders."""

    def __init__(self, repair: bool = False) -> None:
        self.db_conn: Optional[psycopg2.extensions.connection] = None
        self.db_cursor: Optional[psycopg2.extensions.cursor] = None
        self.repair = repair
        # Strict global API pacing gate: every HEAD/GET must pass through this.
        # This prevents bursts even in fast sequential code paths.
        self._api_rate_lock = threading.Lock()
        self._last_api_request_ts = 0.0
        self.stats = {
            "total_rows": 0,
            "total_with_file_id": 0,
            "total_processed": 0,
            "downloaded": 0,
            "failed": 0,
            "skipped_no_mapping": 0,
            "skipped_null_file_id": 0,
            "skipped_already_downloaded": 0,
            "skipped_exists_on_disk": 0,
            "resumed_from": None,
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
        except Exception as exc:
            logger.error(f"❌ Database connection failed: {exc}")
            return False

    def close_db(self) -> None:
        """Close DB cursor and connection."""
        if self.db_cursor:
            self.db_cursor.close()
        if self.db_conn:
            self.db_conn.close()
        logger.info("Database connection closed")

    def ensure_download_tracking_columns(self) -> bool:
        """
        Check if download tracking columns exist, if not add them.
        Returns True if columns exist or were successfully added, False otherwise.
        """
        try:
            check_sql = """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = %s
                  AND column_name IN ('downloaded_at', 'is_downloaded', 'download_error', 'download_attempts', 'created_at')
            """
            self.db_cursor.execute(check_sql, (FILES_TABLE,))
            existing_columns = {row[0] for row in self.db_cursor.fetchall()}

            required_columns = {
                'downloaded_at': 'TIMESTAMP',
                'is_downloaded': 'BOOLEAN DEFAULT FALSE',
                'download_error': 'TEXT',
                'download_attempts': 'INTEGER DEFAULT 0',
                'created_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
            }

            missing_columns = set(required_columns.keys()) - existing_columns

            if not missing_columns:
                logger.info("✅ Download tracking columns already exist")
            else:
                logger.info(f"📝 Adding missing download tracking columns: {', '.join(missing_columns)}")
                for col_name, col_def in required_columns.items():
                    if col_name not in existing_columns:
                        alter_sql = f"ALTER TABLE {FILES_TABLE} ADD COLUMN IF NOT EXISTS {col_name} {col_def}"
                        self.db_cursor.execute(alter_sql)
                        logger.info(f"  ✓ Added column: {col_name}")

                if 'created_at' in missing_columns:
                    logger.info("  📝 Backfilling created_at for existing records...")
                    backfill_sql = f"UPDATE {FILES_TABLE} SET created_at = CURRENT_TIMESTAMP WHERE created_at IS NULL"
                    self.db_cursor.execute(backfill_sql)
                    logger.info(f"  ✓ Backfilled created_at for existing records")

            index_sqls = [
                f"CREATE INDEX IF NOT EXISTS idx_files_is_downloaded ON {FILES_TABLE}(is_downloaded) WHERE is_downloaded = TRUE",
                f"CREATE INDEX IF NOT EXISTS idx_files_downloaded_at ON {FILES_TABLE}(downloaded_at) WHERE downloaded_at IS NOT NULL",
                f"CREATE INDEX IF NOT EXISTS idx_files_created_at ON {FILES_TABLE}(created_at)",
                f"CREATE INDEX IF NOT EXISTS idx_files_source_type_created ON {FILES_TABLE}(source_type, created_at)",
            ]

            for index_sql in index_sqls:
                self.db_cursor.execute(index_sql)

            self.db_conn.commit()
            logger.info("✅ Download tracking columns and indexes created successfully")
            return True

        except Exception as exc:
            logger.error(f"❌ Failed to ensure download tracking columns: {exc}")
            self.db_conn.rollback()
            return False

    def get_last_processed_date_per_source_type(self) -> dict:
        """
        Get the last processed date per source_type (for informational logging only).
        """
        try:
            sql = f"""
                SELECT
                    source_type,
                    MAX(downloaded_at) as last_downloaded_date
                FROM {FILES_TABLE}
                WHERE is_downloaded = TRUE
                  AND downloaded_at IS NOT NULL
                GROUP BY source_type
            """
            self.db_cursor.execute(sql)
            results = self.db_cursor.fetchall()
            return {row[0]: row[1] for row in results}
        except Exception as exc:
            logger.warning(f"⚠️  Could not determine last processed dates: {exc}")
            return {}

    def fetch_files_rows(self, resume_from_date_per_source: Optional[dict] = None):
        """
        Fetch rows from FILES_TABLE that need to be downloaded.

        Downloads ALL files where is_downloaded IS FALSE OR downloaded_at IS NULL.
        Files that succeeded (is_downloaded = TRUE) are automatically skipped.
        Processing order: newest files first (ORDER BY created_at DESC).
        """
        conditions = [
            "file_id IS NOT NULL",
            "has_field IS TRUE",
            "is_empty IS FALSE"
        ]
        query_params = []
        
        if not self.repair:
            conditions.append("(is_downloaded IS FALSE OR downloaded_at IS NULL)")
            conditions.append("(download_attempts IS NULL OR download_attempts < %s)")
            query_params.append(MAX_TOTAL_ATTEMPTS)
            if not RETRY_PERMANENT_ERRORS:
                conditions.append("(download_error IS NULL OR download_error NOT LIKE 'PERMANENT:%')")

        logger.info(f"📥 Fetching file metadata from table: {FILES_TABLE}")
        if self.repair:
            logger.info("   🔧 REPAIR MODE: Fetching ALL files with file_id (including those marked as downloaded)")
        else:
            logger.info("   🔄 Downloading files where: is_downloaded IS FALSE OR downloaded_at IS NULL")
        logger.info("   📌 Processing order: newest files first (created_at DESC)")

        sql = f"""
            SELECT source_type, source_field, file_id
            FROM {FILES_TABLE}
            WHERE {' AND '.join(conditions)}
            ORDER BY 
                CASE 
                    WHEN source_type = 'crime' THEN 1
                    WHEN source_type = 'chargesheets' THEN 2
                    WHEN source_type = 'interrogation' THEN 3
                    ELSE 4
                END,
                created_at DESC NULLS LAST, 
                file_id
        """
        self.db_cursor.execute(sql, tuple(query_params))

        if resume_from_date_per_source:
            logger.info("   📅 Last download dates per source_type (for reference):")
            for source_type, resume_date in sorted(resume_from_date_per_source.items()):
                if resume_date:
                    logger.info(f"      - {source_type}: last downloaded on {resume_date}")
                else:
                    logger.info(f"      - {source_type}: no previous downloads")

        rows = self.db_cursor.fetchall()

        if rows:
            stats_sql = f"""
                SELECT
                    COUNT(*) as total_count,
                    COUNT(CASE WHEN is_downloaded IS NULL THEN 1 END) as never_attempted,
                    COUNT(CASE WHEN is_downloaded IS FALSE THEN 1 END) as previously_failed,
                    MAX(created_at) as newest_date,
                    MIN(created_at) as oldest_date
                FROM {FILES_TABLE}
                WHERE {' AND '.join(conditions)}
            """
            self.db_cursor.execute(stats_sql, tuple(query_params))
            stats = self.db_cursor.fetchone()

            if stats and stats[0]:
                total_count = stats[0]
                never_attempted = stats[1] or 0
                previously_failed = stats[2] or 0
                newest = stats[3].strftime('%Y-%m-%d %H:%M:%S') if stats[3] else 'N/A'
                oldest = stats[4].strftime('%Y-%m-%d %H:%M:%S') if stats[4] else 'N/A'

                logger.info(f"   📊 Download statistics:")
                logger.info(f"      - Total files to download: {total_count}")
                logger.info(f"      - Never attempted: {never_attempted}")
                logger.info(f"      - Previously failed (will retry): {previously_failed}")
                logger.info(f"      - Max total attempts allowed per file_id: {MAX_TOTAL_ATTEMPTS}")
                logger.info(f"      - Date range: {newest} (newest) → {oldest} (oldest)")

        by_source_type = {}
        for row in rows:
            source_type = row[0]
            if source_type not in by_source_type:
                by_source_type[source_type] = 0
            by_source_type[source_type] += 1

        if by_source_type:
            logger.info("   📊 Files to download by source_type (processing newest first):")
            for source_type, count in sorted(by_source_type.items()):
                logger.info(f"      - {source_type}: {count} files")
        else:
            logger.info("   ✅ No files to download - all files are already downloaded!")

        self.stats["total_rows"] = len(rows)
        logger.info(f"Found {len(rows)} files to download (undownloaded or previously failed)")
        return rows

    # -------------------------------------------------------------------------
    # HTTP / download helpers
    # -------------------------------------------------------------------------

    def _wait_for_request_slot(self) -> None:
        """
        Strict pacing: allow only one API request every SECONDS_PER_REQUEST.

        This is enforced before EVERY outbound HEAD/GET call so we do not rely
        on loop speed or sequential execution behavior.
        """
        with self._api_rate_lock:
            now = time.time()
            wait_for = (self._last_api_request_ts + SECONDS_PER_REQUEST) - now
            if wait_for > 0:
                time.sleep(wait_for)
            self._last_api_request_ts = time.time()

    def build_file_url(self, file_id: str) -> str:
        """Build the files API URL for a given file_id."""
        return f"{FILES_BASE_URL}/{file_id}"

    def check_file_exists(self, file_id: str) -> Tuple[bool, Optional[str]]:
        """
        Check if file exists using HEAD request (faster than full download).
        Returns: (exists: bool, error_message: Optional[str])
        """
        url = self.build_file_url(file_id)
        headers = {"x-api-key": API_CONFIG["api_key"]}
        
        # Retry logic for existence check to handle transient connection errors
        for attempt in range(1, 4):
            try:
                # Enforce strict request pacing for every HEAD call.
                self._wait_for_request_slot()
                response = requests.head(
                    url,
                    headers=headers,
                    timeout=API_CONFIG.get("timeout", 30),
                    allow_redirects=True
                )

                if response.status_code == 200:
                    return (True, None)
                elif response.status_code == 404:
                    return (False, "PERMANENT: HTTP 404 Not Found - File does not exist")
                elif response.status_code == 400:
                    return (False, "PERMANENT: HTTP 400 Bad Request - File does not exist or invalid file_id")
                elif response.status_code == 429:
                    # Rate limited, wait and retry
                    time.sleep(SECONDS_PER_REQUEST * attempt)
                    continue
                elif response.status_code in (401, 403):
                    return (False, f"HTTP {response.status_code} - Authentication/Authorization failed")
                else:
                    return (True, None)

            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
                if attempt < 3:
                    logger.warning(f"⚠️  Existence check attempt {attempt} failed for {file_id}: {exc}. Retrying...")
                    time.sleep(SECONDS_PER_REQUEST * attempt)
                    continue
                return (False, f"Connection error after {attempt} attempts: {str(exc)}")
            except Exception as exc:
                return (False, f"Error checking file: {str(exc)}")
        
        return (False, "Failed to check existence after retries")

    def download_single_file(
        self,
        file_id: str,
        source_type: str,
        source_field: str,
    ) -> bool:
        """
        Download a single file by its metadata.

        FIX: download_attempts is now incremented ONCE per attempt at the start
        of each retry loop iteration. It is NOT incremented again in
        _mark_as_downloaded(). This gives an accurate count of real attempts made.
        """
        logger.info(f"🔍 Checking if file exists: file_id={file_id}")
        exists, check_error = self.check_file_exists(file_id)

        if not exists and check_error:
            logger.error(
                f"❌ File does not exist for file_id={file_id}: {check_error} - "
                f"marking as permanently failed and skipping"
            )
            self._mark_as_downloaded(file_id, success=False, error_msg=check_error)
            self.stats["failed"] += 1
            return False

        url = self.build_file_url(file_id)
        headers = {"x-api-key": API_CONFIG["api_key"]}
        max_retries = int(API_CONFIG.get("max_retries", 3))

        for attempt in range(1, max_retries + 1):
            start_time = time.time()

            # ----------------------------------------------------------------
            # FIX: Increment download_attempts ONCE per attempt here.
            # _mark_as_downloaded() no longer increments it, preventing the
            # double-count that occurred on every successful download.
            # ----------------------------------------------------------------
            try:
                increment_sql = f"""
                    UPDATE {FILES_TABLE}
                    SET download_attempts = COALESCE(download_attempts, 0) + 1
                    WHERE file_id = %s
                """
                self.db_cursor.execute(increment_sql, (file_id,))
                self.db_conn.commit()
            except Exception as exc:
                logger.warning(f"⚠️  Failed to increment download_attempts for file_id={file_id}: {exc}")
                self.db_conn.rollback()

            try:
                logger.info(
                    f"⬇️  Downloading file_id={file_id} "
                    f"(source_type={source_type}, source_field={source_field}) "
                    f"from {url} (attempt {attempt}/{max_retries})"
                )
                # Enforce strict request pacing for every GET call.
                self._wait_for_request_slot()
                with requests.get(
                    url,
                    headers=headers,
                    timeout=API_CONFIG["timeout"],
                    stream=True,
                ) as resp:
                    if resp.status_code == 200:
                        dest_path = build_destination_path(
                            file_id=file_id,
                            source_type=source_type,
                            source_field=source_field,
                            resp=resp,
                        )

                        if dest_path is None:
                            logger.warning(
                                f"⚠️  No destination mapping for "
                                f"(source_type={source_type}, source_field={source_field}); "
                                f"skipping file_id={file_id}"
                            )
                            self.stats["skipped_no_mapping"] += 1
                            self._mark_as_downloaded(
                                file_id,
                                success=False,
                                error_msg=(
                                    f"PERMANENT: No destination mapping for "
                                    f"source_type={source_type}, source_field={source_field}"
                                ),
                            )
                            self._respect_rate_limit(start_time)
                            return False

                        if os.path.exists(dest_path):
                            existing_size = os.path.getsize(dest_path)
                            if existing_size > 0:
                                logger.info(
                                    f"⏭️  File already exists on disk for file_id={file_id}: "
                                    f"path={dest_path}, size={existing_size} bytes (skipping download)"
                                )
                                self._mark_as_downloaded(file_id, success=True)
                                self.stats["skipped_exists_on_disk"] += 1
                                self._respect_rate_limit(start_time)
                                return True
                            else:
                                logger.warning(f"⚠️  File {dest_path} exists but is 0 bytes. Re-downloading...")
                                os.remove(dest_path)

                        try:
                            with open(dest_path, "wb") as f:
                                for chunk in resp.iter_content(chunk_size=8192):
                                    if chunk:
                                        f.write(chunk)
                            os.chmod(dest_path, 0o644)
                        except PermissionError as e:
                            logger.error(f"❌ Permission denied writing file {dest_path}: {e}")
                            logger.error(f"   Directory: {os.path.dirname(dest_path)}")
                            logger.error(f"   Directory exists: {os.path.exists(os.path.dirname(dest_path))}")
                            logger.error(f"   Directory writable: {os.access(os.path.dirname(dest_path), os.W_OK)}")
                            logger.error(f"   Current user: {first_env('USER', default='unknown')}")
                            logger.error(f"   Current UID: {os.getuid()}")
                            logger.error(f"   Solution: Run chmod -R 777 {BASE_MEDIA_PATH}")
                            raise

                        new_size = os.path.getsize(dest_path)
                        logger.info(
                            f"✅ Downloaded file_id={file_id} to {dest_path}, "
                            f"size={new_size} bytes"
                        )
                        self._mark_as_downloaded(file_id, success=True)
                        self.stats["downloaded"] += 1
                        self._respect_rate_limit(start_time)
                        return True

                    if resp.status_code == 429 or 500 <= resp.status_code < 600:
                        if resp.status_code == 429:
                            logger.warning(
                                f"⚠️  HTTP 429 (Rate Limited) for file_id={file_id} "
                                f"(attempt {attempt}/{max_retries}) - will retry"
                            )
                        else:
                            logger.error(
                                f"❌ HTTP {resp.status_code} while downloading file_id={file_id} "
                                f"(attempt {attempt}/{max_retries})"
                            )

                        if attempt < max_retries:
                            retry_after_header = resp.headers.get("Retry-After")
                            if retry_after_header:
                                try:
                                    sleep_for = float(retry_after_header)
                                    logger.info(f"⏳ API requested wait: {sleep_for:.1f}s (Retry-After header)")
                                except ValueError:
                                    sleep_for = SECONDS_PER_REQUEST * attempt
                            else:
                                sleep_for = SECONDS_PER_REQUEST * attempt

                            logger.info(f"⏳ Backing off for {sleep_for:.1f}s before retrying file_id={file_id}")
                            self._respect_rate_limit(start_time, extra_sleep=sleep_for)
                            continue

                    else:
                        try:
                            error_body = resp.text[:200].replace("'", "''")
                        except:
                            error_body = "Could not read response body"
                            
                        if resp.status_code == 400:
                            error_msg = f"PERMANENT: HTTP 400 Bad Request - {error_body}"
                            logger.error(f"❌ File does not exist (HTTP 400) for file_id={file_id}: {error_body}")
                        elif resp.status_code == 404:
                            error_msg = f"PERMANENT: HTTP 404 Not Found - {error_body}"
                            logger.error(f"❌ File not found (HTTP 404) for file_id={file_id}")
                        elif resp.status_code == 401:
                            error_msg = f"HTTP 401 Unauthorized - {error_body}"
                            logger.error(f"❌ Authentication failed (HTTP 401) for file_id={file_id}")
                        elif resp.status_code == 403:
                            error_msg = f"HTTP 403 Forbidden - {error_body}"
                            logger.error(f"❌ Access denied (HTTP 403) for file_id={file_id}")
                        else:
                            error_msg = f"HTTP {resp.status_code} - {error_body}"
                            logger.error(f"❌ {error_msg} for file_id={file_id}")

                        self._mark_as_downloaded(file_id, success=False, error_msg=error_msg)
                        self._respect_rate_limit(start_time)
                        self.stats["failed"] += 1
                        return False

            except requests.exceptions.Timeout:
                logger.error(f"❌ Timeout while downloading file_id={file_id} (attempt {attempt}/{max_retries})")
                if attempt < max_retries:
                    sleep_for = SECONDS_PER_REQUEST * attempt
                    logger.info(f"⏳ Backing off for {sleep_for:.1f}s before retrying file_id={file_id}")
                    self._respect_rate_limit(start_time, extra_sleep=sleep_for)
                    continue
            except Exception as exc:
                logger.error(f"❌ Error while downloading file_id={file_id} (attempt {attempt}/{max_retries}): {exc}")
                if attempt < max_retries:
                    sleep_for = SECONDS_PER_REQUEST * attempt
                    logger.info(f"⏳ Backing off for {sleep_for:.1f}s before retrying file_id={file_id}")
                    self._respect_rate_limit(start_time, extra_sleep=sleep_for)
                    continue

            self._respect_rate_limit(start_time)
            break

        error_msg = f"Failed after {max_retries} attempts"
        self._mark_as_downloaded(file_id, success=False, error_msg=error_msg)
        self.stats["failed"] += 1
        return False

    def _mark_as_downloaded(self, file_id: str, success: bool, error_msg: Optional[str] = None) -> None:
        """
        Update database to mark file as downloaded or record error.

        FIX: download_attempts is NO LONGER incremented here. It is incremented
        once at the start of each attempt loop in download_single_file(), which
        gives an accurate count. The previous code incremented it here AND at
        the top of the loop, resulting in double-counting on success.

        Args:
            file_id: The file ID
            success: True if download succeeded, False if failed
            error_msg: Error message if download failed
        """
        try:
            if success:
                # NOTE: download_attempts intentionally NOT incremented here.
                # It is already incremented once per attempt in download_single_file().
                update_sql = f"""
                    UPDATE {FILES_TABLE}
                    SET is_downloaded = TRUE,
                        downloaded_at = CURRENT_TIMESTAMP,
                        download_error = NULL
                    WHERE file_id = %s
                """
                self.db_cursor.execute(update_sql, (file_id,))
            else:
                # NOTE: download_attempts intentionally NOT incremented here either.
                update_sql = f"""
                    UPDATE {FILES_TABLE}
                    SET is_downloaded = FALSE,
                        download_error = %s
                    WHERE file_id = %s
                """
                self.db_cursor.execute(update_sql, (error_msg or "Download failed", file_id,))

            self.db_conn.commit()
        except Exception as exc:
            logger.warning(f"⚠️  Failed to update download status for file_id={file_id}: {exc}")
            self.db_conn.rollback()

    @staticmethod
    def _respect_rate_limit(start_time: float, extra_sleep: float = 0.0) -> None:
        """
        Sleep enough to respect 10 RPM overall.
        Ensures at least SECONDS_PER_REQUEST between the start of this request
        and the next attempt. Adds optional extra_sleep for backoff / Retry-After.
        """
        elapsed = time.time() - start_time
        min_sleep = max(0.0, SECONDS_PER_REQUEST - elapsed)
        total_sleep = min_sleep + max(0.0, extra_sleep)
        if total_sleep > 0:
            time.sleep(total_sleep)

    # -------------------------------------------------------------------------
    # Main run
    # -------------------------------------------------------------------------

    def run(self) -> bool:
        logger.info("=" * 80)
        logger.info("🚀 DOPAMAS ETL - Files Media Server Downloader")
        logger.info("=" * 80)
        logger.info(f"Using FILES_TABLE={FILES_TABLE}")
        logger.info(f"Files base URL: {FILES_BASE_URL}")
        logger.info(f"Media base path: {BASE_MEDIA_PATH}")
        logger.info(f"Rate limit: {FILES_API_MAX_RPM} requests per minute")

        if not self.connect_db():
            logger.error("Failed to connect to database. Exiting.")
            return False

        try:
            logger.info("🔍 Checking for download tracking columns...")
            if not self.ensure_download_tracking_columns():
                logger.error("Failed to set up download tracking columns. Exiting.")
                return False

            last_dates_per_source = self.get_last_processed_date_per_source_type()
            if last_dates_per_source:
                logger.info("📌 Last download dates per source_type (for reference):")
                for source_type, last_date in sorted(last_dates_per_source.items()):
                    logger.info(f"   - {source_type}: {last_date}")
            else:
                logger.info("📌 No previous downloads found - will download all files with file_ids")

            logger.info("🔄 Processing files in backwards order: newest files first, then older files")
            rows = self.fetch_files_rows(resume_from_date_per_source=last_dates_per_source)
            if not rows:
                logger.warning("No rows to process (all files already downloaded or no valid file_ids).")
                return True

            self.stats["total_with_file_id"] = len(rows)

            for idx, (source_type, source_field, file_id) in enumerate(rows, start=1):
                if not file_id:
                    self.stats["skipped_null_file_id"] += 1
                    logger.warning(
                        f"[{idx}/{len(rows)}] Skipping row with NULL file_id "
                        f"(source_type={source_type}, source_field={source_field})"
                    )
                    continue

                logger.info(
                    f"[{idx}/{len(rows)}] Processing file_id={file_id} "
                    f"(source_type={source_type}, source_field={source_field})"
                )
                self.stats["total_processed"] += 1
                success = self.download_single_file(
                    str(file_id), str(source_type), str(source_field)
                )

                if not success:
                    logger.error(
                        f"❌ Download failed for file_id={file_id} after all retries; "
                        f"moving to next file (will retry on next run if is_downloaded = FALSE)"
                    )

            logger.info("")
            logger.info("=" * 80)
            logger.info("📊 FILES MEDIA SERVER ETL SUMMARY")
            logger.info("=" * 80)
            logger.info(f"Total rows fetched: {self.stats['total_rows']}")
            logger.info(f"Rows with non-null file_id: {self.stats['total_with_file_id']}")
            logger.info(f"Total processed: {self.stats['total_processed']}")
            logger.info(f"Downloaded files: {self.stats['downloaded']}")
            logger.info(f"Failed downloads: {self.stats['failed']}")
            logger.info(f"Skipped (no mapping): {self.stats['skipped_no_mapping']}")
            logger.info(f"Skipped (NULL file_id in loop): {self.stats['skipped_null_file_id']}")
            logger.info(f"Skipped (already downloaded in DB): {self.stats['skipped_already_downloaded']}")
            logger.info(f"Skipped (exists on disk): {self.stats['skipped_exists_on_disk']}")
            if self.stats['resumed_from']:
                logger.info(f"Resumed from file_id: {self.stats['resumed_from']}")
            logger.info("=" * 80)

            return True

        except KeyboardInterrupt:
            logger.warning("⚠️  Files Media Server ETL interrupted by user")
            return False
        except Exception as exc:
            logger.error(f"❌ Files Media Server ETL failed with error: {exc}")
            return False
        finally:
            self.close_db()


def main() -> None:
    parser = argparse.ArgumentParser(description="DOPAMAS ETL - Files Media Server Downloader")
    parser.add_argument("--repair", action="store_true", help="Repair mode: Check all files, including those marked as downloaded")
    parser.add_argument("--skip-sync", action="store_true", help="Skip sync validation after download")
    args = parser.parse_args()
    
    etl = FilesMediaServerETL(repair=args.repair)
    success = etl.run()
    
    # Run sync validation after download completes
    if success and not args.skip_sync:
        logger = logging.getLogger("etl-files-media-server")
        logger.info("\n" + "=" * 80)
        logger.info("🔄 STARTING FILES SYNC VALIDATION")
        logger.info("=" * 80)
        
        # Import and run sync to verify downloaded files exist on disk
        try:
            # Add parent directory to path to import sync script
            sys.path.insert(0, str(PARENT_DIR))
            from sync_files_state import run_sync
            sync_success = run_sync(logger=logger)
            if sync_success:
                logger.info("✅ Sync validation completed successfully")
            else:
                logger.warning("⚠️  Sync validation failed")
        except Exception as e:
            logger.error(f"⚠️  Sync validation encountered error: {e}")
            logger.warning("Downloader completed, but sync check failed. Check manually if needed.")
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()  
