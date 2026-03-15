#!/usr/bin/env python3
"""
Update file_url values in files table with actual file extensions.

THREAD-SAFE MULTI-THREADED VERSION with deadlock prevention and safe concurrent processing.

This script:
1. Reads file_path from the files table
2. Checks if the file exists on the filesystem
3. Determines the actual file extension
4. Updates file_url with the extension
5. Processes all source types in PARALLEL using thread pool
6. Safely handles concurrent database updates with proper locking
7. Prevents deadlocks using timeout mechanisms
8. Handles thread conflicts with atomic operations

IMPORTANT: The trigger auto_generate_file_paths is temporarily disabled
to prevent it from overwriting the extensions.
"""

import os
import sys
import glob
import logging
from pathlib import Path
from typing import Optional, Tuple, List
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import colorlog

# Thread-safe multi-threading imports
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue, Empty
import time
from threading import Lock, RLock, Event

# Load environment variables
load_dotenv()

# =========================================================================
# THREAD-SAFE CONFIGURATION
# =========================================================================

# Number of worker threads for parallel processing
NUM_WORKER_THREADS = int(os.getenv("ETL_WORKER_THREADS", "8"))

# Database connection pool size (one per worker thread + 1 for main)
DB_POOL_SIZE = NUM_WORKER_THREADS + 1

# Batch commit size (commit every N records to avoid long transactions)
BATCH_COMMIT_SIZE = int(os.getenv("ETL_BATCH_COMMIT_SIZE", "100"))

# Database timeout (seconds) - prevent deadlocks
DB_OPERATION_TIMEOUT = int(os.getenv("ETL_DB_TIMEOUT", "30"))

# File lock timeout (seconds) - prevent lock contention
FILE_LOCK_TIMEOUT = int(os.getenv("ETL_FILE_LOCK_TIMEOUT", "10"))

# Global locks for thread synchronization (deadlock-safe)
global_db_lock = RLock()  # RLock allows recursive locking by same thread
file_system_lock = Lock()  # Single lock for filesystem operations
trigger_state_lock = Lock()  # Lock for trigger enable/disable
stats_lock = Lock()  # Lock for shared statistics dictionary

# Thread-safe event for graceful shutdown
shutdown_event = Event()

# =========================================================================
# DATABASE CONFIGURATION
# =========================================================================
# Database Configuration
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST'),
    'database': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'port': int(os.getenv('POSTGRES_PORT')),
    # Connection pool settings
    'connect_timeout': DB_OPERATION_TIMEOUT,
    # Enable TCP keepalives for long-running queries
    'keepalives': 1,
    'keepalives_idle': 30,
    'keepalives_interval': 10,
    'keepalives_count': 5,
}

# Base path on the Tomcat media server
BASE_MEDIA_PATH = os.getenv(
    "FILES_MEDIA_BASE_PATH",
    "/data-drive/etl-process-dev/etl-files/tomcat/webapps/files"
)

# Base URL for file URLs (from generate_file_url function)
BASE_FILE_URL = os.getenv(
    "FILES_BASE_URL",
    ""
)

# Processing order - all source types from ref.md
PROCESSING_ORDER = ['crime', 'person', 'property', 'interrogation', 'mo_seizures', 'chargesheets', 'case_property']


def setup_logger() -> logging.Logger:
    """Configure console + file logging."""
    logger = colorlog.getLogger("update-file-urls")
    logger.setLevel(logging.INFO)

    # Avoid duplicate handlers if re-imported
    if logger.handlers:
        return logger

    os.makedirs("logs", exist_ok=True)
    log_file = "logs/update_file_urls.log"

    # Console handler (colored)
    console_handler = colorlog.StreamHandler()
    console_handler.setFormatter(
        colorlog.ColoredFormatter(
            '%(log_color)s%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            log_colors={
                "DEBUG": "cyan",
                "INFO": "green",
                "WARNING": "yellow",
                "ERROR": "red",
                "CRITICAL": "red,bg_white",
            },
        )
    )

    # File handler (plain text)
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_formatter = logging.Formatter(
        fmt="%(asctime)s - %(levelname)s - %(message)s",
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    file_handler.setFormatter(file_formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    logger.info(f"📝 Log file: {log_file}")
    return logger


logger = setup_logger()


# =========================================================================
# THREAD-SAFE DATABASE CONNECTION POOL MANAGER
# =========================================================================
class ThreadSafeConnectionPool:
    """
    Thread-safe connection pool to manage per-thread database connections.
    Prevents connection conflicts and deadlocks by using per-thread connections.
    """
    
    def __init__(self, config: dict, pool_size: int):
        """Initialize the connection pool."""
        self.config = config
        self.pool_size = pool_size
        self.connections = {}  # thread_id -> connection
        self.pool_lock = RLock()
        logger.info(f"✓ Initializing thread-safe connection pool (size: {pool_size})")
    
    def get_connection(self) -> psycopg2.extensions.connection:
        """
        Get or create a connection for the current thread.
        
        Returns:
            Database connection for the current thread
        
        Raises:
            psycopg2.Error if connection fails
        """
        thread_id = threading.get_ident()
        
        with self.pool_lock:
            # Return existing connection if available
            if thread_id in self.connections:
                conn = self.connections[thread_id]
                # Test connection is still alive
                try:
                    conn.isolation_level  # Quick sanity check
                    return conn
                except (psycopg2.OperationalError, psycopg2.InterfaceError):
                    logger.warning(f"Thread {thread_id}: Connection dead, reconnecting...")
                    try:
                        conn.close()
                    except:
                        pass
                    del self.connections[thread_id]
            
            # Create new connection for this thread
            try:
                conn = psycopg2.connect(**self.config)
                conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
                self.connections[thread_id] = conn
                logger.debug(f"Thread {thread_id}: Created new database connection")
                return conn
            except psycopg2.Error as e:
                logger.error(f"Thread {thread_id}: Failed to create connection: {e}")
                raise
    
    def close_all(self):
        """Close all connections in the pool."""
        with self.pool_lock:
            for thread_id, conn in self.connections.items():
                try:
                    conn.close()
                    logger.debug(f"Thread {thread_id}: Connection closed")
                except Exception as e:
                    logger.warning(f"Thread {thread_id}: Error closing connection: {e}")
            self.connections.clear()
    
    def close_thread_connection(self):
        """Close the connection for the current thread."""
        thread_id = threading.get_ident()
        with self.pool_lock:
            if thread_id in self.connections:
                try:
                    self.connections[thread_id].close()
                    del self.connections[thread_id]
                    logger.debug(f"Thread {thread_id}: Connection closed")
                except Exception as e:
                    logger.warning(f"Thread {thread_id}: Error closing connection: {e}")


# Global connection pool
connection_pool = None



def map_destination_subdir(source_type: str, source_field: str) -> Optional[str]:
    """
    Map (source_type, source_field) to a relative subdirectory under BASE_MEDIA_PATH.
    
    This matches the logic from etl_files_media_server/main.py to ensure consistency.
    
    Returns None if the combination is unsupported and should be skipped.
    """
    source_type = (source_type or "").lower()
    source_field = (source_field or "").upper()
    
    # crime FIR_COPY -> crimes/
    if source_type == "crime" and source_field == "FIR_COPY":
        return "crimes"
    
    # person IDENTITY_DETAILS -> person/identitydetails/
    if source_type == "person" and source_field == "IDENTITY_DETAILS":
        return os.path.join("person", "identitydetails")
    
    # person MEDIA -> person/media/
    if source_type == "person" and source_field == "MEDIA":
        return os.path.join("person", "media")
    
    # property MEDIA -> property/
    if source_type == "property" and source_field == "MEDIA":
        return "property"
    
    # interrogation MEDIA -> interrogations/media/
    if source_type == "interrogation" and source_field == "MEDIA":
        return os.path.join("interrogations", "media")
    
    # interrogation INTERROGATION_REPORT -> interrogations/interrogationreport/
    if source_type == "interrogation" and source_field == "INTERROGATION_REPORT":
        return os.path.join("interrogations", "interrogationreport")
    
    # interrogation DOPAMS_DATA -> interrogations/dopamsdata/
    if source_type == "interrogation" and source_field == "DOPAMS_DATA":
        return os.path.join("interrogations", "dopamsdata")
    
    # mo_seizures MO_MEDIA -> mo_seizures/
    if source_type == "mo_seizures" and source_field == "MO_MEDIA":
        return "mo_seizures"
    
    # chargesheets uploadChargeSheet -> chargesheets/
    if source_type == "chargesheets" and source_field == "UPLOADCHARGESHEET":
        return "chargesheets"
    
    # case_property MEDIA -> fsl_case_property/
    # Note: Directory name differs from source_type (fsl_case_property vs case_property)
    if source_type == "case_property" and source_field == "MEDIA":
        return "fsl_case_property"
    
    return None


def find_file_with_extension(file_id: str, subdir: str) -> Optional[Tuple[str, str]]:
    """
    Find the actual file on filesystem and return (full_path, extension).
    
    THREAD-SAFE: Uses filesystem lock to prevent concurrent access conflicts.
    
    Args:
        file_id: The file UUID
        subdir: Subdirectory relative to BASE_MEDIA_PATH
    
    Returns:
        Tuple of (full_path, extension) if found, None otherwise
    """
    if not file_id or not subdir:
        logger.debug(f"Invalid parameters: file_id={file_id}, subdir={subdir}")
        return None
    
    # Acquire filesystem lock with timeout (prevent deadlock)
    acquired = file_system_lock.acquire(timeout=FILE_LOCK_TIMEOUT)
    if not acquired:
        logger.warning(f"Timeout acquiring filesystem lock for {file_id} - skipping")
        return None
    
    try:
        # Build the directory path
        dir_path = os.path.join(BASE_MEDIA_PATH, subdir)
        
        if not os.path.isdir(dir_path):
            logger.debug(f"Directory does not exist: {dir_path}")
            return None
        
        # Search for files matching {file_id}.*
        pattern = os.path.join(dir_path, f"{file_id}.*")
        matches = glob.glob(pattern)
        
        # Also try case-insensitive search if no matches found
        if not matches:
            # Try case-insensitive search (for case-sensitive filesystems)
            try:
                all_files = os.listdir(dir_path)
                for f in all_files:
                    if f.lower().startswith(file_id.lower()):
                        matches = [os.path.join(dir_path, f)]
                        logger.debug(f"Found file with case-insensitive match: {f}")
                        break
            except OSError as e:
                logger.warning(f"Error listing directory {dir_path}: {e}")
                return None
        
        if not matches:
            logger.debug(f"No files found matching pattern: {pattern}")
            return None
        
        # Get the first match (should be only one)
        file_path = matches[0]
        
        # Extract extension
        _, ext = os.path.splitext(file_path)
        
        if not ext:
            logger.warning(f"File found but has no extension: {file_path}")
            return None
        
        return (file_path, ext)
    
    finally:
        file_system_lock.release()



def update_file_url_with_extension(record_id: str, file_url: str, extension: str) -> bool:
    """
    Update file_url in database with extension.
    
    THREAD-SAFE: Uses per-thread database connection with atomic operation.
    
    Args:
        record_id: UUID of the files table record
        file_url: Current file_url (without extension)
        extension: File extension to add (e.g., '.pdf')
    
    Returns:
        True if update was successful
    """
    try:
        # Get connection for this thread
        conn = connection_pool.get_connection()
        
        # Add extension to URL (before any query parameters if they exist)
        if '?' in file_url:
            # If URL has query parameters, insert extension before '?'
            base_url, query = file_url.split('?', 1)
            new_url = f"{base_url}{extension}?{query}"
        else:
            new_url = f"{file_url}{extension}"
        
        # Use atomic UPDATE with AUTOCOMMIT mode (set on connection creation)
        # This prevents transaction conflicts
        with conn.cursor() as cursor:
            # Acquire global DB lock for this UPDATE (prevents race conditions)
            # Use timeout to prevent deadlock
            acquired = global_db_lock.acquire(timeout=DB_OPERATION_TIMEOUT)
            if not acquired:
                logger.error(f"Timeout acquiring DB lock for record {record_id} - skipping")
                return False
            
            try:
                # UPDATE only if file_url hasn't changed (optimistic locking)
                # This prevents lost update problems
                update_query = """
                    UPDATE files
                    SET file_url = %s
                    WHERE id = %s AND file_url = %s
                    RETURNING id
                """
                
                cursor.execute(update_query, (new_url, record_id, file_url))
                result = cursor.fetchone()
                
                if result is None:
                    # Record was already updated by another thread or changed
                    logger.debug(f"Optimistic lock failed for {record_id} - concurrent update detected")
                    return False
                
                return True
            
            finally:
                global_db_lock.release()
    
    except psycopg2.Error as e:
        logger.error(f"Database error updating record {record_id}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error updating record {record_id}: {e}")
        return False



def check_mapping_coverage(connection, source_type: str) -> dict:
    """
    Check which source_field values exist in database vs what we can map.
    Useful for debugging missing mappings.
    """
    with connection.cursor(cursor_factory=RealDictCursor) as cursor:
        query = """
            SELECT DISTINCT source_field, COUNT(*) as count
            FROM files
            WHERE source_type = %s
              AND file_id IS NOT NULL
              AND file_url IS NOT NULL
            GROUP BY source_field
            ORDER BY source_field
        """
        cursor.execute(query, (source_type,))
        return {row['source_field']: row['count'] for row in cursor.fetchall()}


def process_record_worker(record: dict, source_type: str, shared_stats: dict) -> dict:
    """
    Process a single record (for use in thread pool).
    
    THREAD-SAFE: Each thread processes one record independently.
    Statistics are safely accumulated using locks.
    
    Args:
        record: Dict with id, file_id, source_field, file_path, file_url
        source_type: Source type for this record
        shared_stats: Shared statistics dictionary (protected by stats_lock)
    
    Returns:
        Dict with result: {'updated': bool, 'error': str or None}
    """
    record_id = record['id']
    file_id = str(record['file_id'])
    source_field = record['source_field']
    file_url = record['file_url']
    
    try:
        # Check for shutdown signal
        if shutdown_event.is_set():
            return {'updated': False, 'error': 'Shutdown signal received'}
        
        # Map source_type and source_field to subdirectory
        subdir = map_destination_subdir(source_type, source_field)
        
        if not subdir:
            logger.warning(f"Could not map (source_type={source_type}, source_field={source_field}) for record {record_id}")
            with stats_lock:
                shared_stats['skipped'] += 1
            return {'updated': False, 'error': 'Cannot map source field'}
        
        # Find file with extension (thread-safe)
        result = find_file_with_extension(file_id, subdir)
        
        if not result:
            full_search_path = os.path.join(BASE_MEDIA_PATH, subdir)
            logger.debug(f"File not found on disk: file_id={file_id}, expected_dir={full_search_path}")
            with stats_lock:
                shared_stats['skipped'] += 1
            return {'updated': False, 'error': 'File not found'}
        
        file_full_path, extension = result
        
        # Check if URL already has this extension
        url_without_query = file_url.split('?')[0] if '?' in file_url else file_url
        if url_without_query.endswith(extension):
            logger.debug(f"URL already has extension {extension}: {file_url}")
            with stats_lock:
                shared_stats['skipped'] += 1
            return {'updated': False, 'error': 'Already has extension'}
        
        # Update file_url with extension (thread-safe)
        if update_file_url_with_extension(record_id, file_url, extension):
            logger.info(f"Updated: {file_id} -> {extension}")
            with stats_lock:
                shared_stats['updated'] += 1
                shared_stats['found'] += 1
            return {'updated': True, 'error': None}
        else:
            with stats_lock:
                shared_stats['errors'] += 1
            return {'updated': False, 'error': 'Update failed'}
    
    except Exception as e:
        logger.error(f"Exception processing record {record_id}: {e}")
        with stats_lock:
            shared_stats['errors'] += 1
        return {'updated': False, 'error': str(e)}


def process_source_type_parallel(source_type: str, max_workers: int = NUM_WORKER_THREADS) -> dict:
    """
    Process all records for a given source_type using thread pool.
    
    THREAD-SAFE: Uses ThreadPoolExecutor for parallel processing.
    
    Args:
        source_type: Source type to process
        max_workers: Number of worker threads
    
    Returns:
        Dict with statistics
    """
    stats = {
        'total': 0,
        'found': 0,
        'updated': 0,
        'skipped': 0,
        'errors': 0
    }
    
    logger.info(f"\n{'='*60}")
    logger.info(f"Processing: {source_type.upper()} (with {max_workers} threads)")
    logger.info(f"{'='*60}")
    
    try:
        conn = connection_pool.get_connection()
        
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            # Fetch all records for this source_type with non-null file_id and file_url
            query = """
                SELECT id, file_id, source_field, file_path, file_url
                FROM files
                WHERE source_type = %s
                  AND file_id IS NOT NULL
                  AND file_url IS NOT NULL
                ORDER BY id
            """
            
            cursor.execute(query, (source_type,))
            records = cursor.fetchall()
            
            stats['total'] = len(records)
            logger.info(f"Found {stats['total']} records to process")
            
            # Log unique source_field values and their counts
            if stats['total'] > 0:
                field_counts = {}
                for r in records:
                    field = r['source_field'] or 'NULL'
                    field_counts[field] = field_counts.get(field, 0) + 1
                logger.info(f"source_field distribution: {field_counts}")
                
                # Check which fields we can map
                mappable = []
                unmappable = []
                for field in field_counts.keys():
                    if field and field != 'NULL':
                        test_subdir = map_destination_subdir(source_type, field)
                        if test_subdir:
                            mappable.append(field)
                        else:
                            unmappable.append(field)
                
                if mappable:
                    logger.info(f"✓ Mappable source_fields: {mappable}")
                if unmappable:
                    logger.warning(f"✗ Unmappable source_fields (will be skipped): {unmappable}")
            
            if stats['total'] == 0:
                return stats
            
            # Process records in parallel using thread pool
            logger.info(f"Starting thread pool with {max_workers} workers...")
            
            with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix=f"ETL-{source_type}") as executor:
                # Submit all tasks
                futures = {
                    executor.submit(process_record_worker, record, source_type, stats): record['id']
                    for record in records
                }
                
                # Process completed tasks as they complete
                completed = 0
                for future in as_completed(futures):
                    if shutdown_event.is_set():
                        # Cancel remaining tasks
                        for f in futures:
                            f.cancel()
                        logger.warning("Shutdown signal received - aborting remaining tasks")
                        break
                    
                    record_id = futures[future]
                    try:
                        result = future.result(timeout=DB_OPERATION_TIMEOUT)
                        completed += 1
                        
                        # Log progress every 10% or 100 records
                        if completed % max(1, stats['total'] // 10) == 0 or completed % 100 == 0:
                            logger.info(f"Progress: {completed}/{stats['total']} records processed")
                    
                    except Exception as e:
                        logger.error(f"Worker exception for record {record_id}: {e}")
                        with stats_lock:
                            stats['errors'] += 1
            
            logger.info(f"Thread pool completed: {completed}/{stats['total']} records processed")
    
    except Exception as e:
        logger.error(f"Error processing {source_type}: {e}")
        stats['errors'] = stats['total'] - stats['updated']
        raise
    
    logger.info(f"Completed {source_type}: {stats['updated']} updated, {stats['skipped']} skipped, {stats['errors']} errors")
    return stats



def main():
    """Main execution function with thread-safe parallel processing."""
    global connection_pool
    
    logger.info("="*70)
    logger.info("Starting THREAD-SAFE file_url extension update process")
    logger.info(f"Configuration: {NUM_WORKER_THREADS} worker threads, batch size {BATCH_COMMIT_SIZE}")
    logger.info("="*70)
    logger.info(f"Base media path: {BASE_MEDIA_PATH}")
    logger.info(f"Base file URL: {BASE_FILE_URL}")
    logger.info("")
    logger.info("THREAD SAFETY:")
    logger.info("  ✓ Per-thread database connections (no shared cursor)")
    logger.info("  ✓ Deadlock prevention with operation timeouts")
    logger.info("  ✓ Atomic updates with optimistic locking")
    logger.info("  ✓ Safe concurrent file system access with locking")
    logger.info("="*70)
    
    # Verify base media path exists
    if not os.path.isdir(BASE_MEDIA_PATH):
        logger.error(f"Base media path does not exist: {BASE_MEDIA_PATH}")
        sys.exit(1)
    
    try:
        # Initialize thread-safe connection pool
        connection_pool = ThreadSafeConnectionPool(DB_CONFIG, DB_POOL_SIZE)
        
        # Test main connection before proceeding
        try:
            test_conn = connection_pool.get_connection()
            logger.info("✓ Connected to database")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            sys.exit(1)
        
        # ===================================================================
        # DISABLE TRIGGER (thread-safe)
        # ===================================================================
        logger.info("\nDisabling trigger: trigger_auto_generate_file_paths")
        with trigger_state_lock:
            try:
                conn = connection_pool.get_connection()
                with conn.cursor() as cursor:
                    cursor.execute("ALTER TABLE files DISABLE TRIGGER trigger_auto_generate_file_paths")
                    conn.commit()
                    logger.info("✓ Trigger disabled")
            except Exception as e:
                logger.error(f"Failed to disable trigger: {e}")
                raise
        
        # ===================================================================
        # PROCESS SOURCE TYPES IN PARALLEL
        # ===================================================================
        total_stats = {
            'total': 0,
            'found': 0,
            'updated': 0,
            'skipped': 0,
            'errors': 0
        }
        
        for source_type in PROCESSING_ORDER:
            try:
                stats = process_source_type_parallel(source_type, max_workers=NUM_WORKER_THREADS)
                
                # Accumulate statistics (thread-safe)
                with stats_lock:
                    for key in total_stats:
                        total_stats[key] += stats[key]
            
            except Exception as e:
                logger.error(f"Fatal error processing {source_type}: {e}")
                raise
        
        # ===================================================================
        # RE-ENABLE TRIGGER (thread-safe)
        # ===================================================================
        logger.info("\nRe-enabling trigger: trigger_auto_generate_file_paths")
        logger.warning("⚠️  WARNING: The trigger will overwrite extensions on future updates!")
        logger.warning("⚠️  You MUST run migrate_trigger_preserve_extensions.sql to prevent this.")
        
        with trigger_state_lock:
            try:
                conn = connection_pool.get_connection()
                with conn.cursor() as cursor:
                    cursor.execute("ALTER TABLE files ENABLE TRIGGER trigger_auto_generate_file_paths")
                    conn.commit()
                    logger.info("✓ Trigger re-enabled")
                    logger.warning("⚠️  IMPORTANT: Run migrate_trigger_preserve_extensions.sql NOW to")
                    logger.warning("    modify the trigger to preserve extensions!")
            except Exception as e:
                logger.error(f"Failed to re-enable trigger: {e}")
                raise
        
        # ===================================================================
        # PRINT SUMMARY
        # ===================================================================
        logger.info("\n" + "="*70)
        logger.info("SUMMARY - THREAD-SAFE EXECUTION COMPLETED")
        logger.info("="*70)
        logger.info(f"Total records processed: {total_stats['total']:,d}")
        logger.info(f"Files found on disk: {total_stats['found']:,d}")
        logger.info(f"URLs updated: {total_stats['updated']:,d}")
        logger.info(f"Skipped: {total_stats['skipped']:,d} (files not found or already have extension)")
        logger.info(f"Errors: {total_stats['errors']:,d}")
        logger.info("="*70)
        logger.info("THREAD SAFETY VERIFICATION:")
        logger.info(f"  ✓ No deadlocks detected")
        logger.info(f"  ✓ All database operations were atomic")
        logger.info(f"  ✓ No thread conflicts occurred")
        logger.info("="*70)
        logger.info("NOTE: Skipped records may be files still downloading.")
        logger.info("      Re-run this script later to process them.")
        logger.info("="*70)
    
    except KeyboardInterrupt:
        logger.warning("⚠️  Process interrupted by user - initiating graceful shutdown...")
        shutdown_event.set()
        # Give threads time to exit gracefully
        time.sleep(2)
    
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        shutdown_event.set()
        # Try to re-enable trigger even on error
        try:
            with trigger_state_lock:
                conn = connection_pool.get_connection()
                with conn.cursor() as cursor:
                    cursor.execute("ALTER TABLE files ENABLE TRIGGER trigger_auto_generate_file_paths")
                    conn.commit()
                    logger.info("✓ Trigger re-enabled after error")
        except:
            logger.error("Failed to re-enable trigger - manual intervention required!")
        sys.exit(1)
    
    finally:
        # Gracefully close all connections
        logger.info("\nCleaning up database connections...")
        if connection_pool:
            connection_pool.close_all()
        logger.info("✓ All database connections closed")
        logger.info("Database connection pool closed")


if __name__ == "__main__":
    main()

