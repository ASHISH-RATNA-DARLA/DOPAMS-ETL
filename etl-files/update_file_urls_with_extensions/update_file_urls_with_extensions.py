#!/usr/bin/env python3
"""
Update file_url values in files table with actual file extensions.

This script:
1. Reads file_path from the files table
2. Checks if the file exists on the filesystem
3. Determines the actual file extension
4. Updates file_url with the extension
5. Processes all source types: crime, person, property, interrogation, mo_seizures, chargesheets, case_property

IMPORTANT: The trigger auto_generate_file_paths is temporarily disabled
to prevent it from overwriting the extensions.
"""

import os
import sys
import glob
import logging
from pathlib import Path
from typing import Optional, Tuple
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import colorlog

# Load environment variables
load_dotenv()

# Database Configuration
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST'),
    'database': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'port': int(os.getenv('POSTGRES_PORT'))
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
    
    Args:
        file_id: The file UUID
        subdir: Subdirectory relative to BASE_MEDIA_PATH
    
    Returns:
        Tuple of (full_path, extension) if found, None otherwise
    """
    if not file_id or not subdir:
        logger.debug(f"Invalid parameters: file_id={file_id}, subdir={subdir}")
        return None
    
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
        all_files = os.listdir(dir_path)
        for f in all_files:
            if f.lower().startswith(file_id.lower()):
                matches = [os.path.join(dir_path, f)]
                logger.debug(f"Found file with case-insensitive match: {f}")
                break
    
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


def update_file_url_with_extension(cursor, record_id: str, file_url: str, extension: str) -> bool:
    """
    Update file_url in database with extension.
    
    Args:
        cursor: Database cursor
        record_id: UUID of the files table record
        file_url: Current file_url (without extension)
        extension: File extension to add (e.g., '.pdf')
    
    Returns:
        True if update was successful
    """
    try:
        # Add extension to URL (before any query parameters if they exist)
        if '?' in file_url:
            # If URL has query parameters, insert extension before '?'
            base_url, query = file_url.split('?', 1)
            new_url = f"{base_url}{extension}?{query}"
        else:
            new_url = f"{file_url}{extension}"
        
        update_query = """
            UPDATE files
            SET file_url = %s
            WHERE id = %s
        """
        
        cursor.execute(update_query, (new_url, record_id))
        return True
    
    except Exception as e:
        logger.error(f"Error updating record {record_id}: {e}")
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


def process_source_type(connection, source_type: str) -> dict:
    """
    Process all records for a given source_type.
    
    Returns:
        dict with statistics
    """
    stats = {
        'total': 0,
        'found': 0,
        'updated': 0,
        'skipped': 0,
        'errors': 0
    }
    
    logger.info(f"\n{'='*60}")
    logger.info(f"Processing: {source_type.upper()}")
    logger.info(f"{'='*60}")
    
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
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
            
            # Log unique source_field values and their counts for debugging
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
            
            # Process each record
            for record in records:
                record_id = record['id']
                file_id = str(record['file_id'])
                source_field = record['source_field']
                file_url = record['file_url']
                
                # Map source_type and source_field to subdirectory (more reliable than parsing file_path)
                subdir = map_destination_subdir(source_type, source_field)
                
                if not subdir:
                    logger.warning(f"Could not map (source_type={source_type}, source_field={source_field}) for record {record_id} (file_id: {file_id})")
                    stats['skipped'] += 1
                    continue
                
                # Find file with extension
                result = find_file_with_extension(file_id, subdir)
                
                if not result:
                    # File not found - this is expected if files are still downloading
                    # Skip this record and it will be processed in a future run
                    full_search_path = os.path.join(BASE_MEDIA_PATH, subdir)
                    logger.warning(f"File not found on disk: file_id={file_id}, expected_dir={full_search_path}, file_path={record.get('file_path', 'N/A')}")
                    stats['skipped'] += 1
                    continue
                
                file_full_path, extension = result
                stats['found'] += 1
                
                # Check if URL already has this extension
                # Handle query parameters: check if extension exists before '?' if present
                url_without_query = file_url.split('?')[0] if '?' in file_url else file_url
                if url_without_query.endswith(extension):
                    logger.debug(f"URL already has extension {extension}: {file_url}")
                    stats['skipped'] += 1
                    continue
                
                # Update file_url with extension
                if update_file_url_with_extension(cursor, record_id, file_url, extension):
                    logger.info(f"Updated: {file_id} -> {file_url}{extension}")
                    stats['updated'] += 1
                else:
                    stats['errors'] += 1
            
            # Commit after processing all records for this source_type
            connection.commit()
            logger.info(f"Committed {stats['updated']} updates for {source_type}")
    
    except Exception as e:
        connection.rollback()
        logger.error(f"Error processing {source_type}: {e}")
        stats['errors'] += stats['total'] - stats['updated']
        raise
    
    return stats


def main():
    """Main execution function."""
    logger.info("="*60)
    logger.info("Starting file_url extension update process")
    logger.info("="*60)
    logger.info(f"Base media path: {BASE_MEDIA_PATH}")
    logger.info(f"Base file URL: {BASE_FILE_URL}")
    logger.info("")
    logger.info("IMPORTANT: After this script completes, you MUST run")
    logger.info("           migrate_trigger_preserve_extensions.sql to modify")
    logger.info("           the trigger to preserve extensions, otherwise")
    logger.info("           future updates will overwrite the extensions!")
    logger.info("="*60)
    
    # Verify base media path exists
    if not os.path.isdir(BASE_MEDIA_PATH):
        logger.error(f"Base media path does not exist: {BASE_MEDIA_PATH}")
        sys.exit(1)
    
    # Connect to database
    try:
        connection = psycopg2.connect(**DB_CONFIG)
        logger.info("✓ Connected to database")
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        sys.exit(1)
    
    try:
        # Disable the trigger to prevent it from overwriting our updates
        logger.info("\nDisabling trigger: trigger_auto_generate_file_paths")
        with connection.cursor() as cursor:
            cursor.execute("ALTER TABLE files DISABLE TRIGGER trigger_auto_generate_file_paths")
            connection.commit()
            logger.info("✓ Trigger disabled")
        
        # Process each source type in order
        total_stats = {
            'total': 0,
            'found': 0,
            'updated': 0,
            'skipped': 0,
            'errors': 0
        }
        
        for source_type in PROCESSING_ORDER:
            stats = process_source_type(connection, source_type)
            
            # Accumulate statistics
            for key in total_stats:
                total_stats[key] += stats[key]
        
        # Re-enable the trigger
        logger.info("\nRe-enabling trigger: trigger_auto_generate_file_paths")
        logger.warning("⚠️  WARNING: The trigger will overwrite extensions on future updates!")
        logger.warning("⚠️  You MUST run migrate_trigger_preserve_extensions.sql to prevent this.")
        
        with connection.cursor() as cursor:
            cursor.execute("ALTER TABLE files ENABLE TRIGGER trigger_auto_generate_file_paths")
            connection.commit()
            logger.info("✓ Trigger re-enabled")
            logger.warning("⚠️  IMPORTANT: Run migrate_trigger_preserve_extensions.sql NOW to")
            logger.warning("    modify the trigger to preserve extensions!")
        
        # Print summary
        logger.info("\n" + "="*60)
        logger.info("SUMMARY")
        logger.info("="*60)
        logger.info(f"Total records processed: {total_stats['total']}")
        logger.info(f"Files found on disk: {total_stats['found']}")
        logger.info(f"URLs updated: {total_stats['updated']}")
        logger.info(f"Skipped: {total_stats['skipped']} (files not found or already have extension)")
        logger.info(f"Errors: {total_stats['errors']}")
        logger.info("="*60)
        logger.info("NOTE: Skipped records may be files still downloading.")
        logger.info("      Re-run this script later to process them.")
        logger.info("="*60)
    
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        # Try to re-enable trigger even on error
        try:
            with connection.cursor() as cursor:
                cursor.execute("ALTER TABLE files ENABLE TRIGGER trigger_auto_generate_file_paths")
                connection.commit()
                logger.info("✓ Trigger re-enabled after error")
        except:
            logger.error("Failed to re-enable trigger - manual intervention required!")
        sys.exit(1)
    
    finally:
        connection.close()
        logger.info("Database connection closed")


if __name__ == "__main__":
    main()

