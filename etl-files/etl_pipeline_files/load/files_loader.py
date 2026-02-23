"""
Load file records into files table with idempotency
"""
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
from typing import List, Dict, Any
import logging


class FilesLoader:
    """Load file records into database"""
    
    def __init__(self, connection, idempotency_checker, logger=None):
        """
        Initialize loader.
        
        Args:
            connection: Database connection
            idempotency_checker: IdempotencyChecker instance
            logger: Logger instance
        """
        self.connection = connection
        self.idempotency_checker = idempotency_checker
        self.logger = logger or logging.getLogger(__name__)
    
    def load_files(self, file_records: List[Dict[str, Any]], skip_existing: bool = True) -> Dict[str, int]:
        """
        Load file records into database.
        
        Args:
            file_records: List of file records to load
            skip_existing: If True, skip records that already exist
        
        Returns:
            dict: Statistics (inserted, skipped, errors)
        """
        if not file_records:
            return {'inserted': 0, 'skipped': 0, 'errors': 0}
        
        stats = {'inserted': 0, 'skipped': 0, 'errors': 0, 'no_api_date_count': 0}
        records_to_insert = []
        seen_records = set()  # Track seen records to avoid duplicates within the batch
        
        # Filter records based on idempotency and deduplicate within batch
        for record in file_records:
            # Create a unique key for deduplication
            record_key = (
                record['source_type'],
                record['source_field'],
                record['parent_id'],
                record.get('file_id'),
                record.get('file_index')
            )
            
            # Skip if we've already seen this record in this batch
            if record_key in seen_records:
                stats['skipped'] += 1
                continue
            
            seen_records.add(record_key)
            
            if skip_existing:
                # Check if already processed
                is_processed = self.idempotency_checker.is_processed(
                    source_type=record['source_type'],
                    source_field=record['source_field'],
                    parent_id=record['parent_id'],
                    file_id=record.get('file_id'),
                    file_index=record.get('file_index')
                )
                
                if is_processed:
                    # Check if the existing record needs created_at to be updated
                    # If created_at is NULL, we should still try to update it
                    # Let the record go through - ON CONFLICT DO UPDATE will handle it
                    # This allows updating created_at for existing records that have NULL created_at
                    # Note: We could check if created_at is NULL here, but it's simpler to let
                    # ON CONFLICT handle it - it will only update if created_at IS NULL
                    pass  # Don't skip - let ON CONFLICT DO UPDATE handle updating created_at
            
            records_to_insert.append(record)
        
        if not records_to_insert:
            self.logger.info(f"All {len(file_records)} records already processed, skipping")
            return stats
        
        # Insert records
        try:
            with self.connection.cursor() as cursor:
                # Check if created_at column exists, if so include it
                cursor.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = 'files' AND column_name = 'created_at'
                """)
                has_created_at = cursor.fetchone() is not None
                
                if has_created_at:
                    insert_query = """
                        INSERT INTO files (
                            source_type, source_field, parent_id, file_id,
                            file_index, identity_type, identity_number, created_at
                        )
                        VALUES %s
                        ON CONFLICT (source_type, source_field, parent_id, file_id, file_index)
                        DO UPDATE SET 
                            created_at = COALESCE(files.created_at, EXCLUDED.created_at)
                        WHERE files.created_at IS NULL
                        -- Only update created_at if existing value is NULL
                        -- This preserves existing API dates and only fills in missing ones
                        -- NOTE: We explicitly set created_at in VALUES, so DEFAULT CURRENT_TIMESTAMP won't override it
                    """
                    from datetime import datetime
                    values = []
                    for r in records_to_insert:
                        # Use API date if provided, otherwise use current timestamp as fallback
                        api_date = r.get('api_date')
                        
                        # Log first few records without api_date to debug
                        if not api_date and stats.get('no_api_date_count', 0) < 10:
                            self.logger.warning(
                                f"⚠️ No api_date provided for record: "
                                f"source_type={r.get('source_type')}, "
                                f"parent_id={r.get('parent_id')}, "
                                f"file_id={r.get('file_id')} - "
                                f"Will use current time. Check extractor!"
                            )
                            stats['no_api_date_count'] = stats.get('no_api_date_count', 0) + 1
                        
                        if api_date:
                            # If it's already a datetime object, use it; otherwise parse it
                            if isinstance(api_date, datetime):
                                created_at = api_date
                            elif isinstance(api_date, str):
                                # Try to parse the date string
                                try:
                                    # Try ISO format first (with timezone) - e.g., "2024-11-30T14:12:21.233Z"
                                    if 'T' in api_date or 'Z' in api_date:
                                        # Simple approach: remove Z, remove milliseconds, parse
                                        # Format: "2024-11-30T14:12:21.233Z" -> "2024-11-30T14:12:21"
                                        date_str = api_date.replace('Z', '')
                                        # Remove milliseconds (everything after .)
                                        if '.' in date_str:
                                            date_str = date_str.split('.')[0]
                                        
                                        # Parse: "2024-11-30T14:12:21"
                                        if 'T' in date_str:
                                            date_part, time_part = date_str.split('T', 1)
                                            created_at = datetime.strptime(f"{date_part} {time_part}", '%Y-%m-%d %H:%M:%S')
                                        else:
                                            created_at = datetime.strptime(date_str, '%Y-%m-%d')
                                    # Try YYYY-MM-DD format
                                    elif len(api_date) == 10 and api_date.count('-') == 2:
                                        created_at = datetime.strptime(api_date, '%Y-%m-%d')
                                    # Try other common formats
                                    elif len(api_date) >= 19:
                                        # Try datetime format: YYYY-MM-DD HH:MM:SS
                                        created_at = datetime.strptime(api_date[:19], '%Y-%m-%d %H:%M:%S')
                                    else:
                                        raise ValueError(f"Unknown date format: {api_date}")
                                except Exception as e:
                                    # Fallback to current time if parsing fails
                                    self.logger.warning(f"⚠️ Could not parse api_date '{api_date}' (type: {type(api_date)}): {e}, using current time for source_type={r.get('source_type')}, parent_id={r.get('parent_id')}")
                                    created_at = datetime.now()
                            else:
                                self.logger.warning(f"⚠️ api_date is not string or datetime: {type(api_date)}, using current time for source_type={r.get('source_type')}")
                                created_at = datetime.now()
                        else:
                            # No API date provided - use current timestamp as fallback
                            # This should only happen if api_date wasn't extracted from API
                            # Log first few occurrences to understand the pattern
                            if stats.get('no_api_date_count', 0) < 5:
                                self.logger.warning(f"⚠️ No api_date provided for record (source_type={r.get('source_type')}, parent_id={r.get('parent_id')}), using current time")
                                stats['no_api_date_count'] = stats.get('no_api_date_count', 0) + 1
                            created_at = datetime.now()
                        
                        values.append((
                            r['source_type'],
                            r['source_field'],
                            r['parent_id'],
                            r.get('file_id'),
                            r.get('file_index'),
                            r.get('identity_type'),
                            r.get('identity_number'),
                            created_at
                        ))
                else:
                    # Fallback: don't include created_at if column doesn't exist
                    insert_query = """
                        INSERT INTO files (
                            source_type, source_field, parent_id, file_id,
                            file_index, identity_type, identity_number
                        )
                        VALUES %s
                        ON CONFLICT (source_type, source_field, parent_id, file_id, file_index)
                        DO NOTHING
                    """
                    values = [
                        (
                            r['source_type'],
                            r['source_field'],
                            r['parent_id'],
                            r.get('file_id'),
                            r.get('file_index'),
                            r.get('identity_type'),
                            r.get('identity_number')
                        )
                        for r in records_to_insert
                    ]
                
                execute_values(cursor, insert_query, values)
                self.connection.commit()
                
                stats['inserted'] = len(records_to_insert)
                self.logger.info(f"Inserted {stats['inserted']} file records")
        
        except Exception as e:
            self.connection.rollback()
            stats['errors'] = len(records_to_insert)
            self.logger.error(f"Error inserting file records: {e}")
            raise
        
        return stats


