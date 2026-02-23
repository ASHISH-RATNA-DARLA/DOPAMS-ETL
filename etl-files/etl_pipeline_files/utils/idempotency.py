"""
Idempotency checks - verify if records already processed
"""
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Set, Tuple


class IdempotencyChecker:
    """Check if records have already been processed"""
    
    def __init__(self, connection):
        """
        Initialize idempotency checker.
        
        Args:
            connection: PostgreSQL database connection
        """
        self.connection = connection
    
    def is_processed(self, source_type: str, source_field: str, parent_id: str, 
                    file_id: str = None, file_index: int = None) -> bool:
        """
        Check if a record has already been processed.
        
        Args:
            source_type: Source type (crime, person, property, interrogation)
            source_field: Source field (FIR_COPY, MEDIA, etc.)
            parent_id: Parent record ID
            file_id: File ID (optional, for null checks)
            file_index: File index (optional)
        
        Returns:
            bool: True if already processed, False otherwise
        """
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                query = """
                    SELECT COUNT(*) as count
                    FROM files
                    WHERE source_type = %s
                      AND source_field = %s
                      AND parent_id = %s
                """
                params = [source_type, source_field, parent_id]
                
                # Add file_id check if provided
                if file_id is not None:
                    query += " AND file_id = %s"
                    params.append(file_id)
                else:
                    # If file_id is None, check for NULL
                    query += " AND file_id IS NULL"
                
                # Add file_index check if provided
                if file_index is not None:
                    query += " AND file_index = %s"
                    params.append(file_index)
                else:
                    # If file_index is None, check for NULL
                    query += " AND file_index IS NULL"
                
                cursor.execute(query, params)
                result = cursor.fetchone()
                return result['count'] > 0
        
        except Exception as e:
            # On error, assume not processed (safer to reprocess than skip)
            return False
    
    def get_processed_parent_ids(self, source_type: str, source_field: str) -> Set[str]:
        """
        Get set of parent IDs that have already been processed.
        
        Args:
            source_type: Source type
            source_field: Source field
        
        Returns:
            Set of parent IDs
        """
        try:
            with self.connection.cursor() as cursor:
                query = """
                    SELECT DISTINCT parent_id
                    FROM files
                    WHERE source_type = %s
                      AND source_field = %s
                """
                cursor.execute(query, [source_type, source_field])
                return {row[0] for row in cursor.fetchall()}
        except Exception as e:
            return set()

