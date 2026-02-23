"""
Schema Manager - NOW USES schema_reference.py AS SOURCE OF TRUTH!
Handles schema fetching and caching for both databases
"""
import logging
from typing import Dict, Any, List
from database.postgres_executor import PostgreSQLExecutor
from database.mongo_executor import MongoDBExecutor

# ⭐ IMPORT YOUR SCHEMA REFERENCE AS SOURCE OF TRUTH!
from database.schema_reference import (
    POSTGRESQL_SCHEMA,
    MONGODB_SCHEMA,
    MONGODB_FIELD_MAPPINGS,
    get_table_columns
)

logger = logging.getLogger(__name__)


class SchemaManager:
    """Manage database schemas - USES schema_reference.py as source of truth"""
    
    def __init__(self, postgres_executor: PostgreSQLExecutor, mongo_executor: MongoDBExecutor):
        self.postgres = postgres_executor
        self.mongo = mongo_executor
        logger.info("SchemaManager initialized with schema_reference.py as source of truth")
    
    def get_combined_schema(self) -> Dict[str, Any]:
        """
        Get combined schema from schema_reference.py (SOURCE OF TRUTH!)
        
        Returns:
            Dict with 'postgresql' and 'mongodb' keys containing their schemas
        """
        schema = {
            'postgresql': {},
            'mongodb': {}
        }
        
        # ⭐ Use YOUR schema_reference.py for PostgreSQL
        logger.info("Loading PostgreSQL schema from schema_reference.py")
        for table_name, table_schema in POSTGRESQL_SCHEMA.items():
            columns = []
            for col in table_schema.columns:
                columns.append({
                    'column': col.name,
                    'type': col.data_type,
                    'nullable': col.is_nullable,
                    'max_length': col.max_length
                })
            schema['postgresql'][table_name] = columns
        
        logger.info(f"Loaded {len(schema['postgresql'])} PostgreSQL tables from schema_reference.py")
        
        # ⭐ Use YOUR schema_reference.py for MongoDB
        logger.info("Loading MongoDB schema from schema_reference.py")
        for collection_name, collection_info in MONGODB_SCHEMA.items():
            fields = []
            for field_name, field_desc in collection_info['important_fields'].items():
                fields.append({
                    'field': field_name,
                    'type': field_desc.split('(')[0].strip() if '(' in field_desc else field_desc
                })
            schema['mongodb'][collection_name] = {
                'count': 0,  # Will be updated by actual query if needed
                'fields': fields
            }
        
        logger.info(f"Loaded {len(schema['mongodb'])} MongoDB collections from schema_reference.py")
        
        return schema
    
    def format_schema_for_llm(self, compact: bool = True) -> str:
        """
        Format schema in a readable format for LLM
        
        Args:
            compact: If True, show only table names and first 10 columns per table
        
        Returns:
            String representation of both database schemas
        """
        schema = self.get_combined_schema()
        
        output = []
        output.append("=" * 60)
        output.append("DATABASE SCHEMA INFORMATION")
        output.append("=" * 60)
        
        # PostgreSQL Schema
        output.append("\n📊 POSTGRESQL DATABASE:")
        output.append("-" * 60)
        
        if schema['postgresql']:
            for table_name, columns in schema['postgresql'].items():
                output.append(f"\nTable: {table_name}")
                
                if compact and len(columns) > 10:
                    # Show first 10 columns + count
                    for col in columns[:10]:
                        nullable = "NULL" if col['nullable'] else "NOT NULL"
                        output.append(f"  - {col['column']}: {col['type']} ({nullable})")
                    output.append(f"  ... and {len(columns) - 10} more columns")
                else:
                    # Show all columns
                    for col in columns:
                        nullable = "NULL" if col['nullable'] else "NOT NULL"
                        output.append(f"  - {col['column']}: {col['type']} ({nullable})")
        else:
            output.append("  No PostgreSQL tables found or connection failed")
        
        # MongoDB Schema
        output.append("\n\n🍃 MONGODB DATABASE:")
        output.append("-" * 60)
        
        if schema['mongodb']:
            for collection_name, info in schema['mongodb'].items():
                output.append(f"\nCollection: {collection_name}")
                output.append(f"  Document count: {info['count']}")
                output.append("  Fields:")
                
                fields = info['fields']
                if compact and len(fields) > 10:
                    # Show first 10 fields
                    for field in fields[:10]:
                        output.append(f"    - {field['field']}: {field['type']}")
                    output.append(f"    ... and {len(fields) - 10} more fields")
                else:
                    for field in fields:
                        output.append(f"    - {field['field']}: {field['type']}")
        else:
            output.append("  No MongoDB collections found or connection failed")
        
        output.append("\n" + "=" * 60)
        
        return "\n".join(output)
    
    def detect_target_database(self, user_message: str) -> str:
        """
        Detect which database the query should target based on user message
        
        Args:
            user_message: User's natural language query
        
        Returns:
            'postgresql', 'mongodb', or 'both'
        """
        message_lower = user_message.lower()
        
        # Explicit mentions
        if 'postgres' in message_lower or 'sql' in message_lower:
            return 'postgresql'
        
        if 'mongo' in message_lower or 'document' in message_lower:
            return 'mongodb'
        
        # Default to querying both databases
        return 'both'



