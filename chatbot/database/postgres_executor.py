"""
PostgreSQL Query Executor
Handles connection pooling, query execution, and error handling
"""
import psycopg2
from psycopg2 import pool, sql
from psycopg2.extras import RealDictCursor
import logging
from typing import Dict, List, Any, Tuple
from config import Config

logger = logging.getLogger(__name__)


class PostgreSQLExecutor:
    """Execute PostgreSQL queries safely with connection pooling"""
    
    def __init__(self):
        self.connection_pool = None
        self._initialize_pool()
    
    def _initialize_pool(self):
        """Initialize PostgreSQL connection pool"""
        try:
            self.connection_pool = psycopg2.pool.SimpleConnectionPool(
                minconn=1,
                maxconn=10,
                host=Config.POSTGRES_CONFIG['host'],
                port=Config.POSTGRES_CONFIG['port'],
                database=Config.POSTGRES_CONFIG['database'],
                user=Config.POSTGRES_CONFIG['user'],
                password=Config.POSTGRES_CONFIG['password'],
                connect_timeout=10,
                options=f'-c statement_timeout={Config.QUERY_TIMEOUT * 1000}'  # milliseconds
            )
            logger.info("PostgreSQL connection pool initialized")
        except Exception as e:
            logger.error(f"Failed to initialize PostgreSQL pool: {e}")
            raise
    
    def execute_query(self, query: str, params: tuple = None) -> Tuple[bool, Any]:
        """
        Execute a SELECT query safely
        
        Args:
            query: SQL query string
            params: Query parameters for parameterization
        
        Returns:
            Tuple of (success: bool, result: List[Dict] or error_message: str)
        """
        connection = None
        cursor = None
        
        try:
            # Get connection from pool
            connection = self.connection_pool.getconn()
            
            # Use RealDictCursor for dict results
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            # Execute query with timeout
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            # Fetch results with row limit
            results = cursor.fetchmany(Config.MAX_QUERY_ROWS)
            
            # Convert to list of dicts
            data = [dict(row) for row in results]
            
            logger.info(f"Query executed successfully, returned {len(data)} rows")
            return True, data
            
        except psycopg2.OperationalError as e:
            error_msg = "Database connection error"
            logger.error(f"Operational error: {e}")
            return False, error_msg
            
        except psycopg2.ProgrammingError as e:
            # Preserve the actual error message for better debugging
            error_msg = str(e)
            logger.error(f"Programming error: {e}")
            return False, error_msg
            
        except Exception as e:
            error_msg = "Query execution failed"
            logger.error(f"Unexpected error: {e}")
            return False, error_msg
            
        finally:
            # Clean up
            if cursor:
                cursor.close()
            if connection:
                self.connection_pool.putconn(connection)
    
    def get_schema_info(self) -> Tuple[bool, Any]:
        """
        Get database schema information (ONLY base tables, NOT views/indexes)
        
        Returns:
            Tuple of (success: bool, schema: Dict or error_message: str)
        """
        query = """
        SELECT 
            c.table_name,
            c.column_name,
            c.data_type,
            c.is_nullable
        FROM information_schema.columns c
        INNER JOIN information_schema.tables t 
            ON c.table_name = t.table_name 
            AND c.table_schema = t.table_schema
        WHERE c.table_schema = 'public'
            AND t.table_type = 'BASE TABLE'
        ORDER BY c.table_name, c.ordinal_position;
        """
        
        success, result = self.execute_query(query)
        
        if not success:
            return False, result
        
        # Organize schema by table
        schema = {}
        for row in result:
            table = row['table_name']
            if table not in schema:
                schema[table] = []
            
            schema[table].append({
                'column': row['column_name'],
                'type': row['data_type'],
                'nullable': row['is_nullable'] == 'YES'
            })
        
        return True, schema
    
    def test_connection(self) -> bool:
        """Test if database connection is working"""
        try:
            success, _ = self.execute_query("SELECT 1 as test")
            return success
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
    
    def close(self):
        """Close all connections in the pool"""
        if self.connection_pool:
            self.connection_pool.closeall()
            logger.info("PostgreSQL connection pool closed")



