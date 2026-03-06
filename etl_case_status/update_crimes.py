#!/usr/bin/env python3
"""
Script to update crimes table using database credentials from .env file
Automatically reads and executes all UPDATE queries from case-status.sql in parallel.
"""

import os
import sys
import re
from dotenv import load_dotenv
import psycopg2
from concurrent.futures import ThreadPoolExecutor, as_completed

# Enable importing db_pooling from parent directory
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_pooling import PostgreSQLConnectionPool

# Load environment variables from .env file
load_dotenv()

# Path to SQL file
SQL_FILE = 'case-status.sql'

def parse_sql_file(file_path):
    """
    Parse SQL file and extract all UPDATE statements
    Returns a list of UPDATE SQL statements
    """
    if not os.path.exists(file_path):
        print(f"Error: SQL file '{file_path}' not found")
        sys.exit(1)
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except Exception as e:
        print(f"Error reading SQL file: {e}")
        sys.exit(1)
    
    # Simple parser: split by semicolons and filter UPDATE statements
    parts = re.split(r';(?=\s*(?:UPDATE|$))', content, flags=re.IGNORECASE)
    statements = []
    
    for part in parts:
        statement = part.strip()
        if statement.upper().startswith('UPDATE'):
            statement = re.sub(r'\s+', ' ', statement)
            statements.append(statement)
    
    if not statements:
        print(f"Warning: No UPDATE statements found in {file_path}")
    
    return statements

def get_update_description(sql):
    """
    Extract a human-readable description from the UPDATE statement
    """
    match = re.search(r"WHERE\s+case_status\s*=\s*'([^']+)'", sql, re.IGNORECASE)
    if match:
        old_value = match.group(1)
        set_match = re.search(r"SET\s+case_status\s*=\s*'([^']+)'", sql, re.IGNORECASE)
        if set_match:
            new_value = set_match.group(1)
            return f"Updating '{old_value}' to '{new_value}'"
        return f"Updating case_status where value is '{old_value}'"
    return "Executing UPDATE statement"


def execute_update(sql: str, db_pool: PostgreSQLConnectionPool, idx: int, total: int):
    """Worker function to execute a single UPDATE statement."""
    description = get_update_description(sql)
    print(f"[{idx}/{total}] Started: {description}...")
    
    try:
        with db_pool.get_connection_context() as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            rows_affected = cursor.rowcount
            conn.commit()
            print(f"  ✓ [{idx}/{total}] Completed: {description} | Rows affected: {rows_affected}")
            return rows_affected
    except psycopg2.Error as e:
        print(f"  ✗ [{idx}/{total}] Error executing statement: {e}\n  SQL: {sql}")
        raise e
    except Exception as e:
        print(f"  ✗ [{idx}/{total}] Unexpected error: {e}\n  SQL: {sql}")
        raise e


def update_crimes_table():
    """Update crimes table based on UPDATE statements from case-status.sql concurrently"""
    
    # Check if all required environment variables are set
    required_vars = ['DB_HOST', 'DB_NAME', 'DB_USER', 'DB_PASSWORD']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"Error: Missing required environment variables: {', '.join(missing_vars)}")
        print("Please ensure these are set in your .env file")
        sys.exit(1)
    
    # Parse SQL file to get all UPDATE statements
    print(f"Reading UPDATE queries from {SQL_FILE}...")
    update_statements = parse_sql_file(SQL_FILE)
    
    if not update_statements:
        print("No UPDATE statements to execute.")
        return
    
    total_stmts = len(update_statements)
    print(f"Found {total_stmts} UPDATE statement(s)\n")
    
    print("Initializing Database Connection Pool for Parallel Updates...")
    try:
        # Use enough connections for max_workers
        max_workers = min(int(os.getenv('MAX_WORKERS', '5')), total_stmts)
        db_pool = PostgreSQLConnectionPool(minconn=1, maxconn=max_workers)
        
        total_updated = 0
        print(f"Executing {total_stmts} statements concurrently using {max_workers} workers...")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(execute_update, sql, db_pool, idx, total_stmts): sql 
                for idx, sql in enumerate(update_statements, 1)
            }
            
            for future in as_completed(futures):
                try:
                    rows_affected = future.result()
                    total_updated += rows_affected
                except Exception as e:
                    print(f"\nExecution failed, some updates may not have been applied.")
                    sys.exit(1)
                    
        print(f"\n✓ Successfully updated {total_updated} total rows in crimes table")
        
    except psycopg2.Error as e:
        print(f"\nDatabase error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        sys.exit(1)
    finally:
        if 'db_pool' in locals() and hasattr(db_pool, 'close_all'):
            db_pool.close_all()
            print("Database connection pool closed")

if __name__ == "__main__":
    update_crimes_table()
