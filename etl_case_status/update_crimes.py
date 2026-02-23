#!/usr/bin/env python3
"""
Script to update crimes table using database credentials from .env file
Automatically reads and executes all UPDATE queries from case-status.sql
"""

import os
import sys
import re
from dotenv import load_dotenv
import psycopg2

# Load environment variables from .env file
load_dotenv()

# Path to SQL file
SQL_FILE = 'case-status.sql'

def get_db_connection():
    """Create and return a database connection using credentials from .env"""
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT', '5432'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)

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
    
    # Split by semicolons to get individual statements
    statements = []
    current_statement = []
    
    # Simple parser: split by semicolons and filter UPDATE statements
    parts = re.split(r';(?=\s*(?:UPDATE|$))', content, flags=re.IGNORECASE)
    
    for part in parts:
        # Clean up the statement
        statement = part.strip()
        
        # Check if it's an UPDATE statement
        if statement.upper().startswith('UPDATE'):
            # Normalize whitespace (replace multiple spaces/newlines with single space)
            statement = re.sub(r'\s+', ' ', statement)
            statements.append(statement)
    
    if not statements:
        print(f"Warning: No UPDATE statements found in {file_path}")
    
    return statements

def get_update_description(sql):
    """
    Extract a human-readable description from the UPDATE statement
    """
    # Try to extract the WHERE condition for description
    match = re.search(r"WHERE\s+case_status\s*=\s*'([^']+)'", sql, re.IGNORECASE)
    if match:
        old_value = match.group(1)
        # Try to extract the SET value
        set_match = re.search(r"SET\s+case_status\s*=\s*'([^']+)'", sql, re.IGNORECASE)
        if set_match:
            new_value = set_match.group(1)
            return f"Updating '{old_value}' to '{new_value}'"
        return f"Updating case_status where value is '{old_value}'"
    return "Executing UPDATE statement"

def update_crimes_table():
    """Update crimes table based on UPDATE statements from case-status.sql"""
    
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
    
    print(f"Found {len(update_statements)} UPDATE statement(s)\n")
    
    conn = None
    try:
        # Connect to database
        print("Connecting to database...")
        conn = get_db_connection()
        cursor = conn.cursor()
        
        total_updated = 0
        
        # Execute each update statement
        for idx, sql in enumerate(update_statements, 1):
            description = get_update_description(sql)
            print(f"[{idx}/{len(update_statements)}] {description}...")
            print(f"  SQL: {sql}")
            
            try:
                cursor.execute(sql)
                rows_affected = cursor.rowcount
                total_updated += rows_affected
                print(f"  ✓ Rows affected: {rows_affected}\n")
            except psycopg2.Error as e:
                print(f"  ✗ Error executing statement: {e}\n")
                raise
        
        # Commit the transaction
        conn.commit()
        print(f"✓ Successfully updated {total_updated} total rows in crimes table")
        
    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        print(f"\nDatabase error: {e}")
        print("All changes have been rolled back.")
        sys.exit(1)
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"\nUnexpected error: {e}")
        print("All changes have been rolled back.")
        sys.exit(1)
    finally:
        if conn:
            cursor.close()
            conn.close()
            print("\nDatabase connection closed")

if __name__ == "__main__":
    update_crimes_table()


