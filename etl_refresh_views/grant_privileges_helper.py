#!/usr/bin/env python3
"""
Helper script to grant privileges on materialized views.
This script needs to be run with superuser credentials (e.g., postgres user).

Usage:
    python3 grant_privileges_helper.py

Make sure to set SUPERUSER_DB_USER and SUPERUSER_DB_PASSWORD in your .env file
or pass them as environment variables.
"""
import os
import psycopg2
from dotenv import load_dotenv

def grant_privileges():
    """
    Grant ownership of materialized views to dev_dopamas user.
    This must be run as a superuser.
    """
    # Load environment variables from .env file
    load_dotenv()
    
    # Get database credentials - use superuser credentials
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5432'),
        'database': os.getenv('DB_NAME'),
        'user': os.getenv('SUPERUSER_DB_USER', 'postgres'),  # Default to postgres
        'password': os.getenv('SUPERUSER_DB_PASSWORD', '')
    }
    
    # Target user to grant privileges to
    target_user = os.getenv('DB_USER', 'dev_dopamas')
    
    # Validate required credentials
    if not all([db_config['database'], db_config['user'], db_config['password']]):
        raise ValueError("Missing required database credentials. Need SUPERUSER_DB_USER and SUPERUSER_DB_PASSWORD in .env file")
    
    # Materialized views to grant ownership
    materialized_views = [
        'firs_mv',
        'accuseds_mv',
        'criminal_profiles_mv',
        'advanced_search_accuseds_mv',
        'advanced_search_firs_mv'
    ]
    
    connection = None
    cursor = None
    
    try:
        print(f"Connecting to database '{db_config['database']}' as superuser '{db_config['user']}'...")
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()
        
        print(f"\nGranting ownership of materialized views to '{target_user}'...\n")
        
        for view_name in materialized_views:
            try:
                sql = f"ALTER MATERIALIZED VIEW {view_name} OWNER TO {target_user};"
                print(f"Executing: {sql}")
                cursor.execute(sql)
                connection.commit()
                print(f"✓ Successfully changed ownership of {view_name}\n")
            except Exception as e:
                print(f"✗ Failed to change ownership of {view_name}: {str(e)}\n")
                connection.rollback()
        
        print("All ownership changes processed!")
        
    except psycopg2.Error as db_error:
        print(f"Database error: {str(db_error)}")
        if connection:
            connection.rollback()
    except Exception as error:
        print(f"Error: {str(error)}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
            print("\nDatabase connection closed.")

if __name__ == "__main__":
    grant_privileges()


