import os
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

def execute_sql_from_file(sql_file_path):
    """
    Execute SQL commands from a file using database credentials from .env
    
    Args:
        sql_file_path: Path to the .sql file containing SQL commands
    """
    # Load environment variables from .env file
    load_dotenv()
    
    # Get database credentials from environment variables
    db_config = {
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT'),
        'database': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD')
    }
    
    # Validate required credentials
    if not all([db_config['database'], db_config['user'], db_config['password']]):
        raise ValueError("Missing required database credentials in .env file")
    
    # Read SQL file
    try:
        with open(sql_file_path, 'r') as file:
            sql_content = file.read()
    except FileNotFoundError:
        print(f"Error: SQL file '{sql_file_path}' not found")
        return
    
    # Connect to database and execute commands
    connection = None
    cursor = None
    
    try:
        # Establish connection
        print(f"Connecting to database '{db_config['database']}' on {db_config['host']}:{db_config['port']}...")
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()
        
        # Split SQL content by semicolons to execute individual statements
        sql_commands = [cmd.strip() for cmd in sql_content.split(';') if cmd.strip()]
        
        print(f"\nExecuting {len(sql_commands)} SQL command(s)...\n")
        
        # Execute each command
        for idx, command in enumerate(sql_commands, 1):
            try:
                print(f"[{idx}/{len(sql_commands)}] Executing: {command[:60]}{'...' if len(command) > 60 else ''}")
                cursor.execute(command)
                connection.commit()
                print(f"✓ Success\n")
            except Exception as cmd_error:
                print(f"✗ Failed: {str(cmd_error)}\n")
                connection.rollback()
                # Continue with next command instead of stopping
        
        print("All commands processed successfully!")
        
    except psycopg2.Error as db_error:
        print(f"Database error: {str(db_error)}")
        if connection:
            connection.rollback()
    except Exception as error:
        print(f"Error: {str(error)}")
    finally:
        # Close connections
        if cursor:
            cursor.close()
        if connection:
            connection.close()
            print("\nDatabase connection closed.")

if __name__ == "__main__":
    # Specify your SQL file path
    sql_file = "refresh_materialized_views.sql"
    
    execute_sql_from_file(sql_file)
