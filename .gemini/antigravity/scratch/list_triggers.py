import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST'),
    'database': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'port': int(os.getenv('POSTGRES_PORT')),
}

def list_triggers():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            print(f"Checking triggers for table 'files' in database '{DB_CONFIG['database']}'...")
            
            # List all triggers on the 'files' table
            query = """
                SELECT 
                    tgname AS trigger_name,
                    tgenabled AS status,
                    pg_get_triggerdef(pg_trigger.oid) AS definition
                FROM pg_trigger
                JOIN pg_class ON pg_trigger.tgrelid = pg_class.oid
                JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid
                WHERE pg_class.relname = 'files'
                AND tgisinternal = False;
            """
            cur.execute(query)
            triggers = cur.fetchall()
            
            if not triggers:
                print("No user-defined triggers found on table 'files'.")
                
                # Check if the table even exists
                cur.execute("SELECT nspname, relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE relname = 'files'")
                table = cur.fetchone()
                if table:
                    print(f"Table 'files' found in schema '{table['nspname']}'.")
                else:
                    print("Table 'files' NOT found in the database!")
            else:
                print(f"Found {len(triggers)} triggers:")
                for t in triggers:
                    print(f"- {t['trigger_name']} (Status: {t['status']})")
                    # print(f"  Definition: {t['definition']}")
            
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    list_triggers()
