
import psycopg2

from env_utils import load_repo_environment, resolve_db_config


load_repo_environment()

def check_schema():
    try:
        db = resolve_db_config()
        conn = psycopg2.connect(
            host=db['host'],
            port=db['port'],
            dbname=db['dbname'],
            user=db['user'],
            password=db['password'],
        )
        cur = conn.cursor()
        
        # Check current search path
        cur.execute("SHOW search_path")
        print(f"Search Path: {cur.fetchone()[0]}")
        
        # Check all schemas
        cur.execute("SELECT schema_name FROM information_schema.schemata")
        schemas = [s[0] for s in cur.fetchall()]
        print(f"Schemas in DB: {schemas}")
        
        # Check if dev_dopamas has any tables
        if 'dev_dopamas' in schemas:
            cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='dev_dopamas'")
            tables = [t[0] for t in cur.fetchall()]
            print(f"Tables in 'dev_dopamas' schema: {tables}")
            
            for table in tables:
                cur.execute(f"SELECT COUNT(*) FROM dev_dopamas.{table}")
                print(f"  dev_dopamas.{table}: {cur.fetchone()[0]} rows")
        
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_schema()
