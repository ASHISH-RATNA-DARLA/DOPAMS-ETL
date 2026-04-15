
import psycopg2

def list_tables():
    try:
        conn = psycopg2.connect(
            host='192.168.103.106',
            port='5432',
            dbname='dev-3',
            user='dev_dopamas',
            password='ADevingpjveD2rkdoast4s'
        )
        cur = conn.cursor()
        
        print("--- Tables in public schema ---")
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name")
        tables = [t[0] for t in cur.fetchall()]
        for t in tables:
            print(t)
            
        print("\n--- Row counts for key tables ---")
        for table in ['hierarchy', 'crimes', 'accused', 'persons', 'etl_crime_processing_log']:
            if table in tables:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                count = cur.fetchone()[0]
                print(f"{table}: {count}")
            else:
                print(f"{table}: DOES NOT EXIST")
                
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    list_tables()
