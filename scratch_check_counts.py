
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def check_counts():
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )
    cur = conn.cursor()
    
    tables = ['hierarchy', 'crimes', 'accused', 'persons']
    for table in tables:
        try:
            cur.execute(f"SELECT COUNT(*) FROM public.{table}")
            count = cur.fetchone()[0]
            print(f"Table public.{table}: {count} rows")
        except Exception as e:
            print(f"Error checking {table}: {e}")
            conn.rollback()
            
    cur.close()
    conn.close()

if __name__ == "__main__":
    check_counts()
