
import psycopg2
import config

def inspect_crimes_table():
    try:
        conn = psycopg2.connect(
            dbname=config.DB_NAME,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            host=config.DB_HOST,
            port=config.DB_PORT
        )
        cur = conn.cursor()
        
        # Query to get column names for 'crimes' table
        cur.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'crimes';
        """)
        
        columns = cur.fetchall()
        print("Columns in 'crimes' table:")
        for col in columns:
            print(f"- {col[0]} ({col[1]})")
            
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    inspect_crimes_table()

