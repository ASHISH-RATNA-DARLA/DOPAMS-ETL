
import psycopg2
from psycopg2 import sql
import config
import logging

logging.basicConfig(level=logging.INFO)

def add_missing_column():
    try:
        conn = psycopg2.connect(
            dbname=config.DB_NAME,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            host=config.DB_HOST,
            port=config.DB_PORT
        )
        conn.autocommit = True
        cur = conn.cursor()
        
        table_name = config.ACCUSED_TABLE_NAME
        column_name = "existing_accused"
        
        logging.info(f"Checking if column '{column_name}' exists in '{table_name}'...")
        
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = %s AND column_name = %s;
        """, (table_name, column_name))
        
        if cur.fetchone():
            logging.info(f"Column '{column_name}' already exists.")
        else:
            logging.info(f"Adding column '{column_name}'...")
            query = sql.SQL("ALTER TABLE {table} ADD COLUMN {col} BOOLEAN DEFAULT FALSE").format(
                table=sql.Identifier(table_name),
                col=sql.Identifier(column_name)
            )
            cur.execute(query)
            logging.info("Column added successfully.")
            
        conn.close()
    except Exception as e:
        logging.error(f"Error updating schema: {e}")

if __name__ == "__main__":
    add_missing_column()

