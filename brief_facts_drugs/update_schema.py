
import psycopg2
from psycopg2 import sql
import config
import logging

logging.basicConfig(level=logging.INFO)

def update_schema():
    try:
        conn = psycopg2.connect(
            dbname=config.DB_NAME,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            host=config.DB_HOST,
            port=config.DB_PORT
        )
        conn.autocommit = True
        
        with conn.cursor() as cur:
            logging.info(f"Connected to {config.DB_NAME}. updating table {config.DRUG_TABLE_NAME}...")
            
            # The command requested by the user
            commands = [
                "ADD COLUMN IF NOT EXISTS standardized_weight_kg NUMERIC",
                "ADD COLUMN IF NOT EXISTS standardized_volume_ml NUMERIC",
                "ADD COLUMN IF NOT EXISTS standardized_count NUMERIC",
                "ADD COLUMN IF NOT EXISTS primary_unit_type VARCHAR(20)"
            ]
            
            for cmd in commands:
                query = sql.SQL("ALTER TABLE {table} {action}").format(
                    table=sql.Identifier(config.DRUG_TABLE_NAME),
                    action=sql.SQL(cmd)
                )
                cur.execute(query)
                logging.info(f"Executed: {cmd}")
                
        logging.info("Schema update complete.")
        conn.close()
        
    except Exception as e:
        logging.error(f"Schema update failed: {e}")

if __name__ == "__main__":
    update_schema()

