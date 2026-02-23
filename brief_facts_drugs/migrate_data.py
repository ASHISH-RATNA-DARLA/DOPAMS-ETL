
import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
import config
from extractor import standardize_units, DrugExtraction, truncate_string
import logging

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_db_connection():
    try:
        conn = psycopg2.connect(
            dbname=config.DB_NAME,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            host=config.DB_HOST,
            port=config.DB_PORT
        )
        return conn
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        return None

def migrate_rows():
    conn = get_db_connection()
    if not conn:
        return

    table_name = config.DRUG_TABLE_NAME
    logging.info(f"Starting migration on table: {table_name}")

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # 1. Fetch rows that have NOT been standardized yet (primary_unit_type is NULL)
            #    We select only the columns needed to reconstruct a DrugExtraction object.
            query = sql.SQL("""
                SELECT id, drug_name, quantity_numeric, quantity_unit, drug_form, 
                       packaging_details, confidence_score, seizure_worth
                FROM {table}
                WHERE primary_unit_type IS NULL
            """).format(table=sql.Identifier(table_name))
            
            cur.execute(query)
            rows = cur.fetchall()
            
            logging.info(f"Found {len(rows)} rows to migrate.")
            
            for row in rows:
                try:
                    # Construct DrugExtraction object
                    # We have to handle potential None values from legacy data
                    drug = DrugExtraction(
                        drug_name=row['drug_name'] or "Unknown",
                        quantity_numeric=float(row['quantity_numeric']) if row['quantity_numeric'] is not None else 0.0,
                        quantity_unit=row['quantity_unit'] or "Unknown",
                        drug_form=row['drug_form'] or "Unknown",
                        packaging_details=row['packaging_details'] or "",
                        confidence_score=row['confidence_score'] or 0,
                        seizure_worth=float(row['seizure_worth']) if row['seizure_worth'] is not None else 0.0
                    )
                    
                    # Apply Standardization Logic
                    # standardize_units takes a list and modifies in-place
                    standardized_list = standardize_units([drug])
                    std_drug = standardized_list[0]
                    
                    # Update the row in DB
                    update_query = sql.SQL("""
                        UPDATE {table}
                        SET 
                            standardized_weight_kg = %s,
                            standardized_volume_ml = %s,
                            standardized_count = %s,
                            primary_unit_type = %s
                        WHERE id = %s
                    """).format(table=sql.Identifier(table_name))
                    
                    cur.execute(update_query, (
                        std_drug.standardized_weight_kg,
                        std_drug.standardized_volume_ml,
                        std_drug.standardized_count,
                        std_drug.primary_unit_type,
                        row['id']
                    ))
                    
                except Exception as e:
                    logging.error(f"Failed to process row ID {row.get('id')}: {e}")

            conn.commit()
            logging.info("Migration complete successfully.")

    except Exception as e:
        logging.error(f"Migration failed: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    migrate_rows()

