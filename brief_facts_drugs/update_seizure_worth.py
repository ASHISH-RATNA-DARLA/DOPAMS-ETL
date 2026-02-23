
import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
import config
from extractor import extract_drug_info
import logging

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
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

def fetch_processed_crimes_with_zero_worth(conn, batch_size=100):
    """
    Fetches crimes that have been processed but have seizure_worth = 0.0
    Returns unique crime_ids with their brief_facts.
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        query = sql.SQL("""
            SELECT DISTINCT c.crime_id, c.brief_facts
            FROM crimes c
            INNER JOIN {table} d ON c.crime_id = d.crime_id
            WHERE d.seizure_worth = 0.0 OR d.seizure_worth IS NULL
            LIMIT %s
        """).format(table=sql.Identifier(config.DRUG_TABLE_NAME))
        
        cur.execute(query, (batch_size,))
        return cur.fetchall()

def update_seizure_worth_for_crime(conn, crime_id, new_extractions):
    """
    Updates ONLY the seizure_worth field for existing drug entries.
    Does NOT modify any other fields - all other data remains unchanged.
    Matches drugs by drug_name and quantity_numeric to update the correct records.
    Only updates records where seizure_worth is currently 0.0 or NULL.
    """
    updated_count = 0
    with conn.cursor() as cur:
        for extraction in new_extractions:
            # IMPORTANT: This query ONLY updates seizure_worth field
            # All other fields (drug_name, quantity, form, packaging, etc.) remain unchanged
            update_query = sql.SQL("""
                UPDATE {table}
                SET seizure_worth = %s
                WHERE crime_id = %s 
                  AND drug_name = %s
                  AND quantity_numeric = %s
                  AND (seizure_worth = 0.0 OR seizure_worth IS NULL)
            """).format(table=sql.Identifier(config.DRUG_TABLE_NAME))
            
            cur.execute(update_query, (
                extraction.seizure_worth,  # Only updating this field
                crime_id,
                extraction.drug_name,     # Used for matching only
                extraction.quantity_numeric  # Used for matching only
            ))
            updated_count += cur.rowcount
    
    conn.commit()
    return updated_count

def update_seizure_worth_batch():
    """
    Main function to update seizure_worth for processed crimes.
    Processes in batches to avoid memory issues.
    """
    conn = get_db_connection()
    if not conn:
        logging.error("Failed to connect to database. Exiting.")
        return
    
    try:
        batch_size = 50  # Process 50 crimes at a time
        total_processed = 0
        total_updated = 0
        
        while True:
            crimes = fetch_processed_crimes_with_zero_worth(conn, batch_size)
            
            if not crimes:
                logging.info("No more crimes with zero seizure_worth found.")
                break
            
            logging.info(f"Processing batch of {len(crimes)} crimes...")
            
            for crime in crimes:
                crime_id = crime['crime_id']
                brief_facts = crime['brief_facts']
                
                if not brief_facts:
                    logging.warning(f"Skipping crime {crime_id}: no brief_facts text")
                    continue
                
                try:
                    # Re-extract drug information ONLY to get seizure_worth values
                    # We need to extract to get seizure_worth, but we ONLY update that field in DB
                    extractions = extract_drug_info(brief_facts)
                    
                    if not extractions:
                        logging.info(f"No drugs extracted for crime {crime_id}")
                        continue
                    
                    # IMPORTANT: Only updates seizure_worth field, all other fields remain unchanged
                    updated = update_seizure_worth_for_crime(conn, crime_id, extractions)
                    
                    if updated > 0:
                        logging.info(f"Updated {updated} records for crime {crime_id} with seizure_worth")
                        total_updated += updated
                    else:
                        logging.warning(f"No matching records updated for crime {crime_id}")
                    
                    total_processed += 1
                    
                except Exception as e:
                    logging.error(f"Error processing crime {crime_id}: {e}")
                    continue
            
            logging.info(f"Batch complete. Total processed: {total_processed}, Total records updated: {total_updated}")
            
            # If we got fewer than batch_size, we're done
            if len(crimes) < batch_size:
                break
        
        logging.info(f"Migration complete. Processed {total_processed} crimes, updated {total_updated} records.")
        
    except Exception as e:
        logging.error(f"Migration failed: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    logging.info("Starting seizure_worth update migration...")
    logging.info("This will re-process crimes to extract seizure_worth values.")
    logging.info("IMPORTANT: Only the seizure_worth field will be updated. All other fields remain unchanged.")
    logging.info("Press Ctrl+C to stop at any time.")
    
    try:
        update_seizure_worth_batch()
    except KeyboardInterrupt:
        logging.info("Migration interrupted by user.")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")


