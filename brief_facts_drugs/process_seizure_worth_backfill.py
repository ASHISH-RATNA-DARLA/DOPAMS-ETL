
"""
One-Time Script: Backfill seizure_worth for Existing Processed Crimes

This script:
1. Finds all crimes with seizure_worth = 0.0 or NULL
2. Re-extracts drug information to get seizure_worth values
3. ONLY updates seizure_worth field (all other fields remain unchanged)
4. Processes in batches with progress tracking
"""

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
import config
from extractor import extract_drug_info
import logging
import sys

# Setup Logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('seizure_worth_backfill.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

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

def check_status(conn):
    """
    Check current status: How many records need processing?
    Returns statistics about what needs to be updated.
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Count total records with zero/null seizure_worth
        cur.execute(sql.SQL("""
            SELECT COUNT(*) as total_records
            FROM {table}
            WHERE seizure_worth = 0.0 OR seizure_worth IS NULL
        """).format(table=sql.Identifier(config.DRUG_TABLE_NAME)))
        total_records = cur.fetchone()['total_records']
        
        # Count unique crimes that need processing
        cur.execute(sql.SQL("""
            SELECT COUNT(DISTINCT crime_id) as unique_crimes
            FROM {table}
            WHERE seizure_worth = 0.0 OR seizure_worth IS NULL
        """).format(table=sql.Identifier(config.DRUG_TABLE_NAME)))
        unique_crimes = cur.fetchone()['unique_crimes']
        
        # Count records that already have seizure_worth > 0
        cur.execute(sql.SQL("""
            SELECT COUNT(*) as already_processed
            FROM {table}
            WHERE seizure_worth > 0.0
        """).format(table=sql.Identifier(config.DRUG_TABLE_NAME)))
        already_processed = cur.fetchone()['already_processed']
        
        return {
            'total_records_needing_update': total_records,
            'unique_crimes_needing_update': unique_crimes,
            'already_processed_records': already_processed
        }

def fetch_crimes_needing_processing(conn, batch_size=50):
    """
    Fetches unique crimes that have been processed but have seizure_worth = 0.0 or NULL.
    Returns crime_id and brief_facts for processing.
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        query = sql.SQL("""
            SELECT DISTINCT c.crime_id, c.brief_facts
            FROM crimes c
            INNER JOIN {table} d ON c.crime_id = d.crime_id
            WHERE (d.seizure_worth = 0.0 OR d.seizure_worth IS NULL)
              AND c.brief_facts IS NOT NULL
              AND c.brief_facts != ''
            LIMIT %s
        """).format(table=sql.Identifier(config.DRUG_TABLE_NAME))
        
        cur.execute(query, (batch_size,))
        return cur.fetchall()

def update_seizure_worth_only(conn, crime_id, extractions):
    """
    Updates ONLY the seizure_worth field for existing drug entries.
    Does NOT modify any other fields - all other data remains unchanged.
    Matches drugs by crime_id, drug_name, and quantity_numeric.
    Only updates records where seizure_worth is currently 0.0 or NULL.
    """
    updated_count = 0
    with conn.cursor() as cur:
        for extraction in extractions:
            # CRITICAL: This query ONLY updates seizure_worth field
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
                extraction.drug_name,      # Used for matching only
                extraction.quantity_numeric # Used for matching only
            ))
            updated_count += cur.rowcount
    
    conn.commit()
    return updated_count

def process_backfill(dry_run=False):
    """
    Main function to backfill seizure_worth for existing processed crimes.
    
    Args:
        dry_run: If True, only shows what would be processed without making changes
    """
    conn = get_db_connection()
    if not conn:
        logging.error("Failed to connect to database. Exiting.")
        return
    
    try:
        # Step 1: Check current status
        logging.info("=" * 60)
        logging.info("CHECKING CURRENT STATUS...")
        logging.info("=" * 60)
        status = check_status(conn)
        
        logging.info(f"Records already with seizure_worth > 0: {status['already_processed_records']}")
        logging.info(f"Records needing seizure_worth update: {status['total_records_needing_update']}")
        logging.info(f"Unique crimes needing processing: {status['unique_crimes_needing_update']}")
        logging.info("")
        
        if status['total_records_needing_update'] == 0:
            logging.info("✓ All records already have seizure_worth processed. Nothing to do!")
            return
        
        if dry_run:
            logging.info("=" * 60)
            logging.info("DRY RUN MODE - No changes will be made")
            logging.info("=" * 60)
            logging.info(f"Would process {status['unique_crimes_needing_update']} unique crimes")
            logging.info(f"Would update {status['total_records_needing_update']} drug records")
            return
        
        # Step 2: Process in batches
        logging.info("=" * 60)
        logging.info("STARTING BACKFILL PROCESS...")
        logging.info("=" * 60)
        logging.info("IMPORTANT: Only seizure_worth field will be updated.")
        logging.info("All other fields (drug_name, quantity, form, etc.) remain unchanged.")
        logging.info("")
        
        batch_size = 50
        total_crimes_processed = 0
        total_records_updated = 0
        batch_number = 0
        
        while True:
            batch_number += 1
            crimes = fetch_crimes_needing_processing(conn, batch_size)
            
            if not crimes:
                logging.info("No more crimes needing processing.")
                break
            
            logging.info(f"\n--- Processing Batch #{batch_number} ({len(crimes)} crimes) ---")
            
            for idx, crime in enumerate(crimes, 1):
                crime_id = crime['crime_id']
                brief_facts = crime['brief_facts']
                
                if not brief_facts or brief_facts.strip() == '':
                    logging.warning(f"[{idx}/{len(crimes)}] Skipping crime {crime_id}: no brief_facts text")
                    continue
                
                try:
                    # Re-extract drug information to get seizure_worth values
                    # We extract to get seizure_worth, but ONLY update that field in DB
                    extractions = extract_drug_info(brief_facts)
                    
                    if not extractions:
                        logging.warning(f"[{idx}/{len(crimes)}] Crime {crime_id}: No drugs extracted")
                        continue
                    
                    # Update ONLY seizure_worth field for matching records
                    updated = update_seizure_worth_only(conn, crime_id, extractions)
                    
                    if updated > 0:
                        # Log which drugs got seizure_worth updated
                        worth_values = [f"{d.drug_name}: Rs.{d.seizure_worth:,.2f}" 
                                      for d in extractions if d.seizure_worth > 0]
                        if worth_values:
                            logging.info(f"[{idx}/{len(crimes)}] ✓ Crime {crime_id}: Updated {updated} records - {', '.join(worth_values)}")
                        else:
                            logging.info(f"[{idx}/{len(crimes)}] ✓ Crime {crime_id}: Updated {updated} records (seizure_worth = 0.0, not mentioned in text)")
                        total_records_updated += updated
                    else:
                        logging.warning(f"[{idx}/{len(crimes)}] Crime {crime_id}: No matching records updated (may have already been processed)")
                    
                    total_crimes_processed += 1
                    
                except Exception as e:
                    logging.error(f"[{idx}/{len(crimes)}] ✗ Error processing crime {crime_id}: {e}")
                    continue
            
            logging.info(f"Batch #{batch_number} complete. Processed: {total_crimes_processed} crimes, Updated: {total_records_updated} records")
            
            # If we got fewer than batch_size, we're done
            if len(crimes) < batch_size:
                break
        
        # Step 3: Final status
        logging.info("")
        logging.info("=" * 60)
        logging.info("BACKFILL COMPLETE!")
        logging.info("=" * 60)
        logging.info(f"Total crimes processed: {total_crimes_processed}")
        logging.info(f"Total records updated: {total_records_updated}")
        
        # Check final status
        final_status = check_status(conn)
        logging.info("")
        logging.info("FINAL STATUS:")
        logging.info(f"  Records with seizure_worth > 0: {final_status['already_processed_records']}")
        logging.info(f"  Records still needing update: {final_status['total_records_needing_update']}")
        
    except Exception as e:
        logging.error(f"Backfill process failed: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Backfill seizure_worth for existing processed crimes')
    parser.add_argument('--dry-run', action='store_true', 
                       help='Show what would be processed without making changes')
    args = parser.parse_args()
    
    logging.info("=" * 60)
    logging.info("SEIZURE WORTH BACKFILL SCRIPT")
    logging.info("=" * 60)
    logging.info("This script will:")
    logging.info("  1. Find all crimes with seizure_worth = 0.0 or NULL")
    logging.info("  2. Re-extract drug information to get seizure_worth")
    logging.info("  3. ONLY update seizure_worth field (all other fields unchanged)")
    logging.info("")
    
    if args.dry_run:
        logging.info("Running in DRY-RUN mode (no changes will be made)")
    else:
        logging.info("Press Ctrl+C to stop at any time.")
        logging.info("")
    
    try:
        process_backfill(dry_run=args.dry_run)
    except KeyboardInterrupt:
        logging.info("\nProcess interrupted by user.")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")


