
"""
One-Time Script: Convert Existing seizure_worth Values from Rupees to Crores

This script:
1. Finds all records with seizure_worth > 0 (currently stored in rupees)
2. Converts them to crores (divides by 10,000,000)
3. Updates the seizure_worth field in the database
4. Processes in batches with progress tracking

IMPORTANT: This is a one-time conversion script. After running this, all existing
values will be in crores, and new extractions will also be in crores (via extractor.py).
"""

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
import config
import logging
import sys

# Setup Logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('convert_to_crores.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

# Conversion constant: 1 crore = 10,000,000 rupees
RUPEES_PER_CRORE = 10_000_000.0

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
    Check current status: How many records need conversion?
    Returns statistics about what needs to be updated.
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Count total records with seizure_worth > 0 (in rupees, need conversion)
        cur.execute(sql.SQL("""
            SELECT COUNT(*) as total_records
            FROM {table}
            WHERE seizure_worth > 0.0
        """).format(table=sql.Identifier(config.DRUG_TABLE_NAME)))
        total_records = cur.fetchone()['total_records']
        
        # Count unique crimes that have records needing conversion
        cur.execute(sql.SQL("""
            SELECT COUNT(DISTINCT crime_id) as unique_crimes
            FROM {table}
            WHERE seizure_worth > 0.0
        """).format(table=sql.Identifier(config.DRUG_TABLE_NAME)))
        unique_crimes = cur.fetchone()['unique_crimes']
        
        # Get sample values to show what will be converted
        cur.execute(sql.SQL("""
            SELECT id, crime_id, drug_name, seizure_worth
            FROM {table}
            WHERE seizure_worth > 0.0
            ORDER BY seizure_worth DESC
            LIMIT 5
        """).format(table=sql.Identifier(config.DRUG_TABLE_NAME)))
        sample_records = cur.fetchall()
        
        return {
            'total_records_needing_conversion': total_records,
            'unique_crimes_needing_conversion': unique_crimes,
            'sample_records': sample_records
        }

def fetch_records_for_conversion(conn, batch_size=100):
    """
    Fetches records with seizure_worth > 0 that need to be converted to crores.
    Returns records with their current values.
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        query = sql.SQL("""
            SELECT id, crime_id, drug_name, seizure_worth
            FROM {table}
            WHERE seizure_worth > 0.0
            ORDER BY id
            LIMIT %s
        """).format(table=sql.Identifier(config.DRUG_TABLE_NAME))
        
        cur.execute(query, (batch_size,))
        return cur.fetchall()

def convert_and_update_batch(conn, records, dry_run=False):
    """
    Converts seizure_worth from rupees to crores and updates the database.
    
    Args:
        conn: Database connection
        records: List of records to convert
        dry_run: If True, only shows what would be converted without making changes
    
    Returns:
        Number of records updated
    """
    updated_count = 0
    
    with conn.cursor() as cur:
        for record in records:
            record_id = record['id']
            current_value_rupees = float(record['seizure_worth'])
            new_value_crores = current_value_rupees / RUPEES_PER_CRORE
            
            if dry_run:
                logging.info(
                    f"Would convert: ID={record_id}, "
                    f"Drug={record['drug_name']}, "
                    f"Current={current_value_rupees:,.2f} rupees → "
                    f"New={new_value_crores:.6f} crores"
                )
            else:
                update_query = sql.SQL("""
                    UPDATE {table}
                    SET seizure_worth = %s
                    WHERE id = %s
                """).format(table=sql.Identifier(config.DRUG_TABLE_NAME))
                
                cur.execute(update_query, (new_value_crores, record_id))
                updated_count += cur.rowcount
                
                if updated_count % 50 == 0:
                    logging.info(f"Converted {updated_count} records so far...")
    
    if not dry_run:
        conn.commit()
    
    return updated_count

def process_conversion(dry_run=False):
    """
    Main function to convert existing seizure_worth values from rupees to crores.
    
    Args:
        dry_run: If True, only shows what would be converted without making changes
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
        
        logging.info(f"Records with seizure_worth > 0 (need conversion): {status['total_records_needing_conversion']}")
        logging.info(f"Unique crimes affected: {status['unique_crimes_needing_conversion']}")
        logging.info("")
        
        if status['total_records_needing_conversion'] == 0:
            logging.info("✓ No records found with seizure_worth > 0. Nothing to convert!")
            return
        
        # Show sample records
        if status['sample_records']:
            logging.info("Sample records that will be converted:")
            for rec in status['sample_records']:
                current_rupees = float(rec['seizure_worth'])
                new_crores = current_rupees / RUPEES_PER_CRORE
                logging.info(
                    f"  - {rec['drug_name']}: "
                    f"{current_rupees:,.2f} rupees → {new_crores:.6f} crores"
                )
            logging.info("")
        
        if dry_run:
            logging.info("=" * 60)
            logging.info("DRY RUN MODE - No changes will be made")
            logging.info("=" * 60)
            logging.info(f"Would convert {status['total_records_needing_conversion']} records")
            logging.info(f"Would affect {status['unique_crimes_needing_conversion']} unique crimes")
            logging.info("")
            
            # Show what would be converted
            batch_size = 100
            total_shown = 0
            while total_shown < min(10, status['total_records_needing_conversion']):
                records = fetch_records_for_conversion(conn, batch_size)
                if not records:
                    break
                convert_and_update_batch(conn, records, dry_run=True)
                total_shown += len(records)
                if len(records) < batch_size:
                    break
            return
        
        # Step 2: Process conversion in batches
        logging.info("=" * 60)
        logging.info("STARTING CONVERSION PROCESS...")
        logging.info("=" * 60)
        logging.info(f"Converting seizure_worth from rupees to crores (÷ {RUPEES_PER_CRORE:,})")
        logging.info("")
        
        batch_size = 100
        total_records_converted = 0
        batch_number = 0
        
        while True:
            batch_number += 1
            records = fetch_records_for_conversion(conn, batch_size)
            
            if not records:
                logging.info("No more records to convert.")
                break
            
            logging.info(f"\n--- Processing Batch #{batch_number} ({len(records)} records) ---")
            
            updated = convert_and_update_batch(conn, records, dry_run=False)
            total_records_converted += updated
            
            logging.info(f"Batch #{batch_number} complete. Converted: {updated} records")
            
            # If we got fewer than batch_size, we're done
            if len(records) < batch_size:
                break
        
        # Step 3: Final status
        logging.info("")
        logging.info("=" * 60)
        logging.info("CONVERSION COMPLETE!")
        logging.info("=" * 60)
        logging.info(f"Total records converted: {total_records_converted}")
        
        # Verify conversion
        final_status = check_status(conn)
        logging.info("")
        logging.info("FINAL STATUS:")
        logging.info(f"  Records still with seizure_worth > 0: {final_status['total_records_needing_conversion']}")
        if final_status['total_records_needing_conversion'] == 0:
            logging.info("  ✓ All values successfully converted to crores!")
        else:
            logging.warning(f"  ⚠ {final_status['total_records_needing_conversion']} records still need conversion")
        
    except Exception as e:
        logging.error(f"Conversion process failed: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Convert existing seizure_worth values from rupees to crores'
    )
    parser.add_argument(
        '--dry-run', 
        action='store_true', 
        help='Show what would be converted without making changes'
    )
    args = parser.parse_args()
    
    logging.info("=" * 60)
    logging.info("SEIZURE WORTH CONVERSION: RUPEES → CRORES")
    logging.info("=" * 60)
    logging.info("This script will:")
    logging.info("  1. Find all records with seizure_worth > 0 (currently in rupees)")
    logging.info("  2. Convert them to crores (divide by 10,000,000)")
    logging.info("  3. Update the seizure_worth field in the database")
    logging.info("")
    logging.info(f"Conversion formula: crores = rupees / {RUPEES_PER_CRORE:,}")
    logging.info("")
    
    if args.dry_run:
        logging.info("Running in DRY-RUN mode (no changes will be made)")
    else:
        logging.info("⚠️  WARNING: This will modify existing data in the database!")
        logging.info("Press Ctrl+C to stop at any time.")
        logging.info("")
    
    try:
        process_conversion(dry_run=args.dry_run)
    except KeyboardInterrupt:
        logging.info("\nProcess interrupted by user.")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")


