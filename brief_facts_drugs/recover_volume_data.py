"""
Emergency Recovery Script: Recover standardized_volume_ml from original quantity_numeric and quantity_unit

This script will:
1. Find all records with volume units (liters, ml) that have standardized_volume_ml that seems wrong
2. Recalculate standardized_volume_ml from the original quantity_numeric and quantity_unit
3. Update the database with the correct values
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
        logging.FileHandler('recover_volume_data.log'),
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

def standardize_volume(qty, unit):
    """
    Convert volume to liters based on quantity and unit.
    Returns the volume in liters.
    """
    if qty is None or qty == 0:
        return None
    
    unit_lower = str(unit).lower().strip() if unit else "unknown"
    qty_float = float(qty)
    
    # VOLUME UNITS - convert to liters
    if unit_lower in ['l', 'ltr', 'liter', 'liters', 'litre', 'litres']:
        return qty_float  # Already in liters
    elif unit_lower in ['ml', 'ml.', 'milliliter', 'milliliters']:
        return qty_float / 1000.0  # Convert ml to liters
    else:
        return None  # Not a volume unit

def check_status(conn):
    """
    Check current status: How many records need recovery?
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Find records with volume units that need recovery
        # Only fetch records where standardized_volume_ml is NULL, 0, or very small (< 0.001)
        # This prevents re-processing already recovered records
        cur.execute(sql.SQL("""
            SELECT COUNT(*) as total_records
            FROM {table}
            WHERE LOWER(quantity_unit) IN ('l', 'ltr', 'liter', 'liters', 'litre', 'litres', 'ml', 'ml.', 'milliliter', 'milliliters')
            AND quantity_numeric IS NOT NULL
            AND quantity_numeric > 0
            AND (standardized_volume_ml IS NULL OR standardized_volume_ml = 0 OR standardized_volume_ml < 0.001)
        """).format(table=sql.Identifier(config.DRUG_TABLE_NAME)))
        total_records = cur.fetchone()['total_records']
        
        # Get sample records
        cur.execute(sql.SQL("""
            SELECT id, crime_id, drug_name, quantity_numeric, quantity_unit, standardized_volume_ml, primary_unit_type
            FROM {table}
            WHERE LOWER(quantity_unit) IN ('l', 'ltr', 'liter', 'liters', 'litre', 'litres', 'ml', 'ml.', 'milliliter', 'milliliters')
            AND quantity_numeric IS NOT NULL
            AND quantity_numeric > 0
            AND (standardized_volume_ml IS NULL OR standardized_volume_ml = 0 OR standardized_volume_ml < 0.001)
            ORDER BY quantity_numeric DESC
            LIMIT 10
        """).format(table=sql.Identifier(config.DRUG_TABLE_NAME)))
        sample_records = cur.fetchall()
        
        return {
            'total_records': total_records,
            'sample_records': sample_records
        }

def fetch_records_for_recovery(conn, batch_size=100):
    """
    Fetches records with volume units that need recovery.
    Only fetches records where standardized_volume_ml is NULL, 0, or very small (< 0.001).
    This prevents re-processing already recovered records.
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        query = sql.SQL("""
            SELECT id, crime_id, drug_name, quantity_numeric, quantity_unit, standardized_volume_ml, primary_unit_type
            FROM {table}
            WHERE LOWER(quantity_unit) IN ('l', 'ltr', 'liter', 'liters', 'litre', 'litres', 'ml', 'ml.', 'milliliter', 'milliliters')
            AND quantity_numeric IS NOT NULL
            AND quantity_numeric > 0
            AND (standardized_volume_ml IS NULL OR standardized_volume_ml = 0 OR standardized_volume_ml < 0.001)
            ORDER BY id
            LIMIT %s
        """).format(table=sql.Identifier(config.DRUG_TABLE_NAME))
        
        cur.execute(query, (batch_size,))
        return cur.fetchall()

def recover_and_update_batch(conn, records, dry_run=False):
    """
    Recalculates standardized_volume_ml from original quantity_numeric and quantity_unit.
    """
    updated_count = 0
    
    with conn.cursor() as cur:
        for record in records:
            record_id = record['id']
            qty = record['quantity_numeric']
            unit = record['quantity_unit']
            current_value = record['standardized_volume_ml']
            
            # Recalculate the correct value
            correct_value = standardize_volume(qty, unit)
            
            if correct_value is None:
                continue  # Skip if not a volume unit
            
            if dry_run:
                logging.info(
                    f"Would recover: ID={record_id}, "
                    f"Drug={record['drug_name']}, "
                    f"Qty={qty} {unit}, "
                    f"Current={current_value}, "
                    f"Correct={correct_value:.6f} liters"
                )
            else:
                update_query = sql.SQL("""
                    UPDATE {table}
                    SET standardized_volume_ml = %s,
                        primary_unit_type = 'volume'
                    WHERE id = %s
                """).format(table=sql.Identifier(config.DRUG_TABLE_NAME))
                
                cur.execute(update_query, (correct_value, record_id))
                updated_count += cur.rowcount
                
                if updated_count % 50 == 0:
                    logging.info(f"Recovered {updated_count} records so far...")
    
    if not dry_run:
        conn.commit()
    
    return updated_count

def process_recovery(dry_run=False):
    """
    Main function to recover standardized_volume_ml values.
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
        
        logging.info(f"Records with volume units found: {status['total_records']}")
        logging.info("")
        
        if status['total_records'] == 0:
            logging.info("✓ No records found with volume units. Nothing to recover!")
            return
        
        # Show sample records
        if status['sample_records']:
            logging.info("Sample records that will be recovered:")
            for rec in status['sample_records']:
                current = rec['standardized_volume_ml']
                correct = standardize_volume(rec['quantity_numeric'], rec['quantity_unit'])
                logging.info(
                    f"  - {rec['drug_name']}: {rec['quantity_numeric']} {rec['quantity_unit']} → "
                    f"Current={current}, Correct={correct:.6f} liters"
                )
            logging.info("")
        
        if dry_run:
            logging.info("=" * 60)
            logging.info("DRY RUN MODE - No changes will be made")
            logging.info("=" * 60)
            total_records = status['total_records']
            batch_size = 100
            total_batches = (total_records + batch_size - 1) // batch_size  # Ceiling division
            logging.info(f"Would recover {total_records} records")
            logging.info(f"Would process {total_batches} batches (batch size: {batch_size})")
            logging.info("")
            
            # Show what would be recovered
            batch_size = 100
            total_shown = 0
            while total_shown < min(20, status['total_records']):
                records = fetch_records_for_recovery(conn, batch_size)
                if not records:
                    break
                recover_and_update_batch(conn, records, dry_run=True)
                total_shown += len(records)
                if len(records) < batch_size:
                    break
            return
        
        # Step 2: Process recovery in batches
        logging.info("=" * 60)
        logging.info("STARTING RECOVERY PROCESS...")
        logging.info("=" * 60)
        logging.info("Recovering standardized_volume_ml from original quantity_numeric and quantity_unit")
        logging.info("")
        
        batch_size = 100
        total_records = status['total_records']
        total_batches = (total_records + batch_size - 1) // batch_size  # Ceiling division
        
        logging.info(f"Total records to recover: {total_records}")
        logging.info(f"Batch size: {batch_size}")
        logging.info(f"Total batches to process: {total_batches}")
        logging.info("")
        
        total_records_recovered = 0
        batch_number = 0
        
        while True:
            batch_number += 1
            records = fetch_records_for_recovery(conn, batch_size)
            
            if not records:
                logging.info("No more records to recover.")
                break
            
            logging.info(f"\n--- Processing Batch #{batch_number} of {total_batches} ({len(records)} records) ---")
            logging.info(f"Progress: {batch_number}/{total_batches} batches ({total_records_recovered}/{total_records} records recovered)")
            
            updated = recover_and_update_batch(conn, records, dry_run=False)
            total_records_recovered += updated
            
            logging.info(f"Batch #{batch_number} complete. Recovered: {updated} records (Total: {total_records_recovered}/{total_records})")
            
            # If we got fewer than batch_size, we're done
            if len(records) < batch_size:
                break
        
        # Step 3: Final status
        logging.info("")
        logging.info("=" * 60)
        logging.info("RECOVERY COMPLETE!")
        logging.info("=" * 60)
        logging.info(f"Total records recovered: {total_records_recovered}")
        
    except Exception as e:
        logging.error(f"Recovery process failed: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Recover standardized_volume_ml values from original quantity_numeric and quantity_unit'
    )
    parser.add_argument(
        '--dry-run', 
        action='store_true', 
        help='Show what would be recovered without making changes'
    )
    args = parser.parse_args()
    
    logging.info("=" * 60)
    logging.info("VOLUME DATA RECOVERY")
    logging.info("=" * 60)
    logging.info("This script will:")
    logging.info("  1. Find all records with volume units (liters, ml)")
    logging.info("  2. Recalculate standardized_volume_ml from original quantity_numeric and quantity_unit")
    logging.info("  3. Update the standardized_volume_ml field in the database")
    logging.info("")
    
    if args.dry_run:
        logging.info("Running in DRY-RUN mode (no changes will be made)")
    else:
        logging.info("⚠️  WARNING: This will modify existing data in the database!")
        logging.info("Press Ctrl+C to stop at any time.")
        logging.info("")
    
    try:
        process_recovery(dry_run=args.dry_run)
    except KeyboardInterrupt:
        logging.info("\nProcess interrupted by user.")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")


