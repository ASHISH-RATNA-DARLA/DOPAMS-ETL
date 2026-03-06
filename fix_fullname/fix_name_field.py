
import psycopg2
import os
import re
import sys
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from db_pooling import PostgreSQLConnectionPool
except ImportError:
    pass

# Load environment variables
load_dotenv()


def connect_to_db():
    """Connect to PostgreSQL database using db_pool"""
    pool = PostgreSQLConnectionPool()
    return pool


def extract_clean_name(name_with_alias):
    """Extract the primary name from text with @ symbol"""
    if not name_with_alias or "@" not in name_with_alias:
        return name_with_alias

    # Split by @ and get the first part (primary name)
    parts = name_with_alias.split("@")
    clean_name = parts[0].strip()

    return clean_name


def fix_name_field():
    """Fix the name field to match the cleaned full_name field"""

    pool = connect_to_db()

    print("\n=== Fixing 'name' Field ===\n")
    print(
        "This script will update the 'name' field to match the cleaned 'full_name' field\n"
    )

    # Find all records where name has @ but full_name doesn't
    with pool.get_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT person_id, name, full_name, raw_full_name
                FROM persons
                WHERE name LIKE '%@%'
                ORDER BY person_id
            """
            )
        
            records = cursor.fetchall()
            
    print(f"Found {len(records)} records where 'name' field has @ symbol\n")

    if len(records) == 0:
        print("No records to update!")
        return

    # Show sample
    print("=== Sample of Proposed Changes (first 20) ===\n")
    for i, (person_id, name, full_name, raw_full_name) in enumerate(records[:20], 1):
        clean_name = extract_clean_name(name) if name else name
        print(f"{i}. Person ID: {person_id}")
        print(f'   Current name: "{name}"')
        print(f'   Will become: "{clean_name}"')
        print(f'   full_name: "{full_name}"')
        print()

    if len(records) > 20:
        print(f"... and {len(records) - 20} more records\n")

    # Ask for confirmation
    print("\n" + "=" * 60)
    print(f"Total records to update: {len(records)}")
    print("=" * 60)

    # Auto-proceed without confirmation
    print("\nProceeding with updates...\n")

    # Proceed with updates
    print("\n=== Applying Updates ===\n")

    update_count = 0
    error_count = 0
    stats_lock = threading.Lock()

    def process_update_batch(batch):
        local_updated = 0
        local_errors = 0
        with pool.get_connection_context() as conn:
            with conn.cursor() as cursor:
                for person_id, name, full_name, raw_full_name in batch:
                    try:
                        # Extract clean name from the name field
                        clean_name = extract_clean_name(name) if name else name

                        # Update the name field
                        cursor.execute(
                            """
                            UPDATE persons
                            SET name = %s
                            WHERE person_id = %s
                            """,
                            (clean_name, person_id),
                        )

                        local_updated += 1

                    except Exception as e:
                        local_errors += 1
                        print(f"Error updating {person_id}: {e}")
            conn.commit()
            
        with stats_lock:
            nonlocal update_count, error_count
            update_count += local_updated
            error_count += local_errors
            if update_count % 500 == 0:
                print(f"Updated {update_count} records...")

    batch_size = 500
    batches = [records[i:i + batch_size] for i in range(0, len(records), batch_size)]
    
    max_workers = int(os.environ.get('MAX_WORKERS', min(32, (os.cpu_count() or 1) * 4)))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_update_batch, batch): batch for batch in batches}
        for future in as_completed(futures):
            future.result()

    print(f"\n=== Update Complete ===")
    print(f"Successfully updated: {update_count} records")
    print(f"Errors: {error_count} records")

    # Verify the changes
    print("\n=== Verifying Changes ===\n")
    with pool.get_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*) 
                FROM persons 
                WHERE name LIKE '%@%'
            """
            )
            remaining = cursor.fetchone()[0]

    print(f"Records with @ in 'name' field after cleanup: {remaining}")

    if remaining > 0:
        print(f"\nNote: {remaining} records still have @ in name field.")
        print("These may be in surname or other fields.")

    print("\n✅ Name field cleanup completed!\n")


if __name__ == "__main__":
    # Show usage if --help is requested
    if "--help" in sys.argv or "-h" in sys.argv:
        print("\n=== Fix 'name' Field Script ===\n")
        print("Usage: python fix_name_field.py [OPTIONS]\n")
        print("Options:")
        print("  --confirm, -y    Auto-confirm and proceed with updates")
        print("  --help, -h       Show this help message\n")
        print(
            "This script cleans the 'name' field by removing @ symbols and aliases.\n"
        )
        sys.exit(0)

    try:
        fix_name_field()
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback

        traceback.print_exc()

