import psycopg2
import os
import re
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


def clean_surname(surname):
    """Clean surname by removing @ and everything after it"""
    if not surname:
        return surname

    # If surname starts with @, it's just an alias marker, remove it all
    if surname.strip().startswith("@"):
        return ""

    # If @ is in the middle, take only the part before @
    if "@" in surname:
        parts = surname.split("@")
        clean = parts[0].strip()
        return clean if clean else ""

    return surname


def fix_surname_field():
    """Fix the surname field by removing @ symbols"""

    pool = connect_to_db()

    print("\n=== Fixing 'surname' Field ===\n")
    print("This script will clean the 'surname' field by removing @ symbols\n")

    # Find all records where surname has @
    with pool.get_connection_context() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT person_id, name, surname, full_name
                FROM persons
                WHERE surname LIKE '%@%'
                ORDER BY person_id
            """
            )
        
            records = cursor.fetchall()
            
    print(f"Found {len(records)} records where 'surname' field has @ symbol\n")

    if len(records) == 0:
        print("No records to update!")
        return

    # Show sample
    print("=== Sample of Proposed Changes (first 20) ===\n")
    for i, (person_id, name, surname, full_name) in enumerate(records[:20], 1):
        clean = clean_surname(surname)
        print(f"{i}. Person ID: {person_id}")
        print(f'   name: "{name}"')
        print(f'   Current surname: "{surname}"')
        print(
            f"   Will become: \"{clean}\" {'' if clean else '(empty - alias marker)'}"
        )
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
                for person_id, name, surname, full_name in batch:
                    try:
                        # Clean the surname
                        clean = clean_surname(surname)

                        # Update the surname field (can be empty string)
                        cursor.execute(
                            """
                            UPDATE persons
                            SET surname = %s
                            WHERE person_id = %s
                            """,
                            (clean, person_id),
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
                WHERE surname LIKE '%@%'
            """
            )
            remaining = cursor.fetchone()[0]

    print(f"Records with @ in 'surname' field after cleanup: {remaining}")

    print("\n✅ Surname field cleanup completed!\n")


if __name__ == "__main__":
    # Show usage if --help is requested
    if "--help" in sys.argv or "-h" in sys.argv:
        print("\n=== Fix 'surname' Field Script ===\n")
        print("Usage: python fix_surname_field.py [OPTIONS]\n")
        print("Options:")
        print("  --confirm, -y    Auto-confirm and proceed with updates")
        print("  --help, -h       Show this help message\n")
        print("This script cleans the 'surname' field by removing @ symbols.\n")
        sys.exit(0)

    try:
        fix_surname_field()
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback

        traceback.print_exc()

