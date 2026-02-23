import psycopg2
import os
import re
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def connect_to_db():
    """Connect to PostgreSQL database"""
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise Exception("DATABASE_URL not found in environment variables")

    # Remove schema parameter from URL as psycopg2 doesn't recognize it
    if "?schema=" in db_url:
        db_url = db_url.split("?schema=")[0]

    return psycopg2.connect(db_url)


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

    conn = connect_to_db()
    cursor = conn.cursor()

    print("\n=== Fixing 'surname' Field ===\n")
    print("This script will clean the 'surname' field by removing @ symbols\n")

    # Find all records where surname has @
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
        cursor.close()
        conn.close()
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

    for person_id, name, surname, full_name in records:
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

            update_count += 1

            if update_count % 50 == 0:
                print(f"Updated {update_count} records...")
                conn.commit()  # Commit in batches

        except Exception as e:
            error_count += 1
            print(f"Error updating {person_id}: {e}")

    # Final commit
    conn.commit()

    print(f"\n=== Update Complete ===")
    print(f"Successfully updated: {update_count} records")
    print(f"Errors: {error_count} records")

    # Verify the changes
    print("\n=== Verifying Changes ===\n")
    cursor.execute(
        """
        SELECT COUNT(*) 
        FROM persons 
        WHERE surname LIKE '%@%'
    """
    )
    remaining = cursor.fetchone()[0]

    print(f"Records with @ in 'surname' field after cleanup: {remaining}")

    # Close connection
    cursor.close()
    conn.close()

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

