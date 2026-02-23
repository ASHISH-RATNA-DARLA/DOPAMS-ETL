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


def extract_alias_from_name(name):
    """Extract alias from name that contains @ symbol"""
    if not name or "@" not in name:
        return None, name

    # Split by @ and clean up
    parts = name.split("@")
    if len(parts) >= 2:
        primary_name = parts[0].strip()
        alias = parts[1].strip()

        # Remove additional @ symbols and clean
        alias = re.sub(r"@+", "", alias).strip()

        return alias, primary_name

    return None, name


def extract_relationship_info(name):
    """Extract s/o, d/o, w/o relationship information"""
    if not name:
        return None, None, name

    # Check for s/o (son of)
    so_match = re.search(r"\bs/o\.?\s+([^,]+)", name, re.IGNORECASE)
    if so_match:
        relation_type = "Father"
        relative_name = so_match.group(1).strip()
        # Remove the s/o part from name
        clean_name = re.sub(r"\bs/o\.?\s+[^,]+", "", name, flags=re.IGNORECASE).strip()
        return relation_type, relative_name, clean_name

    # Check for d/o (daughter of)
    do_match = re.search(r"\bd/o\.?\s+([^,]+)", name, re.IGNORECASE)
    if do_match:
        relation_type = "Father"
        relative_name = do_match.group(1).strip()
        clean_name = re.sub(r"\bd/o\.?\s+[^,]+", "", name, flags=re.IGNORECASE).strip()
        return relation_type, relative_name, clean_name

    # Check for w/o (wife of)
    wo_match = re.search(r"\bw/o\.?\s+([^,]+)", name, re.IGNORECASE)
    if wo_match:
        relation_type = "Husband"
        relative_name = wo_match.group(1).strip()
        clean_name = re.sub(r"\bw/o\.?\s+[^,]+", "", name, flags=re.IGNORECASE).strip()
        return relation_type, relative_name, clean_name

    return None, None, name


def clean_name(name):
    """Clean name by removing non-noun fields"""
    if not name:
        return name

    original_name = name

    # Remove absconding status
    name = re.sub(r"\s*\(?\s*absconding\s*\)?\s*", "", name, flags=re.IGNORECASE)

    # Remove r/o (resident of) and everything after it
    name = re.sub(r"\s*,?\s*r/o\s+.*$", "", name, flags=re.IGNORECASE)

    # Remove N/o (native of) and everything after it
    name = re.sub(r"\s*,?\s*N/o\s+.*$", "", name, flags=re.IGNORECASE)

    # Remove age information
    name = re.sub(r",?\s*\d+\s*yrs?\.?\s*", "", name, flags=re.IGNORECASE)
    name = re.sub(
        r",?\s*age\.?\s*[:\s]+\d+\s*yrs?\.?\s*", "", name, flags=re.IGNORECASE
    )

    # Remove caste information
    name = re.sub(r",?\s*caste:\s*[^,]+", "", name, flags=re.IGNORECASE)

    # Remove phone numbers
    name = re.sub(r",?\s*cell:\s*\d+", "", name, flags=re.IGNORECASE)
    name = re.sub(r",?\s*ph\.?\s*no\.?:\s*\d+", "", name, flags=re.IGNORECASE)
    name = re.sub(r",?\s*✆\s*\d+", "", name, flags=re.IGNORECASE)

    # Remove Aadhaar numbers
    name = re.sub(
        r",?\s*\(?adhaar\.?\s*no\.?\s*[\d\s]+\)?", "", name, flags=re.IGNORECASE
    )

    # Remove case markers at the beginning
    name = re.sub(r"^A-?\d+[)\.\s]+", "", name, flags=re.IGNORECASE)

    # Remove "and others"
    name = re.sub(r"\s+and\s+others\s*$", "", name, flags=re.IGNORECASE)

    # Remove parentheses with various content
    name = re.sub(r"\s*\([^)]*receiver[^)]*\)\s*", "", name, flags=re.IGNORECASE)
    name = re.sub(r"\s*\([^)]*drug\s+peddler[^)]*\)\s*", "", name, flags=re.IGNORECASE)

    # Remove vehicle information
    name = re.sub(
        r"\s*owner\s+of\s+(bolero\s+)?vehicle.*$", "", name, flags=re.IGNORECASE
    )
    name = re.sub(r"\s*driver\s+of.*$", "", name, flags=re.IGNORECASE)

    # Remove prisoner information
    name = re.sub(r"\s*under\s+trial\s+prisoner.*?,", "", name, flags=re.IGNORECASE)
    name = re.sub(
        r"\s*\(?\s*UT\s+prisoner\s+no\.?\s*\d+\s*\)?\s*", "", name, flags=re.IGNORECASE
    )

    # Remove CRPF/Battalion info
    name = re.sub(r"\s*CRPF.*$", "", name, flags=re.IGNORECASE)

    # Clean up multiple spaces
    name = re.sub(r"\s+", " ", name)

    # Clean up leading/trailing commas, dots, and spaces
    name = re.sub(r"^[,.\s]+|[,.\s]+$", "", name)

    # Remove empty parentheses
    name = re.sub(r"\(\s*\)", "", name)

    # Final cleanup
    name = name.strip()

    # If name became too short or empty, return original
    if not name or len(name) < 2:
        return original_name

    return name


def fix_person_data():
    """Fix person data by cleaning names and extracting information"""

    conn = connect_to_db()
    cursor = conn.cursor()

    print("\n=== Fixing Person Name Data ===\n")
    print("This script will:")
    print("1. Create 'raw_full_name' column to store original data")
    print("2. Extract aliases from names with @ symbol")
    print("3. Extract relationship information (s/o, d/o, w/o)")
    print("4. Remove non-noun fields from names")
    print("5. Normalize spacing and formatting")
    print("\nStarting in DRY RUN mode - no changes will be made yet.\n")

    # Check if raw_full_name column exists, if not create it
    print("Checking if raw_full_name column exists...")
    cursor.execute(
        """
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name='persons' AND column_name='raw_full_name';
    """
    )

    column_exists = cursor.fetchone()

    if not column_exists:
        print("raw_full_name column does NOT exist. Will be created.")
        create_column = True
    else:
        print("raw_full_name column already exists.")
        create_column = False

    print()

    # Fetch all persons with issues
    cursor.execute(
        """
        SELECT person_id, name, surname, full_name, alias, 
               relative_name, relation_type
        FROM persons
    """
    )

    persons = cursor.fetchall()
    print(f"Total Persons to analyze: {len(persons)}\n")

    updates = []

    for person in persons:
        person_id, name, surname, full_name, alias, relative_name, relation_type = (
            person
        )

        # Work with full_name if available, otherwise name
        original_name = (full_name or name or "").strip()

        if not original_name:
            continue

        # Track changes
        changes = {}
        updated_name = original_name

        # 1. Extract alias if @ symbol present and alias field is empty
        if "@" in updated_name and not alias:
            extracted_alias, name_without_alias = extract_alias_from_name(updated_name)
            if extracted_alias:
                changes["alias"] = extracted_alias
                updated_name = name_without_alias

        # 2. Extract relationship information if present and fields are empty
        if not relative_name or not relation_type:
            rel_type, rel_name, name_without_relation = extract_relationship_info(
                updated_name
            )
            if rel_type and rel_name:
                if not relation_type:
                    changes["relation_type"] = rel_type
                if not relative_name:
                    changes["relative_name"] = rel_name
                updated_name = name_without_relation

        # 3. Clean the name
        cleaned_name = clean_name(updated_name)

        # Only update if there's a significant change
        if cleaned_name != original_name or changes:
            # Always store the original data in raw_full_name if we're making any changes
            if changes:
                changes["raw_full_name"] = original_name
                changes["full_name"] = cleaned_name

                updates.append(
                    {
                        "person_id": person_id,
                        "original_name": original_name,
                        "changes": changes,
                    }
                )

    print(f"Found {len(updates)} records that need updates.\n")

    # Show sample of changes
    print("=== Sample of Proposed Changes (first 20) ===\n")
    for i, update in enumerate(updates[:20], 1):
        print(f"{i}. Person ID: {update['person_id']}")
        print(f"   Original: \"{update['original_name']}\"")
        print(f"   Changes:")
        for field, value in update["changes"].items():
            if field == "raw_full_name":
                print(f'     - {field}: "{value}" (storing original)')
            elif field == "full_name":
                print(f'     - {field}: "{value}" (cleaned)')
            elif field in ["alias", "relation_type", "relative_name"]:
                print(f"     - {field}: {value} (extracted)")
            else:
                print(f"     - {field}: {value}")
        print()

    if len(updates) > 20:
        print(f"... and {len(updates) - 20} more records\n")

    # Ask for confirmation
    print("\n" + "=" * 60)
    print("DRY RUN COMPLETE - No changes have been made yet.")
    print("=" * 60)
    print(f"\nTotal records to update: {len(updates)}")

    # Auto-proceed without confirmation
    print("\nProceeding with updates...\n")

    # Proceed with updates
    print("\n=== Applying Updates ===\n")

    # Create raw_full_name column if it doesn't exist
    if create_column:
        print("Creating raw_full_name column...")
        try:
            cursor.execute(
                """
                ALTER TABLE persons 
                ADD COLUMN raw_full_name TEXT;
            """
            )
            conn.commit()
            print("✓ raw_full_name column created successfully.\n")
        except Exception as e:
            print(f"✗ Error creating column: {e}\n")
            cursor.close()
            conn.close()
            return

    update_count = 0
    error_count = 0

    for update in updates:
        try:
            # Build the UPDATE query dynamically based on changes
            set_clauses = []
            values = []

            for field, value in update["changes"].items():
                set_clauses.append(f"{field} = %s")
                values.append(value)

            # Add person_id for WHERE clause
            values.append(update["person_id"])

            query = f"""
                UPDATE persons
                SET {', '.join(set_clauses)}
                WHERE person_id = %s
            """

            cursor.execute(query, values)
            update_count += 1

            if update_count % 50 == 0:
                print(f"Updated {update_count} records...")
                conn.commit()  # Commit in batches

        except Exception as e:
            error_count += 1
            print(f"Error updating {update['person_id']}: {e}")

    # Final commit
    conn.commit()

    print(f"\n=== Update Complete ===")
    print(f"Successfully updated: {update_count} records")
    print(f"Errors: {error_count} records")

    # Show summary of changes by type
    print("\n=== Summary of Changes ===")

    raw_data_stored = sum(1 for u in updates if "raw_full_name" in u["changes"])
    alias_updates = sum(1 for u in updates if "alias" in u["changes"])
    relation_updates = sum(
        1
        for u in updates
        if "relation_type" in u["changes"] or "relative_name" in u["changes"]
    )
    name_cleanups = sum(1 for u in updates if "full_name" in u["changes"])

    print(f"Original data preserved in raw_full_name: {raw_data_stored}")
    print(f"Aliases extracted: {alias_updates}")
    print(f"Relationship info extracted: {relation_updates}")
    print(f"Names cleaned: {name_cleanups}")

    # Close connection
    cursor.close()
    conn.close()

    print("\n✅ Data fix completed successfully!\n")


if __name__ == "__main__":
    # Show usage if --help is requested
    if "--help" in sys.argv or "-h" in sys.argv:
        print("\n=== Person Name Data Fix Script ===\n")
        print("Usage: python fix_person_names.py [OPTIONS]\n")
        print("Options:")
        print("  --confirm, -y    Auto-confirm and proceed with updates")
        print("  --help, -h       Show this help message\n")
        print("What this script does:")
        print("  1. Creates 'raw_full_name' column to preserve original data")
        print("  2. Extracts aliases from names with @ symbol")
        print("  3. Extracts relationship info (s/o, d/o, w/o)")
        print("  4. Removes non-noun fields from names")
        print("  5. Normalizes spacing and formatting\n")
        sys.exit(0)

    try:
        fix_person_data()
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback

        traceback.print_exc()

