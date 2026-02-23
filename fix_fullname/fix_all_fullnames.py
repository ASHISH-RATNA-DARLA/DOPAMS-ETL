import psycopg2
import os
import sys
import re
from dotenv import load_dotenv

load_dotenv()

def connect_to_db():
    db_url = os.getenv("DATABASE_URL")
    if "?schema=" in db_url:
        db_url = db_url.split("?schema=")[0]
    return psycopg2.connect(db_url)

def construct_full_name(name, surname, alias):
    """Construct full_name from name, surname, and alias"""
    parts = []
    if name:
        parts.append(name.strip())
    if surname:
        parts.append(surname.strip())
    if alias:
        parts.append(f"@{alias.strip()}")
    
    return " ".join(parts) if parts else None

def clean_full_name(full_name):
    """Clean full_name by removing @ symbols, relationship info, and all metadata"""
    if not full_name:
        return full_name
    
    original = full_name
    
    # Remove @ and everything after it
    if "@" in full_name:
        full_name = full_name.split("@")[0].strip()
    
    # Remove absconding status
    full_name = re.sub(r"\s*\(?\s*absconding\s*\)?\s*", "", full_name, flags=re.IGNORECASE)
    
    # Remove relationship information (s/o, d/o, w/o)
    full_name = re.sub(r"\bs/o\.?\s+[^,]+", "", full_name, flags=re.IGNORECASE).strip()
    full_name = re.sub(r"\bd/o\.?\s+[^,]+", "", full_name, flags=re.IGNORECASE).strip()
    full_name = re.sub(r"\bw/o\.?\s+[^,]+", "", full_name, flags=re.IGNORECASE).strip()
    
    # Remove r/o (resident of) and everything after it
    full_name = re.sub(r"\s*,?\s*r/o\s+.*$", "", full_name, flags=re.IGNORECASE)
    
    # Remove N/o (native of) and everything after it
    full_name = re.sub(r"\s*,?\s*N/o\s+.*$", "", full_name, flags=re.IGNORECASE)
    
    # Remove age information
    full_name = re.sub(r",?\s*\d+\s*yrs?\.?\s*", "", full_name, flags=re.IGNORECASE)
    full_name = re.sub(r",?\s*age\.?\s*[:\s]+\d+\s*yrs?\.?\s*", "", full_name, flags=re.IGNORECASE)
    
    # Remove caste information
    full_name = re.sub(r",?\s*caste:\s*[^,]+", "", full_name, flags=re.IGNORECASE)
    
    # Remove phone numbers
    full_name = re.sub(r",?\s*cell:\s*\d+", "", full_name, flags=re.IGNORECASE)
    full_name = re.sub(r",?\s*ph\.?\s*no\.?:\s*\d+", "", full_name, flags=re.IGNORECASE)
    full_name = re.sub(r",?\s*✆\s*\d+", "", full_name, flags=re.IGNORECASE)
    
    # Remove Aadhaar numbers
    full_name = re.sub(r",?\s*\(?adhaar\.?\s*no\.?\s*[\d\s]+\)?", "", full_name, flags=re.IGNORECASE)
    
    # Remove house numbers (H No, H.No, H/No, etc.)
    full_name = re.sub(r",?\s*H\.?\s*[Nn]o\.?\s*[\d\-\s]+", "", full_name, flags=re.IGNORECASE)
    full_name = re.sub(r",?\s*H/No\.?\s*[\d\-\s]+", "", full_name, flags=re.IGNORECASE)
    
    # Remove case markers at the beginning
    full_name = re.sub(r"^A-?\d+[)\.\s]+", "", full_name, flags=re.IGNORECASE)
    
    # Remove "and others"
    full_name = re.sub(r"\s+and\s+others\s*$", "", full_name, flags=re.IGNORECASE)
    
    # Remove parentheses with various content
    full_name = re.sub(r"\s*\([^)]*receiver[^)]*\)\s*", "", full_name, flags=re.IGNORECASE)
    full_name = re.sub(r"\s*\([^)]*drug\s+peddler[^)]*\)\s*", "", full_name, flags=re.IGNORECASE)
    
    # Remove vehicle information
    full_name = re.sub(r"\s*owner\s+of\s+(bolero\s+)?vehicle.*$", "", full_name, flags=re.IGNORECASE)
    full_name = re.sub(r"\s*driver\s+of.*$", "", full_name, flags=re.IGNORECASE)
    
    # Remove prisoner information
    full_name = re.sub(r"\s*under\s+trial\s+prisoner.*?,", "", full_name, flags=re.IGNORECASE)
    full_name = re.sub(r"\s*\(?\s*UT\s+prisoner\s+no\.?\s*\d+\s*\)?\s*", "", full_name, flags=re.IGNORECASE)
    
    # Remove CRPF/Battalion info
    full_name = re.sub(r"\s*CRPF.*$", "", full_name, flags=re.IGNORECASE)
    
    # Clean up multiple spaces
    full_name = re.sub(r"\s+", " ", full_name)
    
    # Clean up leading/trailing commas, dots, and spaces
    full_name = re.sub(r"^[,.\s]+|[,.\s]+$", "", full_name)
    
    # Remove empty parentheses
    full_name = re.sub(r"\(\s*\)", "", full_name)
    
    # Final cleanup
    full_name = full_name.strip()
    
    # If name became too short or empty, return original
    if not full_name or len(full_name) < 2:
        return original
    
    return full_name

def fix_all_fullnames():
    conn = connect_to_db()
    cursor = conn.cursor()
    
    print("\n=== Fixing ALL full_name Fields ===\n")
    print("This script will:")
    print("1. Clean full_name fields that contain @ symbols")
    print("2. Construct full_name from name, surname, alias when full_name is empty")
    print("3. Remove relationship info (s/o, d/o, w/o)")
    print("4. Remove all metadata: absconding, age, caste, phone, Aadhaar, case markers")
    print("5. Remove location info (r/o, N/o), vehicle/prisoner/CRPF info")
    print("6. Normalize spacing and formatting\n")
    
    # Fetch ALL records to check for cleaning needs
    cursor.execute("""
        SELECT person_id, full_name, raw_full_name, name, surname, alias
        FROM persons
    """)
    
    all_records = cursor.fetchall()
    print(f"Total records in database: {len(all_records)}\n")
    
    # Categorize records
    records_with_at = []
    records_empty = []
    records_needing_cleanup = []
    
    for record in all_records:
        person_id, full_name, raw_full_name, name, surname, alias = record
        
        if full_name and '@' in full_name:
            records_with_at.append(record)
        elif not full_name or full_name.strip() == '':
            records_empty.append(record)
        elif full_name:
            # Check if cleaning produces a different result
            cleaned = clean_full_name(full_name)
            if cleaned != full_name:
                records_needing_cleanup.append(record)
    
    print(f"Records with @ in full_name: {len(records_with_at)}")
    print(f"Records with empty full_name: {len(records_empty)}")
    print(f"Records needing cleanup (metadata removal): {len(records_needing_cleanup)}")
    print(f"Total records to process: {len(records_with_at) + len(records_empty) + len(records_needing_cleanup)}\n")
    
    if not records_with_at and not records_empty and not records_needing_cleanup:
        print("No records to update!")
        cursor.close()
        conn.close()
        return
    
    # Show samples
    print("=== Sample of Proposed Changes (first 15) ===\n")
    sample_count = 0
    
    # Show samples from records with @
    for i, (pid, fname, raw, name, surname, alias) in enumerate(records_with_at[:5], 1):
        clean = clean_full_name(fname)
        print(f"{sample_count + i}. Person ID: {pid}")
        print(f"   Type: Existing full_name with @")
        print(f"   Before: \"{fname}\"")
        print(f"   After:  \"{clean}\"")
        print()
        sample_count += 1
    
    # Show samples from records needing cleanup
    for i, (pid, fname, raw, name, surname, alias) in enumerate(records_needing_cleanup[:5], 1):
        clean = clean_full_name(fname)
        print(f"{sample_count + i}. Person ID: {pid}")
        print(f"   Type: Existing full_name needing cleanup")
        print(f"   Before: \"{fname}\"")
        print(f"   After:  \"{clean}\"")
        print()
        sample_count += 1
    
    # Show samples from empty full_name records
    for i, (pid, fname, raw, name, surname, alias) in enumerate(records_empty[:5], 1):
        constructed = construct_full_name(name, surname, alias)
        if constructed:
            clean = clean_full_name(constructed)
            print(f"{sample_count + i}. Person ID: {pid}")
            print(f"   Type: Empty full_name - constructing from name, surname, alias")
            print(f"   name: \"{name or ''}\", surname: \"{surname or ''}\", alias: \"{alias or ''}\"")
            print(f"   Constructed: \"{constructed}\"")
            print(f"   After cleaning: \"{clean}\"")
            print()
            sample_count += 1
    
    total_to_process = len(records_with_at) + len(records_empty) + len(records_needing_cleanup)
    if total_to_process > 15:
        print(f"... and {total_to_process - 15} more records\n")
    
    # Auto-proceed without confirmation
    print("\nProceeding with updates...\n")
    
    print("\nUpdating...")
    update_count = 0
    error_count = 0
    
    # Process records with @ in full_name
    for person_id, full_name, raw_full_name, name, surname, alias in records_with_at:
        try:
            clean = clean_full_name(full_name)
            
            # If no raw_full_name exists, save original first
            if not raw_full_name:
                cursor.execute("""
                    UPDATE persons
                    SET full_name = %s, raw_full_name = %s
                    WHERE person_id = %s
                """, (clean, full_name, person_id))
            else:
                cursor.execute("""
                    UPDATE persons
                    SET full_name = %s
                    WHERE person_id = %s
                """, (clean, person_id))
            update_count += 1
        except Exception as e:
            error_count += 1
            print(f"Error updating {person_id}: {e}")
    
    # Process records needing cleanup (metadata removal)
    for person_id, full_name, raw_full_name, name, surname, alias in records_needing_cleanup:
        try:
            clean = clean_full_name(full_name)
            
            # If no raw_full_name exists, save original first
            if not raw_full_name:
                cursor.execute("""
                    UPDATE persons
                    SET full_name = %s, raw_full_name = %s
                    WHERE person_id = %s
                """, (clean, full_name, person_id))
            else:
                cursor.execute("""
                    UPDATE persons
                    SET full_name = %s
                    WHERE person_id = %s
                """, (clean, person_id))
            update_count += 1
        except Exception as e:
            error_count += 1
            print(f"Error updating {person_id}: {e}")
    
    # Process records with empty full_name
    for person_id, full_name, raw_full_name, name, surname, alias in records_empty:
        try:
            # Construct full_name from name, surname, alias
            constructed = construct_full_name(name, surname, alias)
            if constructed:
                clean = clean_full_name(constructed)
                
                # Save constructed value to raw_full_name if empty
                if not raw_full_name:
                    cursor.execute("""
                        UPDATE persons
                        SET full_name = %s, raw_full_name = %s
                        WHERE person_id = %s
                    """, (clean, constructed, person_id))
                else:
                    cursor.execute("""
                        UPDATE persons
                        SET full_name = %s
                        WHERE person_id = %s
                    """, (clean, person_id))
                update_count += 1
        except Exception as e:
            error_count += 1
            print(f"Error updating {person_id}: {e}")
    
    conn.commit()
    
    # Verify changes
    cursor.execute("SELECT COUNT(*) FROM persons WHERE full_name LIKE '%@%'")
    remaining_at = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM persons WHERE (full_name IS NULL OR full_name = '') AND (name IS NOT NULL OR surname IS NOT NULL OR alias IS NOT NULL)")
    remaining_empty = cursor.fetchone()[0]
    
    print(f"\n✅ Updated {update_count} records")
    print(f"Errors: {error_count} records")
    print(f"Remaining with @ in full_name: {remaining_at}")
    print(f"Remaining empty full_name (with available name/surname/alias): {remaining_empty}\n")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    fix_all_fullnames()

