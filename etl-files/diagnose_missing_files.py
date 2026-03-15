#!/usr/bin/env python3
"""
Diagnostic script to analyze why 28,594 out of 28,641 files are missing.

This script:
1. Checks if NFS mount is accessible
2. Counts actual files in each subdirectory
3. Compares database records vs actual files on disk
4. Identifies missing file patterns
5. Provides recommendations for remediation

Run this on the ETL server Linux environment.
"""

import os
import sys
from pathlib import Path
from collections import defaultdict
import subprocess

# Configuration
BASE_MEDIA_PATH = "/mnt/shared-etl-files"

# Expected subdirectories
EXPECTED_SUBDIRS = {
    'crime': 'crimes',
    'person_media': 'person/media',
    'person_identity': 'person/identitydetails',
    'property': 'property',
    'interrogation_media': 'interrogations/media',
    'interrogation_report': 'interrogations/interrogationreport',
    'interrogation_dopams': 'interrogations/dopamsdata',
    'mo_seizures': 'mo_seizures',
    'chargesheets': 'chargesheets',
    'fsl_case_property': 'fsl_case_property',
}


def check_nfs_mount():
    """Verify NFS mount is accessible and mounted."""
    print("\n" + "="*70)
    print("1. CHECK NFS MOUNT STATUS")
    print("="*70)
    
    if not os.path.isdir(BASE_MEDIA_PATH):
        print(f"❌ ERROR: Mount point does not exist: {BASE_MEDIA_PATH}")
        print("   Action: Check if NFS mount is active")
        print("   Command: mount | grep shared-etl-files")
        return False
    
    print(f"✓ Mount point exists: {BASE_MEDIA_PATH}")
    
    # Check if writable
    test_file = os.path.join(BASE_MEDIA_PATH, ".write_test")
    try:
        with open(test_file, 'w') as f:
            f.write("test")
        os.remove(test_file)
        print(f"✓ Mount is writable")
        return True
    except Exception as e:
        print(f"❌ ERROR: Mount is NOT writable: {e}")
        print("   Action: Check NFS permissions and mount options")
        return False


def count_files_by_subdir():
    """Count actual files in each subdirectory."""
    print("\n" + "="*70)
    print("2. COUNT FILES IN EACH SUBDIRECTORY")
    print("="*70)
    
    totals = defaultdict(int)
    subdir_stats = {}
    
    for name, subdir_path in EXPECTED_SUBDIRS.items():
        full_path = os.path.join(BASE_MEDIA_PATH, subdir_path)
        
        if not os.path.isdir(full_path):
            print(f"⚠️  {name:30} : MISSING DIRECTORY")
            totals[name] = 0
            subdir_stats[name] = {'path': full_path, 'count': 0, 'exists': False}
            continue
        
        # Count files (not directories)
        try:
            files = [f for f in os.listdir(full_path) 
                    if os.path.isfile(os.path.join(full_path, f))]
            count = len(files)
            totals[name] = count
            subdir_stats[name] = {'path': full_path, 'count': count, 'exists': True}
            
            status = "✓" if count > 0 else "⚠️ EMPTY"
            print(f"{status} {name:30} : {count:6,d} files")
        except Exception as e:
            print(f"❌ {name:30} : ERROR - {e}")
            totals[name] = 0
            subdir_stats[name] = {'path': full_path, 'count': 0, 'exists': False, 'error': str(e)}
    
    total_files = sum(totals.values())
    print(f"\n{'TOTAL':30} : {total_files:6,d} files")
    
    return subdir_stats, total_files


def analyze_database_vs_disk():
    """Connect to database and compare records vs actual files."""
    print("\n" + "="*70)
    print("3. COMPARE DATABASE RECORDS vs ACTUAL FILES ON DISK")
    print("="*70)
    
    try:
        import psycopg2
        from dotenv import load_dotenv
        
        # Load environment variables
        load_dotenv()
        
        DB_CONFIG = {
            'host': os.getenv('POSTGRES_HOST', '192.168.103.106'),
            'database': os.getenv('POSTGRES_DB', 'dev-2'),
            'user': os.getenv('POSTGRES_USER', 'dev_dopamas'),
            'password': os.getenv('POSTGRES_PASSWORD', ''),
            'port': int(os.getenv('POSTGRES_PORT', 5432))
        }
        
        connection = psycopg2.connect(**DB_CONFIG)
        cursor = connection.cursor()
        
        # Query: Count total records with file_id
        cursor.execute("""
            SELECT COUNT(*) as total_records FROM files WHERE file_id IS NOT NULL
        """)
        total_records = cursor.fetchone()[0]
        
        # Query: Count records by source_type
        cursor.execute("""
            SELECT source_type, COUNT(*) as count 
            FROM files 
            WHERE file_id IS NOT NULL 
            GROUP BY source_type 
            ORDER BY source_type
        """)
        records_by_type = cursor.fetchall()
        
        print(f"\n📊 DATABASE RECORDS:")
        print(f"   Total records with file_id: {total_records:,d}")
        print(f"\n   Breakdown by source_type:")
        for source_type, count in records_by_type:
            print(f"      {source_type:20} : {count:6,d} records")
        
        # Query: Check for NULL extensions
        cursor.execute("""
            SELECT COUNT(*) as count 
            FROM files 
            WHERE file_id IS NOT NULL 
            AND file_url IS NOT NULL
            AND file_url NOT LIKE '%.%'
        """)
        null_ext_count = cursor.fetchone()[0]
        print(f"\n   Records WITHOUT extensions: {null_ext_count:,d}")
        
        cursor.close()
        connection.close()
        
        return total_records, null_ext_count
        
    except Exception as e:
        print(f"❌ ERROR: Could not connect to database: {e}")
        print("   Skipping database comparison")
        return None, None


def print_recommendations(disk_count, db_count, null_ext_count):
    """Print remediation recommendations."""
    print("\n" + "="*70)
    print("4. RECOMMENDATIONS")
    print("="*70)
    
    if disk_count is None or db_count is None:
        print("Unable to analyze - database connection failed")
        return
    
    missing_count = db_count - disk_count
    
    print(f"\n📈 ANALYSIS:")
    print(f"   Database records: {db_count:,d}")
    print(f"   Files on disk:    {disk_count:,d}")
    print(f"   Missing files:    {missing_count:,d} ({(missing_count/db_count*100):.1f}%)")
    
    print(f"\n✅ ACTION PLAN:")
    
    if missing_count > 0:
        print(f"\n   1. CHECK FILE DOWNLOAD PROGRESS")
        print(f"      - Run this diagnostic script again in 1 hour")
        print(f"      - Monitor: ls -lR {BASE_MEDIA_PATH}")
        print(f"      - Check for .partial or .tmp files (incomplete downloads)")
        
        print(f"\n   2. VERIFY NFS MOUNT")
        print(f"      - Command: mount | grep shared-etl-files")
        print(f"      - Command: df -h | grep shared-etl-files")
        print(f"      - Command: showmount -e 192.168.103.106")
        
        print(f"\n   3. CHECK ETL LOGS FOR ERRORS")
        print(f"      - Look for download failures")
        print(f"      - Check network connectivity to download servers")
        
        print(f"\n   4. RE-RUN UPDATE_FILE_EXTENSIONS LATER")
        print(f"      - Command: python3 update_file_urls_with_extensions.py")
        print(f"      - This will process newly downloaded files")
    
    if null_ext_count and null_ext_count > 0:
        print(f"\n   5. FIX EXTENSION PRESERVATION")
        print(f"      - Found {null_ext_count:,d} records without extensions")
        print(f"      - Run: migrate_trigger_preserve_extensions.sql")
        print(f"      - Re-run: update_file_urls_with_extensions.py")
    
    print(f"\n   6. AFTER FILES ARE COMPLETE")
    print(f"      - Verify disk_count == db_count")
    print(f"      - Re-run update_file_urls_with_extensions.py")
    print(f"      - Verify all file_urls have correct extensions")


def main():
    """Main execution."""
    print("\n")
    print("╔" + "="*68 + "╗")
    print("║" + " "*15 + "ETL FILE STORAGE DIAGNOSTIC TOOL" + " "*22 + "║")
    print("╚" + "="*68 + "╝")
    
    # Check mount
    if not check_nfs_mount():
        print("\n❌ CRITICAL: NFS mount is not accessible!")
        print("   Cannot proceed with diagnosis.")
        sys.exit(1)
    
    # Count files
    subdir_stats, disk_count = count_files_by_subdir()
    
    # Analyze database
    db_count, null_ext_count = analyze_database_vs_disk()
    
    # Recommendations
    print_recommendations(disk_count, db_count, null_ext_count)
    
    # Summary
    print("\n" + "="*70)
    print("DIAGNOSTIC COMPLETE")
    print("="*70)
    print("Run this script periodically to monitor file download progress.")
    print("")


if __name__ == "__main__":
    main()
