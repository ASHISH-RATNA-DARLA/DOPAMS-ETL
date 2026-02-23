#!/usr/bin/env python3
"""
Diagnostic script to check why new APIs (mo_seizures, chargesheets, case_property) 
files aren't being downloaded by the files media server.
"""
import os
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database configuration
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'database': os.getenv('POSTGRES_DB', 'dopamasuprddb'),
    'user': os.getenv('POSTGRES_USER', 'dopamasprd_ur'),
    'password': os.getenv('POSTGRES_PASSWORD', ''),
    'port': int(os.getenv('POSTGRES_PORT', 5432))
}

# Import mapping function from files media server
sys.path.insert(0, str(Path(__file__).parent / 'etl_files_media_server'))
from main import map_destination_subdir

def check_new_apis_files():
    """Check if files from new APIs exist and if mapping works"""
    
    print("="*80)
    print("DIAGNOSING NEW APIs FILES")
    print("="*80)
    
    try:
        # Connect to database
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # 1. Check if files exist in database
        print("\n1. Checking if files exist in database...")
        cursor.execute("""
            SELECT 
                source_type,
                source_field,
                COUNT(*) as total_records,
                COUNT(CASE WHEN file_id IS NOT NULL THEN 1 END) as records_with_file_id,
                COUNT(CASE WHEN is_downloaded = TRUE THEN 1 END) as already_downloaded,
                COUNT(CASE WHEN is_downloaded IS NULL OR is_downloaded = FALSE THEN 1 END) as pending_download
            FROM files
            WHERE source_type IN ('mo_seizures', 'chargesheets', 'case_property')
            GROUP BY source_type, source_field
            ORDER BY source_type, source_field
        """)
        
        results = cursor.fetchall()
        if not results:
            print("   ❌ NO FILES FOUND for new APIs in database!")
            print("   → The ETL pipeline may not have inserted any records yet.")
            print("   → Check if ETL pipeline ran successfully for these APIs.")
            return
        else:
            print("   ✓ Files found:")
            for row in results:
                source_type, source_field, total, with_file_id, downloaded, pending = row
                print(f"      - {source_type}/{source_field}: {total} total, {with_file_id} with file_id, {downloaded} downloaded, {pending} pending")
        
        # 2. Check files that should be processed by files media server
        print("\n2. Checking files queued for download...")
        cursor.execute("""
            SELECT 
                source_type,
                source_field,
                COUNT(*) as files_to_download
            FROM files
            WHERE source_type IN ('mo_seizures', 'chargesheets', 'case_property')
              AND file_id IS NOT NULL
              AND has_field IS TRUE
              AND is_empty IS FALSE
              AND (is_downloaded IS NULL OR is_downloaded = FALSE)
            GROUP BY source_type, source_field
            ORDER BY source_type, source_field
        """)
        
        queued_results = cursor.fetchall()
        if not queued_results:
            print("   ⚠️  NO FILES QUEUED for download!")
            print("   → All files may already be marked as downloaded, or")
            print("   → Files may have has_field=FALSE or is_empty=TRUE")
        else:
            print("   ✓ Files queued for download:")
            for row in queued_results:
                source_type, source_field, count = row
                print(f"      - {source_type}/{source_field}: {count} files")
        
        # 3. Test mapping function with actual database values
        print("\n3. Testing mapping function with actual database values...")
        cursor.execute("""
            SELECT DISTINCT source_type, source_field
            FROM files
            WHERE source_type IN ('mo_seizures', 'chargesheets', 'case_property')
              AND file_id IS NOT NULL
            ORDER BY source_type, source_field
        """)
        
        mapping_results = cursor.fetchall()
        if mapping_results:
            print("   Testing mappings:")
            for source_type, source_field in mapping_results:
                mapped_path = map_destination_subdir(source_type, source_field)
                if mapped_path:
                    print(f"      ✓ {source_type}/{source_field} → {mapped_path}")
                else:
                    print(f"      ❌ {source_type}/{source_field} → NO MAPPING (will be skipped!)")
                    print(f"         → This is why files aren't downloading!")
        
        # 4. Show sample records
        print("\n4. Sample records from new APIs:")
        cursor.execute("""
            SELECT 
                source_type,
                source_field,
                parent_id,
                file_id,
                has_field,
                is_empty,
                is_downloaded,
                file_path
            FROM files
            WHERE source_type IN ('mo_seizures', 'chargesheets', 'case_property')
              AND file_id IS NOT NULL
            ORDER BY source_type, source_field
            LIMIT 5
        """)
        
        sample_results = cursor.fetchall()
        if sample_results:
            print("   Sample records:")
            for row in sample_results:
                source_type, source_field, parent_id, file_id, has_field, is_empty, is_downloaded, file_path = row
                print(f"      - {source_type}/{source_field}")
                print(f"        parent_id: {parent_id}")
                print(f"        file_id: {file_id}")
                print(f"        has_field: {has_field}, is_empty: {is_empty}, is_downloaded: {is_downloaded}")
                print(f"        file_path: {file_path}")
        else:
            print("   No sample records found")
        
        # 5. Check if files media server query would find them
        print("\n5. Testing files media server query...")
        cursor.execute("""
            SELECT source_type, source_field, file_id
            FROM files
            WHERE file_id IS NOT NULL
              AND has_field IS TRUE
              AND is_empty IS FALSE
              AND (is_downloaded IS NULL OR is_downloaded = FALSE)
              AND source_type IN ('mo_seizures', 'chargesheets', 'case_property')
            ORDER BY source_type, source_field, file_id
            LIMIT 10
        """)
        
        server_query_results = cursor.fetchall()
        if server_query_results:
            print(f"   ✓ Files media server would find {len(server_query_results)} files (showing first 10):")
            for source_type, source_field, file_id in server_query_results[:5]:
                mapped = map_destination_subdir(source_type, source_field)
                status = "✓ MAPPED" if mapped else "❌ NO MAPPING"
                print(f"      {status}: {source_type}/{source_field} → file_id={file_id}")
        else:
            print("   ❌ Files media server query returns NO RESULTS")
            print("   → Check has_field, is_empty, and is_downloaded columns")
        
        cursor.close()
        conn.close()
        
        print("\n" + "="*80)
        print("DIAGNOSIS COMPLETE")
        print("="*80)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_new_apis_files()

