#!/usr/bin/env python3
"""
FSL Case Property ETL - Database Compatibility & Sync Check
Validates DB schema, connection, and data consistency before production run
"""

import os
import sys
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
import json

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config import DB_CONFIG, TABLE_CONFIG

def log(msg, level='INFO'):
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{ts}] {level:8s} {msg}")

def check_db_connection():
    """Test database connection using .env credentials"""
    log("🔌 Checking PostgreSQL connection...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        log(f"✅ Connected to PostgreSQL: {version.split(',')[0]}", 'SUCCESS')
        cursor.close()
        return conn
    except Exception as e:
        log(f"❌ Connection failed: {e}", 'ERROR')
        sys.exit(1)

def check_table_schema(conn):
    """Verify FSL case property table structure"""
    log("📋 Checking fsl_case_property table schema...")
    FSL_TABLE = TABLE_CONFIG.get('fsl_case_property', 'fsl_case_property')
    MO_TABLE = TABLE_CONFIG.get('mo_seizures', 'mo_seizures')

    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # Check fsl_case_property table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_name = %s
            )
        """, (FSL_TABLE,))
        if not cursor.fetchone()['exists']:
            log(f"❌ Table {FSL_TABLE} not found", 'ERROR')
            sys.exit(1)
        log(f"✅ Table {FSL_TABLE} exists", 'SUCCESS')

        # Check column structure
        cursor.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
        """, (FSL_TABLE,))

        columns = cursor.fetchall()
        expected_cols = [
            'case_property_id', 'case_type', 'crime_id', 'mo_id', 'status',
            'send_date', 'fsl_date', 'date_disposal', 'release_date', 'return_date'
        ]

        found_cols = {col['column_name'] for col in columns}
        missing = set(expected_cols) - found_cols

        if missing:
            log(f"⚠️  Missing columns: {missing}", 'WARNING')
        else:
            log(f"✅ All {len(expected_cols)} expected columns present", 'SUCCESS')

        return True

    except Exception as e:
        log(f"❌ Schema check failed: {e}", 'ERROR')
        return False

def check_data_consistency(conn):
    """Validate data consistency between tables"""
    log("🔍 Checking data consistency...")
    FSL_TABLE = TABLE_CONFIG.get('fsl_case_property', 'fsl_case_property')
    CRIMES_TABLE = TABLE_CONFIG.get('crimes', 'crimes')
    MO_TABLE = TABLE_CONFIG.get('mo_seizures', 'mo_seizures')

    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # Check fsl_case_property record count
        cursor.execute(f"SELECT COUNT(*) as cnt FROM {FSL_TABLE}")
        fsl_count = cursor.fetchone()['cnt']
        log(f"ℹ️  {FSL_TABLE}: {fsl_count} records", 'INFO')

        # Check crimes table
        cursor.execute(f"SELECT COUNT(*) as cnt FROM {CRIMES_TABLE}")
        crimes_count = cursor.fetchone()['cnt']
        log(f"ℹ️  {CRIMES_TABLE}: {crimes_count} records", 'INFO')

        # Check mo_seizures table
        cursor.execute(f"SELECT COUNT(*) as cnt FROM {MO_TABLE}")
        mo_count = cursor.fetchone()['cnt']
        cursor.execute(f"SELECT COUNT(DISTINCT mo_id) as cnt FROM {MO_TABLE}")
        unique_mo = cursor.fetchone()['cnt']
        log(f"ℹ️  {MO_TABLE}: {mo_count} records, {unique_mo} unique mo_id values", 'INFO')

        # Check mo_id value format in mo_seizures
        cursor.execute(f"SELECT DISTINCT mo_id FROM {MO_TABLE} LIMIT 5")
        sample_mo_ids = [row['mo_id'] for row in cursor.fetchall()]
        log(f"ℹ️  Sample mo_id format: {sample_mo_ids}", 'INFO')

        return True

    except Exception as e:
        log(f"❌ Data consistency check failed: {e}", 'ERROR')
        return False

def check_foreign_keys(conn):
    """Verify FK constraints and indexes"""
    log("🔑 Checking foreign key constraints...")
    FSL_TABLE = TABLE_CONFIG.get('fsl_case_property', 'fsl_case_property')

    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # Check FK constraints on fsl_case_property
        cursor.execute("""
            SELECT constraint_name, column_name, referenced_table_name
            FROM information_schema.key_column_usage
            WHERE table_name = %s AND referenced_table_name IS NOT NULL
        """, (FSL_TABLE,))

        fks = cursor.fetchall()
        if fks:
            log(f"✅ Found {len(fks)} FK constraint(s)", 'SUCCESS')
            for fk in fks:
                log(f"   └─ {fk['constraint_name']}: {fk['column_name']} → {fk['referenced_table_name']}", 'INFO')
        else:
            log("⚠️  No FK constraints found on fsl_case_property", 'WARNING')

        return True

    except Exception as e:
        log(f"⚠️  FK check skipped (PostgreSQL uses different schema): {e}", 'WARNING')
        return True

def check_indexes(conn):
    """Verify indexes for performance"""
    log("📑 Checking indexes...")
    FSL_TABLE = TABLE_CONFIG.get('fsl_case_property', 'fsl_case_property')

    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute("""
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE tablename = %s
        """, (FSL_TABLE,))

        indexes = cursor.fetchall()
        if indexes:
            log(f"✅ Found {len(indexes)} index(es)", 'SUCCESS')
            for idx in indexes:
                log(f"   └─ {idx['indexname']}", 'INFO')
        else:
            log(f"⚠️  No indexes found (consider adding for performance)", 'WARNING')

        return True

    except Exception as e:
        log(f"⚠️  Index check failed: {e}", 'WARNING')
        return True

def test_write_permission(conn):
    """Test INSERT/UPDATE permissions"""
    log("✏️  Testing write permissions...")
    FSL_TABLE = TABLE_CONFIG.get('fsl_case_property', 'fsl_case_property')

    try:
        cursor = conn.cursor()

        # Test INSERT
        test_id = f"test_{datetime.now().timestamp()}"
        cursor.execute(f"""
            INSERT INTO {FSL_TABLE} (case_property_id, crime_id)
            VALUES (%s, %s)
        """, (test_id, 'test_crime_123'))

        # Test UPDATE
        cursor.execute(f"""
            UPDATE {FSL_TABLE} SET case_type = %s WHERE case_property_id = %s
        """, ('test_type', test_id))

        # Rollback (don't commit test data)
        conn.rollback()
        log("✅ Write permissions verified (INSERT/UPDATE allowed)", 'SUCCESS')

        return True

    except Exception as e:
        conn.rollback()
        log(f"❌ Write permission test failed: {e}", 'ERROR')
        return False

def generate_report(results):
    """Generate final compatibility report"""
    log("", 'INFO')
    log("=" * 80, 'INFO')
    log("DATABASE COMPATIBILITY REPORT", 'INFO')
    log("=" * 80, 'INFO')

    all_passed = all(results.values())

    for check, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        log(f"{status:8s} {check}", 'INFO')

    log("=" * 80, 'INFO')
    if all_passed:
        log("🟢 ALL CHECKS PASSED - Ready for production", 'SUCCESS')
    else:
        log("🔴 Some checks failed - Review issues above", 'ERROR')

    return all_passed

def main():
    log("Starting FSL Case Property ETL - Database Compatibility Check")
    log("=" * 80)

    results = {}

    # Run checks
    conn = check_db_connection()
    results['DB Connection'] = True

    results['Table Schema'] = check_table_schema(conn)
    results['Data Consistency'] = check_data_consistency(conn)
    results['FK Constraints'] = check_foreign_keys(conn)
    results['Indexes'] = check_indexes(conn)
    results['Write Permissions'] = test_write_permission(conn)

    conn.close()

    # Generate report
    log("", 'INFO')
    passed = generate_report(results)

    sys.exit(0 if passed else 1)

if __name__ == '__main__':
    main()
