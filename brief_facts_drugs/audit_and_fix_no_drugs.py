#!/usr/bin/env python3
"""
DOPAMS ETL Audit & Fix Script
Cleans NO_DRUGS_DETECTED pollution and adds monitoring infrastructure

Usage:
    python audit_and_fix_no_drugs.py --audit      # Show what's contaminated
    python audit_and_fix_no_drugs.py --clean      # Remove NO_DRUGS_DETECTED
    python audit_and_fix_no_drugs.py --verify     # Verify cleanup
"""

import os
import sys
import argparse
from datetime import datetime

os.chdir(os.path.dirname(__file__))
sys.path.insert(0, os.path.dirname(__file__))

try:
    from db import get_connection
    import logging
except Exception as e:
    print(f"❌ Import error: {e}")
    print("Make sure you're running from brief_facts_drugs/ directory")
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)


def audit_contamination():
    """Show NO_DRUGS_DETECTED contamination stats."""
    logger.info("=" * 70)
    logger.info("CONTAMINATION AUDIT")
    logger.info("=" * 70)
    
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        # Overall stats
        cur.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN primary_drug_name = 'NO_DRUGS_DETECTED' THEN 1 END) as no_drugs_count,
                ROUND(100.0 * COUNT(CASE WHEN primary_drug_name = 'NO_DRUGS_DETECTED' THEN 1 END) 
                      / NULLIF(COUNT(*), 0), 1) as pollution_percent
            FROM public.brief_facts_drug
        """)
        
        total, no_drugs, pollution = cur.fetchone()
        
        logger.info(f"\n📊 Overall Statistics:")
        logger.info(f"  Total records:           {total:,}")
        logger.info(f"  NO_DRUGS_DETECTED:       {no_drugs:,}")
        logger.info(f"  Pollution rate:          {pollution if pollution else 0}%")
        
        if no_drugs == 0:
            logger.info("\n✅ No contamination detected!")
            conn.close()
            return
        
        # By crime
        logger.info(f"\n📍 Top 20 crimes with most NO_DRUGS_DETECTED entries:")
        cur.execute("""
            SELECT 
                crime_id,
                COUNT(*) as count,
                MAX(created_at) as latest
            FROM public.brief_facts_drug
            WHERE primary_drug_name = 'NO_DRUGS_DETECTED'
            GROUP BY crime_id
            ORDER BY count DESC
            LIMIT 20
        """)
        
        for crime_id, count, latest in cur.fetchall():
            logger.info(f"  {crime_id:20s} → {count:3d} entries (last: {latest})")
        
        # Sample contaminated crime
        logger.info(f"\n🔍 Sample contaminated crime record:")
        cur.execute("""
            SELECT 
                id, crime_id, primary_drug_name, confidence_score, 
                extraction_metadata, created_at
            FROM public.brief_facts_drug
            WHERE primary_drug_name = 'NO_DRUGS_DETECTED'
            LIMIT 1
        """)
        
        if row := cur.fetchone():
            logger.info(f"  ID:                {row[0]}")
            logger.info(f"  Crime ID:          {row[1]}")
            logger.info(f"  Primary Drug:      {row[2]}")
            logger.info(f"  Confidence:        {row[3]}")
            logger.info(f"  Metadata:          {row[4]}")
            logger.info(f"  Created:           {row[5]}")
        
        logger.info("\n" + "=" * 70)
        
    finally:
        conn.close()


def clean_contamination():
    """Remove NO_DRUGS_DETECTED records (BE CAREFUL!)."""
    logger.info("=" * 70)
    logger.info("CONTAMINATION CLEANUP")
    logger.info("=" * 70)
    
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        # First, get count
        cur.execute("""
            SELECT COUNT(*) FROM public.brief_facts_drug
            WHERE primary_drug_name = 'NO_DRUGS_DETECTED'
        """)
        
        count = cur.fetchone()[0]
        
        if count == 0:
            logger.info("✅ No NO_DRUGS_DETECTED records to clean")
            return
        
        logger.warning(f"⚠️  About to DELETE {count:,} NO_DRUGS_DETECTED records")
        logger.warning("   This is PERMANENT. Make sure you have a backup!")
        
        response = input("\n❓ Are you SURE? Type 'YES' to proceed: ")
        if response != "YES":
            logger.info("❌ Aborted")
            return
        
        # Create backup table first
        logger.info("📋 Creating backup table...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS brief_facts_drug_no_drugs_backup_2026 AS
            SELECT * FROM public.brief_facts_drug
            WHERE primary_drug_name = 'NO_DRUGS_DETECTED'
        """)
        
        cur.execute("""SELECT COUNT(*) FROM brief_facts_drug_no_drugs_backup_2026""")
        backup_count = cur.fetchone()[0]
        logger.info(f"  ✅ Backed up {backup_count:,} records")
        
        # Delete them
        logger.info(f"🗑️  Deleting {count:,} NO_DRUGS_DETECTED records...")
        cur.execute("""
            DELETE FROM public.brief_facts_drug
            WHERE primary_drug_name = 'NO_DRUGS_DETECTED'
        """)
        
        deleted = cur.rowcount
        logger.info(f"  ✅ Deleted {deleted:,} records")
        
        # Commit
        conn.commit()
        logger.info(f"\n✅ Cleanup complete!")
        logger.info(f"   Backup table: brief_facts_drug_no_drugs_backup_2026")
        logger.info(f"   To restore: INSERT INTO brief_facts_drug SELECT * FROM brief_facts_drug_no_drugs_backup_2026")
        
    except Exception as e:
        logger.error(f"❌ Error during cleanup: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


def verify_cleanup():
    """Verify NO_DRUGS_DETECTED are gone."""
    logger.info("=" * 70)
    logger.info("CLEANUP VERIFICATION")
    logger.info("=" * 70)
    
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT COUNT(*) FROM public.brief_facts_drug
            WHERE primary_drug_name = 'NO_DRUGS_DETECTED'
        """)
        
        remaining = cur.fetchone()[0]
        
        if remaining == 0:
            logger.info("✅ SUCCESS: No NO_DRUGS_DETECTED records found")
            logger.info("\nDatabase is now clean!")
        else:
            logger.warning(f"⚠️  WARNING: {remaining:,} NO_DRUGS_DETECTED records still in DB")
        
        # Show overall stats
        cur.execute("SELECT COUNT(*) FROM public.brief_facts_drug")
        total = cur.fetchone()[0]
        logger.info(f"\nTotal records in database: {total:,}")
        
    finally:
        conn.close()


def create_audit_infrastructure():
    """Create tables for tracking filtered/rejected drugs."""
    logger.info("=" * 70)
    logger.info("CREATE AUDIT INFRASTRUCTURE")
    logger.info("=" * 70)
    
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        logger.info("📋 Creating extraction_filter_log table...")
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS extraction_filter_log (
                id SERIAL PRIMARY KEY,
                crime_id VARCHAR(50),
                filter_reason VARCHAR(100),
                raw_drug_name TEXT,
                confidence_score NUMERIC(3,2),
                preprocessing_score INT,
                extraction_metadata JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                CONSTRAINT fk_crime_filter FOREIGN KEY (crime_id)
                    REFERENCES crimes(crime_id) ON DELETE CASCADE
            )
        """)
        
        logger.info("  ✅ extraction_filter_log created")
        
        # Create indexes
        logger.info("📋 Creating indexes...")
        
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_filter_log_reason 
            ON extraction_filter_log(filter_reason)
        """)
        
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_filter_log_crime 
            ON extraction_filter_log(crime_id)
        """)
        
        logger.info("  ✅ Indexes created")
        
        conn.commit()
        logger.info("\n✅ Audit infrastructure ready!")
        logger.info("\nYou can now track why drugs are being filtered:")
        logger.info("  SELECT filter_reason, COUNT(*) FROM extraction_filter_log GROUP BY 1;")
        
    except Exception as e:
        logger.error(f"❌ Error creating infrastructure: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


def show_summary():
    """Show summary of what was fixed."""
    logger.info("\n" + "=" * 70)
    logger.info("SUMMARY OF FIXES")
    logger.info("=" * 70)
    
    logger.info("""
✅ FIXED Issues:

1. ✅ NO_DRUGS_PLACEHOLDER Removal
   - main.py: Lines 189-208 updated
   - Crimes with no drugs NO LONGER insert fake "NO_DRUGS_DETECTED" records
   - They are simply SKIPPED instead
   - Result: New ETL runs will have ZERO contamination

2. ✅ Preprocessing Threshold Lowered
   - extractor.py: Threshold changed from 50 → 30
   - Now catches drug sections with just 2 Tier-2 keywords
   - Reduces false negatives (legitimate drugs filtered out)
   - Result: More drugs extracted, fewer missed

3. ✅ Audit Logging Added
   - extractor.py: Debug logging of filtered sections
   - Shows what's being dropped and why
   - Result: Can identify missed drugs

⏳ OPTIONAL But Recommended:

4. 🗑️  Clean Historical Contamination
   - Run: python audit_and_fix_no_drugs.py --clean
   - Removes all NO_DRUGS_DETECTED from database
   - Creates backup table for recovery

5. 📋 Create Audit Infrastructure
   - Run: python audit_and_fix_no_drugs.py --create-audit
   - Creates extraction_filter_log table
   - Tracks every filtered/rejected drug
   - Helps identify patterns

⚠️  IMPORTANT: 

- These fixes are BACKWARD COMPATIBLE
- Will NOT break existing pipeline
- New ETL runs will automatically use the fixes
- Old contaminated data can be cleaned separately
    """)
    
    logger.info("=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description="Audit and fix NO_DRUGS_DETECTED contamination",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python audit_and_fix_no_drugs.py --audit        # Show contamination
  python audit_and_fix_no_drugs.py --clean        # Remove contamination
  python audit_and_fix_no_drugs.py --verify       # Verify cleanup
  python audit_and_fix_no_drugs.py --create-audit # Create infrastructure
"""
    )
    
    parser.add_argument('--audit', action='store_true', help='Audit contamination')
    parser.add_argument('--clean', action='store_true', help='Remove contamination (PERMANENT!)')
    parser.add_argument('--verify', action='store_true', help='Verify cleanup')
    parser.add_argument('--create-audit', action='store_true', help='Create audit infrastructure')
    parser.add_argument('--summary', action='store_true', help='Show fix summary')
    
    args = parser.parse_args()
    
    # If no args, show summary
    if not any(vars(args).values()):
        show_summary()
        return
    
    if args.summary:
        show_summary()
    elif args.audit:
        audit_contamination()
    elif args.clean:
        clean_contamination()
    elif args.verify:
        verify_cleanup()
    elif args.create_audit:
        create_audit_infrastructure()


if __name__ == '__main__':
    main()
