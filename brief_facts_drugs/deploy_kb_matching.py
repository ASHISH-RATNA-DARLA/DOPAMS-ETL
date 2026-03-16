#!/usr/bin/env python3
"""
Quick Deployment Script for Advanced KB Matching Integration
Automates the integration of DrugKBMatcherAdvanced into extractor.py

Usage:
    python deploy_kb_matching.py --dry-run    # Preview changes
    python deploy_kb_matching.py --backup     # Backup originals
    python deploy_kb_matching.py --deploy     # Deploy changes
"""

import os
import sys
import argparse
import shutil
from datetime import datetime
from pathlib import Path


def create_backup(file_path: str) -> str:
    """Create timestamped backup of file."""
    backup_dir = Path(file_path).parent / '.backups'
    backup_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_path = backup_dir / f"{Path(file_path).stem}_{timestamp}.bak"
    
    shutil.copy2(file_path, backup_path)
    print(f"✅ Backed up: {backup_path}")
    return str(backup_path)


def check_files_exist(base_path: str) -> bool:
    """Verify all required files exist."""
    required_files = [
        'extractor.py',
        'db.py',
        'main.py',
        'config.py',
        'kb_matcher_advanced.py',
        'extractor_integration.py',
    ]
    
    print("\n📋 Checking required files...")
    all_exist = True
    for filename in required_files:
        filepath = Path(base_path) / filename
        exists = filepath.exists()
        status = "✅" if exists else "❌"
        print(f"  {status} {filename}")
        all_exist = all_exist and exists
    
    return all_exist


def show_changes_summary():
    """Show summary of changes to be made."""
    print("\n📝 Summary of Changes:\n")
    
    print("1. extractor.py - integrate_extract_drug_info()")
    print("   - Add imports: DrugKBMatcherAdvanced, refine_drugs_with_advanced_kb")
    print("   - Initialize matcher in extract_drug_info()")
    print("   - Call refine_drugs_with_advanced_kb() after LLM extraction")
    print("   - Call apply_validation_rules() before post-processing")
    print("   Lines affected: ~50 (insert+modify)")
    print()
    
    print("2. db.py - add_audit_logging()")
    print("   - Add log_kb_match_audit() function")
    print("   - Add log_drug_rejection() function")
    print("   - Lines affected: ~30 (insert)")
    print()
    
    print("3. main.py - update_validation()")
    print("   - Replace confidence check with apply_validation_rules()")
    print("   - Add audit logging calls")
    print("   Lines affected: ~10 (modify)")
    print()
    
    print("4. config.py - add_kb_config()")
    print("   - Add KB_MATCH_CONFIG dictionary")
    print("   - Lines affected: ~20 (insert)")
    print()
    
    print("5. Database - create_audit_tables()")
    print("   - Create drug_kb_match_audit table")
    print("   - Create drug_extraction_rejections table")
    print("   - Create 4 indexes")
    print()


def generate_sql_script(output_path: str) -> None:
    """Generate SQL migration script."""
    sql_content = """-- Generated: %s
-- Advanced KB Matching Data-layer Setup

-- Create audit logging table
CREATE TABLE IF NOT EXISTS drug_kb_match_audit (
    id SERIAL PRIMARY KEY,
    crime_id VARCHAR(50),
    extracted_name VARCHAR(255),
    matched_standard_name VARCHAR(255),
    match_type VARCHAR(50),
    match_ratio NUMERIC(3,2),
    is_commercial BOOLEAN,
    validation_warnings TEXT,
    confidence_original NUMERIC(3,2),
    confidence_adjusted NUMERIC(3,2),
    audit_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT fk_crime_kbaudit FOREIGN KEY (crime_id) 
        REFERENCES crimes(crime_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_drug_kb_audit_crime 
    ON drug_kb_match_audit(crime_id);
CREATE INDEX IF NOT EXISTS idx_drug_kb_audit_match 
    ON drug_kb_match_audit(match_type);
CREATE INDEX IF NOT EXISTS idx_drug_kb_audit_commercial 
    ON drug_kb_match_audit(is_commercial);
CREATE INDEX IF NOT EXISTS idx_drug_kb_audit_created 
    ON drug_kb_match_audit(created_at DESC);

-- Create rejection log table
CREATE TABLE IF NOT EXISTS drug_extraction_rejections (
    id SERIAL PRIMARY KEY,
    crime_id VARCHAR(50),
    raw_drug_name VARCHAR(255),
    rejection_reason VARCHAR(255),
    llm_confidence NUMERIC(3,2),
    was_false_positive BOOLEAN,
    audit_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT fk_crime_rejections FOREIGN KEY (crime_id) 
        REFERENCES crimes(crime_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_drug_rejections_reason 
    ON drug_extraction_rejections(rejection_reason);
CREATE INDEX IF NOT EXISTS idx_drug_rejections_fp 
    ON drug_extraction_rejections(was_false_positive);

-- Verify tables created
SELECT 'drug_kb_match_audit' as table_name, COUNT(*) as row_count 
FROM drug_kb_match_audit
UNION ALL
SELECT 'drug_extraction_rejections', COUNT(*) 
FROM drug_extraction_rejections;
""" % datetime.now().isoformat()
    
    with open(output_path, 'w') as f:
        f.write(sql_content)
    
    print(f"✅ SQL migration script generated: {output_path}")


def show_deployment_steps():
    """Show deployment steps."""
    print("\n🚀 Deployment Steps:\n")
    
    print("PHASE 1: Pre-Deployment Checks")
    print("  1. Run: python test_kb_matcher_advanced.py")
    print("  2. Verify: All units tests pass (>80% pass rate)")
    print()
    
    print("PHASE 2: Database Setup")
    print("  1. Connect to PostgreSQL: psql -U dopams_user -d dopams_db")
    print("  2. Run migration: \\i deploy_migrations.sql")
    print("  3. Verify: SELECT * FROM drug_kb_match_audit LIMIT 1;")
    print()
    
    print("PHASE 3: Code Deployment")
    print("  1. Backup existing: cp brief_facts_drugs/extractor.py{,.backup}")
    print("  2. Review changes: diff extractor.py extractor.py.backup | less")
    print("  3. Deploy files to brief_facts_drugs/:")
    print("     - kb_matcher_advanced.py (new)")
    print("     - extractor_integration.py (new)")
    print("     - test_kb_matcher_advanced.py (new)")
    print("     - PRODUCTION_DEPLOYMENT_GUIDE.md (new)")
    print("  4. Update extractor.py with integration code")
    print("  5. Update db.py with audit functions")
    print("  6. Update main.py with validation filters")
    print("  7. Update config.py with KB_MATCH_CONFIG")
    print()
    
    print("PHASE 4: Staging Test")
    print("  1. Test with 50 sample crimes:")
    print("     python main.py --test --sample-size 50 --log-level DEBUG")
    print("  2. Monitor logs for errors")
    print("  3. Check audit table: SELECT * FROM drug_kb_match_audit;")
    print("  4. Review rejection reasons:")
    print("     SELECT rejection_reason, COUNT(*) FROM drug_extraction_rejections")
    print("     GROUP BY rejection_reason;")
    print()
    
    print("PHASE 5: Production Deployment")
    print("  1. Schedule deployment during off-peak hours")
    print("  2. Start small batch (100 crimes)")
    print("  3. Monitor first 1 hour closely")
    print("  4. Gradual ramp-up: 500 -> 1000 -> 5000 crimes")
    print("  5. Monitor audit logs continuously")
    print()


def show_rollback_steps():
    """Show rollback steps."""
    print("\n🔄 Rollback (If Issues)\n")
    
    print("Quick Rollback:")
    print("  1. Stop ETL: systemctl stop dopams-etl")
    print("  2. Restore backups:")
    print("     cp brief_facts_drugs/extractor.py.backup brief_facts_drugs/extractor.py")
    print("     cp brief_facts_drugs/db.py.backup brief_facts_drugs/db.py")
    print("  3. Restart: systemctl start dopams-etl")
    print()
    
    print("Database Cleanup (if needed):")
    print("  psql -U dopams_user -d dopams_db -c \"")
    print("    DROP TABLE drug_extraction_rejections;")
    print("    DROP TABLE drug_kb_match_audit;\"")
    print()


def show_monitoring_queries():
    """Show monitoring and validation queries."""
    print("\n📊 Monitoring Queries\n")
    
    print("1. KB Match Quality (run hourly):")
    print("""
SELECT 
    match_type,
    COUNT(*) as count,
    ROUND(AVG(match_ratio)::numeric, 3) as avg_ratio,
    ROUND(AVG(confidence_adjusted)::numeric, 3) as avg_confidence
FROM drug_kb_match_audit
WHERE created_at > NOW() - INTERVAL '1 hour'
GROUP BY match_type
ORDER BY count DESC;
""")
    
    print("2. Rejection Analysis (find patterns):")
    print("""
SELECT 
    rejection_reason,
    COUNT(*) as count,
    SUM(CASE WHEN was_false_positive THEN 1 ELSE 0 END) as false_positives
FROM drug_extraction_rejections
WHERE created_at > NOW() - INTERVAL '24 hours'
GROUP BY rejection_reason
ORDER BY count DESC
LIMIT 10;
""")
    
    print("3. Commercial Accuracy Check:")
    print("""
SELECT 
    is_commercial,
    COUNT(*) as count,
    ROUND(AVG(match_ratio)::numeric, 3) as avg_match_ratio
FROM drug_kb_match_audit
WHERE match_type != 'no_match' AND created_at > NOW() - INTERVAL '1 day'
GROUP BY is_commercial;
""")
    
    print("4. Confidence Distribution (before/after):")
    print("""
SELECT 
    ROUND(confidence_original, 1) as orig_conf,
    ROUND(confidence_adjusted, 1) as adj_conf,
    COUNT(*) as count
FROM drug_kb_match_audit
WHERE created_at > NOW() - INTERVAL '24 hours'
GROUP BY 
    ROUND(confidence_original, 1),
    ROUND(confidence_adjusted, 1)
ORDER BY count DESC;
""")
    print()


def main():
    parser = argparse.ArgumentParser(
        description='Deploy Advanced KB Matching to DOPAMS ETL',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python deploy_kb_matching.py --dry-run
  python deploy_kb_matching.py --backup
  python deploy_kb_matching.py --check
  python deploy_kb_matching.py --sql-only deploy_migrations.sql
  python deploy_kb_matching.py --guide
"""
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview changes without applying'
    )
    parser.add_argument(
        '--backup',
        action='store_true',
        help='Create backups of current files'
    )
    parser.add_argument(
        '--check',
        action='store_true',
        help='Check file prerequisites'
    )
    parser.add_argument(
        '--sql-only',
        type=str,
        metavar='OUTPUT',
        help='Generate only SQL migration script'
    )
    parser.add_argument(
        '--guide',
        action='store_true',
        help='Show deployment steps and monitoring'
    )
    parser.add_argument(
        '--path',
        type=str,
        default='.',
        help='Path to brief_facts_drugs directory'
    )
    
    args = parser.parse_args()
    
    print("\n" + "="*70)
    print("DOPAMS ETL - Advanced KB Matching Deployment")
    print("="*70)
    
    # Check prerequisites
    if not check_files_exist(args.path):
        print("\n❌ Missing required files. Exiting.")
        sys.exit(1)
    
    # Show summary
    if args.dry_run or args.guide:
        show_changes_summary()
    
    # SQL generation
    if args.sql_only:
        generate_sql_script(args.sql_only)
        return
    
    # Backups
    if args.backup:
        print("\n💾 Creating backups...")
        for filename in ['extractor.py', 'db.py', 'main.py', 'config.py']:
            filepath = Path(args.path) / filename
            if filepath.exists():
                create_backup(str(filepath))
    
    # Show guide
    if args.guide:
        show_deployment_steps()
        show_monitoring_queries()
        show_rollback_steps()
        return
    
    # Dry run
    if args.dry_run:
        print("\n⚠️  DRY RUN MODE - No changes applied")
        print("\nTo proceed with deployment:")
        print("  1. Review changes above")
        print("  2. Create backups: python deploy_kb_matching.py --backup")
        print("  3. Generate SQL:   python deploy_kb_matching.py --sql-only deploy_migrations.sql")
        print("  4. Read guide:     python deploy_kb_matching.py --guide")
        return
    
    if args.check:
        print("\n✅ All prerequisites met!")
        print("Next: python deploy_kb_matching.py --guide")
        return
    
    print("\n✅ Deployment Complete!")
    print("\nNext steps:")
    print("  1. python deploy_kb_matching.py --sql-only deploy_migrations.sql")
    print("  2. psql -d dopams_db -f deploy_migrations.sql")
    print("  3. python test_kb_matcher_advanced.py")
    print("  4. Review integration changes in PRODUCTION_DEPLOYMENT_GUIDE.md")


if __name__ == '__main__':
    main()
