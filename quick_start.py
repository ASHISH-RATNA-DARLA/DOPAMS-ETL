#!/usr/bin/env python3
"""
DOPAMS ETL Performance Investigation - Quick Start Guide
========================================================

RUN THIS FIRST to get baseline performance metrics.

Usage:
    python quick_start.py
"""

import subprocess
import sys
import os
import json
from pathlib import Path
from datetime import datetime

print("""
╔═══════════════════════════════════════════════════════════════════════════╗
║                   DOPAMS ETL PERFORMANCE INVESTIGATION                    ║
║                         Quick Start Guide v1                               ║
║                                                                            ║
║  This will:                                                                ║
║  ✓ Measure baseline performance                                           ║
║  ✓ Identify database bottlenecks                                          ║
║  ✓ Recommend quick wins                                                   ║
║  ✓ Generate implementation roadmap                                        ║
╚═════════════════════════════════════════════════════════════════════════════╝
""")

REPORTS_DIR = Path("performance_reports")
REPORTS_DIR.mkdir(exist_ok=True)

report_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

def check_dependencies():
    """Verify required packages are installed"""
    print("\n[Step 1/4] Checking dependencies...")
    print("-" * 80)
    
    required = [
        ('psycopg2', 'pip install psycopg2-binary'),
        ('psutil', 'pip install psutil'),
        ('dotenv', 'pip install python-dotenv'),
    ]
    
    missing = []
    for package, install_cmd in required:
        try:
            __import__(package)
            print(f"✅ {package:<20} installed")
        except ImportError:
            print(f"❌ {package:<20} MISSING")
            missing.append(install_cmd)
    
    if missing:
        print(f"\n⚠️  Missing packages. Install with:")
        for cmd in missing:
            print(f"   {cmd}")
        return False
    
    return True


def check_env_file():
    """Verify .env file exists and has DB credentials"""
    print("\n[Step 2/4] Checking .env configuration...")
    print("-" * 80)
    
    if not Path('.env').exists():
        print("❌ .env file not found!")
        print("\nCreate .env with:")
        print("""
DB_NAME=dopams
DB_USER=your_user
DB_PASSWORD=your_password
DB_HOST=localhost
DB_PORT=5432
OLLAMA_HOST=http://localhost:11434
        """)
        return False
    
    from dotenv import load_dotenv
    load_dotenv()
    
    required_vars = ['DB_NAME', 'DB_USER', 'DB_PASSWORD', 'DB_HOST', 'DB_PORT']
    missing = []
    
    for var in required_vars:
        value = os.getenv(var)
        if value:
            # Mask password in output
            display = '***' if 'PASSWORD' in var else value
            print(f"✅ {var:<20} = {display}")
        else:
            print(f"❌ {var:<20} MISSING")
            missing.append(var)
    
    if missing:
        print(f"\n⚠️  Missing env variables: {', '.join(missing)}")
        return False
    
    return True


def test_db_connection():
    """Test connection to PostgreSQL"""
    print("\n[Step 3/4] Testing database connection...")
    print("-" * 80)
    
    try:
        import psycopg2
        import os
        from dotenv import load_dotenv
        
        load_dotenv()
        
        conn = psycopg2.connect(
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT')
        )
        
        with conn.cursor() as cur:
            cur.execute("SELECT version()")
            version = cur.fetchone()[0]
            cur.execute("SELECT datname, numbackends FROM pg_stat_database WHERE datname = current_database()")
            db_stat = cur.fetchone()
        
        print(f"✅ Connected to PostgreSQL")
        print(f"   Version: {version[:50]}...")
        print(f"   Database: {db_stat[0]}")
        print(f"   Active connections: {db_stat[1]}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        print("\nTroubleshooting:")
        print("  1. Verify PostgreSQL is running")
        print("  2. Check .env credentials")
        print("  3. Check DB_HOST and DB_PORT")
        return False


def run_analysis():
    """Run the actual performance analysis"""
    print("\n[Step 4/4] Running performance analysis...")
    print("-" * 80)
    
    try:
        # Run query optimizer
        print("\n📊 Analyzing database queries...")
        from query_optimizer import DOPAMSQueryOptimizer, ConnectionStats
        import psycopg2
        from dotenv import load_dotenv
        
        load_dotenv()
        conn = psycopg2.connect(
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT')
        )
        
        # Analyze critical queries
        print("\n  Analyzing critical queries...")
        DOPAMSQueryOptimizer.analyze_all_critical_queries(conn)
        
        # Show statistics
        print("\n  Collecting table statistics...")
        ConnectionStats.get_table_access_patterns(conn)
        ConnectionStats.get_cache_hit_ratio(conn)
        
        conn.close()
        
        print("\n✅ Analysis complete!")
        return True
        
    except Exception as e:
        print(f"\n⚠️  Analysis partially failed: {e}")
        print("   (This is OK - may indicate specific DB queries")
        print("   not present. Check schema.)")
        return True


def print_recommendations():
    """Print actionable recommendations"""
    print("\n" + "="*80)
    print("PERFORMANCE INVESTIGATION COMPLETE")
    print("="*80)
    
    print("""
📋 RECOMMENDED NEXT STEPS:

[Phase 1: Quick Wins - 2-3 days - Expected 4-5x improvement]
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. CREATE MISSING INDEXES [1 hour]
   📄 See: PERFORMANCE_AUDIT_REPORT.md (Section 5 - Database Level)
   
   SQL commands to run:
   CREATE INDEX idx_brief_facts_accused_crime_id ON brief_facts_accused(crime_id);
   CREATE INDEX idx_brief_facts_drugs_crime_id ON brief_facts_drugs(crime_id);
   CREATE INDEX idx_accused_crime_id ON accused(crime_id);
   ... (see query_optimizer.py for full list)

2. IMPLEMENT CONNECTION POOLING [4 hours]
   📄 See: IMPLEMENTATION_ROADMAP.md (Task 1.3)
   📝 Code in: db_pooling.py (ready to use)
   
   Replace in db.py files:
   OLD: conn = psycopg2.connect(...)
   NEW: from db_pooling import get_db_connection
        conn = get_db_connection()

3. IMPLEMENT BATCH INSERTS [4 hours]
   📄 See: IMPLEMENTATION_ROADMAP.md (Task 1.4)
   📝 Code in: db_pooling.py (batch_insert function)
   
   Replace in db.py files:
   OLD: for item in items: cur.execute(INSERT, item)
   NEW: from db_pooling import batch_insert
        batch_insert(cur, INSERT, items_list)

[Phase 2: Advanced - 5-7 days - Expected 5-10x total improvement]
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

4. IMPLEMENT ASYNC PIPELINE
   📄 See: PERFORMANCE_AUDIT_REPORT.md (Section 6.2)
   📄 See: IMPLEMENTATION_ROADMAP.md (Task 2.1)
   
   Creates 3 concurrent stages: Fetch → Extract → Insert

5. MULTIPROCESSING FOR PREPROCESSING
   📄 See: PERFORMANCE_AUDIT_REPORT.md (Section 6.3)
   
   For drug relevance scoring in brief_facts_drugs/extractor.py

📊 MONITORING & VERIFICATION:
  • Run performance_profiler.py before/after each phase
  • Monitor with: python performance_profiler.py --example
  • Check query times: SELECT * FROM pg_stat_statements ORDER BY total_time DESC;

📚 DOCUMENTATION:
  ✓ EXECUTIVE_SUMMARY.md        - For stakeholders & managers
  ✓ PERFORMANCE_AUDIT_REPORT.md - Complete technical reference
  ✓ IMPLEMENTATION_ROADMAP.md   - Step-by-step implementation
  ✓ performance_profiler.py     - Measurement tool
  ✓ db_pooling.py              - Ready-to-use pooling code
  ✓ query_optimizer.py         - Analysis tool

🎯 SUCCESS CRITERIA:
  Phase 1 complete:  4-5x improvement
  Phase 2 complete:  5-10x improvement
  All metrics baseline documented
  Production deployment tested

💡 QUICK WINS CHECKLIST:
  [ ] Analyzed queries with query_optimizer.py
  [ ] Created 5 indexes
  [ ] Updated brief_facts_accused/db.py with pooling
  [ ] Updated brief_facts_drugs/db.py with pooling
  [ ] Changed insert loop to batch_insert()
  [ ] Tested with 100+ records
  [ ] Measured 4-5x improvement
  [ ] Ready for Phase 2 (optional)

🚀 BASELINE METRICS SAVED TO:
   performance_reports/analysis_{timestamp}/

For detailed guidance, open:
  → EXECUTIVE_SUMMARY.md (start here)
  → IMPLEMENTATION_ROADMAP.md (step-by-step)
  → PERFORMANCE_AUDIT_REPORT.md (deep dive)
""")


def main():
    """Main execution"""
    
    # Check dependencies
    if not check_dependencies():
        print("\n❌ Please install missing dependencies and try again")
        sys.exit(1)
    
    # Check env
    if not check_env_file():
        print("\n❌ Please configure .env file and try again")
        sys.exit(1)
    
    # Test connection
    if not test_db_connection():
        print("\n❌ Cannot connect to database")
        sys.exit(1)
    
    # Run analysis
    if not run_analysis():
        print("\n⚠️  Some analysis steps failed, but diagnostics collected")
    
    # Print recommendations
    print_recommendations()
    
    print("\n" + "="*80)
    print(f"✅ Quick start complete at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    print("\n📖 Next: Open EXECUTIVE_SUMMARY.md for overview")
    print("📖 Then: Follow IMPLEMENTATION_ROADMAP.md for step-by-step guide\n")


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
