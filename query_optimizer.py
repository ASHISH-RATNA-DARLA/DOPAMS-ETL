#!/usr/bin/env python3
"""
Query Optimization & EXPLAIN Analysis for DOPAMS ETL
======================================================

Analysis and optimization recommendations for your critical queries.
Run this to identify missing indexes and slow query patterns.

CRITICAL QUERIES IDENTIFIED:
1. fetch_unprocessed_crimes (brief_facts_accused)
2. fetch_existing_accused_for_crime (brief_facts_accused)
3. fetch_unprocessed_crimes (brief_facts_drugs)
4. insert_accused_facts (single row - VERY SLOW)
5. insert_drug_facts (single row - VERY SLOW)
"""

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
import json
import logging
from typing import Dict, List, Any
import time

logger = logging.getLogger(__name__)

# ============================================================================
# EXPLAIN ANALYZER
# ============================================================================

class ExplainAnalyzer:
    """
    Analyze PostgreSQL query execution plans.
    Identifies sequential scans, missing indexes, and inefficiencies.
    """
    
    def __init__(self, conn):
        self.conn = conn
    
    def analyze_query(self, query: str, params: tuple = None) -> Dict[str, Any]:
        """
        Run EXPLAIN ANALYZE on a query and return parsed results.
        """
        try:
            with self.conn.cursor() as cur:
                # Build EXPLAIN query
                explain_query = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query}"
                
                try:
                    cur.execute(explain_query, params)
                    plan = cur.fetchone()[0]
                    
                    # Parse JSON plan
                    return self._parse_plan(plan)
                
                except Exception as e:
                    logger.error(f"EXPLAIN failed: {e}")
                    # Rollback the failed transaction
                    self.conn.rollback()
                    return {'error': str(e)}
        except Exception as e:
            logger.error(f"Cursor error: {e}")
            try:
                self.conn.rollback()
            except:
                pass
            return {'error': str(e)}
    
    def _parse_plan(self, raw_plan: List) -> Dict[str, Any]:
        """Extract key metrics from JSON plan"""
        plan = raw_plan[0]['Plan'] if raw_plan else {}
        
        result = {
            'node_type': plan.get('Node Type', 'UNKNOWN'),
            'actual_time_ms': plan.get('Actual Total Time', 0),
            'actual_rows': plan.get('Actual Rows', 0),
            'planned_rows': plan.get('Plan Rows', 0),
            'row_filter_ratio': 0,
            'issues': [],
            'recommendations': []
        }
        
        # Calculate filter efficiency
        if result['planned_rows'] > 0:
            result['row_filter_ratio'] = result['actual_rows'] / result['planned_rows']
        
        # Identify issues
        if result['node_type'] == 'Seq Scan':
            result['issues'].append('SEQUENTIAL SCAN - Slow! Consider adding index.')
        
        if result['row_filter_ratio'] > 10:
            result['issues'].append(f'POOR FILTER EFFICIENCY - {result["row_filter_ratio"]:.1f}x rows filtered')
        
        if result['actual_time_ms'] > 100:
            result['issues'].append(f'SLOW QUERY - {result["actual_time_ms"]:.1f}ms')
        
        return result
    
    def print_report(self, query: str, name: str, params: tuple = None):
        """Pretty print analysis results"""
        print(f"\n{'='*80}")
        print(f"Query: {name}")
        print(f"{'='*80}")
        print(f"\nSQL: {query[:100]}...")
        
        analysis = self.analyze_query(query, params)
        
        # Check if there was an error
        if 'error' in analysis:
            print(f"\n⚠️  QUERY ANALYSIS ERROR:")
            print(f"  {analysis['error']}")
            print("\n  (This may indicate schema differences or type mismatches)")
            return
        
        print(f"\nExecution Plan:")
        print(f"  Node Type: {analysis.get('node_type', 'N/A')}")
        print(f"  Time: {analysis.get('actual_time_ms', 0):.1f}ms")
        print(f"  Rows Returned: {analysis.get('actual_rows', 0)}")
        print(f"  Rows Planned: {analysis.get('planned_rows', 0)}")
        print(f"  Filter Ratio: {analysis.get('row_filter_ratio', 0):.2f}x")
        
        if analysis.get('issues'):
            print(f"\n⚠️  ISSUES DETECTED:")
            for issue in analysis['issues']:
                print(f"  - {issue}")
        
        if analysis.get('recommendations'):
            print(f"\n💡 RECOMMENDATIONS:")
            for rec in analysis['recommendations']:
                print(f"  - {rec}")
        
        print()


# ============================================================================
# DOPAMS-SPECIFIC QUERY ANALYSIS
# ============================================================================

class DOPAMSQueryOptimizer:
    """
    Analyze and optimize DOPAMS ETL queries specifically.
    """
    
    CRITICAL_QUERIES = [
        {
            'name': 'fetch_unprocessed_crimes (accused)',
            'query': """
                SELECT c.crime_id, c.brief_facts 
                FROM crimes c
                LEFT JOIN brief_facts_accused d ON c.crime_id = d.crime_id
                WHERE d.crime_id IS NULL
                ORDER BY c.date_created DESC, c.date_modified DESC
                LIMIT %s
            """,
            'params': (100,),
            'expected_issue': 'Sequential scan on crimes, missing index on brief_facts_accused(crime_id)',
        },
        {
            'name': 'fetch_existing_accused_for_crime',
            'query': """
                SELECT 
                    a.accused_id, a.person_id, p.full_name, p.alias,
                    a.type as accused_type, p.age, p.gender
                FROM accused a
                JOIN persons p ON a.person_id = p.person_id
                WHERE a.crime_id = %s
            """,
            'params': (1000,),
            'expected_issue': 'Missing index on accused(crime_id)',
        },
        {
            'name': 'fetch_unprocessed_crimes (drugs)',
            'query': """
                SELECT c.crime_id, c.brief_facts 
                FROM crimes c
                LEFT JOIN brief_facts_drugs d ON c.crime_id = d.crime_id
                WHERE d.crime_id IS NULL
                LIMIT %s
            """,
            'params': (100,),
            'expected_issue': 'Missing index on brief_facts_drugs(crime_id)',
        },
    ]
    
    INDEX_RECOMMENDATIONS = [
        ('brief_facts_accused', 'crime_id', 'CREATE INDEX idx_brief_facts_accused_crime_id ON brief_facts_accused(crime_id);'),
        ('brief_facts_drugs', 'crime_id', 'CREATE INDEX idx_brief_facts_drugs_crime_id ON brief_facts_drugs(crime_id);'),
        ('accused', 'crime_id', 'CREATE INDEX idx_accused_crime_id ON accused(crime_id);'),
        ('accused', '(crime_id, person_id)', 'CREATE INDEX idx_accused_crime_person ON accused(crime_id, person_id);'),
        ('crimes', '(date_created DESC, date_modified DESC)', 'CREATE INDEX idx_crimes_dates ON crimes(date_created DESC, date_modified DESC);'),
        ('persons', 'full_name', 'CREATE INDEX idx_persons_full_name ON persons(full_name);'),
        ('persons', 'phone_number', 'CREATE INDEX idx_persons_phone ON persons(phone_number);'),
    ]
    
    @staticmethod
    def analyze_all_critical_queries(conn):
        """Run EXPLAIN on all critical DOPAMS queries
        
        Uses fresh connection for each query to avoid transaction abort issues.
        """
        import os
        from dotenv import load_dotenv
        
        load_dotenv()
        
        print("\n" + "="*80)
        print("DOPAMS ETL CRITICAL QUERY ANALYSIS")
        print("="*80)
        
        for query_info in DOPAMSQueryOptimizer.CRITICAL_QUERIES:
            try:
                # Create fresh connection for this query
                fresh_conn = psycopg2.connect(
                    dbname=os.getenv('DB_NAME'),
                    user=os.getenv('DB_USER'),
                    password=os.getenv('DB_PASSWORD'),
                    host=os.getenv('DB_HOST'),
                    port=os.getenv('DB_PORT', '5432')
                )
                
                analyzer = ExplainAnalyzer(fresh_conn)
                analyzer.print_report(
                    query_info['query'],
                    query_info['name'],
                    query_info['params']
                )
                fresh_conn.close()
                
            except Exception as e:
                print(f"Error analyzing {query_info['name']}: {e}")
                try:
                    fresh_conn.close()
                except:
                    pass
    
    @staticmethod
    def print_index_recommendations():
        """Print index creation recommendations"""
        print("\n" + "="*80)
        print("INDEX CREATION RECOMMENDATIONS")
        print("="*80)
        print("\nRun these SQL statements to create missing indexes:\n")
        
        for table, columns, sql_cmd in DOPAMSQueryOptimizer.INDEX_RECOMMENDATIONS:
            print(f"📊 Table: {table} | Columns: {columns}")
            print(f"   {sql_cmd}")
            print()


# ============================================================================
# CONNECTION STATISTICS
# ============================================================================

class ConnectionStats:
    """Analyze PostgreSQL internal statistics"""
    
    @staticmethod
    def get_table_access_patterns(conn):
        """Show sequential vs index scans per table"""
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT schemaname, tablename, seq_scan, seq_tup_read, idx_scan, idx_tup_fetch
                FROM pg_stat_user_tables
                ORDER BY (seq_scan + idx_scan) DESC
                LIMIT 20
            """)
            
            print("\n" + "="*80)
            print("TABLE ACCESS PATTERNS (Sequential vs Index Scans)")
            print("="*80)
            print(f"\n{'Table':<30} {'Seq Scans':>12} {'Index Scans':>12} {'Seq %':>8}")
            print("-"*80)
            
            for row in cur.fetchall():
                total = row['seq_scan'] + row['idx_scan']
                seq_pct = 100.0 * row['seq_scan'] / total if total > 0 else 0
                
                status = "🟢" if seq_pct < 10 else "🟡" if seq_pct < 50 else "🔴"
                
                print(f"{status} {row['tablename']:<28} {row['seq_scan']:>12} {row['idx_scan']:>12} {seq_pct:>7.1f}%")
    
    @staticmethod
    def get_cache_hit_ratio(conn):
        """Show buffer cache hit ratio (aim for > 99%)"""
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT schemaname, tablename, heap_blks_read, heap_blks_hit
                FROM pg_statio_user_tables
                ORDER BY heap_blks_read DESC
                LIMIT 20
            """)
            
            print("\n" + "="*80)
            print("BUFFER CACHE HIT RATIO (Target: > 99%)")
            print("="*80)
            print(f"\n{'Table':<30} {'Hit Ratio':>12} {'Reads':>12} {'Hits':>12}")
            print("-"*80)
            
            for row in cur.fetchall():
                total = row['heap_blks_read'] + row['heap_blks_hit']
                hit_ratio = 100.0 * row['heap_blks_hit'] / total if total > 0 else 0
                
                status = "🟢" if hit_ratio > 99 else "🟡" if hit_ratio > 95 else "🔴"
                
                print(f"{status} {row['tablename']:<28} {hit_ratio:>11.1f}% {row['heap_blks_read']:>12} {row['heap_blks_hit']:>12}")
    
    @staticmethod
    def get_index_stats(conn):
        """Show index usage"""
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            
            # Unused indexes
            print("\n" + "="*80)
            print("UNUSED INDEXES (Can be dropped to improve INSERT speed)")
            print("="*80)
            
            cur.execute("""
                SELECT schemaname, tablename, indexname, idx_scan
                FROM pg_stat_user_indexes
                WHERE idx_scan = 0
                ORDER BY pg_relation_size(indexrelid) DESC
            """)
            
            unused = cur.fetchall()
            if unused:
                print(f"\n❌ Found {len(unused)} unused indexes:\n")
                for row in unused:
                    print(f"   DROP INDEX {row['schemaname']}.{row['indexname']};  -- on {row['tablename']}")
            else:
                print("\n✅ No unused indexes found")


# ============================================================================
# MAIN ANALYSIS SCRIPT
# ============================================================================

def run_full_analysis():
    """Run complete DOPAMS query optimization analysis"""
    
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    try:
        conn = psycopg2.connect(
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT', '5432')
        )
        
        print("\n🔍 DOPAMS ETL QUERY OPTIMIZATION ANALYSIS")
        print("="*80)
        
        # Analyze critical queries
        DOPAMSQueryOptimizer.analyze_all_critical_queries(conn)
        
        # Show index recommendations
        DOPAMSQueryOptimizer.print_index_recommendations()
        
        # Show statistics
        ConnectionStats.get_table_access_patterns(conn)
        ConnectionStats.get_cache_hit_ratio(conn)
        ConnectionStats.get_index_stats(conn)
        
        print("\n" + "="*80)
        print("ANALYSIS COMPLETE")
        print("="*80)
        print("""
Next Steps:
1. Review the sequential scans identified above
2. Run the INDEX CREATION RECOMMENDATIONS
3. Re-run this analysis to verify improvements
4. Monitor pg_stat_statements for other slow queries

For detailed monitoring, enable:
   CREATE EXTENSION pg_stat_statements;
   SELECT query, calls, mean_time FROM pg_stat_statements ORDER BY mean_time DESC;
        """)
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("\nMake sure:")
        print("  1. PostgreSQL is running")
        print("  2. .env file has correct DB credentials")
        print("  3. you have SELECT privileges on pg_stat_* tables")


if __name__ == '__main__':
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == '--recommendations-only':
        print("\nINDEX RECOMMENDATIONS FOR DOPAMS ETL:")
        print("="*80)
        DOPAMSQueryOptimizer.print_index_recommendations()
    else:
        run_full_analysis()
