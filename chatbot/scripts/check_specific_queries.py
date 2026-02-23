#!/usr/bin/env python3
"""Check specific problematic queries"""

import csv
import sys

# Increase field size limit
csv.field_size_limit(sys.maxsize)

csv_file = 'test_responses_20251114_232525.csv'
problematic_ids = ['12', '18', '29', '28', '27']

with open(csv_file, 'r', encoding='utf-8', errors='ignore') as f:
    reader = csv.DictReader(f)
    rows = {row['Question ID']: row for row in reader}

print("=" * 80)
print("PROBLEMATIC QUERIES ANALYSIS")
print("=" * 80)

for qid in problematic_ids:
    if qid not in rows:
        continue
    row = rows[qid]
    print(f"\n{'='*80}")
    print(f"Q{qid}: {row['Question']}")
    print(f"{'='*80}")
    print(f"Status: {row['Status']}")
    print(f"\nSQL Query:")
    sql = row.get('SQL Query', 'N/A')
    if sql and sql != 'N/A':
        print(f"  {sql[:500]}")
    else:
        print("  (No SQL query found)")
    
    print(f"\nMongoDB Query:")
    mongo = row.get('MongoDB Query', 'N/A')
    if mongo and mongo != 'N/A':
        print(f"  {mongo[:300]}")
    else:
        print("  (No MongoDB query found)")
    
    print(f"\nResponse (first 300 chars):")
    print(f"  {row['Response'][:300]}...")
    
    if row.get('Error Message'):
        print(f"\n⚠️  Error: {row['Error Message']}")


