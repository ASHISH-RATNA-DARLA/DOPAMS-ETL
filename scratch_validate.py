import os
import re
from pathlib import Path

db_schema = Path('DB-schema.sql').read_text(encoding='utf-8', errors='ignore')

# Gather actual tables and their columns from DB-schema.sql
tables = {}
table_blocks = re.findall(r'(?i)CREATE TABLE public\.(\w+)\s*\((.*?)\);', db_schema, re.DOTALL)
for t_name, t_body in table_blocks:
    columns = set()
    for line in t_body.strip().split('\n'):
        line = line.strip()
        # Ignore constraints and empty lines
        if line.startswith('CONSTRAINT') or line == '' or line.startswith('UNIQUE') or line.startswith('PRIMARY KEY'):
            continue
        col_name = line.split()[0].replace(',', '')
        columns.add(col_name.lower())
    tables[t_name.lower()] = columns

# Gather views to exclude them from inserts if needed, though inserts usually happen on tables
def find_inserts():
    res = []
    base = Path('.')
    for path in base.rglob('*.py'):
        if 'venv' in str(path) or 'node_modules' in str(path) or '.git' in str(path):
            continue
        try:
            content = path.read_text(encoding='utf-8', errors='ignore')
            # Extract basic insert structures
            # handles multi-line insert statements
            inserts = re.findall(r'(?i)INSERT INTO (?:public\.)?([a-zA-Z0-9_]+)\s*\(([^\)]+)\)', content, re.DOTALL)
            for t_table, t_cols in inserts:
                t_cols_list = [c.strip().lower() for c in t_cols.split(',')]
                res.append((path, t_table.lower(), t_cols_list))
            
            # also check UPDATE public.db set ...
            updates = re.findall(r'(?i)UPDATE(?: public\.)?([a-zA-Z0-9_]+)\s*SET\s*(.*?)(?:WHERE|$)', content, re.DOTALL)
            for t_table, set_clause in updates:
                cols = []
                for chunk in set_clause.split(','):
                    if '=' in chunk:
                        col = chunk.split('=')[0].strip().lower()
                        cols.append(col)
                res.append((path, t_table.lower(), cols))

        except Exception as e: 
            pass
    return res

issues = []
inserts = find_inserts()

# Known tables defined in other places or dynamic
ignore_tables = ['etl_crime_processing_log']

for path, t_name, t_cols in inserts:
    if t_name in ignore_tables or 'sys_' in t_name:
        continue
    clean_cols = [c.split()[-1] for c in t_cols if '%' not in c and '{' not in c and str(c).strip() != '']
    if t_name not in tables:
        issues.append(f'File {path}: Table {t_name} NOT FOUND in DB-schema.sql')
    else:
        for c in clean_cols:
            c_clean = c.replace('\"', '').replace(' ', '')
            if '\n' in c_clean: c_clean = c_clean.split('\n')[0]
            if c_clean and c_clean not in tables[t_name]:
                issues.append(f'File {path}: Column {c_clean} in table {t_name} NOT FOUND in DB-schema.sql')

if not issues:
    print('No INSERT/UPDATE column mismatches found.')
else:
    for issue in sorted(list(set(issues))):
        print(issue)
