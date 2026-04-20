#!/usr/bin/env python3
"""Ensure geo_countries indexes exist for fast trigram lookups"""

import os
import sys
import psycopg2
from pathlib import Path

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_pooling import PostgreSQLConnectionPool

def ensure_indexes():
    pool = PostgreSQLConnectionPool()
    sql_file = Path(__file__).parent / "create_geo_indexes.sql"

    with open(sql_file) as f:
        sql = f.read()

    # Split statements and execute separately with autocommit
    statements = [s.strip() + ";" for s in sql.split(";") if s.strip()]

    with pool.get_connection_context() as conn:
        conn.commit()  # Commit any existing transaction first
        conn.autocommit = True
        with conn.cursor() as cur:
            for stmt in statements:
                try:
                    cur.execute(stmt)
                except Exception as e:
                    # Index already exists is fine
                    if "already exists" not in str(e):
                        raise
        conn.autocommit = False

    print("✓ geo_countries indexes verified/created")

if __name__ == "__main__":
    ensure_indexes()
