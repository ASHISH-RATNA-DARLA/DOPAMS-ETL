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

    with pool.get_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()

    print("✓ geo_countries indexes verified/created")

if __name__ == "__main__":
    ensure_indexes()
