#!/usr/bin/env python3
"""
Phase 1 PERSON gender backfill.

Scope (safe-only):
- gender is NULL/empty
- gender is exactly "Human Skeleton" (case-insensitive)

Default mode is preview-only (no writes). Use --apply to execute updates.
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Dict, List, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

from person_gender_diagnostics import resolve_db_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Phase 1 safe backfill for persons.gender")
    parser.add_argument("--table", default="persons", help="Target table name in public schema")
    parser.add_argument("--sample", type=int, default=25, help="Preview sample row count")
    parser.add_argument("--apply", action="store_true", help="Apply updates (default is dry-run/preview)")
    return parser.parse_args()


def get_table_columns(cur, table_name: str) -> set:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        """,
        (table_name,),
    )
    return {row['column_name'] for row in cur.fetchall()}


def preview(cur, table_name: str, sample: int) -> Tuple[int, List[Dict]]:
    where_clause = "(gender IS NULL OR BTRIM(gender) = '' OR LOWER(BTRIM(gender)) = 'human skeleton')"
    cur.execute(f"SELECT COUNT(*) FROM public.{table_name} WHERE {where_clause}")
    target_count = int(cur.fetchone()['count'])

    cur.execute(
        f"""
        SELECT person_id, full_name, gender
        FROM public.{table_name}
        WHERE {where_clause}
        ORDER BY person_id
        LIMIT %s
        """,
        (sample,),
    )
    rows = cur.fetchall()
    return target_count, rows


def apply_backfill(cur, table_name: str, columns: set) -> int:
    where_clause = "(gender IS NULL OR BTRIM(gender) = '' OR LOWER(BTRIM(gender)) = 'human skeleton')"

    set_clauses = ["gender = 'Unknown'"]
    if 'gender_confidence' in columns:
        set_clauses.append("gender_confidence = COALESCE(gender_confidence, 0.0)")
    if 'gender_source' in columns:
        set_clauses.append("gender_source = COALESCE(gender_source, 'rule')")

    cur.execute(
        f"""
        UPDATE public.{table_name}
        SET {', '.join(set_clauses)}
        WHERE {where_clause}
        """
    )
    return int(cur.rowcount)


def main() -> int:
    args = parse_args()
    if load_dotenv is not None:
        load_dotenv()

    db_config = resolve_db_config()

    conn = psycopg2.connect(**db_config)
    conn.autocommit = False
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            columns = get_table_columns(cur, args.table)
            target_count, sample_rows = preview(cur, args.table, args.sample)

            print("Phase 1 PERSON Gender Backfill")
            print("==============================")
            print(f"Table: public.{args.table}")
            print(f"Target rows (NULL/empty/Human Skeleton): {target_count}")
            print(f"Mode: {'APPLY' if args.apply else 'DRY-RUN'}")

            print("\nPreview rows")
            print("------------")
            if not sample_rows:
                print("(none)")
            else:
                for row in sample_rows:
                    print(
                        f"person_id={row['person_id']} | full_name={row['full_name']} | gender={row['gender']}"
                    )

            if not args.apply:
                conn.rollback()
                print("\nDry-run complete. No updates were committed.")
                return 0

            updated = apply_backfill(cur, args.table, columns)
            conn.commit()
            print(f"\nCommitted updates: {updated}")
            return 0
    except Exception as exc:
        conn.rollback()
        print(f"Backfill failed: {exc}", file=sys.stderr)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
