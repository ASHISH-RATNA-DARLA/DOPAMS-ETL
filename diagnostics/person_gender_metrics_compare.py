#!/usr/bin/env python3
"""
Current metrics snapshot + optional baseline comparison for PERSON gender quality.

Usage examples:
- Capture baseline:
  python diagnostics/person_gender_metrics_compare.py --save-baseline diagnostics/person_gender_baseline.json
- Compare with baseline:
  python diagnostics/person_gender_metrics_compare.py --compare-with diagnostics/person_gender_baseline.json
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from typing import Any, Dict

import psycopg2
from psycopg2.extras import RealDictCursor

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

from person_gender_diagnostics import resolve_db_config, normalize_gender, is_invalid_name


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare person gender quality metrics against baseline")
    parser.add_argument("--table", default="persons", help="Target table name in public schema")
    parser.add_argument("--save-baseline", help="Write current metrics JSON to this file")
    parser.add_argument("--compare-with", help="Compare current metrics with this baseline JSON")
    return parser.parse_args()


def compute_metrics(cur, table_name: str) -> Dict[str, Any]:
    cur.execute(
        f"""
        SELECT person_id, full_name, gender
        FROM public.{table_name}
        """
    )
    rows = cur.fetchall()

    total_rows = len(rows)
    invalid_gender_rows = 0
    unknown_rows = 0
    invalid_name_rows = 0
    mismatch_rows = 0

    for row in rows:
        full_name = row['full_name']
        gender = row['gender']

        canonical_gender, _ = normalize_gender(gender)
        if canonical_gender is None:
            invalid_gender_rows += 1
        elif canonical_gender == 'Unknown':
            unknown_rows += 1

        invalid_name, _ = is_invalid_name(full_name)
        if invalid_name:
            invalid_name_rows += 1

        if invalid_name and canonical_gender in {'Male', 'Female', 'Transgender'}:
            mismatch_rows += 1

    def pct(value: int) -> float:
        if total_rows == 0:
            return 0.0
        return round((value * 100.0) / total_rows, 3)

    return {
        'timestamp_utc': datetime.now(timezone.utc).isoformat(),
        'table': table_name,
        'total_rows': total_rows,
        'invalid_gender_rows': invalid_gender_rows,
        'unknown_rows': unknown_rows,
        'invalid_name_rows': invalid_name_rows,
        'mismatch_rows': mismatch_rows,
        'invalid_gender_pct': pct(invalid_gender_rows),
        'unknown_pct': pct(unknown_rows),
        'invalid_name_pct': pct(invalid_name_rows),
        'mismatch_pct': pct(mismatch_rows),
    }


def print_current(metrics: Dict[str, Any]) -> None:
    print("Current PERSON Gender Metrics")
    print("=============================")
    print(f"Table: {metrics['table']}")
    print(f"Total rows: {metrics['total_rows']}")
    print(f"Invalid gender: {metrics['invalid_gender_rows']} ({metrics['invalid_gender_pct']}%)")
    print(f"Unknown gender: {metrics['unknown_rows']} ({metrics['unknown_pct']}%)")
    print(f"Invalid full_name: {metrics['invalid_name_rows']} ({metrics['invalid_name_pct']}%)")
    print(f"Mismatch (valid gender + invalid name): {metrics['mismatch_rows']} ({metrics['mismatch_pct']}%)")


def print_delta(before: Dict[str, Any], after: Dict[str, Any]) -> None:
    print("\nBaseline Comparison")
    print("===================")
    keys = [
        ('invalid_gender_rows', 'Invalid gender'),
        ('unknown_rows', 'Unknown gender'),
        ('invalid_name_rows', 'Invalid full_name'),
        ('mismatch_rows', 'Mismatch'),
    ]
    for key, label in keys:
        b = int(before.get(key, 0))
        a = int(after.get(key, 0))
        d = a - b
        trend = 'improved' if d < 0 else ('regressed' if d > 0 else 'unchanged')
        print(f"{label}: before={b}, after={a}, delta={d} ({trend})")


def main() -> int:
    args = parse_args()
    if load_dotenv is not None:
        load_dotenv()

    db_config = resolve_db_config()

    conn = psycopg2.connect(**db_config)
    conn.set_session(readonly=True, autocommit=True)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            current = compute_metrics(cur, args.table)

        print_current(current)

        if args.compare_with:
            with open(args.compare_with, 'r', encoding='utf-8') as handle:
                baseline = json.load(handle)
            print_delta(baseline, current)

        if args.save_baseline:
            with open(args.save_baseline, 'w', encoding='utf-8') as handle:
                json.dump(current, handle, indent=2, ensure_ascii=True)
            print(f"\nSaved baseline: {args.save_baseline}")

        return 0
    except Exception as exc:
        print(f"Metrics script failed: {exc}", file=sys.stderr)
        return 1
    finally:
        conn.close()


if __name__ == '__main__':
    sys.exit(main())
