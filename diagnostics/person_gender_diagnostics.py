#!/usr/bin/env python3
"""
Read-only diagnostics for PERSON gender and full_name data quality.

What it does:
1. Connects to PostgreSQL using credentials from .env
2. Reads a sample of rows from public.persons
3. Reports:
   - Raw gender distribution (full table)
   - Canonical gender distribution (sample)
   - Invalid full_name patterns (sample)
   - Mismatch cases (valid gender but invalid full_name)

Safety:
- Uses read-only transaction mode
- Executes SELECT queries only
- Does not modify any DB data
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from collections import Counter
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None


CANONICAL_GENDERS = {"Male", "Female", "Transgender", "Unknown"}

GENDER_MAP: Dict[str, str] = {
    "male": "Male",
    "m": "Male",
    "man": "Male",
    "boy": "Male",
    "female": "Female",
    "f": "Female",
    "woman": "Female",
    "girl": "Female",
    "transgender": "Transgender",
    "trans gender": "Transgender",
    "third gender": "Transgender",
    "trans": "Transgender",
    "tg": "Transgender",
    "unknown": "Unknown",
    "unk": "Unknown",
    "n/a": "Unknown",
    "na": "Unknown",
    "not known": "Unknown",
    "not available": "Unknown",
    "": "Unknown",
}

INVALID_NAME_EXACT = {
    "absconding",
    "unknown",
    "not known",
    "unidentified",
    "na",
    "n/a",
    "nil",
    "none",
    "not available",
    "no name",
    "name not known",
    "dead body",
    "accused",
    "suspect",
    "person",
}

PLACEHOLDER_TOKENS = {
    "absconding",
    "unknown",
    "unidentified",
    "na",
    "n/a",
    "nil",
    "none",
    "dead",
    "body",
    "accused",
    "suspect",
    "person",
}


@dataclass
class RowExample:
    person_id: str
    full_name: Optional[str]
    raw_full_name: Optional[str]
    gender: Optional[str]


def normalize_space(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip())


def normalize_gender(raw: Optional[str]) -> Tuple[Optional[str], str]:
    """
    Returns (canonical_gender, source_label)
    source_label in: mapped | already_canonical | invalid
    """
    if raw is None:
        return "Unknown", "mapped"

    value = normalize_space(str(raw)).lower()
    if value in GENDER_MAP:
        return GENDER_MAP[value], "mapped"

    # Handle minor variants such as hyphens or extra spaces
    compact = value.replace("-", " ")
    compact = normalize_space(compact)
    if compact in GENDER_MAP:
        return GENDER_MAP[compact], "mapped"

    if value.capitalize() in CANONICAL_GENDERS:
        return value.capitalize(), "already_canonical"

    return None, "invalid"


def is_invalid_name(full_name: Optional[str]) -> Tuple[bool, str]:
    if full_name is None:
        return True, "null"

    name = normalize_space(str(full_name))
    if not name:
        return True, "empty"

    lowered = name.lower()

    if lowered in INVALID_NAME_EXACT:
        return True, "placeholder_exact"

    # Numeric/symbol-heavy values are often placeholders or garbage.
    letters = re.findall(r"[A-Za-z]", lowered)
    if not letters:
        return True, "no_alpha"

    if len(letters) < 2:
        return True, "too_short"

    alpha_ratio = len(letters) / max(len(lowered), 1)
    if alpha_ratio < 0.35:
        return True, "garbage_ratio"

    tokens = [token for token in re.split(r"[^a-z]+", lowered) if token]
    if tokens and all(token in PLACEHOLDER_TOKENS for token in tokens):
        return True, "placeholder_tokens"

    # Pattern-based placeholders such as "name not known" or "unknown person".
    if re.search(r"\b(name\s+not\s+known|unknown\s+person|absconding\s+accused)\b", lowered):
        return True, "placeholder_pattern"

    return False, "ok"


def env_or_default(primary: str, fallback: str = "") -> str:
    value = os.getenv(primary)
    if value is None or value == "":
        return fallback
    return value


def resolve_db_config() -> Dict[str, str]:
    host = env_or_default("POSTGRES_HOST", env_or_default("DB_HOST"))
    port = env_or_default("POSTGRES_PORT", env_or_default("DB_PORT", "5432"))
    database = env_or_default("POSTGRES_DB", env_or_default("DB_NAME"))
    user = env_or_default("POSTGRES_USER", env_or_default("DB_USER"))
    password = env_or_default("POSTGRES_PASSWORD", env_or_default("DB_PASSWORD"))

    missing = [
        key
        for key, value in {
            "host": host,
            "database": database,
            "user": user,
            "password": password,
            "port": port,
        }.items()
        if not value
    ]
    if missing:
        raise RuntimeError(f"Missing DB config fields: {', '.join(missing)}")

    return {
        "host": host,
        "port": int(port),
        "database": database,
        "user": user,
        "password": password,
    }


def print_top_counter(title: str, counter: Counter, top_n: int) -> None:
    print(f"\n{title}")
    print("-" * len(title))
    if not counter:
        print("(none)")
        return
    for key, value in counter.most_common(top_n):
        print(f"{key}: {value}")


def format_name(name: Optional[str]) -> str:
    if name is None:
        return "<NULL>"
    text = name.strip()
    return text if text else "<EMPTY>"


def analyze_sample_rows(rows: Iterable[Dict], top_n: int) -> Dict[str, object]:
    canonical_distribution = Counter()
    invalid_gender_raw = Counter()
    invalid_name_reasons = Counter()
    invalid_name_values = Counter()
    mismatch_examples: List[RowExample] = []
    mismatch_total = 0

    total = 0
    for row in rows:
        total += 1
        person_id = row.get("person_id")
        full_name = row.get("full_name")
        raw_full_name = row.get("raw_full_name")
        gender = row.get("gender")

        canonical_gender, source = normalize_gender(gender)
        if canonical_gender is None:
            invalid_gender_raw[format_name(gender)] += 1
        else:
            canonical_distribution[canonical_gender] += 1

        invalid_name, reason = is_invalid_name(full_name)
        if invalid_name:
            invalid_name_reasons[reason] += 1
            invalid_name_values[format_name(full_name)] += 1

        if invalid_name and canonical_gender in {"Male", "Female", "Transgender"}:
            mismatch_total += 1
            if len(mismatch_examples) < top_n:
                mismatch_examples.append(
                    RowExample(
                        person_id=str(person_id),
                        full_name=full_name,
                        raw_full_name=raw_full_name,
                        gender=gender,
                    )
                )

    return {
        "sample_total": total,
        "canonical_distribution": canonical_distribution,
        "invalid_gender_raw": invalid_gender_raw,
        "invalid_name_reasons": invalid_name_reasons,
        "invalid_name_values": invalid_name_values,
        "mismatch_examples": mismatch_examples,
        "mismatch_total": mismatch_total,
        "invalid_name_total": sum(invalid_name_reasons.values()),
        "invalid_gender_total": sum(invalid_gender_raw.values()),
    }


def run_diagnostics(sample_size: int, top_n: int, table_name: str) -> int:
    if load_dotenv is not None:
        load_dotenv()

    db_config = resolve_db_config()

    conn = psycopg2.connect(**db_config)
    conn.set_session(readonly=True, autocommit=True)

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(f"SELECT COUNT(*) AS total_rows FROM public.{table_name}")
            total_rows = int(cur.fetchone()["total_rows"])

            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = %s
                """,
                (table_name,),
            )
            columns = {row["column_name"] for row in cur.fetchall()}

            completeness = {
                "full_name_missing": None,
                "raw_full_name_missing": None,
                "gender_missing": None,
                "person_id_missing": None,
            }

            if {"person_id", "full_name", "gender"}.issubset(columns):
                if "raw_full_name" in columns:
                    cur.execute(
                        f"""
                        SELECT
                            COUNT(*) FILTER (WHERE person_id IS NULL OR BTRIM(person_id) = '') AS person_id_missing,
                            COUNT(*) FILTER (WHERE full_name IS NULL OR BTRIM(full_name) = '') AS full_name_missing,
                            COUNT(*) FILTER (WHERE raw_full_name IS NULL OR BTRIM(raw_full_name) = '') AS raw_full_name_missing,
                            COUNT(*) FILTER (WHERE gender IS NULL OR BTRIM(gender) = '') AS gender_missing
                        FROM public.{table_name}
                        """
                    )
                    row = cur.fetchone()
                    completeness = {
                        "person_id_missing": int(row["person_id_missing"]),
                        "full_name_missing": int(row["full_name_missing"]),
                        "raw_full_name_missing": int(row["raw_full_name_missing"]),
                        "gender_missing": int(row["gender_missing"]),
                    }
                else:
                    cur.execute(
                        f"""
                        SELECT
                            COUNT(*) FILTER (WHERE person_id IS NULL OR BTRIM(person_id) = '') AS person_id_missing,
                            COUNT(*) FILTER (WHERE full_name IS NULL OR BTRIM(full_name) = '') AS full_name_missing,
                            COUNT(*) FILTER (WHERE gender IS NULL OR BTRIM(gender) = '') AS gender_missing
                        FROM public.{table_name}
                        """
                    )
                    row = cur.fetchone()
                    completeness = {
                        "person_id_missing": int(row["person_id_missing"]),
                        "full_name_missing": int(row["full_name_missing"]),
                        "raw_full_name_missing": None,
                        "gender_missing": int(row["gender_missing"]),
                    }

            cur.execute(
                f"""
                SELECT COALESCE(NULLIF(BTRIM(gender), ''), '<NULL_OR_EMPTY>') AS gender_value,
                       COUNT(*) AS cnt
                FROM public.{table_name}
                GROUP BY 1
                ORDER BY cnt DESC, gender_value
                """
            )
            gender_distribution_all = cur.fetchall()

            cur.execute(
                f"""
                SELECT person_id,
                       full_name,
                       raw_full_name,
                       gender,
                       date_created,
                       date_modified
                FROM public.{table_name}
                ORDER BY COALESCE(date_modified, date_created) DESC NULLS LAST
                LIMIT %s
                """,
                (sample_size,),
            )
            sampled_rows = cur.fetchall()

        print("PERSON Gender + Name Diagnostics (READ-ONLY)")
        print("=========================================")
        print(f"Table: public.{table_name}")
        print(f"Total rows in table: {total_rows}")
        print(f"Sample rows analyzed: {len(sampled_rows)}")

        print("\nColumn completeness (full table)")
        print("-------------------------------")
        print(f"Missing person_id rows: {completeness['person_id_missing']}")
        print(f"Missing full_name rows: {completeness['full_name_missing']}")
        if completeness["raw_full_name_missing"] is None:
            print("Missing raw_full_name rows: <column not present>")
        else:
            print(f"Missing raw_full_name rows: {completeness['raw_full_name_missing']}")
        print(f"Missing gender rows: {completeness['gender_missing']}")

        print("\nRaw gender distribution (full table)")
        print("-----------------------------------")
        for row in gender_distribution_all[: max(top_n, 50)]:
            print(f"{row['gender_value']}: {row['cnt']}")

        analysis = analyze_sample_rows(sampled_rows, top_n)

        print("\nSample quality summary")
        print("----------------------")
        print(f"Invalid full_name rows: {analysis['invalid_name_total']}")
        print(f"Invalid raw gender rows: {analysis['invalid_gender_total']}")
        print(f"Valid-gender + invalid-name mismatches: {analysis['mismatch_total']}")

        print_top_counter(
            "Canonical gender distribution (sample)",
            analysis["canonical_distribution"],
            top_n,
        )
        print_top_counter(
            "Invalid raw gender values (sample)",
            analysis["invalid_gender_raw"],
            top_n,
        )
        print_top_counter(
            "Invalid full_name reason counts (sample)",
            analysis["invalid_name_reasons"],
            top_n,
        )
        print_top_counter(
            "Top invalid full_name values (sample)",
            analysis["invalid_name_values"],
            top_n,
        )

        print("\nMismatch examples: valid gender + invalid full_name")
        print("----------------------------------------------------")
        mismatch_examples: List[RowExample] = analysis["mismatch_examples"]
        if not mismatch_examples:
            print("(none)")
        else:
            for item in mismatch_examples:
                print(
                    f"person_id={item.person_id} | full_name={format_name(item.full_name)} | "
                    f"raw_full_name={format_name(item.raw_full_name)} | gender={format_name(item.gender)}"
                )

        return 0
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only PERSON gender diagnostics")
    parser.add_argument(
        "--sample-size",
        type=int,
        default=20000,
        help="Number of most recently modified rows to inspect for anomaly checks",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=20,
        help="Top-N values to print for counters",
    )
    parser.add_argument(
        "--table",
        type=str,
        default="persons",
        help="Target person table name (without schema)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    try:
        sys.exit(run_diagnostics(args.sample_size, args.top_n, args.table))
    except Exception as exc:
        print(f"Diagnostic execution failed: {exc}", file=sys.stderr)
        sys.exit(1)
