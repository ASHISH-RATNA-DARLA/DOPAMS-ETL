"""
Unified run-mode configuration for DOPAMS-ETL master pipeline.

Three env flags (set in .env or shell):
  RESTART        = true | false     Full DB wipe + reload from RESTART_DATE.
  RESTART_DATE   = YYYY-MM-DD       Origin date for RESTART mode (default 2022-01-01).
  LAST_RUN       = YYYY-MM-DD       Auto-updated on pipeline success; FROM_DATE for incremental.
  STEP_TIMEOUT_SEC = <int>          Per-step subprocess timeout in seconds (default 7200).

Master injects into child subprocess env:
  ETL_FROM_DATE  = YYYY-MM-DD
  ETL_TO_DATE    = YYYY-MM-DD

KB tables preserved on RESTART (never truncated):
  geo_countries, geo_reference, drug_categories, drug_ignore_list
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from psycopg2 import sql

IST = timezone(timedelta(hours=5, minutes=30))
ABSOLUTE_ORIGIN = "2022-01-01"
KB_PRESERVE_TABLES = frozenset({"geo_countries", "geo_reference", "drug_categories", "drug_ignore_list"})


def _yesterday_ist() -> str:
    return (datetime.now(IST) - timedelta(days=1)).strftime("%Y-%m-%d")


def is_restart_mode() -> bool:
    return os.environ.get("RESTART", "false").strip().lower() in ("1", "true", "yes")


def get_restart_date() -> str:
    raw = os.environ.get("RESTART_DATE", "").strip()
    if raw:
        try:
            datetime.strptime(raw, "%Y-%m-%d")
            return raw
        except ValueError:
            pass
    return ABSOLUTE_ORIGIN


def get_from_date() -> str:
    """
    RESTART mode → RESTART_DATE.
    Incremental   → LAST_RUN (or ABSOLUTE_ORIGIN if not set).
    """
    if is_restart_mode():
        return get_restart_date()
    last_run = os.environ.get("LAST_RUN", "").strip()
    if last_run:
        try:
            datetime.strptime(last_run, "%Y-%m-%d")
            return last_run
        except ValueError:
            pass
    return ABSOLUTE_ORIGIN


def get_to_date() -> str:
    """Yesterday's date in IST as YYYY-MM-DD."""
    raw = os.environ.get("ETL_TO_DATE", "").strip()
    if raw:
        try:
            datetime.strptime(raw, "%Y-%m-%d")
            return raw
        except ValueError:
            pass
    return _yesterday_ist()


def persist_last_run(to_date: str) -> None:
    """Write LAST_RUN=<to_date> back into the project-root .env file."""
    try:
        from dotenv import set_key
        env_path = Path(__file__).resolve().parent / ".env"
        if env_path.exists():
            set_key(str(env_path), "LAST_RUN", to_date)
    except Exception as exc:
        import logging
        logging.getLogger(__name__).warning("persist_last_run failed: %s", exc)


def wipe_database(pool, logger) -> None:
    """
    Truncate all public tables EXCEPT KB_PRESERVE_TABLES.
    Uses RESTART IDENTITY CASCADE so FK order doesn't matter.
    Safe to call only when RESTART=true.
    """
    logger.warning("RESTART=true: wiping all data tables (KB tables preserved).")
    with pool.get_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT tablename
                  FROM pg_tables
                 WHERE schemaname = 'public'
            """)
            all_tables = [row[0] for row in cur.fetchall()]

        tables_to_wipe = [t for t in all_tables if t not in KB_PRESERVE_TABLES]
        if not tables_to_wipe:
            logger.info("No tables to wipe.")
            return

        wiped_count = 0
        with conn.cursor() as cur:
            for table in tables_to_wipe:
                try:
                    cur.execute(
                        sql.SQL("TRUNCATE TABLE {}.{} RESTART IDENTITY CASCADE").format(
                            sql.Identifier("public"),
                            sql.Identifier(table),
                        )
                    )
                    logger.info("  wiped: %s", table)
                    wiped_count += 1
                except Exception as exc:
                    logger.warning("  wipe failed for %s: %s (continuing)", table, exc)
                    conn.rollback()
                    continue
            conn.commit()

    logger.warning("DB wipe complete. Wiped %d/%d tables.", wiped_count, len(tables_to_wipe))
