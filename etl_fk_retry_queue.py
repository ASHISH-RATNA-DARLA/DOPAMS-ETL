"""
etl_fk_retry_queue.py — Shared FK retry queue for child ETL modules.

Problem solved
--------------
Five child ETL modules (disposal, arrests, chargesheets, updated_chargesheet,
fsl_case_property) validate foreign keys before inserting records.  When the
parent record (crime_id / person_id / mo_id) does not yet exist at insert time
the record was previously logged and silently dropped — permanently, because
the incremental watermark advances forward and will never re-fetch that record.

This module provides a lightweight, shared queue table (etl_fk_retry_queue)
shared across all five ETLs.  Records with unresolvable FKs are parked here.
Each ETL calls drain_fk_queue() at startup to attempt re-insertion of queued
records before processing the new API window.

Usage (in each affected ETL)
-----------------------------
from etl_fk_retry_queue import push_fk_failure, drain_fk_queue

# On FK validation failure — instead of log-and-drop:
push_fk_failure(conn, source_table='disposal', record_id='abc-123',
                record_json=json.dumps(raw_api_row),
                missing_fk_column='crime_id', missing_fk_value=crime_id)

# At ETL startup — retry previously failed records:
drain_fk_queue(conn, source_table='disposal',
               retry_fn=lambda conn, record: insert_disposal(conn, record))
"""

import json
import logging

logger = logging.getLogger(__name__)

# DDL — table is created lazily on first use.
_CREATE_DDL = """
CREATE TABLE IF NOT EXISTS public.etl_fk_retry_queue (
    queue_id            BIGSERIAL PRIMARY KEY,
    source_table        VARCHAR(100)  NOT NULL,
    record_id           TEXT          NOT NULL,
    record_json         JSONB         NOT NULL,
    missing_fk_column   VARCHAR(100)  NOT NULL,
    missing_fk_value    TEXT          NOT NULL,
    attempt_count       INTEGER       NOT NULL DEFAULT 0,
    last_attempted_at   TIMESTAMPTZ,
    first_failed_at     TIMESTAMPTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolved            BOOLEAN       NOT NULL DEFAULT FALSE,
    error_detail        TEXT
);

-- Index for per-table drain scans
CREATE INDEX IF NOT EXISTS etl_fk_retry_queue_source_unresolved
    ON public.etl_fk_retry_queue (source_table)
    WHERE resolved = FALSE;
"""

_MAX_ATTEMPTS = int(__import__('os').environ.get('FK_RETRY_MAX_ATTEMPTS', '5'))


def _ensure_queue_table(conn):
    """Create etl_fk_retry_queue if it does not exist (idempotent)."""
    with conn.cursor() as cur:
        cur.execute(_CREATE_DDL)


def push_fk_failure(conn, source_table: str, record_id: str,
                    record_json: str, missing_fk_column: str,
                    missing_fk_value: str):
    """Park a record whose FK could not be resolved into the retry queue.

    Args:
        conn:               Active DB connection (caller commits).
        source_table:       ETL module name, e.g. 'disposal', 'arrests'.
        record_id:          Natural ID of the record (e.g. disposal_id).
        record_json:        Full raw API record as a JSON string.
        missing_fk_column:  Column name of the unresolved FK (e.g. 'crime_id').
        missing_fk_value:   Value of the unresolved FK key.
    """
    _ensure_queue_table(conn)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.etl_fk_retry_queue
                (source_table, record_id, record_json,
                 missing_fk_column, missing_fk_value)
            VALUES (%s, %s, %s::jsonb, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (source_table, record_id,
             record_json if isinstance(record_json, str) else json.dumps(record_json),
             missing_fk_column, missing_fk_value),
        )
    logger.warning(
        "FK retry queue: parked %s record_id=%s (missing %s=%s)",
        source_table, record_id, missing_fk_column, missing_fk_value,
    )


def drain_fk_queue(conn, source_table: str, retry_fn):
    """Attempt re-insertion of all unresolved records for source_table.

    For each queued record:
    - Calls retry_fn(conn, record_dict) → True on success, False on failure.
    - Marks resolved=TRUE on success.
    - Increments attempt_count and updates error_detail on failure.
    - Records that exceed FK_RETRY_MAX_ATTEMPTS (default 5) are left in the
      table with their full error history for manual review.

    Args:
        conn:         Active DB connection. drain_fk_queue commits per record.
        source_table: Table name matching what was passed to push_fk_failure.
        retry_fn:     Callable(conn, record: dict) -> bool.
                      Must not commit \u2014 drain_fk_queue handles that.
    """
    _ensure_queue_table(conn)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT queue_id, record_id, record_json, attempt_count
            FROM public.etl_fk_retry_queue
            WHERE source_table = %s
              AND resolved = FALSE
              AND attempt_count < %s
            ORDER BY first_failed_at
            """,
            (source_table, _MAX_ATTEMPTS),
        )
        rows = cur.fetchall()

    if not rows:
        logger.info("FK retry queue: no pending records for %s", source_table)
        return

    logger.info(
        "FK retry queue: attempting %d queued records for %s",
        len(rows), source_table,
    )
    resolved = 0
    still_failing = 0

    for queue_id, record_id, record_json, attempt_count in rows:
        record = record_json if isinstance(record_json, dict) else json.loads(record_json)
        try:
            success = retry_fn(conn, record)
        except Exception as exc:
            success = False
            err = str(exc)
        else:
            err = None

        with conn.cursor() as cur:
            if success:
                cur.execute(
                    """
                    UPDATE public.etl_fk_retry_queue
                    SET resolved = TRUE,
                        last_attempted_at = CURRENT_TIMESTAMP,
                        attempt_count = attempt_count + 1,
                        error_detail = NULL
                    WHERE queue_id = %s
                    """,
                    (queue_id,),
                )
                resolved += 1
                logger.info(
                    "FK retry queue: resolved %s record_id=%s after %d attempts",
                    source_table, record_id, attempt_count + 1,
                )
            else:
                cur.execute(
                    """
                    UPDATE public.etl_fk_retry_queue
                    SET last_attempted_at = CURRENT_TIMESTAMP,
                        attempt_count = attempt_count + 1,
                        error_detail = %s
                    WHERE queue_id = %s
                    """,
                    (err, queue_id),
                )
                still_failing += 1
                logger.warning(
                    "FK retry queue: %s record_id=%s still unresolvable "
                    "(attempt %d/%d): %s",
                    source_table, record_id, attempt_count + 1, _MAX_ATTEMPTS, err,
                )
        conn.commit()

    logger.info(
        "FK retry queue drain complete for %s: resolved=%d, still_failing=%d",
        source_table, resolved, still_failing,
    )
