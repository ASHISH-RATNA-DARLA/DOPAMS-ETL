"""
etl_fk_retry_queue.py — Shared FK retry queue for child ETL modules.

Problem solved
--------------
Five child ETL modules (disposal, arrests, chargesheets, updated_chargesheet,
fsl_case_property) validate foreign keys before inserting records.  When the
parent record (crime_id / person_id / mo_id) does not yet exist at insert time
the record was previously logged and silently dropped — permanently, because
the incremental watermark advances forward and will never re-fetch that record.

This module parks records with unresolvable FKs in the consolidated
etl_bookkeeping table (kind='fk_retry'), shared across all five ETLs — the
same table that also holds checkpoint/run_state/failure bookkeeping rows for
other modules (see cctns-v2_schema.sql / cctns-v2_schema_mapping_report.md).
This table replaces the former dedicated etl_fk_retry_queue table; the
`queue_id` column below is now `id`, and `error_detail` is now `reason`, but
the push/drain call signatures are unchanged.

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

# DDL — table is created lazily on first use (idempotent). This is the same
# consolidated etl_bookkeeping table cctns-v2_schema.sql defines; the schema
# file is the source of truth, this is only a defensive fallback for
# environments where it hasn't been applied yet.
_CREATE_DDL = """
DO $$ BEGIN
    CREATE TYPE public.etl_bookkeeping_kind AS ENUM ('checkpoint', 'run_state', 'fk_retry', 'failure');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

CREATE TABLE IF NOT EXISTS public.etl_bookkeeping (
    id                  BIGSERIAL PRIMARY KEY,
    kind                public.etl_bookkeeping_kind NOT NULL,
    module_name         TEXT          NOT NULL,
    record_key          TEXT,
    run_id              TEXT,
    checkpoint_value    TEXT,
    watermark           TIMESTAMPTZ,
    record_json         JSONB,
    missing_fk_column   VARCHAR(100),
    missing_fk_value    TEXT,
    reason              TEXT,
    attempt_count       INTEGER       NOT NULL DEFAULT 0,
    last_attempted_at   TIMESTAMPTZ,
    first_failed_at     TIMESTAMPTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolved            BOOLEAN       NOT NULL DEFAULT FALSE,
    resolved_at         TIMESTAMPTZ,
    created_at          TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_etl_bookkeeping_singleton
    ON public.etl_bookkeeping (kind, module_name)
    WHERE kind IN ('checkpoint', 'run_state');

CREATE UNIQUE INDEX IF NOT EXISTS uq_etl_bookkeeping_failure
    ON public.etl_bookkeeping (kind, module_name, record_key)
    WHERE kind = 'failure';

-- Without this, ON CONFLICT DO NOTHING below has no matching constraint to
-- target, so re-queuing a still-unresolved record on a later run inserts a
-- duplicate row instead of being skipped.
CREATE UNIQUE INDEX IF NOT EXISTS uq_etl_bookkeeping_fk_retry
    ON public.etl_bookkeeping (kind, module_name, record_key)
    WHERE kind = 'fk_retry';

-- Index for per-table drain scans
CREATE INDEX IF NOT EXISTS idx_etl_bookkeeping_fk_retry_unresolved
    ON public.etl_bookkeeping (module_name)
    WHERE kind = 'fk_retry' AND resolved = FALSE;
"""

_MAX_ATTEMPTS = int(__import__('os').environ.get('FK_RETRY_MAX_ATTEMPTS', '5'))


def _ensure_queue_table(conn):
    """Create etl_bookkeeping if it does not exist (idempotent)."""
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
            INSERT INTO public.etl_bookkeeping
                (kind, module_name, record_key, record_json,
                 missing_fk_column, missing_fk_value)
            VALUES ('fk_retry', %s, %s, %s::jsonb, %s, %s)
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
    - Increments attempt_count and updates the failure reason on failure.
    - Records that exceed FK_RETRY_MAX_ATTEMPTS (default 5) are left in the
      table with their full error history for manual review.

    Args:
        conn:         Active DB connection. drain_fk_queue commits per record.
        source_table: Table name matching what was passed to push_fk_failure.
        retry_fn:     Callable(conn, record: dict) -> bool.
                      Must not commit — drain_fk_queue handles that.
    """
    _ensure_queue_table(conn)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, record_key, record_json, attempt_count
            FROM public.etl_bookkeeping
            WHERE kind = 'fk_retry'
              AND module_name = %s
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
                    UPDATE public.etl_bookkeeping
                    SET resolved = TRUE,
                        resolved_at = CURRENT_TIMESTAMP,
                        last_attempted_at = CURRENT_TIMESTAMP,
                        attempt_count = attempt_count + 1,
                        reason = NULL
                    WHERE id = %s
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
                    UPDATE public.etl_bookkeeping
                    SET last_attempted_at = CURRENT_TIMESTAMP,
                        attempt_count = attempt_count + 1,
                        reason = %s
                    WHERE id = %s
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
