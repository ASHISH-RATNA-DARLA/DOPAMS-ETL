from __future__ import annotations

import logging
from typing import Optional

from psycopg2.extras import Json

logger = logging.getLogger(__name__)

MODULE_NAME = "etl-address"

# etl_address_failures was consolidated into etl_bookkeeping (kind='failure').
# record_key holds the original person_id. module_name is always 'etl-address'
# (the only writer of this kind), so (kind, module_name, record_key) is the
# exact equivalent of the original global PRIMARY KEY (person_id).
# See cctns-v2_schema.sql / cctns-v2_schema_mapping_report.md.


def record_failure(
    pool,
    person_id: str,
    reason: str,
    details: Optional[dict] = None,
) -> None:
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("record_failure person_id=%s reason=%s details=%s", person_id, reason, details or {})

    sql = """
        INSERT INTO etl_bookkeeping (kind, module_name, record_key, reason, record_json, attempt_count, last_attempted_at)
        VALUES ('failure', %s, %s, %s, %s, 1, now())
        ON CONFLICT (kind, module_name, record_key) WHERE kind = 'failure' DO UPDATE SET
            reason            = EXCLUDED.reason,
            record_json       = EXCLUDED.record_json,
            attempt_count     = etl_bookkeeping.attempt_count + 1,
            last_attempted_at = now()
    """
    payload = Json(details or {})
    with pool.get_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (MODULE_NAME, person_id, reason, payload))
        conn.commit()


def clear_failure(pool, person_id: str) -> None:
    sql = "DELETE FROM etl_bookkeeping WHERE kind = 'failure' AND module_name = %s AND record_key = %s"
    with pool.get_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (MODULE_NAME, person_id))
        conn.commit()


def clear_failures_by_reason(pool, reason: str) -> int:
    sql = "DELETE FROM etl_bookkeeping WHERE kind = 'failure' AND module_name = %s AND reason = %s"
    with pool.get_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (MODULE_NAME, reason))
            rowcount = cur.rowcount
        conn.commit()
    return rowcount


def clear_stale_failures(pool, stale_days: int) -> int:
    if stale_days <= 0:
        return 0

    sql = """
        DELETE FROM etl_bookkeeping
         WHERE kind = 'failure'
           AND module_name = %s
           AND last_attempted_at < now() - (%s * INTERVAL '1 day')
    """
    with pool.get_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (MODULE_NAME, stale_days))
            rowcount = cur.rowcount
        conn.commit()
    return rowcount


def fetch_deferred_records(pool, limit: int = 1000) -> list[str]:
    """Fetch person_ids of records deferred due to LLM capacity exhaustion.

    Returns list of person_ids to retry.
    """
    sql = """
        SELECT record_key
        FROM etl_bookkeeping
        WHERE kind = 'failure'
          AND module_name = %s
          AND reason = 'llm_deferred_capacity_exhausted'
        ORDER BY last_attempted_at ASC
        LIMIT %s
    """
    with pool.get_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (MODULE_NAME, limit))
            rows = cur.fetchall()
    return [row[0] for row in rows]
