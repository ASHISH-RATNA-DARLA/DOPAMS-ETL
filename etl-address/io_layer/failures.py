from __future__ import annotations

import logging
from typing import Optional

from psycopg2.extras import Json

logger = logging.getLogger(__name__)


def record_failure(
    pool,
    person_id: str,
    reason: str,
    details: Optional[dict] = None,
) -> None:
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("record_failure person_id=%s reason=%s details=%s", person_id, reason, details or {})

    sql = """
        INSERT INTO etl_address_failures (person_id, reason, details, attempted, last_try)
        VALUES (%s, %s, %s, 1, now())
        ON CONFLICT (person_id) DO UPDATE SET
            reason    = EXCLUDED.reason,
            details   = EXCLUDED.details,
            attempted = etl_address_failures.attempted + 1,
            last_try  = now()
    """
    payload = Json(details or {})
    with pool.get_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (person_id, reason, payload))
        conn.commit()


def clear_failure(pool, person_id: str) -> None:
    sql = "DELETE FROM etl_address_failures WHERE person_id = %s"
    with pool.get_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (person_id,))
        conn.commit()


def clear_failures_by_reason(pool, reason: str) -> int:
    sql = "DELETE FROM etl_address_failures WHERE reason = %s"
    with pool.get_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (reason,))
            rowcount = cur.rowcount
        conn.commit()
    return rowcount


def clear_stale_failures(pool, stale_days: int) -> int:
    if stale_days <= 0:
        return 0

    sql = """
        DELETE FROM etl_address_failures
         WHERE last_try < now() - (%s * INTERVAL '1 day')
    """
    with pool.get_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (stale_days,))
            rowcount = cur.rowcount
        conn.commit()
    return rowcount
