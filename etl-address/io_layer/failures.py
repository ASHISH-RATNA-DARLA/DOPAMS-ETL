from __future__ import annotations

import json
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
