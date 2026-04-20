from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)

ETL_NAME = "etl-address"


def read_checkpoint(pool) -> Optional[str]:
    sql = "SELECT last_seen_id FROM etl_checkpoint WHERE etl_name = %s"
    with pool.get_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (ETL_NAME,))
            row = cur.fetchone()
            return row[0] if row and row[0] else None


def write_checkpoint(pool, last_seen_id: str, run_id: str) -> None:
    sql = """
        INSERT INTO etl_checkpoint (etl_name, last_seen_id, run_id, updated_at)
        VALUES (%s, %s, %s, now())
        ON CONFLICT (etl_name) DO UPDATE SET
            last_seen_id = EXCLUDED.last_seen_id,
            run_id       = EXCLUDED.run_id,
            updated_at   = EXCLUDED.updated_at
    """
    with pool.get_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (ETL_NAME, last_seen_id, run_id))
        conn.commit()


def clear_checkpoint(pool) -> None:
    sql = "DELETE FROM etl_checkpoint WHERE etl_name = %s"
    with pool.get_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (ETL_NAME,))
        conn.commit()
