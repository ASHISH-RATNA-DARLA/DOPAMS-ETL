from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)

ETL_NAME = "etl-address"

# etl_checkpoint was consolidated into etl_bookkeeping (kind='checkpoint').
# See cctns-v2_schema.sql / cctns-v2_schema_mapping_report.md.


def read_checkpoint(pool) -> Optional[str]:
    sql = "SELECT checkpoint_value FROM etl_bookkeeping WHERE kind = 'checkpoint' AND module_name = %s"
    with pool.get_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (ETL_NAME,))
            row = cur.fetchone()
            return row[0] if row and row[0] else None


def write_checkpoint(pool, last_seen_id: str, run_id: str) -> None:
    sql = """
        INSERT INTO etl_bookkeeping (kind, module_name, checkpoint_value, run_id, updated_at)
        VALUES ('checkpoint', %s, %s, %s, now())
        ON CONFLICT (kind, module_name) WHERE kind = 'checkpoint' DO UPDATE SET
            checkpoint_value = EXCLUDED.checkpoint_value,
            run_id           = EXCLUDED.run_id,
            updated_at       = EXCLUDED.updated_at
    """
    with pool.get_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (ETL_NAME, last_seen_id, run_id))
        conn.commit()


def clear_checkpoint(pool) -> None:
    sql = "DELETE FROM etl_bookkeeping WHERE kind = 'checkpoint' AND module_name = %s"
    with pool.get_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (ETL_NAME,))
        conn.commit()
