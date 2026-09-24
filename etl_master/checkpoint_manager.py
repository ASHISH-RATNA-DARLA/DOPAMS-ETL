"""
Master ETL checkpoint manager - updates only when all 28 steps complete successfully

Persists into the consolidated etl_bookkeeping table (kind='run_state'),
which replaces the former dedicated etl_run_state table. See
cctns-v2_schema.sql / cctns-v2_schema_mapping_report.md.
"""

import logging
from datetime import datetime, timedelta, timezone
import os
import sys

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db_pooling import PostgreSQLConnectionPool
from env_utils import resolve_db_config

logger = logging.getLogger(__name__)

IST_OFFSET = timezone(timedelta(hours=5, minutes=30))


def mark_backfill_complete():
    """
    Mark backfill as complete after ALL 28 ETL steps finish successfully.

    This updates the master_etl_backfill_complete checkpoint in etl_bookkeeping (kind='run_state').
    Only call this if the entire pipeline completes without errors.

    After this is called:
    - config.py will use dynamic yesterday's end for end_date
    - Daily incremental runs will start instead of backfill
    """
    try:
        db_config = resolve_db_config()
        db_pool = PostgreSQLConnectionPool(db_config)

        with db_pool.get_connection_context() as conn:
            with conn.cursor() as cur:
                # Ensure the consolidated etl_bookkeeping table exists
                cur.execute("""
                    DO $$ BEGIN
                        CREATE TYPE public.etl_bookkeeping_kind AS ENUM ('checkpoint', 'run_state', 'fk_retry', 'failure');
                    EXCEPTION WHEN duplicate_object THEN NULL; END $$;

                    CREATE TABLE IF NOT EXISTS etl_bookkeeping (
                        id BIGSERIAL PRIMARY KEY,
                        kind public.etl_bookkeeping_kind NOT NULL,
                        module_name TEXT NOT NULL,
                        record_key TEXT,
                        run_id TEXT,
                        checkpoint_value TEXT,
                        watermark TIMESTAMPTZ,
                        record_json JSONB,
                        missing_fk_column VARCHAR(100),
                        missing_fk_value TEXT,
                        reason TEXT,
                        attempt_count INTEGER NOT NULL DEFAULT 0,
                        last_attempted_at TIMESTAMPTZ,
                        first_failed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        resolved BOOLEAN NOT NULL DEFAULT FALSE,
                        resolved_at TIMESTAMPTZ,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    );
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_etl_bookkeeping_singleton
                        ON etl_bookkeeping (kind, module_name)
                        WHERE kind IN ('checkpoint', 'run_state');
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_etl_bookkeeping_failure
                        ON etl_bookkeeping (kind, module_name, record_key)
                        WHERE kind = 'failure';
                """)

                # Update master checkpoint to yesterday's end
                # This marks the backfill as complete
                now_ist = datetime.now(IST_OFFSET)
                yesterday_end = (now_ist - timedelta(days=1)).replace(
                    hour=23, minute=59, second=59, microsecond=0
                )

                cur.execute("""
                    INSERT INTO etl_bookkeeping (kind, module_name, watermark, updated_at)
                    VALUES ('run_state', %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (kind, module_name) WHERE kind = 'run_state'
                    DO UPDATE SET
                        watermark = EXCLUDED.watermark,
                        updated_at = CURRENT_TIMESTAMP
                """, ('master_etl_backfill_complete', yesterday_end))

                conn.commit()

                logger.info(
                    "✅ Master checkpoint updated: backfill complete, switching to daily incremental mode"
                )
                logger.info(f"   Last successful end: {yesterday_end.isoformat()}")
                logger.info(
                    "   Future runs will use dynamic yesterday's end for end_date"
                )

                return True

    except Exception as e:
        logger.error(f"❌ Failed to update master checkpoint: {str(e)}")
        logger.warning(
            "   Backfill not marked complete - next run will retry from where it left off"
        )
        return False


def is_backfill_complete():
    """Check if backfill has been marked as complete."""
    try:
        db_config = resolve_db_config()
        db_pool = PostgreSQLConnectionPool(db_config)

        with db_pool.get_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT watermark FROM etl_bookkeeping WHERE kind = 'run_state' AND module_name = %s",
                    ('master_etl_backfill_complete',)
                )
                result = cur.fetchone()
                return result is not None
    except Exception:
        return False


def get_backfill_completion_date():
    """Get the date when backfill was marked complete."""
    try:
        db_config = resolve_db_config()
        db_pool = PostgreSQLConnectionPool(db_config)

        with db_pool.get_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT watermark FROM etl_bookkeeping WHERE kind = 'run_state' AND module_name = %s",
                    ('master_etl_backfill_complete',)
                )
                result = cur.fetchone()
                if result:
                    return result[0]
    except Exception:
        pass

    return None
