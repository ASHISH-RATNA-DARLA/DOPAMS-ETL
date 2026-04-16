"""
Master ETL checkpoint manager - updates only when all 28 steps complete successfully
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

    This updates the master_etl_backfill_complete checkpoint in etl_run_state.
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
                # Ensure etl_run_state table exists
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS etl_run_state (
                        module_name TEXT PRIMARY KEY,
                        last_successful_end TIMESTAMPTZ NOT NULL,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                """)

                # Update master checkpoint to yesterday's end
                # This marks the backfill as complete
                now_ist = datetime.now(IST_OFFSET)
                yesterday_end = (now_ist - timedelta(days=1)).replace(
                    hour=23, minute=59, second=59, microsecond=0
                )

                cur.execute("""
                    INSERT INTO etl_run_state (module_name, last_successful_end, updated_at)
                    VALUES (%s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (module_name)
                    DO UPDATE SET
                        last_successful_end = EXCLUDED.last_successful_end,
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
                    "SELECT last_successful_end FROM etl_run_state WHERE module_name = %s",
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
                    "SELECT last_successful_end FROM etl_run_state WHERE module_name = %s",
                    ('master_etl_backfill_complete',)
                )
                result = cur.fetchone()
                if result:
                    return result[0]
    except Exception:
        pass

    return None
