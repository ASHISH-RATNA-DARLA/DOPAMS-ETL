"""
Drug Name Standardization Script
Database-driven workflow using:
  - drug_ignore_list  -> Gatekeeper  (exact-match skip list)
  - drug_categories   -> Brain       (fuzzy taxonomy via pg_trgm)
  - brief_facts_drug  -> Muscle      (primary_drug_name target column)

Expected behaviour
------------------
  "Heroinn"      -> Heroin        (fuzzy match, dist ~0.1)
  "Whisky"       -> NULL          (blocked by ignore list)
  "Canabis"      -> Ganja         (fuzzy match, dist ~0.15)
  "Unknown Tablet"-> NULL         (blocked by ignore list)
  <no close match>-> raw value    (kept for manual review)
"""

import os
import sys
import logging
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

DB_CONFIG = {
    "host":     os.getenv("DB_HOST"),
    "port":     int(os.getenv("DB_PORT")),
    "database": os.getenv("DB_NAME"),
    "user":     os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

# pg_trgm distance threshold -- lower = stricter (0.3 strict, 0.5 lenient)
FUZZY_THRESHOLD = float(os.getenv("FUZZY_THRESHOLD"))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


def get_standard_name(cursor, raw_input: str):
    """
    Resolve raw_input to a standard drug name using a two-step lookup:

    1. Gatekeeper -- exact match against public.drug_ignore_list
       If found, return None (primary_drug_name will be set to NULL).

    2. Brain      -- fuzzy match against public.drug_categories via pg_trgm
       (<-> operator). If the closest hit is within FUZZY_THRESHOLD, return
       its standard_name. Otherwise fall back to the raw value so it can be
       flagged for manual admin review.
    """
    if not raw_input:
        return None

    clean = raw_input.strip().lower()

    # STEP 1 -- Gatekeeper (exact match, case-insensitive)
    cursor.execute(
        "SELECT 1 FROM public.drug_ignore_list WHERE LOWER(term) = %s",
        (clean,),
    )
    if cursor.fetchone():
        logger.info(f"  [-] Ignored  : {raw_input}")
        return None

    # STEP 2 -- Brain (fuzzy match via pg_trgm <-> operator)
    cursor.execute(
        """
        SELECT standard_name, raw_name <-> %s AS distance
        FROM   public.drug_categories
        ORDER  BY distance ASC
        LIMIT  1
        """,
        (clean,),
    )
    result = cursor.fetchone()

    if result and result["distance"] < FUZZY_THRESHOLD:
        logger.info(
            f"  [+] Matched  : {raw_input!r:40s} -> {result['standard_name']!r}"
            f"  (dist={result['distance']:.2f})"
        )
        return result["standard_name"]

    # Fallback -- no sufficiently close match; keep raw for manual review
    dist_str = f"{result['distance']:.2f}" if result else "N/A"
    logger.warning(
        f"  [?] No match : {raw_input!r} (best dist={dist_str}), keeping raw"
    )
    return raw_input


# ---------------------------------------------------------------------------
# Main ETL
# ---------------------------------------------------------------------------
def run_standardization():
    conn = get_db_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)

    stats = {"processed": 0, "matched": 0, "ignored": 0, "fallback": 0, "errors": 0}

    try:
        logger.info("=" * 70)
        logger.info("DRUG STANDARDIZATION ETL -- START")
        logger.info("=" * 70)
        logger.info(f"DB           : {DB_CONFIG['host']} / {DB_CONFIG['database']}")
        logger.info(f"Fuzzy thresh : {FUZZY_THRESHOLD}")

        cur.execute(
            """
            SELECT id, drug_name
            FROM   public.brief_facts_drug
            WHERE  drug_name IS NOT NULL
              AND  drug_name <> ''
            """
        )
        records = cur.fetchall()
        logger.info(f"Records to process: {len(records)}")

        for row in records:
            stats["processed"] += 1
            try:
                standard = get_standard_name(cur, row["drug_name"])

                if standard is None:
                    stats["ignored"] += 1
                elif standard == row["drug_name"]:
                    stats["fallback"] += 1
                else:
                    stats["matched"] += 1

                cur.execute(
                    """
                    UPDATE public.brief_facts_drug
                    SET    primary_drug_name = %s
                    WHERE  id = %s
                    """,
                    (standard, row["id"]),
                )

            except Exception as row_err:
                stats["errors"] += 1
                logger.error(f"  [!] Error on id={row['id']}: {row_err}")

        conn.commit()

        logger.info("=" * 70)
        logger.info("DRUG STANDARDIZATION ETL -- COMPLETE")
        logger.info(f"  Processed : {stats['processed']}")
        logger.info(f"  Matched   : {stats['matched']}   (fuzzy -> standard_name)")
        logger.info(f"  Ignored   : {stats['ignored']}   (blocked by drug_ignore_list -> NULL)")
        logger.info(f"  Fallback  : {stats['fallback']}   (no close match, kept raw for review)")
        logger.info(f"  Errors    : {stats['errors']}")
        logger.info("=" * 70)

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        conn.rollback()
        raise

    finally:
        cur.close()
        conn.close()


# ---------------------------------------------------------------------------
# Legacy class stub (kept so any external imports don't break immediately)
# ---------------------------------------------------------------------------
class DrugStandardizer:
    """Deprecated: use run_standardization() directly."""
    
    def __init__(self):
        logger.warning(
            "DrugStandardizer class is deprecated. "
            "Call run_standardization() instead."
        )
    
    def run(self):
        run_standardization()


def main():
    """Entry point."""
    run_standardization()


if __name__ == "__main__":
    main()

