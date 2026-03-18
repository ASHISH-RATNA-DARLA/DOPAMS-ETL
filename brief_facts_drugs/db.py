import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
import config
import logging

logger = logging.getLogger(__name__)

import sys
import os
# Import PostgreSQLConnectionPool
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_pooling import get_db_connection as get_pooled_connection

def get_db_connection():
    """Establishes a connection to the PostgreSQL database via pool."""
    try:
        return get_pooled_connection()
    except Exception as e:
        logger.error(f"Error connecting to database via pool: {e}")
        raise


def ensure_connection(conn):
    """Check if DB connection is alive; reconnect if dropped."""
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        return conn
    except Exception:
        logger.warning("DB connection lost. Reconnecting...")
        try:
            conn.close()
        except Exception:
            pass
        return get_db_connection()


def fetch_drug_categories(conn):
    """
    Fetches the verified knowledge base of drug categories.

    Returns raw_name and standard_name only — category_group is no longer
    sent to the LLM (KB removed from prompt to free ~3,700 tokens of context).
    The KB is now used exclusively for Python-side name standardisation via
    resolve_primary_drug_name() and fuzzy_match_drug_name().
    """
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = """
                SELECT raw_name, standard_name
                FROM public.drug_categories
                WHERE is_verified = true
                ORDER BY standard_name
            """
            cur.execute(query)
            return cur.fetchall()
    except Exception as e:
        logger.warning(f"Could not fetch drug_categories: {e}")
        return []


def fuzzy_match_drug_name(conn, raw_drug_name: str, threshold: float = 0.35) -> str:
    """
    pg_trgm fuzzy match: find the best KB standard_name for a raw drug name
    that did NOT match the kb_lookup dict exactly or by substring.

    Called in resolve_primary_drug_name() as the final fallback after exact
    and substring matching fail — catches misspellings, transliterations, and
    regional aliases not in the KB (e.g. 'ganza'→'Ganja', 'heroien'→'Heroin',
    'kokain'→'Cocaine', 'smak'→'Heroin').

    Args:
        conn:           Active DB connection (read-only query, no transaction).
        raw_drug_name:  The raw drug name string extracted by the LLM.
        threshold:      Minimum pg_trgm similarity score (0.0–1.0).
                        0.35 is intentionally conservative — high enough to
                        catch clear misspellings, low enough to avoid false
                        positives on short or ambiguous strings.

    Returns:
        The best-matching standard_name string if similarity >= threshold,
        otherwise None (caller keeps LLM's primary_drug_name unchanged).

    Performance:
        Uses the GIN trigram index idx_drug_categories_raw_name already
        present on public.drug_categories(raw_name). Each call is a single
        indexed lookup — typically < 2 ms. Called only for entries that
        failed exact + substring match, so the hot path (known drugs) never
        touches the DB for this query.
    """
    if not raw_drug_name or not raw_drug_name.strip():
        return None
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT standard_name,
                       similarity(raw_name, %s) AS sim
                FROM public.drug_categories
                WHERE is_verified = true
                  AND similarity(raw_name, %s) >= %s
                ORDER BY sim DESC
                LIMIT 1
                """,
                (raw_drug_name.lower().strip(),
                 raw_drug_name.lower().strip(),
                 threshold)
            )
            row = cur.fetchone()
            if row:
                return row[0]  # standard_name
            return None
    except Exception as e:
        logger.debug(f"fuzzy_match_drug_name error for '{raw_drug_name}': {e}")
        return None


def fetch_drug_ignore_list(conn):
    """
    Fetches the drug ignore list from DB.

    Returns a dict of {lowercased_term: reason} for all entries in
    public.drug_ignore_list.

    Usage in pipeline:
      - Build ignore_set = set(ignore_dict.keys()) for O(1) exact lookups.
      - Apply ONLY against primary_drug_name (after KB lookup has standardized
        it), NEVER as substring match against raw_drug_name — see analysis in
        docs for why (e.g. 'powder' is a substring of 'dry mixed heroin powder').

    Safe to call at startup and cache for the entire run; the table changes
    infrequently and a restart is acceptable to pick up new entries.
    """
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = """
                SELECT term, reason
                FROM public.drug_ignore_list
                ORDER BY id
            """
            cur.execute(query)
            rows = cur.fetchall()
            result = {row['term'].lower().strip(): (row['reason'] or '') for row in rows if row['term']}
            logger.info(f"Loaded {len(result)} terms from drug_ignore_list.")
            return result
    except Exception as e:
        logger.warning(f"Could not fetch drug_ignore_list: {e}")
        return {}


def fetch_crimes_by_ids(conn, crime_ids):
    """
    Fetches specific crimes based on a list of IDs.
    """
    if not crime_ids:
        return []

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        query = "SELECT crime_id, brief_facts FROM crimes WHERE crime_id = ANY(%s)"
        cur.execute(query, (crime_ids,))
        return cur.fetchall()


def fetch_unprocessed_crimes(conn, limit=100):
    """
    Fetches crimes that do NOT yet have an entry in the configured drug table.
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        query = sql.SQL("""
            SELECT c.crime_id, c.brief_facts
            FROM crimes c
            LEFT JOIN {table} d ON c.crime_id = d.crime_id
            WHERE d.crime_id IS NULL
            ORDER BY c.crime_id ASC
            LIMIT %s
        """).format(table=sql.Identifier(config.DRUG_TABLE_NAME))

        cur.execute(query, (limit,))
        return cur.fetchall()


def _prepare_insert_values(crime_id, drug_data):
    """Prepare the values tuple for a single drug insert. Shared by single and batch insert."""
    import json

    metadata = drug_data.get('extraction_metadata', {})

    return (
        crime_id,
        drug_data.get('raw_drug_name'),
        drug_data.get('raw_quantity'),
        drug_data.get('raw_unit'),
        drug_data.get('primary_drug_name'),
        drug_data.get('drug_form'),
        round(float(drug_data.get('weight_g')), 6) if drug_data.get('weight_g') is not None else None,
        round(float(drug_data.get('weight_kg')), 6) if drug_data.get('weight_kg') is not None else None,
        round(float(drug_data.get('volume_ml')), 6) if drug_data.get('volume_ml') is not None else None,
        round(float(drug_data.get('volume_l')), 6) if drug_data.get('volume_l') is not None else None,
        round(float(drug_data.get('count_total') or 0.0), 6),
        round(float(drug_data.get('confidence_score') or 0.0), 2),
        json.dumps(metadata),
        bool(drug_data.get('is_commercial', False)),
        round(float(drug_data.get('seizure_worth') or 0.0), 2)
    )


def insert_drug_facts(conn, crime_id, drug_data):
    """Inserts extracted drug information into the database (single row)."""
    with conn.cursor() as cur:
        query = sql.SQL("""
            INSERT INTO {table}
            (crime_id, raw_drug_name, raw_quantity, raw_unit, primary_drug_name, drug_form,
             weight_g, weight_kg, volume_ml, volume_l, count_total,
             confidence_score, extraction_metadata, is_commercial, seizure_worth)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """).format(table=sql.Identifier(config.DRUG_TABLE_NAME))

        cur.execute(query, _prepare_insert_values(crime_id, drug_data))
    conn.commit()


def batch_insert_drug_facts(conn, inserts):
    """
    Batch-insert multiple drug rows in a single transaction.
    `inserts` is a list of (crime_id, drug_data_dict) tuples.
    Much faster than per-row commits — reduces DB round-trips by ~100x.
    """
    if not inserts:
        return

    query = sql.SQL("""
        INSERT INTO {table}
        (crime_id, raw_drug_name, raw_quantity, raw_unit, primary_drug_name, drug_form,
         weight_g, weight_kg, volume_ml, volume_l, count_total,
         confidence_score, extraction_metadata, is_commercial, seizure_worth)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """).format(table=sql.Identifier(config.DRUG_TABLE_NAME))

    try:
        with conn.cursor() as cur:
            values_list = [
                _prepare_insert_values(crime_id, drug_data)
                for crime_id, drug_data in inserts
            ]

            # execute_batch is much faster than individual execute calls
            from psycopg2.extras import execute_batch
            execute_batch(cur, query.as_string(conn), values_list, page_size=100)

        conn.commit()
        logger.info(f"Batch insert committed: {len(inserts)} rows.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Batch insert failed, rolling back: {e}")
        raise