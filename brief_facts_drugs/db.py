
import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
import config
import logging

logger = logging.getLogger(__name__)

def get_db_connection():
    """Establishes a connection to the PostgreSQL database with TCP keepalive."""
    try:
        conn = psycopg2.connect(
            dbname=config.DB_NAME,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            host=config.DB_HOST,
            port=config.DB_PORT,
            connect_timeout=10,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5
        )
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
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
    Returns raw_name, standard_name, and category_group for LLM prompt context.
    """
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = """
                SELECT raw_name, standard_name, category_group
                FROM public.drug_categories
                WHERE is_verified = true
                ORDER BY category_group, standard_name
            """
            cur.execute(query)
            return cur.fetchall()
    except Exception as e:
        logger.warning(f"Could not fetch drug_categories: {e}")
        return []

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
            LIMIT %s
        """).format(table=sql.Identifier(config.DRUG_TABLE_NAME))
        
        cur.execute(query, (limit,))
        return cur.fetchall()


def _prepare_insert_values(crime_id, drug_data):
    """Prepare the values tuple for a single drug insert. Shared by single and batch insert."""
    import json

    # Preserve the LLM-extracted accused_id in extraction_metadata for audit,
    # even though the DB column is set to NULL to avoid FK constraint issues.
    metadata = drug_data.get('extraction_metadata', {})
    llm_accused_id = drug_data.get('accused_id')
    if llm_accused_id and str(llm_accused_id).strip():
        metadata['accused_ref'] = str(llm_accused_id).strip()

    return (
        crime_id,
        None,  # DB column stays NULL (FK constraint); accused ref stored in extraction_metadata
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
            (crime_id, accused_id, raw_drug_name, raw_quantity, raw_unit, primary_drug_name, drug_form,
             weight_g, weight_kg, volume_ml, volume_l, count_total,
             confidence_score, extraction_metadata, is_commercial, seizure_worth)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
        (crime_id, accused_id, raw_drug_name, raw_quantity, raw_unit, primary_drug_name, drug_form,
         weight_g, weight_kg, volume_ml, volume_l, count_total,
         confidence_score, extraction_metadata, is_commercial, seizure_worth)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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

