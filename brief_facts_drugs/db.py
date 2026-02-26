
import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
import config

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            dbname=config.DB_NAME,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            host=config.DB_HOST,
            port=config.DB_PORT
        )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        raise

def fetch_drug_categories(conn):
    """
    Fetches the knowledge base of drug categories, mapping raw_name -> standard_name.
    """
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Assumes the table is called `drug_categories` and has columns `raw_name` and `standard_name` 
            # as per user instructions.
            query = "SELECT raw_name, standard_name FROM public.drug_categories"
            cur.execute(query)
            return cur.fetchall()
    except Exception as e:
        print(f"Warning: Could not fetch drug_categories: {e}")
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


def insert_drug_facts(conn, crime_id, drug_data):
    """Inserts extracted drug information into the database."""
    with conn.cursor() as cur:
        query = sql.SQL("""
            INSERT INTO {table} 
            (crime_id, accused_id, raw_drug_name, raw_quantity, raw_unit, primary_drug_name, drug_form,
             weight_g, weight_kg, volume_ml, volume_l, count_total,
             confidence_score, extraction_metadata, is_commercial, seizure_worth)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """).format(table=sql.Identifier(config.DRUG_TABLE_NAME))
        
        import json
        
        cur.execute(query, (
            crime_id,
            drug_data.get('accused_id'),
            drug_data.get('raw_drug_name'),
            drug_data.get('raw_quantity'),
            drug_data.get('raw_unit'),
            drug_data.get('primary_drug_name'),
            drug_data.get('drug_form'),
            drug_data.get('weight_g'),
            drug_data.get('weight_kg'),
            drug_data.get('volume_ml'),
            drug_data.get('volume_l'),
            drug_data.get('count_total'),
            drug_data.get('confidence_score'),
            json.dumps(drug_data.get('extraction_metadata', {})),
            drug_data.get('is_commercial', False),
            drug_data.get('seizure_worth', 0.0)
        ))
    conn.commit()

