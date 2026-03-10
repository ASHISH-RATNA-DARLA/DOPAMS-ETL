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
    Fetches crimes that do NOT yet have an entry in the configured accused table.
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        query = sql.SQL("""
            SELECT c.crime_id, c.brief_facts 
            FROM crimes c
            LEFT JOIN {table} d ON c.crime_id = d.crime_id
            WHERE d.crime_id IS NULL
            ORDER BY c.date_created DESC, c.date_modified DESC
            LIMIT %s
        """).format(table=sql.Identifier(config.ACCUSED_TABLE_NAME))

        cur.execute(query, (limit,))
        return cur.fetchall()


def fetch_existing_accused_for_crime(conn, crime_id):
    """
    Fetches all accused rows for a given crime_id with a LEFT JOIN to persons.

    Always returns accused-level fields (accused_id, accused_code, seq_num,
    is_ccl, accused_status). Returns person identity fields only when person_id
    IS NOT NULL; otherwise those columns are NULL.

    Column mapping to brief_facts_accused output:
      persons.alias        → alias_name
      persons.phone_number → phone_numbers
      CONCAT(present_*)   → address  (single string, NULL if all parts empty)
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        query = """
            SELECT
                a.accused_id,
                a.person_id,
                a.accused_code,
                a.seq_num,
                a.is_ccl,
                a.accused_status,
                p.full_name,
                p.alias             AS alias_name,
                p.age,
                p.gender,
                p.occupation,
                p.phone_number      AS phone_numbers,
                NULLIF(TRIM(CONCAT_WS(', ',
                    NULLIF(TRIM(p.present_house_no), ''),
                    NULLIF(TRIM(p.present_street_road_no), ''),
                    NULLIF(TRIM(p.present_locality_village), ''),
                    NULLIF(TRIM(p.present_area_mandal), ''),
                    NULLIF(TRIM(p.present_district), ''),
                    NULLIF(TRIM(p.present_state_ut), '')
                )), '') AS address
            FROM accused a
            LEFT JOIN persons p ON a.person_id = p.person_id
            WHERE a.crime_id = %s
        """
        cur.execute(query, (crime_id,))
        return cur.fetchall()


# ---------------------------------------------------------------------------
# Status Normalisation
# ---------------------------------------------------------------------------

_ARRESTED_KEYWORDS = frozenset([
    "arrested", "caught", "apprehended", "detained", "nabbed", "held",
    "taken into custody", "remanded", "produced before court",
    "surrendered", "confessed", "confession",
])

_ABSCONDING_KEYWORDS = frozenset([
    "absconding", "absconder", "evading", "fled", "on the run",
    "not traceable", "not found", "missing", "could not be traced",
    "yet to be arrested", "failed to appear", "escaped",
])


def normalize_accused_status(raw_status):
    """
    Normalises the free-text accused.accused_status field from the API into
    one of: 'arrested' | 'absconding' | None.

    Handles NULL, empty-string, and mixed-case variants gracefully.
    """
    if not raw_status:
        return None

    lowered = raw_status.strip().lower()
    if not lowered:
        return None

    for kw in _ARRESTED_KEYWORDS:
        if kw in lowered:
            return "arrested"

    for kw in _ABSCONDING_KEYWORDS:
        if kw in lowered:
            return "absconding"

    return None


# ---------------------------------------------------------------------------
# Value Guards
# ---------------------------------------------------------------------------

def truncate_varchar(value, max_length=255):
    """Truncate string values to fit VARCHAR constraints."""
    if value is None:
        return None
    if isinstance(value, str) and len(value) > max_length:
        return value[:max_length]
    return value


def validate_age(age_value):
    """
    Validate and sanitize age values for database insertion.
    Returns None if invalid, otherwise returns an integer between 0-150.
    """
    if age_value is None:
        return None

    try:
        if isinstance(age_value, str):
            import re
            match = re.search(r'\d+', str(age_value))
            if match:
                age_value = int(match.group())
            else:
                return None

        age_int = int(age_value)
        if age_int < 0 or age_int > 150:
            return None

        return age_int
    except (ValueError, TypeError, OverflowError):
        return None


def insert_accused_facts(conn, item_data):
    """Inserts extracted accused information into the database."""
    with conn.cursor() as cur:
        query = sql.SQL("""
            INSERT INTO {table} 
            (
                bf_accused_id, crime_id, 
                accused_id, person_id, existing_accused,
                full_name, alias_name, age, gender, occupation, address, phone_numbers,
                role_in_crime, key_details, accused_type, status, is_ccl,
                source_person_fields, source_accused_fields, source_summary_fields
            )
            VALUES (
                %s, %s, 
                %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s
            )
        """).format(table=sql.Identifier(config.ACCUSED_TABLE_NAME))

        import json
        import uuid

        bf_id = str(uuid.uuid4())

        # Guard constrained columns before the per-crime transaction commits.
        full_name    = truncate_varchar(item_data.get('full_name'), 500)
        alias_name   = truncate_varchar(item_data.get('alias_name'), 255)
        occupation   = truncate_varchar(item_data.get('occupation'), 255)
        phone_numbers = truncate_varchar(item_data.get('phone_numbers'), 255)
        gender       = truncate_varchar(item_data.get('gender'), 20)
        status       = truncate_varchar(item_data.get('status'), 40)
        age          = validate_age(item_data.get('age'))

        cur.execute(query, (
            bf_id,
            item_data.get('crime_id'),
            item_data.get('accused_id'),
            item_data.get('person_id'),
            item_data.get('existing_accused', False),
            full_name,
            alias_name,
            age,
            gender,
            occupation,
            item_data.get('address'),
            phone_numbers,
            item_data.get('role_in_crime'),
            item_data.get('key_details'),
            item_data.get('accused_type'),
            status,
            item_data.get('is_ccl', False),
            json.dumps(item_data.get('source_person_fields', {})),
            json.dumps(item_data.get('source_accused_fields', {})),
            json.dumps(item_data.get('source_summary_fields', {}))
        ))
