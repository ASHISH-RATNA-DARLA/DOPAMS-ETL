import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
import re
import json
import uuid
import logging
import config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Column Length Constants (match DB-schema.sql)
# ---------------------------------------------------------------------------
MAX_FULL_NAME_LEN = 500
MAX_ALIAS_LEN     = 255
MAX_OCCUPATION_LEN = 255
MAX_PHONE_LEN     = 255
MAX_GENDER_LEN    = 20
MAX_STATUS_LEN    = 40


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
        conn.autocommit = False  # Explicit transaction control
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
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
    Uses NOT EXISTS (faster and safer than LEFT JOIN for existence checks).
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        query = sql.SQL("""
            SELECT c.crime_id, c.brief_facts
            FROM crimes c
            WHERE NOT EXISTS (
                SELECT 1
                FROM {table} d
                WHERE d.crime_id = c.crime_id
            )
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
      CONCAT(present_*)    → address  (single string, NULL if all parts empty)
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
                    NULLIF(TRIM(p.present_state_ut), ''),
                    NULLIF(TRIM(p.present_country), '')
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

_ABSCONDING_KEYWORDS = [
    "absconding", "absconder", "evading", "fled", "on the run",
    "not traceable", "not found", "missing", "could not be traced",
    "yet to be arrested", "failed to appear", "escaped",
]

_ARRESTED_KEYWORDS = [
    "arrested", "caught", "apprehended", "detained", "nabbed", "held",
    "taken into custody", "remanded", "produced before court",
    "surrendered", "confessed", "confession",
]


def normalize_accused_status(raw_status):
    """
    Normalises the free-text accused.accused_status field from the API into
    one of: 'arrested' | 'absconding' | None.

    Priority: absconding checked first (legally, a person is absconding
    until proven arrested — "absconding but later arrested" edge case).
    """
    if not raw_status:
        return None

    lowered = raw_status.strip().lower()
    if not lowered:
        return None

    # Absconding has higher legal priority
    for kw in _ABSCONDING_KEYWORDS:
        if kw in lowered:
            return "absconding"

    for kw in _ARRESTED_KEYWORDS:
        if kw in lowered:
            return "arrested"

    return None


# ---------------------------------------------------------------------------
# Phone Number Normalisation
# ---------------------------------------------------------------------------

def normalize_phone_numbers(raw_phone):
    """
    Extracts 10-digit Indian phone numbers from free-text.
    Input: 'Ph: 9347584387, Cell: 9989478322' or '9347584387, 9989478322'
    Output: '9347584387, 9989478322' or None
    """
    if not raw_phone:
        return None
    raw = str(raw_phone)
    numbers = re.findall(r'\d{10}', raw)
    if numbers:
        return ', '.join(numbers)
    # Fallback: return raw if it looks phone-like but doesn't match 10-digit
    stripped = raw.strip()
    return stripped if stripped else None


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
    """
    Inserts extracted accused information into the database.
    Uses ON CONFLICT to prevent duplicates on re-runs.
    Catches DB errors per-row so a single bad row doesn't abort the batch.
    """
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

        bf_id = str(uuid.uuid4())

        # Guard constrained columns before the per-crime transaction commits.
        full_name     = truncate_varchar(item_data.get('full_name'), MAX_FULL_NAME_LEN)
        alias_name    = truncate_varchar(item_data.get('alias_name'), MAX_ALIAS_LEN)
        occupation    = truncate_varchar(item_data.get('occupation'), MAX_OCCUPATION_LEN)
        phone_numbers = truncate_varchar(
            normalize_phone_numbers(item_data.get('phone_numbers')),
            MAX_PHONE_LEN
        )
        gender        = truncate_varchar(item_data.get('gender'), MAX_GENDER_LEN)
        status        = truncate_varchar(item_data.get('status'), MAX_STATUS_LEN)
        age           = validate_age(item_data.get('age'))

        try:
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
        except psycopg2.Error as e:
            logger.error(
                f"DB insert failed for crime={item_data.get('crime_id')} "
                f"name={full_name}: {e}"
            )
            raise
