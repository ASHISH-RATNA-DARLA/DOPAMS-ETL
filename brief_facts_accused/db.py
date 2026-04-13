import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
import re
import json
import uuid
import logging
import config

logger = logging.getLogger(__name__)

UNIFIED_TABLE_NAME = "brief_facts_ai"
PROCESSING_LOG_TABLE = "etl_crime_processing_log"

# ---------------------------------------------------------------------------
# Column Length Constants (match DB-schema.sql)
# ---------------------------------------------------------------------------
MAX_FULL_NAME_LEN = 500
MAX_ALIAS_LEN     = 255
MAX_OCCUPATION_LEN = 255
MAX_PHONE_LEN     = 255
MAX_GENDER_LEN    = 20
MAX_STATUS_LEN    = 40


import sys
import os
# Import PostgreSQLConnectionPool
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_pooling import get_db_connection as get_pooled_connection, return_db_connection

def get_db_connection():
    """Establishes a connection to the PostgreSQL database via pool."""
    try:
        conn = get_pooled_connection()
        # Note: autocommit is already False by default in psycopg2
        # Do not set it here as connection may already be in a transaction
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database via pool: {e}")
        raise


def fetch_crimes_by_ids(conn, crime_ids):
    """
    Fetches specific crimes based on a list of IDs.
    """
    if not crime_ids:
        return []

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        query = "SELECT crime_id, ps_code, brief_facts FROM crimes WHERE crime_id = ANY(%s)"
        cur.execute(query, (crime_ids,))
        return cur.fetchall()


def fetch_unprocessed_crimes(conn, limit=100):
    """
    Fetches crimes that need processing.

    In unified mode, a crime is selected when:
      - it has never completed successfully, or
      - the source crime row has been modified after the last successful run.

    In legacy mode, the old NOT EXISTS behavior is preserved.
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        if (config.ACCUSED_TABLE_NAME or "").lower() == UNIFIED_TABLE_NAME:
            query = """
                SELECT
                    c.crime_id,
                    c.ps_code,
                    c.brief_facts,
                    COALESCE(c.date_modified, c.date_created) AS source_changed_at,
                    last_run.last_completed_at
                FROM crimes c
                LEFT JOIN LATERAL (
                    SELECT MAX(l.completed_at) AS last_completed_at
                    FROM public.etl_crime_processing_log l
                    WHERE l.crime_id = c.crime_id
                      AND l.status = 'complete'
                ) last_run ON TRUE
                WHERE last_run.last_completed_at IS NULL
                   OR COALESCE(c.date_modified, c.date_created) > last_run.last_completed_at
                ORDER BY COALESCE(c.date_modified, c.date_created) DESC NULLS LAST,
                         c.date_created DESC NULLS LAST
                LIMIT %s
            """
            cur.execute(query, (limit,))
            return cur.fetchall()

        query = sql.SQL("""
            SELECT c.crime_id, c.ps_code, c.brief_facts
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


def delete_brief_facts_for_crime(conn, crime_id):
    """Remove all unified brief_facts rows for a crime before a refresh run."""
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM public.brief_facts_ai
            WHERE crime_id = %s
            """,
            (crime_id,),
        )


def fetch_dedup_candidates(conn, current_crime_id, full_name, ps_code=None, limit=200):
    """
    Candidate retrieval for canonical identity resolution from brief_facts_ai.
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
                        SELECT bfa.bf_accused_id,
                                     bfa.canonical_person_id,
                                     bfa.accused_id,
                                     bfa.person_code,
                                     bfa.full_name,
                                     bfa.alias_name,
                                     bfa.age,
                                     bfa.gender,
                                     bfa.address,
                                     bfa.source_accused_fields,
                                     bfa.crime_id,
                                     c.major_head,
                                     c.minor_head,
                                     c.crime_type,
                                     c.acts_sections
                        FROM public.brief_facts_ai bfa
                        LEFT JOIN public.crimes c ON c.crime_id = bfa.crime_id
                        WHERE bfa.crime_id != %s
                            AND bfa.full_name IS NOT NULL
              AND (
                                        SOUNDEX(bfa.full_name) = SOUNDEX(%s)
                                        OR dmetaphone(COALESCE(bfa.full_name, '')) = dmetaphone(%s)
                                        OR bfa.source_accused_fields->>'ps_code' = %s
                  )
            LIMIT %s
            """,
                        (current_crime_id, full_name or '', full_name or '', ps_code, limit),
        )
        return cur.fetchall()


def fetch_crime_profile(conn, crime_id):
    """Fetches contextual offense metadata used for dedup scoring boosts."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT crime_id, major_head, minor_head, crime_type, acts_sections
            FROM public.crimes
            WHERE crime_id = %s
            LIMIT 1
            """,
            (crime_id,),
        )
        return cur.fetchone() or {}


def fetch_crime_associate_person_codes(conn, crime_id):
    """
    Returns normalized person-code set (A-<n>) for accused in a crime,
    used as co-accused network signal in dedup scoring.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT person_code
            FROM public.brief_facts_ai
            WHERE crime_id = %s
              AND accused_id IS NOT NULL
              AND person_code IS NOT NULL
            """,
            (crime_id,),
        )
        rows = cur.fetchall()

    normalized = set()
    for row in rows:
        code = row[0] if row else None
        if not code:
            continue
        match = re.search(r'A\s*[-.]?\s*(\d+)', str(code), flags=re.IGNORECASE)
        if match:
            normalized.add(f"A-{int(match.group(1))}")
    return normalized


def update_sentinel_role(conn, crime_id, old_role, new_role):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE public.brief_facts_ai
            SET role_in_crime = %s,
                date_modified = CURRENT_TIMESTAMP
            WHERE crime_id = %s
              AND role_in_crime = %s
            """,
            (new_role, crime_id, old_role),
        )


def start_crime_processing_run(conn, crime_id):
    """Create a new processing-log run row and return run_id."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.etl_crime_processing_log (crime_id, status)
            VALUES (%s, 'in_progress')
            RETURNING run_id
            """,
            (crime_id,),
        )
        return str(cur.fetchone()[0])


def complete_crime_processing_run(conn, run_id, accused_count_written):
    """Mark a processing-log run complete."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE public.etl_crime_processing_log
            SET status = 'complete',
                accused_count_written = %s,
                completed_at = CURRENT_TIMESTAMP,
                error_detail = NULL
            WHERE run_id = %s
            """,
            (accused_count_written, run_id),
        )


def fail_crime_processing_run(conn, run_id, error_detail):
    """Mark a processing-log run failed."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE public.etl_crime_processing_log
            SET status = 'failed',
                completed_at = CURRENT_TIMESTAMP,
                error_detail = %s
            WHERE run_id = %s
            """,
            (str(error_detail)[:4000], run_id),
        )


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
                a.type              AS accused_type_db,
                a.is_ccl,
                a.accused_status,
                p.full_name,
                p.alias             AS alias_name,
                p.age,
                p.date_of_birth,
                p.gender,
                p.occupation,
                p.phone_number      AS phone_numbers,
                NULLIF(TRIM(CONCAT_WS(', ',
                    NULLIF(TRIM(p.present_house_no), ''),
                    NULLIF(TRIM(p.present_street_road_no), ''),
                    NULLIF(TRIM(p.present_ward_colony), ''),
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


def resolve_status_for_insert(raw_db_status, text, name_hint):
    """
    Returns the raw DB accused_status if present (stored as-is per spec),
    otherwise falls back to keyword-based detection from brief_facts text.
    """
    if raw_db_status and str(raw_db_status).strip():
        return str(raw_db_status).strip()

    # Fallback: keyword scan on local context window
    return normalize_accused_status(raw_db_status) or _keyword_status_fallback(text, name_hint)


def _keyword_status_fallback(text, name_hint):
    """Keyword scan on local context window (±120 chars around name)."""
    text_lower = (text or "").lower()
    candidate = (name_hint or "").lower()
    combined = ""
    if candidate:
        idx = text_lower.find(candidate)
        if idx >= 0:
            start = max(0, idx - 120)
            end = min(len(text_lower), idx + len(candidate) + 120)
            combined = text_lower[start:end]

    if not combined:
        return None

    for kw in _ABSCONDING_KEYWORDS:
        if kw in combined:
            return "absconding"
    for kw in _ARRESTED_KEYWORDS:
        if kw in combined:
            return "arrested"

    return None


# ---------------------------------------------------------------------------
# Alias Stripping
# ---------------------------------------------------------------------------

def strip_alias_name(raw_alias):
    """
    Strips 'name @alias' format to just the alias portion.
    E.g. 'Mohammed Imran @Rocky' → 'Rocky'
         'Rocky' → 'Rocky'
         None → None
    """
    if not raw_alias:
        return None
    alias_str = str(raw_alias).strip()
    if not alias_str:
        return None
    if '@' in alias_str:
        return alias_str.split('@', 1)[1].strip() or None
    return alias_str


# ---------------------------------------------------------------------------
# Age from DOB Fallback
# ---------------------------------------------------------------------------

def compute_age_from_dob(dob):
    """
    Computes age from date_of_birth if age is NULL.
    Returns int age or None.
    """
    if not dob:
        return None
    try:
        from datetime import date
        today = date.today()
        if hasattr(dob, 'year'):
            return today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
        return None
    except Exception:
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
        unified_mode = (config.ACCUSED_TABLE_NAME or "").lower() == UNIFIED_TABLE_NAME

        if unified_mode:
            query = """
                INSERT INTO public.brief_facts_ai
                (
                    bf_accused_id, crime_id,
                    accused_id, person_id, canonical_person_id,
                    person_code, seq_num, existing_accused,
                    full_name, alias_name, age, gender, occupation, address, phone_numbers,
                    role_in_crime, key_details, accused_type, status, is_ccl,
                    dedup_match_tier, dedup_confidence, dedup_review_flag,
                    source_person_fields, source_accused_fields, source_summary_fields,
                    drugs, etl_run_id
                )
                VALUES (
                    %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s
                )
                ON CONFLICT (crime_id, accused_id) DO NOTHING
            """
        else:
            query = sql.SQL("""
                INSERT INTO {table} 
                (
                    bf_accused_id, crime_id, 
                    accused_id, person_id, person_code, seq_num, existing_accused,
                    full_name, alias_name, age, gender, occupation, address, phone_numbers,
                    role_in_crime, key_details, accused_type, status, is_ccl,
                    source_person_fields, source_accused_fields, source_summary_fields
                )
                VALUES (
                    %s, %s, 
                    %s, %s, %s, %s, %s,
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

        person_code = truncate_varchar(item_data.get('person_code'), 50)
        seq_num     = truncate_varchar(item_data.get('seq_num'), 50)

        try:
            if unified_mode:
                etl_run_id = item_data.get('etl_run_id')
                if not etl_run_id:
                    raise ValueError("etl_run_id is required for brief_facts_ai inserts")

                role_in_crime = item_data.get('role_in_crime')
                if not item_data.get('accused_id') and role_in_crime in {
                    'LLM_EXTRACTION_FAILED',
                    'NO_ACCUSED_IN_TEXT',
                    'NO_ACCUSED_DRUGS_ONLY',
                }:
                    cur.execute(
                        """
                        DELETE FROM public.brief_facts_ai
                        WHERE crime_id = %s
                          AND role_in_crime = %s
                          AND accused_id IS NULL
                        """,
                        (item_data.get('crime_id'), role_in_crime),
                    )

                cur.execute(query, (
                    bf_id,
                    item_data.get('crime_id'),
                    item_data.get('accused_id'),
                    item_data.get('person_id'),
                    item_data.get('canonical_person_id'),
                    person_code,
                    seq_num,
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
                    item_data.get('dedup_match_tier'),
                    item_data.get('dedup_confidence'),
                    item_data.get('dedup_review_flag', False),
                    json.dumps(item_data.get('source_person_fields', {})),
                    json.dumps(item_data.get('source_accused_fields', {})),
                    json.dumps(item_data.get('source_summary_fields', {})),
                    json.dumps(item_data.get('drugs')) if item_data.get('drugs') is not None else None,
                    etl_run_id,
                ))
            else:
                cur.execute(query, (
                    bf_id,
                    item_data.get('crime_id'),
                    item_data.get('accused_id'),
                    item_data.get('person_id'),
                    person_code,
                    seq_num,
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
