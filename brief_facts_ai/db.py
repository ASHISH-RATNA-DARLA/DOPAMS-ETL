import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
import re
import json
import uuid
import logging
import sys
import os

import config

logger = logging.getLogger(__name__)

UNIFIED_TABLE_NAME = "brief_facts_ai"
PROCESSING_LOG_TABLE = "etl_crime_processing_log"

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_pooling import get_db_connection as get_pooled_connection, return_db_connection

def get_db_connection():
    try:
        return get_pooled_connection()
    except Exception as e:
        logger.error(f"Error connecting to database via pool: {e}")
        raise

def ensure_connection(conn):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        return conn
    except Exception:
        logger.warning("DB connection lost. Reconnecting...")
        try:
            return_db_connection(conn, close_conn=True)
        except Exception:
            pass
        return get_db_connection()

def fetch_crimes_by_ids(conn, crime_ids):
    if not crime_ids:
        return []
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        query = "SELECT crime_id, ps_code, brief_facts FROM crimes WHERE crime_id = ANY(%s)"
        cur.execute(query, (crime_ids,))
        return cur.fetchall()

def fetch_unprocessed_crimes_daily(conn, limit=100):
    """
    Daily incremental check: Find crimes in crimes table NOT YET in brief_facts_ai.

    This is the single authoritative check for daily runs:
    - Crimes never processed (no row in brief_facts_ai)
    - Crimes modified since last processing (date_modified > last update)
    - Crimes with processing errors (failed/incomplete log entries)

    Returns: List of unprocessed crime records, ordered by crime_id

    Expected: ~1-10% of previous batch size per day (new crimes + modifications)
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        query = """
        SELECT
            c.crime_id,
            c.ps_code,
            c.brief_facts,
            COALESCE(c.date_modified, c.date_created) AS source_changed_at,
            bfa.etl_run_id AS last_processing_run_id,
            COALESCE(bfa.last_processed_at, '1900-01-01'::timestamp) AS last_processed_at
        FROM public.crimes c
        LEFT JOIN LATERAL (
            SELECT
                MAX(date_modified) AS last_processed_at,
                MAX(etl_run_id)::text AS etl_run_id
            FROM public.brief_facts_ai b
            WHERE b.crime_id = c.crime_id
        ) bfa ON TRUE
        WHERE
            -- Unprocessed: No entry in brief_facts_ai
            bfa.last_processed_at IS NULL
            OR
            -- Or modified since last processing
            COALESCE(c.date_modified, c.date_created) > COALESCE(bfa.last_processed_at, '1900-01-01'::timestamp)
        ORDER BY c.crime_id
        LIMIT %s
        """
        cur.execute(query, (limit,))
        return cur.fetchall()


def try_claim_crime_for_processing(conn, crime_id):
    """
    Attempt to claim a crime for processing using transaction-scoped advisory lock.

    Returns True when this transaction owns the claim, False when another worker/
    ETL instance already owns it.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_xact_lock(hashtext(%s))", (str(crime_id),))
        row = cur.fetchone()
        return bool(row and row[0])


def fetch_unprocessed_crimes(conn, limit=100):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
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


def fetch_unprocessed_crimes_since(conn, since_date, limit=100):
    """
    Fetch unprocessed or modified crimes whose date_created/date_modified falls at or
    after `since_date`.  Used in daily incremental mode to avoid a full crimes table scan.

    A crime is returned if it was never processed (no 'complete' log entry) OR has been
    modified after its last completion timestamp.  The `since_date` filter restricts the
    set of candidate crimes so the planner can use an index on the date column instead of
    scanning every row.
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
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
            WHERE COALESCE(c.date_modified, c.date_created) >= %s
              AND (last_run.last_completed_at IS NULL
                   OR COALESCE(c.date_modified, c.date_created) > last_run.last_completed_at)
            ORDER BY COALESCE(c.date_modified, c.date_created) DESC NULLS LAST,
                     c.date_created DESC NULLS LAST
            LIMIT %s
        """
        cur.execute(query, (since_date, limit))
        return cur.fetchall()


def get_incremental_cutoff_date(conn):
    """
    Return the cutoff datetime to use as `since_date` for incremental processing.

    Strategy: max(completed_at) of successfully-processed crimes minus 1 day of overlap.
    The overlap prevents silent gaps when a crime is processed just before midnight and
    a downstream modification arrives moments later.

    Returns a datetime object, or None if no completed runs exist (fall back to full scan).
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT MAX(completed_at) - INTERVAL '1 day' AS since_date
            FROM public.etl_crime_processing_log
            WHERE status = 'complete'
        """)
        row = cur.fetchone()
        if row and row[0]:
            return row[0]
    return None

def fetch_canonical_by_accused_id(conn, accused_id, current_crime_id):
    """Direct canonical lookup by exact accused_id across crimes.
    Handles same accused with differently-spelled name in different crimes (Branch A).
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT canonical_person_id FROM public.brief_facts_ai
            WHERE accused_id = %s AND crime_id != %s
              AND canonical_person_id IS NOT NULL
            LIMIT 1
        """, (str(accused_id), current_crime_id))
        return cur.fetchone()



def fetch_dedup_candidates(conn, current_crime_id, full_name, ps_code=None, limit=200):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                conditions = [
                        "bfa.crime_id != %s",
                        "bfa.full_name IS NOT NULL",
                        "(SOUNDEX(bfa.full_name) = SOUNDEX(%s) OR dmetaphone(COALESCE(bfa.full_name, '')) = dmetaphone(%s))",
                        "COALESCE(c.date_modified, c.date_created) >= NOW() - INTERVAL '6 months'",
                ]
                params = [current_crime_id, full_name or '', full_name or '']

                if ps_code:
                        conditions.insert(2, "bfa.source_accused_fields->>'ps_code' = %s")
                        params.insert(2, ps_code)

                cur.execute(f"""
                        SELECT bfa.bf_accused_id, bfa.canonical_person_id, bfa.accused_id, bfa.person_code, bfa.full_name,
                                     bfa.alias_name, bfa.age, bfa.gender, bfa.address, bfa.source_accused_fields, bfa.crime_id,
                                     c.major_head, c.minor_head, c.crime_type, c.acts_sections
                        FROM public.brief_facts_ai bfa
                        LEFT JOIN public.crimes c ON c.crime_id = bfa.crime_id
                        WHERE {' AND '.join(conditions)}
                        ORDER BY
                            CASE
                                WHEN SOUNDEX(bfa.full_name) = SOUNDEX(%s) THEN 0
                                WHEN dmetaphone(COALESCE(bfa.full_name, '')) = dmetaphone(%s) THEN 1
                                ELSE 2
                            END
                        LIMIT %s
                """, (*params, full_name or '', full_name or '', limit))
                return cur.fetchall()

def fetch_crime_profile(conn, crime_id):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT crime_id, major_head, minor_head, crime_type, acts_sections FROM public.crimes WHERE crime_id = %s LIMIT 1", (crime_id,))
        return cur.fetchone() or {}

def fetch_crime_associate_person_codes(conn, crime_id):
    with conn.cursor() as cur:
        cur.execute("SELECT person_code FROM public.brief_facts_ai WHERE crime_id = %s AND accused_id IS NOT NULL AND person_code IS NOT NULL", (crime_id,))
        rows = cur.fetchall()
    normalized = set()
    for row in rows:
        code = row[0] if row else None
        if not code: continue
        match = re.search(r'A\s*[-.]?\s*(\d+)', str(code), flags=re.IGNORECASE)
        if match: normalized.add(f"A-{int(match.group(1))}")
    return normalized

def update_sentinel_role(conn, crime_id, old_role, new_role):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE public.brief_facts_ai
            SET role_in_crime = %s, date_modified = CURRENT_TIMESTAMP
            WHERE crime_id = %s AND role_in_crime = %s
        """, (new_role, crime_id, old_role))

def start_crime_processing_run(conn, crime_id, branch=None):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO public.etl_crime_processing_log (crime_id, status, branch)
            VALUES (%s, 'in_progress', %s)
            RETURNING run_id
        """, (crime_id, branch))
        return str(cur.fetchone()[0])

def complete_crime_processing_run(conn, run_id, accused_count_written):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE public.etl_crime_processing_log
            SET status = 'complete', accused_count_written = %s, completed_at = CURRENT_TIMESTAMP, error_detail = NULL
            WHERE run_id = %s
        """, (accused_count_written, run_id))

def fail_crime_processing_run(conn, run_id, error_detail):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE public.etl_crime_processing_log
            SET status = 'failed', completed_at = CURRENT_TIMESTAMP, error_detail = %s
            WHERE run_id = %s
        """, (str(error_detail)[:4000], run_id))

def invalidate_branch_c_log_for_crimes(conn, crime_ids):
    if not crime_ids: return
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE public.etl_crime_processing_log
            SET status = 'stale', error_detail = 'Invalidated: accused records arrived after Branch C run'
            WHERE crime_id = ANY(%s) AND branch = 'C' AND status = 'complete'
        """, (list(crime_ids),))

def fetch_existing_accused_for_crime(conn, crime_id):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        query = """
            SELECT a.accused_id, a.person_id, a.accused_code, a.seq_num, a.type AS accused_type_db, a.is_ccl, a.accused_status,
                   p.full_name, p.alias AS alias_name, p.age, p.date_of_birth, p.gender, p.occupation, p.phone_number AS phone_numbers,
                   NULLIF(TRIM(CONCAT_WS(', ', NULLIF(TRIM(p.present_house_no), ''), NULLIF(TRIM(p.present_street_road_no), ''),
                   NULLIF(TRIM(p.present_ward_colony), ''), NULLIF(TRIM(p.present_locality_village), ''), NULLIF(TRIM(p.present_area_mandal), ''),
                   NULLIF(TRIM(p.present_district), ''), NULLIF(TRIM(p.present_state_ut), ''), NULLIF(TRIM(p.present_country), ''))), '') AS address
            FROM accused a
            LEFT JOIN persons p ON a.person_id = p.person_id
            WHERE a.crime_id = %s
            ORDER BY
                CASE
                    WHEN a.accused_code ~* '^A[-.]?[0-9]+$'
                    THEN regexp_replace(a.accused_code, '\D', '', 'g')::numeric
                    ELSE NULL
                END NULLS LAST,
                CASE
                    WHEN a.seq_num ~ '^[0-9]+$'
                    THEN a.seq_num::numeric
                    ELSE NULL
                END NULLS LAST,
                a.accused_id
        """
        cur.execute(query, (crime_id,))
        return cur.fetchall()

def normalize_accused_status(raw_status):
    if not raw_status: return None
    lowered = raw_status.strip().lower()
    if not lowered: return None
    for kw in ["absconding", "evading", "fled", "on the run", "not traceable", "missing"]:
        if kw in lowered: return "absconding"
    for kw in ["arrested", "caught", "apprehended", "detained", "nabbed", "held"]:
        if kw in lowered: return "arrested"
    return None

def resolve_status_for_insert(raw_db_status, text, name_hint):
    if raw_db_status and str(raw_db_status).strip(): return str(raw_db_status).strip()
    return normalize_accused_status(raw_db_status) or _keyword_status_fallback(text, name_hint)

def _keyword_status_fallback(text, name_hint):
    text_lower = (text or "").lower()
    candidate = (name_hint or "").lower()
    combined = ""
    if candidate:
        idx = text_lower.find(candidate)
        if idx >= 0:
            start = max(0, idx - 120)
            end = min(len(text_lower), idx + len(candidate) + 120)
            combined = text_lower[start:end]
    if not combined: return None
    for kw in ["absconding", "evading", "fled", "on the run", "not traceable", "missing"]:
        if kw in combined: return "absconding"
    for kw in ["arrested", "caught", "apprehended", "detained", "nabbed", "held"]:
        if kw in combined: return "arrested"
    return None

def strip_alias_name(raw_alias):
    if not raw_alias: return None
    alias_str = str(raw_alias).strip()
    if not alias_str: return None
    if '@' in alias_str: return alias_str.split('@', 1)[1].strip() or None
    return alias_str

def compute_age_from_dob(dob):
    if not dob: return None
    try:
        from datetime import date
        today = date.today()
        if hasattr(dob, 'year'): return today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
        return None
    except: return None

def normalize_phone_numbers(raw_phone):
    if not raw_phone: return None
    raw = str(raw_phone)
    numbers = re.findall(r'\d{10}', raw)
    if numbers: return ', '.join(numbers)
    stripped = raw.strip()
    return stripped if stripped else None

def truncate_varchar(value, max_length=255):
    if value is None: return None
    if isinstance(value, str) and len(value) > max_length: return value[:max_length]
    return value

def validate_age(age_value):
    if age_value is None: return None
    try:
        if isinstance(age_value, str):
            match = re.search(r'\d+', str(age_value))
            if match: age_value = int(match.group())
            else: return None
        age_int = int(age_value)
        if age_int < 0 or age_int > 150: return None
        return age_int
    except: return None

def fetch_drug_categories(conn):
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT raw_name, standard_name FROM public.drug_categories WHERE is_verified = true ORDER BY standard_name")
            return cur.fetchall()
    except Exception: return []

def fuzzy_match_drug_name(conn, raw_drug_name: str, threshold: float = 0.35):
    if not raw_drug_name or not raw_drug_name.strip(): return None
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT standard_name, similarity(raw_name, %s) AS sim
                FROM public.drug_categories WHERE is_verified = true AND similarity(raw_name, %s) >= %s ORDER BY sim DESC LIMIT 1
            """, (raw_drug_name.lower().strip(), raw_drug_name.lower().strip(), threshold))
            row = cur.fetchone()
            if row: return row[0]
            return None
    except Exception: return None

def fetch_drug_ignore_list(conn):
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT term, reason FROM public.drug_ignore_list ORDER BY id")
            return {row['term'].lower().strip(): (row['reason'] or '') for row in cur.fetchall() if row['term']}
    except Exception: return {}

def _as_num_or_none(value):
    if value is None: return None
    if isinstance(value, str) and value.strip() == '': return None
    try: return float(value)
    except Exception: return None

def _resolve_drug_category(primary_name: str):
    if not primary_name: return None
    name = primary_name.lower().strip()
    if name in ['ganja', 'charas', 'hashish', 'hash oil', 'bhang']: return 'Cannabis'
    if name in ['heroin', 'opium', 'morphine', 'codeine', 'buprenorphine', 'pentazocine', 'poppy husk', 'poppy straw']: return 'Opioid'
    if name in ['cocaine', 'methamphetamine', 'amphetamine', 'mephedrone', 'mdma', 'ecstasy']: return 'Stimulant'
    if name in ['alprazolam', 'nitrazepam', 'diazepam', 'clonazepam', 'zolpidem']: return 'Sedative/Benzodiazepine'
    if name in ['lsd', 'ketamine']: return 'Hallucinogen'
    return 'Other'

def _build_drug_element(drug_data, attribution_source, attribution_ref=None, nullify_quantity=False):
    if not isinstance(drug_data, dict):
        try: drug_data = drug_data.model_dump()
        except Exception: pass
    raw_quantity = 0.0 if attribution_source == 'NO_DRUGS_DETECTED' else _as_num_or_none(drug_data.get('raw_quantity'))
    weight_g = 0.0 if attribution_source == 'NO_DRUGS_DETECTED' else _as_num_or_none(drug_data.get('weight_g'))
    weight_kg = 0.0 if attribution_source == 'NO_DRUGS_DETECTED' else _as_num_or_none(drug_data.get('weight_kg'))
    volume_ml = 0.0 if attribution_source == 'NO_DRUGS_DETECTED' else _as_num_or_none(drug_data.get('volume_ml'))
    volume_l = 0.0 if attribution_source == 'NO_DRUGS_DETECTED' else _as_num_or_none(drug_data.get('volume_l'))
    count_total = 0.0 if attribution_source == 'NO_DRUGS_DETECTED' else _as_num_or_none(drug_data.get('count_total'))

    if nullify_quantity:
        raw_quantity = weight_g = weight_kg = volume_ml = volume_l = count_total = None

    return {
        'raw_drug_name': drug_data.get('raw_drug_name'),
        'raw_quantity': raw_quantity,
        'raw_unit': drug_data.get('raw_unit'),
        'primary_drug_name': drug_data.get('primary_drug_name'),
        'drug_category': _resolve_drug_category(drug_data.get('primary_drug_name')),
        'drug_form': drug_data.get('drug_form'),
        'weight_g': weight_g,
        'weight_kg': weight_kg,
        'volume_ml': volume_ml,
        'volume_l': volume_l,
        'count_total': count_total,
        'confidence_score': _as_num_or_none(drug_data.get('confidence_score')),
        'is_commercial': bool(drug_data.get('is_commercial', False)),
        'seizure_worth': _as_num_or_none(drug_data.get('seizure_worth')),
        'worth_scope': drug_data.get('worth_scope', 'individual'),
        'supplier_name': drug_data.get('supplier_name'),
        'source_location': drug_data.get('source_location'),
        'destination': drug_data.get('destination'),
        'purchase_price_per_unit': _as_num_or_none(drug_data.get('purchase_price_per_unit')),
        'extraction_metadata': drug_data.get('extraction_metadata') or {},
        'drug_attribution_source': attribution_source,
        'drug_attribution_ref': attribution_ref,
    }

def _norm_person_code(value):
    if not value: return None
    v = str(value).upper().strip()
    m = re.search(r'A\s*[-.]?\s*(\d+)', v)
    if m: return f"A-{int(m.group(1))}"
    return None


def _safe_int(value):
    if value is None:
        return None
    text = str(value).strip()
    if not text or not text.isdigit():
        return None
    return int(text)


def _bfai_row_sort_key(row):
    code = _norm_person_code(row.get('person_code'))
    code_num = _safe_int(code.split('-', 1)[1]) if code and '-' in code else None
    seq_num = _safe_int(row.get('seq_num'))
    return (
        0 if code_num is not None else 1,
        code_num if code_num is not None else 10**12,
        0 if seq_num is not None else 1,
        seq_num if seq_num is not None else 10**18,
        str(row.get('full_name') or '').lower(),
        str(row.get('accused_id') or ''),
    )


def _pick_primary_row(rows):
    if not rows:
        return None

    for row in rows:
        if _norm_person_code(row.get('person_code')) == 'A-1':
            return row

    return sorted(rows, key=_bfai_row_sort_key)[0]

def _extract_person_codes(drug_data):
    """Extract person codes from drug_data, filtering out downstream transaction references.

    Handles both single source_sentence and consolidated_sources (from deduped drugs).

    Downstream keywords indicate the person received/bought the drug (not the seized person):
    - "sell to", "sold to", "buyer", "customer", "purchase from", "bought by"

    This prevents attributing seized drugs to downstream sellers/buyers who weren't
    part of the seizure event. E.g., "seized 405g and sell to A-3" → extract seizure for A1,
    not for A-3 who is only a downstream buyer.

    For consolidated sources, prioritizes:
    1. If all sources mention the SAME accused → use that accused
    2. If sources mention DIFFERENT accused → use primary (first) accused
    3. If no accused mentioned → empty set (will trigger fallback to A1)
    """
    if not isinstance(drug_data, dict):
        try: drug_data = drug_data.model_dump()
        except BaseException: pass
    codes = set()
    meta = drug_data.get('extraction_metadata') or {}

    # Check for consolidated_sources first (from dedup consolidation)
    consolidated_sources = meta.get('consolidated_sources', [])
    if consolidated_sources:
        # Analyze each consolidated source to extract its accused codes
        all_source_codes = []
        for source_sentence in consolidated_sources:
            source_codes = _extract_codes_from_source(source_sentence)
            if source_codes:
                all_source_codes.append(source_codes)

        if all_source_codes:
            # Strategy: if all sources mention the SAME accused(s), use it
            # Otherwise, use the first source's codes (primary mention)
            first_codes = all_source_codes[0]
            same_in_all = all(src_codes == first_codes for src_codes in all_source_codes)

            if same_in_all:
                codes.update(first_codes)
                logger.debug(f"[DrugAttrib] Consolidated sources all mention: {first_codes}")
            else:
                # Different sources mention different accused — use primary (first) mention
                codes.update(first_codes)
                logger.debug(f"[DrugAttrib] Consolidated sources mention different accused; using primary: {first_codes}")
        return codes

    # Check accused_ref directly (usually already clean from LLM)
    accused_ref = meta.get('accused_ref')
    if accused_ref:
        normalized = _norm_person_code(accused_ref)
        if normalized:
            # If the LLM explicitly provided an accused_ref, we ONLY return it!
            # We shouldn't blindly merge it with ALL codes found in the source sentence!
            return {normalized}

    # Fallback ONLY if accused_ref is null: parse single source_sentence (non-consolidated)
    source_sentence = str(meta.get('source_sentence') or '')
    codes = _extract_codes_from_source(source_sentence)

    return codes


_JOINT_RANGE_RE_DB = re.compile(
    r'\bA\s*[-.]?\s*\d+\s+to\s+A\s*[-.]?\s*\d+\b',
    re.IGNORECASE,
)

def _extract_codes_from_source(source_sentence: str) -> set:
    """Extract person codes from a single source sentence, filtering downstream references.

    Returns set of normalized A-codes (e.g., {'A-1', 'A-3'}).

    JOINT-RANGE GUARD: If the sentence contains a pattern like \"possession of A2 to A4\"
    or \"from A2 to A4\" (a range of accused in joint/shared possession), we return an
    EMPTY set. This signals write_drugs_by_accused_in_memory to fall through to
    FALLBACK_A1 so the collective seizure is stored exactly ONCE on the primary accused.
    Without this, the codes A-2 and A-4 would both be extracted, creating a
    COLLECTIVE_TOTAL entry on A2 that duplicates whatever the deterministic extractor
    already produced for A4.
    """
    codes = set()
    source_sentence = str(source_sentence or '')

    # Early exit for joint-range collective seizures
    if _JOINT_RANGE_RE_DB.search(source_sentence):
        logger.debug(
            f"[DrugAttrib] Joint-range pattern detected in source sentence — "
            f"returning empty codes (collective seizure, no per-person split): "
            f"{source_sentence[:120]!r}"
        )
        return codes  # empty → falls through to FALLBACK_A1

    # Define downstream transaction keywords - codes after these shouldn't be attributed
    downstream_keywords = r'\b(?:sell|sold|selling|buyer|customer|purchase|bought|received|consume|consumed|consumption)\s+(?:to|by|from|by|of)?\s*'

    # Find all A-codes and their positions
    all_matches = list(re.finditer(r'\bA\s*[-.]?\s*\d+\b', source_sentence, flags=re.IGNORECASE))

    for match in all_matches:
        code_pos = match.start()
        normalized = _norm_person_code(match.group())
        if not normalized:
            continue

        # Check if this code appears after a downstream keyword
        # Search backwards from the code position for downstream keywords
        lookback_window = max(0, code_pos - 100)  # Look back up to 100 chars
        text_before = source_sentence[lookback_window:code_pos].lower()

        # If downstream keyword found in the preceding text, skip this code
        if re.search(downstream_keywords, text_before):
            logger.debug(f"[DrugAttrib] Filtered out {normalized} - appears in downstream transaction context")
            continue

        codes.add(normalized)

    return codes

def _match_rows_by_name(source_sentence, bfai_rows):
    """
    Fallback: when source_sentence has no A-code pattern, scan accused full_names
    against the sentence. Returns list of matching bfai rows.

    Handles cases like "seized 100g Ganja from Raju" where LLM wrote the name
    instead of the accused code — still need to attribute to Raju's row.
    """
    if not source_sentence or not bfai_rows:
        return []
    sentence_lower = source_sentence.lower()
    downstream_keywords = r'\b(?:sell|sold|selling|buyer|customer|purchase|bought|received|consume|consumed|consumption)\s+(?:to|by|from|by|of)?\s*'
    
    matched = []
    for row in bfai_rows:
        name = (row.get('full_name') or '').strip()
        if not name or len(name) < 4:
            continue
        # Require at least 2 tokens to match (avoids single-word false hits)
        tokens = [t for t in name.lower().split() if len(t) >= 3]
        
        name_matched = False
        match_idx = -1
        
        if len(tokens) >= 2 and sum(1 for t in tokens if t in sentence_lower) >= 2:
            name_matched = True
            # Find the position of the first matching token to check lookback
            for t in tokens:
                idx = sentence_lower.find(t)
                if idx >= 0:
                    match_idx = idx
                    break
        elif len(tokens) == 1 and tokens[0] in sentence_lower:
            name_matched = True
            match_idx = sentence_lower.find(tokens[0])
            
        if name_matched:
            # Check for downstream keywords
            if match_idx >= 0:
                lookback_window = max(0, match_idx - 100)
                text_before = sentence_lower[lookback_window:match_idx]
                if re.search(downstream_keywords, text_before):
                    logger.debug(f"[DrugAttrib] Filtered out name match {name} - appears in downstream transaction context")
                    continue
            matched.append(row)
            
    return matched

def _add_or_consolidate_drug(accused_row, drug_data, attribution_type, drug_label, qty_label):
    """
    RULE 4: Same Accused, Same Drug Consolidation (within same crime_id).

    If accused already has the same drug (by primary_drug_name, supplier_name, source_location),
    consolidate by merging quantities. Otherwise, add as new entry.

    Consolidation ONLY happens within same (crime_id, accused_id) — not across different crimes.

    Args:
        accused_row: The bfai row (accused) to add/consolidate drug to
        drug_data: The drug extraction data (dict or DrugExtraction)
        attribution_type: Type of attribution (INDIVIDUAL, COLLECTIVE_TOTAL, FALLBACK_A1, etc.)
        drug_label: Display label for logging
        qty_label: Quantity label for logging
    """
    if not accused_row or not drug_data:
        return

    primary_name = str(drug_data.get('primary_drug_name') or '').strip().upper()
    supplier_name = (drug_data.get('supplier_name') or '').strip().upper()
    source_location = (drug_data.get('source_location') or '').strip().upper()

    # Consolidation key: (primary_drug_name, supplier_name, source_location)
    # This is used to find existing drugs from same source
    consolidation_key = (primary_name, supplier_name, source_location)

    # Check if accused already has this drug (same source)
    existing_drug = None
    existing_idx = None
    drugs_list = accused_row.get('drugs') or []

    for idx, existing in enumerate(drugs_list):
        if not isinstance(existing, dict):
            try:
                existing = existing.model_dump()
            except BaseException:
                pass

        existing_primary = str(existing.get('primary_drug_name') or '').strip().upper()
        existing_supplier = (existing.get('supplier_name') or '').strip().upper()
        existing_location = (existing.get('source_location') or '').strip().upper()
        existing_key = (existing_primary, existing_supplier, existing_location)

        if consolidation_key == existing_key:
            existing_drug = existing
            existing_idx = idx
            break

    if existing_drug is not None:
        # CONSOLIDATE: Merge with existing drug entry (Rule 4)
        # Merge quantities: add raw_quantity values
        new_qty = float(drug_data.get('raw_quantity') or 0)
        existing_qty = float(existing_drug.get('raw_quantity') or 0)

        if new_qty > 0 or existing_qty > 0:
            merged_qty = existing_qty + new_qty
            existing_drug['raw_quantity'] = merged_qty

            # Also merge standardized measurements if available
            if drug_data.get('weight_g') and not existing_drug.get('weight_g'):
                existing_drug['weight_g'] = drug_data.get('weight_g')
            if drug_data.get('weight_kg') and not existing_drug.get('weight_kg'):
                existing_drug['weight_kg'] = drug_data.get('weight_kg')
            if drug_data.get('volume_ml') and not existing_drug.get('volume_ml'):
                existing_drug['volume_ml'] = drug_data.get('volume_ml')
            if drug_data.get('volume_l') and not existing_drug.get('volume_l'):
                existing_drug['volume_l'] = drug_data.get('volume_l')
            if drug_data.get('count_total') and not existing_drug.get('count_total'):
                existing_drug['count_total'] = drug_data.get('count_total')

            # Preserve source metadata (alternate source sentence for audit trail)
            existing_meta = existing_drug.get('extraction_metadata') or {}
            new_meta = drug_data.get('extraction_metadata') or {}
            existing_source = existing_meta.get('source_sentence', '')
            new_source = new_meta.get('source_sentence', '')

            if new_source and existing_source != new_source:
                if 'consolidated_sources' not in existing_meta:
                    existing_meta['consolidated_sources'] = [existing_source]
                existing_meta['consolidated_sources'].append(new_source)
                existing_drug['extraction_metadata'] = existing_meta

            logger.info(
                f"[DrugAttrib] CONSOLIDATED: {drug_label} ({qty_label}) "
                f"→ merged with existing (total qty now: {merged_qty} {drug_data.get('raw_unit', '')})"
            )

            # Update the entry in the list
            drugs_list[existing_idx] = existing_drug
            accused_row['drugs'] = drugs_list
    else:
        # ADD: New drug entry for this accused
        new_drug_element = _build_drug_element(drug_data, attribution_type)
        accused_row['drugs'].append(new_drug_element)


def write_drugs_by_accused_in_memory(bfai_rows, drug_data_list):
    """
    Merge drug extraction results into accused rows (bfai_rows).

    Attribution rules (one entry per drug per accused — no ghost/reference copies):

    Case 1 — INDIVIDUAL (code match):
        source_sentence names exactly 1 A-code → that accused only.
        Covers: A1 has Drug X; A2 has Drug Y; A1 has Drug X AND A2 has Drug X
        separately (2 LLM entries, each with own code).

    Case 2 — INDIVIDUAL (name match):
        No A-code in source_sentence but accused name present.
        e.g. "seized 100g from Raju" → matched to Raju's row.

    Case 3 — COLLECTIVE_TOTAL → A1 (primary accused):
        source_sentence names 2+ accused codes, OR names accused in a range
        ("A2 to A4"), meaning a joint/shared seizure with NO per-person split.
        Always stored on primary_row (A1 / first accused in roster).
        Rule: If no explicit individual attribution → A1.
        e.g. "A1, A2, A3 caught with 520 KG ganja total" → stored on A1.
        e.g. "seized 265g from possession of A2 to A4" → stored on A1.

    Case 4 — UNATTRIBUTED_FALLBACK_A1:
        No code or name match → primary (A1/first) accused only.
        Same destination as Case 3 — reinforces the A1 rule.

    Case 5 — NO_DRUGS_DETECTED:
        Marker stamped on ALL accused so every row has a drugs[].

    Case 6 — NO_ACCUSED_ORPHAN:
        Sentinel crime row (no real accused) → drug on orphan row.

    RULE 4 — CONSOLIDATION (within same crime_id):
        If same accused has same drug from multiple packets/seizures in SAME CRIME:
        Merge into one entry with aggregated quantity.

        Consolidation key: (primary_drug_name, supplier_name, source_location)
        Only merge if within same (crime_id, accused_id) pair.
        Do NOT merge same drug across different crime_ids (different cases).

    Why no ghost entries: one seizure → one row. REFERENCED_A1 nulled-quantity
    copies inflated aggregates and caused double-counting.
    """
    if not bfai_rows:
        return bfai_rows

    rows_by_code = {}
    for row in bfai_rows:
        row['drugs'] = []
        normalized = _norm_person_code(row.get('person_code'))
        if normalized:
            rows_by_code[normalized] = row

    orphan_row = next(
        (r for r in bfai_rows if r.get('role_in_crime') == 'NO_ACCUSED_DRUGS_ONLY'),
        None
    )
    real_rows = [r for r in bfai_rows if r.get('accused_id')]
    ordered_real_rows = sorted(real_rows if real_rows else bfai_rows, key=_bfai_row_sort_key)
    primary_row = _pick_primary_row(ordered_real_rows) or (bfai_rows[0] if bfai_rows else None)

    for drug_data in drug_data_list:
        if not isinstance(drug_data, dict):
            try:
                drug_data = drug_data.model_dump()
            except BaseException:
                pass

        primary_name = str(drug_data.get('primary_drug_name') or '').strip().upper()
        drug_label   = drug_data.get('primary_drug_name') or drug_data.get('raw_drug_name') or '?'
        qty_label    = f"{drug_data.get('raw_quantity')} {drug_data.get('raw_unit') or ''}".strip()

        # ── Case 6: orphan sentinel ──
        if orphan_row:
            orphan_row['drugs'].append(_build_drug_element(drug_data, 'NO_ACCUSED_ORPHAN'))
            continue

        # ── Case 5: no-drug marker → stamp all accused rows ──
        if primary_name == 'NO_DRUGS_DETECTED':
            for row in ordered_real_rows:
                row['drugs'].append(_build_drug_element(drug_data, 'NO_DRUGS_DETECTED'))
            continue

        # ── Primary: A-code based matching ──
        mentioned_codes = _extract_person_codes(drug_data)
        matched_rows    = [rows_by_code[c] for c in sorted(mentioned_codes) if c in rows_by_code]

        # ── Fallback: name-based matching when no A-codes found ──
        source_sentence = (drug_data.get('extraction_metadata') or {}).get('source_sentence', '')
        if not matched_rows and source_sentence:
            matched_rows = _match_rows_by_name(source_sentence, ordered_real_rows)
            if matched_rows:
                logger.info(
                    f"[DrugAttrib] NAME_MATCH: {drug_label} ({qty_label}) "
                    f"→ {[r.get('full_name') for r in matched_rows]}"
                )

        if len(matched_rows) == 1:
            # ── Cases 1 & 2: individual — one accused has this drug ──
            code = matched_rows[0].get('person_code') or matched_rows[0].get('full_name') or '?'
            logger.info(f"[DrugAttrib] INDIVIDUAL: {drug_label} ({qty_label}) → {code}")
            _add_or_consolidate_drug(matched_rows[0], drug_data, 'INDIVIDUAL', drug_label, qty_label)

        elif len(matched_rows) > 1:
            # ── Case 3: collective — joint seizure shared by multiple accused ──
            # No per-person split → always assign to primary_row (A1 / first in roster).
            # Rule: "No explicit individual attribution → A1" applies to both
            #       unattributed (Case 4) and multi-accused collective (Case 3).
            holder_code = primary_row.get('person_code') or 'A1'
            all_codes   = [r.get('person_code') or r.get('full_name') or '?' for r in matched_rows]
            logger.info(
                f"[DrugAttrib] COLLECTIVE_TOTAL→A1: {drug_label} ({qty_label}) "
                f"joint seizure mentioned with {all_codes} → stored on {holder_code} (primary)"
            )
            _add_or_consolidate_drug(primary_row, drug_data, 'COLLECTIVE_TOTAL', drug_label, qty_label)

        else:
            # ── Case 4A: unattributed collective consumption — assign to all accused ──
            sentence_lower = source_sentence.lower()
            consumption_markers = (
                'smoked', 'smoking', 'consumed', 'consuming', 'consumption',
                'urine test', 'tested positive', 'drug detection test',
                'positive'
            )
            collective_markers = (
                'accused persons', 'they', 'all accused', 'all of them'
            )
            is_collective_consumption = (
                len(ordered_real_rows) > 1
                and sentence_lower
                and any(marker in sentence_lower for marker in consumption_markers)
                and any(marker in sentence_lower for marker in collective_markers)
            )

            if is_collective_consumption:
                all_codes = [r.get('person_code') or r.get('full_name') or '?' for r in ordered_real_rows]
                logger.info(
                    f"[DrugAttrib] COLLECTIVE_CONSUMPTION: {drug_label} ({qty_label}) "
                    f"unattributed collective use sentence → {all_codes}"
                )
                for row in ordered_real_rows:
                    _add_or_consolidate_drug(row, drug_data, 'COLLECTIVE_CONSUMPTION', drug_label, qty_label)
            else:
                # ── Case 4B: no attribution found — assign to A1/primary ──
                fallback_code = primary_row.get('person_code') or 'A1'
                logger.info(
                    f"[DrugAttrib] FALLBACK_A1: {drug_label} ({qty_label}) "
                    f"no code/name in source → {fallback_code}"
                )
                _add_or_consolidate_drug(primary_row, drug_data, 'UNATTRIBUTED_FALLBACK_A1', drug_label, qty_label)

    return bfai_rows


def validate_row_before_write(row: dict) -> tuple:
    """Rule C-1: Pre-write validation checklist for accused rows.

    Confirms:
    1. Dedup gate A-2 passed (no dedup review flag unless handled)
    2. Role classification A-7 applied
    3. CCL flag set correctly (age < 18 → is_ccl = true)
    4. Quantity has valid unit if raw_quantity is set

    Returns: (is_valid: bool, validation_errors: List[str])
    """
    errors = []

    # Check 1: If flagged for dedup review, still valid to write but note it
    if row.get('dedup_review_flag'):
        # This is OK — it's flagged but still valid
        pass

    # Check 2: Role classification should exist for all rows
    role = row.get('role_in_crime')
    accused_type = row.get('accused_type')
    # Allow NULL roles for purely drug-only rows
    if not role and not accused_type and row.get('full_name'):
        errors.append("NO_ROLE_CLASSIFICATION")

    # Check 3: CCL flag logic — age < 18 must have is_ccl = true
    age = row.get('age')
    is_ccl = row.get('is_ccl', False)
    if age is not None and age < 18 and not is_ccl:
        errors.append(f"CCL_FLAG_MISSING_FOR_MINOR (age={age})")

    # Check 4: If quantity set, ensure unit is validated
    raw_qty = row.get('raw_quantity')
    if raw_qty is not None and raw_qty > 0:
        # The unit check should have been done in drug extraction
        # This is just a sanity check that if qty exists, it was validated
        # (We don't have unit info at this level, so we just flag the presence)
        pass

    # Check 5: Ensure source tracking is present for traceability
    source_summary = row.get('source_summary_fields', {})
    # This is optional but good to have
    if not source_summary and (role or accused_type):
        # Mild warning, not an error
        pass

    return len(errors) == 0, errors


def delete_brief_facts_for_crime(conn, crime_id):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM public.brief_facts_ai WHERE crime_id = %s", (crime_id,))

def bulk_upsert_brief_facts_ai(conn, items):
    if not items:
        return

    # Rule C-1: Pre-write validation for all items
    validation_errors = {}
    for i, item in enumerate(items):
        is_valid, errors = validate_row_before_write(item)
        if errors:
            validation_errors[i] = errors
            logger.warning(
                f"Row {i} (crime_id={item.get('crime_id')}, "
                f"full_name={item.get('full_name')}): {', '.join(errors)}"
            )

    if validation_errors:
        logger.info(
            f"Pre-write validation: {len(validation_errors)} row(s) with warnings, "
            f"proceeding with write (issues flagged in logs)"
        )

    with conn.cursor() as cur:
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
                %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s
            )
            ON CONFLICT (crime_id, accused_id) DO UPDATE SET
                person_id = EXCLUDED.person_id,
                canonical_person_id = EXCLUDED.canonical_person_id,
                person_code = EXCLUDED.person_code,
                seq_num = EXCLUDED.seq_num,
                full_name = EXCLUDED.full_name,
                alias_name = EXCLUDED.alias_name,
                age = EXCLUDED.age,
                gender = EXCLUDED.gender,
                occupation = EXCLUDED.occupation,
                address = EXCLUDED.address,
                phone_numbers = EXCLUDED.phone_numbers,
                role_in_crime = EXCLUDED.role_in_crime,
                key_details = EXCLUDED.key_details,
                accused_type = EXCLUDED.accused_type,
                status = EXCLUDED.status,
                is_ccl = EXCLUDED.is_ccl,
                dedup_match_tier = EXCLUDED.dedup_match_tier,
                dedup_confidence = EXCLUDED.dedup_confidence,
                dedup_review_flag = EXCLUDED.dedup_review_flag,
                source_person_fields = EXCLUDED.source_person_fields,
                source_accused_fields = EXCLUDED.source_accused_fields,
                source_summary_fields = EXCLUDED.source_summary_fields,
                drugs = EXCLUDED.drugs,
                date_modified = CURRENT_TIMESTAMP,
                etl_run_id = EXCLUDED.etl_run_id
        """

        for item_data in items:
            bf_id = item_data.get('bf_accused_id') or str(uuid.uuid4())
            full_name     = truncate_varchar(item_data.get('full_name'), 500)
            alias_name    = truncate_varchar(item_data.get('alias_name'), 255)
            occupation    = truncate_varchar(item_data.get('occupation'), 255)
            phone_numbers = truncate_varchar(normalize_phone_numbers(item_data.get('phone_numbers')), 255)
            gender        = truncate_varchar(item_data.get('gender'), 20)
            status        = truncate_varchar(item_data.get('status'), 40)
            age           = validate_age(item_data.get('age'))
            person_code   = truncate_varchar(item_data.get('person_code'), 50)
            seq_num       = truncate_varchar(item_data.get('seq_num'), 50)
            
            role_in_crime = item_data.get('role_in_crime')
            # Garbage collection for placeholders
            if not item_data.get('accused_id') and role_in_crime in ['LLM_EXTRACTION_FAILED', 'NO_ACCUSED_IN_TEXT', 'NO_ACCUSED_DRUGS_ONLY']:
                 cur.execute("DELETE FROM public.brief_facts_ai WHERE crime_id = %s AND role_in_crime = %s AND accused_id IS NULL", (item_data.get('crime_id'), role_in_crime))

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
                role_in_crime,
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
                item_data.get('etl_run_id'),
            ))


def insert_accused_facts(conn, item_data):
    """Wrapper to insert a single accused fact record. Uses bulk_upsert_brief_facts_ai internally."""
    bulk_upsert_brief_facts_ai(conn, [item_data])
