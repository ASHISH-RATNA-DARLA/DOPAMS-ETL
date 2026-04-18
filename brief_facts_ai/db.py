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
        cur.execute("""
            SELECT bfa.bf_accused_id, bfa.canonical_person_id, bfa.accused_id, bfa.person_code, bfa.full_name,
                   bfa.alias_name, bfa.age, bfa.gender, bfa.address, bfa.source_accused_fields, bfa.crime_id,
                   c.major_head, c.minor_head, c.crime_type, c.acts_sections
            FROM public.brief_facts_ai bfa
            LEFT JOIN public.crimes c ON c.crime_id = bfa.crime_id
            WHERE bfa.crime_id != %s AND bfa.full_name IS NOT NULL
              AND (SOUNDEX(bfa.full_name) = SOUNDEX(%s)
                   OR dmetaphone(COALESCE(bfa.full_name, '')) = dmetaphone(%s)
                   OR bfa.source_accused_fields->>'ps_code' = %s)
            ORDER BY
              CASE
                WHEN SOUNDEX(bfa.full_name) = SOUNDEX(%s) THEN 0
                WHEN dmetaphone(COALESCE(bfa.full_name, '')) = dmetaphone(%s) THEN 1
                ELSE 2
              END
            LIMIT %s
        """, (current_crime_id, full_name or '', full_name or '', ps_code,
              full_name or '', full_name or '', limit))
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
            FROM accused a LEFT JOIN persons p ON a.person_id = p.person_id WHERE a.crime_id = %s
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

def _extract_person_codes(drug_data):
    if not isinstance(drug_data, dict):
        try: drug_data = drug_data.model_dump()
        except BaseException: pass
    codes = set()
    meta = drug_data.get('extraction_metadata') or {}
    source_sentence = str(meta.get('source_sentence') or '')
    for raw in re.findall(r'\bA\s*[-.]?\s*\d+\b', source_sentence, flags=re.IGNORECASE):
        normalized = _norm_person_code(raw)
        if normalized: codes.add(normalized)
    accused_ref = meta.get('accused_ref')
    if accused_ref:
        normalized = _norm_person_code(accused_ref)
        if normalized: codes.add(normalized)
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
    matched = []
    for row in bfai_rows:
        name = (row.get('full_name') or '').strip()
        if not name or len(name) < 4:
            continue
        # Require at least 2 tokens to match (avoids single-word false hits)
        tokens = [t for t in name.lower().split() if len(t) >= 3]
        if len(tokens) >= 2 and sum(1 for t in tokens if t in sentence_lower) >= 2:
            matched.append(row)
        elif len(tokens) == 1 and tokens[0] in sentence_lower:
            matched.append(row)
    return matched


def write_drugs_by_accused_in_memory(bfai_rows, drug_data_list):
    """
    Merge drug extraction results into accused rows (bfai_rows).

    Attribution rules (one entry per drug — no ghost/reference copies):

    Case 1 — INDIVIDUAL (code match):
        source_sentence names exactly 1 A-code → that accused only.
        Covers: A1 has Drug X; A2 has Drug Y; A1 has Drug X AND A2 has Drug X
        separately (2 LLM entries, each with own code).

    Case 2 — INDIVIDUAL (name match):
        No A-code in source_sentence but accused name present.
        e.g. "seized 100g from Raju" → matched to Raju's row.

    Case 3 — COLLECTIVE_TOTAL:
        source_sentence names 2+ accused → first mentioned only, no duplication.
        e.g. "A1, A2, A3 caught with 520 KG ganja total" → stored on A1.

    Case 4 — UNATTRIBUTED_FALLBACK_A1:
        No code or name match → primary (A1/first) accused only.

    Case 5 — NO_DRUGS_DETECTED:
        Marker stamped on ALL accused so every row has a drugs[].

    Case 6 — NO_ACCUSED_ORPHAN:
        Sentinel crime row (no real accused) → drug on orphan row.

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
    ordered_real_rows = real_rows if real_rows else bfai_rows
    primary_row = ordered_real_rows[0] if ordered_real_rows else bfai_rows[0]

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
            matched_rows[0]['drugs'].append(_build_drug_element(drug_data, 'INDIVIDUAL'))

        elif len(matched_rows) > 1:
            # ── Case 3: collective — same drug/seizure shared by multiple accused ──
            # One entry on the first accused; no ghost copies on the rest.
            holder_code = matched_rows[0].get('person_code') or '?'
            all_codes   = [r.get('person_code') or r.get('full_name') or '?' for r in matched_rows]
            logger.info(
                f"[DrugAttrib] COLLECTIVE_TOTAL: {drug_label} ({qty_label}) "
                f"mentioned with {all_codes} → stored on {holder_code} only"
            )
            matched_rows[0]['drugs'].append(_build_drug_element(drug_data, 'COLLECTIVE_TOTAL'))

        else:
            # ── Case 4: no attribution found — assign to A1/primary ──
            fallback_code = primary_row.get('person_code') or 'A1'
            logger.info(
                f"[DrugAttrib] FALLBACK_A1: {drug_label} ({qty_label}) "
                f"no code/name in source → {fallback_code}"
            )
            primary_row['drugs'].append(_build_drug_element(drug_data, 'UNATTRIBUTED_FALLBACK_A1'))

    return bfai_rows

def delete_brief_facts_for_crime(conn, crime_id):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM public.brief_facts_ai WHERE crime_id = %s", (crime_id,))

def bulk_upsert_brief_facts_ai(conn, items):
    if not items:
        return
    
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
