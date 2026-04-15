import sys
import re
import logging
import threading
import uuid
import os
import unicodedata
from difflib import SequenceMatcher
import config

try:
    from unidecode import unidecode as _unidecode
except Exception:  # pragma: no cover - optional dependency fallback
    _unidecode = None

from db import (
    get_db_connection,
    return_db_connection,
    fetch_crimes_by_ids,
    
    fetch_unprocessed_crimes,
    fetch_existing_accused_for_crime,
    start_crime_processing_run,
    complete_crime_processing_run,
    fail_crime_processing_run,
    normalize_accused_status,
    resolve_status_for_insert,
    strip_alias_name,
    compute_age_from_dob,
    fetch_dedup_candidates,
    fetch_crime_profile,
    fetch_crime_associate_person_codes,
    delete_brief_facts_for_crime,
    update_sentinel_role, bulk_upsert_brief_facts_ai, write_drugs_by_accused_in_memory,
)
from extractor_accused import (
    extract_accused_info,
    extract_roles_for_known_accused,
    detect_gender,
    detect_ccl,
    classify_accused_type,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

UNIFIED_TABLE_NAME = "brief_facts_ai"

# Allow imports from sibling ETL modules (e.g., brief_facts_drugs)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


def _synthetic_accused_id(crime_id, full_name, seq_num):
    base = f"{crime_id}|{(full_name or '').strip().lower()}|{(seq_num or '').strip().lower()}"
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, base))


def _canonical_person_id(full_name, gender, ps_code):
    base = f"{(full_name or '').strip().lower()}|{(gender or '').strip().lower()}|{(ps_code or '').strip().lower()}"
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, base))


_RELATIONAL_PREFIX_RE = re.compile(r'\b(?:s/o|d/o|w/o|h/o)\b', re.IGNORECASE)
_COMMON_NAME_TOKENS = {
    'kumar', 'singh', 'rao', 'reddy', 'sharma', 'naidu', 'babu', 'raju'
}

_INDIC_TOKEN_MAP = {
    'A': 'a', 'AA': 'aa', 'I': 'i', 'II': 'ii', 'U': 'u', 'UU': 'uu',
    'E': 'e', 'EE': 'ee', 'AI': 'ai', 'O': 'o', 'OO': 'oo', 'AU': 'au',
    'KA': 'k', 'KHA': 'kh', 'GA': 'g', 'GHA': 'gh', 'NGA': 'ng',
    'CA': 'ch', 'CHA': 'chh', 'JA': 'j', 'JHA': 'jh', 'NYA': 'ny',
    'TTA': 't', 'TTHA': 'th', 'DDA': 'd', 'DDHA': 'dh', 'NNA': 'n',
    'TA': 't', 'THA': 'th', 'DA': 'd', 'DHA': 'dh', 'NA': 'n',
    'PA': 'p', 'PHA': 'ph', 'BA': 'b', 'BHA': 'bh', 'MA': 'm',
    'YA': 'y', 'RA': 'r', 'LA': 'l', 'VA': 'v',
    'SHA': 'sh', 'SSA': 'sh', 'SA': 's', 'HA': 'h',
    'LLA': 'l', 'RRA': 'r',
}


def _transliterate_indic_approx(value):
    if not value:
        return ''
    if _unidecode is not None:
        return _unidecode(str(value))
    out = []
    for ch in str(value):
        try:
            uname = unicodedata.name(ch)
        except ValueError:
            out.append(ch)
            continue

        if 'DEVANAGARI' not in uname and 'TELUGU' not in uname and 'KANNADA' not in uname:
            out.append(ch)
            continue

        token = None
        if 'LETTER ' in uname:
            token = uname.split('LETTER ', 1)[1]
        elif 'VOWEL SIGN ' in uname:
            token = uname.split('VOWEL SIGN ', 1)[1]
        elif 'SIGN VIRAMA' in uname:
            token = ''

        if token is None:
            out.append(' ')
            continue

        out.append(_INDIC_TOKEN_MAP.get(token, ''))

    translit = ''.join(out)
    return translit if translit.strip() else str(value)


def _normalize_name(value):
    if not value:
        return ''
    cleaned = _RELATIONAL_PREFIX_RE.sub(' ', str(value))
    cleaned = _transliterate_indic_approx(cleaned)
    cleaned = cleaned.split('@')[0]
    cleaned = re.sub(r'[^a-zA-Z0-9\s]', ' ', cleaned.lower())
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return cleaned


def _norm_person_code(value):
    if not value:
        return None
    m = re.search(r'A\s*[-.]?\s*(\d+)', str(value), flags=re.IGNORECASE)
    if not m:
        return None
    return f"A-{int(m.group(1))}"


def _token_set_similarity(a, b):
    ta = set(_normalize_name(a).split())
    tb = set(_normalize_name(b).split())
    if not ta or not tb:
        return 0.0
    inter = len(ta & tb)
    return (2.0 * inter) / (len(ta) + len(tb))


def _phonetic_overlap(a, b):
    na = _normalize_name(a)
    nb = _normalize_name(b)
    if not na or not nb:
        return 0.0
    prefix = 4
    return 1.0 if na[:prefix] == nb[:prefix] else 0.0


def _address_similarity(a, b):
    ta = set(re.findall(r'[a-z0-9]+', (a or '').lower()))
    tb = set(re.findall(r'[a-z0-9]+', (b or '').lower()))
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)


def _age_score(current_age, candidate_age):
    if current_age is None or candidate_age is None:
        return 0.5
    try:
        diff = abs(int(current_age) - int(candidate_age))
    except Exception:
        return 0.5
    if diff <= 2:
        return 0.8
    if diff >= 10:
        return 0.0
    return max(0.0, 0.8 - ((diff - 2) * (0.8 / 8.0)))


def _alias_score(current_alias, candidate_alias):
    if not current_alias or not candidate_alias:
        return 0.0
    return 1.0 if _normalize_name(current_alias) == _normalize_name(candidate_alias) else 0.0


def _crime_tokens(value):
    return set(re.findall(r'[a-z0-9]+', (value or '').lower()))


def _dedup_score(current, candidate, ps_code, current_crime_profile, current_assoc_codes, candidate_assoc_codes):
    name_a = current.get('full_name')
    name_b = candidate.get('full_name')

    prefix_similarity = SequenceMatcher(None, _normalize_name(name_a), _normalize_name(name_b)).ratio()
    token_similarity = _token_set_similarity(name_a, name_b)
    phonetic_similarity = _phonetic_overlap(name_a, name_b)
    addr_similarity = _address_similarity(current.get('address'), candidate.get('address'))
    age_similarity = _age_score(current.get('age'), candidate.get('age'))
    alias_similarity = _alias_score(current.get('alias_name'), candidate.get('alias_name'))

    score = (
        0.35 * prefix_similarity +
        0.20 * token_similarity +
        0.15 * phonetic_similarity +
        0.12 * addr_similarity +
        0.10 * age_similarity +
        0.08 * alias_similarity
    )

    # Layer 4 contextual boosts
    cand_ps = (candidate.get('source_accused_fields') or {}).get('ps_code') if isinstance(candidate.get('source_accused_fields'), dict) else None
    if ps_code and cand_ps and str(ps_code) == str(cand_ps):
        score += 0.05

    current_tokens = set()
    candidate_tokens = set()
    for key in ('major_head', 'minor_head', 'crime_type', 'acts_sections'):
        current_tokens |= _crime_tokens((current_crime_profile or {}).get(key))
        candidate_tokens |= _crime_tokens(candidate.get(key))
    if current_tokens and candidate_tokens and (current_tokens & candidate_tokens):
        score += 0.04

    if current_assoc_codes and candidate_assoc_codes and (current_assoc_codes & candidate_assoc_codes):
        score += 0.06

    normalized = _normalize_name(name_a)
    if len(normalized.split()) == 1 and normalized in _COMMON_NAME_TOKENS:
        score *= 0.85

    return round(min(score, 1.0), 2)


def _resolve_canonical_identity(conn, current_crime_id, payload, ps_code):
    current_accused_id = payload.get('accused_id')
    current_person_code = payload.get('person_code')
    full_name = payload.get('full_name')
    gender = payload.get('gender')
    fallback_canonical = _canonical_person_id(full_name, gender, ps_code)
    current_crime_profile = fetch_crime_profile(conn, current_crime_id)
    assoc_cache = {current_crime_id: fetch_crime_associate_person_codes(conn, current_crime_id)}
    current_assoc_codes = assoc_cache[current_crime_id]

    # Layer 1: deterministic exact identity reuse by accused_id/person_code
    candidates = fetch_dedup_candidates(conn, current_crime_id, full_name, ps_code)
    for cand in candidates:
        if current_accused_id and cand.get('accused_id') and str(current_accused_id) == str(cand.get('accused_id')):
            return cand.get('canonical_person_id') or fallback_canonical, None, 1, False
        if current_person_code and cand.get('person_code') and str(current_person_code) == str(cand.get('person_code')):
            return cand.get('canonical_person_id') or fallback_canonical, None, 1, False

    # Layer 3-5: weighted match and thresholding
    best_cand = None
    best_score = -1.0
    for cand in candidates:
        cand_crime_id = cand.get('crime_id')
        if cand_crime_id not in assoc_cache:
            assoc_cache[cand_crime_id] = fetch_crime_associate_person_codes(conn, cand_crime_id)
        candidate_assoc_codes = assoc_cache.get(cand_crime_id, set())
        score = _dedup_score(
            payload,
            cand,
            ps_code,
            current_crime_profile,
            current_assoc_codes,
            candidate_assoc_codes,
        )
        if score > best_score:
            best_score = score
            best_cand = cand

    if best_cand and best_score >= 0.82 and best_cand.get('canonical_person_id'):
        return best_cand.get('canonical_person_id'), best_score, 1, False

    if best_score >= 0.60:
        return fallback_canonical, best_score, 2, True

    return fallback_canonical, (best_score if best_score >= 0 else 0.0), 3, False


# ---------------------------------------------------------------------------
# Branch Detector
# ---------------------------------------------------------------------------

def _classify_db_accused(db_accused):
    """
    Returns the processing branch for a given crime's accused rows.

      A — DB has accused rows AND at least one person_id IS NOT NULL
      B — DB has accused rows BUT ALL person_id IS NULL  (stub / orphan)
      C — DB has zero accused rows
    """
    if not db_accused:
        return 'C'
    if any(row.get('person_id') for row in db_accused):
        return 'A'
    return 'B'


# ---------------------------------------------------------------------------
# Role Pairing: accused_code + name + positional fallback
# ---------------------------------------------------------------------------

def _pair_role_to_accused(accused_code, full_name, roles_by_code):
    """
    Maps an LLM role entry to a DB accused row.

    Priority:
      1. Exact accused_code match (A-1 → A-1)
      2. Normalised code match   (A-1 → A1 → A.1)
      3. Name-based match        (LLM returned full_name as key instead of code)
      4. No match → {}
    """
    if not roles_by_code:
        return {}

    def _norm_code(s):
        return (s or '').replace(' ', '').replace('-', '').replace('.', '').upper()

    code_norm = _norm_code(accused_code)

    # 1. Exact code match
    if accused_code and accused_code in roles_by_code:
        return roles_by_code[accused_code]

    # 2. Normalised code match
    if code_norm:
        for k, v in roles_by_code.items():
            if _norm_code(k) == code_norm:
                return v

    # 3. Name-based match (LLM returned names as accused_code, e.g. 'Jog Singh')
    if full_name:
        name_lower = full_name.lower().strip()
        for k, v in roles_by_code.items():
            if k.lower().strip() == name_lower:
                return v
            # Partial: LLM name key contains DB name or vice versa
            k_lower = k.lower().strip()
            if len(k_lower) > 3 and len(name_lower) > 3:
                if k_lower in name_lower or name_lower in k_lower:
                    return v

    return {}


# ---------------------------------------------------------------------------
# Branch B: Accused-code pairing by text context
# ---------------------------------------------------------------------------

_ACCUSED_CODE_RE = re.compile(r'A[-.\s]?\d+', re.IGNORECASE)


def _pair_accused_id_from_db(extracted_clean_name, facts_text, db_accused_rows):
    """
    Branch B helper: match LLM-extracted clean name to a DB accused row
    by searching for the DB accused_code near the name in the original text.
    """
    if not extracted_clean_name or not db_accused_rows or not facts_text:
        return None, None, False, None

    text_lower = facts_text.lower()
    name_lower = extracted_clean_name.lower().strip()

    # Find name in text — try full name first, then longest token prefix
    idx = text_lower.find(name_lower)
    if idx < 0:
        tokens = name_lower.split()
        for length in range(len(tokens), 0, -1):
            partial = ' '.join(tokens[:length])
            idx = text_lower.find(partial)
            if idx >= 0:
                name_lower = partial
                break

    if idx < 0:
        return None, None, False, None

    window_start = max(0, idx - 30)
    window_end = min(len(facts_text), idx + len(name_lower) + 30)
    context_window = facts_text[window_start:window_end]

    found_codes = _ACCUSED_CODE_RE.findall(context_window)
    if not found_codes:
        return None, None, False, None

    def _norm(s):
        return (s or '').upper().replace(' ', '').replace('-', '').replace('.', '')

    for code_in_text in found_codes:
        code_norm = _norm(code_in_text)
        for row in db_accused_rows:
            db_code = _norm(row.get('accused_code') or '')
            if db_code and db_code == code_norm:
                return (
                    row.get('accused_id'),
                    row.get('accused_code'),
                    row.get('is_ccl', False),
                    row.get('accused_status'),
                )

    return None, None, False, None


# ---------------------------------------------------------------------------
# Status helper (DB-first, LOCAL text keyword fallback)
# ---------------------------------------------------------------------------

def _resolve_status(db_status_raw, text, name_hint):
    """
    Resolves final 'status' using DB value first, then keyword scan on
    LOCAL context window only (±120 chars around name).
    """
    db_status = normalize_accused_status(db_status_raw)
    if db_status:
        return db_status

    _absconding_kw = [
        "absconding", "evading", "fled", "on the run", "not traceable",
        "not found", "missing", "could not be traced", "yet to be arrested",
        "failed to appear", "escaped",
    ]
    _arrested_kw = [
        "arrested", "caught", "apprehended", "detained", "nabbed", "held",
        "taken into custody", "remanded", "produced before court",
        "surrendered", "confessed", "confession",
    ]

    text_lower = (text or "").lower()
    combined = ""
    candidate = (name_hint or "").lower()
    if candidate:
        idx = text_lower.find(candidate)
        if idx >= 0:
            start = max(0, idx - 120)
            end = min(len(text_lower), idx + len(candidate) + 120)
            combined = text_lower[start:end]

    if not combined:
        return None

    if any(k in combined for k in _absconding_kw):
        return "absconding"
    if any(k in combined for k in _arrested_kw):
        return "arrested"

    return None





def _fetch_current_bfai_rows(conn, crime_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT bf_accused_id, accused_id, person_code, full_name, role_in_crime
            FROM public.brief_facts_ai
            WHERE crime_id = %s
            ORDER BY
                CASE
                    WHEN seq_num ~ '^[0-9]+$' THEN seq_num::int
                    ELSE 2147483647
                END,
                bf_accused_id
            """,
            (crime_id,),
        )
        return cur.fetchall()


def _inject_accused_roster(facts_text, rows):
    roster_lines = []
    for row in rows:
        person_code = row[2] or 'UNKNOWN'
        full_name = row[3] or 'UNKNOWN'
        roster_lines.append(f"- {person_code}: {full_name}")
    if not roster_lines:
        return facts_text
    roster_block = "Known accused roster for attribution:\n" + "\n".join(roster_lines) + "\n\n"
    return roster_block + (facts_text or '')








# ---------------------------------------------------------------------------
# Main + batch loop
# ---------------------------------------------------------------------------

def main():
    logging.info("Starting Accused Extraction Service (Hybrid DB+LLM 3-Branch)...")

    try:
        conn = get_db_connection()
        logging.info("Database connection established.")
    except Exception as e:
        logging.error(f"Failed to connect to DB: {e}")
        sys.exit(1)

    try:
        input_file = "input.txt"
        crime_ids = []

        try:
            with open(input_file, "r") as f:
                crime_ids = [line.strip() for line in f if line.strip() and not line.startswith('#')]
        except FileNotFoundError:
            logging.info(f"{input_file} not found. Will fetch unprocessed crimes from DB.")

        if crime_ids:
            logging.info(f"Read {len(crime_ids)} IDs from {input_file}.")
            crimes = fetch_crimes_by_ids(conn, crime_ids)
            process_crimes_parallel(crimes)
        else:
            logging.info("No input IDs provided. Starting Dynamic Batch Processing...")
            # Scale batch size for parallel processing on 64GB server
            batch_size = int(os.environ.get('BATCH_SIZE', '30'))
            total_processed = 0
            while True:
                crimes = fetch_unprocessed_crimes(conn, limit=batch_size)
                if not crimes:
                    logging.info("No more unprocessed crimes found. Exiting.")
                    break
                logging.info(f"Fetched batch of {len(crimes)} unprocessed crimes.")
                process_crimes_parallel(crimes)
                total_processed += len(crimes)
                logging.info(f"Batch complete. Total processed so far: {total_processed}")

    except KeyboardInterrupt:
        logging.info("Process interrupted by user.")
    except Exception as e:
        logging.error(f"Unexpected error in main loop: {e}", exc_info=True)
    finally:
        return_db_connection(conn)
        logging.info("Database connection closed.")


# ---------------------------------------------------------------------------
# Per-crime dispatcher — commits per crime (safe for production)
# ---------------------------------------------------------------------------

from concurrent.futures import ThreadPoolExecutor, as_completed
import os

def process_crimes_parallel(crimes):
    """Processes a list of crimes in parallel using thread pool and connection pool."""
    # Scale LLM workers for 64GB server
    max_workers = int(os.environ.get('PARALLEL_LLM_WORKERS', '6'))
    logging.info(f"🚀 Scaling accused extraction with {max_workers} parallel workers")

    def worker(crime):
        crime_id = crime['crime_id']
        ps_code = crime.get('ps_code')
        facts_text = (crime['brief_facts'] or "").strip()
        
        # Use connection context manager to ensure proper return to pool
        from db_pooling import PostgreSQLConnectionPool
        pool = PostgreSQLConnectionPool()
        
        with pool.get_connection_context() as conn:
            run_id = None
            rows_written = 0
            unified_mode = (config.ACCUSED_TABLE_NAME or "").lower() == UNIFIED_TABLE_NAME
            try:
                db_accused = fetch_existing_accused_for_crime(conn, crime_id)
                branch = _classify_db_accused(db_accused)

                if unified_mode:
                    # Record branch in the log so Branch C entries can be
                    # invalidated later when accused records arrive.
                    run_id = start_crime_processing_run(conn, crime_id, branch=branch)
                    delete_brief_facts_for_crime(conn, crime_id)

                
                if branch == 'A':
                    rows_written, branch_records = _process_branch_a(conn, crime_id, ps_code, facts_text, db_accused, run_id)
                elif branch == 'B':
                    rows_written, branch_records = _process_branch_b(conn, crime_id, ps_code, facts_text, db_accused, run_id)
                else:
                    rows_written, branch_records = _process_branch_c(conn, crime_id, ps_code, facts_text, run_id)

                if unified_mode:
                    # _run_unified_drug_enrichment(conn, crime_id, facts_text) -> replaced
                    from extractor_drugs import extract_drug_info
                    import db as db_module
                    
                    db_module._load_drug_context = lambda c: None # mock to prevent error, we can just load directly
                    
                    drug_categories = db_module.fetch_drug_categories(conn)
                    ignore_dict = db_module.fetch_drug_ignore_list(conn)
                    ignore_set = set(ignore_dict.keys())
                    kb_lookup = {row['raw_name'].lower().strip(): row['standard_name'] for row in drug_categories}
                    from extractor_drugs import build_drug_keywords
                    dynamic_keywords = build_drug_keywords(drug_categories)
                    
                    augmented_text = _inject_accused_roster(facts_text, [tuple([None, None, r.get('person_code'), r.get('full_name')]) for r in branch_records if r.get('person_code')])
                    
                    extractions = extract_drug_info(augmented_text, drug_categories, ignore_set=ignore_set, kb_lookup=kb_lookup, dynamic_drug_keywords=dynamic_keywords, conn=conn)
                    
                    if extractions:
                        pass
                    else:
                        if branch_records:
                            placeholder = {'raw_drug_name': 'NO_DRUGS_DETECTED'}
                            extractions = [placeholder]
                        else:
                            update_sentinel_role(conn, crime_id, 'NO_ACCUSED_IN_TEXT', 'NO_ACCUSED_DRUGS_ONLY')
                            update_sentinel_role(conn, crime_id, 'LLM_EXTRACTION_FAILED', 'NO_ACCUSED_DRUGS_ONLY')
                            extractions = []
                    
                    enriched_rows = db_module.write_drugs_by_accused_in_memory(branch_records, extractions)
                    db_module.bulk_upsert_brief_facts_ai(conn, enriched_rows)

                if unified_mode and run_id:
                    complete_crime_processing_run(conn, run_id, rows_written)


                conn.commit()
                return True, crime_id, branch
            except Exception as e:
                try:
                    if unified_mode and run_id:
                        fail_crime_processing_run(conn, run_id, str(e))
                        conn.commit()
                except Exception:
                    conn.rollback()
                conn.rollback()
                logging.error(f"Failed processing Crime {crime_id}: {e}")
                return False, crime_id, None
            # Connection automatically returned to pool via context manager

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_crime = {executor.submit(worker, crime): crime['crime_id'] for crime in crimes}
        for future in as_completed(future_to_crime):
            success, cid, branch = future.result()
            if success:
                logging.info(f"✅ Crime {cid} processed successfully (branch={branch}).")
            else:
                logging.error(f"❌ Crime {cid} processing failed (branch={branch}).")


# ---------------------------------------------------------------------------
# Branch A — DB has accused rows, at least one person_id IS NOT NULL
# ---------------------------------------------------------------------------

def _process_branch_a(conn, crime_id, ps_code, facts_text, db_accused, run_id):
    """
    DB is authoritative for identity. LLM extracts roles + fills missing fields.
    Skips accused rows where person_id IS NULL (spec SKIP RULE).

    person_code logic by accused.type:
      - 'Accused' / 'CCL': person_code = accused_code (direct from DB)
      - 'Known' / 'Respondent' / 'Suspect': LLM assigns person_code (A1, A2 by mention)
    """
    # Process all DB accused rows; no silent dropping.
    valid_accused = list(db_accused)
    if not valid_accused:
        logging.info(f"Branch A: No accused rows found for Crime {crime_id}")
        return 0, []

    # ---- Pre-scan: determine person_code strategy and missing fields per accused ----
    DIRECT_CODE_TYPES = {'Accused', 'CCL'}
    LLM_CODE_TYPES = {'Known', 'Respondent', 'Suspect'}

    missing_fields_map = {}   # accused_code -> [list of missing field names]
    needs_person_code = []    # accused_codes that need LLM assignment
    PERSON_FIELDS_TO_CHECK = ['age', 'address', 'alias_name', 'occupation']

    for i, row in enumerate(valid_accused, start=1):
        code = row.get('accused_code') or f'A-{i}'
        accused_type_db = (row.get('accused_type_db') or 'Accused').strip()

        # Check if this type needs LLM-assigned person_code
        if accused_type_db in LLM_CODE_TYPES:
            needs_person_code.append(code)

        # Detect which person fields are NULL and need LLM fallback
        missing = []
        if not row.get('age') and not row.get('date_of_birth'):
            missing.append('age')
        if not row.get('address'):
            missing.append('address')
        if not row.get('alias_name'):
            missing.append('alias_name')
        if not row.get('occupation'):
            missing.append('occupation')
        if missing:
            missing_fields_map[code] = missing

    # ---- Call LLM with annotations ----
    roles_by_code = extract_roles_for_known_accused(
        facts_text, list(valid_accused),
        missing_fields_map=missing_fields_map,
        needs_person_code=needs_person_code,
    )
    if not roles_by_code:
        roles_by_code = {}

    # Positional fallback: if only 1 accused and 1 role, pair directly
    single_role = None
    if len(valid_accused) == 1 and len(roles_by_code) == 1:
        single_role = list(roles_by_code.values())[0]

    branch_records = []
    count = 0
    for i, row in enumerate(valid_accused, start=1):
        accused_id    = row.get('accused_id')
        person_id     = row.get('person_id')
        accused_code  = row.get('accused_code') or ''
        seq_num       = row.get('seq_num')
        is_ccl_db     = row.get('is_ccl', False)
        accused_type_db = (row.get('accused_type_db') or 'Accused').strip()

        # ---- person_code by accused.type ----
        if accused_type_db in DIRECT_CODE_TYPES:
            person_code = accused_code or None
        else:
            # Known/Respondent/Suspect: try LLM-assigned code
            person_code = None  # will be filled from LLM below

        # ---- DB person fields ----
        full_name     = row.get('full_name')
        if not accused_id and full_name:
            accused_id = _synthetic_accused_id(crime_id, full_name, seq_num)
        raw_alias     = row.get('alias_name')
        alias_name    = strip_alias_name(raw_alias)
        age           = row.get('age')
        if age is None:
            age = compute_age_from_dob(row.get('date_of_birth'))
        gender        = row.get('gender')
        occupation    = row.get('occupation')
        address       = row.get('address')
        phone_numbers = row.get('phone_numbers')

        # ---- Role pairing: code → normalised code → name → positional ----
        effective_code = accused_code or f'A-{i}'
        role_data = _pair_role_to_accused(effective_code, full_name, roles_by_code)
        if not role_data and single_role:
            role_data = single_role

        role_in_crime = role_data.get('role_in_crime') if role_data else None
        key_details   = role_data.get('key_details') if role_data else None

        # ---- LLM fallback: fill missing person fields ----
        source_person = {k: 'DB' for k in ['full_name', 'alias_name', 'age', 'gender', 'occupation', 'phone_numbers', 'address']
                         if row.get(k) is not None and row.get(k) != ''}

        if role_data:
            # Fill NULL DB fields with LLM-extracted values
            if not address and role_data.get('address'):
                address = role_data['address']
                source_person['address'] = 'LLM_FALLBACK'
            if age is None and role_data.get('age') is not None:
                age = role_data['age']
                source_person['age'] = 'LLM_FALLBACK'
            if not alias_name and role_data.get('alias_name'):
                alias_name = role_data['alias_name']
                source_person['alias_name'] = 'LLM_FALLBACK'
            if not occupation and role_data.get('occupation'):
                occupation = role_data['occupation']
                source_person['occupation'] = 'LLM_FALLBACK'
            # LLM-assigned person_code for Known/Respondent/Suspect
            if person_code is None and role_data.get('person_code_assigned'):
                person_code = role_data['person_code_assigned']

        # ---- Status: raw DB value first, keyword fallback ----
        status = resolve_status_for_insert(row.get('accused_status'), facts_text, full_name or accused_code)

        # ---- Classification ----
        if role_in_crime:
            classification_text = role_in_crime + (" " + key_details if key_details else "")
            accused_type = classify_accused_type(classification_text)
        else:
            accused_type = None

        # Gender fallback
        if not gender:
            gender = detect_gender(facts_text, full_name or accused_code)

        # CCL
        is_ccl = bool(is_ccl_db) or detect_ccl(full_name or '', role_in_crime or '')

        if accused_type == 'unknown':
            accused_type = None

        # ---- Source audit trail ----
        source_accused = {k: 'DB' for k, v in [
            ('accused_id', accused_id), ('person_code', accused_code),
            ('seq_num', seq_num), ('is_ccl', is_ccl_db),
            ('status', row.get('accused_status')),
            ('accused_type_db', accused_type_db),
        ] if v is not None and v != ''}
        source_accused['ps_code'] = ps_code
        source_summary = {}
        if role_in_crime:
            source_summary['role_in_crime'] = 'LLM'
        if key_details:
            source_summary['key_details'] = 'LLM'
        if accused_type:
            source_summary['accused_type'] = 'LLM_CLASSIFICATION'

        canonical_person_id, dedup_confidence, dedup_match_tier, dedup_review_flag = _resolve_canonical_identity(
            conn,
            crime_id,
            {
                'accused_id': accused_id,
                'person_code': person_code,
                'full_name': full_name,
                'alias_name': alias_name,
                'age': age,
                'gender': gender,
                'address': address,
            },
            ps_code,
        )

        insert_accused_facts(conn, {
            'crime_id'             : crime_id,
            'accused_id'           : accused_id,
            'person_id'            : person_id,
            'canonical_person_id'  : canonical_person_id,
            'person_code'          : person_code,
            'seq_num'              : seq_num,
            'existing_accused'     : True,
            'full_name'            : full_name,
            'alias_name'           : alias_name,
            'age'                  : age,
            'gender'               : gender,
            'occupation'           : occupation,
            'address'              : address,
            'phone_numbers'        : phone_numbers,
            'role_in_crime'        : role_in_crime,
            'key_details'          : key_details,
            'accused_type'         : accused_type,
            'status'               : status,
            'is_ccl'               : is_ccl,
            'dedup_match_tier'     : dedup_match_tier,
            'dedup_confidence'     : dedup_confidence,
            'dedup_review_flag'    : dedup_review_flag,
            'source_person_fields' : source_person,
            'source_accused_fields': source_accused,
            'source_summary_fields': source_summary,
            'etl_run_id'           : run_id,
        })
        count += 1

    logging.info(f"Branch A processed Crime {crime_id}. row_count={count}")
    return count, branch_records


# ---------------------------------------------------------------------------
# Branch B — ALL person_id IS NULL. Full LLM + pair accused_id from DB.
# ---------------------------------------------------------------------------

def _process_branch_b(conn, crime_id, ps_code, facts_text, db_accused, run_id):
    """Full LLM pipeline + accused_id recovery from DB by code matching."""
    logging.info(
        f"Branch B: crime {crime_id} has {len(db_accused)} stub accused rows "
        f"(person_id IS NULL). Running full LLM extraction."
    )

    extractions = extract_accused_info(facts_text)

    if extractions is None:
        logging.error(f"Branch B: LLM extraction failed for Crime {crime_id}.")
        insert_accused_facts(conn, {
            'crime_id'        : crime_id,
            'full_name'       : None,
            'accused_type'    : None,
            'status'          : None,
            'existing_accused': False,
            'role_in_crime'   : 'LLM_EXTRACTION_FAILED',
            'source_summary_fields': {'error': 'LLM_EXTRACTION_FAILED'},
            'etl_run_id'      : run_id,
        })
        return 1

    branch_records = []
    count = 0

    if not extractions:
        logging.info(f"Branch B: No accused found by LLM for Crime {crime_id}.")
        insert_accused_facts(conn, {
            'crime_id'        : crime_id,
            'full_name'       : None,
            'accused_type'    : None,
            'status'          : None,
            'existing_accused': False,
            'role_in_crime'   : 'NO_ACCUSED_IN_TEXT',
            'source_summary_fields': {'note': 'NO_ACCUSED_IN_TEXT'},
            'etl_run_id'      : run_id,
        })
        count = 1
    else:
        for accused in extractions:
            data = accused.model_dump()
            data['crime_id']  = crime_id
            data['person_id'] = None

            accused_id, matched_code, is_ccl_db, accused_status_raw = \
                _pair_accused_id_from_db(accused.full_name, facts_text, db_accused)

            data['accused_id']       = accused_id
            data['person_code']      = matched_code
            data['existing_accused'] = False

            if not data.get('accused_id') and accused.full_name:
                data['accused_id'] = _synthetic_accused_id(crime_id, accused.full_name, data.get('seq_num'))

            if accused_id:
                # Use raw DB status, fallback to LLM-detected
                data['status'] = resolve_status_for_insert(
                    accused_status_raw, facts_text, accused.full_name
                ) or data.get('status')

            data['gender'] = detect_gender(facts_text, accused.full_name, data.get('gender'))

            if accused_id and bool(is_ccl_db):
                data['is_ccl'] = True

            if data.get('accused_type') == 'unknown':
                data['accused_type'] = None
            if data.get('status') == 'unknown':
                data['status'] = None

            # Source audit trail: all from LLM in Branch B
            data['source_person_fields'] = {k: 'LLM' for k in
                ['full_name', 'alias_name', 'age', 'gender', 'occupation', 'address', 'phone_numbers']
                if data.get(k) is not None}
            data['source_accused_fields'] = {
                'accused_id': 'DB_PAIRED' if accused_id else 'UNMATCHED',
                'ps_code': ps_code,
            }
            data['source_summary_fields'] = {k: 'LLM' for k in
                ['role_in_crime', 'key_details', 'accused_type', 'status']
                if data.get(k) is not None}

            canonical_person_id, dedup_confidence, dedup_match_tier, dedup_review_flag = _resolve_canonical_identity(
                conn,
                crime_id,
                data,
                ps_code,
            )
            data['canonical_person_id'] = canonical_person_id
            data['dedup_confidence'] = dedup_confidence
            data['dedup_match_tier'] = dedup_match_tier
            data['dedup_review_flag'] = dedup_review_flag
            data['etl_run_id'] = run_id

            insert_accused_facts(conn, data)
            count += 1

    logging.info(f"Branch B processed Crime {crime_id}. row_count={count}")
    return count, branch_records


# ---------------------------------------------------------------------------
# Branch C — No accused rows in DB. Full LLM only.
# ---------------------------------------------------------------------------

def _process_branch_c(conn, crime_id, ps_code, facts_text, run_id):
    """Original full LLM flow. No DB reference at all."""
    extractions = extract_accused_info(facts_text)

    if extractions is None:
        logging.error(f"Branch C: Extraction failed for Crime {crime_id}.")
        insert_accused_facts(conn, {
            'crime_id'        : crime_id,
            'full_name'       : None,
            'accused_type'    : None,
            'status'          : None,
            'existing_accused': False,
            'role_in_crime'   : 'LLM_EXTRACTION_FAILED',
            'source_summary_fields': {'error': 'LLM_EXTRACTION_FAILED'},
            'etl_run_id'      : run_id,
        })
        return 1

    branch_records = []
    count = 0

    if not extractions:
        logging.info(f"Branch C: No accused found for Crime {crime_id}.")
        insert_accused_facts(conn, {
            'crime_id'        : crime_id,
            'full_name'       : None,
            'accused_type'    : None,
            'status'          : None,
            'existing_accused': False,
            'role_in_crime'   : 'NO_ACCUSED_IN_TEXT',
            'source_summary_fields': {'note': 'NO_ACCUSED_IN_TEXT'},
            'etl_run_id'      : run_id,
        })
        count = 1
    else:
        for accused in extractions:
            data = accused.model_dump()
            data['crime_id']         = crime_id
            data['accused_id']       = _synthetic_accused_id(crime_id, accused.full_name, data.get('seq_num'))
            data['person_id']        = None
            data['existing_accused'] = False
            data['gender']           = detect_gender(facts_text, accused.full_name, data.get('gender'))
            data['etl_run_id']       = run_id

            if data.get('accused_type') == 'unknown':
                data['accused_type'] = None
            if data.get('status') == 'unknown':
                data['status'] = None

            # Source audit trail: all from LLM in Branch C
            data['source_person_fields'] = {k: 'LLM' for k in
                ['full_name', 'alias_name', 'age', 'gender', 'occupation', 'address', 'phone_numbers']
                if data.get(k) is not None}
            data['source_accused_fields'] = {'ps_code': ps_code}
            data['source_summary_fields'] = {k: 'LLM' for k in
                ['role_in_crime', 'key_details', 'accused_type', 'status']
                if data.get(k) is not None}

            canonical_person_id, dedup_confidence, dedup_match_tier, dedup_review_flag = _resolve_canonical_identity(
                conn,
                crime_id,
                data,
                ps_code,
            )
            data['canonical_person_id'] = canonical_person_id
            data['dedup_confidence'] = dedup_confidence
            data['dedup_match_tier'] = dedup_match_tier
            data['dedup_review_flag'] = dedup_review_flag

            insert_accused_facts(conn, data)
            count += 1

    logging.info(f"Branch C processed Crime {crime_id}. row_count={count}")
    return count, branch_records


if __name__ == "__main__":
    main()
