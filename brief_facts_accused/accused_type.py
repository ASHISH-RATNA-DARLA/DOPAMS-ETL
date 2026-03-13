import sys
import re
import logging
from db import (
    get_db_connection,
    fetch_crimes_by_ids,
    insert_accused_facts,
    fetch_unprocessed_crimes,
    fetch_existing_accused_for_crime,
    normalize_accused_status,
    resolve_status_for_insert,
    strip_alias_name,
    compute_age_from_dob,
)
from extractor import (
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
        conn.close()
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
        facts_text = (crime['brief_facts'] or "").strip()
        
        # Use connection context manager to ensure proper return to pool
        from db_pooling import PostgreSQLConnectionPool
        pool = PostgreSQLConnectionPool()
        
        with pool.get_connection_context() as conn:
            try:
                db_accused = fetch_existing_accused_for_crime(conn, crime_id)
                branch = _classify_db_accused(db_accused)
                
                if branch == 'A':
                    _process_branch_a(conn, crime_id, facts_text, db_accused)
                elif branch == 'B':
                    _process_branch_b(conn, crime_id, facts_text, db_accused)
                else:
                    _process_branch_c(conn, crime_id, facts_text)

                conn.commit()
                return True, crime_id
            except Exception as e:
                conn.rollback()
                logging.error(f"Failed processing Crime {crime_id}: {e}")
                return False, crime_id
            # Connection automatically returned to pool via context manager

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_crime = {executor.submit(worker, crime): crime['crime_id'] for crime in crimes}
        for future in as_completed(future_to_crime):
            success, cid = future.result()
            if success:
                logging.info(f"✅ Crime {cid} processed successfully.")
            else:
                logging.error(f"❌ Crime {cid} processing failed.")


# ---------------------------------------------------------------------------
# Branch A — DB has accused rows, at least one person_id IS NOT NULL
# ---------------------------------------------------------------------------

def _process_branch_a(conn, crime_id, facts_text, db_accused):
    """
    DB is authoritative for identity. LLM extracts roles + fills missing fields.
    Skips accused rows where person_id IS NULL (spec SKIP RULE).

    person_code logic by accused.type:
      - 'Accused' / 'CCL': person_code = accused_code (direct from DB)
      - 'Known' / 'Respondent' / 'Suspect': LLM assigns person_code (A1, A2 by mention)
    """
    # Filter to only rows with person_id (spec: skip NULL person_id)
    valid_accused = [row for row in db_accused if row.get('person_id')]
    skipped = len(db_accused) - len(valid_accused)
    if skipped:
        logging.info(f"Branch A: Skipped {skipped} accused rows with NULL person_id")

    if not valid_accused:
        logging.info(f"Branch A: No valid accused with person_id for Crime {crime_id}")
        return

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
        source_summary = {}
        if role_in_crime:
            source_summary['role_in_crime'] = 'LLM'
        if key_details:
            source_summary['key_details'] = 'LLM'
        if accused_type:
            source_summary['accused_type'] = 'LLM_CLASSIFICATION'

        insert_accused_facts(conn, {
            'crime_id'             : crime_id,
            'accused_id'           : accused_id,
            'person_id'            : person_id,
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
            'source_person_fields' : source_person,
            'source_accused_fields': source_accused,
            'source_summary_fields': source_summary,
        })
        count += 1

    logging.info(f"Branch A processed Crime {crime_id}. row_count={count}")


# ---------------------------------------------------------------------------
# Branch B — ALL person_id IS NULL. Full LLM + pair accused_id from DB.
# ---------------------------------------------------------------------------

def _process_branch_b(conn, crime_id, facts_text, db_accused):
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
        })
        return

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
            data['source_accused_fields'] = {'accused_id': 'DB_PAIRED' if accused_id else 'UNMATCHED'}
            data['source_summary_fields'] = {k: 'LLM' for k in
                ['role_in_crime', 'key_details', 'accused_type', 'status']
                if data.get(k) is not None}

            insert_accused_facts(conn, data)
            count += 1

    logging.info(f"Branch B processed Crime {crime_id}. row_count={count}")


# ---------------------------------------------------------------------------
# Branch C — No accused rows in DB. Full LLM only.
# ---------------------------------------------------------------------------

def _process_branch_c(conn, crime_id, facts_text):
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
        })
        return

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
        })
        count = 1
    else:
        for accused in extractions:
            data = accused.model_dump()
            data['crime_id']         = crime_id
            data['accused_id']       = None
            data['person_id']        = None
            data['existing_accused'] = False
            data['gender']           = detect_gender(facts_text, accused.full_name, data.get('gender'))

            if data.get('accused_type') == 'unknown':
                data['accused_type'] = None
            if data.get('status') == 'unknown':
                data['status'] = None

            # Source audit trail: all from LLM in Branch C
            data['source_person_fields'] = {k: 'LLM' for k in
                ['full_name', 'alias_name', 'age', 'gender', 'occupation', 'address', 'phone_numbers']
                if data.get(k) is not None}
            data['source_accused_fields'] = {}
            data['source_summary_fields'] = {k: 'LLM' for k in
                ['role_in_crime', 'key_details', 'accused_type', 'status']
                if data.get(k) is not None}

            insert_accused_facts(conn, data)
            count += 1

    logging.info(f"Branch C processed Crime {crime_id}. row_count={count}")


if __name__ == "__main__":
    main()
