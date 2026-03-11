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
            process_crimes(conn, crimes)
        else:
            logging.info("No input IDs provided. Starting Dynamic Batch Processing...")
            batch_size = 50
            total_processed = 0
            while True:
                crimes = fetch_unprocessed_crimes(conn, limit=batch_size)
                if not crimes:
                    logging.info("No more unprocessed crimes found. Exiting.")
                    break
                logging.info(f"Fetched batch of {len(crimes)} unprocessed crimes.")
                process_crimes(conn, crimes)
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

def process_crimes(conn, crimes):
    """Processes a list of crimes. Commits per crime for safety."""
    for crime in crimes:
        crime_id = crime['crime_id']
        facts_text = (crime['brief_facts'] or "").strip()
        logging.info(f"Processing Crime ID: {crime_id}")

        try:
            db_accused = fetch_existing_accused_for_crime(conn, crime_id)
            branch = _classify_db_accused(db_accused)
            logging.info(
                f"Crime {crime_id}: Branch {branch} "
                f"({len(db_accused)} accused rows, "
                f"{sum(1 for r in db_accused if r.get('person_id'))} with person_id)"
            )

            if branch == 'A':
                _process_branch_a(conn, crime_id, facts_text, db_accused)
            elif branch == 'B':
                _process_branch_b(conn, crime_id, facts_text, db_accused)
            else:
                _process_branch_c(conn, crime_id, facts_text)

            # Per-crime commit — safe, each crime is independent
            conn.commit()
            logging.info(f"Crime {crime_id} committed successfully.")

        except Exception as e:
            conn.rollback()
            logging.error(f"Failed processing Crime {crime_id}: {e}", exc_info=True)


# ---------------------------------------------------------------------------
# Branch A — DB has accused rows, at least one person_id IS NOT NULL
# ---------------------------------------------------------------------------

def _process_branch_a(conn, crime_id, facts_text, db_accused):
    """
    DB is authoritative for identity. LLM extracts roles only.
    Handles NULL accused_code by using positional index fallback.
    """
    roles_by_code = extract_roles_for_known_accused(facts_text, list(db_accused))
    if not roles_by_code:
        roles_by_code = {}

    # Positional fallback: if only 1 accused and 1 role, pair directly
    single_role = None
    if len(db_accused) == 1 and len(roles_by_code) == 1:
        single_role = list(roles_by_code.values())[0]

    count = 0
    for row in db_accused:
        accused_id    = row.get('accused_id')
        person_id     = row.get('person_id')
        accused_code  = row.get('accused_code') or ''
        is_ccl_db     = row.get('is_ccl', False)

        full_name     = row.get('full_name')
        alias_name    = row.get('alias_name')
        age           = row.get('age')
        gender        = row.get('gender')
        occupation    = row.get('occupation')
        address       = row.get('address')
        phone_numbers = row.get('phone_numbers')

        # Role pairing: code → normalised code → name → positional
        role_data = _pair_role_to_accused(accused_code, full_name, roles_by_code)
        if not role_data and single_role:
            role_data = single_role  # Positional fallback for single-accused cases

        role_in_crime = role_data.get('role_in_crime') if role_data else None
        key_details   = role_data.get('key_details') if role_data else None

        # Status
        status = _resolve_status(row.get('accused_status'), facts_text, full_name or accused_code)

        # Classification
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
        if status == 'unknown':
            status = None

        insert_accused_facts(conn, {
            'crime_id'             : crime_id,
            'accused_id'           : accused_id,
            'person_id'            : person_id,
            'existing_accused'     : person_id is not None,
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
            'source_person_fields' : {},
            'source_accused_fields': {},
            'source_summary_fields': {},
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
            data['existing_accused'] = False

            if accused_id:
                db_status_norm = normalize_accused_status(accused_status_raw)
                data['status'] = db_status_norm or data.get('status')

            data['gender'] = detect_gender(facts_text, accused.full_name, data.get('gender'))

            if accused_id and bool(is_ccl_db):
                data['is_ccl'] = True

            if data.get('accused_type') == 'unknown':
                data['accused_type'] = None
            if data.get('status') == 'unknown':
                data['status'] = None

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

            insert_accused_facts(conn, data)
            count += 1

    logging.info(f"Branch C processed Crime {crime_id}. row_count={count}")


if __name__ == "__main__":
    main()
