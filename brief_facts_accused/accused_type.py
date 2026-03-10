import sys
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
          → DB-primary for identity fields; LLM for roles only
      B — DB has accused rows BUT ALL person_id IS NULL  (stub / orphan persons)
          → Run full LLM (Pass 1 + Pass 2); pair accused_id from DB by code; person_id = NULL
      C — DB has zero accused rows
          → Full LLM (Pass 1 + Pass 2); no DB reference; everything = NULL
    """
    if not db_accused:
        return 'C'
    if any(row.get('person_id') for row in db_accused):
        return 'A'
    return 'B'


# ---------------------------------------------------------------------------
# Accused-code pairing helper (Branch A)
# ---------------------------------------------------------------------------

def _pair_role_to_accused(accused_code, roles_by_code):
    """
    Maps an LLM role entry to a DB accused row using accused_code.
    Handles format variants: 'A-1' vs 'A1' vs 'A.1'.
    Returns dict with role_in_crime / key_details, or {} if no match.
    """
    if not roles_by_code or not accused_code:
        return {}

    def _norm(s):
        return s.replace(' ', '').replace('-', '').replace('.', '').upper()

    code_norm = _norm(accused_code)

    if accused_code in roles_by_code:
        return roles_by_code[accused_code]

    for k, v in roles_by_code.items():
        if _norm(k) == code_norm:
            return v

    return {}


# ---------------------------------------------------------------------------
# Branch B: Accused-code pairing by text context
# ---------------------------------------------------------------------------

import re

# Pre-compiled regex: matches accused codes like A-1, A.2, A1, A 3 etc.
_ACCUSED_CODE_RE = re.compile(r'A[-.\s]?\d+', re.IGNORECASE)


def _pair_accused_id_from_db(extracted_clean_name, facts_text, db_accused_rows):
    """
    Branch B helper: tries to match an LLM-extracted clean name to a DB accused
    row by searching for the DB accused_code near the clean name in the
    original brief_facts text.

    Why not search the extracted name directly?
      extract_accused_info() returns clean_name (e.g. "Rahul Singh") — the prefix
      "A-1" is stripped by clean_accused_name(). So we search the source text.

    Approach:
      1. Find the longest-matching token sequence of extracted_clean_name in text
      2. Look ±30 chars around it for an accused_code pattern (A-1, A.2, etc.)
      3. Match found code against DB rows

    Returns (accused_id, accused_code, is_ccl, accused_status) or (None, None, False, None)
    """
    if not extracted_clean_name or not db_accused_rows or not facts_text:
        return None, None, False, None

    text_lower = facts_text.lower()
    name_lower = extracted_clean_name.lower().strip()

    # Find name in text — try full name first, then longest token prefix
    idx = text_lower.find(name_lower)
    if idx < 0:
        # Try longest token sequence (fixes "Rahul Singh" vs "Rahul Kumar" ambiguity)
        tokens = name_lower.split()
        for length in range(len(tokens), 0, -1):
            partial = ' '.join(tokens[:length])
            idx = text_lower.find(partial)
            if idx >= 0:
                name_lower = partial  # Update for window sizing
                break

    if idx < 0:
        return None, None, False, None

    # Context window ±30 chars around name occurrence
    window_start = max(0, idx - 30)
    window_end = min(len(facts_text), idx + len(name_lower) + 30)
    context_window = facts_text[window_start:window_end]

    # Find all accused codes in the context window using regex
    found_codes = _ACCUSED_CODE_RE.findall(context_window)
    if not found_codes:
        return None, None, False, None

    def _norm(s):
        return (s or '').upper().replace(' ', '').replace('-', '').replace('.', '')

    # Match found codes against DB rows
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
    LOCAL context window only (±120 chars around the name, to avoid
    misclassifying one accused with another's status).
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

    # No full-text fallback — only scan the local context window
    # This prevents cross-contamination between accused statuses
    if not combined:
        return None

    # Absconding checked first (legal priority: absconding until confirmed arrested)
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
# Per-crime dispatcher
# ---------------------------------------------------------------------------

def process_crimes(conn, crimes):
    """Processes a batch of crimes. Commits once per batch, not per crime."""
    success_count = 0

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

            success_count += 1

        except Exception as e:
            conn.rollback()
            logging.error(f"Failed processing Crime {crime_id}: {e}", exc_info=True)

    # Batch-level commit (Issue 7: commit per batch, not per crime)
    try:
        conn.commit()
        logging.info(f"Batch committed. {success_count}/{len(crimes)} crimes succeeded.")
    except Exception as e:
        conn.rollback()
        logging.error(f"Batch commit failed: {e}", exc_info=True)


# ---------------------------------------------------------------------------
# Branch A — DB has accused rows, at least one person_id IS NOT NULL
# Identity: DB-primary (persons JOIN). Roles: LLM.
# ---------------------------------------------------------------------------

def _process_branch_a(conn, crime_id, facts_text, db_accused):
    """
    DB is the authoritative source for identity.
    LLM runs a targeted role-only pass (no Pass 1 name extraction).
    Per-row logic handles the rare in-batch NULL person_id (no persons JOIN data).
    """
    roles_by_code = extract_roles_for_known_accused(facts_text, list(db_accused))
    if not roles_by_code:
        roles_by_code = {}

    count = 0
    for row in db_accused:
        accused_id    = row.get('accused_id')
        person_id     = row.get('person_id')   # may be NULL for a few rows even in Branch A
        accused_code  = row.get('accused_code') or ''
        is_ccl_db     = row.get('is_ccl', False)

        # Identity — fully populated when person_id IS NOT NULL,
        # all None when this specific row has no person_id
        full_name     = row.get('full_name')
        alias_name    = row.get('alias_name')
        age           = row.get('age')
        gender        = row.get('gender')
        occupation    = row.get('occupation')
        address       = row.get('address')
        phone_numbers = row.get('phone_numbers')

        # LLM role for this accused_code
        role_data     = _pair_role_to_accused(accused_code, roles_by_code)
        role_in_crime = role_data.get('role_in_crime')
        key_details   = role_data.get('key_details')

        # Status: DB first, LOCAL keyword scan fallback
        status = _resolve_status(row.get('accused_status'), facts_text, full_name or accused_code)

        # Classification — only if role was extracted
        if role_in_crime:
            classification_text = role_in_crime + (" " + key_details if key_details else "")
            accused_type = classify_accused_type(classification_text)
        else:
            accused_type = None

        # Gender fallback if not in persons — use local context
        if not gender:
            gender = detect_gender(facts_text, full_name or accused_code)

        # CCL: DB boolean first, text cross-check as safety net
        is_ccl = bool(is_ccl_db) or detect_ccl(full_name or '', role_in_crime or '')

        # Normalise sentinels
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
# Branch B — DB has accused rows, ALL person_id IS NULL (stub/orphan persons)
# Scenario 3: crime_id IS NOT NULL AND person_id IS NULL — "few records"
#
# Strategy: Run full LLM (Pass 1 + Pass 2) exactly as before.
#           Try to enrich each LLM result with accused_id from DB via accused_code.
#           person_id stays NULL (no persons data available anywhere).
# ---------------------------------------------------------------------------

def _process_branch_b(conn, crime_id, facts_text, db_accused):
    """
    All accused rows in DB have no person_id.
    Run full LLM pipeline, then pair accused_id from DB by text context matching.
    person_id is always NULL here.
    """
    logging.info(
        f"Branch B: crime {crime_id} has {len(db_accused)} stub accused rows "
        f"(person_id IS NULL). Running full LLM extraction."
    )

    extractions = extract_accused_info(facts_text)

    if extractions is None:
        # Issue 5: Insert failure marker instead of silently skipping
        logging.error(
            f"Branch B: LLM extraction failed for Crime {crime_id}. "
            "Inserting failure marker."
        )
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
        # Issue 6: Use NULL full_name instead of "NO_ACCUSED_FOUND" sentinel
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
            data['person_id'] = None   # always NULL — no source has it

            # Pair accused_id from DB by finding accused_code near the name in text
            accused_id, matched_code, is_ccl_db, accused_status_raw = \
                _pair_accused_id_from_db(accused.full_name, facts_text, db_accused)

            data['accused_id']       = accused_id  # NULL if no code match
            data['existing_accused'] = False  # person_id is NULL, so not "existing"

            # Status: DB first (if we matched a row), keyword scan fallback
            if accused_id:
                db_status_norm = normalize_accused_status(accused_status_raw)
                data['status'] = db_status_norm or data.get('status')

            # Gender
            data['gender'] = detect_gender(facts_text, accused.full_name, data.get('gender'))

            # CCL: DB first if matched
            if accused_id and bool(is_ccl_db):
                data['is_ccl'] = True

            # Normalise sentinels
            if data.get('accused_type') == 'unknown':
                data['accused_type'] = None
            if data.get('status') == 'unknown':
                data['status'] = None

            insert_accused_facts(conn, data)
            count += 1

    logging.info(f"Branch B processed Crime {crime_id}. row_count={count}")


# ---------------------------------------------------------------------------
# Branch C — No accused rows in DB at all
# Exactly the original LLM-only flow. Everything NULL except LLM output.
# ---------------------------------------------------------------------------

def _process_branch_c(conn, crime_id, facts_text):
    """
    Original full LLM flow. Crime has no accused rows in DB at all.
    accused_id = NULL, person_id = NULL, existing_accused = False.
    """
    extractions = extract_accused_info(facts_text)

    if extractions is None:
        # Issue 5: Insert failure marker so it doesn't retry infinitely
        logging.error(
            f"Branch C: Extraction failed for Crime {crime_id}. "
            "Inserting failure marker."
        )
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
        # Issue 6: Use NULL full_name, mark in role_in_crime for traceability
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
