import sys
import re
import logging
import threading
import uuid
import os
import unicodedata
from difflib import SequenceMatcher
# Allow imports from sibling ETL modules (e.g., env_utils from parent)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import config

try:
    from unidecode import unidecode as _unidecode
except Exception:  # pragma: no cover - optional dependency fallback
    _unidecode = None

try:
    import Levenshtein as _lev
    _jaro_winkler = _lev.jaro_winkler
except Exception:  # pragma: no cover - optional
    _jaro_winkler = None

from db import (
    get_db_connection,
    return_db_connection,
    fetch_crimes_by_ids,
    fetch_unprocessed_crimes,
    fetch_unprocessed_crimes_since,
    fetch_unprocessed_crimes_daily,
    get_incremental_cutoff_date,
    fetch_existing_accused_for_crime,
    start_crime_processing_run,
    complete_crime_processing_run,
    fail_crime_processing_run,
    normalize_accused_status,
    resolve_status_for_insert,
    strip_alias_name,
    compute_age_from_dob,
    fetch_dedup_candidates,
    fetch_canonical_by_accused_id,
    fetch_crime_profile,
    fetch_crime_associate_person_codes,
    delete_brief_facts_for_crime,
    update_sentinel_role, bulk_upsert_brief_facts_ai, write_drugs_by_accused_in_memory, insert_accused_facts,
)
from extractor_accused import (
    extract_accused_info,
    extract_accused_names_pass1,
    extract_details_pass2,
    extract_roles_for_known_accused,
    detect_gender,
    detect_ccl,
    classify_accused_type,
    compute_shared_role,
    _is_procedural_role,
    clean_accused_name,
    _is_police_name,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

UNIFIED_TABLE_NAME = "brief_facts_ai"


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


_SOUNDEX_MAP = {
    'B': '1', 'F': '1', 'P': '1', 'V': '1',
    'C': '2', 'G': '2', 'J': '2', 'K': '2', 'Q': '2', 'S': '2', 'X': '2', 'Z': '2',
    'D': '3', 'T': '3',
    'L': '4',
    'M': '5', 'N': '5',
    'R': '6',
}

def _soundex(token):
    """SOUNDEX matching PostgreSQL's algorithm.
    H/W are transparent. Vowels reset prev so same-code consonants across a
    vowel are counted separately (e.g., MOHAMMED → M530, not M300).
    """
    if not token:
        return '0000'
    t = token.upper()
    result = t[0]
    prev = _SOUNDEX_MAP.get(t[0], '0')
    for ch in t[1:]:
        if ch in 'HW':
            continue
        if ch in 'AEIOU':
            prev = '0'
            continue
        code = _SOUNDEX_MAP.get(ch, '0')
        if code != '0' and code != prev:
            result += code
            if len(result) == 4:
                break
        prev = code
    return result.ljust(4, '0')[:4]


def _name_similarity(a, b):
    """Best of SequenceMatcher and Jaro-Winkler for robust Indian name matching."""
    na = _normalize_name(a)
    nb = _normalize_name(b)
    sm = SequenceMatcher(None, na, nb).ratio()
    if _jaro_winkler and na and nb:
        return max(sm, _jaro_winkler(na, nb))
    return sm


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
    if inter:
        return (2.0 * inter) / (len(ta) + len(tb))
    # Single-token names with no overlap: use char-level similarity (discounted)
    if len(ta) == 1 and len(tb) == 1:
        return _name_similarity(list(ta)[0], list(tb)[0]) * 0.5
    return 0.0


def _phonetic_overlap(a, b):
    na = _normalize_name(a)
    nb = _normalize_name(b)
    if not na or not nb:
        return 0.0
    # Compare SOUNDEX of the first (primary) name token — matches PostgreSQL
    first_a = na.split()[0]
    first_b = nb.split()[0]
    if _soundex(first_a) == _soundex(first_b) and _soundex(first_a) != '0000':
        return 1.0
    # Fallback: 3-char prefix for very short names
    return 1.0 if na[:3] == nb[:3] else 0.0


def _address_similarity(a, b):
    ta = set(re.findall(r'[a-z0-9]+', (a or '').lower()))
    tb = set(re.findall(r'[a-z0-9]+', (b or '').lower()))
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)


def _normalize_phone_digits(value):
    if not value:
        return ''
    digits = re.sub(r'\D', '', str(value))
    return digits[-10:] if len(digits) >= 10 else digits


def _is_same_crime_duplicate_accused(row_a, row_b):
    """
    Conservative duplicate detector for source accused rows within ONE crime.
    Only merges when name is effectively the same person (often token reorder)
    plus corroborating identity fields.
    """
    name_a = row_a.get('full_name') or ''
    name_b = row_b.get('full_name') or ''
    norm_a = _normalize_name(name_a)
    norm_b = _normalize_name(name_b)
    if not norm_a or not norm_b:
        return False

    tokens_a = set(norm_a.split())
    tokens_b = set(norm_b.split())
    if len(tokens_a) < 2 or len(tokens_b) < 2:
        return False

    same_tokens_reordered = tokens_a == tokens_b
    strong_name_similarity = _name_similarity(name_a, name_b) >= 0.93
    if not (same_tokens_reordered or strong_name_similarity):
        return False

    phone_a = _normalize_phone_digits(row_a.get('phone_numbers'))
    phone_b = _normalize_phone_digits(row_b.get('phone_numbers'))
    same_phone = bool(phone_a and phone_b and phone_a == phone_b)

    age_a = row_a.get('age')
    age_b = row_b.get('age')
    same_age = age_a is not None and age_b is not None and str(age_a) == str(age_b)

    gender_a = (row_a.get('gender') or '').strip().lower()
    gender_b = (row_b.get('gender') or '').strip().lower()
    same_gender = bool(gender_a and gender_b and gender_a == gender_b)

    addr_sim = _address_similarity(row_a.get('address'), row_b.get('address'))

    # Require either exact phone match, or age+gender+address corroboration.
    return same_phone or (same_age and same_gender and addr_sim >= 0.45)


def _is_supplier_context(name: str, text: str) -> bool:
    if not name or not text:
        return False

    lowered_text = text.lower()
    lowered_name = name.lower().strip()
    idx = lowered_text.find(lowered_name)
    while idx >= 0:
        start = max(0, idx - 100)
        end = min(len(lowered_text), idx + len(lowered_name) + 50)
        window = lowered_text[start:end]
        if any(
            marker in window
            for marker in (
                # Direct purchase/obtain markers
                'purchased from', 'purchase from', 'purchase ganja from',
                'procured from', 'obtain from', 'obtained from',
                'bought from', 'buy from', 'buy ganja from',
                'brought from', 'get from', 'got from', 'sourced from',
                # Supplier relationship markers
                'supplied by', 'supplier', 'selling ganja', 'sell the ganja',
                'deliver the ganja', 'supply ganja', 'supply ', 'from the supplier',
                'used to purchase', 'used to buy', 'use to purchase', 'use to buy'
            )
        ):
            return True
        idx = lowered_text.find(lowered_name, idx + 1)

    return False


def _is_absconding_context(name: str, text: str) -> bool:
    """Detect if name appears only in absconding/evading context, not as apprehended accused."""
    if not name or not text:
        return False

    lowered_text = text.lower()
    lowered_name = name.lower().strip()
    idx = lowered_text.find(lowered_name)
    while idx >= 0:
        start = max(0, idx - 100)
        end = min(len(lowered_text), idx + len(lowered_name) + 100)
        window = lowered_text[start:end]
        if any(
            marker in window
            for marker in (
                'absconding', 'absconded', 'on the run', 'evading', 'evaded',
                'fled', 'escaped', 'not traceable', 'untraceable', 'whereabouts unknown',
                'at large', 'in hiding', 'fugitive', 'not apprehended', 'not arrested'
            )
        ):
            # Check that accused is NOT also mentioned as apprehended in same context
            if not any(marker in window for marker in ('caught', 'apprehended', 'arrested', 'confessed')):
                return True
        idx = lowered_text.find(lowered_name, idx + 1)

    return False


def _is_associate_only_context(name: str, text: str) -> bool:
    """Detect if name appears only as associate/reference, not primary accused."""
    if not name or not text:
        return False

    lowered_text = text.lower()
    lowered_name = name.lower().strip()
    idx = lowered_text.find(lowered_name)
    while idx >= 0:
        start = max(0, idx - 100)
        end = min(len(lowered_text), idx + len(lowered_name) + 100)
        window = lowered_text[start:end]
        if any(
            marker in window
            for marker in (
                'along with', 'accompanied by', 'together with', 'in company with',
                'associate', 'associates of', 'known associates', 'friend of', 'friends of',
                'relative of', 'relatives of', 'in association with'
            )
        ):
            # Exclude if also mentioned as directly involved
            if not any(marker in window for marker in ('caught', 'apprehended', 'arrested', 'possession', 'seized')):
                return True
        idx = lowered_text.find(lowered_name, idx + 1)

    return False


def _is_harbourer_context(name: str, text: str) -> bool:
    """Detect if name appears only as someone who provided shelter/safe house."""
    if not name or not text:
        return False

    lowered_text = text.lower()
    lowered_name = name.lower().strip()
    idx = lowered_text.find(lowered_name)
    while idx >= 0:
        start = max(0, idx - 100)
        end = min(len(lowered_text), idx + len(lowered_name) + 100)
        window = lowered_text[start:end]
        if any(
            marker in window
            for marker in (
                'harboured', 'harbored', 'provided shelter', 'safe house', 'hideout',
                'protected', 'shelter to', 'allowed to stay', 'hide', 'hiding place'
            )
        ):
            return True
        idx = lowered_text.find(lowered_name, idx + 1)

    return False


def _is_financier_context(name: str, text: str) -> bool:
    """Detect if name appears only as financier/funder."""
    if not name or not text:
        return False

    lowered_text = text.lower()
    lowered_name = name.lower().strip()
    idx = lowered_text.find(lowered_name)
    while idx >= 0:
        start = max(0, idx - 100)
        end = min(len(lowered_text), idx + len(lowered_name) + 100)
        window = lowered_text[start:end]
        if any(
            marker in window
            for marker in (
                'financed', 'financer', 'financier', 'provided funds', 'funded',
                'paid for', 'financial support', 'money provider', 'funded the operation'
            )
        ):
            return True
        idx = lowered_text.find(lowered_name, idx + 1)

    return False



def _should_skip_role_only_mention(name: str, facts_text: str) -> tuple:
    """
    Detect if name should be skipped or reassigned due to specific role context.
    Returns (should_skip, corrected_status, corrected_accused_type, reason).
    """
    # Check contexts in order of severity (skip most reference-only mentions)
    if _is_supplier_context(name, facts_text):
        return (False, None, "supplier", "supplier-only context")
    if _is_associate_only_context(name, facts_text):
        return (True, None, None, "associate-only reference")
    if _is_absconding_context(name, facts_text):
        return (False, "absconding", None, "absconding accused")
    if _is_harbourer_context(name, facts_text):
        return (False, None, "harbourer", "harbourer-only context")
    if _is_financier_context(name, facts_text):
        return (False, None, "financier", "financier-only context")

    return (False, None, None, None)

def _match_extracted_name_to_db_accused(extracted_name: str, db_name_variants: list) -> bool:
    """
    Conservative matching to detect when extracted text name already exists in DB accused
    as a variant (spelling, order, phonetic, partial). Returns True if a match is found.
    """
    if not extracted_name or not db_name_variants:
        return False

    extracted_lower = extracted_name.lower().strip()
    extracted_tokens = set(extracted_lower.split())

    for db_name in db_name_variants:
        if not db_name:
            continue
        db_lower = db_name.lower().strip()
        db_tokens = set(db_lower.split())

        # 1. Exact match (after normalization)
        if extracted_lower == db_lower:
            return True

        # 2. Name-order reversal: "Muniraju Gollari" vs "Gollari Muniraju"
        if extracted_tokens == db_tokens:
            return True

        # 3. Substring match: single extracted name is part of DB name
        # E.g., "Prakash" in "Prakash Pawar", "Ramesh" in "Ramesh Margel"
        if len(extracted_tokens) == 1:
            extracted_word = list(extracted_tokens)[0]
            if any(extracted_word == token for token in db_tokens):
                return True

        # 4. Phonetic component matching: "Majji" vs "Majhi", "Naini" vs "Nainu"
        # Use dmetaphone on name tokens
        try:
            from metaphone import doublemetaphone
            extracted_phones = set()
            for token in extracted_tokens:
                primary, secondary = doublemetaphone(token)
                if primary:
                    extracted_phones.add(primary)
            db_phones = set()
            for token in db_tokens:
                primary, secondary = doublemetaphone(token)
                if primary:
                    db_phones.add(primary)
            if extracted_phones and db_phones and extracted_phones & db_phones:
                return True
        except Exception:
            pass

        # 5. High fuzzy similarity on full names (85% threshold for shorter names)
        if _name_similarity(extracted_name, db_name) >= 0.85:
            return True

        # 6. High token overlap (2+ tokens in common, at least 50% of smaller name)
        overlap = len(extracted_tokens & db_tokens)
        min_tokens = min(len(extracted_tokens), len(db_tokens))
        if overlap >= 2 and (min_tokens == 0 or overlap / min_tokens >= 0.5):
            return True

    return False


def _accused_row_priority(row):
    score = 0
    if row.get('person_id'):
        score += 2
    if row.get('accused_code'):
        score += 2
    accused_type_db = (row.get('accused_type_db') or '').strip().lower()
    if accused_type_db in {'accused', 'ccl'}:
        score += 2
    status = (row.get('accused_status') or '').lower()
    if any(k in status for k in ('arrest', 'apprehend', 'detain', 'caught')):
        score += 1
    return score


def _dedupe_same_crime_accused_rows(rows):
    if not rows:
        return [], []

    kept = []
    dropped = []
    for row in rows:
        match_index = None
        for idx, existing in enumerate(kept):
            if _is_same_crime_duplicate_accused(existing, row):
                match_index = idx
                break

        if match_index is None:
            kept.append(row)
            continue

        existing = kept[match_index]
        if _accused_row_priority(row) > _accused_row_priority(existing):
            kept[match_index] = row
            dropped.append(existing)
        else:
            dropped.append(row)

    return kept, dropped


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


def _token_fuzzy_similarity(a, b):
    """Fuzzy matching on individual name tokens to handle reordering and spelling variants.
    E.g., 'Afeez Amaan Sayed' vs 'Syed Afeez Aman' should match despite token reordering and 'Amaan'/'Aman' variant."""
    ta = set(_normalize_name(a).split())
    tb = set(_normalize_name(b).split())
    if not ta or not tb:
        return 0.0

    matched = 0
    threshold = 0.85

    for token_a in ta:
        best_match = 0.0
        for token_b in tb:
            sim = _name_similarity(token_a, token_b)
            if sim > best_match:
                best_match = sim
        if best_match >= threshold:
            matched += 1

    if matched == 0:
        return 0.0

    return (2.0 * matched) / (len(ta) + len(tb))


def _crime_tokens(value):
    return set(re.findall(r'[a-z0-9]+', (value or '').lower()))


def _dedup_score(current, candidate, ps_code, current_crime_profile, current_assoc_codes, candidate_assoc_codes):
    name_a = current.get('full_name')
    name_b = candidate.get('full_name')

    prefix_similarity = _name_similarity(name_a, name_b)
    token_similarity = _token_set_similarity(name_a, name_b)
    fuzzy_token_similarity = _token_fuzzy_similarity(name_a, name_b)
    phonetic_similarity = _phonetic_overlap(name_a, name_b)
    addr_similarity = _address_similarity(current.get('address'), candidate.get('address'))
    age_similarity = _age_score(current.get('age'), candidate.get('age'))
    alias_similarity = _alias_score(current.get('alias_name'), candidate.get('alias_name'))

    score = (
        0.30 * prefix_similarity +
        0.10 * token_similarity +
        0.20 * fuzzy_token_similarity +
        0.15 * phonetic_similarity +
        0.12 * addr_similarity +
        0.10 * age_similarity +
        0.03 * alias_similarity
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

    # Contextual boost: age and gender both match in same crime (only if name already has overlap)
    # Only apply when there's already some name similarity to avoid false positives
    current_age = current.get('age')
    current_gender = current.get('gender')
    candidate_age = candidate.get('age')
    candidate_gender = candidate.get('gender')
    if (fuzzy_token_similarity > 0 and current_age and candidate_age and
        current_gender and candidate_gender and
        str(current_age) == str(candidate_age) and
        str(current_gender).lower() == str(candidate_gender).lower()):
        score += 0.12

    normalized = _normalize_name(name_a)
    if len(normalized.split()) == 1 and normalized in _COMMON_NAME_TOKENS:
        score *= 0.85

    return round(min(score, 1.0), 2)


def _resolve_canonical_identity(conn, current_crime_id, payload, ps_code,
                                _crime_profile_cache=None, _assoc_cache=None,
                                _dedup_candidate_cache=None):
    """
    _crime_profile_cache and _assoc_cache are caller-owned dicts passed in so
    repeated calls within the same crime reuse already-fetched data instead of
    hitting the DB once per accused.  Both default to None (first call or
    standalone use) and are populated in place.
    """
    if _crime_profile_cache is None:
        _crime_profile_cache = {}
    if _assoc_cache is None:
        _assoc_cache = {}
    if _dedup_candidate_cache is None:
        _dedup_candidate_cache = {}

    current_accused_id = payload.get('accused_id')
    current_person_code = payload.get('person_code')
    full_name = payload.get('full_name')
    gender = payload.get('gender')
    fallback_canonical = _canonical_person_id(full_name, gender, ps_code)

    if current_crime_id not in _crime_profile_cache:
        _crime_profile_cache[current_crime_id] = fetch_crime_profile(conn, current_crime_id)
    current_crime_profile = _crime_profile_cache[current_crime_id]

    if current_crime_id not in _assoc_cache:
        _assoc_cache[current_crime_id] = fetch_crime_associate_person_codes(conn, current_crime_id)
    current_assoc_codes = _assoc_cache[current_crime_id]

    # Layer 0: direct accused_id lookup — bypasses candidate pool entirely.
    # person_code (A1, A2...) is crime-relative sequence, NOT a cross-crime identifier.
    # Only accused_id (DB UUID from public.accused) is person-specific and safe to match across crimes.
    if current_accused_id:
        row = fetch_canonical_by_accused_id(conn, current_accused_id, current_crime_id)
        if row and row.get('canonical_person_id'):
            return row['canonical_person_id'], None, 0, False

    # Layer 1: exact accused_id match within candidate pool (phonetic neighbours)
    cache_key = (current_crime_id, full_name, ps_code)
    if cache_key not in _dedup_candidate_cache:
        _dedup_candidate_cache[cache_key] = fetch_dedup_candidates(
            conn, current_crime_id, full_name, ps_code
        )
    candidates = _dedup_candidate_cache[cache_key]
    for cand in candidates:
        if current_accused_id and cand.get('accused_id') and str(current_accused_id) == str(cand.get('accused_id')):
            return cand.get('canonical_person_id') or fallback_canonical, None, 1, False

    # Layer 3-5: weighted match and thresholding
    best_cand = None
    best_score = -1.0
    for cand in candidates:
        cand_crime_id = cand.get('crime_id')
        if cand_crime_id not in _assoc_cache:
            _assoc_cache[cand_crime_id] = fetch_crime_associate_person_codes(conn, cand_crime_id)
        candidate_assoc_codes = _assoc_cache.get(cand_crime_id, set())
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

    if best_cand and best_score >= 0.70 and best_cand.get('canonical_person_id'):
        return best_cand.get('canonical_person_id'), best_score, 1, False

    if best_score >= 0.45:
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
                    WHEN seq_num ~ '^[0-9]+$' THEN seq_num::numeric
                    ELSE 999999999999999999
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
        from db_pooling import PostgreSQLConnectionPool
        from brief_facts_ai.etl_config import get_config as get_brief_facts_config

        config_obj = get_brief_facts_config()
        pool = PostgreSQLConnectionPool(
            minconn=config_obj.db_pool_min_conn,
            maxconn=config_obj.db_pool_max_conn,
        )
        pool.reset()
        logging.info("Connection pool reset for fresh start")

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
            # ---------------------------------------------------------------
            # Determine processing mode:
            #   Backfill  — first run; no processing history exists yet.
            #               Must scan the full crimes table.
            #   Incremental — daily run after backfill; limit the scan to
            #               crimes modified since the last successful run
            #               (with 1-day overlap for safety).
            # ---------------------------------------------------------------
            try:
                sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'etl_master')))
                from checkpoint_manager import is_backfill_complete
                backfill_done = is_backfill_complete()
            except Exception as _cp_err:
                logging.warning("Could not read backfill checkpoint (%s); defaulting to full scan.", _cp_err)
                backfill_done = False

            # Scale batch size for parallel processing on 64GB server
            batch_size = int(os.environ.get('BATCH_SIZE', '30'))
            total_processed = 0

            if backfill_done:
                # ----------------------------------------------------------
                # Daily incremental mode
                # Single authoritative check: Find crimes in crimes table NOT YET in brief_facts_ai
                # This catches:
                # - Crimes never processed (no row in brief_facts_ai)
                # - Crimes modified since last processing
                # - Crimes with incomplete/failed processing
                # ----------------------------------------------------------
                logging.info("Daily incremental mode: checking for unprocessed crimes in crimes table...")
                unprocessed_count = 0
                while True:
                    crimes = fetch_unprocessed_crimes_daily(conn, limit=batch_size)
                    if not crimes:
                        logging.info("✅ All crimes processed. No unprocessed records found in crimes table.")
                        break
                    unprocessed_count = len(crimes)
                    logging.info(f"📋 Fetched batch of {unprocessed_count} unprocessed crimes (daily check).")
                    process_crimes_parallel(crimes)
                    total_processed += len(crimes)
                    logging.info(f"✅ Batch complete. Total processed so far: {total_processed}")

                if total_processed == 0:
                    logging.info("✅ Daily run complete: No unprocessed crimes found. System up-to-date.")
            else:
                # ----------------------------------------------------------
                # Backfill mode — scan the full crimes table.
                # ----------------------------------------------------------
                logging.info("Backfill mode: scanning all unprocessed crimes.")
                while True:
                    crimes = fetch_unprocessed_crimes(conn, limit=batch_size)
                    if not crimes:
                        logging.info("No more unprocessed crimes found. Exiting.")
                        break
                    logging.info("Fetched batch of %d unprocessed crimes.", len(crimes))
                    process_crimes_parallel(crimes)
                    total_processed += len(crimes)
                    logging.info("Batch complete. Total processed so far: %d", total_processed)

            logging.info("Total crimes processed this run: %d", total_processed)

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

def process_crimes_parallel(crimes):
    """Processes a list of crimes in parallel using thread pool and connection pool."""
    # Load configuration from environment (fail-fast on missing vars)
    from brief_facts_ai.etl_config import get_config
    config_obj = get_config()
    max_workers = config_obj.parallel_llm_workers
    batch_size_limit = config_obj.batch_size
    batch_commit_size = config_obj.batch_commit_size
    logging.info(f"🚀 Scaling accused extraction with {max_workers} parallel workers (commit every {batch_commit_size} crimes)")

    # Fetch drug KB once — shared read-only across all worker threads.
    # Previously fetched+rebuilt inside every worker (3 DB queries + 379KB parse per crime).
    from extractor_drugs import build_drug_keywords, extract_drug_info
    import db as db_module
    from db_pooling import get_singleton_pool
    _pool = get_singleton_pool()
    _bootstrap_conn = _pool.get_connection()
    try:
        _drug_categories = db_module.fetch_drug_categories(_bootstrap_conn)
        _ignore_dict     = db_module.fetch_drug_ignore_list(_bootstrap_conn)
    finally:
        _pool.return_connection(_bootstrap_conn)
    _ignore_set      = set(_ignore_dict.keys())
    _kb_lookup       = {row['raw_name'].lower().strip(): row['standard_name'] for row in _drug_categories}
    _dynamic_keywords = build_drug_keywords(_drug_categories)
    logging.info(f"Drug KB loaded once: {len(_dynamic_keywords)} keywords, {len(_drug_categories)} categories")

    # Track batch commits for performance monitoring
    crime_count = 0
    commit_count = 0

    def worker(crime):
        nonlocal crime_count, commit_count
        crime_id = crime['crime_id']
        ps_code = crime.get('ps_code')
        facts_text = (crime['brief_facts'] or "").strip()

        # Get connection from singleton pool (not recreated per crime)
        pool = get_singleton_pool()

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
                    augmented_text = _inject_accused_roster(
                        facts_text,
                        [tuple([None, None, r.get('person_code'), r.get('full_name')])
                         for r in branch_records if r.get('person_code')]
                    )
                    extractions = extract_drug_info(
                        augmented_text, _drug_categories,
                        ignore_set=_ignore_set, kb_lookup=_kb_lookup,
                        dynamic_drug_keywords=_dynamic_keywords, conn=conn,
                    )

                    if not extractions and branch_records:
                        # Accused exist but no drugs found — stamp NO_DRUGS_DETECTED on each accused row
                        extractions = [{'raw_drug_name': 'NO_DRUGS_DETECTED'}]

                    if not branch_records and extractions:
                        # Drugs found but no accused — upgrade sentinel and attach each drug individually
                        update_sentinel_role(conn, crime_id, 'NO_ACCUSED_IN_TEXT', 'NO_ACCUSED_DRUGS_ONLY')
                        update_sentinel_role(conn, crime_id, 'LLM_EXTRACTION_FAILED', 'NO_ACCUSED_DRUGS_ONLY')
                        orphan_row = {
                            'crime_id'             : crime_id,
                            'accused_id'           : None,
                            'person_id'            : None,
                            'canonical_person_id'  : None,
                            'person_code'          : None,
                            'seq_num'              : None,
                            'existing_accused'     : False,
                            'full_name'            : None,
                            'alias_name'           : None,
                            'age'                  : None,
                            'gender'               : None,
                            'occupation'           : None,
                            'address'              : None,
                            'phone_numbers'        : None,
                            'role_in_crime'        : 'NO_ACCUSED_DRUGS_ONLY',
                            'key_details'          : None,
                            'accused_type'         : None,
                            'status'               : None,
                            'is_ccl'               : False,
                            'dedup_match_tier'     : None,
                            'dedup_confidence'     : None,
                            'dedup_review_flag'    : False,
                            'source_person_fields' : {},
                            'source_accused_fields': {},
                            'source_summary_fields': {'note': 'NO_ACCUSED_DRUGS_ONLY'},
                            'drugs'                : [],
                            'etl_run_id'           : run_id,
                        }
                        enriched_rows = db_module.write_drugs_by_accused_in_memory([orphan_row], extractions)
                    else:
                        enriched_rows = db_module.write_drugs_by_accused_in_memory(branch_records, extractions)

                    db_module.bulk_upsert_brief_facts_ai(conn, enriched_rows)
                    logging.info(f"Crime {crime_id}: {len(enriched_rows)} rows written to brief_facts_ai")

                if unified_mode and run_id:
                    complete_crime_processing_run(conn, run_id, rows_written)

                # Batch commit strategy: commit every N crimes, use SAVEPOINT for per-crime rollback
                # This reduces fsync overhead (~50%) while preserving per-crime atomicity
                crime_count += 1
                should_commit = (crime_count % batch_commit_size == 0)

                if should_commit:
                    batch_start = crime_count - batch_commit_size + 1
                    batch_end = crime_count
                    conn.commit()
                    commit_count += 1
                    logging.info(
                        f"Batch commit #{commit_count}: crimes {batch_start}-{batch_end}, "
                        f"total_rows_written={rows_written}"
                    )
                else:
                    # Use SAVEPOINT for per-crime rollback within batch
                    sp_name = f"sp_crime_{crime_id}"
                    try:
                        with conn.cursor() as cur:
                            cur.execute(f"SAVEPOINT {sp_name}")
                    except Exception:
                        pass  # Savepoint not critical, proceed without it

                return True, crime_id, branch
            except Exception as e:
                try:
                    # Try to rollback to savepoint first (per-crime rollback)
                    sp_name = f"sp_crime_{crime_id}"
                    try:
                        with conn.cursor() as cur:
                            cur.execute(f"ROLLBACK TO SAVEPOINT {sp_name}")
                    except Exception:
                        # Fallback to full transaction rollback
                        conn.rollback()

                    if unified_mode and run_id:
                        fail_crime_processing_run(conn, run_id, str(e))
                        # Don't commit failure marker - it will be in the batch commit
                except Exception as inner_e:
                    conn.rollback()
                    logging.error(f"Failed to handle error for Crime {crime_id}: {inner_e}")

                logging.error(f"Failed processing Crime {crime_id}: {e}")
                return False, crime_id, None
            # Connection automatically returned to pool via context manager

    # Final commit for any remaining work not yet committed
    final_conn = get_singleton_pool().get_connection()
    try:
        final_conn.commit()
        logging.info(f"Final commit completed. Total commits: {commit_count}")
    finally:
        get_singleton_pool().return_connection(final_conn)

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
    Shared dedup caches passed to all _resolve_canonical_identity calls so
    crime_profile and co-accused lookups are fetched once per crime, not once per accused.

    person_code logic by accused.type:
      - 'Accused' / 'CCL': person_code = accused_code (direct from DB)
      - 'Known' / 'Respondent' / 'Suspect': LLM assigns person_code (A1, A2 by mention)
    """
    # Process DB accused rows with a conservative same-crime duplicate collapse
    # for reordered-name identity duplicates (e.g. "Dharo Kumar" vs "Kumar Dharo").
    valid_accused, dropped_duplicates = _dedupe_same_crime_accused_rows(list(db_accused))
    if dropped_duplicates:
        logging.info(
            "Branch A duplicate-collapse for Crime %s: dropped=%s",
            crime_id,
            [
                {
                    'accused_id': r.get('accused_id'),
                    'accused_code': r.get('accused_code'),
                    'full_name': r.get('full_name'),
                }
                for r in dropped_duplicates
            ],
        )
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

    # Shared-role fallback: when the FIR describes a collective action but the
    # LLM assigns the role text to only a subset of accused codes, inherit the
    # dominant role for the crime so every accused gets classified consistently.
    shared_role_text, shared_role_key_details = compute_shared_role(roles_by_code)

    branch_records = []
    count = 0
    _cp_cache: dict = {}   # crime_profile cache — shared across all accused in this crime
    _ac_cache: dict = {}   # associate codes cache — shared across all accused in this crime
    _dedup_cache: dict = {}  # dedup candidate cache — shared across all accused in this crime
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

        # ---- Shared-role inheritance ----
        # If this accused has no role (or only a procedural/status note like
        # "41A Cr.P.C issued"), inherit the dominant crime-wide role so the
        # downstream classifier can assign accused_type.
        shared_role_applied = False
        if (not role_in_crime or _is_procedural_role(role_in_crime)) and shared_role_text:
            role_in_crime = shared_role_text
            if not key_details and shared_role_key_details:
                key_details = shared_role_key_details
            shared_role_applied = True

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
            source_summary['role_in_crime'] = 'LLM_SHARED' if shared_role_applied else 'LLM'
        if key_details:
            source_summary['key_details'] = 'LLM_SHARED' if shared_role_applied else 'LLM'
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
            _crime_profile_cache=_cp_cache,
            _assoc_cache=_ac_cache,
            _dedup_candidate_cache=_dedup_cache,
        )

        row_data = {
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
        }
        branch_records.append(row_data)
        count += 1

    # ── Branch A gap-fill: text-only accused not in DB ──────────────────────


    # FIRs sometimes name accused (suppliers, absconders, associates) who are
    # not registered in the accused/persons tables yet. Branch A would silently
    # drop them. We detect them via Pass 1 name extraction, diff against DB
    # names, and create LLM-sourced rows so they still appear in output.
    try:
        db_name_variants = []
        db_names_norm = set()
        for row in valid_accused:
            for name_variant in (row.get('full_name'), row.get('alias_name')):
                if not name_variant:
                    continue
                db_name_variants.append(str(name_variant))
                db_names_norm.add(_normalize_name(name_variant))

        text_names = extract_accused_names_pass1(facts_text)
        if text_names:
            new_names = []
            for raw in text_names:
                clean = clean_accused_name(raw)
                if not clean:
                    continue
                # Police guard: skip names found near police/official titles in text
                if _is_police_name(raw, facts_text) or _is_police_name(clean, facts_text):
                    logging.info(f"Branch A gap-fill: police guard dropped '{clean}'")
                    continue
                if _is_supplier_context(clean, facts_text):
                    logging.info(f"Branch A gap-fill: supplier guard dropped '{clean}'")
                    continue
                # Comprehensive DB guard: detect name variants (spelling, order, phonetic, partial)
                if _match_extracted_name_to_db_accused(clean, db_name_variants):
                    logging.info(f"Branch A gap-fill: DB guard matched '{clean}' to existing accused")
                    continue
                new_names.append(raw)

            if new_names:
                logging.info(
                    f"Branch A gap-fill: {len(new_names)} text-only accused "
                    f"found for Crime {crime_id}: {new_names}"
                )
                extra_details = extract_details_pass2(facts_text, new_names) or []
                detail_map_extra = {
                    d.full_name.lower().strip(): d for d in extra_details
                }
                for raw_name in new_names:
                    clean = clean_accused_name(raw_name)
                    d_obj = detail_map_extra.get(raw_name.lower().strip()) \
                        or detail_map_extra.get(clean.lower().strip())
                    if not d_obj:
                        # Partial name match fallback
                        toks = set(clean.lower().split())
                        for k, v in detail_map_extra.items():
                            if len(toks & set(k.split())) >= 2:
                                d_obj = v
                                break

                    role_desc = (d_obj.role_in_crime if d_obj else None) or None
                    key_details_extra = d_obj.key_details if d_obj else None
                    age_extra = d_obj.age if d_obj else None
                    gender_extra = d_obj.gender if d_obj else None
                    occupation_extra = d_obj.occupation if d_obj else None
                    address_extra = d_obj.address if d_obj else None
                    alias_extra = d_obj.alias_name if d_obj else None
                    phone_extra = d_obj.phone_numbers if d_obj else None

                    # Apply shared-role inheritance for text-only accused too
                    if (not role_desc or _is_procedural_role(role_desc)) and shared_role_text:
                        role_desc = shared_role_text
                        if not key_details_extra and shared_role_key_details:
                            key_details_extra = shared_role_key_details

                    synth_id = _synthetic_accused_id(crime_id, clean, None)
                    accused_type_extra = classify_accused_type(
                        role_desc + (" " + key_details_extra if key_details_extra else "")
                    ) if role_desc else None
                    if accused_type_extra == "unknown":
                        accused_type_extra = None

                    # Defensive fix: if name appears only in supplier context, override type to supplier
                    if _is_supplier_context(clean, facts_text):
                        accused_type_extra = "supplier"
                        logging.info(f"Branch A gap-fill: '{clean}' detected as supplier-only context, classified as supplier")

                        # Check for role-specific context that affects skip, status, or type
                        should_skip_role, status_override, type_override, reason = _should_skip_role_only_mention(clean, facts_text)
                        if should_skip_role:
                            logging.info(f"Branch A gap-fill: '{clean}' skipped due to {reason}")
                            continue
                        if status_override:
                            status_extra = status_override
                            logging.info(f"Branch A gap-fill: '{clean}' status set to {status_override} ({reason})")
                        if type_override:
                            accused_type_extra = type_override
                            logging.info(f"Branch A gap-fill: '{clean}' type set to {type_override} ({reason})")

                    gender_extra = detect_gender(facts_text, clean, gender_extra)
                    status_extra = resolve_status_for_insert(None, facts_text, clean)
                    is_ccl_extra = detect_ccl(clean, role_desc or "")

                    canonical_extra, dedup_conf_extra, dedup_tier_extra, dedup_flag_extra = \
                        _resolve_canonical_identity(
                            conn,
                            crime_id,
                            {
                                'accused_id': synth_id,
                                'person_code': None,
                                'full_name': clean,
                                'alias_name': alias_extra,
                                'age': age_extra,
                                'gender': gender_extra,
                                'address': address_extra,
                            },
                            ps_code,
                            _crime_profile_cache=_cp_cache,
                            _assoc_cache=_ac_cache,
                            _dedup_candidate_cache=_dedup_cache,
                        )

                    extra_row = {
                        'crime_id'             : crime_id,
                        'accused_id'           : synth_id,
                        'person_id'            : None,
                        'canonical_person_id'  : canonical_extra,
                        'person_code'          : None,
                        'seq_num'              : None,
                        'existing_accused'     : False,
                        'full_name'            : clean,
                        'alias_name'           : alias_extra,
                        'age'                  : age_extra,
                        'gender'               : gender_extra,
                        'occupation'           : occupation_extra,
                        'address'              : address_extra,
                        'phone_numbers'        : phone_extra,
                        'role_in_crime'        : role_desc,
                        'key_details'          : key_details_extra,
                        'accused_type'         : accused_type_extra,
                        'status'               : status_extra,
                        'is_ccl'               : is_ccl_extra,
                        'dedup_match_tier'     : dedup_tier_extra,
                        'dedup_confidence'     : dedup_conf_extra,
                        'dedup_review_flag'    : dedup_flag_extra,
                        'source_person_fields' : {},  # populated below
                        'source_accused_fields': {'ps_code': ps_code, 'accused_id': 'SYNTHETIC_GAP_FILL'},
                        'source_summary_fields': {
                            k: 'LLM' for k, v in [
                                ('role_in_crime', role_desc),
                                ('key_details', key_details_extra),
                                ('accused_type', accused_type_extra),
                            ] if v is not None
                        },
                        'etl_run_id'           : run_id,
                    }
                    # Fix source_person_fields using the actual values
                    extra_row['source_person_fields'] = {
                        k: 'LLM' for k, v in [
                            ('full_name', clean),
                            ('alias_name', alias_extra),
                            ('age', age_extra),
                            ('gender', gender_extra),
                            ('occupation', occupation_extra),
                            ('address', address_extra),
                            ('phone_numbers', phone_extra),
                        ] if v is not None
                    }
                    branch_records.append(extra_row)
                    insert_accused_facts(conn, extra_row)
                    count += 1
    except Exception as gap_err:
        logging.warning(f"Branch A gap-fill failed for Crime {crime_id}: {gap_err}", exc_info=True)

    dedup_tiers = {}
    for row in branch_records:
        tier = row.get('dedup_match_tier')
        dedup_tiers[tier] = dedup_tiers.get(tier, 0) + 1
    logging.info(f"Branch A Crime {crime_id}: dedup distribution={dedup_tiers}")

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
    _cp_cache: dict = {}
    _ac_cache: dict = {}
    _dedup_cache: dict = {}

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
        return 1, []

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
                _crime_profile_cache=_cp_cache,
                _assoc_cache=_ac_cache,
                _dedup_candidate_cache=_dedup_cache,
            )
            data['canonical_person_id'] = canonical_person_id
            data['dedup_confidence'] = dedup_confidence
            data['dedup_match_tier'] = dedup_match_tier
            data['dedup_review_flag'] = dedup_review_flag
            data['etl_run_id'] = run_id

            branch_records.append(data)
            insert_accused_facts(conn, data)
            count += 1

    # ── Branch B gap-fill: DB stubs LLM didn't extract ──────────────────────
    # LLM may not find every accused in text (uncommon names, implicit refs).
    # DB stubs that were never paired remain unwritten. Detect them via the
    # matched accused_ids collected above, then fill details from text via
    # Pass 2 for each missed stub's accused_code/name.
    try:
        matched_accused_ids = {
            r.get('accused_id')
            for r in branch_records
            if r.get('accused_id')
        }

        unmatched_stubs = [
            row for row in db_accused
            if row.get('accused_id') not in matched_accused_ids
        ]

        if unmatched_stubs:
            logging.info(
                f"Branch B gap-fill: {len(unmatched_stubs)} unmatched DB stubs "
                f"for Crime {crime_id}"
            )
            # Compute shared role from what was already extracted
            shared_b_role, shared_b_kd = compute_shared_role({
                r.get('person_code') or str(i): {
                    'role_in_crime': r.get('role_in_crime'),
                    'key_details': r.get('key_details'),
                }
                for i, r in enumerate(branch_records)
            })

            for stub in unmatched_stubs:
                stub_code    = stub.get('accused_code') or ''
                stub_name    = stub.get('full_name') or stub_code or 'Unknown'
                stub_id      = stub.get('accused_id')
                stub_seq     = stub.get('seq_num')
                stub_is_ccl  = stub.get('is_ccl', False)
                stub_status  = stub.get('accused_status')

                # Try Pass 2 for this specific stub to get any details in text
                stub_details_list = extract_details_pass2(facts_text, [stub_name]) or []
                d_obj = stub_details_list[0] if stub_details_list else None

                role_desc  = (d_obj.role_in_crime if d_obj else None) or None
                key_d      = d_obj.key_details if d_obj else None
                age_s      = d_obj.age if d_obj else None
                gender_s   = d_obj.gender if d_obj else None
                occ_s      = d_obj.occupation if d_obj else None
                addr_s     = d_obj.address if d_obj else None
                alias_s    = d_obj.alias_name if d_obj else None
                phone_s    = d_obj.phone_numbers if d_obj else None

                # Inherit shared role when Pass 2 found nothing useful
                if (not role_desc or _is_procedural_role(role_desc)) and shared_b_role:
                    role_desc = shared_b_role
                    if not key_d and shared_b_kd:
                        key_d = shared_b_kd

                accused_type_s = classify_accused_type(
                    role_desc + (" " + key_d if key_d else "")
                ) if role_desc else None
                if accused_type_s == 'unknown':
                    accused_type_s = None

                gender_s  = detect_gender(facts_text, stub_name, gender_s)
                status_s  = resolve_status_for_insert(stub_status, facts_text, stub_name)
                is_ccl_s  = bool(stub_is_ccl) or detect_ccl(stub_name, role_desc or '')

                synth_id = stub_id or _synthetic_accused_id(crime_id, stub_name, stub_seq)

                canonical_s, dedup_conf_s, dedup_tier_s, dedup_flag_s = \
                    _resolve_canonical_identity(
                        conn,
                        crime_id,
                        {
                            'accused_id': synth_id,
                            'person_code': stub_code or None,
                            'full_name': stub_name,
                            'alias_name': alias_s,
                            'age': age_s,
                            'gender': gender_s,
                            'address': addr_s,
                        },
                        ps_code,
                        _crime_profile_cache=_cp_cache,
                        _assoc_cache=_ac_cache,
                        _dedup_candidate_cache=_dedup_cache,
                    )

                stub_row = {
                    'crime_id'             : crime_id,
                    'accused_id'           : synth_id,
                    'person_id'            : None,
                    'canonical_person_id'  : canonical_s,
                    'person_code'          : stub_code or None,
                    'seq_num'              : stub_seq,
                    'existing_accused'     : False,
                    'full_name'            : stub_name,
                    'alias_name'           : alias_s,
                    'age'                  : age_s,
                    'gender'               : gender_s,
                    'occupation'           : occ_s,
                    'address'              : addr_s,
                    'phone_numbers'        : phone_s,
                    'role_in_crime'        : role_desc,
                    'key_details'          : key_d,
                    'accused_type'         : accused_type_s,
                    'status'               : status_s,
                    'is_ccl'               : is_ccl_s,
                    'dedup_match_tier'     : dedup_tier_s,
                    'dedup_confidence'     : dedup_conf_s,
                    'dedup_review_flag'    : dedup_flag_s,
                    'source_person_fields' : {
                        k: 'LLM' for k, v in [
                            ('full_name', stub_name), ('alias_name', alias_s),
                            ('age', age_s), ('gender', gender_s),
                            ('occupation', occ_s), ('address', addr_s),
                            ('phone_numbers', phone_s),
                        ] if v is not None
                    },
                    'source_accused_fields': {
                        'accused_id': 'DB_STUB_GAP_FILL',
                        'person_code': stub_code or 'UNKNOWN',
                        'ps_code': ps_code,
                    },
                    'source_summary_fields': {
                        k: 'LLM' for k, v in [
                            ('role_in_crime', role_desc),
                            ('key_details', key_d),
                            ('accused_type', accused_type_s),
                        ] if v is not None
                    },
                    'etl_run_id'           : run_id,
                }
                branch_records.append(stub_row)
                insert_accused_facts(conn, stub_row)
                count += 1

        # Gap-fill wrote real accused — delete stale NO_ACCUSED_IN_TEXT sentinel
        # that was inserted earlier when LLM returned empty. Leaving it causes
        # a ghost row with no identity alongside real accused rows.
        if unmatched_stubs and branch_records:
            with conn.cursor() as _cur:
                _cur.execute(
                    "DELETE FROM public.brief_facts_ai "
                    "WHERE crime_id = %s AND role_in_crime = 'NO_ACCUSED_IN_TEXT' AND accused_id IS NULL",
                    (crime_id,),
                )
            count = max(0, count - 1)  # sentinel no longer in final count

    except Exception as gap_b_err:
        logging.warning(
            f"Branch B gap-fill failed for Crime {crime_id}: {gap_b_err}",
            exc_info=True
        )

    dedup_tiers = {}
    for row in branch_records:
        tier = row.get('dedup_match_tier')
        dedup_tiers[tier] = dedup_tiers.get(tier, 0) + 1
    logging.info(f"Branch B Crime {crime_id}: dedup distribution={dedup_tiers}")
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
        return 1, []

    branch_records = []
    count = 0
    _cp_cache: dict = {}
    _ac_cache: dict = {}
    _dedup_cache: dict = {}

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
                _crime_profile_cache=_cp_cache,
                _assoc_cache=_ac_cache,
                _dedup_candidate_cache=_dedup_cache,
            )
            data['canonical_person_id'] = canonical_person_id
            data['dedup_confidence'] = dedup_confidence
            data['dedup_match_tier'] = dedup_match_tier
            data['dedup_review_flag'] = dedup_review_flag

            branch_records.append(data)
            insert_accused_facts(conn, data)
            count += 1

    dedup_tiers = {}
    for row in branch_records:
        tier = row.get('dedup_match_tier')
        dedup_tiers[tier] = dedup_tiers.get(tier, 0) + 1
    logging.info(f"Branch C Crime {crime_id}: dedup distribution={dedup_tiers}")

    logging.info(f"Branch C processed Crime {crime_id}. row_count={count}")
    return count, branch_records


if __name__ == "__main__":
    main()
