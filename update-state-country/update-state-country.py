#!/usr/bin/env python3
"""
Address State & Country Determination Script for DOPAMAS

Pipeline (3 steps):
  1.  LLM EXTRACTION — Ask the LLM to read the raw permanent_* address
      fields and extract ONLY the place names (village, town, mandal,
      district).  The LLM must NOT guess state or country.
  2.  pg_trgm MATCHING — Fuzzy-match the extracted names against the
      geo_reference table (villages → sub-districts → districts → states).
      If a match is found, permanent_state_ut and permanent_country are
      filled from the geo_reference row (country = "India").
  3.  FOREIGN FALLBACK — If pg_trgm finds no Indian match, check
      ref.txt (foreign-only reference) and, as a last resort, ask the
      LLM to classify state + country.

Rules per record:
  - Both state & country already set → skip
  - State set, country missing      → determine country only
  - State missing                    → full pipeline (extract → match → fill)
  - All permanent_* fields NULL      → set state & country to NULL

Data sources:
  - READ:  persons.permanent_* columns
  - MATCH: geo_reference table (pg_trgm GIN indexes)
  - REF:   ref.txt  (foreign countries only — India removed)
  - WRITE: persons.permanent_state_ut, persons.permanent_country
"""

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List, Tuple

import psycopg

# Ensure core is accessible
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from core.llm_service import get_llm

from dotenv import load_dotenv

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# --- Configuration ---
load_dotenv()

DB_DSN = os.environ.get(
    "DB_DSN",
    " ".join([
        f"dbname={os.environ.get('DB_NAME', 'dopamas')}",
        f"user={os.environ.get('DB_USER')}",
        f"password={os.environ.get('DB_PASSWORD')}",
        f"host={os.environ.get('DB_HOST', 'localhost')}",
        f"port={os.environ.get('DB_PORT', '5432')}",
    ])
)

REF_DATA_FILE = os.environ.get("REF_DATA_FILE", "ref.txt")
DEFAULT_TABLE_NAME = os.environ.get("TABLE_NAME", "persons")
DEFAULT_ID_COLUMN = os.environ.get("ID_COLUMN", "id")

# pg_trgm similarity threshold (0.0 – 1.0).  Lower = more lenient.
TRGM_SIMILARITY_THRESHOLD = float(os.environ.get("TRGM_SIMILARITY_THRESHOLD", "0.35"))

logger.info(f"Database: {os.environ.get('DB_NAME', 'dopamas')}@{os.environ.get('DB_HOST', 'localhost')}")
logger.info(f"Default Table: {DEFAULT_TABLE_NAME}, ID Column: {DEFAULT_ID_COLUMN}")
logger.info(f"Reference Data File (foreign): {REF_DATA_FILE}")
logger.info(f"pg_trgm similarity threshold: {TRGM_SIMILARITY_THRESHOLD}")


# ===================================================================
#  DATA MODELS
# ===================================================================

@dataclass
class AddressRecord:
    """One row from the persons table."""
    record_id: int
    # --- input fields (read-only) ---
    permanent_house_no: Optional[str] = None
    permanent_street_road_no: Optional[str] = None
    permanent_ward_colony: Optional[str] = None
    permanent_landmark_milestone: Optional[str] = None
    permanent_locality_village: Optional[str] = None
    permanent_area_mandal: Optional[str] = None
    permanent_district: Optional[str] = None
    # --- output fields (to be updated) ---
    permanent_state_ut: Optional[str] = None
    permanent_country: Optional[str] = None

    def get_address_components(self) -> str:
        """Formatted non-empty permanent_* values."""
        parts = [
            ("House No", self.permanent_house_no),
            ("Street/Road", self.permanent_street_road_no),
            ("Ward/Colony", self.permanent_ward_colony),
            ("Landmark", self.permanent_landmark_milestone),
            ("Locality/Village", self.permanent_locality_village),
            ("Area/Mandal", self.permanent_area_mandal),
            ("District", self.permanent_district),
        ]
        pieces = [f"{l}: {v.strip()}" for l, v in parts if v and v.strip()]
        return ", ".join(pieces) if pieces else ""

    # --- convenience flags ---
    def needs_state(self) -> bool:
        return not (self.permanent_state_ut and self.permanent_state_ut.strip())

    def needs_country(self) -> bool:
        return not (self.permanent_country and self.permanent_country.strip())

    def is_complete(self) -> bool:
        return not self.needs_state() and not self.needs_country()

    def has_any_address(self) -> bool:
        return any(
            f and f.strip()
            for f in [
                self.permanent_house_no, self.permanent_street_road_no,
                self.permanent_ward_colony, self.permanent_landmark_milestone,
                self.permanent_locality_village, self.permanent_area_mandal,
                self.permanent_district,
            ]
        )


@dataclass
class GeoMatch:
    """Result of a geo_reference pg_trgm lookup."""
    village: Optional[str] = None
    sub_district: Optional[str] = None
    district: Optional[str] = None
    state: Optional[str] = None
    similarity: float = 0.0


@dataclass
class LocationResult:
    state: Optional[str] = None
    country: Optional[str] = None
    confidence: str = "low"
    reasoning: str = ""


# ===================================================================
#  STEP 1 — LLM EXTRACTION  (extract place names, no guessing)
# ===================================================================

class LocationExtractor:
    """Ask the LLM to pull raw place names out of address text."""

    def __init__(self):
        self.llm = get_llm("classification")

    def extract(self, address_text: str) -> List[str]:
        """Return a list of extracted location names (village, town, mandal, district)."""
        if not address_text or not address_text.strip():
            return []

        prompt = (
            "Read the following raw address text. "
            "Extract ONLY the names of the village, town, mandal, or district mentioned. "
            "Do NOT guess the state or country. "
            "Return a JSON object with a single key:\n"
            '{"extracted_locations": ["Location1", "Location2", ...]}\n\n'
            "If no identifiable location name is found, return:\n"
            '{"extracted_locations": []}\n\n'
            f"Address text:\n{address_text}\n\n"
            "Return ONLY the JSON object, no other text."
        )

        try:
            raw = self.llm.generate(prompt=prompt)
            if not raw:
                return []
            data = _extract_json(raw)
            locations = data.get("extracted_locations", [])
            # Normalise: accept a single string as well as a list
            if isinstance(locations, str):
                locations = [locations]
            return [loc.strip() for loc in locations if loc and loc.strip()]
        except Exception as e:
            logger.error(f"LLM extraction failed: {e}")
            return []


# ===================================================================
#  STEP 2 — pg_trgm MATCHING against geo_reference
# ===================================================================

def trgm_match_locations(
    extracted_locations: List[str],
    threshold: float = TRGM_SIMILARITY_THRESHOLD,
) -> Optional[GeoMatch]:
    """
    Try each extracted location against geo_reference using pg_trgm.
    Search order: village → sub_district → district.
    Returns the best match (highest similarity) or None.
    """
    if not extracted_locations:
        return None

    best: Optional[GeoMatch] = None

    # Build a UNION query that checks all columns in one round-trip per location
    query = """
        WITH candidates AS (
            SELECT
                village_name_english,
                sub_district_name,
                district_name,
                state_name,
                GREATEST(
                    similarity(village_name_english, %(loc)s),
                    similarity(sub_district_name,   %(loc)s),
                    similarity(district_name,        %(loc)s)
                ) AS sim
            FROM geo_reference
            WHERE village_name_english  %% %(loc)s
               OR sub_district_name     %% %(loc)s
               OR district_name         %% %(loc)s
        )
        SELECT village_name_english, sub_district_name, district_name,
               state_name, sim
        FROM candidates
        ORDER BY sim DESC
        LIMIT 1;
    """

    try:
        with psycopg.connect(DB_DSN) as conn:
            # Set the similarity threshold for this session
            with conn.cursor() as cur:
                cur.execute(
                    "SET pg_trgm.similarity_threshold = %s;",
                    (threshold,),
                )

                for loc in extracted_locations:
                    loc_clean = loc.strip()
                    if not loc_clean:
                        continue
                    cur.execute(query, {"loc": loc_clean})
                    row = cur.fetchone()
                    if row:
                        match = GeoMatch(
                            village=row[0],
                            sub_district=row[1],
                            district=row[2],
                            state=row[3],
                            similarity=float(row[4]),
                        )
                        if best is None or match.similarity > best.similarity:
                            best = match
    except Exception as e:
        logger.error(f"pg_trgm lookup failed: {e}", exc_info=True)

    if best:
        logger.info(
            f"  pg_trgm best match: state={best.state}, district={best.district}, "
            f"sim={best.similarity:.3f}"
        )
    return best


# ===================================================================
#  STEP 3 — FOREIGN REFERENCE FALLBACK  (ref.txt + LLM classify)
# ===================================================================

def parse_foreign_reference(file_path: str) -> Dict[str, Dict[str, List[str]]]:
    """Parse ref.txt → { country: { states: [...], cities: [...] } }"""
    ref: Dict[str, Dict[str, List[str]]] = {}
    current_country: Optional[str] = None

    if not os.path.exists(file_path):
        logger.warning(f"Reference file not found: {file_path}")
        return ref

    try:
        with open(file_path, "r", encoding="utf-8") as fh:
            for raw_line in fh:
                line = raw_line.strip()
                if not line or line.startswith("#"):
                    continue
                if "(country):" in line:
                    current_country = line.split("(country):")[0].strip()
                    ref[current_country] = {"states": [], "cities": []}
                elif current_country:
                    entry = line.split("(")[0].strip() if "(" in line else line
                    is_city = "(city)" in line.lower()
                    if is_city:
                        ref[current_country]["cities"].append(entry)
                    else:
                        ref[current_country]["states"].append(entry)
        logger.info(f"Loaded foreign ref data: {len(ref)} countries")
    except Exception as e:
        logger.error(f"Failed to parse ref.txt: {e}")
    return ref


def _norm(text: str) -> str:
    return " ".join((text or "").lower().split())


def lookup_foreign(
    extracted_locations: List[str],
    existing_state: Optional[str],
    ref_data: Dict[str, Dict[str, List[str]]],
) -> Optional[Tuple[Optional[str], str]]:
    """
    Try to match locations or existing_state against foreign ref data.
    Returns (state_or_None, country) or None.
    """
    if not ref_data:
        return None

    # Check existing state against foreign refs
    if existing_state:
        norm_st = _norm(existing_state)
        for country, data in ref_data.items():
            for s in data["states"]:
                if _norm(s) == norm_st or _norm(s) in norm_st or norm_st in _norm(s):
                    if len(_norm(s)) > 3 and len(norm_st) > 3:
                        return (existing_state, country)
            for c in data["cities"]:
                if _norm(c) == norm_st:
                    return (existing_state, country)

    # Check extracted locations
    for loc in extracted_locations:
        norm_loc = _norm(loc)
        if not norm_loc:
            continue
        for country, data in ref_data.items():
            for s in data["states"]:
                if _norm(s) == norm_loc or _norm(s) in norm_loc or norm_loc in _norm(s):
                    if len(_norm(s)) > 3 and len(norm_loc) > 3:
                        return (s, country)
            for c in data["cities"]:
                if _norm(c) == norm_loc:
                    return (None, country)
    return None


def llm_classify_state_country(address_text: str, existing_state: Optional[str] = None) -> LocationResult:
    """
    Last-resort: ask the LLM to determine state + country when both
    pg_trgm and ref.txt failed.
    """
    llm = get_llm("classification")

    if existing_state and existing_state.strip():
        prompt = (
            "You are an expert in world geography.\n"
            f"The address has State/UT: {existing_state}\n"
            f"Full address: {address_text}\n\n"
            "Determine the COUNTRY for this state/address.\n"
            "Return ONLY a JSON object:\n"
            '{"state": "' + existing_state + '", "country": "...", '
            '"confidence": "high/medium/low", "reasoning": "..."}'
        )
    else:
        prompt = (
            "You are an expert in Indian and international geography.\n"
            "Determine the STATE/UT and COUNTRY from this address:\n\n"
            f"{address_text}\n\n"
            "Return ONLY a JSON object:\n"
            '{"state": "...", "country": "...", '
            '"confidence": "high/medium/low", "reasoning": "..."}'
        )

    try:
        raw = llm.generate(prompt=prompt)
        data = _extract_json(raw or "")
        return LocationResult(
            state=data.get("state"),
            country=data.get("country"),
            confidence=data.get("confidence", "low"),
            reasoning=data.get("reasoning", ""),
        )
    except Exception as e:
        logger.error(f"LLM classify failed: {e}")
        return LocationResult(reasoning=f"LLM error: {e}")


# ===================================================================
#  JSON HELPER
# ===================================================================

def _extract_json(text: str) -> Dict[str, Any]:
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    start, end = text.find("{"), text.rfind("}")
    if start != -1 and end != -1:
        try:
            return json.loads(text[start : end + 1])
        except json.JSONDecodeError:
            pass
    logger.warning(f"Could not parse JSON from: {text[:200]}")
    return {}


# ===================================================================
#  DATABASE OPERATIONS
# ===================================================================

def fetch_records_needing_update(
    table_name: str,
    id_column: str,
    limit: Optional[int] = None,
    update_state: bool = True,
    update_country: bool = True,
) -> List[AddressRecord]:
    """Fetch persons rows where state or country is missing."""
    conds = []
    if update_state:
        conds.append("(permanent_state_ut IS NULL OR permanent_state_ut = '')")
    if update_country:
        conds.append("(permanent_country IS NULL OR permanent_country = '')")
    if not conds:
        return []

    where = (
        f"({' OR '.join(conds)}) "
        "AND NOT (permanent_state_ut IS NOT NULL AND permanent_state_ut != '' "
        "AND permanent_country IS NOT NULL AND permanent_country != '')"
    )

    sql = f"""
        SELECT {id_column},
               permanent_house_no, permanent_street_road_no,
               permanent_ward_colony, permanent_landmark_milestone,
               permanent_locality_village, permanent_area_mandal,
               permanent_district,
               permanent_state_ut, permanent_country
        FROM {table_name}
        WHERE {where}
        ORDER BY {id_column}
    """
    params: tuple = ()
    if limit is not None:
        sql += " LIMIT %s"
        params = (limit,)
        logger.info(f"Fetching up to {limit} records from {table_name}")
    else:
        logger.info(f"Fetching ALL records from {table_name}")

    with psycopg.connect(DB_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

    records = [
        AddressRecord(
            record_id=r[0],
            permanent_house_no=r[1],
            permanent_street_road_no=r[2],
            permanent_ward_colony=r[3],
            permanent_landmark_milestone=r[4],
            permanent_locality_village=r[5],
            permanent_area_mandal=r[6],
            permanent_district=r[7],
            permanent_state_ut=r[8],
            permanent_country=r[9],
        )
        for r in rows
    ]
    logger.info(f"Fetched {len(records)} records needing updates")
    return records


def update_location(
    table_name: str,
    id_column: str,
    record_id: int,
    state: Optional[str],
    country: Optional[str],
    update_state_field: bool = True,
    update_country_field: bool = True,
) -> None:
    """Write permanent_state_ut and/or permanent_country."""
    updates, params = [], []

    if update_state_field:
        val = state.strip() if state and state.strip() else None
        if val is not None:
            updates.append("permanent_state_ut = %s"); params.append(val)
        else:
            updates.append("permanent_state_ut = NULL")

    if update_country_field:
        val = country.strip() if country and country.strip() else None
        if val is not None:
            updates.append("permanent_country = %s"); params.append(val)
        else:
            updates.append("permanent_country = NULL")

    if not updates:
        return

    params.append(record_id)
    sql = f"UPDATE {table_name} SET {', '.join(updates)} WHERE {id_column} = %s"

    with psycopg.connect(DB_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()
    logger.info(f"Updated {table_name}.{id_column}={record_id}: state={state}, country={country}")


# ===================================================================
#  MAIN PIPELINE
# ===================================================================

def process_records(
    table_name: str,
    id_column: str,
    limit: Optional[int] = None,
    dry_run: bool = False,
    update_state: bool = True,
    update_country: bool = True,
) -> None:
    logger.info("=" * 80)
    logger.info("Address State & Country Pipeline  (pg_trgm + LLM)")
    logger.info("=" * 80)
    logger.info(f"Table: {table_name}  |  ID: {id_column}  |  Limit: {limit or 'ALL'}")
    logger.info(f"Dry run: {dry_run}  |  Update state: {update_state}  |  Update country: {update_country}")
    logger.info("=" * 80)

    # Load foreign reference
    ref_data = parse_foreign_reference(REF_DATA_FILE)

    # LLM extractor (step 1)
    extractor = LocationExtractor()

    # Fetch records
    records = fetch_records_needing_update(
        table_name, id_column, limit, update_state, update_country
    )
    if not records:
        logger.info("No records need updates.")
        return

    total = len(records)
    stats = {"ok": 0, "fail": 0, "skip": 0, "geo": 0, "ref": 0, "llm": 0, "null": 0}

    for idx, rec in enumerate(records, 1):
        logger.info("-" * 80)
        logger.info(f"[{idx}/{total}] ID={rec.record_id}  state={rec.permanent_state_ut}  country={rec.permanent_country}")
        addr_text = rec.get_address_components()
        logger.info(f"  Address: {addr_text or '(empty)'}")

        # --- already complete ---
        if rec.is_complete():
            logger.info("  Skip: both state & country already set")
            stats["skip"] += 1
            continue

        try:
            state_out: Optional[str] = None
            country_out: Optional[str] = None
            source = "none"

            # === RULE: all address fields NULL → set both to NULL ===
            if not rec.has_any_address():
                logger.info("  No address info → setting state & country to NULL")
                state_out, country_out, source = None, None, "null"
                stats["null"] += 1

            # === RULE: state exists, only country missing ===
            elif not rec.needs_state() and rec.needs_country():
                logger.info(f"  State exists ({rec.permanent_state_ut}), determining country only")

                # 1) foreign ref check
                foreign = lookup_foreign([], rec.permanent_state_ut, ref_data)
                if foreign:
                    _, country_out = foreign
                    source = "ref"
                    stats["ref"] += 1
                else:
                    # 2) pg_trgm — see if the state itself matches an Indian state
                    geo = trgm_match_locations([rec.permanent_state_ut], TRGM_SIMILARITY_THRESHOLD)
                    if geo and geo.state:
                        country_out = "India"
                        source = "geo"
                        stats["geo"] += 1
                    else:
                        # 3) LLM classify
                        res = llm_classify_state_country(addr_text, rec.permanent_state_ut)
                        country_out = res.country
                        source = "llm"
                        stats["llm"] += 1

                state_out = None  # do NOT touch state

            # === RULE: state missing → full pipeline ===
            else:
                # --- Step 1: LLM extract location names ---
                logger.info("  Step 1: LLM extracting location names …")
                extracted = extractor.extract(addr_text)
                logger.info(f"    extracted_locations = {extracted}")

                # Also include raw field values as candidates
                raw_candidates = [
                    rec.permanent_locality_village,
                    rec.permanent_area_mandal,
                    rec.permanent_district,
                ]
                all_candidates = list(
                    dict.fromkeys(  # deduplicate, preserve order
                        [c.strip() for c in (extracted + [r for r in raw_candidates if r]) if c and c.strip()]
                    )
                )
                logger.info(f"    all_candidates = {all_candidates}")

                # --- Step 2: pg_trgm against geo_reference ---
                logger.info("  Step 2: pg_trgm matching …")
                geo = trgm_match_locations(all_candidates, TRGM_SIMILARITY_THRESHOLD)

                if geo and geo.state:
                    state_out = geo.state
                    country_out = "India"
                    source = "geo"
                    stats["geo"] += 1
                    logger.info(f"    Matched via geo_reference: state={geo.state}, district={geo.district}, sim={geo.similarity:.3f}")
                else:
                    # --- Step 3a: foreign ref.txt ---
                    logger.info("  Step 3: foreign ref lookup …")
                    foreign = lookup_foreign(all_candidates, None, ref_data)
                    if foreign:
                        state_out, country_out = foreign
                        source = "ref"
                        stats["ref"] += 1
                        logger.info(f"    Matched via ref.txt: state={state_out}, country={country_out}")
                    else:
                        # --- Step 3b: LLM classify (last resort) ---
                        logger.info("  Step 3b: LLM classify (last resort) …")
                        res = llm_classify_state_country(addr_text)
                        state_out = res.state
                        country_out = res.country
                        source = "llm"
                        stats["llm"] += 1
                        logger.info(f"    LLM result: state={state_out}, country={country_out}, conf={res.confidence}")

            # --- WRITE ---
            logger.info(f"  Result ({source}): state={state_out}, country={country_out}")

            if dry_run:
                logger.info("  [DRY RUN] — not writing")
                stats["ok"] += 1
                continue

            # Determine what to write
            write_state = rec.needs_state() and (state_out is not None or not rec.has_any_address())
            write_country = rec.needs_country() and (country_out is not None or not rec.has_any_address())

            if write_state or write_country:
                update_location(
                    table_name, id_column, rec.record_id,
                    state=state_out if write_state else None,
                    country=country_out if write_country else None,
                    update_state_field=write_state,
                    update_country_field=write_country,
                )
                stats["ok"] += 1
            else:
                logger.warning(f"  No updates applicable for ID={rec.record_id}")
                stats["skip"] += 1

        except Exception as e:
            logger.error(f"  FAILED ID={rec.record_id}: {e}", exc_info=True)
            stats["fail"] += 1

    # --- Summary ---
    logger.info("=" * 80)
    logger.info("Pipeline Summary")
    logger.info(f"  Total:         {total}")
    logger.info(f"  Updated:       {stats['ok']}")
    logger.info(f"  Skipped:       {stats['skip']}")
    logger.info(f"  Failed:        {stats['fail']}")
    logger.info(f"  via geo_ref:   {stats['geo']}")
    logger.info(f"  via ref.txt:   {stats['ref']}")
    logger.info(f"  via LLM:       {stats['llm']}")
    logger.info(f"  set to NULL:   {stats['null']}")
    logger.info("=" * 80)


# ===================================================================
#  CLI
# ===================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Determine state/country from address via pg_trgm + LLM"
    )
    parser.add_argument("--table", default=DEFAULT_TABLE_NAME)
    parser.add_argument("--id-column", default=DEFAULT_ID_COLUMN)
    parser.add_argument("--limit", type=int, default=None,
                        help="Max records (default: all)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-state", action="store_true")
    parser.add_argument("--skip-country", action="store_true")
    parser.add_argument("--trgm-threshold", type=float, default=TRGM_SIMILARITY_THRESHOLD,
                        help=f"pg_trgm similarity threshold (default {TRGM_SIMILARITY_THRESHOLD})")

    args = parser.parse_args()

    # Update module-level threshold from CLI arg
    _mod = sys.modules[__name__]
    _mod.TRGM_SIMILARITY_THRESHOLD = args.trgm_threshold

    process_records(
        table_name=args.table,
        id_column=args.id_column,
        limit=args.limit,
        dry_run=args.dry_run,
        update_state=not args.skip_state,
        update_country=not args.skip_country,
    )


if __name__ == "__main__":
    main()
