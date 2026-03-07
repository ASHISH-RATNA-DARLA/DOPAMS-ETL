#!/usr/bin/env python3
"""
Address State & Country Determination Script for DOPAMAS

Pipeline (2 steps — no LLM required):
  1.  pg_trgm MATCHING — Use the structured permanent_* columns
      (locality_village, area_mandal, district) directly as candidates
      and fuzzy-match against the geo_reference table.
      If a match is found, any missing fields among permanent_district,
      permanent_state_ut, and permanent_country are filled from the
      geo_reference row (country = "India").
  2.  FOREIGN FALLBACK — If pg_trgm finds no Indian match, fuzzy-match
      against the geo_countries table (foreign countries/states).

Rules per record:
  - District, state & country all set → skip
  - Any of district/state/country missing → pg_trgm match → fill gaps
  - All permanent_* fields NULL → set district, state & country to NULL

Data sources:
  - READ:  persons.permanent_* columns (already structured/mapped)
  - MATCH: geo_reference table (pg_trgm GIN indexes) for Indian locations
  - MATCH: geo_countries table (pg_trgm GIN indexes) for foreign locations
  - WRITE: persons.permanent_district, permanent_state_ut, permanent_country
"""

import argparse
import logging
import os
import sys
import time
import re
from dataclasses import dataclass
from typing import Optional, Dict, List, Tuple
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import psycopg2
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_pooling import PostgreSQLConnectionPool

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

DEFAULT_TABLE_NAME = os.environ.get("TABLE_NAME", "persons")
DEFAULT_ID_COLUMN = os.environ.get("ID_COLUMN", "person_id")

# pg_trgm similarity thresholds (tiered by field trust level)
TRGM_THRESHOLD_STATE    = float(os.environ.get("TRGM_THRESHOLD_STATE",    "0.90"))  # States are a small finite list
TRGM_THRESHOLD_DISTRICT = float(os.environ.get("TRGM_THRESHOLD_DISTRICT", "0.90"))  # Districts are finite, high confidence needed
TRGM_THRESHOLD_MANDAL   = float(os.environ.get("TRGM_THRESHOLD_MANDAL",   "0.70"))  # Mandals have spelling variations
TRGM_THRESHOLD_VILLAGE  = float(os.environ.get("TRGM_THRESHOLD_VILLAGE",  "0.65"))  # Villages have high noise, acceptable if district/mandal anchor
TRGM_THRESHOLD_FOREIGN  = float(os.environ.get("TRGM_THRESHOLD_FOREIGN",  "0.50"))  # Foreign names need lower threshold for transliteration variations

logger.info(f"Database: {os.environ.get('DB_NAME', 'dopamas')}@{os.environ.get('DB_HOST', 'localhost')}")
logger.info(f"Default Table: {DEFAULT_TABLE_NAME}, ID Column: {DEFAULT_ID_COLUMN}")
logger.info(f"pg_trgm thresholds: state={TRGM_THRESHOLD_STATE}, district={TRGM_THRESHOLD_DISTRICT}, mandal={TRGM_THRESHOLD_MANDAL}, village={TRGM_THRESHOLD_VILLAGE}, foreign={TRGM_THRESHOLD_FOREIGN}")


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
    def needs_district(self) -> bool:
        return not (self.permanent_district and self.permanent_district.strip())

    def needs_state(self) -> bool:
        return not (self.permanent_state_ut and self.permanent_state_ut.strip())

    def needs_country(self) -> bool:
        return not (self.permanent_country and self.permanent_country.strip())

    def is_complete(self) -> bool:
        return not self.needs_district() and not self.needs_state() and not self.needs_country()

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

    def get_extended_candidates(self) -> List[str]:
        """Extract potential location tokens from ALL address fields (not just village/mandal)."""
        all_fields = [
            self.permanent_ward_colony, self.permanent_landmark_milestone,
            self.permanent_street_road_no, self.permanent_house_no,
            self.permanent_locality_village, self.permanent_area_mandal,
        ]
        tokens = []
        for field in all_fields:
            if field and field.strip():
                tokens.extend(_extract_location_tokens(field))
        seen = set()
        result = []
        for t in tokens:
            norm = t.lower()
            if norm not in seen:
                seen.add(norm)
                result.append(t)
        return result

    def get_full_address_text(self) -> str:
        """Return all address field values concatenated."""
        parts = [
            self.permanent_house_no, self.permanent_street_road_no,
            self.permanent_ward_colony, self.permanent_landmark_milestone,
            self.permanent_locality_village, self.permanent_area_mandal,
            self.permanent_district, self.permanent_state_ut,
        ]
        return " ".join(p.strip() for p in parts if p and p.strip())


@dataclass
class GeoMatch:
    """Result of a geo_reference pg_trgm lookup."""
    village: Optional[str] = None
    sub_district: Optional[str] = None
    district: Optional[str] = None
    state: Optional[str] = None
    similarity: float = 0.0


# ===================================================================
#  STEP 1 — pg_trgm MATCHING against geo_reference
#
#  Hierarchy (highest trust first):
#    1a. State exists, country missing → match state_name → get country.
#    1b. District known → match district_name → get state.
#    1c. Village/mandal known → match (scoped to district if known).
#
#  Tiered thresholds: state/district >= 90%, mandal >= 70%, village >= 65%.
# ===================================================================

def trgm_match_state(
    state: str,
    threshold: float = TRGM_THRESHOLD_STATE,
) -> Optional[GeoMatch]:
    """
    Match a known state value directly against state_name in geo_reference.
    Returns the best match or None. Used to confirm Indian state → set country.
    """
    query = """
        SELECT DISTINCT state_name,
               similarity(state_name, %(loc)s) AS sim
        FROM geo_reference
        WHERE state_name %% %(loc)s
        ORDER BY sim DESC
        LIMIT 1;
    """
    try:
        pool = PostgreSQLConnectionPool()
        with pool.get_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET pg_trgm.similarity_threshold = {threshold};")
                cur.execute(query, {"loc": state.strip()})
                row = cur.fetchone()
                if row and float(row[1]) >= threshold:
                    match = GeoMatch(
                        state=row[0], similarity=float(row[1]),
                    )
                    logger.info(
                        f"  pg_trgm state match: state={match.state}, "
                        f"sim={match.similarity:.3f}"
                    )
                    return match
    except Exception as e:
        logger.error(f"pg_trgm state lookup failed: {e}", exc_info=True)
    return None


def trgm_match_district(
    district: str,
    threshold: float = TRGM_THRESHOLD_DISTRICT,
) -> Optional[GeoMatch]:
    """
    Match a known district value directly against district_name in geo_reference.
    Returns the best match or None.
    """
    query = """
        SELECT DISTINCT district_name, state_name,
               similarity(district_name, %(loc)s) AS sim
        FROM geo_reference
        WHERE district_name %% %(loc)s
        ORDER BY sim DESC
        LIMIT 1;
    """
    try:
        pool = PostgreSQLConnectionPool()
        with pool.get_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET pg_trgm.similarity_threshold = {threshold};")
                cur.execute(query, {"loc": district.strip()})
                row = cur.fetchone()
                if row and float(row[2]) >= threshold:
                    match = GeoMatch(
                        district=row[0], state=row[1], similarity=float(row[2]),
                    )
                    logger.info(
                        f"  pg_trgm district match: district={match.district}, "
                        f"state={match.state}, sim={match.similarity:.3f}"
                    )
                    return match
                elif row:
                    logger.info(
                        f"  pg_trgm district match rejected (sim={float(row[2]):.3f} < {threshold})"
                    )
    except Exception as e:
        logger.error(f"pg_trgm district lookup failed: {e}", exc_info=True)
    return None


def trgm_match_village_mandal(
    candidates: List[str],
    within_district: Optional[str] = None,
    mandal_threshold: float = TRGM_THRESHOLD_MANDAL,
    village_threshold: float = TRGM_THRESHOLD_VILLAGE,
) -> Optional[GeoMatch]:
    """
    Match village/mandal names against geo_reference.
    Uses mandal_threshold (0.70) for sub_district matches and
    village_threshold (0.65) for village matches.
    If within_district is provided, results are scoped to that district only
    (prevents cross-state jumps like Kotapalle/Mancherial → Chittoor).
    """
    if not candidates:
        return None

    best: Optional[GeoMatch] = None

    # Use the lower threshold (village) for the pg_trgm operator so candidates
    # are not filtered out prematurely. We apply the tiered check in Python.
    pg_threshold = min(mandal_threshold, village_threshold)

    if within_district:
        # Scoped: only match within the known district
        query = """
            SELECT village_name_english, sub_district_name, district_name,
                   state_name,
                   similarity(sub_district_name,   %(loc)s) AS sim_mandal,
                   similarity(village_name_english, %(loc)s) AS sim_village
            FROM geo_reference
            WHERE district_name %% %(dist)s
              AND (village_name_english %% %(loc)s OR sub_district_name %% %(loc)s)
            ORDER BY GREATEST(
                similarity(sub_district_name, %(loc)s),
                similarity(village_name_english, %(loc)s)
            ) DESC
            LIMIT 1;
        """
    else:
        # Unscoped: search all districts
        query = """
            SELECT village_name_english, sub_district_name, district_name,
                   state_name,
                   similarity(sub_district_name,   %(loc)s) AS sim_mandal,
                   similarity(village_name_english, %(loc)s) AS sim_village
            FROM geo_reference
            WHERE village_name_english %% %(loc)s
               OR sub_district_name   %% %(loc)s
            ORDER BY GREATEST(
                similarity(sub_district_name, %(loc)s),
                similarity(village_name_english, %(loc)s)
            ) DESC
            LIMIT 1;
        """

    try:
        pool = PostgreSQLConnectionPool()
        with pool.get_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET pg_trgm.similarity_threshold = {pg_threshold};")

                for loc in candidates:
                    loc_clean = loc.strip()
                    if not loc_clean:
                        continue
                    params = {"loc": loc_clean}
                    if within_district:
                        params["dist"] = within_district
                    cur.execute(query, params)
                    row = cur.fetchone()
                    if row:
                        sim_mandal = float(row[4])
                        sim_village = float(row[5])

                        # Apply tiered threshold: mandal needs >= 0.70, village needs >= 0.65
                        if sim_mandal >= mandal_threshold:
                            best_sim = sim_mandal
                            matched_via = "mandal"
                        elif sim_village >= village_threshold:
                            best_sim = sim_village
                            matched_via = "village"
                        else:
                            logger.info(
                                f"    Rejected '{loc_clean}': mandal_sim={sim_mandal:.3f} < {mandal_threshold}, "
                                f"village_sim={sim_village:.3f} < {village_threshold}"
                            )
                            continue

                        match = GeoMatch(
                            village=row[0], sub_district=row[1],
                            district=row[2], state=row[3],
                            similarity=best_sim,
                        )
                        if best is None or match.similarity > best.similarity:
                            best = match
                            logger.info(f"    Accepted '{loc_clean}' via {matched_via}: sim={best_sim:.3f}")
    except Exception as e:
        logger.error(f"pg_trgm village/mandal lookup failed: {e}", exc_info=True)

    if best:
        logger.info(
            f"  pg_trgm village/mandal best: district={best.district}, "
            f"state={best.state}, sim={best.similarity:.3f}"
            f"{' (scoped to ' + within_district + ')' if within_district else ''}"
        )
    return best


# ===================================================================
#  STEP 2 — FOREIGN MATCHING against geo_countries (pg_trgm)
# ===================================================================

def _norm(text: str) -> str:
    return " ".join((text or "").lower().split())


def _extract_location_tokens(text: str) -> List[str]:
    """Extract potential location names from a free-text address field."""
    if not text or not text.strip():
        return []

    tokens = []
    text = text.strip()

    # Pattern: "X Dist" or "X District"
    for m in re.finditer(r'([\w][\w\s]*?)\s+Dist(?:rict|\.?)(?:\s|$|,)', text, re.IGNORECASE):
        name = m.group(1).strip()
        if name and len(name) >= 3:
            tokens.append(name)

    # Pattern: "X (V)" → village, "X (M)" → mandal
    for m in re.finditer(r'([\w][\w\s]*?)\s*\([VMvm]\)', text):
        name = m.group(1).strip()
        if name and len(name) >= 3:
            tokens.append(name)

    # Split by comma and try each part
    for part in re.split(r'[,;]', text):
        part = part.strip()
        if not part:
            continue
        # Skip obvious non-location patterns
        if re.match(
            r'^(H\.?\s*NO|House\s*No|Street|Road\s*no\.?\s*\d|Plot|Flat|Door\s*No'
            r'|S/?O|D/?O|W/?O|C/?O|Near|Opp|Behind|Adjacent)',
            part, re.IGNORECASE,
        ):
            continue
        if re.match(r'^[\d\.\-/\s]+$', part):  # Pure numbers
            continue
        if len(part) < 3:
            continue
        # Clean up markers
        part = re.sub(r'\s*\([VMvm]\)\s*', ' ', part).strip()
        part = re.sub(r'\s+Dist(?:rict|\.?)$', '', part, flags=re.IGNORECASE).strip()
        if part and len(part) >= 3:
            tokens.append(part)

    # Deduplicate preserving order
    seen = set()
    result = []
    for t in tokens:
        t_clean = t.strip()
        t_norm = t_clean.lower()
        if t_norm not in seen and len(t_clean) >= 3:
            seen.add(t_norm)
            result.append(t_clean)
    return result


@dataclass
class ForeignMatch:
    """Result of a geo_countries pg_trgm lookup."""
    country: Optional[str] = None
    state: Optional[str] = None
    similarity: float = 0.0


def trgm_match_foreign_country(
    candidates: List[str],
    threshold: float = TRGM_THRESHOLD_FOREIGN,
) -> Optional[ForeignMatch]:
    """
    Fuzzy-match candidate strings against geo_countries.country_name.
    Returns the best match (country + associated state if the candidate
    also matches a state_name row for that country).
    """
    if not candidates:
        return None

    best: Optional[ForeignMatch] = None

    # Match country_name directly
    query_country = """
        SELECT DISTINCT country_name,
               similarity(country_name, %(loc)s) AS sim
        FROM geo_countries
        WHERE country_name %% %(loc)s
        ORDER BY sim DESC
        LIMIT 1;
    """

    try:
        pool = PostgreSQLConnectionPool()
        with pool.get_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET pg_trgm.similarity_threshold = {threshold};")
                for loc in candidates:
                    loc_clean = loc.strip()
                    if not loc_clean or len(loc_clean) < 3:
                        continue
                    cur.execute(query_country, {"loc": loc_clean})
                    row = cur.fetchone()
                    if row and float(row[1]) >= threshold:
                        match = ForeignMatch(
                            country=row[0], similarity=float(row[1]),
                        )
                        if best is None or match.similarity > best.similarity:
                            best = match
                            logger.info(
                                f"    pg_trgm foreign country match: '{loc_clean}' → "
                                f"country={match.country}, sim={match.similarity:.3f}"
                            )
    except Exception as e:
        logger.error(f"pg_trgm foreign country lookup failed: {e}", exc_info=True)

    return best


def trgm_match_foreign_state(
    candidates: List[str],
    within_country: Optional[str] = None,
    threshold: float = TRGM_THRESHOLD_FOREIGN,
) -> Optional[ForeignMatch]:
    """
    Fuzzy-match candidate strings against geo_countries.state_name.
    If within_country is provided, scopes to that country.
    Returns (state, country) or None.
    """
    if not candidates:
        return None

    best: Optional[ForeignMatch] = None

    if within_country:
        query = """
            SELECT state_name, country_name,
                   similarity(state_name, %(loc)s) AS sim
            FROM geo_countries
            WHERE country_name = %(country)s
              AND state_name %% %(loc)s
            ORDER BY sim DESC
            LIMIT 1;
        """
    else:
        query = """
            SELECT state_name, country_name,
                   similarity(state_name, %(loc)s) AS sim
            FROM geo_countries
            WHERE state_name %% %(loc)s
            ORDER BY sim DESC
            LIMIT 1;
        """

    try:
        pool = PostgreSQLConnectionPool()
        with pool.get_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET pg_trgm.similarity_threshold = {threshold};")
                for loc in candidates:
                    loc_clean = loc.strip()
                    if not loc_clean or len(loc_clean) < 3:
                        continue
                    params = {"loc": loc_clean}
                    if within_country:
                        params["country"] = within_country
                    cur.execute(query, params)
                    row = cur.fetchone()
                    if row and float(row[2]) >= threshold:
                        match = ForeignMatch(
                            state=row[0], country=row[1],
                            similarity=float(row[2]),
                        )
                        if best is None or match.similarity > best.similarity:
                            best = match
                            logger.info(
                                f"    pg_trgm foreign state match: '{loc_clean}' → "
                                f"state={match.state}, country={match.country}, "
                                f"sim={match.similarity:.3f}"
                            )
    except Exception as e:
        logger.error(f"pg_trgm foreign state lookup failed: {e}", exc_info=True)

    return best


def trgm_match_foreign_full(
    candidates: List[str],
    full_address_text: str,
    existing_state: Optional[str] = None,
    threshold: float = TRGM_THRESHOLD_FOREIGN,
) -> Optional[Tuple[Optional[str], str]]:
    """
    Combined foreign lookup: try country name first (both from candidates and
    full address text), then state/city matching. Returns (state, country) or None.
    """
    # 1. Try matching existing state against geo_countries.state_name
    if existing_state and existing_state.strip():
        fm = trgm_match_foreign_state([existing_state], threshold=threshold)
        if fm:
            return (existing_state, fm.country)

    # 2. Direct country name match from candidates
    fm_country = trgm_match_foreign_country(candidates, threshold=threshold)
    if fm_country:
        # Found country — try to find state within that country
        fm_state = trgm_match_foreign_state(
            candidates, within_country=fm_country.country, threshold=threshold
        )
        state_out = fm_state.state if fm_state else None
        return (state_out, fm_country.country)

    # 3. Try country name from full address text (word-by-word)
    if full_address_text:
        words = re.split(r'[,;\s]+', full_address_text)
        # Try multi-word combinations (2-3 words) for countries like "Saudi Arabia"
        text_candidates = []
        for i in range(len(words)):
            w = words[i].strip()
            if w and len(w) >= 3:
                text_candidates.append(w)
            if i + 1 < len(words):
                pair = f"{words[i]} {words[i+1]}".strip()
                if len(pair) >= 5:
                    text_candidates.append(pair)
            if i + 2 < len(words):
                triple = f"{words[i]} {words[i+1]} {words[i+2]}".strip()
                if len(triple) >= 5:
                    text_candidates.append(triple)

        fm_country = trgm_match_foreign_country(text_candidates, threshold=threshold)
        if fm_country:
            fm_state = trgm_match_foreign_state(
                candidates + text_candidates,
                within_country=fm_country.country, threshold=threshold,
            )
            state_out = fm_state.state if fm_state else None
            return (state_out, fm_country.country)

    # 4. Try state/city match (no country anchor) — broader search
    fm_state = trgm_match_foreign_state(candidates, threshold=threshold)
    if fm_state:
        return (fm_state.state, fm_state.country)

    return None


# ===================================================================
#  DATABASE OPERATIONS
# ===================================================================

def fetch_records_needing_update(
    table_name: str,
    id_column: str,
    limit: Optional[int] = None,
) -> List[AddressRecord]:
    """Fetch persons rows where district, state, or country is missing."""
    where = (
        "(permanent_district IS NULL OR permanent_district = '' "
        "OR permanent_state_ut IS NULL OR permanent_state_ut = '' "
        "OR permanent_country IS NULL OR permanent_country = '') "
        "AND NOT (permanent_district IS NOT NULL AND permanent_district != '' "
        "AND permanent_state_ut IS NOT NULL AND permanent_state_ut != '' "
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

    pool = PostgreSQLConnectionPool()
    with pool.get_connection_context() as conn:
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
    district: Optional[str] = None,
    state: Optional[str] = None,
    country: Optional[str] = None,
    update_district_field: bool = False,
    update_state_field: bool = False,
    update_country_field: bool = False,
) -> None:
    """Write permanent_district, permanent_state_ut, and/or permanent_country."""
    updates, params = [], []

    if update_district_field:
        val = district.strip() if district and district.strip() else None
        if val is not None:
            updates.append("permanent_district = %s"); params.append(val)
        else:
            updates.append("permanent_district = NULL")

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

    pool = PostgreSQLConnectionPool()
    with pool.get_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()
    logger.info(f"Updated {table_name}.{id_column}={record_id}: district={district}, state={state}, country={country}")


# ===================================================================
#  MAIN PIPELINE
# ===================================================================

def process_records(
    table_name: str,
    id_column: str,
    limit: Optional[int] = None,
    dry_run: bool = False,
) -> None:
    logger.info("=" * 80)
    logger.info("Address District/State/Country Pipeline  (pg_trgm: geo_reference + geo_countries)")
    logger.info("=" * 80)
    logger.info(f"Table: {table_name}  |  ID: {id_column}  |  Limit: {limit or 'ALL'}")
    logger.info(f"Dry run: {dry_run}")
    logger.info("=" * 80)

    # Fetch records
    records = fetch_records_needing_update(table_name, id_column, limit)
    if not records:
        logger.info("No records need updates.")
        return

    total = len(records)
    
    stats_lock = threading.Lock()
    stats = {"ok": 0, "fail": 0, "skip": 0, "geo": 0, "ref": 0, "unresolved": 0, "null": 0}

    def process_single_record(idx, rec):
        logger.info("-" * 80)
        logger.info(f"[{idx}/{total}] ID={rec.record_id}  district={rec.permanent_district}  state={rec.permanent_state_ut}  country={rec.permanent_country}")
        addr_text = rec.get_address_components()
        logger.info(f"  Address: {addr_text or '(empty)'}")

        # --- already complete ---
        if rec.is_complete():
            logger.info("  Skip: district, state & country already set")
            with stats_lock:
                stats["skip"] += 1
            return

        try:
            district_out: Optional[str] = None
            state_out: Optional[str] = None
            country_out: Optional[str] = None
            source = "none"

            # === RULE: all address fields NULL → set all to NULL ===
            if not rec.has_any_address():
                logger.info("  No address info → setting district, state & country to NULL")
                district_out, state_out, country_out, source = None, None, None, "null"
                with stats_lock:
                    stats["null"] += 1

            else:
                geo: Optional[GeoMatch] = None

                # --- Step 1a: State exists, country missing → match state (top priority) ---
                if not rec.needs_state() and rec.needs_country():
                    logger.info(f"  Step 1a: matching state '{rec.permanent_state_ut}' …")
                    geo = trgm_match_state(rec.permanent_state_ut)
                    if geo:
                        state_out = rec.permanent_state_ut
                        country_out = "India"
                        source = "geo"
                        with stats_lock:
                            stats["geo"] += 1

                # --- Step 1b: District matching (high trust) ---
                if not geo and not rec.needs_district():
                    logger.info(f"  Step 1b: matching district '{rec.permanent_district}' …")
                    geo = trgm_match_district(rec.permanent_district)
                    if geo:
                        district_out = rec.permanent_district
                        state_out = geo.state
                        country_out = "India"
                        source = "geo"
                        with stats_lock:
                            stats["geo"] += 1

                # --- Step 1b2: District unknown → extract from address text ---
                if not geo and rec.needs_district():
                    ext_candidates = rec.get_extended_candidates()
                    if ext_candidates:
                        known_state = rec.permanent_state_ut.strip() if not rec.needs_state() else None
                        logger.info(f"  Step 1b2: trying district from address text {ext_candidates[:5]} …")
                        for cand in ext_candidates:
                            geo = trgm_match_district(cand)
                            if geo:
                                # If state is known, verify the match is in same state
                                if known_state and geo.state and _norm(geo.state) != _norm(known_state):
                                    logger.info(f"    District '{cand}' → {geo.district}/{geo.state}, state mismatch with {known_state}")
                                    geo = None
                                    continue
                                district_out = geo.district
                                state_out = geo.state or known_state
                                country_out = "India"
                                source = "geo"
                                with stats_lock:
                                    stats["geo"] += 1
                                break

                # --- Step 1c: Village/Mandal matching (expanded candidates) ---
                if not geo:
                    vm_candidates = [
                        rec.permanent_locality_village,
                        rec.permanent_area_mandal,
                    ]
                    vm_candidates = list(
                        dict.fromkeys(
                            [c.strip() for c in vm_candidates if c and c.strip()]
                        )
                    )

                    # If primary candidates are empty, use extended candidates from all fields
                    if not vm_candidates:
                        vm_candidates = rec.get_extended_candidates()

                    if vm_candidates:
                        # If district is known, scope the search to that district
                        scope_district = rec.permanent_district.strip() if not rec.needs_district() else None
                        logger.info(f"  Step 1c: matching village/mandal {vm_candidates}"
                                    f"{' within ' + scope_district if scope_district else ''} …")
                        geo = trgm_match_village_mandal(
                            vm_candidates,
                            within_district=scope_district,
                        )
                        if geo and geo.state:
                            district_out = geo.district
                            state_out = geo.state
                            country_out = "India"
                            source = "geo"
                            with stats_lock:
                                stats["geo"] += 1

                # --- Step 2: foreign geo_countries pg_trgm fallback ---
                if source == "none":
                    # Use extended candidates from ALL address fields
                    all_candidates = rec.get_extended_candidates()
                    if not all_candidates:
                        all_candidates = list(
                            dict.fromkeys(
                                [c.strip() for c in [
                                    rec.permanent_locality_village,
                                    rec.permanent_area_mandal,
                                    rec.permanent_district,
                                ] if c and c.strip()]
                            )
                        )
                    full_text = rec.get_full_address_text()
                    logger.info(f"  Step 2: foreign geo_countries lookup (candidates={all_candidates[:5]}) …")
                    foreign = trgm_match_foreign_full(
                        all_candidates, full_text,
                        existing_state=rec.permanent_state_ut,
                    )
                    if foreign:
                        state_out, country_out = foreign
                        source = "ref"
                        with stats_lock:
                            stats["ref"] += 1
                        logger.info(f"    Matched via geo_countries: state={state_out}, country={country_out}")
                    else:
                        logger.warning(f"  Unresolved: no geo or foreign match")
                        with stats_lock:
                            stats["unresolved"] += 1

            # --- WRITE ---
            logger.info(f"  Result ({source}): district={district_out}, state={state_out}, country={country_out}")

            if dry_run:
                logger.info("  [DRY RUN] — not writing")
                with stats_lock:
                    stats["ok"] += 1
                return

            # Only write fields that are currently missing
            write_district = rec.needs_district() and (district_out is not None or not rec.has_any_address())
            write_state = rec.needs_state() and (state_out is not None or not rec.has_any_address())
            write_country = rec.needs_country() and (country_out is not None or not rec.has_any_address())

            if write_district or write_state or write_country:
                update_location(
                    table_name, id_column, rec.record_id,
                    district=district_out if write_district else None,
                    state=state_out if write_state else None,
                    country=country_out if write_country else None,
                    update_district_field=write_district,
                    update_state_field=write_state,
                    update_country_field=write_country,
                )
                with stats_lock:
                    stats["ok"] += 1
            else:
                logger.warning(f"  No updates applicable for ID={rec.record_id}")
                with stats_lock:
                    stats["skip"] += 1

        except Exception as e:
            logger.error(f"  FAILED ID={rec.record_id}: {e}", exc_info=True)
            with stats_lock:
                stats["fail"] += 1

    pool = PostgreSQLConnectionPool() # ensure initialized
    max_workers = int(os.environ.get('MAX_WORKERS', min(32, (os.cpu_count() or 1) * 4)))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_single_record, idx, rec): rec for idx, rec in enumerate(records, 1)}
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Thread execution failed: {e}")

    # --- Summary ---
    logger.info("=" * 80)
    logger.info("Pipeline Summary")
    logger.info(f"  Total:         {total}")
    logger.info(f"  Updated:       {stats['ok']}")
    logger.info(f"  Skipped:       {stats['skip']}")
    logger.info(f"  Failed:        {stats['fail']}")
    logger.info(f"  via geo_ref:   {stats['geo']}")
    logger.info(f"  via geo_countries: {stats['ref']}")
    logger.info(f"  unresolved:    {stats['unresolved']}")
    logger.info(f"  set to NULL:   {stats['null']}")
    logger.info("=" * 80)


# ===================================================================
#  CLI
# ===================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Determine district/state/country from address via pg_trgm"
    )
    parser.add_argument("--table", default=DEFAULT_TABLE_NAME)
    parser.add_argument("--id-column", default=DEFAULT_ID_COLUMN)
    parser.add_argument("--limit", type=int, default=None,
                        help="Max records (default: all)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    process_records(
        table_name=args.table,
        id_column=args.id_column,
        limit=args.limit,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
