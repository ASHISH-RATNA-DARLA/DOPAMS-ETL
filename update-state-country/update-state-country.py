#!/usr/bin/env python3
"""
Geographic Address Standardization Script for DOPAMAS
======================================================

Standardizes permanent and present address fields in the `persons` table
through a two-phase pipeline:

PHASE 1 — Indian geo_reference matching
----------------------------------------
1. Permanent pass  — match permanent_state_ut / permanent_district /
                     permanent_area_mandal (whichever are non-NULL).
   → Write: permanent_state_ut, permanent_district,
            permanent_area_mandal, permanent_country = 'India'

2. Present pass    — only when ALL THREE permanent geo fields are NULL.
   Match present_state_ut / present_district / present_area_mandal.
   → Write: present_state_ut, present_district,
            present_area_mandal, present_country = 'India'

PHASE 2 — Foreign country fallback (geo_countries)
----------------------------------------------------
Triggered only for records where Phase 1 found NO match in geo_reference.
Collects all non-null geo tokens from BOTH address sets and fuzzy-matches
them against geo_countries.state_name and geo_countries.country_name.

Matching preference: state_name first (state → resolve country), then
country_name directly.  The highest similarity match above threshold wins.

→ Write ONLY: permanent_country = resolved country_name
  (district / mandal / state are left untouched — foreign records should
   not be forced into Indian administrative hierarchy)

Similarity thresholds (tunable via env-vars)
--------------------------------------------
GEO_SIM_STATE     default 0.85   (geo_reference state_name)
GEO_SIM_DISTRICT  default 0.80   (geo_reference district_name)
GEO_SIM_MANDAL    default 0.65   (geo_reference sub_district_name)
GEO_SIM_FOREIGN   default 0.50   (geo_countries — lower for transliteration)

Batch size: configurable via BATCH_SIZE env-var (default 500).
Concurrency: MAX_WORKERS env-var (default: min(32, cpu*4)).
"""

import argparse
import logging
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Optional, List, Tuple

from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_pooling import PostgreSQLConnectionPool

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
load_dotenv()

DB_DSN = os.environ.get(
    "DB_DSN",
    " ".join([
        f"dbname={os.environ.get('DB_NAME', 'dopamas')}",
        f"user={os.environ.get('DB_USER')}",
        f"password={os.environ.get('DB_PASSWORD')}",
        f"host={os.environ.get('DB_HOST', 'localhost')}",
        f"port={os.environ.get('DB_PORT', '5432')}",
    ]),
)

TABLE_NAME   = os.environ.get("TABLE_NAME",   "persons")
ID_COLUMN    = os.environ.get("ID_COLUMN",    "person_id")
BATCH_SIZE   = int(os.environ.get("BATCH_SIZE",  "500"))
MAX_WORKERS  = int(os.environ.get("MAX_WORKERS",  str(min(32, (os.cpu_count() or 1) * 4))))

SIM_STATE    = float(os.environ.get("GEO_SIM_STATE",    "0.85"))
SIM_DISTRICT = float(os.environ.get("GEO_SIM_DISTRICT", "0.80"))
SIM_MANDAL   = float(os.environ.get("GEO_SIM_MANDAL",   "0.65"))
SIM_FOREIGN  = float(os.environ.get("GEO_SIM_FOREIGN",  "0.65"))

logger.info("DB      : %s @ %s", os.environ.get("DB_NAME", "dopamas"), os.environ.get("DB_HOST", "localhost"))
logger.info("Table   : %s  ID: %s", TABLE_NAME, ID_COLUMN)
logger.info("Batch   : %s  Workers: %s", BATCH_SIZE, MAX_WORKERS)
logger.info("Thresholds — state: %.2f  district: %.2f  mandal: %.2f  foreign: %.2f",
            SIM_STATE, SIM_DISTRICT, SIM_MANDAL, SIM_FOREIGN)


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

def _val(v: Optional[str]) -> Optional[str]:
    """Return stripped value or None for empty / whitespace-only strings."""
    return v.strip() if v and v.strip() else None


@dataclass
class PersonRecord:
    person_id: str

    # permanent
    perm_state:    Optional[str] = None
    perm_district: Optional[str] = None
    perm_mandal:   Optional[str] = None
    perm_country:  Optional[str] = None

    # present
    pres_state:    Optional[str] = None
    pres_district: Optional[str] = None
    pres_mandal:   Optional[str] = None
    pres_country:  Optional[str] = None

    def permanent_has_any_geo(self) -> bool:
        return any([_val(self.perm_state), _val(self.perm_district), _val(self.perm_mandal)])

    def permanent_is_complete(self) -> bool:
        return all([_val(self.perm_state), _val(self.perm_district),
                    _val(self.perm_mandal), _val(self.perm_country)])

    def present_has_any_geo(self) -> bool:
        return any([_val(self.pres_state), _val(self.pres_district), _val(self.pres_mandal)])

    def present_is_complete(self) -> bool:
        return all([_val(self.pres_state), _val(self.pres_district),
                    _val(self.pres_mandal), _val(self.pres_country)])


@dataclass
class GeoMatch:
    state:    str
    district: str
    mandal:   str
    score:    float = 0.0


@dataclass
class ForeignMatch:
    """Result of a geo_countries fuzzy lookup."""
    country:    str
    state:      Optional[str] = None   # the matched state_name (if matched via state)
    similarity: float = 0.0
    matched_via: str = ""              # "state_name" | "country_name"


# ---------------------------------------------------------------------------
# Phase 2: foreign country matching via geo_countries
# ---------------------------------------------------------------------------

def _collect_foreign_candidates(rec: "PersonRecord") -> List[str]:
    """
    Collect all distinct, non-empty geo token strings from both permanent
    and present address fields of the record.

    We pull from six source fields:
        permanent_state_ut, permanent_district, permanent_area_mandal
        present_state_ut,   present_district,   present_area_mandal

    Each value is stripped and de-duplicated (case-insensitive).
    Only values of 3+ characters are included to avoid noise.
    """
    raw = []
    if not rec.permanent_is_complete():
        raw.extend([rec.perm_state, rec.perm_district, rec.perm_mandal])
    if not rec.present_is_complete():
        raw.extend([rec.pres_state, rec.pres_district, rec.pres_mandal])
    seen: set = set()
    tokens: List[str] = []
    for v in raw:
        cleaned = _val(v)
        if cleaned and len(cleaned) >= 3:
            key = cleaned.lower()
            if key not in seen:
                seen.add(key)
                tokens.append(cleaned)
    return tokens


def match_foreign_country(
    candidates: List[str],
    record_id:  str,
) -> Optional[ForeignMatch]:
    """
    Phase 2: fuzzy-match candidate tokens against geo_countries.

    Matching order (preference: state → country):
      Step A — match each token against geo_countries.state_name.
               If found, the country is resolved from that row.
               Rationale: state names are more specific than country names
               and uniquely identify the country, so a state hit is a
               high-confidence country signal.

      Step B — match each token against geo_countries.country_name directly.
               Used when no state matched.

    The best similarity across ALL candidates and BOTH columns wins,
    subject to the SIM_FOREIGN threshold.

    Returns ForeignMatch(country, state, similarity, matched_via) or None.
    """
    if not candidates:
        return None

    pool = PostgreSQLConnectionPool()
    best: Optional[ForeignMatch] = None

    # ------------------------------------------------------------------
    # Step A: match against state_name → derive country
    # ------------------------------------------------------------------
    state_sql = """
        SELECT
            state_name,
            country_name,
            similarity(state_name, %(token)s) AS sim
        FROM geo_countries
        WHERE state_name %% %(token)s
          AND similarity(state_name, %(token)s) >= %(thr)s
        ORDER BY sim DESC
        LIMIT 1
    """

    # ------------------------------------------------------------------
    # Step B: match against country_name directly
    # ------------------------------------------------------------------
    country_sql = """
        SELECT
            country_name,
            similarity(country_name, %(token)s) AS sim
        FROM geo_countries
        WHERE country_name %% %(token)s
          AND similarity(country_name, %(token)s) >= %(thr)s
        ORDER BY sim DESC
        LIMIT 1
    """

    try:
        with pool.get_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET pg_trgm.similarity_threshold = {SIM_FOREIGN};")

                for token in candidates:
                    # --- Step A: state_name ---
                    cur.execute(state_sql, {"token": token, "thr": SIM_FOREIGN})
                    row = cur.fetchone()
                    if row:
                        sim = float(row[2])
                        if best is None or sim > best.similarity:
                            best = ForeignMatch(
                                country=row[1],
                                state=row[0],
                                similarity=sim,
                                matched_via="state_name",
                            )
                            logger.info(
                                "  [%s] foreign Phase2/A: token='%s' → state=%s "
                                "country=%s sim=%.3f",
                                record_id, token, row[0], row[1], sim,
                            )

                    # --- Step B: country_name (only if no state match beat it) ---
                    cur.execute(country_sql, {"token": token, "thr": SIM_FOREIGN})
                    row = cur.fetchone()
                    if row:
                        sim = float(row[1])
                        if best is None or (
                            sim > best.similarity
                            and best.matched_via == "country_name"
                        ):
                            # Only replace a country_name match with a better one;
                            # never let a country_name match demote a state_name match.
                            if best is None or best.matched_via != "state_name":
                                best = ForeignMatch(
                                    country=row[0],
                                    state=None,
                                    similarity=sim,
                                    matched_via="country_name",
                                )
                                logger.info(
                                    "  [%s] foreign Phase2/B: token='%s' → "
                                    "country=%s sim=%.3f",
                                    record_id, token, row[0], sim,
                                )

    except Exception as exc:
        logger.error("  [%s] foreign geo_countries lookup failed: %s",
                     record_id, exc, exc_info=True)
        return None

    if best:
        logger.info(
            "  [%s] foreign best: country=%s (via %s, sim=%.3f)",
            record_id, best.country, best.matched_via, best.similarity,
        )
    else:
        logger.warning("  [%s] foreign Phase 2: no match above threshold %.2f",
                       record_id, SIM_FOREIGN)

    return best


def apply_foreign_country(
    table:      str,
    id_col:     str,
    person_id:  str,
    country:    str,
    dry_run:    bool,
) -> bool:
    """
    Write only permanent_country (Phase 2 update).
    district / mandal / state are deliberately left untouched.
    """
    if dry_run:
        logger.info("  [DRY-RUN] %s: would set permanent_country=%s", person_id, country)
        return True

    sql = f"UPDATE {table} SET permanent_country = %s WHERE {id_col} = %s"
    pool = PostgreSQLConnectionPool()
    with pool.get_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (country, person_id))
        conn.commit()
    logger.info("  [WRITE] %s: permanent_country=%s", person_id, country)
    return True


# ---------------------------------------------------------------------------
# Core: dynamic pg_trgm matcher
# ---------------------------------------------------------------------------

def _build_match_query(
    state_val:    Optional[str],
    district_val: Optional[str],
    mandal_val:   Optional[str],
) -> Tuple[Optional[str], dict]:
    """
    Build a dynamic SQL query that joins only the non-NULL input fields.

    Strategy
    --------
    * For each available field we compute its similarity against the
      corresponding geo_reference column.
    * A candidate row must meet every supplied field's threshold.
    * Rows are ranked by a weighted composite score:
        district_weight=3, mandal_weight=2, state_weight=1
      This ensures district anchors trump broader state matches.
    * We use the pg_trgm '%' operator (threshold-filtered) in WHERE to
      leverage GIN indexes, then re-check exact similarity in Python to
      avoid cross-threshold contamination.

    Returns (sql, params) or (None, {}) if no fields available.
    """
    fields = {}
    if _val(state_val):
        fields["state"] = _val(state_val)
    if _val(district_val):
        fields["district"] = _val(district_val)
    if _val(mandal_val):
        fields["mandal"] = _val(mandal_val)

    if not fields:
        return None, {}

    # SELECT clause — always pull canonical names + per-field similarities
    select_parts = [
        "state_name",
        "district_name",
        "sub_district_name",
    ]
    sim_exprs = []
    weight_expr_parts = []

    if "state" in fields:
        select_parts.append("similarity(state_name,    %(state)s)    AS sim_state")
        sim_exprs.append("similarity(state_name,    %(state)s)    >= %(thr_state)s")
        weight_expr_parts.append("similarity(state_name,    %(state)s)    * 1")
    else:
        select_parts.append("NULL::float AS sim_state")

    if "district" in fields:
        select_parts.append("similarity(district_name, %(district)s) AS sim_district")
        sim_exprs.append("similarity(district_name, %(district)s) >= %(thr_district)s")
        weight_expr_parts.append("similarity(district_name, %(district)s) * 3")
    else:
        select_parts.append("NULL::float AS sim_district")

    if "mandal" in fields:
        select_parts.append("similarity(sub_district_name, %(mandal)s) AS sim_mandal")
        sim_exprs.append("similarity(sub_district_name, %(mandal)s) >= %(thr_mandal)s")
        weight_expr_parts.append("similarity(sub_district_name, %(mandal)s) * 2")
    else:
        select_parts.append("NULL::float AS sim_mandal")

    # WHERE: use pg_trgm '%' operator to benefit from GIN index,
    # but scope to only available fields so we don't over-filter
    where_parts = []
    if "state" in fields:
        where_parts.append("state_name %% %(state)s")
    if "district" in fields:
        where_parts.append("district_name %% %(district)s")
    if "mandal" in fields:
        where_parts.append("sub_district_name %% %(mandal)s")

    # Combine WHERE with AND so ALL available fields must trgm-match
    # (This scopes the result tightly when multiple fields are available,
    # while still using each GIN index for its respective column.)
    where_clause = " AND ".join(where_parts)

    score_expr = " + ".join(weight_expr_parts) if weight_expr_parts else "0"

    # HAVING: all supplied fields must individually clear their threshold
    having_clause = " AND ".join(sim_exprs) if sim_exprs else "TRUE"

    sql = f"""
        SELECT
            {", ".join(select_parts)},
            ({score_expr}) AS weighted_score
        FROM geo_reference
        WHERE {where_clause}
          AND ({having_clause})
        ORDER BY weighted_score DESC
        LIMIT 1
    """

    params = {
        "thr_state":    SIM_STATE,
        "thr_district": SIM_DISTRICT,
        "thr_mandal":   SIM_MANDAL,
    }
    params.update(fields)

    return sql, params


def match_geo(
    state_val:    Optional[str],
    district_val: Optional[str],
    mandal_val:   Optional[str],
    record_id:    str,
    addr_label:   str,
) -> Optional[GeoMatch]:
    """
    Run the dynamic pg_trgm query against geo_reference.

    Falls back progressively if the full multi-field query returns nothing:
      1. All available fields together (strictest — most precise)
      2. District + state (drop mandal)
      3. District alone
      4. Mandal alone
      5. State alone
    Each fall-back step relaxes the anchor, never mixes mis-matched fields.
    """

    # Build candidate probe sets (ordered most-specific → least-specific)
    probes: List[Tuple[Optional[str], Optional[str], Optional[str], str]] = []

    s = _val(state_val)
    d = _val(district_val)
    m = _val(mandal_val)

    available = sum([bool(s), bool(d), bool(m)])

    if available == 3:
        probes = [
            (s, d, m, "state+district+mandal"),
            (s, d, None, "state+district"),
            (None, d, m, "district+mandal"),
            (None, d, None, "district"),
            (None, None, m, "mandal"),
            (s, None, None, "state"),
        ]
    elif available == 2:
        if s and d:
            probes = [(s, d, None, "state+district"), (None, d, None, "district"), (s, None, None, "state")]
        elif d and m:
            probes = [(None, d, m, "district+mandal"), (None, d, None, "district"), (None, None, m, "mandal")]
        elif s and m:
            probes = [(s, None, m, "state+mandal"), (None, None, m, "mandal"), (s, None, None, "state")]
    elif available == 1:
        probes = [(s, d, m, ("state" if s else "district" if d else "mandal"))]

    if not probes:
        return None

    pool = PostgreSQLConnectionPool()

    for ps, pd, pm, label in probes:
        sql, params = _build_match_query(ps, pd, pm)
        if not sql:
            continue
        try:
            with pool.get_connection_context() as conn:
                with conn.cursor() as cur:
                    # Set the lowest threshold so the '%' operator in WHERE
                    # matches liberally; our HAVING clause enforces field-level
                    # thresholds precisely.
                    min_thr = min(SIM_STATE, SIM_DISTRICT, SIM_MANDAL)
                    cur.execute(f"SET pg_trgm.similarity_threshold = {min_thr};")
                    cur.execute(sql, params)
                    row = cur.fetchone()
                    if row:
                        geo = GeoMatch(
                            state=row[0],
                            district=row[1],
                            mandal=row[2] if pm else None,
                            # weighted_score is the last column (index 6)
                            score=float(row[6]),
                        )
                        logger.info(
                            "  [%s] %s | probe=%s → state=%s district=%s mandal=%s score=%.3f",
                            record_id, addr_label, label,
                            geo.state, geo.district, geo.mandal, geo.score,
                        )
                        return geo
                    else:
                        logger.debug(
                            "  [%s] %s | probe=%s → no match", record_id, addr_label, label
                        )
        except Exception as exc:
            logger.error("  [%s] %s | probe=%s query failed: %s",
                         record_id, addr_label, label, exc, exc_info=True)

    logger.warning("  [%s] %s | all probes exhausted — unresolved", record_id, addr_label)
    return None


# ---------------------------------------------------------------------------
# Database I/O
# ---------------------------------------------------------------------------

def fetch_batch(
    table: str,
    id_col: str,
    offset: int,
    limit: int,
) -> List[PersonRecord]:
    """
    Fetch one batch of persons that need standardization on permanent
    and/or present address fields.

    A record is included when:
      • Any of permanent_state_ut / permanent_district / permanent_area_mandal
        is non-NULL/non-empty AND permanent address is not already fully complete
      OR
      • All permanent geo fields are NULL/empty AND any present geo field exists
        AND present address is not already fully complete
    """
    sql = f"""
        SELECT
            {id_col},
            TRIM(COALESCE(permanent_state_ut,    '')) AS perm_state,
            TRIM(COALESCE(permanent_district,    '')) AS perm_district,
            TRIM(COALESCE(permanent_area_mandal, '')) AS perm_mandal,
            TRIM(COALESCE(permanent_country,     '')) AS perm_country,
            TRIM(COALESCE(present_state_ut,      '')) AS pres_state,
            TRIM(COALESCE(present_district,      '')) AS pres_district,
            TRIM(COALESCE(present_area_mandal,   '')) AS pres_mandal,
            TRIM(COALESCE(present_country,       '')) AS pres_country
        FROM {table}
        WHERE (
            -- Phase 1A: permanent has at least one geo field but is incomplete
            (
                (
                    TRIM(COALESCE(permanent_state_ut,    '')) <> ''
                 OR TRIM(COALESCE(permanent_district,    '')) <> ''
                 OR TRIM(COALESCE(permanent_area_mandal, '')) <> ''
                )
                AND NOT (
                    TRIM(COALESCE(permanent_state_ut,    '')) <> ''
                    AND TRIM(COALESCE(permanent_district,    '')) <> ''
                    AND TRIM(COALESCE(permanent_area_mandal, '')) <> ''
                    AND TRIM(COALESCE(permanent_country,     '')) <> ''
                )
            )
            OR
            -- Phase 1B: all permanent geo empty, present has at least one
            (
                TRIM(COALESCE(permanent_state_ut,    '')) = ''
                AND TRIM(COALESCE(permanent_district,    '')) = ''
                AND TRIM(COALESCE(permanent_area_mandal, '')) = ''
                AND (
                    TRIM(COALESCE(present_state_ut,    '')) <> ''
                 OR TRIM(COALESCE(present_district,    '')) <> ''
                 OR TRIM(COALESCE(present_area_mandal, '')) <> ''
                )
                AND NOT (
                    TRIM(COALESCE(present_state_ut,    '')) <> ''
                    AND TRIM(COALESCE(present_district,    '')) <> ''
                    AND TRIM(COALESCE(present_area_mandal, '')) <> ''
                    AND TRIM(COALESCE(present_country,     '')) <> ''
                )
            )
            OR
            -- Phase 2: any unresolved geo token exists but permanent_country is still empty
            -- (covers records where Phase 1 already ran but left country blank)
            (
                TRIM(COALESCE(permanent_country, '')) = ''
                AND (
                    TRIM(COALESCE(permanent_state_ut,    '')) <> ''
                 OR TRIM(COALESCE(permanent_district,    '')) <> ''
                 OR TRIM(COALESCE(permanent_area_mandal, '')) <> ''
                 OR (
                     TRIM(COALESCE(present_country, '')) = ''
                     AND (
                         TRIM(COALESCE(present_state_ut,    '')) <> ''
                      OR TRIM(COALESCE(present_district,    '')) <> ''
                      OR TRIM(COALESCE(present_area_mandal, '')) <> ''
                     )
                 )
                )
            )
        )
        ORDER BY {id_col}
        OFFSET %s LIMIT %s
    """
    pool = PostgreSQLConnectionPool()
    with pool.get_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (offset, limit))
            rows = cur.fetchall()

    return [
        PersonRecord(
            person_id   = str(r[0]),
            perm_state   = r[1] or None,
            perm_district= r[2] or None,
            perm_mandal  = r[3] or None,
            perm_country = r[4] or None,
            pres_state   = r[5] or None,
            pres_district= r[6] or None,
            pres_mandal  = r[7] or None,
            pres_country = r[8] or None,
        )
        for r in rows
    ]


def count_pending(table: str, id_col: str) -> int:
    sql = f"""
        SELECT COUNT(*)
        FROM {table}
        WHERE (
            (
                (
                    TRIM(COALESCE(permanent_state_ut,    '')) <> ''
                 OR TRIM(COALESCE(permanent_district,    '')) <> ''
                 OR TRIM(COALESCE(permanent_area_mandal, '')) <> ''
                )
                AND NOT (
                    TRIM(COALESCE(permanent_state_ut,    '')) <> ''
                    AND TRIM(COALESCE(permanent_district,    '')) <> ''
                    AND TRIM(COALESCE(permanent_area_mandal, '')) <> ''
                    AND TRIM(COALESCE(permanent_country,     '')) <> ''
                )
            )
            OR
            (
                TRIM(COALESCE(permanent_state_ut,    '')) = ''
                AND TRIM(COALESCE(permanent_district,    '')) = ''
                AND TRIM(COALESCE(permanent_area_mandal, '')) = ''
                AND (
                    TRIM(COALESCE(present_state_ut,    '')) <> ''
                 OR TRIM(COALESCE(present_district,    '')) <> ''
                 OR TRIM(COALESCE(present_area_mandal, '')) <> ''
                )
                AND NOT (
                    TRIM(COALESCE(present_state_ut,    '')) <> ''
                    AND TRIM(COALESCE(present_district,    '')) <> ''
                    AND TRIM(COALESCE(present_area_mandal, '')) <> ''
                    AND TRIM(COALESCE(present_country,     '')) <> ''
                )
            )
            OR
            (
                TRIM(COALESCE(permanent_country, '')) = ''
                AND (
                    TRIM(COALESCE(permanent_state_ut,    '')) <> ''
                 OR TRIM(COALESCE(permanent_district,    '')) <> ''
                 OR TRIM(COALESCE(permanent_area_mandal, '')) <> ''
                 OR (
                     TRIM(COALESCE(present_country, '')) = ''
                     AND (
                         TRIM(COALESCE(present_state_ut,    '')) <> ''
                      OR TRIM(COALESCE(present_district,    '')) <> ''
                      OR TRIM(COALESCE(present_area_mandal, '')) <> ''
                     )
                 )
                )
            )
        )
    """
    pool = PostgreSQLConnectionPool()
    with pool.get_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchone()[0]


def apply_updates(
    table:       str,
    id_col:      str,
    person_id:   str,
    perm_geo:    Optional[GeoMatch],
    pres_geo:    Optional[GeoMatch],
    dry_run:     bool,
) -> bool:
    """
    Write standardized values back to the persons row.
    Only updates fields that the match resolved — never clears an existing value.
    permanent_country / present_country are set to 'India' when a geo match
    is found (geo_reference only covers India).
    Returns True if any update was applied.
    """
    sets: List[str] = []
    params: List = []

    if perm_geo:
        sets.append("permanent_state_ut = %s")
        params.append(perm_geo.state)

        sets.append("permanent_district = %s")
        params.append(perm_geo.district)

        if perm_geo.mandal:
            sets.append("permanent_area_mandal = %s")
            params.append(perm_geo.mandal)

        sets.append("permanent_country = %s")
        params.append("India")

    if pres_geo:
        sets.append("present_state_ut = %s")
        params.append(pres_geo.state)

        sets.append("present_district = %s")
        params.append(pres_geo.district)

        if pres_geo.mandal:
            sets.append("present_area_mandal = %s")
            params.append(pres_geo.mandal)

        sets.append("present_country = %s")
        params.append("India")

    if not sets:
        return False

    if dry_run:
        logger.info("  [DRY-RUN] %s: would set %s", person_id, dict(zip(sets, params)))
        return True

    params.append(person_id)
    sql = f"UPDATE {table} SET {', '.join(sets)} WHERE {id_col} = %s"

    pool = PostgreSQLConnectionPool()
    with pool.get_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()

    logger.info(
        "  [WRITE] %s: perm=%s/%s/%s  pres=%s/%s/%s",
        person_id,
        perm_geo.state    if perm_geo else "-",
        perm_geo.district if perm_geo else "-",
        perm_geo.mandal   if perm_geo else "-",
        pres_geo.state    if pres_geo else "-",
        pres_geo.district if pres_geo else "-",
        pres_geo.mandal   if pres_geo else "-",
    )
    return True


# ---------------------------------------------------------------------------
# Per-record processing
# ---------------------------------------------------------------------------

def process_record(
    rec:     PersonRecord,
    table:   str,
    id_col:  str,
    idx:     int,
    total:   int,
    dry_run: bool,
    stats:   dict,
    lock:    threading.Lock,
) -> None:
    logger.info("[%d/%d] ID=%s", idx, total, rec.person_id)

    perm_geo:     Optional[GeoMatch]     = None
    pres_geo:     Optional[GeoMatch]     = None
    foreign_match: Optional[ForeignMatch] = None

    # ------------------------------------------------------------------
    # PHASE 1A — Permanent address (geo_reference)
    # ------------------------------------------------------------------
    phase1_attempted = False

    if rec.permanent_has_any_geo() and not rec.permanent_is_complete():
        phase1_attempted = True
        perm_geo = match_geo(
            rec.perm_state, rec.perm_district, rec.perm_mandal,
            rec.person_id, "permanent",
        )
        if perm_geo:
            with lock:
                stats["perm_matched"] += 1
        else:
            with lock:
                stats["perm_unresolved"] += 1

    # ------------------------------------------------------------------
    # PHASE 1B — Present address fallback (geo_reference)
    # (only when ALL three permanent geo fields are absent)
    # ------------------------------------------------------------------
    if (not rec.permanent_has_any_geo()
            and rec.present_has_any_geo()
            and not rec.present_is_complete()):
        phase1_attempted = True
        pres_geo = match_geo(
            rec.pres_state, rec.pres_district, rec.pres_mandal,
            rec.person_id, "present",
        )
        if pres_geo:
            with lock:
                stats["pres_matched"] += 1
        else:
            with lock:
                stats["pres_unresolved"] += 1

    # ------------------------------------------------------------------
    # PHASE 2 — Foreign country fallback (geo_countries)
    #
    # Triggered when:
    #   • Phase 1 was attempted but produced no match (perm_geo & pres_geo
    #     are both None after Phase 1), OR
    #   • permanent_country is still empty on a record that has any geo
    #     token (i.e. Phase 1 ran in a prior execution and left country blank)
    #
    # Only permanent_country is written; other fields are left intact.
    # ------------------------------------------------------------------
    phase1_failed   = phase1_attempted and perm_geo is None and pres_geo is None
    country_missing = not _val(rec.perm_country)
    
    unresolved_has_token = False
    if rec.permanent_has_any_geo() and not rec.permanent_is_complete():
        unresolved_has_token = True
    if rec.present_has_any_geo() and not rec.present_is_complete():
        unresolved_has_token = True

    if (phase1_failed or (country_missing and unresolved_has_token and not phase1_attempted)):
        logger.info("  [%s] Phase 1 unresolved → attempting Phase 2 (geo_countries)",
                    rec.person_id)
        candidates = _collect_foreign_candidates(rec)
        if candidates:
            foreign_match = match_foreign_country(candidates, rec.person_id)
            if foreign_match:
                with lock:
                    stats["foreign_matched"] += 1
            else:
                with lock:
                    stats["foreign_unresolved"] += 1
        else:
            logger.debug("  [%s] Phase 2: no candidate tokens", rec.person_id)
            with lock:
                stats["skipped"] += 1

    # ------------------------------------------------------------------
    # Nothing resolved at all
    # ------------------------------------------------------------------
    if perm_geo is None and pres_geo is None and foreign_match is None:
        with lock:
            stats["skipped"] += 1
        return

    # ------------------------------------------------------------------
    # Write Phase 1 results (full geo standardization)
    # ------------------------------------------------------------------
    try:
        if perm_geo is not None or pres_geo is not None:
            written = apply_updates(table, id_col, rec.person_id,
                                    perm_geo, pres_geo, dry_run)
            if written:
                with lock:
                    stats["updated"] += 1

        # Write Phase 2 result (country only)
        if foreign_match is not None:
            apply_foreign_country(
                table, id_col, rec.person_id,
                foreign_match.country, dry_run,
            )
            with lock:
                stats["foreign_written"] += 1

    except Exception as exc:
        logger.error("  [%s] write failed: %s", rec.person_id, exc, exc_info=True)
        with lock:
            stats["failed"] += 1


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run(
    table:    str,
    id_col:   str,
    limit:    Optional[int],
    dry_run:  bool,
) -> None:
    logger.info("=" * 80)
    logger.info("Geo Address Standardization  (pg_trgm → geo_reference)")
    logger.info("=" * 80)
    logger.info("Table: %s  |  ID: %s  |  Limit: %s  |  Dry-run: %s",
                table, id_col, limit or "ALL", dry_run)

    # Ensure the pool is initialised before spawning threads
    pool = PostgreSQLConnectionPool(minconn=5, maxconn=MAX_WORKERS + 5)

    total_pending = count_pending(table, id_col)
    effective_total = min(total_pending, limit) if limit else total_pending
    logger.info("Pending records: %d  |  Will process: %d", total_pending, effective_total)

    if effective_total == 0:
        logger.info("Nothing to process — all records are already complete.")
        return

    lock  = threading.Lock()
    stats = {
        "updated":            0,
        "skipped":            0,
        "failed":             0,
        "perm_matched":       0,
        "perm_unresolved":    0,
        "pres_matched":       0,
        "pres_unresolved":    0,
        "foreign_matched":    0,
        "foreign_unresolved": 0,
        "foreign_written":    0,
    }

    processed = 0
    offset    = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        while processed < effective_total:
            batch_size = min(BATCH_SIZE, effective_total - processed)
            batch = fetch_batch(table, id_col, offset, batch_size)
            if not batch:
                break

            logger.info("-" * 60)
            logger.info("Batch offset=%d  size=%d", offset, len(batch))

            futures = {
                executor.submit(
                    process_record,
                    rec, table, id_col,
                    processed + i + 1, effective_total,
                    dry_run, stats, lock,
                ): rec
                for i, rec in enumerate(batch)
            }
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as exc:
                    logger.error("Thread error: %s", exc, exc_info=True)
                    with lock:
                        stats["failed"] += 1

            processed += len(batch)
            offset    += len(batch)

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    logger.info("=" * 80)
    logger.info("Pipeline complete")
    logger.info("  Processed              : %d", processed)
    logger.info("  Updated (Phase 1)      : %d", stats["updated"])
    logger.info("  Skipped                : %d", stats["skipped"])
    logger.info("  Failed                 : %d", stats["failed"])
    logger.info("  --- Phase 1 (geo_reference) ---")
    logger.info("  Perm matched           : %d", stats["perm_matched"])
    logger.info("  Perm unresolved        : %d", stats["perm_unresolved"])
    logger.info("  Pres matched           : %d", stats["pres_matched"])
    logger.info("  Pres unresolved        : %d", stats["pres_unresolved"])
    logger.info("  --- Phase 2 (geo_countries) ---")
    logger.info("  Foreign matched        : %d", stats["foreign_matched"])
    logger.info("  Foreign unresolved     : %d", stats["foreign_unresolved"])
    logger.info("  Foreign country written: %d", stats["foreign_written"])
    logger.info("=" * 80)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Standardize persons geo-address fields via pg_trgm. "
            "Phase 1: geo_reference (India). Phase 2: geo_countries (foreign fallback)."
        )
    )
    parser.add_argument("--table",    default=TABLE_NAME,  help="Target table (default: persons)")
    parser.add_argument("--id-column",default=ID_COLUMN,   help="Primary key column (default: person_id)")
    parser.add_argument("--limit",    type=int, default=None, help="Max records to process (default: all)")
    parser.add_argument("--dry-run",  action="store_true",  help="Match but do not write")
    args = parser.parse_args()

    run(
        table   = args.table,
        id_col  = args.id_column,
        limit   = args.limit,
        dry_run = args.dry_run,
    )


if __name__ == "__main__":
    main()