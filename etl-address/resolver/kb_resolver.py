from __future__ import annotations

import logging
import os
from typing import Optional

from .kb_cache import GeoKB
from .types import AddressCandidate, ResolvedAddress

logger = logging.getLogger(__name__)

# Similarity thresholds for pg_trgm parent-bounded fallback
SIM_STATE = float(os.environ.get("ADDRESS_SIM_STATE", "0.80"))
SIM_DISTRICT = float(os.environ.get("ADDRESS_SIM_DISTRICT", "0.75"))
SIM_MANDAL = float(os.environ.get("ADDRESS_SIM_MANDAL", "0.65"))
SIM_VILLAGE = float(os.environ.get("ADDRESS_SIM_VILLAGE", "0.55"))
SIM_COUNTRY_STATE = float(os.environ.get("ADDRESS_SIM_COUNTRY_STATE", "0.80"))
SIM_COUNTRY = float(os.environ.get("ADDRESS_SIM_COUNTRY", "0.70"))


def resolve_kb(pool, cand: AddressCandidate) -> ResolvedAddress:
    """Deterministic resolve. Order:
      1. Exact in-memory match (state → district → mandal).
      2. pg_trgm parent-bounded fallback for each unresolved field.
      3. Country derived from state (India or foreign via geo_countries).
    Returns ResolvedAddress (possibly partial)."""
    kb = GeoKB.instance()
    out = ResolvedAddress(slot=cand.slot, path="kb")

    # --- state ---
    state = kb.canon_state(cand.state)
    if state is None and cand.state:
        state = _trgm_state(pool, cand.state)
    out.state = state

    # --- district ---
    district = kb.canon_district(state, cand.district) if state else None
    if district is None and state and cand.district:
        district = _trgm_district(pool, state, cand.district)
    if district is None and cand.district and not state:
        # district-only: try any-state match
        district, state_guess = _trgm_district_anystate(pool, cand.district)
        if state_guess and out.state is None:
            out.state = state_guess
    out.district = district

    # --- mandal ---
    mandal = None
    if out.state and out.district and cand.mandal:
        mandal = kb.canon_mandal(out.state, out.district, cand.mandal)
        if mandal is None:
            mandal = _trgm_mandal(pool, out.state, out.district, cand.mandal)
    # mandal-from-locality / village / street tokens (best-effort; only with state+district known)
    village_hit = False
    if mandal is None and out.state and out.district:
        for tok in (cand.locality, cand.landmark, cand.ward, cand.street):
            if not tok:
                continue
            village_m = kb.canon_village(out.state, out.district, tok)
            if village_m is None:
                village_m = _trgm_village(pool, out.state, out.district, tok)
            m = village_m
            if village_m:
                village_hit = True
            if not m:
                m = _trgm_mandal(pool, out.state, out.district, tok)
            if m:
                mandal = m
                break
    out.mandal = mandal
    if village_hit:
        out.path += "+village"

    # --- country ---
    country = kb.canon_country(cand.country)
    if country is None:
        # India first
        country = kb.country_of_indian_state(out.state)
    if country is None:
        # foreign state → country
        country = kb.country_of_foreign_state(out.state)
    if country is None and cand.country:
        country = _trgm_country(pool, cand.country)
    if country is None and cand.nationality:
        country = kb.canon_country(cand.nationality) or _trgm_country(pool, cand.nationality)
    out.country = country

    # Title-case fallback if trgm returned lowered values
    if out.is_complete:
        out.confidence = 1.0
    elif out.has_any:
        out.confidence = 0.6
    return out


# ---------- pg_trgm fallbacks (parent-bounded) ----------

def _trgm_state(pool, token: str) -> Optional[str]:
    sql = """
        SELECT DISTINCT state_name, similarity(lower(state_name), lower(%s)) AS sim
        FROM geo_reference
        WHERE lower(state_name) %% lower(%s)
          AND similarity(lower(state_name), lower(%s)) >= %s
        ORDER BY sim DESC
        LIMIT 1
    """
    with pool.get_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (token, token, token, SIM_STATE))
            row = cur.fetchone()
            return row[0] if row else None


def _trgm_district(pool, state: str, token: str) -> Optional[str]:
    sql = """
        SELECT DISTINCT district_name, similarity(lower(district_name), lower(%s)) AS sim
        FROM geo_reference
        WHERE lower(state_name) = lower(%s)
          AND lower(district_name) %% lower(%s)
          AND similarity(lower(district_name), lower(%s)) >= %s
        ORDER BY sim DESC
        LIMIT 1
    """
    with pool.get_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (token, state, token, token, SIM_DISTRICT))
            row = cur.fetchone()
            return row[0] if row else None


def _trgm_district_anystate(pool, token: str):
    sql = """
        SELECT DISTINCT district_name, state_name,
               similarity(lower(district_name), lower(%s)) AS sim
        FROM geo_reference
        WHERE lower(district_name) %% lower(%s)
          AND similarity(lower(district_name), lower(%s)) >= %s
        ORDER BY sim DESC
        LIMIT 1
    """
    with pool.get_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (token, token, token, SIM_DISTRICT))
            row = cur.fetchone()
            if not row:
                return None, None
            return row[0], row[1]


def _trgm_mandal(pool, state: str, district: str, token: str) -> Optional[str]:
    sql = """
        SELECT DISTINCT sub_district_name,
               similarity(lower(sub_district_name), lower(%s)) AS sim
        FROM geo_reference
        WHERE lower(state_name) = lower(%s)
          AND lower(district_name) = lower(%s)
          AND sub_district_name IS NOT NULL
          AND lower(sub_district_name) %% lower(%s)
          AND similarity(lower(sub_district_name), lower(%s)) >= %s
        ORDER BY sim DESC
        LIMIT 1
    """
    with pool.get_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (token, state, district, token, token, SIM_MANDAL))
            row = cur.fetchone()
            return row[0] if row else None


def _trgm_village(pool, state: str, district: str, token: str) -> Optional[str]:
    sql = """
        SELECT DISTINCT sub_district_name,
               similarity(lower(village_name_english), lower(%s)) AS sim
        FROM geo_reference
        WHERE lower(state_name) = lower(%s)
          AND lower(district_name) = lower(%s)
          AND village_name_english IS NOT NULL
          AND sub_district_name IS NOT NULL
          AND lower(village_name_english) %% lower(%s)
          AND similarity(lower(village_name_english), lower(%s)) >= %s
        ORDER BY sim DESC
        LIMIT 1
    """
    with pool.get_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (token, state, district, token, token, SIM_VILLAGE))
            row = cur.fetchone()
            return row[0] if row else None


def _trgm_country(pool, token: str) -> Optional[str]:
    sql = """
        SELECT country_name, similarity(lower(country_name), lower(%s)) AS sim
        FROM geo_countries
        WHERE lower(country_name) %% lower(%s)
          AND similarity(lower(country_name), lower(%s)) >= %s
        ORDER BY sim DESC
        LIMIT 1
    """
    with pool.get_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (token, token, token, SIM_COUNTRY))
            row = cur.fetchone()
            return row[0] if row else None
