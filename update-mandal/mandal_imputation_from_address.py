#!/usr/bin/env python3
"""
Mandal Imputation Script for DOPAMAS
====================================

Fills missing mandals using address tokens BEFORE geo standardization.

Pipeline Position
-----------------
Run BEFORE update-state-country.py

Strategy
--------
1. Combine address fields into token string
2. Reverse match against geo_reference.sub_district_name
3. Restrict search scope:
      district present → threshold 0.70
      state only       → threshold 0.65
4. Update present_area_mandal / permanent_area_mandal

Batch + Threaded for large datasets.
"""

import os
import sys
import logging
import threading
from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from difflib import SequenceMatcher

from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_pooling import PostgreSQLConnectionPool


# ------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stdout
)

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Config
# ------------------------------------------------------------------

load_dotenv()

TABLE_NAME = os.environ.get("TABLE_NAME", "persons")
ID_COLUMN  = os.environ.get("ID_COLUMN", "person_id")

BATCH_SIZE  = int(os.environ.get("BATCH_SIZE", "500"))
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "16"))

SIM_DISTRICT = float(os.environ.get("MANDAL_SIM_DISTRICT", "0.70"))
SIM_STATE    = float(os.environ.get("MANDAL_SIM_STATE", "0.65"))


# ------------------------------------------------------------------
# Geo Reference Cache (Performance Optimization)
# ------------------------------------------------------------------

class GeoReferenceCache:
    """Pre-loads geo_reference into memory for ~10x faster lookups."""

    def __init__(self):
        self.cache_by_district: Dict[str, List[str]] = {}
        self.cache_by_state: Dict[str, List[str]] = {}
        self._load()

    def _load(self):
        pool = PostgreSQLConnectionPool()
        try:
            with pool.get_connection_context() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT UPPER(district_name), UPPER(state_name), sub_district_name
                        FROM geo_reference
                    """)
                    rows = cur.fetchall()

            for district, state, mandal in rows:
                if district not in self.cache_by_district:
                    self.cache_by_district[district] = []
                self.cache_by_district[district].append(mandal)

                if state not in self.cache_by_state:
                    self.cache_by_state[state] = []
                self.cache_by_state[state].append(mandal)

            logger.info("GeoReferenceCache loaded: %d districts, %d states",
                       len(self.cache_by_district),
                       len(self.cache_by_state))
        except Exception as e:
            logger.error("Failed to load GeoReferenceCache: %s", e)

    def find_mandal(self, tokens: str, district: Optional[str], 
                    state: Optional[str], sim_threshold: float) -> Optional[str]:
        """Find best matching mandal using local similarity."""
        if not tokens:
            return None

        candidates = []

        if _val(district):
            candidates = self.cache_by_district.get(district.upper(), [])
        elif _val(state):
            candidates = self.cache_by_state.get(state.upper(), [])

        if not candidates:
            return None

        best_match = None
        best_score = 0.0

        for mandal in candidates:
            score = SequenceMatcher(None, tokens.lower(), mandal.lower()).ratio()
            if score > best_score:
                best_score = score
                best_match = mandal

        if best_score >= sim_threshold:
            return best_match

        return None


_GEO_CACHE = None


def get_geo_cache() -> GeoReferenceCache:
    """Singleton accessor for geo cache."""
    global _GEO_CACHE
    if _GEO_CACHE is None:
        _GEO_CACHE = GeoReferenceCache()
    return _GEO_CACHE


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _val(v: Optional[str]) -> Optional[str]:
    return v.strip() if v and v.strip() else None


def build_tokens(fields: List[Optional[str]]) -> Optional[str]:

    tokens = []

    for f in fields:
        v = _val(f)
        if v:
            tokens.append(v.lower())

    if not tokens:
        return None

    text = " ".join(tokens)

    text = text.replace(",", " ")
    text = text.replace("-", " ")
    text = " ".join(text.split())

    return text


# ------------------------------------------------------------------
# Data model
# ------------------------------------------------------------------

@dataclass
class Record:

    person_id: str

    perm_state: Optional[str]
    perm_district: Optional[str]
    perm_mandal: Optional[str]

    perm_house: Optional[str]
    perm_street: Optional[str]
    perm_ward: Optional[str]
    perm_locality: Optional[str]
    perm_landmark: Optional[str]

    pres_state: Optional[str]
    pres_district: Optional[str]
    pres_mandal: Optional[str]

    pres_house: Optional[str]
    pres_street: Optional[str]
    pres_ward: Optional[str]
    pres_locality: Optional[str]
    pres_landmark: Optional[str]


# ------------------------------------------------------------------
# Mandal recovery
# ------------------------------------------------------------------

def recover_mandal(tokens, district, state):
    """Recover mandal using cached geo reference data."""
    if not tokens:
        return None

    cache = get_geo_cache()

    if _val(district):
        return cache.find_mandal(tokens, district, None, SIM_DISTRICT)
    elif _val(state):
        return cache.find_mandal(tokens, None, state, SIM_STATE)

    return None


# ------------------------------------------------------------------
# Fetch records
# ------------------------------------------------------------------

def fetch_batch(offset, limit):

    sql = f"""
    SELECT
        {ID_COLUMN},

        permanent_state_ut,
        permanent_district,
        permanent_area_mandal,

        permanent_house_no,
        permanent_street_road_no,
        permanent_ward_colony,
        permanent_locality_village,
        permanent_landmark_milestone,

        present_state_ut,
        present_district,
        present_area_mandal,

        present_house_no,
        present_street_road_no,
        present_ward_colony,
        present_locality_village,
        present_landmark_milestone

    FROM {TABLE_NAME}

    WHERE
        TRIM(COALESCE(permanent_area_mandal,'')) = ''
        OR TRIM(COALESCE(present_area_mandal,'')) = ''

    ORDER BY {ID_COLUMN}
    OFFSET %s LIMIT %s
    """

    pool = PostgreSQLConnectionPool()

    with pool.get_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (offset, limit))
            rows = cur.fetchall()

    records = []

    for r in rows:

        records.append(

            Record(

                person_id=str(r[0]),

                perm_state=r[1],
                perm_district=r[2],
                perm_mandal=r[3],

                perm_house=r[4],
                perm_street=r[5],
                perm_ward=r[6],
                perm_locality=r[7],
                perm_landmark=r[8],

                pres_state=r[9],
                pres_district=r[10],
                pres_mandal=r[11],

                pres_house=r[12],
                pres_street=r[13],
                pres_ward=r[14],
                pres_locality=r[15],
                pres_landmark=r[16],
            )
        )

    return records


# ------------------------------------------------------------------
# Update DB
# ------------------------------------------------------------------

def update_mandal(person_id, perm_mandal, pres_mandal):

    sets = []
    params = []

    if perm_mandal:
        sets.append("permanent_area_mandal = %s")
        params.append(perm_mandal)

    if pres_mandal:
        sets.append("present_area_mandal = %s")
        params.append(pres_mandal)

    if not sets:
        return

    params.append(person_id)

    sql = f"""
    UPDATE {TABLE_NAME}
    SET {", ".join(sets)}
    WHERE {ID_COLUMN} = %s
    """

    pool = PostgreSQLConnectionPool()

    with pool.get_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)

        conn.commit()


# ------------------------------------------------------------------
# Worker
# ------------------------------------------------------------------

def process_record(rec, stats, lock):

    perm_new = None
    pres_new = None

    if not _val(rec.perm_mandal):

        tokens = build_tokens([
            rec.perm_house,
            rec.perm_street,
            rec.perm_ward,
            rec.perm_locality,
            rec.perm_landmark,
            rec.perm_district
        ])

        perm_new = recover_mandal(tokens, rec.perm_district, rec.perm_state)

    if not _val(rec.pres_mandal):

        tokens = build_tokens([
            rec.pres_house,
            rec.pres_street,
            rec.pres_ward,
            rec.pres_locality,
            rec.pres_landmark,
            rec.pres_district
        ])

        pres_new = recover_mandal(tokens, rec.pres_district, rec.pres_state)

    if perm_new or pres_new:

        update_mandal(rec.person_id, perm_new, pres_new)

        with lock:
            stats["updated"] += 1
    else:
        with lock:
            stats["skipped"] += 1


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def run():

    logger.info("initializing geo reference cache...")
    get_geo_cache()  # Pre-load cache at startup
    logger.info("cache initialized successfully")

    pool = PostgreSQLConnectionPool()

    offset = 0
    processed = 0

    stats = {
        "updated": 0,
        "skipped": 0
    }

    lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        while True:

            batch = fetch_batch(offset, BATCH_SIZE)

            if not batch:
                break

            futures = []

            for rec in batch:
                futures.append(
                    executor.submit(process_record, rec, stats, lock)
                )

            for f in as_completed(futures):
                f.result()

            processed += len(batch)
            offset += len(batch)

            logger.info("processed=%s updated=%s skipped=%s",
                        processed,
                        stats["updated"],
                        stats["skipped"])

    logger.info("mandal imputation completed")


# ------------------------------------------------------------------

if __name__ == "__main__":
    run()