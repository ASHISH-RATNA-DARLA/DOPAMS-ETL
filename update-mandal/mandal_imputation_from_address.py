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
from typing import Optional, List
from concurrent.futures import ThreadPoolExecutor, as_completed

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

    if not tokens:
        return None

    pool = PostgreSQLConnectionPool()

    try:

        with pool.get_connection_context() as conn:
            with conn.cursor() as cur:

                if _val(district):

                    cur.execute("""
                        SELECT sub_district_name,
                               similarity(sub_district_name, %(tok)s) AS sim
                        FROM geo_reference
                        WHERE district_name ILIKE %(district)s
                        ORDER BY sim DESC
                        LIMIT 1
                    """, {
                        "tok": tokens,
                        "district": district
                    })

                    row = cur.fetchone()

                    if row and row[1] >= SIM_DISTRICT:
                        return row[0]

                elif _val(state):

                    cur.execute("""
                        SELECT sub_district_name,
                               similarity(sub_district_name, %(tok)s) AS sim
                        FROM geo_reference
                        WHERE state_name ILIKE %(state)s
                        ORDER BY sim DESC
                        LIMIT 1
                    """, {
                        "tok": tokens,
                        "state": state
                    })

                    row = cur.fetchone()

                    if row and row[1] >= SIM_STATE:
                        return row[0]

    except Exception as e:
        logger.error("mandal lookup failed: %s", e)

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
        permanent_area_mandal IS NULL
        OR present_area_mandal IS NULL

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