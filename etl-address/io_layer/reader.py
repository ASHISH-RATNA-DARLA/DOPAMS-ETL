from __future__ import annotations

import os

from typing import List, Optional

from resolver.types import PersonRow


QUARANTINE_THRESHOLD = int(os.environ.get("ADDRESS_ROW_RETRIES", "3"))


PENDING_WHERE = f"""
    (
            -- actionable geo signal exists, or we still need to fill country from nationality
      (
          TRIM(COALESCE(permanent_state_ut,''))           <> ''
       OR TRIM(COALESCE(present_state_ut,''))             <> ''
       OR TRIM(COALESCE(permanent_district,''))           <> ''
       OR TRIM(COALESCE(present_district,''))             <> ''
       OR TRIM(COALESCE(permanent_area_mandal,''))        <> ''
       OR TRIM(COALESCE(present_area_mandal,''))          <> ''
       OR TRIM(COALESCE(permanent_locality_village,''))   <> ''
       OR TRIM(COALESCE(present_locality_village,''))     <> ''
       OR TRIM(COALESCE(permanent_landmark_milestone,'')) <> ''
       OR TRIM(COALESCE(present_landmark_milestone,''))   <> ''
             OR TRIM(COALESCE(permanent_ward_colony,''))        <> ''
             OR TRIM(COALESCE(present_ward_colony,''))          <> ''
             OR TRIM(COALESCE(permanent_street_road_no,''))     <> ''
             OR TRIM(COALESCE(present_street_road_no,''))       <> ''
             OR TRIM(COALESCE(permanent_pin_code,''))           <> ''
             OR TRIM(COALESCE(present_pin_code,''))             <> ''
               OR (
                     TRIM(COALESCE(nationality,'')) <> ''
                 AND (
                        TRIM(COALESCE(permanent_country,'')) = ''
                     OR TRIM(COALESCE(present_country,''))   = ''
                 )
               )
      )
      -- not fully resolved
      AND NOT (
           TRIM(COALESCE(permanent_country,''))   <> ''
       AND TRIM(COALESCE(permanent_state_ut,''))  <> ''
       AND TRIM(COALESCE(permanent_district,''))  <> ''
             AND TRIM(COALESCE(present_country,''))     <> ''
             AND TRIM(COALESCE(present_state_ut,''))    <> ''
             AND TRIM(COALESCE(present_district,''))    <> ''
      )
      -- not quarantined
      AND NOT EXISTS (
        SELECT 1 FROM etl_address_failures f
        WHERE f.person_id = persons.person_id::text
                    AND f.attempted >= {QUARANTINE_THRESHOLD}
      )
    )
"""


def count_pending(pool, table: str = "persons") -> int:
    sql = f"SELECT COUNT(*) FROM {table} WHERE {PENDING_WHERE}"
    with pool.get_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchone()[0]


def fetch_batch(
    pool,
    last_seen_id: Optional[str],
    limit: int,
    table: str = "persons",
    id_col: str = "person_id",
) -> List[PersonRow]:
    sql = f"""
        SELECT
            {id_col}::text,
            TRIM(COALESCE(permanent_state_ut,          '')),
            TRIM(COALESCE(permanent_district,          '')),
            TRIM(COALESCE(permanent_area_mandal,       '')),
            TRIM(COALESCE(permanent_country,           '')),
            TRIM(COALESCE(permanent_ward_colony,       '')),
            TRIM(COALESCE(permanent_street_road_no,    '')),
            TRIM(COALESCE(permanent_pin_code,          '')),
            TRIM(COALESCE(present_state_ut,            '')),
            TRIM(COALESCE(present_district,            '')),
            TRIM(COALESCE(present_area_mandal,         '')),
            TRIM(COALESCE(present_country,             '')),
            TRIM(COALESCE(present_ward_colony,         '')),
            TRIM(COALESCE(present_street_road_no,      '')),
            TRIM(COALESCE(present_pin_code,            '')),
            TRIM(COALESCE(permanent_locality_village,  '')),
            TRIM(COALESCE(permanent_landmark_milestone,'')),
            TRIM(COALESCE(present_locality_village,    '')),
            TRIM(COALESCE(present_landmark_milestone,  '')),
            TRIM(COALESCE(nationality,                 ''))
        FROM {table}
        WHERE {PENDING_WHERE}
          AND (%s IS NULL OR {id_col}::text > %s)
        ORDER BY {id_col}::text, ctid
        LIMIT %s
    """
    with pool.get_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (last_seen_id, last_seen_id, limit))
            rows = cur.fetchall()

    return [
        PersonRow(
            person_id    = r[0],
            perm_state   = r[1] or None,
            perm_district= r[2] or None,
            perm_mandal  = r[3] or None,
            perm_country = r[4] or None,
            perm_ward    = r[5] or None,
            perm_street  = r[6] or None,
            perm_pin     = r[7] or None,
            pres_state   = r[8] or None,
            pres_district= r[9] or None,
            pres_mandal  = r[10] or None,
            pres_country = r[11] or None,
            pres_ward    = r[12] or None,
            pres_street  = r[13] or None,
            pres_pin     = r[14] or None,
            perm_locality= r[15] or None,
            perm_landmark= r[16] or None,
            pres_locality= r[17] or None,
            pres_landmark= r[18] or None,
            nationality  = r[19] or None,
        )
        for r in rows
    ]
