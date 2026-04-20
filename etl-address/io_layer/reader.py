from __future__ import annotations

from typing import List, Optional

from resolver.types import PersonRow


PENDING_WHERE = """
    (
      -- at least one geo signal exists
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
       OR TRIM(COALESCE(nationality,''))                  <> ''
      )
      -- not fully resolved
      AND NOT (
           TRIM(COALESCE(permanent_country,''))   <> ''
       AND TRIM(COALESCE(permanent_state_ut,''))  <> ''
       AND TRIM(COALESCE(permanent_district,''))  <> ''
      )
      -- not quarantined
      AND NOT EXISTS (
        SELECT 1 FROM etl_address_failures f
        WHERE f.person_id = persons.person_id::text
          AND f.attempted >= 3
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
            TRIM(COALESCE(present_state_ut,            '')),
            TRIM(COALESCE(present_district,            '')),
            TRIM(COALESCE(present_area_mandal,         '')),
            TRIM(COALESCE(present_country,             '')),
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
            pres_state   = r[5] or None,
            pres_district= r[6] or None,
            pres_mandal  = r[7] or None,
            pres_country = r[8] or None,
            perm_locality= r[9] or None,
            perm_landmark= r[10] or None,
            pres_locality= r[11] or None,
            pres_landmark= r[12] or None,
            nationality  = r[13] or None,
        )
        for r in rows
    ]
