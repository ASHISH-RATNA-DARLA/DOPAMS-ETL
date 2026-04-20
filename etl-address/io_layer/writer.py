from __future__ import annotations

import logging
from typing import Optional, Tuple

from resolver.types import ResolvedAddress

logger = logging.getLogger(__name__)


def apply_resolution(
    pool,
    person_id: str,
    perm: Optional[ResolvedAddress],
    pres: Optional[ResolvedAddress],
    table: str = "persons",
    id_col: str = "person_id",
) -> Tuple[bool, bool]:
    """Single idempotent UPDATE for both address slots.
    Returns (wrote, unchanged). wrote=True if at least one field changed.
    """
    perm = perm or ResolvedAddress(slot="permanent")
    pres = pres or ResolvedAddress(slot="present")

    sql = f"""
        UPDATE {table} SET
            permanent_country     = COALESCE(%(p_country)s,  permanent_country),
            permanent_state_ut    = COALESCE(%(p_state)s,    permanent_state_ut),
            permanent_district    = COALESCE(%(p_district)s, permanent_district),
            permanent_area_mandal = COALESCE(%(p_mandal)s,   permanent_area_mandal),
            present_country       = COALESCE(%(r_country)s,  present_country),
            present_state_ut      = COALESCE(%(r_state)s,    present_state_ut),
            present_district      = COALESCE(%(r_district)s, present_district),
            present_area_mandal   = COALESCE(%(r_mandal)s,   present_area_mandal)
        WHERE {id_col}::text = %(pid)s
          AND (
               permanent_country     IS DISTINCT FROM COALESCE(%(p_country)s,  permanent_country)
            OR permanent_state_ut    IS DISTINCT FROM COALESCE(%(p_state)s,    permanent_state_ut)
            OR permanent_district    IS DISTINCT FROM COALESCE(%(p_district)s, permanent_district)
            OR permanent_area_mandal IS DISTINCT FROM COALESCE(%(p_mandal)s,   permanent_area_mandal)
            OR present_country       IS DISTINCT FROM COALESCE(%(r_country)s,  present_country)
            OR present_state_ut      IS DISTINCT FROM COALESCE(%(r_state)s,    present_state_ut)
            OR present_district      IS DISTINCT FROM COALESCE(%(r_district)s, present_district)
            OR present_area_mandal   IS DISTINCT FROM COALESCE(%(r_mandal)s,   present_area_mandal)
          )
    """

    params = {
        "pid":        person_id,
        "p_country":  perm.country,
        "p_state":    perm.state,
        "p_district": perm.district,
        "p_mandal":   perm.mandal,
        "r_country":  pres.country,
        "r_state":    pres.state,
        "r_district": pres.district,
        "r_mandal":   pres.mandal,
    }

    with pool.get_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rowcount = cur.rowcount
        conn.commit()

    wrote = rowcount > 0
    return wrote, not wrote
