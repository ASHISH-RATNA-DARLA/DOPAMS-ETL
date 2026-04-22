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
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(
            "apply_resolution person_id=%s perm=%s/%s/%s/%s pres=%s/%s/%s/%s",
            person_id,
            getattr(perm, "country", None), getattr(perm, "state", None),
            getattr(perm, "district", None), getattr(perm, "mandal", None),
            getattr(pres, "country", None), getattr(pres, "state", None),
            getattr(pres, "district", None), getattr(pres, "mandal", None),
        )

    perm = perm or ResolvedAddress(slot="permanent")
    pres = pres or ResolvedAddress(slot="present")

    sql = f"""
        UPDATE {table} SET
            permanent_country = CASE
                WHEN NULLIF(BTRIM(permanent_country), '') IS NULL
                    THEN COALESCE(%(p_country)s, permanent_country)
                ELSE permanent_country
            END,
            permanent_state_ut = CASE
                WHEN NULLIF(BTRIM(permanent_state_ut), '') IS NULL
                    THEN COALESCE(%(p_state)s, permanent_state_ut)
                ELSE permanent_state_ut
            END,
            permanent_district = CASE
                WHEN NULLIF(BTRIM(permanent_district), '') IS NULL
                    THEN COALESCE(%(p_district)s, permanent_district)
                ELSE permanent_district
            END,
            permanent_area_mandal = CASE
                WHEN NULLIF(BTRIM(permanent_area_mandal), '') IS NULL
                    THEN COALESCE(%(p_mandal)s, permanent_area_mandal)
                ELSE permanent_area_mandal
            END,
            present_country = CASE
                WHEN NULLIF(BTRIM(present_country), '') IS NULL
                    THEN COALESCE(%(r_country)s, present_country)
                ELSE present_country
            END,
            present_state_ut = CASE
                WHEN NULLIF(BTRIM(present_state_ut), '') IS NULL
                    THEN COALESCE(%(r_state)s, present_state_ut)
                ELSE present_state_ut
            END,
            present_district = CASE
                WHEN NULLIF(BTRIM(present_district), '') IS NULL
                    THEN COALESCE(%(r_district)s, present_district)
                ELSE present_district
            END,
            present_area_mandal = CASE
                WHEN NULLIF(BTRIM(present_area_mandal), '') IS NULL
                    THEN COALESCE(%(r_mandal)s, present_area_mandal)
                ELSE present_area_mandal
            END
        WHERE {id_col}::text = %(pid)s
          AND (
               (NULLIF(BTRIM(permanent_country), '') IS NULL AND %(p_country)s IS NOT NULL)
            OR (NULLIF(BTRIM(permanent_state_ut), '') IS NULL AND %(p_state)s IS NOT NULL)
            OR (NULLIF(BTRIM(permanent_district), '') IS NULL AND %(p_district)s IS NOT NULL)
            OR (NULLIF(BTRIM(permanent_area_mandal), '') IS NULL AND %(p_mandal)s IS NOT NULL)
            OR (NULLIF(BTRIM(present_country), '') IS NULL AND %(r_country)s IS NOT NULL)
            OR (NULLIF(BTRIM(present_state_ut), '') IS NULL AND %(r_state)s IS NOT NULL)
            OR (NULLIF(BTRIM(present_district), '') IS NULL AND %(r_district)s IS NOT NULL)
            OR (NULLIF(BTRIM(present_area_mandal), '') IS NULL AND %(r_mandal)s IS NOT NULL)
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
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("apply_resolution person_id=%s wrote=%s rowcount=%s", person_id, wrote, rowcount)
    return wrote, not wrote
