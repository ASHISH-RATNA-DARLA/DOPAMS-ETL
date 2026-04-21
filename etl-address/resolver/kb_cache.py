from __future__ import annotations

import logging
import threading
from typing import Dict, Optional, Set, Tuple

logger = logging.getLogger(__name__)


class GeoKB:
    """In-memory authoritative KB built from geo_reference + geo_countries.

    Loaded once per process. Read-only after build. Thread-safe for reads.
    """

    _instance: Optional["GeoKB"] = None
    _lock = threading.Lock()

    def __init__(self) -> None:
        self.states: Set[str] = set()
        self.state_canon: Dict[str, str] = {}           # lower -> canonical
        self.districts_by_state: Dict[str, Set[str]] = {}
        self.district_canon: Dict[Tuple[str, str], str] = {}   # (state_lower, district_lower) -> canonical
        self.mandals_by_district: Dict[Tuple[str, str], Set[str]] = {}
        self.mandal_canon: Dict[Tuple[str, str, str], str] = {}  # (state_l, district_l, mandal_l) -> canonical
        self.villages_by_district: Dict[Tuple[str, str], Set[str]] = {}
        self.village_canon: Dict[Tuple[str, str, str], str] = {}
        self.countries: Set[str] = set()
        self.country_canon: Dict[str, str] = {}
        self.state_to_country: Dict[str, str] = {}       # state_lower -> country (for foreign)
        self._built = False

    @classmethod
    def instance(cls) -> "GeoKB":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def build(self, pool) -> None:
        if self._built:
            return
        with self._lock:
            if self._built:
                return
            self._load_geo_reference(pool)
            self._load_geo_countries(pool)
            self._built = True
            logger.info(
                "GeoKB loaded: states=%d districts=%d mandals=%d countries=%d foreign_states=%d",
                len(self.states),
                sum(len(v) for v in self.districts_by_state.values()),
                sum(len(v) for v in self.mandals_by_district.values()),
                len(self.countries),
                len(self.state_to_country),
            )

    def _load_geo_reference(self, pool) -> None:
        with pool.get_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT
                        TRIM(state_name),
                        TRIM(district_name),
                        TRIM(COALESCE(sub_district_name,'')),
                        TRIM(COALESCE(village_name_english,''))
                    FROM geo_reference
                    WHERE state_name IS NOT NULL
                """)
                for state, district, mandal, village in cur.fetchall():
                    if not state:
                        continue
                    sl = state.lower()
                    self.states.add(state)
                    self.state_canon.setdefault(sl, state)

                    if district:
                        dl = district.lower()
                        self.districts_by_state.setdefault(sl, set()).add(district)
                        self.district_canon.setdefault((sl, dl), district)

                        if mandal:
                            ml = mandal.lower()
                            self.mandals_by_district.setdefault((sl, dl), set()).add(mandal)
                            self.mandal_canon.setdefault((sl, dl, ml), mandal)

                        if village:
                            vl = village.lower()
                            self.villages_by_district.setdefault((sl, dl), set()).add(village)
                            self.village_canon.setdefault((sl, dl, vl), village)

    def _load_geo_countries(self, pool) -> None:
        with pool.get_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT
                        TRIM(country_name),
                        TRIM(COALESCE(state_name,''))
                    FROM geo_countries
                    WHERE country_name IS NOT NULL
                """)
                for country, state in cur.fetchall():
                    if not country:
                        continue
                    cl = country.lower()
                    self.countries.add(country)
                    self.country_canon.setdefault(cl, country)
                    if state:
                        self.state_to_country.setdefault(state.lower(), country)

    # ---- lookup API ----

    def has_state(self, name: Optional[str]) -> bool:
        return bool(name) and name.lower() in self.state_canon

    def canon_state(self, name: Optional[str]) -> Optional[str]:
        if not name:
            return None
        return self.state_canon.get(name.lower())

    def canon_district(self, state: Optional[str], district: Optional[str]) -> Optional[str]:
        if not state or not district:
            return None
        return self.district_canon.get((state.lower(), district.lower()))

    def canon_mandal(
        self, state: Optional[str], district: Optional[str], mandal: Optional[str]
    ) -> Optional[str]:
        if not state or not district or not mandal:
            return None
        return self.mandal_canon.get((state.lower(), district.lower(), mandal.lower()))

    def canon_village(
        self, state: Optional[str], district: Optional[str], village: Optional[str]
    ) -> Optional[str]:
        if not state or not district or not village:
            return None
        return self.village_canon.get((state.lower(), district.lower(), village.lower()))

    def canon_country(self, name: Optional[str]) -> Optional[str]:
        if not name:
            return None
        return self.country_canon.get(name.lower())

    def country_of_indian_state(self, state: Optional[str]) -> Optional[str]:
        if state and state.lower() in self.state_canon:
            return "India"
        return None

    def country_of_foreign_state(self, state: Optional[str]) -> Optional[str]:
        if not state:
            return None
        return self.state_to_country.get(state.lower())

    def districts_in_state(self, state: Optional[str]) -> Set[str]:
        if not state:
            return set()
        return self.districts_by_state.get(state.lower(), set())

    def mandals_in_district(self, state: Optional[str], district: Optional[str]) -> Set[str]:
        if not state or not district:
            return set()
        return self.mandals_by_district.get((state.lower(), district.lower()), set())

    def villages_in_district(self, state: Optional[str], district: Optional[str]) -> Set[str]:
        if not state or not district:
            return set()
        return self.villages_by_district.get((state.lower(), district.lower()), set())
