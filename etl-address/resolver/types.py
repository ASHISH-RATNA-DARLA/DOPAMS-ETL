from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class PersonRow:
    person_id: str
    perm_state: Optional[str]
    perm_district: Optional[str]
    perm_mandal: Optional[str]
    perm_country: Optional[str]
    perm_ward: Optional[str]
    perm_street: Optional[str]
    perm_pin: Optional[str]
    pres_state: Optional[str]
    pres_district: Optional[str]
    pres_mandal: Optional[str]
    pres_country: Optional[str]
    pres_ward: Optional[str]
    pres_street: Optional[str]
    pres_pin: Optional[str]
    perm_locality: Optional[str]
    perm_landmark: Optional[str]
    pres_locality: Optional[str]
    pres_landmark: Optional[str]
    nationality: Optional[str]


@dataclass
class AddressCandidate:
    raw_state: Optional[str] = None
    raw_district: Optional[str] = None
    raw_mandal: Optional[str] = None
    raw_country: Optional[str] = None
    raw_ward: Optional[str] = None
    raw_street: Optional[str] = None
    raw_pin: Optional[str] = None
    raw_locality: Optional[str] = None
    raw_landmark: Optional[str] = None
    raw_nationality: Optional[str] = None
    # normalized
    state: Optional[str] = None
    district: Optional[str] = None
    mandal: Optional[str] = None
    country: Optional[str] = None
    ward: Optional[str] = None
    street: Optional[str] = None
    pin: Optional[str] = None
    locality: Optional[str] = None
    landmark: Optional[str] = None
    nationality: Optional[str] = None
    # bookkeeping
    slot: str = "permanent"  # "permanent" | "present"
    enriched_locality_hit: bool = False

    @property
    def has_any_signal(self) -> bool:
        return any([
            self.state, self.district, self.mandal, self.country,
            self.ward, self.street, self.pin, self.locality,
            self.landmark, self.nationality,
        ])

    @property
    def is_fully_resolvable(self) -> bool:
        return bool(self.state and self.district and self.mandal and self.country)


@dataclass
class ResolvedAddress:
    country: Optional[str] = None
    state: Optional[str] = None
    district: Optional[str] = None
    mandal: Optional[str] = None
    slot: str = "permanent"
    path: str = ""          # "kb" | "llm" | "kb+llm"
    confidence: float = 0.0
    notes: str = ""

    @property
    def has_any(self) -> bool:
        return any([self.country, self.state, self.district, self.mandal])

    @property
    def is_complete(self) -> bool:
        return bool(self.country and self.state and self.district)


@dataclass
class FailureReason:
    person_id: str
    reason: str
    details: dict = field(default_factory=dict)
