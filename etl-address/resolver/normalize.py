from __future__ import annotations

import re
import unicodedata
from typing import Optional

from .types import AddressCandidate, PersonRow

STATE_ABBREV = {
    "ap": "Andhra Pradesh",
    "ts": "Telangana",
    "tg": "Telangana",
    "tn": "Tamil Nadu",
    "ka": "Karnataka",
    "kl": "Kerala",
    "mh": "Maharashtra",
    "wb": "West Bengal",
    "up": "Uttar Pradesh",
    "mp": "Madhya Pradesh",
    "rj": "Rajasthan",
    "gj": "Gujarat",
    "br": "Bihar",
    "or": "Odisha",
    "od": "Odisha",
    "pb": "Punjab",
    "hr": "Haryana",
    "hp": "Himachal Pradesh",
    "jk": "Jammu and Kashmir",
    "uk": "Uttarakhand",
    "ut": "Uttarakhand",
    "ga": "Goa",
    "cg": "Chhattisgarh",
    "jh": "Jharkhand",
    "as": "Assam",
    "mn": "Manipur",
    "ml": "Meghalaya",
    "mz": "Mizoram",
    "nl": "Nagaland",
    "sk": "Sikkim",
    "tr": "Tripura",
    "ar": "Arunachal Pradesh",
    "dl": "Delhi",
    "py": "Puducherry",
    "ch": "Chandigarh",
    "an": "Andaman and Nicobar Islands",
    "dn": "Dadra and Nagar Haveli and Daman and Diu",
    "ld": "Lakshadweep",
}

CITY_NICKNAMES = {
    "hyd": "Hyderabad",
    "sec": "Secunderabad",
    "vij": "Vijayawada",
    "vizag": "Visakhapatnam",
    "vskp": "Visakhapatnam",
    "bglr": "Bengaluru",
    "blr": "Bengaluru",
    "bangalore": "Bengaluru",
    "mum": "Mumbai",
    "bom": "Mumbai",
    "del": "Delhi",
    "ncr": "Delhi",
    "rr": "Ranga Reddy",
    "rr dist": "Ranga Reddy",
    "rrdist": "Ranga Reddy",
    "madras": "Chennai",
}

LOCALITY_TO_DISTRICT_MANDAL = {
    "suraram": ("Telangana", "Medchal Malkajgiri", "Quthbullapur"),
    "suraram colony": ("Telangana", "Medchal Malkajgiri", "Quthbullapur"),
    "jagadgirigutta": ("Telangana", "Medchal Malkajgiri", "Quthbullapur"),
    "rgk": ("Telangana", "Medchal Malkajgiri", "Quthbullapur"),
    "bachupally": ("Telangana", "Medchal Malkajgiri", "Bachupally"),
    "medipally": ("Telangana", "Medchal Malkajgiri", "Medipally"),
    "perzadiguda": ("Telangana", "Medchal Malkajgiri", "Medipally"),
    "beeramguda": ("Telangana", "Sangareddy", "Ameenpur"),
    "ameenpur": ("Telangana", "Sangareddy", "Ameenpur"),
    "mamillagudem": ("Telangana", "Nalgonda", "Miryalaguda"),
    "achanpally": ("Telangana", "Nizamabad", "Bodhan"),
    "yedapally": ("Telangana", "Nizamabad", "Yedapally"),
    "bhainsa": ("Telangana", "Nirmal", "Bhainsa"),
    "rayanch enclave": ("Telangana", "Medchal Malkajgiri", "Medipally"),
}

_WS = re.compile(r"\s+")
_PUNCT = re.compile(r"[^\w\s\-&/.]", re.UNICODE)
_WORD_RE = re.compile(r"\b([A-Za-z][a-z]{2,}(?:\s+[A-Za-z][a-z]{2,})?)\b")
_NOISE_WORDS = {
    "flat", "no", "house", "road", "block", "room", "plot", "colony",
    "nagar", "enclave", "street", "lane", "door", "building", "floor", "wing",
}


def _nfkc(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s = unicodedata.normalize("NFKC", s)
    return s


def _strip_diacritics(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))


def norm_token(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s = _nfkc(s) or ""
    s = _strip_diacritics(s)
    s = _PUNCT.sub(" ", s)
    s = _WS.sub(" ", s).strip()
    return s or None


def expand_state(raw: Optional[str]) -> Optional[str]:
    t = norm_token(raw)
    if not t:
        return None
    key = t.lower().strip().rstrip(".")
    if key in STATE_ABBREV:
        return STATE_ABBREV[key]
    return _titlecase(t)


def expand_city(raw: Optional[str]) -> Optional[str]:
    t = norm_token(raw)
    if not t:
        return None
    key = t.lower().strip().rstrip(".")
    if key in CITY_NICKNAMES:
        return CITY_NICKNAMES[key]
    return _titlecase(t)


def _titlecase(s: str) -> str:
    parts = s.split()
    out = []
    for p in parts:
        if p.isupper() and len(p) <= 3:
            out.append(p)
        else:
            out.append(p.capitalize())
    return " ".join(out)


def _merge_locality(*parts: Optional[str]) -> Optional[str]:
    tokens = []
    seen = set()
    for part in parts:
        token = norm_token(part)
        if not token:
            continue
        key = token.lower()
        if key in seen:
            continue
        seen.add(key)
        tokens.append(token)
    return " ".join(tokens) or None


def enrich_candidate(cand: AddressCandidate) -> AddressCandidate:
    if cand.is_fully_resolvable:
        return cand

    combined = " ".join(
        value for value in (
            cand.raw_ward,
            cand.raw_street,
            cand.raw_locality,
            cand.raw_landmark,
        ) if value
    ).lower()

    if not combined:
        return cand

    for phrase, (state, district, mandal) in LOCALITY_TO_DISTRICT_MANDAL.items():
        if phrase in combined:
            cand.state = cand.state or state
            cand.district = cand.district or district
            cand.mandal = cand.mandal or mandal
            cand.enriched_locality_hit = True
            break

    if not cand.locality:
        merged = _merge_locality(cand.raw_ward, cand.raw_street, cand.raw_locality)
        if merged:
            cand.locality = merged

    # Fallback: mine free-text words to create a locality hint for KB trgm probes.
    if not cand.locality:
        title_text = combined.title()
        tokens = [t.strip() for t in _WORD_RE.findall(title_text)]
        meaningful = [
            t for t in tokens
            if t and t.lower() not in _NOISE_WORDS and len(t) > 3
        ]
        if meaningful:
            cand.locality = " ".join(meaningful[:3])

    return cand


def build_candidates(row: PersonRow) -> tuple[AddressCandidate, AddressCandidate]:
    """Return (permanent, present) candidates with normalized tokens."""
    perm = AddressCandidate(
        slot="permanent",
        raw_state=row.perm_state, raw_district=row.perm_district,
        raw_mandal=row.perm_mandal, raw_country=row.perm_country,
        raw_ward=row.perm_ward, raw_street=row.perm_street, raw_pin=row.perm_pin,
        raw_locality=row.perm_locality, raw_landmark=row.perm_landmark,
        raw_nationality=row.nationality,
        state=expand_state(row.perm_state),
        district=expand_city(row.perm_district),
        mandal=expand_city(row.perm_mandal),
        country=_titlecase(norm_token(row.perm_country) or "") or None,
        ward=norm_token(row.perm_ward),
        street=norm_token(row.perm_street),
        pin=norm_token(row.perm_pin),
        locality=_merge_locality(row.perm_locality, row.perm_ward, row.perm_street),
        landmark=norm_token(row.perm_landmark),
        nationality=norm_token(row.nationality),
    )
    pres = AddressCandidate(
        slot="present",
        raw_state=row.pres_state, raw_district=row.pres_district,
        raw_mandal=row.pres_mandal, raw_country=row.pres_country,
        raw_ward=row.pres_ward, raw_street=row.pres_street, raw_pin=row.pres_pin,
        raw_locality=row.pres_locality, raw_landmark=row.pres_landmark,
        raw_nationality=row.nationality,
        state=expand_state(row.pres_state),
        district=expand_city(row.pres_district),
        mandal=expand_city(row.pres_mandal),
        country=_titlecase(norm_token(row.pres_country) or "") or None,
        ward=norm_token(row.pres_ward),
        street=norm_token(row.pres_street),
        pin=norm_token(row.pres_pin),
        locality=_merge_locality(row.pres_locality, row.pres_ward, row.pres_street),
        landmark=norm_token(row.pres_landmark),
        nationality=norm_token(row.nationality),
    )
    return enrich_candidate(perm), enrich_candidate(pres)
