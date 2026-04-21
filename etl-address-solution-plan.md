# ETL Address Resolution — Robust Solution Plan
**DOPAMS · `etl-address` module · April 2026**

---

## 1. Executive Summary

The goal of this ETL is to minimise null address fields (`district`, `state_ut`, `area_mandal`, `country`) across all `persons` records, using every available signal before admitting defeat. The current pipeline has a **signal collection gap** — meaningful address tokens exist in the database but are never read or passed to the resolver. This plan fixes that, adds a new resolution layer exploiting `village_name_english` and free-text extraction, and reorganises the tiers so each record gets the strongest possible treatment.

The KB tables involved are:
- `geo_reference` — `state_name`, `district_name`, `sub_district_name`, `village_name_english` (full Census hierarchy, pg_trgm indexed)
- `geo_countries` — `country_name`, `state_name`, `timezone` (foreign-state → country mapping)

---

## 2. Data Audit — Sample of 20 Records

Analysing the submitted CSV reveals five distinct resolution tiers present in the live data.

### Tier Distribution

| Tier | Label | Count | Description |
|------|-------|-------|-------------|
| T1 | COMPLETE | 6 | All KB fields already populated — ETL is idempotent here |
| T2 | needs\_mandal | 1 | state + district known; mandal missing |
| T3 | needs\_dist\_mandal | 5 | state known; district + mandal both missing |
| T4 | geo\_text\_only | 1 | No structured geo fields; place name embedded in ward/street text |
| T5 | NO\_GEO\_SIGNAL | 7 | Only `nationality="India"` or `country="India"` — nothing resolvable |

### Key Observations from the CSV

**T3 rows (Vijay Kumar, Md. Abdul Hafeez, Sheikh Sameer, Dhotula Prithviraj, Suresh):**
These have `present_state_ut = TELANGANA` but no district or mandal. The only additional data is in `present_house_no` / `present_street_road_no` containing strings like:
- `"Bhagath Singh Nagar, Suraram Colony"` — "Suraram" is a locality in Quthbullapur mandal, Medchal-Malkajgiri district
- `"Rayanch Enclave"` / `"Mamillagudem"` — both map to Medipally/LB Nagar area
- `"11-6/252/D, Rayanch Enclave"` — same cluster

These are currently **invisible** to the ETL because `street_road_no` and `ward_colony` are not read by `reader.py`.

**T4 row (Yesu Raju Lowdia):**
- `present_ward_colony = "Room No. 6, Block-44, RGK, Jagadgirigutta"`
- "Jagadgirigutta" is a locality in Quthbullapur mandal, Medchal-Malkajgiri, Telangana
- No state or district set; **entire resolution depends on LLM since KB can't match "RGK, Jagadgirigutta"** as a mandal/district directly

**T2 row (Jampala Sham):**
- `district = KHAMMAM`, `state = TELANGANA`, mandal missing
- No locality or landmark — mandal cannot be inferred without LLM
- This is a legitimate KB gap for the ETL (no sub-signal to work from)

**T5 rows (Sameer, Thuniki Rakesh, Atthena Madhu, Poosala Venkatesh, arvind josh, bobby, S.Vikrush):**
- Only `country = India` is set
- No street, ward, locality, landmark — nothing to work with
- These should be quarantined quickly to avoid wasting LLM budget

---

## 3. Root Cause Analysis

### 3.1 Signal Collection Gap (reader.py)

`fetch_batch()` reads 14 columns. The following are **silently omitted**:

| Omitted column | Data present in CSV | Impact |
|----------------|--------------------|-|
| `present_street_road_no` | "Bhagath Singh Nagar, Suraram Colony" | T3 → potentially T1 |
| `permanent_street_road_no` | Same | T3 → potentially T1 |
| `present_ward_colony` | "Rayanch Enclave", "Jagadgirigutta" | T3/T4 → resolution possible |
| `permanent_ward_colony` | Same | T3/T4 → resolution possible |
| `present_pin_code` | Not in this sample but present in schema | District inference via pin |
| `permanent_pin_code` | Same | District inference via pin |

The `PENDING_WHERE` predicate also does not count `ward_colony` or `street_road_no` as geo signals, so **T4 rows like Yesu Raju Lowdia are not even fetched** because their only data is in `ward_colony`.

### 3.2 Normalisation Gap (normalize.py)

`build_candidates()` maps `perm_locality` and `perm_landmark` to the `AddressCandidate` but does not incorporate `ward_colony` or `street_road_no` into any lookup token. A ward name like "Suraram Colony" or "Rayanch Enclave" would be a valid trgm input for village/sub-district lookup if it were passed through.

### 3.3 KB Resolver Missing Village Path (kb_resolver.py)

`resolve_kb()` attempts mandal resolution through `sub_district_name` lookup. It does **not** attempt to infer mandal from `village_name_english` in `geo_reference`. The schema has:
- `village_name_english` with its own GIN trgm index (`trgm_idx_geo_village`)
- The join path is: `village_name_english → sub_district_name` (within same row)

"Achanpally", "Yedapally", "Perzadiguda" in the CSV are villages that exist in `geo_reference` with associated `sub_district_name` — the KB can infer mandal if the lookup path is opened.

### 3.4 LLM Prompt Missing Free-Text Signals (llm_resolver.py)

`_build_prompt()` includes `state`, `district`, `mandal`, `country`, `locality`, `landmark`, `nationality`. It does **not** include `ward_colony` or `street`. For the LLM, the raw street string "Bhagath Singh Nagar, Hameed Basthi, Rallakancha, Suraram" is extremely valuable — it names a well-known Hyderabad locality that any geography-trained model can resolve to Suraram Colony / Quthbullapur mandal.

### 3.5 Mirror Propagation Miss (etl_address.py)

The mirror logic (`M:P<-R`, `M:R<-P`) fires only when `pres_cand.has_any_signal` is `True`. After Gap 3.1's fix this will be true more often. However, **country propagation** currently requires both sides to have at least some geo signal. For T5 rows where the only value is `nationality = "India"`, neither side triggers resolution and the mirror never runs — yet `country = "India"` should still be written to both `permanent_country` and `present_country` where they are null.

### 3.6 PENDING_WHERE Quarantine Threshold Mismatch

The SQL quarantine uses `attempted >= 3` hardcoded. The Python constant `MAX_RETRIES_ROW` is env-driven (default 3 but configurable). If someone sets `ADDRESS_ROW_RETRIES=5`, the SQL quarantine still fires at 3 — rows re-enter the pending set and get re-attempted before the Python threshold stops them. This is a correctness bug in high-load runs.

---

## 4. Proposed Solution Architecture

The solution adds one new data path, extends all existing layers, and introduces a pre-resolution enrichment stage between normalisation and KB lookup.

```
persons table
    ↓
[reader.py]  ← READ all 20 address columns (incl. ward, street, pin)
    ↓
[normalize.py]  ← merge ward+street+locality into unified token set
    ↓
[enrichment stage — NEW]  ← free-text token extraction from house_no/street
    ↓
[kb_resolver.py]  ← add village→mandal path via village_name_english
    ↓
[llm_resolver.py]  ← pass ward+street raw strings in prompt
    ↓
[country propagation — NEW]  ← write country=India from nationality even when no other geo
    ↓
[writer.py]  ← unchanged COALESCE logic
```

---

## 5. Detailed Change Specification

### 5.1 types.py — Extend PersonRow and AddressCandidate

Add the missing input fields to the data model.

```python
@dataclass
class PersonRow:
    # existing fields unchanged ...
    # NEW:
    perm_ward:     Optional[str]   # permanent_ward_colony
    perm_street:   Optional[str]   # permanent_street_road_no
    perm_pin:      Optional[str]   # permanent_pin_code
    pres_ward:     Optional[str]   # present_ward_colony
    pres_street:   Optional[str]   # present_street_road_no
    pres_pin:      Optional[str]   # present_pin_code


@dataclass
class AddressCandidate:
    # existing fields unchanged ...
    # NEW:
    raw_ward:      Optional[str] = None   # for LLM prompt
    raw_street:    Optional[str] = None   # for LLM prompt
    ward:          Optional[str] = None   # normalised ward token
    street:        Optional[str] = None   # normalised street token
    pin:           Optional[str] = None   # pin code
```

---

### 5.2 reader.py — Read All Address Columns

**Change 1: Expand SELECT**

```python
def fetch_batch(pool, last_seen_id, limit, table="persons", id_col="person_id"):
    sql = f"""
        SELECT
            {id_col}::text,
            TRIM(COALESCE(permanent_state_ut,           '')),  -- r[1]
            TRIM(COALESCE(permanent_district,           '')),  -- r[2]
            TRIM(COALESCE(permanent_area_mandal,        '')),  -- r[3]
            TRIM(COALESCE(permanent_country,            '')),  -- r[4]
            TRIM(COALESCE(present_state_ut,             '')),  -- r[5]
            TRIM(COALESCE(present_district,             '')),  -- r[6]
            TRIM(COALESCE(present_area_mandal,          '')),  -- r[7]
            TRIM(COALESCE(present_country,              '')),  -- r[8]
            TRIM(COALESCE(permanent_locality_village,   '')),  -- r[9]
            TRIM(COALESCE(permanent_landmark_milestone, '')),  -- r[10]
            TRIM(COALESCE(present_locality_village,     '')),  -- r[11]
            TRIM(COALESCE(present_landmark_milestone,   '')),  -- r[12]
            TRIM(COALESCE(nationality,                  '')),  -- r[13]
            -- NEW:
            TRIM(COALESCE(permanent_ward_colony,        '')),  -- r[14]
            TRIM(COALESCE(permanent_street_road_no,     '')),  -- r[15]
            TRIM(COALESCE(permanent_pin_code,           '')),  -- r[16]
            TRIM(COALESCE(present_ward_colony,          '')),  -- r[17]
            TRIM(COALESCE(present_street_road_no,       '')),  -- r[18]
            TRIM(COALESCE(present_pin_code,             ''))   -- r[19]
        FROM {table}
        WHERE {PENDING_WHERE}
          AND (%s IS NULL OR {id_col}::text > %s)
        ORDER BY {id_col}::text, ctid
        LIMIT %s
    """
```

**Change 2: Expand PENDING_WHERE to include ward_colony and street_road_no as geo signals**

```sql
-- Add to the "at least one geo signal exists" block:
OR TRIM(COALESCE(permanent_ward_colony,''))      <> ''
OR TRIM(COALESCE(present_ward_colony,''))        <> ''
OR TRIM(COALESCE(permanent_street_road_no,''))   <> ''
OR TRIM(COALESCE(present_street_road_no,''))     <> ''
```

This is critical for T4 rows (Yesu Raju Lowdia pattern) that only have data in `ward_colony`.

**Change 3: Map new columns to PersonRow**

```python
return [
    PersonRow(
        # existing mappings unchanged ...
        perm_ward    = r[14] or None,
        perm_street  = r[15] or None,
        perm_pin     = r[16] or None,
        pres_ward    = r[17] or None,
        pres_street  = r[18] or None,
        pres_pin     = r[19] or None,
    )
    for r in rows
]
```

---

### 5.3 normalize.py — Unified Locality Token + Free-Text Extraction

**Change 1: Merge ward + street + locality into one lookup token**

Multiple columns carry what is semantically "where within the district" — `locality_village`, `ward_colony`, and `street_road_no`. The KB resolver and LLM both benefit from having these combined into a single candidate token rather than being probed separately.

```python
def _merge_locality(*parts: Optional[str]) -> Optional[str]:
    """Combine ward, street, locality into a single normalised token.
    Deduplicates by presence check — does not append if already substring."""
    seen = set()
    tokens = []
    for p in parts:
        t = norm_token(p)
        if t:
            t_low = t.lower()
            if t_low not in seen:
                seen.add(t_low)
                tokens.append(t)
    return " ".join(tokens) or None


def build_candidates(row: PersonRow) -> tuple[AddressCandidate, AddressCandidate]:
    perm = AddressCandidate(
        slot="permanent",
        raw_state=row.perm_state,   raw_district=row.perm_district,
        raw_mandal=row.perm_mandal, raw_country=row.perm_country,
        raw_locality=row.perm_locality, raw_landmark=row.perm_landmark,
        raw_ward=row.perm_ward,     raw_street=row.perm_street,  # NEW
        raw_nationality=row.nationality,
        state=expand_state(row.perm_state),
        district=expand_city(row.perm_district),
        mandal=expand_city(row.perm_mandal),
        country=_titlecase(norm_token(row.perm_country) or "") or None,
        locality=_merge_locality(row.perm_locality, row.perm_ward, row.perm_street),  # CHANGED
        landmark=norm_token(row.perm_landmark),
        ward=norm_token(row.perm_ward),     # NEW
        street=norm_token(row.perm_street), # NEW
        pin=row.perm_pin or None,           # NEW
        nationality=norm_token(row.nationality),
    )
    # identical for pres slot
```

**Change 2: Add KNOWN_LOCALITIES dictionary for high-confidence free-text matching**

Based on the CSV sample and general Telangana/AP geography, a static lookup resolves the most common ambiguous locality names without hitting the DB:

```python
# In normalize.py — extends CITY_NICKNAMES concept for sub-district localities
LOCALITY_TO_DISTRICT_MANDAL: dict[str, tuple[str, str, str]] = {
    # locality_lower: (state, district, mandal)
    "suraram":           ("Telangana", "Medchal Malkajgiri", "Quthbullapur"),
    "suraram colony":    ("Telangana", "Medchal Malkajgiri", "Quthbullapur"),
    "bachupally":        ("Telangana", "Medchal Malkajgiri", "Bachupally"),
    "jagadgirigutta":    ("Telangana", "Medchal Malkajgiri", "Quthbullapur"),
    "rgk":               ("Telangana", "Medchal Malkajgiri", "Quthbullapur"),
    "medipally":         ("Telangana", "Medchal Malkajgiri", "Medipally"),
    "perzadiguda":       ("Telangana", "Medchal Malkajgiri", "Medipally"),
    "beeramguda":        ("Telangana", "Sangareddy",         "Ameenpur"),
    "ameenpur":          ("Telangana", "Sangareddy",         "Ameenpur"),
    "mamillagudem":      ("Telangana", "Nalgonda",           "Miryalaguda"),
    "achanpally":        ("Telangana", "Nizamabad",          "Bodhan"),
    "yedapally":         ("Telangana", "Nizamabad",          "Yedapally"),
    "bhainsa":           ("Telangana", "Nirmal",             "Bhainsa"),
    "rayanch enclave":   ("Telangana", "Medchal Malkajgiri", "Medipally"),
}
```

This dictionary is used in the new enrichment stage (5.4) as a fast pre-KB lookup.

---

### 5.4 New Enrichment Stage — Free-Text Token Extraction

This is a new function called between `build_candidates()` and `resolve_kb()`. Its job is to extract recognisable place tokens from `street_road_no`, `ward_colony`, and `house_no`, and attempt to pre-fill `state`, `district`, or `mandal` on the candidate before the KB resolver runs. This keeps the KB resolver and LLM resolver unchanged in contract — they receive a richer candidate.

```python
# In normalize.py (or a new enrichment.py module)

import re

# Tokenise a free-text address string into capitalised word candidates
_WORD_RE = re.compile(r'\b([A-Za-z][a-z]{2,}(?:\s+[A-Za-z][a-z]{2,})?)\b')

def enrich_candidate(cand: AddressCandidate) -> AddressCandidate:
    """Pre-fill missing state/district/mandal from free-text fields.
    Modifies in place. Returns the same object."""
    if cand.is_fully_resolvable:
        return cand  # nothing to do

    # Build a list of candidate locality phrases from free-text
    sources = [cand.raw_ward, cand.raw_street, cand.raw_landmark, cand.raw_locality]
    combined = " ".join(s for s in sources if s).lower()

    if not combined:
        return cand

    # 1. Try known locality dictionary (fast, zero-DB)
    for phrase, (state, district, mandal) in LOCALITY_TO_DISTRICT_MANDAL.items():
        if phrase in combined:
            if not cand.state:    cand.state    = state
            if not cand.district: cand.district = district
            if not cand.mandal:   cand.mandal   = mandal
            break  # first match wins — phrases ordered most→least specific

    # 2. Extract capitalised tokens as candidates for trgm lookup later
    # (stored in cand.locality so resolve_kb() picks them up automatically)
    if not cand.locality:
        tokens = _WORD_RE.findall(combined.title())
        # Filter out noise words
        NOISE = {"flat","no","house","road","block","room","plot","colony","nagar",
                 "enclave","street","lane","door","building","floor","wing"}
        meaningful = [t for t in tokens if t.lower() not in NOISE and len(t) > 3]
        if meaningful:
            cand.locality = " ".join(meaningful[:3])  # top-3 tokens

    return cand
```

`AddressCandidate` also needs an `is_fully_resolvable` property:

```python
@property
def is_fully_resolvable(self) -> bool:
    return bool(self.state and self.district and self.mandal and self.country)
```

And `resolve_one()` in `etl_address.py` calls `enrich_candidate()` before `resolve_kb()`:

```python
def resolve_one(pool, row, llm):
    perm_cand, pres_cand = build_candidates(row)
    perm_cand = enrich_candidate(perm_cand)   # NEW
    pres_cand = enrich_candidate(pres_cand)   # NEW
    # rest unchanged ...
```

---

### 5.5 kb_resolver.py — Add Village→Mandal Resolution Path

**Change: New trgm function using village_name_english**

```python
def _trgm_village_to_mandal(
    pool, state: str, district: str, token: str
) -> Optional[str]:
    """Look up a village name and return its parent sub_district_name (mandal)."""
    sql = """
        SELECT DISTINCT sub_district_name,
               similarity(lower(village_name_english), lower(%s)) AS sim
        FROM geo_reference
        WHERE lower(state_name)       = lower(%s)
          AND lower(district_name)    = lower(%s)
          AND village_name_english    IS NOT NULL
          AND sub_district_name       IS NOT NULL
          AND lower(village_name_english) %% lower(%s)
          AND similarity(lower(village_name_english), lower(%s)) >= 0.60
        ORDER BY sim DESC
        LIMIT 1
    """
    with pool.get_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (token, state, district, token, token))
            row = cur.fetchone()
            return row[0] if row else None
```

**Change: Insert village probe into resolve_kb() after existing locality/landmark mandal block**

The existing code already has a locality-as-mandal fallback. Extend it:

```python
# After existing: "mandal-from-locality (best-effort)"
if mandal is None and out.state and out.district:
    for tok in (cand.locality, cand.landmark, cand.ward, cand.street):  # ward+street NEW
        if not tok:
            continue
        # existing: direct trgm on sub_district_name
        m = _trgm_mandal(pool, out.state, out.district, tok)
        if m:
            mandal = m
            break
        # NEW: try village_name_english → sub_district_name
        m = _trgm_village_to_mandal(pool, out.state, out.district, tok)
        if m:
            mandal = m
            out.path += "+village"
            break
out.mandal = mandal
```

**Change: Add ward and street to AddressCandidate lookup tokens in _trgm_mandal calls**

The existing `SIM_MANDAL = 0.65` threshold is appropriate for village names (short, variable spelling). No threshold change needed.

---

### 5.6 llm_resolver.py — Enrich Prompt with Ward and Street

**Change: Extend prompt payload**

```python
def _build_prompt(cand: AddressCandidate, partial: ResolvedAddress) -> str:
    payload = {
        "input": {
            "state":       cand.raw_state,
            "district":    cand.raw_district,
            "mandal":      cand.raw_mandal,
            "country":     cand.raw_country,
            "locality":    cand.raw_locality,
            "landmark":    cand.raw_landmark,
            "nationality": cand.raw_nationality,
            # NEW — most valuable for LLM resolution of T3/T4 rows:
            "ward_colony": cand.raw_ward,
            "street":      cand.raw_street,
        },
        "kb_partial": {
            "country":  partial.country,
            "state":    partial.state,
            "district": partial.district,
            "mandal":   partial.mandal,
        },
    }
```

**Change: Update system prompt to reference ward_colony**

```python
_SYSTEM = (
    "You are an expert in Indian and global administrative geography. "
    "Given partial address fields including ward_colony and street text, "
    "return strict JSON: "
    '{"country": string|null, "state": string|null, "district": string|null, '
    '"mandal": string|null, "confidence": number between 0 and 1, "reason": string}. '
    "Use canonical English names. For Indian addresses, extract any identifiable "
    "locality, ward, or neighbourhood name from ward_colony or street and use it "
    "to infer the district and mandal. "
    "Prefer Indian official names (e.g. 'Telangana', 'Medchal Malkajgiri'). "
    "For Indian states always set country to 'India'. "
    "If a field cannot be determined, set it to null. Do not invent names. "
    "Output ONLY valid JSON."
)
```

---

### 5.7 etl_address.py — Country Propagation from Nationality

**Change: Add country-only mirror at the end of resolve_one()**

Many T5 rows only have `nationality = "India"` or `country = "India"` but no geo structure. The existing code does not write `country` in this case because `_is_worth_writing()` returns `False` when only country is set and state/district are null. That logic is correct for deciding whether to run KB/LLM. But the final write should still propagate country from nationality.

Add a dedicated nationality→country pass at the end of `resolve_one()`:

```python
def resolve_one(pool, row, llm):
    # ... existing logic ...

    # NEW: country-only propagation for records with no other geo signal
    # This catches: nationality="India", country="India" with null state/district
    _propagate_country(row, perm_out, pres_out, paths)

    return perm_out, pres_out, "|".join(paths) or "none"


def _propagate_country(row, perm_out, pres_out, paths):
    """Write country to both slots from nationality or existing country fields
    even when state/district are unresolvable."""
    kb = GeoKB.instance()

    # Derive country from nationality
    nat_country = None
    if row.nationality:
        nat_country = (
            kb.canon_country(row.nationality)
            or _nationality_to_country(row.nationality)
        )

    for out, other in [(perm_out, pres_out), (pres_out, perm_out)]:
        if out is None:
            continue
        if out.country:
            continue  # already set
        # try: country from other slot
        if other and other.country:
            out.country = other.country
            paths.append("M:country_cross")
        # try: from nationality
        elif nat_country:
            out.country = nat_country
            paths.append("M:country_nat")
        # try: from raw country field
        elif (row.perm_country if out.slot == "permanent" else row.pres_country):
            raw = row.perm_country if out.slot == "permanent" else row.pres_country
            out.country = kb.canon_country(raw) or raw.strip().title()
            paths.append("M:country_raw")


# Common nationality string → country name mapping
_NAT_MAP = {
    "indian": "India",
    "india":  "India",
    "hindu":  "India",   # common data entry error in Indian police records
    "muslim": None,       # religion, not nationality — skip
    "bangladeshi": "Bangladesh",
    "pakistani": "Pakistan",
    "nepali": "Nepal",
}

def _nationality_to_country(nat: str) -> Optional[str]:
    return _NAT_MAP.get(nat.lower().strip())
```

**Important note on the `hindu`/`muslim` mapping:** Indian police records frequently have `nationality = "Hindu"` (the religion, entered in the wrong field). The map converts "Hindu" to "India" which is the correct country for the vast majority of such records. "Muslim" is deliberately mapped to `None` because Muslim nationality is not country-specific.

---

### 5.8 reader.py — Fix PENDING_WHERE Quarantine Alignment

**Change: Make quarantine threshold consistent with Python constant**

```python
# In reader.py, replace the hardcoded `>= 3` in PENDING_WHERE:
-- OLD:
AND NOT EXISTS (
  SELECT 1 FROM etl_address_failures f
  WHERE f.person_id = persons.person_id::text
    AND f.attempted >= 3
)

-- NEW (parameterised via SQL parameter):
-- In count_pending() and fetch_batch(), pass MAX_RETRIES_ROW as a parameter
```

```python
# In reader.py module level:
import os
_QUARANTINE_THRESHOLD = int(os.environ.get("ADDRESS_ROW_RETRIES", "3"))

PENDING_WHERE_TEMPLATE = """
    (
      (... geo signal conditions ...)
      AND NOT (... fully resolved ...)
      AND NOT EXISTS (
        SELECT 1 FROM etl_address_failures f
        WHERE f.person_id = persons.person_id::text
          AND f.attempted >= {threshold}
      )
    )
"""

def _pending_where():
    return PENDING_WHERE_TEMPLATE.format(threshold=_QUARANTINE_THRESHOLD)
```

---

## 6. Resolution Strategy Per Data Pattern

This section maps each pattern found in the CSV to the resolution path after all fixes are applied.

### Pattern A — T1: Already Complete (Rajinikanth, Naveen reddy, Rambo, etc.)

**Before:** state + district + mandal all set, ETL writes nothing (idempotent).
**After:** Same. No change. The ETL correctly detects these are resolved and skips.
**Action needed:** None. The `PENDING_WHERE` fully-resolved exclusion handles these.

---

### Pattern B — T2: State + District known, Mandal missing (Jampala Sham — KHAMMAM)

**Before:** ETL reads state and district, KB resolver tries trgm on `sub_district_name` but has no locality token to work from. LLM is invoked with empty locality. LLM may or may not guess the mandal.
**After (no structural change):** Same outcome — no new signal is available for Khammam mandal inference. The ETL correctly records this as `failed_no_resolution` or writes with null mandal.
**Recommendation:** For district-only records with no locality signal, accept partial resolution. Write `state` and `district`, leave `mandal` null. Mark in `etl_address_failures` with reason `"partial_no_mandal_signal"` rather than the generic `"no_resolution"`, so future data enrichment can target these specifically.

---

### Pattern C — T3a: State known, address in street_road_no (Vijay Kumar, Md. Abdul Hafeez)

Raw data: `present_state_ut = TELANGANA`, `present_house_no = "Bhagath Singh Nagar, Suraram Colony"`

**Before:** `house_no` not read. `street` not read. ETL has only state. KB finds nothing. LLM prompt has only `state=Telangana`. LLM output is unreliable.
**After:**
1. `reader.py` reads `present_street_road_no` = "Bhagath Singh Nagar, Suraram Colony"
2. `normalize.py` `_merge_locality()` produces token `"Bhagath Singh Nagar Suraram Colony"`
3. `enrich_candidate()` matches "suraram" in `LOCALITY_TO_DISTRICT_MANDAL` → fills district = Medchal Malkajgiri, mandal = Quthbullapur
4. KB resolver validates and confirms
5. LLM skipped (already complete)
**Expected result:** `present_district = Medchal Malkajgiri`, `present_area_mandal = Quthbullapur`

> **Note:** `house_no` is not in the SELECT because it rarely contains locality-identifiable text and is very noisy. However `street_road_no` often contains the colony name in Indian police record entry practice.

---

### Pattern D — T3b: State known, locality in ward_colony (Sheikh Sameer, Dhotula Prithviraj)

Raw data: `present_state_ut = TELANGANA`, `present_ward_colony = "Rayanch Enclave"`, `permanent_ward_colony = "Mamillagudem"` (Sheikh Sameer)

**Before:** `ward_colony` not read. ETL has only state. Resolution fails.
**After:**
1. `reader.py` reads `ward_colony` for both slots
2. `normalize.py` creates locality token from ward
3. `enrich_candidate()` matches "rayanch enclave" → Medchal Malkajgiri / Medipally; "mamillagudem" → Nalgonda / Miryalaguda
4. For Dhotula Prithviraj (only "Rayanch Enclave"): district + mandal resolved via enrichment
5. For Sheikh Sameer: **present** side resolves from ward → perm mirrors from present
**Expected result for Sheikh Sameer:** permanent gets state=Telangana from DB, present gets district/mandal from ward "Rayanch Enclave". Mirror propagates permanent district from present.

---

### Pattern E — T4: No structured geo, locality in ward_colony only (Yesu Raju Lowdia)

Raw data: `present_country = India`, `present_ward_colony = "Room No. 6, Block-44, RGK, Jagadgirigutta"`

**Before:** Record **not even fetched** because `PENDING_WHERE` does not include `ward_colony` as a geo signal.
**After:**
1. `PENDING_WHERE` updated — record enters the pipeline
2. `reader.py` reads ward_colony = "Room No. 6, Block-44, RGK, Jagadgirigutta"
3. `enrich_candidate()` matches "jagadgirigutta" → Telangana / Medchal Malkajgiri / Quthbullapur; also "rgk" is listed
4. KB resolver confirms state + district + mandal against `geo_reference`
5. Country already set = "India"
**Expected result:** All four fields resolved for both slots via enrichment + KB confirmation.

---

### Pattern F — T3c: State known, no locality signal (Suresh — ANDHRA PRADESH)

Raw data: `present_state_ut = ANDHRA PRADESH`, nothing else

**Before:** ETL reads state, KB confirms it, country = India inferred from state. No district or mandal signal. LLM invoked with `state=Andhra Pradesh`, no locality. LLM budget consumed for a nearly impossible resolution.
**After:** 
1. Enrichment stage finds no useful tokens
2. LLM prompt now includes ward/street (both null here) — still no improvement
3. **Recommendation:** Skip LLM for records where only state is known and there is zero free-text signal. Add a new `has_enough_for_llm()` check:

```python
def has_enough_for_llm(cand: AddressCandidate) -> bool:
    """Returns True only if there is non-state signal to give the LLM."""
    return bool(
        cand.district or cand.mandal or cand.locality
        or cand.landmark or cand.ward or cand.street
    )
```

Call in `resolve_one()`:
```python
if not kb_p.is_complete and llm is not None and has_enough_for_llm(perm_cand):
    kb_p = llm.resolve(perm_cand, kb_p)
```

This preserves LLM budget for cases where it can actually help.

---

### Pattern G — T5: Only country = India (Sameer, Thuniki Rakesh, etc.)

Raw data: `nationality = India` or `present_country = India`, nothing else

**Before:** ETL fetches record (nationality counts as geo signal in current PENDING_WHERE). KB finds no match. LLM invoked, returns null for everything because `state=None`. Record logged as `failed_no_resolution`.
**After:**
1. Country propagation pass in `resolve_one()` writes `country = India` to both `permanent_country` and `present_country` where null (using the raw country or nationality value)
2. State/district/mandal remain null — that is correct; we do not invent data
3. Record is marked `failed_no_resolution` for state/district only — not quarantined since the country write was valid
4. **New failure reason:** Use `"insufficient_geo_signal"` for T5 records so they are distinguishable from genuine processing failures

---

## 7. New Failure Reason Taxonomy

The current code uses two reasons: `"no_resolution"` and `"unexpected"`. Expand to enable targeted re-runs after data enrichment:

| Reason | Meaning | Re-run candidate? |
|--------|---------|-------------------|
| `no_resolution` | (removed — too generic) | — |
| `partial_no_mandal_signal` | State + district written; mandal has no source signal | Yes, after locality data enrichment |
| `insufficient_geo_signal` | Only country/nationality available; state/district impossible | No |
| `state_only_no_locality` | State known but no free-text locality to derive district from | Yes, if FIR locality data added later |
| `kb_and_llm_rejected` | Both KB and LLM returned nothing or unverifiable values | Yes, with relaxed KB thresholds |
| `unexpected` | Exception during processing | Yes, after fix |

Update `record_failure()` in `failures.py` to accept these reasons. Update `process_record()` in `etl_address.py` to classify before logging:

```python
def _classify_failure(perm_cand, pres_cand, perm_out, pres_out) -> str:
    has_signal = perm_cand.has_any_signal or pres_cand.has_any_signal
    has_state  = bool((perm_out and perm_out.state) or (pres_out and pres_out.state))
    has_dist   = bool((perm_out and perm_out.district) or (pres_out and pres_out.district))

    if not has_signal:
        return "insufficient_geo_signal"
    if has_state and has_dist:
        return "partial_no_mandal_signal"
    if has_state:
        return "state_only_no_locality"
    return "kb_and_llm_rejected"
```

---

## 8. KB Confidence Thresholds — Tuning Recommendations

Based on the sample data, the current thresholds in `kb_resolver.py` are appropriate for standard addresses but may be too tight for Telangana-specific text where district/mandal names appear in combined strings:

| Field | Current threshold | Recommended | Rationale |
|-------|------------------|-------------|-----------|
| `SIM_STATE` | 0.85 | 0.80 | "ANDHRA PRADESH" vs "Andhra Pradesh" should match; 0.85 sometimes misses case variants |
| `SIM_DISTRICT` | 0.80 | 0.75 | "MEDCHAL MALKAJGIRI" vs "Medchal-Malkajgiri" (hyphen form) needs room |
| `SIM_MANDAL` | 0.65 | 0.65 | Correct; short names need tolerance |
| `SIM_COUNTRY_STATE` | 0.80 | 0.80 | Fine |
| `SIM_COUNTRY` | 0.70 | 0.70 | Fine |
| Village (new) | — | 0.60 | Set conservatively for village names — more false positives possible |

---

## 9. Implementation Phases

### Phase 1 — Signal Collection (1–2 days, zero risk)

Read-side only. No change to resolution logic or write logic.

- [ ] `types.py` — add 6 new fields to `PersonRow` and `AddressCandidate`
- [ ] `reader.py` — expand SELECT to 20 columns
- [ ] `reader.py` — update `PENDING_WHERE` to include ward/street as signals
- [ ] `normalize.py` — implement `_merge_locality()` and use in `build_candidates()`
- [ ] Run in `--dry-run` mode and verify T4 rows (Yesu Raju pattern) now appear in pending set

### Phase 2 — Enrichment + Village Path (2–3 days)

- [ ] `normalize.py` — implement `LOCALITY_TO_DISTRICT_MANDAL` and `enrich_candidate()`
- [ ] `etl_address.py` — call `enrich_candidate()` in `resolve_one()` before KB resolve
- [ ] `kb_resolver.py` — add `_trgm_village_to_mandal()` function
- [ ] `kb_resolver.py` — extend mandal fallback loop with village probe + ward/street tokens
- [ ] `llm_resolver.py` — add `raw_ward` and `raw_street` to prompt payload and system prompt
- [ ] Run dry-run on full dataset; verify T3/T4 rows resolve correctly
- [ ] Verify no T1 rows are modified (idempotency)

### Phase 3 — Country Propagation + Failure Classification (1 day)

- [ ] `etl_address.py` — implement `_propagate_country()` and `_nationality_to_country()`
- [ ] `etl_address.py` — implement `has_enough_for_llm()` guard
- [ ] `etl_address.py` — implement `_classify_failure()` and wire into `process_record()`
- [ ] `failures.py` — no schema change needed; reason field already text
- [ ] `reader.py` — align `PENDING_WHERE` quarantine threshold with `MAX_RETRIES_ROW` env var
- [ ] Verify T5 rows get `country = India` written and correct failure reason logged

### Phase 4 — Validation (1 day)

- [ ] Run full ETL on a backup/staging copy of the persons table
- [ ] Query: `SELECT COUNT(*), SUM(CASE WHEN permanent_district IS NULL THEN 1 END) FROM persons` — compare before/after
- [ ] Query: Check all records where enrichment path was used (`path LIKE '%M:country_nat%'` etc.) — spot-check 20 manually
- [ ] Verify `etl_address_failures` classification distribution matches expected T5 volume
- [ ] Confirm no records have `permanent_state_ut` overwritten with a different state than was originally there (idempotency check)

---

## 10. Metrics to Track Post-Deployment

Add these to the final stats log in `run()`:

```python
logger.info("  enriched_by_locality  : %d", snap.get("enriched_locality", 0))
logger.info("  enriched_by_village   : %d", snap.get("enriched_village", 0))
logger.info("  country_from_nat      : %d", snap.get("country_from_nat", 0))
logger.info("  llm_skipped_no_signal : %d", snap.get("llm_skipped_no_signal", 0))
logger.info("  failure_partial       : %d", snap.get("failed_partial_no_mandal", 0))
logger.info("  failure_insufficient  : %d", snap.get("failed_insufficient_geo", 0))
```

Track the null-rate reduction metric as the primary KPI:

```sql
-- Run before and after each phase
SELECT
  COUNT(*)                                                        AS total_persons,
  SUM(CASE WHEN permanent_district  IS NULL THEN 1 END)          AS null_perm_district,
  SUM(CASE WHEN permanent_state_ut  IS NULL THEN 1 END)          AS null_perm_state,
  SUM(CASE WHEN permanent_country   IS NULL THEN 1 END)          AS null_perm_country,
  SUM(CASE WHEN permanent_area_mandal IS NULL THEN 1 END)        AS null_perm_mandal,
  ROUND(100.0 * SUM(CASE WHEN permanent_district IS NULL THEN 1 END) / COUNT(*), 2) AS pct_null_dist
FROM persons;
```

---

## 11. Summary of Expected Improvements Against Sample CSV

| Person | Tier | Before | After Phase 1+2 | After Phase 3 |
|--------|------|--------|-----------------|---------------|
| Paidipally Sri Pranay | T1 | Complete | No change | No change |
| Vijay Kumar | T3 | state only | + district + mandal via street token | |
| Md. Abdul Hafeez | T3 | state only | + district + mandal via street token | |
| Sameer | T5 | country only | No change | country written cleanly |
| Thuniki Rakesh | T5 | country only | No change | country written cleanly |
| Atthena Madhu | T5 | country only | No change | country written cleanly |
| Poosala Venkatesh | T5 | country only | No change | country written cleanly |
| Rajinikanth Palsiwar | T1 | Complete | No change | No change |
| Naveen reddy Kadgam | T1 | Complete | No change | No change |
| Rambo | T1 | Complete | No change | No change |
| Yesu Raju Lowdia | T4 | **not fetched** | Fetched + fully resolved via ward token | |
| arvind josh | T5 | country only | No change | country written |
| bobby | T5 | country only | No change | country written |
| S.Vikrush | T5 | country only | No change | country written |
| Suresh | T3 | state (AP) only | LLM skipped (no signal) — no worse | failure classified correctly |
| Jampala Sham | T2 | state + district | partial written, failure classified | |
| Modapalli Daya Sagar | T1 | Complete | No change | No change |
| Veerabattini Phani Kishore | T1 | Complete | No change | No change |
| Sheikh Sameer | T3 | perm-state only | ward→district+mandal; mirror to present | |
| Dhotula Prithviraj | T3 | state only | ward→district+mandal resolved | |

**Estimated null-field reduction in this sample: ~8 of 14 non-T1 records gain at least district-level resolution.**
