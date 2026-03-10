# Technical Specification - Brief Facts Accused ETL: NULL Elimination

## 1. Technical Context

- **Language**: Python 3.x
- **Key Libraries**: `psycopg2`, `fuzzywuzzy`, `langchain`, `pydantic`
- **Entry Point**: `brief_facts_accused/accused_type.py` (main + process_crimes)
- **LLM Chain**: `brief_facts_accused/extractor.py` (2-pass: name ID → detail extraction)
- **DB Layer**: `brief_facts_accused/db.py`
- **Retry Infrastructure**: `core/llm_service.py::invoke_extraction_with_retry` (already has 1-retry + correction prompt)

---

## 2. Root Cause Map

| Column | Root Cause | Fix Location |
|---|---|---|
| `accused_type` NULL | `classify_accused_type` returns `transporter`/`producer` which are invalid schema values and get silently nullified; `unknown` has no meaningful catch-all | `extractor.py` |
| `status` stuck as `"unknown"` | `accused_type.py` maps `accused_type=unknown` → NULL but has NO equivalent logic for `status` | `accused_type.py` |
| `person_id`/`accused_id` NULL | Fuzzy threshold 85 too strict for scoped per-crime search (small candidate set) | `accused_type.py` |
| `key_details` always NULL | `AccusedDetails` model (Pass 2) has no `key_details` field; never extracted or populated | `extractor.py` |
| `gender` partial NULL | DB-match enrichment exists but name-based heuristic result not applied on DB enrichment path | Already handled; no change needed |

---

## 3. Implementation Approach

### 3.1 Fix `accused_type` - Remap Non-Schema Values (`extractor.py`)

**File**: `brief_facts_accused/extractor.py`  
**Function**: `classify_accused_type`

Remap the two invalid return values to schema-valid equivalents:
- `transporter` → `supplier` (transporting drugs is a supply-chain role)
- `producer` → `manufacturer` (cultivating/growing is manufacturing)

Remove `transporter` and `producer` from the `AccusedExtraction` model's `accused_type` description field to keep the schema contract clean.

**Extra keywords to add** for better coverage:

| Category | New Keywords to Add |
|---|---|
| `peddler` | "pushing", "hawking", "street sale", "spot sale", "trafficking", "trafficking in" |
| `consumer` | "personal consumption", "consumed", "using drugs", "drug user", "under influence" |
| `supplier` | "transporting", "carrying", "delivering", "courier", "driver", "dispatch", "shipment" (fold transporter → supplier) |
| `organizer_kingpin` | "ringleader", "boss", "gang leader", "commander", "in-charge", "leader of", "overseeing" |
| `harbourer` | "hiding", "hiding place", "stash house", "storing", "stored at", "kept at" |
| `processor` | "processing", "packaging", "packed", "repacked", "mixing", "adulteration" |
| `financier` | "backer", "lender", "loan for drugs", "money for purchase", "sponsored" |
| `manufacturer` | "producing", "producer", "growing", "cultivator", "cultivated", "grown", "grower", "farming", "farm" |

### 3.2 Fix `status` NULL Mapping (`accused_type.py`)

**File**: `brief_facts_accused/accused_type.py`  
**Function**: `process_crimes`

After line 153-154 (which maps `accused_type` to NULL), add an equivalent guard:
```python
if data.get('status') == 'unknown':
    data['status'] = None
```

Also expand status detection keywords in `extract_accused_info` (`extractor.py` lines 683-686):
- **arrested**: add "detained", "nabbed", "held", "seized person", "taken into custody", "remanded"
- **absconding**: add "evading", "fled", "on the run", "not traceable", "not found", "missing", "failed to appear"

### 3.3 Fix Fuzzy Threshold (`accused_type.py`)

**File**: `brief_facts_accused/accused_type.py`  
**Function**: `find_best_match`

Lower default threshold from `85` → `75`. The existing `fetch_existing_accused_for_crime` already scopes the candidate list to the specific `crime_id` via `accused JOIN persons WHERE a.crime_id = %s`, so a lower threshold is safe.

### 3.4 Add `key_details` to Pass 2 Extraction (`extractor.py`)

**File**: `brief_facts_accused/extractor.py`  
**Model**: `AccusedDetails` (Pass 2 intermediate)  
**Prompt**: `PASS2_PROMPT`

1. Add `key_details: Optional[str]` field to `AccusedDetails` model with description "Notable facts, quantities, items seized, or context not captured elsewhere".
2. Add instruction to `PASS2_PROMPT`: "6. Extract Key Details: Note quantities of drugs, type of substance, vehicle used, or any other specific fact relevant to this accused."
3. In `extract_accused_info`, pass `d_obj.key_details` to the final `AccusedExtraction` object alongside other fields.

---

## 4. Source Code Structure Changes

| File | Changes |
|---|---|
| `extractor.py` | (1) `classify_accused_type`: remap transporter→supplier, producer→manufacturer; add keywords. (2) `AccusedDetails`: add `key_details` field. (3) `PASS2_PROMPT`: add key_details instruction. (4) `extract_accused_info`: propagate `key_details`. (5) Status detection: more keywords. |
| `accused_type.py` | (1) Lower fuzzy threshold 85→75. (2) Add `status` unknown→None mapping. |
| `db.py` | No changes. |

---

## 5. Delivery Phases

1. **Phase A**: Fix `accused_type` schema compliance + expand keywords (extractor.py)
2. **Phase B**: Fix `status` unknown→NULL mapping + expand status keywords (accused_type.py + extractor.py)
3. **Phase C**: Lower fuzzy threshold (accused_type.py)
4. **Phase D**: Add `key_details` to Pass 2 extraction (extractor.py)

---

## 6. Verification Approach

- Run `python accused_type.py` locally with an `input.txt` containing a known `crime_id`.
- Confirm via SQL: `SELECT accused_type, status, person_id, key_details FROM brief_facts_accused WHERE crime_id = '<test_id>';`
- Confirm no `"unknown"` strings remain in `accused_type` or `status` columns.
- Confirm `key_details` is populated where the FIR has investigative detail.
