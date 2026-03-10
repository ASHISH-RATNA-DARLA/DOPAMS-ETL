# Full SDD workflow

## Workflow Steps

### [x] Step: Requirements

Create a Product Requirements Document (PRD) based on the feature description.

1. Review existing codebase to understand current architecture and patterns
2. Analyze the feature definition and identify unclear aspects
3. Ask the user for clarifications on aspects that significantly impact scope or user experience
4. Make reasonable decisions for minor details based on context and conventions
5. If user can't clarify, make a decision, state the assumption, and continue

Save the PRD to `d:\DOPAMS\Toystack\dopams-etl-pipelines\.zencoder\chats\7266eee5-a4f8-4c04-835d-b0132452af53/requirements.md`.

### [x] Step: Technical Specification

Create a technical specification based on the PRD in `d:\DOPAMS\Toystack\dopams-etl-pipelines\.zencoder\chats\7266eee5-a4f8-4c04-835d-b0132452af53/requirements.md`.

1. Review existing codebase architecture and identify reusable components
2. Define the implementation approach

Save to `d:\DOPAMS\Toystack\dopams-etl-pipelines\.zencoder\chats\7266eee5-a4f8-4c04-835d-b0132452af53/spec.md`.

### [x] Step: Planning

Break down the spec into concrete implementation tasks. Each task is one coherent unit of work targeting a specific file and function.

---

## Implementation Tasks

### [x] Task 1: Fix `accused_type` — remap invalid schema values in `extractor.py`

**File**: `brief_facts_accused/extractor.py`  
**Function**: `classify_accused_type`

- Change the `transporter` branch: return `"supplier"` instead of `"transporter"`
- Change the `producer` branch: return `"manufacturer"` instead of `"producer"`
- Remove `transporter` and `producer` from the `AccusedExtraction.accused_type` field description
- Add new keywords per category (see spec.md §3.1 table)

**Verification**: After change, no call to `classify_accused_type` should ever return `"transporter"` or `"producer"`.

---

### [x] Task 2: Expand `accused_type` keyword coverage in `extractor.py`

**File**: `brief_facts_accused/extractor.py`  
**Function**: `classify_accused_type`

Add keywords to each existing category block:

- `peddler`: add `"pushing"`, `"hawking"`, `"street sale"`, `"spot sale"`, `"trafficking"`, `"peddling"`
- `consumer`: add `"personal consumption"`, `"consumed"`, `"using drugs"`, `"drug user"`, `"under influence"`, `"for own use"`
- `supplier` (absorbs transporter): add `"transporting"`, `"carrying"`, `"delivering"`, `"courier"`, `"driver"`, `"dispatch"`, `"shipment"`, `"transit"`
- `organizer_kingpin`: add `"ringleader"`, `"boss"`, `"gang leader"`, `"in-charge"`, `"overseeing"`, `"coordinating"`, `"managing the operation"`
- `harbourer`: add `"hiding"`, `"hiding place"`, `"stash house"`, `"storing at"`, `"stored at"`, `"kept at"`, `"concealing"`
- `processor`: add `"processing"`, `"packaging"`, `"packed"`, `"repacked"`, `"mixing"`, `"adulteration"`, `"weighing and packing"`
- `financier`: add `"backer"`, `"sponsored"`, `"money for purchase"`, `"provided money"`, `"lender"`
- `manufacturer`: add `"growing"`, `"cultivator"`, `"cultivated"`, `"grown"`, `"grower"`, `"farming"`, `"farm"`, `"producing"`, `"producer"`, `"cultivation"`

**Verification**: Run the inline `__main__` test in `extractor.py` and confirm roles like "transporting drugs" → `supplier`, "cultivating ganja" → `manufacturer`.

---

### [x] Task 3: Expand `status` detection keywords in `extractor.py`

**File**: `brief_facts_accused/extractor.py`  
**Function**: `extract_accused_info` (the inline status detection block, lines ~683–686)

Expand the keyword sets for the inline status detection:

- **arrested**: add `"detained"`, `"nabbed"`, `"held"`, `"taken into custody"`, `"remanded"`, `"produced before court"`, `"surrendered"`
- **absconding**: add `"evading"`, `"fled"`, `"on the run"`, `"not traceable"`, `"not found"`, `"missing"`, `"could not be traced"`, `"yet to be arrested"`

Also check the `clean_name` (after clean_accused_name) for these keywords, not just `role_desc`.

**Verification**: Test inputs with "nabbed", "fled the scene" confirm correct status assignment.

---

### [x] Task 4: Map `status = "unknown"` to `None` in `accused_type.py`

**File**: `brief_facts_accused/accused_type.py`  
**Function**: `process_crimes`

After the existing block (lines 153–154):
```python
if data.get('accused_type') == 'unknown':
    data['accused_type'] = None
```

Add:
```python
if data.get('status') == 'unknown':
    data['status'] = None
```

**Verification**: Confirm `status` column in DB has no `"unknown"` string values after run.

---

### [x] Task 5: Lower fuzzy match threshold from 85 → 75 in `accused_type.py`

**File**: `brief_facts_accused/accused_type.py`  
**Function**: `find_best_match`

Change default parameter:
```python
def find_best_match(extracted_name, existing_records, threshold=75):
```

This is safe because `existing_records` is already scoped to the specific `crime_id` (small candidate set of only persons linked to this case).

**Verification**: Confirm `person_id` and `accused_id` are populated for crimes where an accused match exists in the DB.

---

### [x] Task 6: Add `key_details` to Pass 2 extraction in `extractor.py`

**File**: `brief_facts_accused/extractor.py`

Three sub-changes:

1. **`AccusedDetails` model** — add field:
   ```python
   key_details: Optional[str] = Field(default=None, description="Quantities seized, substance type, vehicle used, or other specific investigative facts")
   ```

2. **`PASS2_PROMPT`** — add instruction after "5. Extract Role.":
   ```
   6. Extract Key Details: Note any quantities of substances, type of drugs, vehicle used, items seized, or other specific investigative facts unique to this accused. Be concise.
   ```

3. **`extract_accused_info`** — in the final object construction block, propagate:
   ```python
   key_details = d_obj.key_details if d_obj else None
   ```
   And pass `key_details=key_details` to `AccusedExtraction(...)`.

**Verification**: After run, `key_details` in DB is non-null for FIRs that mention quantities or seized items.

---

### [ ] Step: Final Verification

After all tasks complete:

1. Run `python accused_type.py` with a test `input.txt` containing a known `crime_id`
2. Query:
   ```sql
   SELECT accused_type, status, person_id, key_details, gender
   FROM brief_facts_accused
   WHERE crime_id = '<test_id>';
   ```
3. Confirm:
   - No `"unknown"` strings in `accused_type` or `status`
   - `person_id` populated for matched records
   - `key_details` non-null where FIR has substance/quantity details
   - No `transporter` or `producer` values in `accused_type`
