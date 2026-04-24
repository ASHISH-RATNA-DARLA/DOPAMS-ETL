# Brief Facts AI - Expected Behavior & Architecture

**Module:** `/data-drive/etl-process-dev/brief_facts_ai/`  
**Purpose:** AI-powered deduplication and fact extraction for accused persons in crimes  
**Table Name:** `brief_facts_ai` (unified output)  
**Input Window:** ETL_FROM_DATE → ETL_TO_DATE (injected by master)

---

## Overview: What Brief Facts AI Does

Brief Facts AI takes **raw accused data** from the crimes table and produces a **unified, deduplicated, AI-enriched** record of who is involved in each crime.

```
INPUT (from crimes table):
  - Crime ID, FIR, brief facts (text description)
  - Raw accused persons (multiple rows per crime)
  - Person data (names, gender, age, address, person_codes)

PROCESSING (3-branch hybrid approach):
  ├─ Branch A: DB has accused + person_id exists → Use DB, LLM enriches
  ├─ Branch B: DB has accused + person_id is NULL → LLM extracts everything
  └─ Branch C: No accused in DB → LLM only (extract from text)

OUTPUT (per crime, per accused):
  - Deduplicated accused record with canonical person_id
  - Extracted roles (perpetrator, supplier, handler, etc.)
  - Detected drugs (if mentioned)
  - CCL classification (crime classification)
  - Status (Charged, Convicted, Acquitted, Absconding)
```

---

## Key Input Data

### 1. Crimes Table Columns (INPUT)
```sql
crime_id          BIGINT          -- PK, unique identifier
fir_number        VARCHAR         -- FIR reference
date_of_crime     TIMESTAMP       -- When crime occurred
brief_facts       TEXT            -- Narrative description (50-2000 chars)
ps_code           VARCHAR         -- Police station code (e.g., "A-001")
district          VARCHAR         -- District name
location          VARCHAR         -- Crime location
ipc_acts          JSONB           -- IPC acts violated
ipc_sections      JSONB           -- IPC sections violated
```

### 2. Accused Table Columns (INPUT)
```sql
accused_id        UUID            -- Unique per accused (per crime)
crime_id          BIGINT (FK)     -- Links to crimes
person_id         UUID            -- FK to persons table (NULL = unknown)
person_code       VARCHAR         -- Police station person code (e.g., "A-123")
full_name         VARCHAR         -- Accused's name
alias_name        VARCHAR         -- Nicknames
gender            VARCHAR         -- M/F/Other
age               INT             -- Age at crime
address           VARCHAR         -- Residence
phone_numbers     VARCHAR         -- Contact (if available)
status            VARCHAR         -- Arrested/Absconding/Held
role_in_crime     VARCHAR         -- Initial role classification
```

### 3. Persons Table (Canonical Reference)
```sql
person_id         UUID (PK)       -- Canonical person identifier
ps_code           VARCHAR         -- Police station code
full_name         VARCHAR         -- Standard name
gender            VARCHAR         -- Deduplicated gender
dob               DATE            -- Date of birth (if known)
address           VARCHAR         -- Standard address
person_code       VARCHAR         -- Official person identifier
```

---

## Processing Modes

### **Mode 1: Backfill (First Run)**
- Triggered when `is_backfill_complete() == False`
- Scans **all unprocessed crimes** in the crimes table
- No processing history exists yet
- Output: First `brief_facts_ai` records for all crimes

```sql
-- Query: Find crimes NOT YET in brief_facts_ai
SELECT * FROM crimes c
WHERE NOT EXISTS (
  SELECT 1 FROM brief_facts_ai bfa 
  WHERE bfa.crime_id = c.crime_id
)
ORDER BY c.date_of_crime DESC
```

### **Mode 2: Daily Incremental**
- Triggered when `is_backfill_complete() == True`
- Runs every night automatically
- Processes only **new or modified crimes** since last run
- Much faster than backfill (only ~100-200 new crimes/day)

```sql
-- Query: Incremental unprocessed crimes
SELECT c.* FROM crimes c
WHERE NOT EXISTS (
  SELECT 1 FROM brief_facts_ai bfa 
  WHERE bfa.crime_id = c.crime_id
)
LIMIT 30  -- batch size
```

### **Mode 3: Manual Re-run (via input.txt)**
- If `/data-drive/etl-process-dev/brief_facts_ai/input.txt` exists
- Processes ONLY the crime_ids listed (one per line)
- Useful for re-processing specific crimes

```
# input.txt
12345
12346
12347
```

---

## The 3-Branch Processing Architecture

### **Branch A: "Accused in DB, Person Known"**
**Condition:** DB has accused rows AND at least one has `person_id IS NOT NULL`

**Flow:**
```
1. Fetch all accused rows for crime from DB
   └─ Skip rows where person_id IS NULL (spec rule)

2. For each accused:
   a. Canonical person lookup:
      - Match by (person_id, ps_code, full_name)
      - Resolve canonical_person_id from persons table
   
   b. LLM extraction (Claude API):
      - Input: brief_facts text + accused name + crime narrative
      - Extract: roles, drugs, CCL, status confirmation
      - Examples:
        * "Raj Kumar was the handler" → role = "handler"
        * "...supplied 2kg ganja..." → drugs = ["ganja", "2kg"]
        * "...convicted under IPC-141..." → ccl = "organized crime"
        * "...was arrested..." → status = "Charged"
   
   c. Person code assignment:
      - Type = "Accused" or "CCL" → use accused_code directly
      - Type = "Known" or "Respondent" → LLM assigns (A1, A2, etc.)
   
   d. Merge all data:
      - DB data (from accused + persons tables)
      - LLM-extracted roles/drugs/status
      - Computed fields (age, canonical_person_id)

3. Deduplication (within crime):
   - If multiple accused rows match same person:
     * Keep most complete row
     * Merge roles/drugs across duplicates
   
4. Insert into brief_facts_ai
   - One row per unique accused
   - Columns: crime_id, accused_id, canonical_person_id, roles, drugs, etc.
```

**Example Output:**
```sql
INSERT INTO brief_facts_ai (
  crime_id, accused_id, canonical_person_id, person_code, full_name, 
  role_in_crime, drugs, is_ccl, status, etl_run_id
) VALUES (
  12345,                          -- crime_id
  'uuid-abc123',                  -- accused_id (from DB)
  'uuid-person-001',              -- canonical_person_id (from persons table)
  'A-123',                        -- person_code (assigned by DB or LLM)
  'Raj Kumar',                    -- full_name
  '["perpetrator", "supplier"]',  -- roles (LLM extracted)
  '["ganja"]',                    -- drugs (LLM extracted)
  false,                          -- is_ccl
  'Charged',                      -- status (LLM confirmed or DB)
  'run-uuid-001'                  -- etl_run_id (processing run)
);
```

---

### **Branch B: "Accused in DB, Person Unknown"**
**Condition:** DB has accused rows BUT all have `person_id IS NULL`

**Flow:**
```
1. Fetch all accused rows for crime from DB
   └─ All person_id values are NULL

2. LLM extracts comprehensive information:
   - Extract names from brief_facts text
   - Match extracted names to DB accused rows
   - Extract roles, drugs, status for each
   - Assign person_codes (LLM assigns A1, A2, A3 as no DB codes exist)

3. Canonical person resolution:
   - For each accused, generate canonical_person_id:
     * Hash of (full_name, gender, ps_code)
     * Used to link to persons table (if exists)
   
4. Merge and insert:
   - Use LLM-extracted fields as primary source
   - Supplement with DB accused data where available
   - Mark as "LLM_PERSON_INFERRED" in metadata
```

**When This Happens:**
- Accused record exists but person was never matched during persons ETL stage
- Person doesn't exist in persons table yet
- LLM must infer identity from text

---

### **Branch C: "No Accused in DB, Text-Only Extraction"**
**Condition:** No accused rows in DB for this crime (or all filtered by rules)

**Flow:**
```
1. LLM reads brief_facts text entirely
   - Extract all person names mentioned
   - Extract roles: "A and B committed theft" → both perpetrators
   - Extract drugs: "10kg heroin recovered" → drug mention
   - Extract status: "2 arrested, 1 absconding"

2. Create synthetic records:
   - Generate UUID for each extracted person (not in DB)
   - Create entries in brief_facts_ai
   - Mark as "TEXT_ONLY_EXTRACTION"
   - Status: Tentative (not confirmed by arrest record)

3. When Accused Arrives Later:
   - Master checkpoint tracks which crimes were text-only
   - When accused records arrive (later in same day or next day):
     * These text-only records are INVALIDATED
     * Branch A/B processing takes over
     * Replaces text-only rows with DB-sourced rows
```

**Example Scenario:**
```
Day 1, Evening ETL:
  Crime 12345 has no accused records yet (investigation ongoing)
  Brief Facts: "Raj Kumar and Priya Singh committed theft..."
  Output (Branch C):
    - Raj Kumar (synthetic ID, role=perpetrator, status=extracted)
    - Priya Singh (synthetic ID, role=perpetrator, status=extracted)

Day 2, Morning ETL:
  Accused records now exist (suspects arrested overnight)
  Branch A/B kicks in, replaces synthetic records with DB records
  Output (Branch A):
    - Raj Kumar (DB-sourced ID, confirmed perpetrator)
    - Priya Singh (DB-sourced ID, confirmed perpetrator)
```

---

## LLM Extraction Details (All Branches)

### **What Claude Extracts:**

#### 1. **Roles** (primary classification)
The LLM analyzes brief_facts text for linguistic markers:

| Role | Markers | Example |
|------|---------|---------|
| **perpetrator** | "committed", "carried out", "was involved in", "along with" | "Raj and Priya committed theft" |
| **supplier** | "supplied", "sold", "provided", "gave", "procured" | "Raj supplied ganja to other accused" |
| **handler/broker** | "received", "handled", "handed over", "facilitated" | "Priya was handler for payments" |
| **harbourer** | "harboured", "sheltered", "aided", "abetted" | "Harbourer: provided shelter" |
| **financier** | "financed", "funded", "paid for", "sponsored" | "Financed the operation with 10k" |
| **informant** | "informed", "tipped off", "gave information" | "Tipped off police about location" |
| **absconding** | "absconding", "at large", "not apprehended" | "2 arrested, 1 still absconding" |

**Logic:**
```python
# For each accused name found in text:
1. Search for role markers in 100-char window around name mention
2. If "supplied from" in window: role = "supplier"
3. If "harboured" in window: role = "harbourer"
4. Else: role = "perpetrator" (default)
5. Merge if same person mentioned multiple times with different roles

# Flag false positives:
- "Raj Kumar" mentioned only as "informant of Raj Kumar" → skip
- "Priya Singh" mentioned only as "associate/reference" → don't create row
```

#### 2. **Drugs** (substance detection)
Extracted drug keywords are matched against KB:

```python
# Process:
1. LLM passes entire brief_facts to drug extractor
2. Drug KB: 379KB lookup of ~500 standard drugs + street names
3. Match: "ganja", "charas", "NDPS", "heroin", "cocaine", etc.
4. Quantity: Extract "2kg", "10 tablets", "500ml"
5. Output: [{raw_drug_name: "heroin", standard_name: "Heroin", qty: "10kg"}]
```

**Examples:**
```
Text: "10kg ganja and 5kg charas seized"
Output: [{name: "ganja", qty: "10kg"}, {name: "charas", qty: "5kg"}]

Text: "NDPS material found during search"
Output: [{name: "NDPS", qty: null}]

Text: "No narcotics involved"
Output: [{name: "NO_DRUGS_DETECTED"}]  ← Sentinel value
```

#### 3. **CCL (Criminal Code / Classification)**
Detects if accused is classified as organized crime/habitual criminal:

```python
# Markers:
- CCL mention: "accused is CCL", "CCL criminal"
- Organized crime: "operates gang", "organized smuggling ring"
- Habitual criminal: "history of", "repeat offender", "known criminal"

# Output: True if CCL, False otherwise
```

#### 4. **Status** (legal disposition)
Detects accused's current legal status:

| Status | Markers |
|--------|---------|
| **Charged** | "arrested", "accused", "apprehended", "remanded" |
| **Convicted** | "convicted", "sentenced", "found guilty", "serving time" |
| **Acquitted** | "acquitted", "discharged", "found not guilty" |
| **Absconding** | "absconding", "at large", "not apprehended", "whereabouts unknown" |

```python
# Logic per accused:
1. Check role first:
   - If role="absconding" → status="Absconding"
   - Else → analyze text for conviction/acquittal markers
   
2. Default to DB status if available (more authoritative)
3. LLM confirms/overrides if text explicitly contradicts
```

---

## Deduplication Strategy (Within Crime)

### **Problem:**
One person may appear multiple times in accused list:
```
Accused 1: "Raj Kumar s/o Ravi"
Accused 2: "Raj Kumar, 25 yrs, male"
→ Same person, two rows
```

### **Solution (Multi-Level Matching):**

```python
def is_same_crime_duplicate(row_a, row_b):
    """
    Conservative dedup: require name similarity PLUS corroborating fields.
    """
    # Level 1: Name check
    name_a = "raj kumar s/o ravi"  # normalized
    name_b = "raj kumar 25 years"   # normalized
    
    # Soundex match: R200 = R200 ✓
    # Jaro-Winkler: 0.95 (>0.85 threshold) ✓
    # Token set: {raj, kumar} = {raj, kumar} ✓
    if NOT name_match:
        return False  # Different names, not duplicates
    
    # Level 2: Corroboration (at least one must match)
    phone_a = "9876543210"
    phone_b = "9876543210"
    if phone_a == phone_b:
        return True  # Same phone → SAME PERSON
    
    age_a = 25
    age_b = 25
    gender_a = "M"
    gender_b = "M"
    address_sim = 0.8  # 80% token overlap
    
    if (age_a == age_b) AND (gender_a == gender_b) AND (address_sim >= 0.45):
        return True  # Age + gender + address match → SAME PERSON
    
    return False  # No corroboration
```

**When Duplicates Found:**
```
1. Keep row with most complete data
2. Merge roles: [perpetrator, supplier] + [perpetrator] → [perpetrator, supplier]
3. Merge drugs: [ganja] + [charas] → [ganja, charas]
4. Single INSERT into brief_facts_ai (not two)
```

---

## Canonical Person Resolution

### **Goal:**
Link each accused to the official `persons` table record (if exists).

### **Matching Hierarchy:**

```python
def resolve_canonical_person(accused_data):
    """Match accused to persons table using fuzzy logic."""
    
    # Tier 0: Direct match (fastest)
    if accused_data['person_id']:
        return accused_data['person_id']  # Already linked, use it
    
    # Tier 1: Fuzzy match by (name, gender, ps_code)
    candidates = query_persons_table(
        full_name=accused_data['full_name'],
        gender=accused_data['gender'],
        ps_code=accused_data['ps_code']
    )
    
    if len(candidates) == 1:
        return candidates[0]['person_id']  # Unique match ✓
    
    if len(candidates) > 1:
        # Multiple matches: use Soundex + Jaro-Winkler to pick best
        best = max(candidates, key=lambda c: _name_similarity(
            accused_data['full_name'],
            c['full_name']
        ))
        if similarity >= 0.85:
            return best['person_id']
        # Else: continue to Tier 2
    
    # Tier 2: Fuzzy match by (DOB, address, person_code)
    candidates = query_persons_table(
        dob=accused_data['dob'],
        address=accused_data['address'],
    )
    if best:
        return best['person_id']
    
    # Tier 3: No match found
    # Generate synthetic canonical_person_id = Hash(name, gender, ps_code)
    canonical_id = uuid5(
        uuid.NAMESPACE_DNS,
        f"{accused_data['full_name']}|{accused_data['gender']}|{accused_data['ps_code']}"
    )
    return canonical_id  # Synthetic, not in persons table
```

---

## Parallel Processing & Performance

### **Configuration:**
```
PARALLEL_LLM_WORKERS=2      # Number of concurrent Claude API calls
BATCH_COMMIT_SIZE=30        # Commit every 30 crimes (not every crime)
BATCH_SIZE=30               # Fetch 30 crimes per DB query
STEP_TIMEOUT_SEC=36000      # 10 hours per crime (for slow LLM calls)
```

### **Execution Flow:**

```
main()
  │
  ├─ Check backfill status
  │
  ├─ If backfill: Fetch ALL unprocessed crimes
  │  Else: Fetch only NEW crimes since last run
  │
  ├─ Batch fetch 30 crimes at a time
  │
  ├─ process_crimes_parallel(30 crimes)
  │  │
  │  ├─ Load drug KB once (379KB)
  │  │
  │  ├─ ThreadPoolExecutor with max_workers=2
  │  │  │
  │  │  ├─ Worker 1: Processing crime #12345
  │  │  │  ├─ Fetch accused from DB
  │  │  │  ├─ Classify branch (A/B/C)
  │  │  │  ├─ Call Claude API (blocks ~5-10 sec per accused)
  │  │  │  ├─ Extract drugs
  │  │  │  ├─ Build brief_facts_ai rows
  │  │  │  └─ SAVEPOINT (per-crime rollback)
  │  │  │
  │  │  ├─ Worker 2: Processing crime #12346
  │  │  │  ├─ (parallel execution, independent DB connections)
  │  │  │  ...
  │  │  │
  │  │  ├─ Results collected as_completed()
  │  │  │
  │  │  └─ Every 30 crimes: conn.commit() (batch commit)
  │  │     Savings: 50% fewer fsync operations
  │  │
  │  └─ Return to main(), fetch next 30 crimes
  │
  └─ Final commit (any remaining work)
```

### **Timing:**
```
Per crime (average):
  - Branch A (accused known): ~8-10 sec (LLM call dominant)
  - Branch B (person unknown): ~8-10 sec (LLM extracts all)
  - Branch C (text only): ~5 sec (no DB lookups)

Example: 1000 crimes with PARALLEL_LLM_WORKERS=2
  - Sequential: 1000 * 8 sec = 8000 sec ≈ 2.2 hours
  - Parallel (2 workers): 1000 * 8 / 2 = 4000 sec ≈ 1.1 hours
  - With batch commits: ~50 fewer fsync → ~1.0 hour effective
```

---

## Expected Output Table Structure

### **brief_facts_ai Table:**
```sql
CREATE TABLE brief_facts_ai (
    -- Primary keys
    bf_accused_id                UUID PRIMARY KEY,      -- Unique per crime×accused
    crime_id                     BIGINT NOT NULL,       -- FK to crimes
    
    -- Identity (from DB or LLM)
    accused_id                   UUID,                  -- FK to accused (nullable for Branch C)
    person_id                    UUID,                  -- FK to persons (nullable)
    canonical_person_id          UUID,                  -- Fuzzy-matched persons ID
    person_code                  VARCHAR,               -- Police station code (A-123)
    
    -- Person details
    full_name                    VARCHAR,               -- Standardized name
    alias_name                   VARCHAR,               -- Aliases/nicknames
    age                          INT,                   -- Computed from DOB
    gender                       VARCHAR,               -- M/F/Other
    occupation                   VARCHAR,               -- Job if extracted
    address                      VARCHAR,               -- Residence/location
    phone_numbers                VARCHAR,               -- Contact numbers
    
    -- Extracted/inferred facts
    role_in_crime                TEXT,                  -- JSON array of roles
    extracted_roles              TEXT[],                -- PostgreSQL array format
    is_ccl                       BOOLEAN,               -- Criminal code/habitual?
    extracted_drugs              TEXT[],                -- Drugs mentioned
    key_details                  VARCHAR,               -- Summary of extraction
    accused_type                 VARCHAR,               -- Accused/Known/Respondent/Suspect
    status                       VARCHAR,               -- Charged/Convicted/Acquitted/Absconding
    
    -- Deduplication metadata
    dedup_match_tier             VARCHAR,               -- Which tier matched (0/1/2/3)
    dedup_confidence             DECIMAL(3,2),          -- 0.00-1.00 match score
    dedup_review_flag            BOOLEAN,               -- Manual review needed?
    
    -- Source data
    source_person_fields         JSONB,                 -- Original persons row
    source_accused_fields        JSONB,                 -- Original accused row
    source_summary_fields        JSONB,                 -- Summary of extraction
    
    -- Processing metadata
    existing_accused             BOOLEAN,               -- Was in DB before extraction?
    seq_num                      INT,                   -- Sequence in accused list
    etl_run_id                   VARCHAR,               -- Which processing run?
    
    -- Timestamps
    date_created                 TIMESTAMP DEFAULT NOW(),
    date_modified                TIMESTAMP DEFAULT NOW()
);

-- Indices for query performance
CREATE INDEX idx_bfai_crime_id ON brief_facts_ai(crime_id);
CREATE INDEX idx_bfai_person_id ON brief_facts_ai(person_id);
CREATE INDEX idx_bfai_canonical_person_id ON brief_facts_ai(canonical_person_id);
CREATE INDEX idx_bfai_ps_code ON brief_facts_ai(person_code);
```

---

## Expected Output: Example Records

### **Example 1: Branch A (DB Accused, Person Known)**
```sql
INSERT INTO brief_facts_ai (
    bf_accused_id,              'b2b2b2b2-0000-0000-0000-000000000001',
    crime_id,                   12345,
    accused_id,                 'a1a1a1a1-0000-0000-0000-000000000001',
    person_id,                  'p1p1p1p1-0000-0000-0000-000000000001',
    canonical_person_id,        'p1p1p1p1-0000-0000-0000-000000000001',
    person_code,                'A-456',
    full_name,                  'Raj Kumar',
    alias_name,                 'Raju',
    age,                        28,
    gender,                     'M',
    address,                    '123 Main Street, District A',
    phone_numbers,              '9876543210',
    role_in_crime,              '["perpetrator", "supplier"]',
    extracted_roles,            ARRAY['perpetrator', 'supplier'],
    is_ccl,                     false,
    extracted_drugs,            ARRAY['ganja'],
    status,                     'Charged',
    dedup_match_tier,           '0',  -- Direct match from person_id
    dedup_confidence,           1.00,
    accused_type,               'Accused',
    existing_accused,           true,
    source_accused_fields,      '{"accused_id":"...", "role_in_crime":"Perpetrator"}',
    etl_run_id,                 'run-uuid-001'
) ON CONFLICT DO UPDATE SET date_modified=NOW();
```

### **Example 2: Branch B (DB Accused, Person Unknown)**
```sql
INSERT INTO brief_facts_ai (
    bf_accused_id,              'b2b2b2b2-0000-0000-0000-000000000002',
    crime_id,                   12345,
    accused_id,                 'a1a1a1a1-0000-0000-0000-000000000002',
    person_id,                  NULL,  -- Unknown, not in persons table
    canonical_person_id,        'c1c1c1c1-8da0-f8e1-beef-0000000002',  -- Synthetic hash
    person_code,                'A-2',  -- LLM assigned
    full_name,                  'Priya Singh',
    alias_name,                 NULL,
    age,                        25,
    gender,                     'F',
    address,                    '456 Oak Lane, District A',
    role_in_crime,              '["harbourer"]',
    extracted_roles,            ARRAY['harbourer'],
    is_ccl,                     false,
    extracted_drugs,            ARRAY[],
    status,                     'Charged',
    dedup_match_tier,           '3',  -- Synthetic (no match found)
    dedup_confidence,           0.00,
    accused_type,               'Known',
    existing_accused,           true,
    source_accused_fields,      '{"accused_id":"...", "full_name":"Priya Singh", "person_id":null}',
    etl_run_id,                 'run-uuid-001'
);
```

### **Example 3: Branch C (Text-Only Extraction, No DB Accused)**
```sql
INSERT INTO brief_facts_ai (
    bf_accused_id,              'b2b2b2b2-0000-0000-0000-000000000003',
    crime_id,                   12346,  -- Different crime, no accused yet
    accused_id,                 NULL,  -- Not in DB
    person_id,                 NULL,
    canonical_person_id,        'c1c1c1c1-8da0-f8e1-beef-0000000003',
    person_code,                'A-1',  -- LLM assigned
    full_name,                  'Unknown Male 1',  -- Extracted from text
    alias_name,                 'Raja',
    age,                        NULL,
    gender,                     'M',
    address,                    NULL,
    role_in_crime,              '["perpetrator"]',
    extracted_roles,            ARRAY['perpetrator'],
    is_ccl,                     false,
    extracted_drugs,            ARRAY['heroin'],
    status,                     'Extracted',  -- Tentative
    dedup_match_tier,           NULL,
    dedup_confidence,           0.00,
    accused_type,               NULL,
    existing_accused,           false,  -- Not in accused table
    source_summary_fields,      '{"note":"TEXT_ONLY_EXTRACTION","mark_for_invalidation":"on_accused_arrival"}',
    etl_run_id,                 'run-uuid-002'
);
```

---

## Data Quality Safeguards

### **Validation Rules (Skip/Flag):**

| Condition | Action | Reason |
|-----------|--------|--------|
| `person_id IS NULL` in Branch A | Skip | Spec rule: don't output unconfirmed persons in Branch A |
| No `full_name` | Flag "MISSING_NAME" | Cannot identify person |
| Duplicate within crime (same person) | Merge | Keep one, merge roles/drugs |
| Role only mention (e.g., "informant John told police") | Skip | Not a primary accused, just reference |
| Absconding but also arrested context | Keep Charged | Most recent status wins |
| Drug found but no accused | Create sentinel row | "NO_ACCUSED_DRUGS_ONLY" |
| No accused and no drugs | Skip | Crime has no extraction potential |

### **Metadata Flags:**

```python
dedup_review_flag = True if (
    (dedup_confidence > 0.5 and dedup_confidence < 0.85)  # Borderline match
    or (multiple_names_extracted for same person)          # Name variations
    or (roles_conflict)                                     # Contradictory roles
    or (status_conflict)                                    # Arrested vs absconding
)
# → Manual review needed by analysts
```

---

## Error Handling & Recovery

### **Per-Crime Isolation:**
```python
with pool.get_connection_context() as conn:
    try:
        # Process crime
        rows_written, records = _process_branch_a(...)
        sp_name = f"sp_crime_{crime_id}"
        cur.execute(f"SAVEPOINT {sp_name}")  # Create per-crime rollback point
    except Exception as e:
        # Roll back only THIS crime, not entire batch
        cur.execute(f"ROLLBACK TO SAVEPOINT {sp_name}")
        fail_crime_processing_run(conn, run_id, str(e))
        # Continue with next crime
```

### **Batch Commit Strategy:**
```
Commit every 30 crimes (not every crime)
Benefits:
  - 50% fewer fsync operations to disk
  - Per-crime isolation via SAVEPOINT
  - Faster overall execution

Risk: If process crashes mid-batch, re-run from last committed crime
```

---

## Expected Behavior Checklist

| When | Expected Behavior |
|------|-------------------|
| **First run (backfill)** | Processes ALL crimes, creates brief_facts_ai records for entire history |
| **Daily run (incremental)** | Processes only new/modified crimes since yesterday |
| **New accused arrives** | Text-only Branch C records are invalidated, replaced with Branch A/B |
| **Duplicate accused detected** | Merged into single row with combined roles/drugs |
| **Absconding person mentioned** | Status="Absconding", role assignment per context |
| **Drugs found but no accused** | Creates sentinel row with "NO_ACCUSED_DRUGS_ONLY" |
| **LLM extraction fails** | Entire crime is rolled back (savepoint), continues to next crime |
| **Person not in persons table** | Uses synthetic canonical_person_id (hash-based) |
| **Multiple person codes** | LLM assigns A1, A2, A3 based on mention order |
| **Batch commit every 30 crimes** | Reduces disk I/O by 50%, preserves per-crime atomicity |

---

## Summary: Expected Data Flow

```
INPUT (crimes table)
  ↓
┌─────────────────────────────────────────┐
│ Check: Do accused records exist in DB? │
└─────────────────────────────────────────┘
  │
  ├─ YES + person_id NOT NULL  → Branch A (DB authoritative)
  ├─ YES + person_id IS NULL   → Branch B (LLM enriches DB data)
  └─ NO                         → Branch C (LLM text-only)
  ↓
┌─────────────────────────────────────────┐
│ LLM Extraction (Claude API)             │
│ - Extract roles, drugs, status          │
│ - Deduplicate within crime              │
│ - Canonicalize persons                  │
└─────────────────────────────────────────┘
  ↓
┌─────────────────────────────────────────┐
│ brief_facts_ai table (output)           │
│ - 1 row per unique accused per crime    │
│ - Enriched with AI facts                │
│ - Deduplicated and normalized           │
└─────────────────────────────────────────┘
  ↓
READY for:
  - Case analytics
  - Network analysis (co-accused)
  - Drug trafficking networks
  - Repeat offender identification
  - Status tracking
```

---

**Document Generated:** 2026-04-24  
**Status:** Ready for production execution
