# Requirements Analysis: `brief_facts_ai` ETL

> **Scope:** Pure requirements understanding — no solutions, no code, no design.  
> **Source of truth:** `brief_facts_accused/` pipeline, `brief_facts_drugs/` pipeline, `DB-schema.sql`, and all supporting documentation.  
> **Last updated:** All ambiguities resolved. Document is implementation-ready.

---

## 1. What the ETL Is

`brief_facts_ai` is a **single, unified ETL pipeline** that processes crime records from the `crimes` table and produces one output table called **`brief_facts_ai`**.

It is the merger of two currently separate ETLs:

| Existing ETL | Output Table | What it does |
|---|---|---|
| `brief_facts_accused` | `public.brief_facts_accused` (configured via `ACCUSED_TABLE_NAME`) | Extracts accused persons and their attributes from `brief_facts` text |
| `brief_facts_drugs` | `public.brief_facts_drug` (configured via `DRUG_TABLE_NAME`) | Extracts drug seizure records from `brief_facts` text |

The merged `brief_facts_ai` table must contain **all columns from both** existing output tables as a **single combined schema**. Two separate tables become one. Both old tables are **decommissioned** once `brief_facts_ai` is live.

---

## 2. Input Sources

### Primary
| Source | Table | Key Fields Used |
|---|---|---|
| Crime records | `crimes` | `crime_id`, `brief_facts` |
| Accused records | `accused` | `accused_id`, `accused_code`, `seq_num`, `type`, `is_ccl`, `accused_status`, `person_id` |
| Person records | `persons` | `full_name`, `alias`, `age`, `date_of_birth`, `gender`, `occupation`, `phone_number`, address fields |

### Reference / Lookup
| Source | Table | Used For |
|---|---|---|
| Drug Knowledge Base | `public.drug_categories` | Standardizing raw drug names via exact, substring, and fuzzy matching |
| Drug Ignore List | `public.drug_ignore_list` | Filtering out non-drug entries after KB resolution |
| Cross-crime identity | `public.brief_facts_ai` (self) | Candidate lookup for 5-layer de-duplication — `canonical_person_id` resolution queries previous rows in this same table (Option A) |

> [!NOTE]
> `person_deduplication_tracker` is **not used** by this ETL. It is a separate offline process that clusters records already in `persons`/`accused`. It has no coverage of Branch B/C accused who have no `persons` entry, making it unsuitable as the de-dup lookup store. The `brief_facts_ai` table itself serves as the canonical identity store via self-lookup (see REQ-12).

---

## 3. Output Table: `brief_facts_ai`

### Row Granularity
> **One row = One accused person per crime.**

This is the controlling constraint of the entire ETL. Drug data enriches accused rows — it never creates new ones.

### Schema

#### Identity Block

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `bf_accused_id` | UUID | NOT NULL (PK) | Generated per row — always `gen_random_uuid()` |
| `crime_id` | VARCHAR(50) | NOT NULL | FK to `crimes` |
| `accused_id` | VARCHAR(50) | NULL only for sentinels | UUID-v5 of `crime_id + full_name + seq_num`; DB value for Branch A; synthetic for B/C |
| `person_id` | VARCHAR(50) | NULL only for sentinels | `persons.person_id` if DB has one; else NULL (Branch B/C use `canonical_person_id` instead) |
| `canonical_person_id` | VARCHAR(50) | NULL only for sentinels | Global cross-crime identity — resolved by 5-layer de-dup engine (REQ-12); never NULL for real detected accused |

#### Accused Identity Block (DB + LLM fallback)

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `person_code` | VARCHAR(50) | YES | e.g. `A-1`, `A-2` |
| `seq_num` | VARCHAR(50) | YES | Sequence number from `accused` table |
| `existing_accused` | BOOLEAN | NOT NULL DEFAULT false | `true` only for Branch A rows |
| `full_name` | VARCHAR(500) | YES | Accused's full name |
| `alias_name` | VARCHAR(255) | YES | Alias/nickname |
| `age` | INTEGER | YES | |
| `gender` | VARCHAR(20) | YES | Male/Female/Transgender |
| `occupation` | VARCHAR(255) | YES | |
| `address` | TEXT | YES | Concatenated present-address fields from `persons` |
| `phone_numbers` | VARCHAR(255) | YES | Comma-separated 10-digit numbers |

#### Accused Classification Block (LLM-derived)

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `role_in_crime` | TEXT | YES | LLM-extracted role description; sentinel values written here |
| `key_details` | TEXT | YES | Quantities, vehicles, facts |
| `accused_type` | VARCHAR(40) | YES | CHECK: peddler/consumer/supplier/harbourer/organizer_kingpin/processor/financier/manufacturer/transporter/producer |
| `status` | TEXT | YES | arrested/absconding/unknown |
| `is_ccl` | BOOLEAN | YES | Is Child in Conflict with Law |

#### Drug Seizure Block

> [!IMPORTANT]
> Multi-drug per accused is stored as a **JSONB array** in the `drugs` column (AMB-1 resolved). Flat scalar drug columns from the old `brief_facts_drug` table **do not appear** as top-level columns in `brief_facts_ai`. All drug data lives inside the `drugs` array.

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `drugs` | JSONB | YES | Array of drug objects. NULL when accused has no drug context at all. See drug object shape below. |

**Each element in the `drugs` array has this shape:**

```json
{
  "raw_drug_name":          "string  — as extracted by LLM",
  "raw_quantity":           "numeric — extracted number, null if missing",
  "raw_unit":               "string  — extracted unit string",
  "primary_drug_name":      "string  — KB-standardized name",
  "drug_form":              "string  — solid/liquid/count",
  "weight_g":               "numeric(18,6)",
  "weight_kg":              "numeric(18,6)",
  "volume_ml":              "numeric(18,6)",
  "volume_l":               "numeric(18,6)",
  "count_total":            "numeric(18,6)",
  "confidence_score":       "numeric(3,2) — 0.50–1.00",
  "is_commercial":          "boolean",
  "seizure_worth":          "numeric  — rupee value",
  "worth_scope":            "string   — individual/drug_total/overall_total",
  "extraction_metadata":    "jsonb    — includes source_sentence",
  "drug_attribution_source":"string  — see attribution states below",
  "drug_attribution_ref":   "string  — person_code of A1 row, only for REFERENCED_A1"
}
```

**Valid `drug_attribution_source` values per drug array element:**

| Value | Meaning | `raw_quantity` | `drug_attribution_ref` |
|---|---|---|---|
| `INDIVIDUAL` | Drug explicitly attributed to this accused in text | Accused-specific quantity | NULL |
| `COLLECTIVE_TOTAL` | Group seizure — this accused's row holds the group total | Full group total | NULL |
| `UNATTRIBUTED_FALLBACK_A1` | No attribution in text — A1 row holds full quantity by rule | Full total | NULL |
| `REFERENCED_A1` | Remaining accused in same unattributed seizure | NULL | `person_code` of A1 row |
| `NO_DRUGS_DETECTED` | Accused caught but no drug physically seized | `0` | NULL |
| `NO_ACCUSED_ORPHAN` | Orphan drug crime — drugs exist but no accused identified | Full seizure quantity | NULL |

#### Audit Trail Block

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `source_person_fields` | JSONB | YES | Per-field source audit trail |
| `source_accused_fields` | JSONB | YES | Per-field source audit trail |
| `source_summary_fields` | JSONB | YES | Includes `consumer_inferred_prior` flag when accused_type is inferred |

#### Processing Metadata

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `etl_run_id` | UUID | NOT NULL | Links to `etl_crime_processing_log` for partial-run tracking |
| `date_created` | TIMESTAMP | NOT NULL DEFAULT CURRENT_TIMESTAMP | |
| `date_modified` | TIMESTAMP | NOT NULL DEFAULT CURRENT_TIMESTAMP | |

#### Constraints & Indexes

```sql
-- Primary key
PRIMARY KEY (bf_accused_id)

-- Idempotency (enables ON CONFLICT DO NOTHING for re-runs)
UNIQUE (crime_id, accused_id)   -- NULLs excluded automatically; sentinels unaffected

-- accused_type domain
CHECK (accused_type IS NULL OR accused_type IN (
  'peddler','consumer','supplier','harbourer',
  'organizer_kingpin','processor','financier',
  'manufacturer','transporter','producer'
))

-- Indexes
CREATE INDEX idx_bfai_crime_id           ON brief_facts_ai (crime_id);
CREATE INDEX idx_bfai_accused_id         ON brief_facts_ai (accused_id);
CREATE INDEX idx_bfai_person_id          ON brief_facts_ai (person_id);
CREATE INDEX idx_bfai_canonical_person_id ON brief_facts_ai (canonical_person_id);
CREATE INDEX idx_bfai_soundex_name       ON brief_facts_ai (SOUNDEX(full_name));
CREATE INDEX idx_bfai_drugs_gin          ON brief_facts_ai USING GIN (drugs);
```

#### Companion Table: Processing Log

```sql
CREATE TABLE public.etl_crime_processing_log (
    run_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    crime_id      VARCHAR(50) NOT NULL,
    status        VARCHAR(20) NOT NULL DEFAULT 'in_progress',
        -- values: in_progress | complete | failed
    accused_count_written INTEGER,
    started_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at  TIMESTAMP,
    error_detail  TEXT
);

CREATE INDEX idx_etl_log_crime_status ON etl_crime_processing_log (crime_id, status);
```

---

## 4. Detailed Requirements

### REQ-1: Accused Record Completeness
- Every accused identified for a crime — whether found in the `accused` DB table or only in the `brief_facts` text — must produce **exactly one row** in `brief_facts_ai`.
- Total output rows for a crime = Total distinct accused persons identified.
- No accused may be silently dropped due to partial data, missing identity, or absence from the DB.

### REQ-2: Three-Branch Identity Handling
The existing accused ETL uses a **3-branch dispatcher** based on DB state. This logic carries over unchanged:

| Branch | DB Condition | Identity Source |
|---|---|---|
| **A** | Accused rows exist AND ≥1 has `person_id` | DB is authoritative |
| **B** | Accused rows exist but ALL `person_id` IS NULL | LLM extracts, code-pairs to DB |
| **C** | No accused rows in DB at all | LLM only, no DB reference |

**Identity fields** (`accused_id`, `person_id`) may be NULL in Branch A rows where the DB itself has no value for those fields. For Branch B and C, see REQ-11.

### REQ-3: Drug Extraction Must Be Accused-Specific
- Drug data (name, quantity, units, worth) must be **attributed to the same accused person** whose row it will populate.
- The LLM prompts in the drugs pipeline already mention accused codes (A1, A2, etc.) in extractions. This attribution information must be used.
- Drug elements in an accused row's `drugs` array must reflect only **that accused person's drugs**, not all drugs in the crime.

### REQ-4: Drug Extraction Integration (No Duplication)
- The drug extraction logic from `brief_facts_drugs/extractor.py` must be **used as-is** within the accused ETL flow.
- This includes:
  - `preprocess_brief_facts()` — multi-FIR filtering
  - `extract_drug_info()` — LLM extraction
  - `resolve_primary_drug_name()` — KB 3-tier name standardization
  - `filter_non_drug_entries()` — non-drug filtering
  - `standardize_units()` — unit normalization
- The `brief_facts_drugs` pipeline must **not** be run as a separate process for crimes processed by `brief_facts_ai`.
- Drug extraction is called **once per crime**, not once per accused.
- The accused list (person_codes, full names, seq_nums) is **injected into the drug extraction prompt** so the LLM can attribute drugs to specific accused codes. Accused extraction therefore runs first; drug extraction runs second.

### REQ-5: One-to-One Row Mapping Constraint
- Each accused → exactly one row.
- The `drugs` JSONB column on that row holds all drug entries for that accused (zero, one, or many elements).
- **Drug attribution must not alter the row count** — drugs enrich existing rows, they do not create new ones.

### REQ-6: Multi-Drug Per Accused — RESOLVED
When one accused is associated with multiple different drugs (e.g., A1 had 100g Ganja AND 10 tablets Alprazolam), both drug entries are stored as **separate elements in the `drugs` JSONB array** on A1's single row. No additional rows are created. This satisfies REQ-5 exactly while preserving all drug data without loss.

### REQ-7: Record Count Integrity
- `SELECT COUNT(*) FROM brief_facts_ai WHERE crime_id = :id` must equal the number of accused persons identified for that crime (including sentinel rows).
- The `drugs` JSONB array can have N elements without affecting this count.
- Analytics that need flat drug rows must use a view that unnests the `drugs` array — they do not query `brief_facts_ai` directly for per-drug counts.

### REQ-8: Handling Missing or Partial Data

| Scenario | Required Behavior |
|---|---|
| Drug found, quantity missing | Capture drug name; `raw_quantity` = NULL in the drug element |
| Quantity found, accused mapping unclear | Best-effort attribution; do not drop the drug |
| Accused has no drugs | Row exists; `drugs` = NULL (if truly no drug context) |
| LLM extraction fails entirely | Sentinel row: `role_in_crime = 'LLM_EXTRACTION_FAILED'` |
| No accused found by LLM | Sentinel row: `role_in_crime = 'NO_ACCUSED_IN_TEXT'` |
| Drugs seized, no per-accused attribution in text | Full seizure assigned to A1 (`UNATTRIBUTED_FALLBACK_A1`); remaining accused get drug name + NULL quantity (`REFERENCED_A1`) |
| Accused caught, no drugs physically seized | `drugs` array has one element with `drug_attribution_source = 'NO_DRUGS_DETECTED'`, all quantity fields = `0` |
| Drugs found, no accused identified | Sentinel row: `role_in_crime = 'NO_ACCUSED_DRUGS_ONLY'`; `drugs` array fully populated; identity columns NULL |

### REQ-9: Unprocessed Crime Tracking
- `fetch_unprocessed_crimes()` checks `etl_crime_processing_log` for absence or `failed` status — **not** `brief_facts_ai` row presence.
- A crime with `status = 'in_progress'` or `'failed'` in the log is eligible for re-processing.
- Row-level idempotency is provided by the `UNIQUE (crime_id, accused_id)` constraint + `ON CONFLICT DO NOTHING` — already-written rows are skipped on re-run.
- Processing log is set to `complete` only after all accused rows for a crime are written successfully.

### REQ-10: Drug Name Standardization Still Required
- All drug names must be resolved through the 3-tier KB matching:
  1. Exact match in `drug_categories`
  2. Substring match
  3. `pg_trgm` fuzzy match via DB (`pg_trgm` extension already loaded)
- The `drug_ignore_list` must still be applied after standardization.
- This applies to every drug element in every accused row.

### REQ-11: Synthetic Identity Generation for LLM-Detected Accused (MANDATORY)

#### accused_id
- **Must never be NULL** for any accused row where the LLM identified a real person.
- If the DB provides a matching `accused_id` (Branch B code-pairing), use it.
- If no DB match exists, **generate a synthetic `accused_id`** using UUID-v5 of `(crime_id + full_name + seq_num)`.

#### canonical_person_id
- **Must never be NULL** for any accused row where the LLM identified a real person.
- If the DB provides a `person_id` via the `persons` table (Branch A), `person_id` takes precedence; `canonical_person_id` is also populated via REQ-12.
- For Branch B/C where no DB `person_id` exists, `canonical_person_id` is the sole cross-crime identity anchor — resolved by REQ-12.

#### person_id
- Populated from `persons.person_id` when DB provides it (Branch A, matched Branch B).
- NULL for Branch B/C rows with no DB `person_id` match — these rows are identified cross-crime via `canonical_person_id` instead.

#### existing_accused flag
- `true` only for Branch A rows where identity came from the `accused` DB table.
- `false` for all Branch B/C rows, even when a synthetic identity is generated.

#### Sentinel Row Exception
Synthetic identity generation applies **only** to rows representing real detected accused persons. The following are the only valid cases where `accused_id`, `person_id`, and `canonical_person_id` remain NULL:

| Condition | Sentinel Value Written |
|---|---|
| LLM finds no accused in text | `role_in_crime = 'NO_ACCUSED_IN_TEXT'` |
| LLM extraction fails entirely | `role_in_crime = 'LLM_EXTRACTION_FAILED'` |
| Drugs found but no accused identified | `role_in_crime = 'NO_ACCUSED_DRUGS_ONLY'` |

#### Branch Behavior Summary

| Branch | `accused_id` | `person_id` | `canonical_person_id` |
|---|---|---|---|
| **A** | From DB | From DB (may be NULL if DB has none) | Via REQ-12 lookup |
| **B** | DB value if code-paired, else UUID-v5 synthetic | NULL (no persons entry) | Via REQ-12 lookup |
| **C** | UUID-v5 synthetic | NULL | Via REQ-12 lookup |
| **Sentinel** | NULL | NULL | NULL |

---

### REQ-12: 5-Layer De-Duplication & Canonical Identity Framework (MANDATORY)

> [!IMPORTANT]
> `canonical_person_id` is resolved by querying **`brief_facts_ai` itself** — not `person_deduplication_tracker`. The ETL performs a self-lookup against previous rows in the output table. This is Option A and is the confirmed approach.

#### Why Self-Lookup on `brief_facts_ai`

`person_deduplication_tracker` only contains records that exist in `persons`/`accused`. Branch B and C accused have no `persons` entry — meaning the tracker has no candidates to return for the cases that most need de-duplication. Querying `brief_facts_ai` directly covers all previously processed accused regardless of branch.

#### Candidate Retrieval Query (Layer 2 blocking)

```sql
SELECT bf_accused_id, canonical_person_id, full_name, age,
       gender, address, person_code, source_accused_fields
FROM brief_facts_ai
WHERE crime_id != :current_crime_id
  AND full_name IS NOT NULL
  AND (
      SOUNDEX(full_name) = SOUNDEX(:name)
      OR source_accused_fields->>'ps_code' = :ps_code
  )
LIMIT 200;
```

The `idx_bfai_soundex_name` index makes this fast. The 200-row cap keeps Python-side scoring bounded.

#### Layer 0 — Pre-Processing & Normalisation (Runs Before All Layers)

Mandatory. Operates on `full_name`, `address`, and `alias_name` before any comparison.

| Step | What it does |
|---|---|
| Relational prefix stripping | Remove `s/o`, `w/o`, `d/o`, `r/o`, `h/o` from name field |
| Lowercase + trim | Normalize casing and whitespace |
| Script unification | Devanagari / Telugu / Kannada → Roman transliteration |
| Address tokenisation | Extract PS name, district, state tokens |
| Alias extraction | Split `full_name @ alias` format; store separately |

#### Layer 1 — Deterministic Matching (Exact)

Zero-cost. Runs first. If matched → stop, no further layers.

| Signal | Match Condition | Result |
|---|---|---|
| CCTNS `accused_id` | Exact string match against `brief_facts_ai.accused_id` | Confidence 1.00 → reuse `canonical_person_id` |
| `person_code` | Exact match | Confidence 1.00 → reuse `canonical_person_id` |

#### Layer 2 — Phonetic Blocking (Candidate Reduction)

Reduces candidate universe ~95% before scoring.

| Scheme | What it captures |
|---|---|
| **Soundex** | English phonetic bucketing |
| **Metaphone** | Consonant-cluster matching |
| **Indic Soundex** | Custom: Raju/Rajoo, Reddy/Reddi, Mohammed/Mohd/Mohammad, etc. |
| **PS + token blocking** | Same PS jurisdiction geo-locality |

Only candidates matching any scheme advance to Layer 3.

#### Layer 3 — Fuzzy Similarity Scoring (Weighted)

Produces a composite score (0.0–1.0) over Layer 2 survivors.

| Component | Weight | Rationale |
|---|---|---|
| Jaro-Winkler name similarity | 35% | Rewards shared prefixes — correct for Indian names |
| Token set ratio | 20% | Handles reordered names |
| Phonetic overlap | 15% | Confirms spelling variant |
| Address similarity | 12% | PS + district token overlap |
| Age proximity | 10% | ±2yr → 0.8; >10yr → 0.0; **neutral 0.5 when age unknown** |
| Alias / nickname match | 8% | Cross-field alias bridge |

**Common-Name Penalty:** ×0.85 multiplier when normalized name is a single high-frequency token (`Kumar`, `Singh`, `Rao`, `Reddy`, `Sharma`, `Naidu`, `Babu`, `Raju`, etc.). Prevents false merges on common names within the same PS.

#### Layer 4 — Contextual Corroboration (Police-Specific Boosts)

Additive boosts on top of Layer 3 score.

| Signal | Boost | Condition |
|---|---|---|
| PS jurisdiction overlap | +0.05 | Both records registered at same Police Station |
| MO / offence type match | +0.04 | Same section + drug type |
| Co-accused network | +0.06 | One or more shared FIR associates |

#### Layer 5 — Identity Decision & Action

| Final Score | Tier | Action |
|---|---|---|
| **≥ 0.82** | HIGH | Reuse existing `canonical_person_id` |
| **0.60–0.81** | MEDIUM | Generate new `canonical_person_id` + set `dedup_review_flag = true` for analyst queue |
| **< 0.60** | LOW | Generate new `canonical_person_id` |

> [!NOTE]
> Thresholds (0.82 / 0.60) must be calibrated against a labelled sample of ≥200 known true-match and ≥200 known true-non-match pairs from existing CCTNS data before production go-live.

#### Additional Columns for De-Dup Metadata

| Column | Type | Notes |
|---|---|---|
| `dedup_match_tier` | SMALLINT | Layer 5 tier (1=HIGH, 2=MEDIUM, 3=LOW, NULL for sentinels) |
| `dedup_confidence` | NUMERIC(3,2) | Layer 3+4 composite score (NULL if Layer 1 exact hit) |
| `dedup_review_flag` | BOOLEAN DEFAULT false | true when MEDIUM band — surfaces to analyst review queue |

#### Canonical ID Format

| Identifier | Scope | Format |
|---|---|---|
| `canonical_person_id` | Global — same person across all crimes | UUID-v5 of normalised `name + gender + ps_code` |
| `accused_id` | Crime-scoped — unique per FIR appearance | UUID-v5 of `crime_id + full_name + seq_num` |

A single `canonical_person_id` may link to multiple `accused_id` rows across different crimes. `accused_id` is never reused across crimes.

#### Implementation Priority Order

1. Layer 0 — name normalisation function
2. Layer 1 — exact match on `accused_id` / `person_code` against `brief_facts_ai`
3. Layer 2 — Soundex blocking + PS token blocking
4. Layer 3 — Jaro-Winkler + token set ratio (top 4 weights)
5. Layer 3 — Common-name penalty
6. Layer 3 — Age + address + alias scoring
7. Layer 4 — PS jurisdiction + co-accused boosts
8. Layer 5 — Threshold decision + `canonical_person_id` generation + `dedup_review_flag`

---

### REQ-13: Drug Attribution Fallback Rules (MANDATORY)

#### Scenario A — Drugs Seized, No Per-Accused Attribution in Text

**Trigger:** `brief_facts` mentions a drug seizure but does not attribute it to a specific accused code.

| Row | `drugs` array element | `drug_attribution_source` |
|---|---|---|
| A1 (lowest `seq_num`) | Full seized quantity — all measurement fields populated | `UNATTRIBUTED_FALLBACK_A1` |
| A2, A3 … An | Drug name populated; `raw_quantity` + all measurement fields NULL; `drug_attribution_ref` = A1's `person_code` | `REFERENCED_A1` |

**Why A1:** FIR convention places the primary accused first. Assigning full quantity to A1 prevents double-counting in analytics.

#### Scenario B — Accused Caught, No Drugs Physically Seized

**Trigger:** Accused is in `brief_facts` but no drug seizure is mentioned for them.

The `drugs` array contains **one element** with:

| Field | Value |
|---|---|
| `primary_drug_name` | `NO_DRUGS_DETECTED` |
| `raw_drug_name` | `NO_DRUGS_DETECTED` |
| `raw_quantity` | `0` |
| `raw_unit` | `None` |
| All measurement columns | `0` |
| `seizure_worth` | `0.0` |
| `confidence_score` | `1.00` |
| `drug_attribution_source` | `NO_DRUGS_DETECTED` |
| `drug_attribution_ref` | NULL |

**`accused_type` inference:** When `NO_DRUGS_DETECTED` and no explicit role is stated, infer `accused_type = 'consumer'` at ~60% prior confidence. Flag in `source_summary_fields` as `{"accused_type_inference": "consumer_inferred_prior"}`. Explicit LLM signals always override the prior.

#### Scenario C — Drugs Found, No Accused Identified (Orphan Contraband)

**Trigger:** Drug content extracted from `brief_facts` but LLM finds no accused persons.

One sentinel row is written:

| Field | Value |
|---|---|
| `role_in_crime` | `NO_ACCUSED_DRUGS_ONLY` |
| `accused_id`, `person_id`, `canonical_person_id` | NULL |
| `drugs` | Fully populated array with all extracted drug data |
| `drug_attribution_source` (per element) | `NO_ACCUSED_ORPHAN` |

---

## 5. Data Flow: Source Tables → Output

### From `accused` table (via DB query with LEFT JOIN persons)
- `accused_id`, `person_id`, `accused_code` → `person_code`, `seq_num`, `type` (as `accused_type_db`), `is_ccl`, `accused_status`

### From `persons` table (via LEFT JOIN)
- `full_name`, `alias` → `alias_name`, `age`, `date_of_birth` (age fallback), `gender`, `occupation`, `phone_number` → `phone_numbers`, address fields → concatenated `address`

### From LLM (accused extraction — `brief_facts` text)
- `role_in_crime`, `key_details`, `accused_type`, `status`, `is_ccl`, any missing person fields (Branch A gap-fill), all person fields (Branch C)

### From LLM (drug extraction — `brief_facts` text, run after accused list is known)
- Per accused element in `drugs`: `raw_drug_name`, `raw_quantity`, `raw_unit`, `primary_drug_name`, `drug_form`, `seizure_worth`, `worth_scope`, `is_commercial`, `confidence_score`, `extraction_metadata` (source_sentence)

### Computed
- Standardized measurements: `weight_g`, `weight_kg`, `volume_ml`, `volume_l`, `count_total`
- `accused_id` (UUID-v5 synthetic where needed)
- `canonical_person_id` (via 5-layer de-dup self-lookup)
- `source_person_fields`, `source_accused_fields`, `source_summary_fields` (audit trail JSONB)

---

## 6. Materialized View Migration (Required Before Cutover)

Five existing materialized views reference `brief_facts_accused` and `brief_facts_drug` directly. All five must be updated to read from `brief_facts_ai` instead.

| View | Current dependency | Required change |
|---|---|---|
| `firs_mv` | `brief_facts_accused`, `brief_facts_drug` | Replace drug subqueries with `jsonb_array_elements(bfa.drugs)` unnest |
| `accuseds_mv` | `brief_facts_accused` | Change join target from `brief_facts_accused` to `brief_facts_ai` |
| `advanced_search_firs_mv` | `brief_facts_accused`, `brief_facts_drug` | Same as `firs_mv` |
| `advanced_search_accuseds_mv` | `brief_facts_accused` | Same as `accuseds_mv` |
| `criminal_profiles_mv` | `brief_facts_accused`, `brief_facts_drug` | Same as `firs_mv` |

All five must be refreshed (`REFRESH MATERIALIZED VIEW WITH NO DATA`) after DDL updates land in Phase 4.

---

## 7. All Ambiguities — Resolved

| ID | Question | Resolution |
|---|---|---|
| AMB-1 | Multi-drug per accused — how stored? | **JSONB array** on the accused row. Each drug is one element. REQ-5 row count unchanged. |
| AMB-2 | Collective seizure attribution | **RESOLVED by REQ-13:** Full quantity → A1 (`UNATTRIBUTED_FALLBACK_A1`). Others → drug name + NULL quantity (`REFERENCED_A1`). |
| AMB-3 | `worth_scope` retention | **Retained** inside each JSONB drug element alongside `seizure_worth`. No top-level column needed. |
| AMB-4 | Partial run recovery | **Row-level idempotency** via `UNIQUE (crime_id, accused_id)` + `ON CONFLICT DO NOTHING`. Crime-level state tracked in `etl_crime_processing_log`. |
| AMB-5 | No accused but drugs found | **Sentinel row** with `role_in_crime = 'NO_ACCUSED_DRUGS_ONLY'`; identity columns NULL; `drugs` array fully populated; `drug_attribution_source = 'NO_ACCUSED_ORPHAN'`. |
| AMB-6 | NULL vs placeholder for no-drug accused | **RESOLVED by REQ-13:** One `NO_DRUGS_DETECTED` element in `drugs` array; quantity fields = `0`; `confidence_score = 1.00`. |
| AMB-7 | Drug extraction LLM call timing | **Sequential:** Accused extraction runs first. Accused list (person_codes) injected into drug prompt. Attribution is LLM-driven, not post-hoc. |
| AMB-8 | `accused_id` column intent in old drug table | **Confirmed as correct intent** — now implemented in `brief_facts_ai` as a top-level column. |
| AMB-9 | `drug_facts` vs `brief_facts_drug` naming | Old table name is `public.brief_facts_drug`. Env var `DRUG_TABLE_NAME` is irrelevant after decommission. |
| AMB-10 | Cross-crime `person_id` stability | **RESOLVED by REQ-12:** 5-layer de-dup with self-lookup on `brief_facts_ai`. `person_deduplication_tracker` not used by this ETL. |

---

## 8. Confirmed Assumptions

| # | Assumption | Status |
|---|---|---|
| A1 | `brief_facts_ai` replaces both old tables — they are no longer written to | ✅ Confirmed |
| A2 | Row granularity is accused-per-crime | ✅ Confirmed |
| A3 | Multi-drug accused uses JSONB array in same row | ✅ Confirmed (AMB-1 resolved) |
| A4 | `worth_scope` retained inside JSONB drug elements | ✅ Confirmed (AMB-3 resolved) |
| A5 | 3-branch accused dispatcher (A/B/C) carried over unchanged | ✅ Confirmed |
| A6 | Drug extraction called once per crime, not once per accused | ✅ Confirmed |
| A7 | Crime with no accused AND no drugs → zero rows in `brief_facts_ai` | ✅ Confirmed |
| A8 | Accused with no drug context → `drugs = NULL` (not a placeholder element) | ✅ Confirmed |
| A9 | `brief_facts_drugs` pipeline decommissioned once `brief_facts_ai` is live | ✅ Confirmed |

---

## 9. Column Tree — Final

```
brief_facts_ai
├── Row Identity
│   ├── bf_accused_id           UUID PK — always generated
│   ├── crime_id                VARCHAR(50) FK
│   ├── accused_id              VARCHAR(50) — UUID-v5 synthetic or DB value; NULL only for sentinels
│   ├── person_id               VARCHAR(50) — DB persons.person_id if available; else NULL
│   └── canonical_person_id     VARCHAR(50) — self-lookup de-dup result; NULL only for sentinels
│
├── Accused Identity (DB + LLM fallback)
│   ├── full_name, alias_name, age, gender
│   ├── occupation, address, phone_numbers
│   └── person_code, seq_num, existing_accused
│
├── Accused Classification (LLM-derived)
│   ├── role_in_crime           TEXT — sentinel values written here
│   ├── key_details             TEXT
│   ├── accused_type            VARCHAR(40) — CHECK constraint
│   ├── status                  TEXT
│   └── is_ccl                  BOOLEAN
│
├── Drug Seizure Data
│   └── drugs                   JSONB — array of drug objects
│       └── each element:
│           ├── raw_drug_name, raw_quantity, raw_unit
│           ├── primary_drug_name, drug_form
│           ├── weight_g, weight_kg, volume_ml, volume_l, count_total
│           ├── confidence_score, is_commercial
│           ├── seizure_worth, worth_scope
│           ├── extraction_metadata           (includes source_sentence)
│           ├── drug_attribution_source       INDIVIDUAL | COLLECTIVE_TOTAL |
│           │                                 UNATTRIBUTED_FALLBACK_A1 | REFERENCED_A1 |
│           │                                 NO_DRUGS_DETECTED | NO_ACCUSED_ORPHAN
│           └── drug_attribution_ref          person_code of A1 (only for REFERENCED_A1)
│
├── De-Duplication Metadata
│   ├── dedup_match_tier        SMALLINT (1=HIGH, 2=MEDIUM, 3=LOW)
│   ├── dedup_confidence        NUMERIC(3,2) — Layer 3+4 composite score
│   └── dedup_review_flag       BOOLEAN DEFAULT false
│
├── Audit Trail
│   ├── source_person_fields    JSONB
│   ├── source_accused_fields   JSONB
│   └── source_summary_fields   JSONB — includes consumer_inferred_prior flag
│
└── Processing Metadata
    ├── etl_run_id              UUID → etl_crime_processing_log
    ├── date_created            TIMESTAMP
    └── date_modified           TIMESTAMP
```

---

## 10. What Is Clear vs. What Is Not

### ✅ All requirements are clear and implementation-ready

- Single unified ETL, single output table `brief_facts_ai`
- Row granularity = one accused per crime; JSONB array for multi-drug accused
- Drug extraction sequential — accused list injected into drug LLM prompt
- 3-branch dispatcher (A/B/C) preserved verbatim
- Drug logic reused verbatim from `brief_facts_drugs/extractor.py`
- `canonical_person_id` resolved by self-lookup on `brief_facts_ai` (not `person_deduplication_tracker`)
- 5-layer de-dup engine operates over previous `brief_facts_ai` rows as candidate pool
- Row-level idempotency via `UNIQUE (crime_id, accused_id)` + `etl_crime_processing_log`
- All attribution states defined: `INDIVIDUAL`, `COLLECTIVE_TOTAL`, `UNATTRIBUTED_FALLBACK_A1`, `REFERENCED_A1`, `NO_DRUGS_DETECTED`, `NO_ACCUSED_ORPHAN`
- Five materialized views require rewrite before cutover
- De-dup thresholds (0.82 / 0.60) to be calibrated on labelled CCTNS sample before production
- `dedup_review_flag = true` rows surface to analyst review queue (MEDIUM confidence band)
- Both old pipelines decommissioned post-cutover; old tables retained read-only for 30 days

### ❌ Nothing is unclear — all ambiguities resolved
