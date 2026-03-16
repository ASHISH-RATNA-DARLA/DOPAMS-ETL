# Strict Drug Extraction Policy

## Overview

This ETL pipeline has been enhanced with **strict drug extraction and validation** policies. NO drug records are inserted as placeholders, and all extracted drugs must pass rigorous validation before insertion.

## Core Policy Rules

### 1. No Placeholders Policy
- **NEVER insert `NO_DRUGS_DETECTED` placeholder records**
- If a crime has no valid drugs, it is simply **skipped** (no database record)
- Only crimes with **valid, verified drug extractions** are inserted

### 2. Strict Drug Validation

A drug extraction is **VALID for insertion** if and ONLY if:

```
(confidence_score >= 0.50)  AND  NOT is_in_ignored_checklist(raw_name, threshold=0.80)
```

#### Validation Steps:

1. **LLM Extracts** drug information from brief facts
2. **KB Mapping** applies fuzzy matching to categorize drugs
3. **Ignored Checklist Check** (>80% match threshold)
   - If raw_name matches an entry in `drug_ignore_list` with ≥80% similarity → **REJECT**
   - Uses Python's `difflib.SequenceMatcher` for fuzzy comparison
4. **Confidence Score Check**
   - If confidence < 50% AND not in KB → **SKIP**
   - If confidence ≥ 50% AND not in ignored list → **INSERT**

### 3. Drug Category Learning

The system automatically learns drug categories through:

- **KB Mapping**: Matches raw extracted drug names to `drug_categories` table
- **Fuzzy Matching**: Uses >80% similarity threshold for category mapping
- **Confidence Boosting**: Increases extraction confidence (+5%) if KB match found
- **Metadata Enrichment**: Stores KB category, original name, and mapping confidence in `extraction_metadata`

## Database Tables

### `drug_ignore_list`
Stores drug names/terms that should be rejected during extraction.

**Columns:**
- `id`: Primary key
- `term`: Drug name or pattern to ignore
- `reason`: Why this term is ignored (e.g., "false positive", "non-drug substance")
- `created_at`: Timestamp

**Example entries:**
```
term='unknown', reason='Non-specific term'
term='unidentified substance', reason='Cannot determine drug type'
term='liquid', reason='Too generic'
```

### `drug_categories`
Verified knowledge base of drug mappings.

**Columns:**
- `id`: Primary key
- `raw_name`: Original drug name variant
- `standard_name`: Canonical drug name
- `category_group`: Drug category (e.g., "Narcotic", "Psychotropic")
- `is_verified`: Boolean flag for verification status
- `created_at`: Timestamp

## Code Changes

### 1. `db.py`

**New Functions:**

- `fetch_ignored_checklist(conn)`: Fetches all ignored drug terms
- `is_drug_ignored(drug_name, ignore_list, threshold=0.80)`: Checks if drug matches ignored list with fuzzy matching
  - Returns: `(is_ignored: bool, matched_term: str, similarity_score: float)`

### 2. `main.py`

**Changes:**

- Fetches `ignored_checklist` at startup (alongside drug_categories)
- Updated `process_crimes_parallel()` to:
  - Accept `ignored_checklist` parameter
  - Check each extracted drug against ignored list before insertion
  - Track metrics: `total_ignored` (rejected by checklist), `total_skipped` (no drugs/low confidence)
  - **NO placeholder insertion** - crimes with no valid drugs are skipped entirely
- Enhanced logging shows:
  - Drugs rejected by ignored checklist (with similarity score)
  - Drugs skipped for low confidence
  - Final batch summary with strict policy note

### 3. `extractor.py`

**New Functions:**

- `_map_drug_to_kb_category(raw_name, drug_categories)`: Maps drug to KB category using fuzzy matching
  - Returns: `(primary_drug_name, category_group, confidence_score)`
  - Confidence: 0.95 for exact match, 0.80-0.94 for fuzzy match >80%
  
- `_apply_kb_mapping(drugs, drug_categories)`: Applies KB learning to all extracted drugs
  - Updates `primary_drug_name` with mapped name
  - Stores mapping info in `extraction_metadata`: `{kb_mapped, kb_original_name, kb_category, kb_confidence}`
  - Boosts confidence (+5%) if KB match found
  - Integrated into post-processing pipeline

**Pipeline Order:**
```
KB Mapping → Packet Handling → Unit Standardization → Worth Distribution → Commercial Check → Deduplication
```

## Logging & Metrics

### Batch Processing Log

```
[INFO] Crime 001: no drugs extracted → skipping (no placeholder, strict policy)
[INFO] Crime 002: 'xyz drug' matched ignored list 'xyz' (85%) → REJECTING (strict policy)
[INFO] Crime 003: 'abc drug' skipped (low confidence 45%, not in KB)
[INFO] Crime 004: 2/3 drug entries queued for insert (after strict validation)

[INFO] Batch done: 50 crimes in 12.5s (4.0 crimes/s) — 
       45 valid drugs inserted, 8 ignored (matched >80%), 
       12 skipped (no drugs/low confidence/errors). STRICT POLICY: NO placeholders.
```

### Individual Drug Logging

```
[DEBUG] KB mapping: 'Ganj' → 'Ganja' (Narcotic) with 98% similarity (confidence 0.95)
[INFO] Drug 'Unknown Substance' matched ignore list 'unknown substance' with 92% similarity
[DEBUG] No KB match for 'some_odd_name' (will be flagged for ignored checklist)
```

## Validation Examples

### Example 1: Valid Drug → INSERT
```
raw_drug_name: "Dry Ganja"
confidence_score: 0.85
is_ignored: False (not in drug_ignore_list)

Result: ✅ INSERT (confidence >= 0.50 AND not ignored)
```

### Example 2: Low Confidence, Not in KB → SKIP
```
raw_drug_name: "Some unknown substance"
confidence_score: 0.35
is_ignored: False (not in drug_ignore_list)

Result: ❌ SKIP (confidence < 0.50)
```

### Example 3: Matches Ignored Checklist → REJECT
```
raw_drug_name: "Unknown Drug"
confidence_score: 0.75
is_ignored: True (matches "unknown drug" at 92% similarity)

Result: ❌ REJECT (matched ignored list, strict policy)
```

### Example 4: No Drugs Found → SKIP
```
brief_facts: "No drugs mentioned in this FIR"
extracted_drugs: []

Result: ❌ SKIP (no placeholder, strict policy - crime gets no entry)
```

## Configuration

### Environment Variables

- `PARALLEL_LLM_WORKERS`: Number of concurrent LLM threads (default 6)
- `BATCH_SIZE`: Crimes fetched per batch (default 15)

### Thresholds (Hard-coded)

- **Ignored Checklist Match**: ≥ 80% similarity (Levenshtein distance)
- **Low Confidence**: < 50%
- **KB Mapping Confidence**: Exact match 95%, fuzzy match 80-94%

## Database Setup

### Required Tables

Ensure the following tables exist:

1. **`drug_ignore_list`**: Ignored drug terms
   ```sql
   CREATE TABLE drug_ignore_list (
       id SERIAL PRIMARY KEY,
       term TEXT NOT NULL UNIQUE,
       reason TEXT,
       created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
   );
   ```

2. **`drug_categories`**: Knowledge base
   ```sql
   CREATE TABLE drug_categories (
       id SERIAL PRIMARY KEY,
       raw_name TEXT NOT NULL UNIQUE,
       standard_name TEXT NOT NULL,
       category_group TEXT NOT NULL,
       is_verified BOOLEAN DEFAULT TRUE,
       created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
   );
   ```

3. **`crimes`**: Source crimes table
   - Must have `crime_id` and `brief_facts` columns

4. **Drug Facts Table** (configured via `DRUG_TABLE_NAME`):
   - Stores all extracted drug records
   - See schema for columns

## Updating Ignored Checklist

To add/remove ignored drug terms:

```sql
-- Add a term
INSERT INTO drug_ignore_list (term, reason) 
VALUES ('unidentified substance', 'Cannot determine drug type');

-- Remove a term
DELETE FROM drug_ignore_list WHERE term = 'some_term';

-- View current list
SELECT term, reason FROM drug_ignore_list ORDER BY term;
```

## Updating Drug Categories

To add/update KB mappings:

```sql
-- Add a drug variant
INSERT INTO drug_categories (raw_name, standard_name, category_group, is_verified)
VALUES ('Dry Ganja', 'Ganja', 'Narcotic', TRUE);

-- Update verification status
UPDATE drug_categories SET is_verified = FALSE WHERE raw_name = 'some_variant';
```

## Performance Impact

- **Fuzzy Matching**: ~O(n) per drug where n = size of ignored_checklist
  - Typical ignored_checklist: 50-200 terms → negligible impact
- **KB Mapping**: ~O(m) where m = size of drug_categories (typically 500-2000)
  - Done once per extracted drug → minimal overhead
- **Overall**: Strict validation adds ~5-10ms per crime batch

## Backward Compatibility

- **NOT backward compatible** with old pipeline that inserted placeholders
- Old placeholder records (`raw_drug_name = 'NO_DRUGS_DETECTED'`) should be archived/removed
- New runs will only produce valid drug records

## Troubleshooting

### No Drugs Being Inserted

**Check:**
1. Is LLM extracting drugs? (look for "LLM returned X raw drug entries")
2. Are all being rejected by ignored checklist? (look for "matched ignored list" logs)
3. Are all low confidence? (look for "low confidence" logs)

**Solution:**
- Review `drug_ignore_list` for false positives
- Check LLM confidence scores
- Verify KB mappings in `drug_categories`

### Unexpected Drug Rejection

**Check:**
1. What's the matched term in `drug_ignore_list`?
2. What's the similarity percentage?

**Solution:**
- If false positive, remove from `drug_ignore_list`
- If legitimate, keep in list and update documentation

## Future Enhancements

- [ ] Machine learning-based confidence scoring
- [ ] Active learning from human-verified corrections
- [ ] Multi-language drug name support
- [ ] Drug interaction detection
- [ ] Automated ignored_checklist updates based on low-confidence patterns
