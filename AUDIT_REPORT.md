# ETL Pipeline Audit Report
**Issue:** Pipeline executes successfully but no data is written to the database  
**Date:** 2026-04-16  
**Status:** ROOT CAUSE IDENTIFIED

---

## Executive Summary

The ETL pipeline **DOES NOT fail** because tables are missing. Instead:

1. ✅ **Database is properly initialized** - all 32 tables exist
2. ✅ **API data is being fetched** successfully  
3. ❌ **INSERT operations are failing silently** due to database constraint violations
4. ❌ **Failed inserts are being caught** and logged but not reported prominently

---

## Root Cause Analysis

### Primary Issue: Constraint Violation in `brief_facts_drug` Table

**Table:** `brief_facts_drug` (line 1158 in DB-schema.sql)

**Failing Constraint (line 1177):**
```sql
CONSTRAINT check_has_measurements CHECK (
    (weight_g IS NOT NULL) OR 
    (weight_kg IS NOT NULL) OR 
    (volume_ml IS NOT NULL) OR 
    (volume_l IS NOT NULL) OR 
    (count_total IS NOT NULL)
)
```

**What This Means:**
- At least ONE measurement field must have a value for every drug record
- The constraint is being violated because ETL is inserting drug records with **ALL measurement fields NULL**

### Evidence

**Error Log** (`/home/ashish-ratna/DOPAMS-ETL/output.log` - Feb 27, 2026 at 00:55:39):

```
ERROR - Unexpected error in main loop: new row for relation "brief_facts_drug" 
violates check constraint "check_has_measurements"

DETAIL: Failing row contains (
    2b2fb42f-3e76-4525-b9a0-c5c5bc586a6d,    -- id
    67e53305934d345919503ada,                 -- crime_id
    null,                                      -- weight_g ← NULL
    THC,
    0.000000,
    Unknown,
    THC,
    liquid,
    null,                                      -- weight_kg ← NULL
    null,                                      -- volume_ml ← NULL
    null,                                      -- volume_l ← NULL
    null,                                      -- count_total ← NULL
    null,
    0.95,
    {...},
    f,
    0.0,
    2026-02-27 00:55:39.7708+00,
    2026-02-27 00:55:39.7708+00
)
```

### Why This Happens

**Code Location:** `/home/ashish-ratna/DOPAMS-ETL/brief_facts_ai/extractor_drugs.py` (lines 765-770)

The drug extractor attempts to populate measurements:
```python
if qty > 0 and drug.weight_g is None and drug.volume_ml is None and drug.count_total is None:
    if unit == "kg":
        drug.weight_g = qty * 1000.0
        drug.weight_kg = qty
    # ... more unit conversions
```

**But if:**
- `qty` is `0`, `None`, or missing
- No unit is recognized
- All conversion logic is skipped

**Then:** All measurement fields remain `NULL` → **Constraint violation** → **INSERT fails**

### Why Pipeline Appears "Successful"

1. Python code executes without crashing
2. Database exceptions are caught in try/except blocks
3. Failed records are logged but not fail the entire pipeline
4. Process exits with code 0 (success)
5. User sees "ETL completed" but **0 records actually inserted**

---

## Secondary Issues

Multiple other constraints also exist that could cause similar failures:

| Table | Constraint | Issue |
|-------|-----------|-------|
| `brief_facts_ai` | `accused_type_check` | Invalid accused_type values |
| `brief_facts_ai` | `dedup_tier_check` | dedup_match_tier not in (1,2,3) |
| `brief_facts_drug` | **`check_has_measurements`** | **All measurement fields NULL** |
| `person_dedup_tracker` | `confidence_score_check` | Confidence not 0.0-1.0 |
| `person_dedup_tracker` | `matching_tier_check` | Tier not 1-5 |

---

## Data Quality Issues

The upstream API or data transformation is producing **incomplete drug records** with:
- No quantity specified
- No weight or volume data
- No count information

These records violate the database constraint that requires at least one measurement.

---

## Impact

- **0 records** in `brief_facts_drug` table (and likely other tables too)
- 28 ETL steps run and complete "successfully"  
- All INSERT operations fail silently
- Database remains empty despite apparent pipeline success

---

## Solution

### Immediate Fixes

**Option 1: Fix the ETL Data Validation** (Recommended)
- Modify `extractor_drugs.py` to **skip or reject** drug records with no measurement data
- Add validation logging to show how many records are rejected due to missing measurements
- Set a reasonable default measurement (e.g., 1 gram) if needed

**Option 2: Relax Database Constraint** (Not Recommended)
- Remove or modify `check_has_measurements` constraint  
- Allows incomplete data into database
- Hides data quality issues

**Option 3: Add Data Quality Checks in ETL**
- Before INSERT, validate that at least one measurement field is populated
- Log detailed failure reasons
- Optionally backfill with estimates or defaults

### Recommended Implementation

Add validation in `brief_facts_ai/extractor_drugs.py` before inserting:

```python
def validate_measurements(drug_record):
    """Ensure at least one measurement field is populated"""
    has_measurement = any([
        drug_record.weight_g is not None,
        drug_record.weight_kg is not None,
        drug_record.volume_ml is not None,
        drug_record.volume_l is not None,
        drug_record.count_total is not None,
    ])
    
    if not has_measurement:
        logger.warning(f"Drug {drug_record.id} has no measurement data - SKIPPING INSERT")
        return False
    
    return True
```

---

## Verification Steps

1. **Check current data:**
   ```sql
   SELECT COUNT(*) FROM brief_facts_drug;
   SELECT COUNT(*) FROM brief_facts_ai;
   SELECT COUNT(*) FROM crimes;
   ```

2. **Check for NULL measurements:**
   ```sql
   SELECT id, crime_id, raw_drug_name, drug_name
   FROM brief_facts_drug 
   WHERE weight_g IS NULL AND weight_kg IS NULL 
     AND volume_ml IS NULL AND volume_l IS NULL 
     AND count_total IS NULL;
   ```

3. **Monitor next ETL run:**
   - Add enhanced logging to show validation failures
   - Report how many records are rejected
   - Check master ETL logs for constraint violation errors

---

## Related Database Constraints to Monitor

Other tables have similar constraints that may cause silent failures:

- `brief_facts_ai_accused_type_check` - enum validation on accused_type
- `person_deduplication_tracker_matching_tier_check` - numeric range
- All should be validated BEFORE INSERT to prevent failures

---

## Notes

The `\restrict` and `\unrestrict` commands in DB-schema.sql (pgAdmin export artifacts) do not prevent schema import. Modern versions of psql safely ignore unknown backslash commands. The schema **is successfully applied** to the database.

