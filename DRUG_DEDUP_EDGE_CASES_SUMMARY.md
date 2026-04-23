# Drug Deduplication Edge Cases - Summary & Fixes

## Issue Overview

The `brief_facts_ai` ETL was creating **multiple entries for the same drug seizure** when extracted in different unit representations. This violates the design principle: **1 distinct drug = 1 entry**.

## Real-World Example

**Crime ID: 69e5d1579f8dba4f0a706c31** (NDPS Act violation, Miryalaguda Police Station)

**Seizure:** SPASMO PROXYVON PLUS tablets
- Extracted as: 32 tablets
- Also extracted as: 19.648 grams
- Result BEFORE fix: **2 database rows** (should be 1)

## Root Cause Analysis

| Component | Issue | Impact |
|-----------|-------|--------|
| **Extraction** | LLM extracts same seizure in multiple representations (count + weight) | Correctly captures all measurements |
| **Unit Standardization** | Converts all units to standardized forms (g, kg, ml, l, count) | Measurements are normalized |
| **Deduplication** ❌ | Key includes `raw_unit`, so "tablets" ≠ "grams" | Different keys = no consolidation |
| **Database Insert** | Each unique key = separate JSONB entry | Duplicate entries stored |

## The Fix

**File:** `brief_facts_ai/extractor_drugs.py` → `deduplicate_extractions()` function

**Changed deduplication key from:**
```python
(primary_drug_name, raw_drug_name, raw_quantity, raw_unit)
```

**To:**
```python
(primary_drug_name, raw_drug_name, supplier_name, source_location)
```

**Rationale:**
- `raw_quantity` + `raw_unit`: Represent different measurements of the SAME seizure (not different seizures)
- `supplier_name` + `source_location`: Distinguish independent seizures (different sources = different drugs)

## Edge Cases Now Properly Handled

### ✅ Case 1: Same Drug, Different Unit Representations
```
Input extractions:
  1. Spasmo Proxyvon, 32 tablets, confidence=0.95
  2. Spasmo Proxyvon, 19.648 grams, confidence=0.95

Processing:
  → Deduplicate by (primary_drug_name, raw_drug_name) only
  → Merge measurements: count_total=32, weight_g=19.648
  → Preserve both source sentences for audit trail

Output (brief_facts_ai table):
  1 entry with all measurements consolidated
```

### ✅ Case 2: Same Drug, Different Suppliers
```
Input extractions:
  1. Ganja, 100g, supplier='Raju'
  2. Ganja, 50g, supplier='Mohan'

Processing:
  → Different supplier_name → different keys
  → No consolidation (correctly identifies separate seizures)

Output (brief_facts_ai table):
  2 separate entries (correct)
```

### ✅ Case 3: Same Drug, Different Locations
```
Input extractions:
  1. Cocaine, 50g, location='Hyderabad'
  2. Cocaine, 50g, location='Bangalore'

Processing:
  → Different source_location → different keys
  → No consolidation (correctly identifies separate seizures)

Output (brief_facts_ai table):
  2 separate entries (correct)
```

### ✅ Case 4: Exact Duplicates
```
Input extractions:
  1. Heroin, 10g, confidence=0.90
  2. Heroin, 10g, confidence=0.85

Processing:
  → Same key
  → Keep entry with higher confidence (0.90)
  → Merge any missing measurements

Output (brief_facts_ai table):
  1 entry (highest confidence preserved)
```

### ✅ Case 5: Partial Measurements
```
Input extractions:
  1. Tablets, raw_quantity=32, raw_unit='tablets', count_total=32, weight_g=NULL
  2. Tablets, raw_quantity=19.648, raw_unit='grams', count_total=NULL, weight_g=19.648

Processing:
  → Same key
  → Merge measurements: entry 1 has count, entry 2 has weight
  → Result: both count_total AND weight_g populated

Output (brief_facts_ai table):
  1 entry with complete measurement data
```

## Data Quality Impact

### Before Fix
```
Crime: 69e5d1579f8dba4f0a706c31
Rows in brief_facts_ai_drug_flat: 2 (WRONG)

Row 1:
  primary_drug_name: Spasmo Proxyvon
  raw_quantity: 32
  raw_unit: tablets
  count_total: 32
  weight_g: NULL ❌ (missing)

Row 2:
  primary_drug_name: Spasmo Proxyvon
  raw_quantity: 19.648
  raw_unit: grams
  count_total: NULL ❌ (missing)
  weight_g: 19.648
```

### After Fix
```
Crime: 69e5d1579f8dba4f0a706c31
Rows in brief_facts_ai_drug_flat: 1 (CORRECT)

Row 1:
  primary_drug_name: Spasmo Proxyvon
  raw_quantity: 32
  raw_unit: tablets
  count_total: 32
  weight_g: 19.648 ✅ (merged)
```

## Implementation Details

### Consolidation Logic
When duplicate keys are detected:

1. **Confidence-based selection:** Keep entry with highest `confidence_score`
2. **Measurement merging:** Combine all standardized measurements
   - weight_g, weight_kg, volume_ml, volume_l, count_total
   - Use value from either entry if one is NULL
3. **Metadata preservation:** Store both source sentences
   - Primary: `extraction_metadata['source_sentence']`
   - Alternate: `extraction_metadata['alternate_source_sentence']`

### Logging
```
[INFO] Deduplicated extractions: 2 -> 1 (consolidated multi-unit seizures)
```

## Testing Strategy

### Unit Tests (test_drug_dedup.py)
- ✅ Multi-unit consolidation
- ✅ Different suppliers → separate entries
- ✅ Exact duplicates → highest confidence kept
- ✅ Partial measurements → merged

### Integration Testing
```sql
-- Verify consolidation worked
SELECT crime_id, primary_drug_name, COUNT(*) as entry_count
FROM brief_facts_ai_drug_flat
WHERE crime_id = '69e5d1579f8dba4f0a706c31'
GROUP BY crime_id, primary_drug_name;

-- Should show: 1 entry per drug (not 2)
```

## Backwards Compatibility

✅ **Fully backwards compatible**
- No schema changes
- No API changes  
- Only affects new ETL runs
- Consolidation is additive (more correct, not breaking)
- Existing data unaffected

## Performance Implications

✅ **No negative impact**
- Dedup key is smaller (4 fields instead of 4 fields, but units removed)
- Consolidation is O(n) with early exit
- Reduced database rows → better query performance

## Related Issues Identified

### 1. NDPS Threshold Calculations ✅
- **Impact:** COMMERCIAL quantity threshold uses aggregate seizure weights
- **Status:** Fixed — consolidated entries provide single source of truth

### 2. Supplier/Location Attribution ✅
- **Impact:** Aggregations need to respect supplier/location boundaries
- **Status:** Fixed — dedup key includes supplier_name + source_location

### 3. Audit Trail Completeness ✅
- **Impact:** Need to preserve all extraction details for legal review
- **Status:** Fixed — both source sentences stored in metadata

## Recommendations

1. **Re-run ETL on all crimes:** Consolidation only applies to new runs
   ```bash
   RESTART=true ./master_etl.py  # Full reload with consolidation
   ```

2. **Validate output:** Check for single entries per drug
   ```sql
   SELECT primary_drug_name, COUNT(*) as entry_count, 
          STRING_AGG(DISTINCT raw_unit, ', ') as units
   FROM brief_facts_ai_drug_flat
   GROUP BY crime_id, primary_drug_name
   HAVING COUNT(*) > 1
   LIMIT 10;
   -- Should return 0 rows (no duplicates)
   ```

3. **Monitor logs:** Watch for consolidation messages
   ```
   Deduplicated extractions: X -> Y (consolidated multi-unit seizures)
   ```

---

**Status:** ✅ IMPLEMENTED  
**Risk Level:** LOW  
**Testing:** UNIT TESTS READY  
**Deployment:** READY FOR PRODUCTION
