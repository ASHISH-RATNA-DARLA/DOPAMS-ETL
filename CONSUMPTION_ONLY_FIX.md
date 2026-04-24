# Consumption-Only Cases: Fix Required

## The Problem

**Current Behavior (WRONG):**
```
Text: "Accused tested positive for ganja in urine test. No narcotics seized."

Current extraction:
  → drugs: [{'name': 'GANJA', 'qty': null, 'attribution': 'COLLECTIVE_CONSUMPTION'}]
  
View brief_facts_ai_drug_flat:
  ✗ Shows drug entry for ganja (WRONG!)
  
Reality:
  ✗ No drugs were seized
  ✗ Should NOT appear in drug extraction/view
```

**Expected Behavior (CORRECT):**
```
Text: "Accused tested positive for ganja in urine test. No narcotics seized."

Expected extraction:
  → drugs: []  ← Empty, no seizure
  
View brief_facts_ai_drug_flat:
  ✓ No drug entries (CORRECT!)
  
Accused record:
  ✓ Can still capture "tested positive for ganja" in role/status if needed
  ✓ But NOT in drugs array (since nothing was seized)
```

---

## Root Cause

**Location:** `/brief_facts_ai/extractor_drugs.py` lines 794-816

**Current EXTRACTION_PROMPT:**
```python
EXTRACTION_PROMPT = """You are an expert forensic analyst extracting NDPS drug seizure rows...

Rules:
1. One row per actual seizure incident.
...
7. Extract only the quantity physically seized at arrest. Skip historical 
   purchase quantities, already-sold quantities, samples S1/S2, and remaining 
   property breakdowns...
11. Never extract vehicles, phones, SIM cards, cash, alcohol, empty covers, 
    weighing scales, or other non-drug property as drug rows.
12. Use the actual NDPS drug name for primary_drug_name whenever identifiable.
"""
```

**Missing Rule:** No explicit instruction to **SKIP extraction if NO drugs were physically seized**.

---

## The Fix

### **Add New Rule to EXTRACTION_PROMPT:**

```python
EXTRACTION_PROMPT = """You are an expert forensic analyst extracting NDPS drug seizure rows...

Rules:
...
13. CRITICAL: If the text mentions drug consumption (tested positive, urine test, 
    drug detection test, smoking, ingestion) BUT does NOT mention any seized/
    confiscated narcotics, return EMPTY drugs array: {"drugs":[]}
    
    Examples (return empty):
    - "Accused tested positive for ganja. No narcotics seized."
    - "Urine test showed heroin use. No substances found."
    - "All accused found positive in drug test."
    
    Examples (extract normally):
    - "Tested positive AND 2kg ganja was seized."
    - "Urine positive for heroin, 50 tablets confiscated."
"""
```

---

## Implementation Strategy

### **Option 1: LLM Rule (Best - Immediate Fix)**

**What:** Add Rule 13 to EXTRACTION_PROMPT to tell LLM: "No seizure mentioned → return empty"

**Pros:**
- LLM will skip extraction automatically
- No need to modify post-processing logic
- Clear, explicit instruction
- Prevents false extractions at source

**Cons:**
- Requires LLM understanding (high confidence, but not 100%)

**Code Change:**

```python
# File: /brief_facts_ai/extractor_drugs.py, line 811
EXTRACTION_PROMPT = """..."""

# Add after Rule 12:
13. CRITICAL CONSUMPTION-ONLY FILTER: If the text mentions drug consumption 
    (tested positive, drug test, positive test, smoking, ingestion, detected in 
    urine) BUT does NOT contain any of: seized, confiscated, recovered, found, 
    possessed, apprehended with, arrested with, caught with, then:
    
    → Return EMPTY drugs array: {"drugs":[]}
    
    This distinguishes:
    ✓ "Tested positive and 2kg ganja seized" → Extract ganja
    ✗ "Tested positive but no drugs seized" → Empty array
```

---

### **Option 2: Post-Processing Filter (Backup)**

**What:** After LLM extraction, check if all drugs are "consumption-only" and filter them out.

**Pros:**
- Guaranteed catch (backup to LLM)
- Works even if LLM misses the rule

**Cons:**
- More complex logic
- Requires heuristic detection

**Code Location:**

```python
# File: /brief_facts_ai/extractor_drugs.py, line 1630

def extract_drug_info(...):
    ...
    # Step 9: Post-filter consumption-only drugs
    final_drugs = []
    for drug in drugs:
        metadata = drug.get('extraction_metadata', {})
        source_sentence = (metadata.get('source_sentence') or '').lower()
        
        # Check if sentence is consumption-only (no seizure markers)
        consumption_markers = ['tested positive', 'positive for', 'urine test', 
                             'drug test', 'positive in test', 'ingested', 'smoked']
        seizure_markers = ['seized', 'confiscated', 'recovered', 'found with', 
                          'possessed', 'arrested with', 'caught with', 'apprehended with']
        
        is_consumption_only = (
            any(m in source_sentence for m in consumption_markers)
            and not any(m in source_sentence for m in seizure_markers)
        )
        
        if is_consumption_only:
            logger.info(f"Filtered consumption-only: {drug['primary_drug_name']}")
            continue  # Skip this entry
        
        final_drugs.append(drug)
    
    return final_drugs
```

---

### **Option 3: Dual Check (Safest - Recommended)**

Use **both** Option 1 (LLM rule) + Option 2 (post-filter) for defense-in-depth:

1. **LLM Rule 13** prevents extraction at source (primary defense)
2. **Post-filter** catches any misses by LLM (backup)

---

## Testing Cases

### **Test Case 1: Consumption Only (NO seizure)**

```
Input: "Accused tested positive for ganja in urine test. 
        No contraband seized from residence."

Expected Output: drugs: []  ← Empty

Current (WRONG): 
  drugs: [{'name': 'GANJA', 'attribution': 'COLLECTIVE_CONSUMPTION'}]
```

**Fix Status:** ❌ Needs Rule 13

---

### **Test Case 2: Consumption + Seizure**

```
Input: "Accused tested positive for heroin. 
        50 tablets of heroin were confiscated."

Expected Output: 
  drugs: [{'name': 'HEROIN', 'qty': 50, 'unit': 'tablets'}]

Current: ✓ Correct
  drugs: [{'name': 'HEROIN', 'qty': 50, 'unit': 'tablets'}]
```

**Fix Status:** ✓ Already works

---

### **Test Case 3: Consumption Mentioned Twice**

```
Input: "Raj tested positive for ganja. Later, 2kg ganja 
        was seized from his residence."

Expected Output:
  drugs: [{'name': 'GANJA', 'qty': 2, 'unit': 'kg'}]  ← Only seized amount

Current: ✓ Correct
  drugs: [{'name': 'GANJA', 'qty': 2, 'unit': 'kg'}]
```

**Fix Status:** ✓ Already works (Rule 7 catches this)

---

## Impact on brief_facts_ai_drug_flat View

### **Before Fix:**

```sql
SELECT * FROM brief_facts_ai_drug_flat 
WHERE crime_id IN (SELECT crime_id FROM brief_facts_ai 
                  WHERE status='tested_positive_only');

-- Shows drug entries even though NO drugs were seized
-- WRONG: Inflates drug-related crime statistics
┌──────────────┬────────────────┬──────────┐
│ crime_id     │ primary_drug   │ qty      │
├──────────────┼────────────────┼──────────┤
│ 12345        │ GANJA          │ NULL     │  ← FALSE ENTRY!
│ 12346        │ HEROIN         │ NULL     │  ← FALSE ENTRY!
└──────────────┴────────────────┴──────────┘
```

### **After Fix:**

```sql
SELECT * FROM brief_facts_ai_drug_flat 
WHERE crime_id IN (SELECT crime_id FROM brief_facts_ai 
                  WHERE status='tested_positive_only');

-- No drug entries (CORRECT!)
-- view is empty
┌──────────────┬────────────────┬──────────┐
│ crime_id     │ primary_drug   │ qty      │
├──────────────┼────────────────┼──────────┤
│ (no rows)    │                │          │  ✓ CORRECT: No seizure = no entry
└──────────────┴────────────────┴──────────┘
```

---

## Accused Record Handling

**Note:** The fix only affects drug EXTRACTION. Accused records can still capture consumption status:

```sql
brief_facts_ai (accused record):
  person_code: 'A1'
  full_name: 'Raj Kumar'
  role_in_crime: 'user'  ← Can indicate drug consumption
  status: 'Charged'
  
  drugs: []              ← EMPTY (no seizure)

-- Can add drug consumption to role/status if needed
-- But NOT in drugs array
```

---

## Recommended Implementation

### **Step 1: Add Rule 13 to LLM Prompt (NOW)**

```python
# File: /brief_facts_ai/extractor_drugs.py, after line 811

13. CONSUMPTION-ONLY FILTER: If text contains drug consumption references 
    (tested positive, urine test, drug test, positive for, detected in test)
    BUT DOES NOT contain seizure indicators (seized, confiscated, recovered, 
    found with, apprehended with, caught with, arrested with), return:
    
    {"drugs":[]}
    
    This prevents false extraction of non-seized drug references.
```

**Cost:** 1 new rule in prompt (~50 tokens)

---

### **Step 2: Add Post-Filter (BACKUP)**

```python
# File: /brief_facts_ai/extractor_drugs.py, after Step 8

# Step 9: Consumption-only filter
def filter_consumption_only_drugs(drugs_list, text):
    consumption_keywords = {'tested positive', 'positive for', 'urine test', 
                           'drug test', 'detected in test', 'found positive'}
    seizure_keywords = {'seized', 'confiscated', 'recovered', 'arrested with', 
                       'apprehended with', 'found with', 'caught with', 'possessed'}
    
    filtered = []
    for drug in drugs_list:
        metadata = drug.get('extraction_metadata', {})
        source = (metadata.get('source_sentence') or '').lower()
        
        # Check if this specific drug entry is consumption-only
        has_consumption = any(k in source for k in consumption_keywords)
        has_seizure = any(k in source for k in seizure_keywords)
        
        # Keep if: seizure exists, OR consumption without seizure is invalid
        if has_seizure or not has_consumption:
            filtered.append(drug)
        else:
            logger.info(f"Filtered {drug['primary_drug_name']}: consumption-only (no seizure)")
    
    return filtered
```

**Cost:** ~30 lines of code

---

## Summary

| Issue | Current | After Fix |
|-------|---------|-----------|
| **Consumption-only case with no seizure** | Extracts drug ✗ | No extraction ✓ |
| **Consumption + seizure** | Extracts both ✓ | Extracts seizure only ✓ |
| **Seized only (no consumption)** | Extracts ✓ | Extracts ✓ |
| **brief_facts_ai_drug_flat view** | False positives ✗ | Only real seizures ✓ |
| **Data quality** | ❌ Inflated drug counts | ✅ Accurate seizure counts |

---

## Recommendation

**Implement BOTH:**
1. Add Rule 13 to EXTRACTION_PROMPT (LLM-level fix)
2. Add filter_consumption_only_drugs() post-processing (safety net)

**Rationale:** Defense-in-depth ensures consumption-only drugs are never extracted, preventing data quality issues in downstream views and analytics.

---

**Status:** ISSUE CONFIRMED ✓  
**Severity:** MEDIUM (data quality)  
**Impact:** brief_facts_ai_drug_flat view shows false drug entries  
**Effort:** LOW (~50 lines)  
**Recommended Priority:** NEXT SPRINT
