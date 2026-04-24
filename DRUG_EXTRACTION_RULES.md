# Drug Extraction Rules Documentation

## Overview
Extract **ONLY drugs physically seized at the crime spot** during arrest/apprehension. Filter out sold, consumed, and historically purchased drugs.

---

## Rule Hierarchy

### RULE 0: SEIZURE-ONLY (CRITICAL)
Extract ONLY drugs physically seized at crime spot during arrest.

**DO NOT extract:**
- Sold quantities ("sold 1kg to A-3")
- Consumed quantities ("tested positive")
- Historical purchases ("purchased 6kg before arrest")
- Downstream transactions ("buyer was A-3")

**DO extract:**
- Seized at arrest ("caught with 50g")
- Confiscated ("recovered 2kg from location")
- Found in possession ("apprehended with")

---

## Rule 1-12: Clubbing & Individual Entry Logic (MAINTAINED)

### Rule 1: One Row Per Seizure Incident
```
Principle: Same drug, different accused = separate rows
Example: A1 seized with 100g, A2 with 50g
→ 2 rows (different people, different seizures)
```

### Rule 2-3: Collective vs Individual Seizures
```
Collective (1 row): Multiple accused, ONE group total, NO per-person split
"6 accused apprehended together, seized total 520 Kg Ganja"
→ 1 row: raw_quantity=520, accused_ref=null

Individual (N rows): Per-person amounts clearly separate
"A1: 100g Ganja, A2: 50g Ganja, A3: 75g Ganja"
→ 3 rows: each with their quantity
```

### Rule 4: Per-Accused Rows Not Duplicates
```
6 accused each have 50g seized individually
→ 6 separate rows (not duplication, 6 different seizures)
```

### Rule 5-6: Skip Non-Seized Buyers
```
Rule 5: Skip buyers/customers mentioned in confession (nothing seized from them)
"A-1 confessed he sold to A-3, A-4, A-5"
→ Skip A-3, A-4, A-5 if nothing seized from them

Rule 6: But if buyer was LATER apprehended with seizure
"A-1 sold to A-3. Later A-3 caught with 200g Ganja"
→ A-3 IS valid seizure row (was seized from possession)
```

### Rule 6b: Joint Possession (Key Rule)
```
If A1 and CCL jointly possessed drugs and seized as group
→ 1 row with accused_ref=null (collective, not per-person)
→ NO separate rows for each person (no duplication)
→ NO downstream buyer/seller included
```

### Rule 7: Exclude Subsets
```
Skip historical purchases, already-sold quantities, samples/remaining property
Example: "purchased 20 boxes (100g), sold 5 (25g), seized 15 (75g), 
          sample 2 (10g), remaining 13 (65g)"
→ Extract ONLY 75g (what was seized)
→ Skip 100g, 25g, 10g, 65g (not actual seizures)
```

---

## Rule 13: Seizure Verification Filter

### What Gets Filtered (SKIP)

#### A) Consumption-Only (No Seizure)
```
Consumption markers: tested positive, drug test, urine test, smoked, consumed, ingestion
Condition: consumption marker EXISTS but NO seizure marker anywhere

Examples:
- "Tested positive for ganja in urine test. No drugs seized."
  → SKIP (consumption-only)
  
- "All accused found positive in drug detection test."
  → SKIP (test result, no seizure)
  
- "Suspected consumption but no evidence seized."
  → SKIP (consumption only)
```

#### B) Sold-Only (No Seizure)
```
Sold markers: sold to, sold by, buyer, customers, purchased by, transaction, 
             dealt to, distributed to
Condition: sold marker EXISTS but NO seizure marker anywhere

Examples:
- "A-1 sold 1kg to A-3 in transaction. No seizure."
  → SKIP (sold, not seized)
  
- "A-4 dealt drugs to customers. Nothing confiscated at arrest."
  → SKIP (sold, not seized)
  
- "A-2 purchased 6kg before arrest. None found during apprehension."
  → SKIP (purchased, not seized)
```

#### C) No Seizure Evidence
```
No seizure markers (seized, confiscated, recovered, caught with, etc.)
anywhere in source sentence or text

Examples:
- "Drug history mentioned but nothing seized"
  → SKIP (no seizure)
```

### What Gets Extracted (KEEP)

#### Seizure Markers (Presence = Extract)
```
seized, confiscated, recovered, found with, apprehended with, caught with,
arrested with, in possession of, possessed of, contraband, apprehended
```

```
Examples:

1. "Seized 2kg ganja"
   → KEEP (explicit seizure)

2. "Tested positive AND 2kg ganja was seized"
   → KEEP (consumption mentioned BUT seizure also present)

3. "Apprehended with 50g, A-2 caught with 30g"
   → KEEP (seizure markers present)

4. "Recovered 1kg from location, contraband confiscated"
   → KEEP (seizure + contraband)

5. "Found in possession of 100g, apprehended at arrest"
   → KEEP (possession at arrest)
```

---

## Combined Examples: Rules Working Together

### Example 1: Simple Individual Seizures
```
FIR: "A-1 apprehended with 100g Ganja. A-2 caught with 50g Ganja."

Extraction:
✓ A-1: 100g Ganja (seized at arrest)
✓ A-2: 50g Ganja (seized at arrest)
✓ Total: 2 rows (Rule 1: different accused = separate rows)

Drug Assignment (after extraction):
- A-1 100g → assign to A-1 (explicit mention)
- A-2 50g → assign to A-2 (explicit mention)
```

### Example 2: Collective Seizure
```
FIR: "6 accused apprehended together. Seized total 520 Kg Ganja worth Rs.52,00,000"

Extraction:
✓ 520 Kg Ganja, accused_ref=null (Rule 3: collective, no per-person split)
✓ Total: 1 row

Drug Assignment:
- Single drug entry → assign to A-1 (primary accused, default)
```

### Example 3: Per-Accused Split
```
FIR: "Seized 100g from A-1, 50g from A-2, 30g from A-3. Total seizure worth Rs.1,80,000"

Extraction:
✓ A-1: 100g Ganja (per-person amount)
✓ A-2: 50g Ganja (per-person amount)
✓ A-3: 30g Ganja (per-person amount)
✓ Total: 3 rows (Rule 2: per-person split = separate rows)

Drug Assignment (worth_scope="overall_total"):
- Each row gets full Rs.1,80,000 in extraction_metadata
- Assignment happens per accused
```

### Example 4: FILTERED - Sold Quantity (Not Seized)
```
FIR: "A-1 sold 1kg Ganja to A-3. Police found nothing during apprehension."

Processing:
✗ FILTERED by SeizureFilter (sold marker + no seizure marker)
✓ Extraction: {} (empty, no rows)

Reason: This is a downstream transaction, not a seizure at crime spot
```

### Example 5: FILTERED - Consumption Only
```
FIR: "Urine test showed heroin positive for A-1. No narcotics confiscated."

Processing:
✗ FILTERED by SeizureFilter (consumption marker + no seizure marker)
✓ Extraction: {} (empty, no rows)

Reason: Test result, no drugs physically seized
```

### Example 6: KEPT - Consumption + Seizure
```
FIR: "Urine test positive for ganja. Later, 2kg ganja was seized during search."

Processing:
✓ KEPT (consumption marker EXISTS, but seizure marker ALSO present)
✓ Extraction: 1 row with 2kg Ganja (Rule 13: seizure marker overrides)

Reason: Drugs were physically seized at crime spot
```

### Example 7: Mixed - Consumed Some, Seized Some
```
FIR: "A-1 had 6kg Ganja total. Smoked some before arrest. 
      Apprehended with remaining 5kg. Seized 5kg from possession."

Processing:
✓ Extract: 5kg (Rule 7: only seized quantity)
✓ Total: 1 row for 5kg

Reason: 6kg total is mixed (consumed + seized). Only extract the 5kg seized.
        Do NOT extract 6kg (includes consumed).
```

### Example 8: Joint Possession
```
FIR: "A-1 (adult) and A-3 (CCL) jointly possessed 200g Ganja. 
      Both apprehended together. Seized 200g total."

Extraction:
✓ 200g Ganja, accused_ref=null (Rule 6b: collective, joint possession)
✓ Total: 1 row (NOT 2 rows, no duplication)

Drug Assignment:
- Single entry (no specific accused mentioned in source_sentence)
→ Assign to A-1 (primary accused, default)

Why not 2 rows?
- Rule 6b prevents duplication when seized as group
- Both possessed together = 1 group seizure, not 2 individual seizures
```

### Example 9: Buyer Later Arrested (Rule 6 Edge Case)
```
FIR: "A-1 confessed selling to A-3. Later, A-3 apprehended with 100g Ganja. 
      Seized from A-3's possession."

Extraction:
✓ A-3: 100g Ganja (Rule 6: buyer was later apprehended with seizure)
✓ Total: 1 row

Reason: A-3 is NOT just a "buyer mentioned in confession".
        A-3 WAS apprehended and had seizure from their possession.
        This is a valid seized drug row.
```

### Example 10: Complex Multi-Fact with Filters
```
FIR: "A-1 purchased 6kg Ganja before arrest for Rs.60,000.
      Smoked some during jail waiting period (tested positive for 2kg).
      Apprehended with 4kg remaining. Seized 4kg at arrest location.
      A-3 (buyer) was told about the deal but nothing seized from A-3."

Extraction:
✓ Extract ONLY: 4kg Ganja (seized at arrest)
✓ Total: 1 row

Filtering:
✗ Purchased 6kg → NOT extracted (historical purchase, not seizure)
✗ Smoked/tested positive 2kg → NOT extracted (consumption, no seizure)
✗ A-3 buyer → NOT extracted (Rule 5: buyer with no seizure from them)
✓ Seized 4kg → EXTRACTED (Rule 0: physically seized at crime spot)

Drug Assignment:
- 4kg Ganja → assign to A-1 (only accused with seizure)
```

---

## Summary: How Rules Interact

1. **RULE 0 (Seizure-Only)**: Is it seized? If no → filter out completely
2. **RULE 13 (Seizure Filter)**: Apply consumption-only and sold-only checks
3. **RULES 1-7 (Clubbing Logic)**: How many rows? (collective vs individual)
4. **RULE 8-12 (Details)**: Who, worth, type, form
5. **Drug Assignment**: Link to accused if mentioned, else A-1

**All rules work together to ensure:**
- Only seized drugs extracted (not sold/consumed/historical)
- Correct grouping (collective or per-person)
- Proper worth attribution (individual/drug_total/overall_total)
- Correct accused assignment (explicit or A-1 default)

---

## File Locations

- **Extraction Prompt**: `brief_facts_ai/extractor_drugs.py` line 794+
- **Seizure Filter**: `brief_facts_ai/extractor_drugs.py` line 946+ (filter_consumption_only_drugs)
- **Consolidation Logic**: `brief_facts_ai/extractor_drugs.py` line 1459+ (deduplicate_extractions)
- **Drug Assignment**: `brief_facts_ai/db.py` line 666+ (write_drugs_by_accused_in_memory)
