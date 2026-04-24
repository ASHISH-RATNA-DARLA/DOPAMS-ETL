# Drug Extraction & Association with Accused - Complete Flow

## Overview

```
Brief Facts Text
    ↓
[STEP 1] Extract all drugs mentioned
    ↓
[STEP 2] Extract all accused mentioned
    ↓
[STEP 3] Link drugs to specific accused (6 cases)
    ↓
brief_facts_ai table (accused with drugs array)
```

---

## STEP 1: Drug Extraction Pipeline

### **Input:**
```
Brief Facts Text: "Raj Kumar and Priya Singh were caught with 2kg ganja 
                   and 100 heroin tablets. Raj had the ganja, Priya had the heroin."
```

### **Process: 9-Step Pipeline**

```python
def extract_drug_info(text):
    
    # Step 0: Preprocess
    # Split multi-FIR text, filter only drug-relevant sections
    filtered_text = preprocess_brief_facts(text, dynamic_drug_keywords)
    
    # Step 1: Token budget check
    # Ensure text fits in LLM context window
    est_tokens = estimate_tokens(filtered_text)
    
    # Step 2: LLM Extraction
    # Call Claude/LLM to extract drug information
    drug_data = llm.extract_drugs(filtered_text)
    # Returns:
    # [
    #   {
    #     'primary_drug_name': 'ganja',
    #     'raw_quantity': '2',
    #     'raw_unit': 'kg',
    #     'extraction_metadata': {
    #       'source_sentence': 'Raj had the ganja',
    #       'person_codes': ['A1']  ← If code mentioned
    #     }
    #   },
    #   {
    #     'primary_drug_name': 'heroin',
    #     'raw_quantity': '100',
    #     'raw_unit': 'tablets',
    #     'extraction_metadata': {
    #       'source_sentence': 'Priya had the heroin',
    #       'person_codes': ['A2']  ← If code mentioned
    #     }
    #   }
    # ]
    
    # Step 3: Sanitize input
    # None guards, validate entries
    
    # Step 4: Resolve drug names against KB
    # Convert "ganja" → standard "Cannabis/Ganja"
    # Using 3-tier matching: exact, substring, fuzzy (DB)
    
    # Step 5: Filter non-drugs
    # Remove vehicles, alcohol, paraphernalia from KB ignore_list
    
    # Step 6: Standardize units
    # "2 kg" → weight_g: 2000
    # "100 tablets" → count_total: 100
    
    # Step 7: Distribute worth
    # If total_seizure_worth = $5000 and 2 drugs:
    # Allocate $2500 to each drug
    
    # Step 8: Commercial quantity check
    # If quantity >= NDPS threshold → is_commercial = True
    
    # Step 9: Deduplicate
    # Merge duplicate entries, cap at 100
    
    return drug_data  # List of standardized drug extractions
```

### **Output:**

```python
[
    {
        'primary_drug_name': 'GANJA',           # Standardized
        'raw_quantity': '2',
        'raw_unit': 'kg',
        'weight_g': 2000,
        'weight_kg': 2,
        'seizure_worth': '2500',                # Allocated portion
        'is_commercial': True,                  # >= NDPS threshold?
        'supplier_name': None,                  # If mentioned
        'source_location': 'crime scene',       # If mentioned
        'extraction_metadata': {
            'source_sentence': 'Raj had the ganja',
            'person_codes': ['A1'],             # ← Person code if mentioned
            'persons_mentioned': ['Raj Kumar']   # ← Names if mentioned
        }
    },
    {
        'primary_drug_name': 'HEROIN',
        'raw_quantity': '100',
        'raw_unit': 'tablets',
        'count_total': 100,
        'seizure_worth': '2500',
        'is_commercial': False,
        'extraction_metadata': {
            'source_sentence': 'Priya had the heroin',
            'person_codes': ['A2'],
            'persons_mentioned': ['Priya Singh']
        }
    }
]
```

---

## STEP 2: Accused Extraction (Parallel)

```python
# Accused are extracted separately using LLM
# See: _process_branch_a(), _process_branch_b(), _process_branch_c()

bfai_rows = [
    {
        'crime_id': 12345,
        'accused_id': 'uuid-1',
        'person_code': 'A1',              # ← Person code (DB or LLM assigned)
        'full_name': 'Raj Kumar',
        'role_in_crime': 'perpetrator',
        'drugs': []                       # ← Empty, will be filled below
    },
    {
        'crime_id': 12345,
        'accused_id': 'uuid-2',
        'person_code': 'A2',
        'full_name': 'Priya Singh',
        'role_in_crime': 'perpetrator',
        'drugs': []                       # ← Empty, will be filled below
    }
]
```

---

## STEP 3: Link Drugs to Accused (write_drugs_by_accused_in_memory)

This is the **CRITICAL ASSOCIATION LOGIC**. For each drug extracted, determine which accused(s) it belongs to.

### **6 Association Cases:**

```python
def write_drugs_by_accused_in_memory(bfai_rows, drug_data_list):
    """
    For each drug in drug_data_list:
        ├─ Case 1: INDIVIDUAL (code match)
        │           → Single accused mentioned by A-code
        │
        ├─ Case 2: INDIVIDUAL (name match)
        │           → Single accused mentioned by name
        │
        ├─ Case 3: COLLECTIVE_TOTAL
        │           → Multiple accused mentioned (shared seizure)
        │           → Drug on FIRST accused only (no ghost copies)
        │
        ├─ Case 4A: COLLECTIVE_CONSUMPTION
        │           → No code/name, but consumption markers
        │           → Drug on ALL accused
        │
        ├─ Case 4B: UNATTRIBUTED_FALLBACK_A1
        │           → No code or name match
        │           → Drug on PRIMARY (first) accused
        │
        ├─ Case 5: NO_DRUGS_DETECTED (sentinel)
        │           → Stamp on ALL accused
        │
        └─ Case 6: NO_ACCUSED_ORPHAN (sentinel)
                    → Drug on orphan sentinel row
    """
```

### **Case-by-Case Explanation:**

#### **Case 1: INDIVIDUAL (Code Match)**

**Text:**
```
"A1 had 2kg ganja. A2 had 100 heroin tablets."
```

**Extraction:**
```
Drug 1:
  primary_drug_name: 'ganja'
  extraction_metadata:
    person_codes: ['A1']    ← Code extracted!

Drug 2:
  primary_drug_name: 'heroin'
  extraction_metadata:
    person_codes: ['A2']    ← Code extracted!
```

**Association Logic:**

```python
for drug in drug_data_list:
    mentioned_codes = extract_person_codes(drug)
    # Drug 1: mentioned_codes = ['A1']
    # Drug 2: mentioned_codes = ['A2']
    
    matched_rows = [rows_by_code[code] for code in mentioned_codes if code in rows_by_code]
    # Drug 1: matched_rows = [Raj Kumar row] (A1)
    # Drug 2: matched_rows = [Priya Singh row] (A2)
    
    if len(matched_rows) == 1:
        # One accused has this drug
        logger.info(f"INDIVIDUAL: {drug_name} → {matched_rows[0]['full_name']}")
        add_drug_to_accused(matched_rows[0], drug)
```

**Output:**

```sql
brief_facts_ai:

Row 1 (Raj Kumar):
  person_code: 'A1'
  full_name: 'Raj Kumar'
  drugs: [
    {
      'primary_drug_name': 'GANJA',
      'raw_quantity': '2 kg',
      'attribution_type': 'INDIVIDUAL'
    }
  ]

Row 2 (Priya Singh):
  person_code: 'A2'
  full_name: 'Priya Singh'
  drugs: [
    {
      'primary_drug_name': 'HEROIN',
      'raw_quantity': '100 tablets',
      'attribution_type': 'INDIVIDUAL'
    }
  ]
```

---

#### **Case 2: INDIVIDUAL (Name Match)**

**Text:**
```
"Seized 2kg ganja from Raj Kumar."
```

**No A-code in source_sentence, but name is present.**

**Association Logic:**

```python
source_sentence = "Seized 2kg ganja from Raj Kumar."

# No A-codes found
if not matched_rows and source_sentence:
    # Fallback: try name matching
    matched_rows = match_rows_by_name(source_sentence, ordered_real_rows)
    # Searches for "Raj Kumar" in rows
    # → Finds: Raj Kumar row
    
    logger.info(f"NAME_MATCH: ganja → Raj Kumar")
    add_drug_to_accused(matched_rows[0], drug)
```

**Output:**

```sql
Row (Raj Kumar):
  drugs: [
    {
      'primary_drug_name': 'GANJA',
      'raw_quantity': '2 kg',
      'attribution_type': 'INDIVIDUAL'
    }
  ]
```

---

#### **Case 3: COLLECTIVE_TOTAL (Multiple Accused, One Seizure)**

**Text:**
```
"A1, A2, A3 were caught with a total of 520 kg ganja."
```

**Multiple codes but SINGLE seizure event.**

**Association Logic:**

```python
mentioned_codes = ['A1', 'A2', 'A3']
matched_rows = [row_A1, row_A2, row_A3]

if len(matched_rows) > 1:
    # Collective seizure
    logger.info(f"COLLECTIVE_TOTAL: 520kg ganja → {[r['person_code'] for r in matched_rows]} → stored on {matched_rows[0]['person_code']} only")
    
    # Add drug to FIRST accused only, no ghost copies on A2/A3
    add_drug_to_accused(matched_rows[0], drug)  # Only A1
```

**Why no ghost copies?**
- Prevents double-counting in aggregates
- 520 kg belongs to the seizure event, not separately to each accused
- A2 and A3 can still be linked via network analysis (co-accused)

**Output:**

```sql
Row 1 (A1 - Raj Kumar):
  drugs: [
    {
      'primary_drug_name': 'GANJA',
      'raw_quantity': '520 kg',
      'attribution_type': 'COLLECTIVE_TOTAL',
      'persons_included': ['A1', 'A2', 'A3']  ← Metadata
    }
  ]

Row 2 (A2 - Priya Singh):
  drugs: []  ← NO ghost entry

Row 3 (A3 - Amit Sharma):
  drugs: []  ← NO ghost entry
```

---

#### **Case 4A: COLLECTIVE_CONSUMPTION (All Accused Share Drug)**

**Text:**
```
"All three accused persons tested positive for ganja in urine test."
```

**No code/name match, but consumption markers + multiple accused.**

**Association Logic:**

```python
source_sentence = "All three accused persons tested positive for ganja in urine test."
consumption_markers = ['tested positive', 'consumption', 'smoked', 'consumed']
collective_markers = ['all accused', 'they', 'all of them']

is_collective_consumption = (
    len(ordered_real_rows) > 1  # Multiple accused
    and any(m in source_sentence.lower() for m in consumption_markers)  # True
    and any(m in source_sentence.lower() for m in collective_markers)  # True
)

if is_collective_consumption:
    # Drug on ALL accused
    for row in ordered_real_rows:
        add_drug_to_accused(row, drug)
```

**Output:**

```sql
Row 1 (A1 - Raj Kumar):
  drugs: [
    {
      'primary_drug_name': 'GANJA',
      'attribution_type': 'COLLECTIVE_CONSUMPTION'
    }
  ]

Row 2 (A2 - Priya Singh):
  drugs: [
    {
      'primary_drug_name': 'GANJA',
      'attribution_type': 'COLLECTIVE_CONSUMPTION'
    }
  ]

Row 3 (A3 - Amit Sharma):
  drugs: [
    {
      'primary_drug_name': 'GANJA',
      'attribution_type': 'COLLECTIVE_CONSUMPTION'
    }
  ]
```

---

#### **Case 4B: FALLBACK_A1 (No Attribution Found)**

**Text:**
```
"100 tablets seized from the crime scene."
```

**No code, no name, nothing to match.**

**Association Logic:**

```python
mentioned_codes = []  # No codes
source_sentence = "100 tablets seized from the crime scene."

# No name-based match either
matched_rows = match_rows_by_name(source_sentence, ordered_real_rows)
# Returns [] (no names found)

if not matched_rows:
    # Fallback: assign to primary (first) accused
    primary_row = get_primary_row(ordered_real_rows)  # Usually A1
    logger.info(f"FALLBACK_A1: {drug_name} → {primary_row['person_code']}")
    add_drug_to_accused(primary_row, drug)
```

**Output:**

```sql
Row 1 (A1 - Raj Kumar - PRIMARY):
  drugs: [
    {
      'primary_drug_name': 'UNKNOWN/HEROIN',
      'raw_quantity': '100 tablets',
      'attribution_type': 'UNATTRIBUTED_FALLBACK_A1'
    }
  ]

Row 2 (A2 - Priya Singh):
  drugs: []

Row 3 (A3 - Amit Sharma):
  drugs: []
```

---

#### **Case 5: NO_DRUGS_DETECTED (Sentinel)**

**Text:**
```
"Crime occurred. No narcotics involved."
```

**Extracted:** `NO_DRUGS_DETECTED` sentinel

**Association Logic:**

```python
if primary_name == 'NO_DRUGS_DETECTED':
    # Stamp on ALL accused so every row has drugs[]
    for row in ordered_real_rows:
        add_drug_to_accused(row, {'primary_drug_name': 'NO_DRUGS_DETECTED'})
```

**Output:**

```sql
Row 1 (A1):
  drugs: [{'primary_drug_name': 'NO_DRUGS_DETECTED'}]

Row 2 (A2):
  drugs: [{'primary_drug_name': 'NO_DRUGS_DETECTED'}]

Row 3 (A3):
  drugs: [{'primary_drug_name': 'NO_DRUGS_DETECTED'}]
```

---

#### **Case 6: NO_ACCUSED_ORPHAN (Sentinel for Drugs But No Accused)**

**Text:**
```
"10kg heroin seized. Suspects still unknown."
```

**Drugs found but no accused records exist.**

**Association Logic:**

```python
# Special orphan row created for this case
orphan_row = {
    'crime_id': crime_id,
    'accused_id': None,
    'full_name': None,
    'role_in_crime': 'NO_ACCUSED_DRUGS_ONLY'  ← Marker
}

if orphan_row:
    # Add drug to orphan
    add_drug_to_accused(orphan_row, drug)
```

**Output:**

```sql
Row (ORPHAN):
  crime_id: 12345
  accused_id: NULL
  full_name: NULL
  role_in_crime: 'NO_ACCUSED_DRUGS_ONLY'
  drugs: [
    {
      'primary_drug_name': 'HEROIN',
      'raw_quantity': '10 kg',
      'attribution_type': 'NO_ACCUSED_ORPHAN'
    }
  ]
```

---

## STEP 4: Consolidation (Merge Duplicate Drugs)

**Rule:** If same accused has same drug from multiple seizures in SAME CRIME, merge into one entry with aggregated quantity.

```python
# Consolidation key: (primary_drug_name, supplier_name, source_location)

def add_or_consolidate_drug(accused_row, drug_data):
    """
    If drug already exists with same key:
        Merge quantities (1kg + 2kg = 3kg)
        Merge worth ($1000 + $1500 = $2500)
    Else:
        Add as new entry
    """
    
    # Example:
    # Raj Kumar already has:
    #   {primary_drug_name: 'GANJA', supplier: 'X', source: 'location1', qty: 1000g}
    #
    # New drug entry:
    #   {primary_drug_name: 'GANJA', supplier: 'X', source: 'location1', qty: 500g}
    #
    # Check consolidation key:
    #   ('GANJA', 'X', 'location1') = ('GANJA', 'X', 'location1')  ✓ Match!
    #
    # Result:
    #   {primary_drug_name: 'GANJA', supplier: 'X', source: 'location1', qty: 1500g}
```

**Output:**

```sql
Raj Kumar's drugs array:
  Before:
    [
      {'name': 'GANJA', 'qty': 1000, 'supplier': 'X', 'source': 'loc1'},
      {'name': 'GANJA', 'qty': 500, 'supplier': 'X', 'source': 'loc1'}
    ]
  
  After consolidation:
    [
      {'name': 'GANJA', 'qty': 1500, 'supplier': 'X', 'source': 'loc1'}
    ]
```

---

## Complete Example: End-to-End

### **Input: Crime #12345**

```
Brief Facts: "Raj Kumar (A1) and Priya Singh (A2) committed theft.
              2kg ganja was seized from Raj. 100 heroin tablets found 
              with Priya. They had total $5000 worth of drugs."
```

### **Processing:**

```
STEP 1: Extract drugs
  → [
      {primary_drug_name: 'GANJA', qty: '2kg', source_sentence: 'from Raj', person_codes: ['A1']},
      {primary_drug_name: 'HEROIN', qty: '100 tablets', source_sentence: 'with Priya', person_codes: ['A2']}
    ]

STEP 2: Extract accused
  → [
      {crime_id: 12345, person_code: 'A1', full_name: 'Raj Kumar', drugs: []},
      {crime_id: 12345, person_code: 'A2', full_name: 'Priya Singh', drugs: []}
    ]

STEP 3: Link drugs to accused
  Drug 1 (GANJA):
    - Check person_codes: ['A1']
    - Matched rows: [Raj Kumar]
    - len(matched_rows) == 1 → Case 1 (INDIVIDUAL)
    - Add to Raj Kumar
  
  Drug 2 (HEROIN):
    - Check person_codes: ['A2']
    - Matched rows: [Priya Singh]
    - len(matched_rows) == 1 → Case 1 (INDIVIDUAL)
    - Add to Priya Singh

STEP 4: Insert into brief_facts_ai
```

### **Output: brief_facts_ai table**

```sql
SELECT * FROM brief_facts_ai WHERE crime_id = 12345;

┌────────────────────────────────────────────────────────┐
│ Row 1 (Raj Kumar - A1)                                 │
├────────────────────────────────────────────────────────┤
│ person_code: 'A1'                                      │
│ full_name: 'Raj Kumar'                                 │
│ role_in_crime: 'perpetrator'                          │
│ drugs: [                                               │
│   {                                                    │
│     'primary_drug_name': 'GANJA',                      │
│     'raw_quantity': '2',                               │
│     'raw_unit': 'kg',                                  │
│     'weight_kg': 2,                                    │
│     'seizure_worth': '2500',  ← Allocated 50% of $5K  │
│     'is_commercial': True,                            │
│     'attribution_type': 'INDIVIDUAL',                  │
│     'source_sentence': '2kg ganja from Raj'           │
│   }                                                    │
│ ]                                                      │
├────────────────────────────────────────────────────────┤
│ Row 2 (Priya Singh - A2)                               │
├────────────────────────────────────────────────────────┤
│ person_code: 'A2'                                      │
│ full_name: 'Priya Singh'                               │
│ role_in_crime: 'perpetrator'                          │
│ drugs: [                                               │
│   {                                                    │
│     'primary_drug_name': 'HEROIN',                     │
│     'raw_quantity': '100',                             │
│     'raw_unit': 'tablets',                             │
│     'count_total': 100,                                │
│     'seizure_worth': '2500',  ← Allocated 50% of $5K  │
│     'is_commercial': False,                           │
│     'attribution_type': 'INDIVIDUAL',                  │
│     'source_sentence': '100 heroin tablets with Priya'│
│   }                                                    │
│ ]                                                      │
└────────────────────────────────────────────────────────┘
```

---

## Association Rules Summary

| Case | Condition | Assignment | Reason |
|------|-----------|-----------|--------|
| **1** | Single A-code in source | That accused only | Code is explicit |
| **2** | Single name in source | That accused only | Name is explicit |
| **3** | Multiple A-codes (shared seizure) | First accused only | No duplication |
| **4A** | No code/name, but consumption markers | ALL accused | Shared consumption |
| **4B** | No code, no name, no markers | Primary (A1) accused | Safe fallback |
| **5** | NO_DRUGS_DETECTED sentinel | ALL accused | Mark all clear |
| **6** | NO_ACCUSED_ORPHAN sentinel | Orphan row | No accused yet |

---

## Key Points

1. **One drug entry per (primary_drug_name, accused_id) pair per crime**
   - No ghost copies for collective seizures
   - Prevents double-counting in aggregates

2. **Worth is distributed proportionally**
   - If 2 drugs with total worth $5000
   - Each gets $2500

3. **Person codes (A1, A2, A3) are primary matching**
   - Set by DB or LLM during accused processing
   - Used to link drugs to specific accused

4. **Consolidation merges duplicate drugs**
   - Same drug from multiple packets → single entry with summed quantity
   - Only within same crime + same accused

5. **Fallback to A1 if no attribution**
   - Ensures every drug is linked to someone
   - Marked with attribution_type for transparency

6. **Collective consumption means all accused share the drug**
   - Use only for consumption markers (tested positive, urine test)
   - Not for seizures

---

## Code References

```
Drug Extraction:
  /brief_facts_ai/extractor_drugs.py:1530  → extract_drug_info()

Drug Association:
  /brief_facts_ai/db.py:666  → write_drugs_by_accused_in_memory()

Helper Functions:
  _extract_person_codes()          → Get A-codes from drug metadata
  _match_rows_by_name()             → Match accused by name
  _pick_primary_row()               → Get first/primary accused
  _add_or_consolidate_drug()        → Add or merge drug to accused
  _build_drug_element()             → Create drug entry with metadata
```

---

**Summary:** Drugs are associated with accused through 6 intelligent cases, prioritizing explicit mentions (codes/names), falling back to rules-based attribution, and preventing double-counting through consolidation.
