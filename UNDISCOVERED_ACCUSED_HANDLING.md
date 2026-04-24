# Undiscovered Accused Handling: Gap-Fill Logic

## The Problem

**Scenario:**
```
Crime #12345 in brief_facts_ai table:
  - Accused in DB: Raj Kumar (person_id=uuid-person-1, in persons table)
  - Processing: BRANCH A (DB has accused, person confirmed)

BUT when LLM reads brief_facts text:
  "Raj Kumar and Priya Singh stole 100kg gold from XYZ store..."
  
  → Priya Singh is MENTIONED but NEVER ARRESTED/RECORDED
  → She exists in the narrative but NOT in the accused table
  → She would be missed if we only process DB accused!
```

---

## The Solution: "Gap-Fill" Logic

When processing **BRANCH A**, the code does NOT stop after processing DB accused. It continues with a secondary pass called **"gap-fill"** that:

1. **Extracts all names from brief_facts text** using LLM
2. **Matches extracted names against DB accused**
3. **Identifies unmatched names** (accused mentioned in text but NOT in DB)
4. **Creates synthetic records** for these undiscovered accused
5. **Inserts them into brief_facts_ai table**

### **Code Location:**
```
File: /data-drive/etl-process-dev/brief_facts_ai/main.py
Function: _process_branch_a()
Lines: 1504-1656
```

---

## Step-by-Step Flow

### **Step 1: Build DB Name Variants (Lines 1490-1502)**

```python
# Collect all possible name variations from DB accused
db_name_variants = []
db_names_norm = set()

for row in valid_accused:
    full_name = row.get('full_name')
    alias_name = row.get('alias_name')
    
    if full_name:
        db_name_variants.append(full_name)
        db_names_norm.add(_normalize_name(full_name))
    
    if alias_name:
        db_name_variants.append(alias_name)
        db_names_norm.add(_normalize_name(alias_name))

# Result: ['Raj Kumar', 'Raju', 'Priya Singh']  (from DB)
```

### **Step 2: Extract Names from Text (Line 1504)**

```python
text_names = extract_accused_names_pass1(facts_text)

# Result: ['Raj Kumar', 'Priya Singh', 'Unknown Male']  (from LLM)
```

### **Step 3: Filter Out Known Accused (Lines 1505-1522)**

For each name extracted from text, check if it's already in DB:

```python
new_names = []  # Names NOT in DB

for raw in text_names:
    clean = clean_accused_name(raw)
    
    # GUARD 1: Skip police/official names
    if _is_police_name(clean, facts_text):
        logging.info(f"Dropped '{clean}' (police guard)")
        continue
    
    # GUARD 2: Skip supplier-context names
    if _is_supplier_context(clean, facts_text):
        logging.info(f"Dropped '{clean}' (supplier guard)")
        continue
    
    # GUARD 3: Check if name is already in DB accused
    if _match_extracted_name_to_db_accused(clean, db_name_variants):
        logging.info(f"'{clean}' matched to existing accused, skipping")
        continue  # ← Already in DB, skip it
    
    # ✓ Not in DB, add to new_names
    new_names.append(raw)

# Result: new_names = ['Priya Singh', 'Unknown Male']
```

### **Step 4: Extract Details for New Accused (Line 1529)**

```python
extra_details = extract_details_pass2(facts_text, new_names)

# Returns:
[
    {
        'full_name': 'Priya Singh',
        'age': 26,
        'gender': 'Female',
        'role_in_crime': 'accomplice',
        'address': '456 Oak Lane',
        'alias_name': 'Pri',
        'occupation': 'Jeweler'
    },
    {
        'full_name': 'Unknown Male',
        'age': None,
        'gender': 'Male',
        'role_in_crime': 'handler',
        'address': None,
        ...
    }
]
```

### **Step 5: Create Synthetic Records (Lines 1533-1655)**

For each new_name not in DB:

```python
for raw_name in new_names:
    clean = clean_accused_name(raw_name)
    
    # Look up extracted details
    d_obj = detail_map_extra.get(clean.lower().strip())
    
    # Create synthetic record
    synth_id = _synthetic_accused_id(crime_id, clean, None)
    # synth_id = UUID5(hash of crime_id + clean_name)
    
    extra_row = {
        'crime_id'             : 12345,
        'accused_id'           : synth_id,          # Synthetic UUID
        'person_id'            : None,              # Not in persons table
        'canonical_person_id'  : canonical_extra,  # Fuzzy-matched canonical
        'person_code'          : None,              # No DB code
        'full_name'            : clean,             # "Priya Singh"
        'alias_name'           : d_obj.alias_name,  # "Pri"
        'age'                  : d_obj.age,         # 26
        'gender'               : d_obj.gender,      # "Female"
        'role_in_crime'        : d_obj.role_in_crime,  # "accomplice"
        'status'               : 'Extracted',       # Tentative (not arrested)
        'is_ccl'               : False,
        'existing_accused'     : False,             # NOT in DB
        'source_accused_fields': {
            'ps_code': ps_code,
            'accused_id': 'SYNTHETIC_GAP_FILL'    # ← Marker: synthetic record
        },
        'source_summary_fields': {
            'note': 'Gap-filled from brief_facts text, not in accused table'
        },
    }
    
    # Insert into brief_facts_ai
    insert_accused_facts(conn, extra_row)
    count += 1

# Logs:
# "Branch A gap-fill: 2 text-only accused found for Crime 12345: 
#  ['Priya Singh', 'Unknown Male']"
```

---

## The Matching Logic: `_match_extracted_name_to_db_accused()`

How does it determine if a name is "already in DB"?

```python
def _match_extracted_name_to_db_accused(extracted_name: str, db_name_variants: list) -> bool:
    """
    Conservative matching to detect when extracted text name 
    already exists in DB accused.
    """
    for db_name in db_name_variants:
        # Level 1: Exact match
        if extracted_name.lower().strip() == db_name.lower().strip():
            return True
        
        # Level 2: Fuzzy match (Jaro-Winkler >= 0.85)
        if _name_similarity(extracted_name, db_name) >= 0.85:
            return True
        
        # Level 3: Soundex match
        if _soundex(extracted_name) == _soundex(db_name):
            return True
        
        # Level 4: Token-set similarity (>= 0.8)
        if _token_set_similarity(extracted_name, db_name) >= 0.80:
            return True
    
    return False  # Not in DB
```

**Examples:**
```
Extracted name: "Raj Kumar"
DB names: ["Raj Kumar", "Raju", "Kumar Raj"]

✓ Match: "Raj Kumar" == "Raj Kumar" (exact)
✓ Match: Soundex("Raj Kumar") == Soundex("Kumar Raj")
✓ Match: Tokens {raj, kumar} == {raj, kumar}

→ return True (already in DB, skip)


Extracted name: "Priya Singh"
DB names: ["Raj Kumar", "Raju", "Kumar Raj"]

✗ No exact match
✗ No fuzzy match (similarity < 0.85)
✗ No soundex match
✗ No token match

→ return False (NOT in DB, add to new_names)
```

---

## Guards Against False Positives

The code has 3 **guards** to prevent creating duplicate/false records:

### **Guard 1: Police Name Filter (Line 1512)**
```python
if _is_police_name(raw, facts_text) or _is_police_name(clean, facts_text):
    logging.info(f"Branch A gap-fill: police guard dropped '{clean}'")
    continue  # Don't create record for police officer
```

**Why?** Don't create records for investigators/officers mentioned in text.

**Example:**
```
Text: "Investigation Officer Amit Sharma arrested the accused..."
      → "Amit Sharma" should NOT be a separate accused record
      → Skip due to police guard
```

---

### **Guard 2: Supplier Context Filter (Line 1515)**
```python
if _is_supplier_context(clean, facts_text):
    logging.info(f"Branch A gap-fill: supplier guard dropped '{clean}'")
    continue
```

**Why?** When context is purely supplier/procurement, may not be accused.

**Example:**
```
Text: "...purchased from xyz supplier..."
      → "xyz" is not an accused, just a supplier
      → Skip due to supplier guard
```

---

### **Guard 3: Role-Only Mention Filter (Line 1573)**
```python
should_skip_role, status_override, type_override, reason = \
    _should_skip_role_only_mention(clean, facts_text)

if should_skip_role:
    logging.info(f"Branch A gap-fill: '{clean}' skipped due to {reason}")
    continue  # Don't create record
```

**Why?** Some names are mentioned only in context of roles, not as primary accused.

**Example:**
```
Text: "...informant informed by John about the crime..."
      → "John" is just a reference, not an accused
      → Skip due to role-only filter
```

---

## Output: What Gets Inserted

For each undiscovered accused found in text:

```sql
INSERT INTO brief_facts_ai (
    crime_id,                    12345,
    accused_id,                  'synthetic-uuid-xyz',  -- Generated UUID
    person_id,                   NULL,                  -- Not in DB
    canonical_person_id,         'canonical-uuid-abc',  -- Fuzzy-matched
    person_code,                 NULL,                  -- No DB code
    full_name,                   'Priya Singh',
    age,                         26,
    gender,                      'Female',
    role_in_crime,               '["accomplice"]',
    status,                      'Extracted',
    existing_accused,            FALSE,                 -- Key: NOT in DB
    source_accused_fields,       '{"accused_id":"SYNTHETIC_GAP_FILL"}',
    source_summary_fields,       '{"note":"gap-filled from brief_facts"}',
    date_created,                NOW()
) ON CONFLICT DO UPDATE SET date_modified=NOW();
```

**Key Fields:**
- `source_accused_fields.accused_id = "SYNTHETIC_GAP_FILL"` ← Marker for synthetic
- `existing_accused = FALSE` ← Indicates it's a gap-fill, not DB-sourced
- `status = "Extracted"` ← Tentative status (not confirmed by arrest)

---

## Example: Complete Scenario

### **Input Data:**

```
Crime #12345 in Database:
┌────────────────────────────────────────────┐
│ crimes table                               │
├────────────────────────────────────────────┤
│ crime_id:  12345                           │
│ fir:       "2024-1234"                     │
│ brief_facts: "Raj Kumar and Priya Singh    │
│              stole gold from XYZ store.    │
│              Raj arrested, Priya fled."    │
└────────────────────────────────────────────┘

accused table:
┌────────────────────────────────────────┐
│ accused_id (uuid-1)                    │
│ crime_id: 12345                        │
│ person_id: uuid-person-101  ✓          │
│ full_name: "Raj Kumar"                 │
│ status: "Arrested"                     │
└────────────────────────────────────────┘
(Only 1 row! Priya is NOT in accused table)
```

### **Processing (Branch A):**

```python
1. Fetch accused for crime 12345
   db_accused = [{'accused_id': uuid-1, 'person_id': uuid-person-101, ...}]
   
2. Classify branch
   if not db_accused: return 'C'  # False
   if any(person_id): return 'A'  # True! (uuid-person-101 exists)
   
3. Process DB accused (Raj Kumar)
   → Inserts 1 row into brief_facts_ai (Raj Kumar, role=perpetrator)
   
4. Gap-fill: Find undiscovered accused
   
   a. Extract names from text:
      text_names = ["Raj Kumar", "Priya Singh"]
   
   b. Build DB names:
      db_name_variants = ["Raj Kumar"]
   
   c. Filter new names:
      - "Raj Kumar" → _match_extracted_name_to_db_accused() → True
        (already in DB, skip)
      - "Priya Singh" → _match_extracted_name_to_db_accused() → False
        (NOT in DB, add to new_names)
      
      new_names = ["Priya Singh"]
   
   d. Extract details for Priya Singh:
      → age: 26, gender: Female, role: "accomplice", status: "Absconding"
   
   e. Create synthetic record for Priya Singh:
      extra_row = {
          'crime_id': 12345,
          'accused_id': 'synthetic-uuid-xyz',
          'person_id': NULL,
          'full_name': 'Priya Singh',
          'role_in_crime': 'accomplice',
          'status': 'Absconding',
          'existing_accused': FALSE,
          'source_accused_fields': {'accused_id': 'SYNTHETIC_GAP_FILL'},
      }
   
   f. Insert into brief_facts_ai
```

### **Output (brief_facts_ai table):**

```sql
SELECT * FROM brief_facts_ai WHERE crime_id = 12345;

┌─────────────────────────────────────────────────────────┐
│ Row 1 (from DB accused):                                │
│ accused_id:      uuid-1                                 │
│ person_id:       uuid-person-101  ✓                     │
│ full_name:       "Raj Kumar"                            │
│ role_in_crime:   "perpetrator"                         │
│ status:          "Charged"                             │
│ existing_accused: TRUE           ← From DB             │
│ source_accused_fields: {...}     ← From accused table  │
├─────────────────────────────────────────────────────────┤
│ Row 2 (from gap-fill):                                  │
│ accused_id:      synthetic-uuid-xyz                    │
│ person_id:       NULL            ✗                     │
│ full_name:       "Priya Singh"                         │
│ role_in_crime:   "accomplice"                         │
│ status:          "Absconding"                         │
│ existing_accused: FALSE          ← NOT from DB        │
│ source_accused_fields: {                              │
│    "accused_id": "SYNTHETIC_GAP_FILL" ← Marker       │
│ }                                                      │
└─────────────────────────────────────────────────────────┘
```

**Result:** Both Raj Kumar AND Priya Singh are captured in brief_facts_ai!

---

## Similar Logic in Branch B

Branch B also has gap-fill logic (Lines 1780-1850):

```python
# Branch B: DB has accused but person_id = NULL

# Process extracted names from LLM extraction
text_names = extract_accused_names_pass1(facts_text)

# Match to DB accused (by name matching)
for name in text_names:
    if name in db_names:
        # Use DB accused_id
        pair_with_db_accused()
    else:
        # Create synthetic record
        create_synthetic_record()
```

Similar logic but:
- DB accused rows are stubs (person_id = NULL)
- LLM must infer identity AND match to DB accused by name
- Creates synthetic records for text-only names

---

## Branch C (No Gap-Fill Needed)

Branch C (text-only) doesn't need gap-fill because:
- **Already processing ALL text-extracted names**
- **No DB accused to compare against**
- Everything is synthetic/LLM-extracted

---

## Summary: Undiscovered Accused Handling

| Aspect | Behavior |
|--------|----------|
| **When it happens** | Branch A or B processing encounters names in text not matching DB accused |
| **How detected** | `_match_extracted_name_to_db_accused()` checks fuzzy matching (4 levels) |
| **Guard filters** | Police names, supplier-context, role-only mentions (prevented from becoming accused) |
| **Record type** | Synthetic records marked `'SYNTHETIC_GAP_FILL'` + `existing_accused=FALSE` |
| **Status** | Usually "Extracted" (tentative, not confirmed by arrest) |
| **person_id** | NULL (person not in persons table) |
| **Inserted** | Into brief_facts_ai table alongside DB-sourced records |
| **Future update** | If person gets arrested later, this record is supplemented (not replaced) |

---

## Key Code References

```python
# Main gap-fill logic in Branch A:
/brief_facts_ai/main.py:1504-1656
    ├─ extract_accused_names_pass1()     # Extract text names
    ├─ _match_extracted_name_to_db_accused()  # Check if in DB
    ├─ extract_details_pass2()           # Get details for new names
    ├─ _is_police_name()                 # Guard: skip police
    ├─ _is_supplier_context()            # Guard: skip suppliers
    ├─ _should_skip_role_only_mention()  # Guard: skip role-only
    └─ insert_accused_facts()            # Insert synthetic record

# Matching function:
/brief_facts_ai/main.py:442-505
    └─ _match_extracted_name_to_db_accused()
```

---

**Conclusion:** Brief Facts AI DOES capture undiscovered accused persons mentioned in brief_facts text even in Branch A/B, through intelligent "gap-fill" logic with safeguards against false positives.
