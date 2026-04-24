# Undiscovered Accused: Concrete Example

## Real Scenario

### **DB State (accused table):**
```sql
SELECT * FROM accused WHERE crime_id = 12345;

4 rows returned:

┌──────────────────────────────────────────────────────┐
│ Row 1: Raj Kumar      (person_id = uuid-p-1) ✓      │
│ Row 2: Priya Singh    (person_id = uuid-p-2) ✓      │
│ Row 3: Amit Sharma    (person_id = NULL)            │
│ Row 4: Vikram Patel   (person_id = NULL)            │
└──────────────────────────────────────────────────────┘
```

**Branch Decision:**
- Does any row have person_id NOT NULL? → YES (Raj Kumar, Priya Singh)
- → **BRANCH A** (DB has at least some person_id confirmed)

---

### **Brief Facts Text:**
```
"Raj Kumar and Priya Singh, along with Amit Sharma and Vikram Patel, 
robbed the gold store. They were joined by Mohit Reddy and Anita Verma 
who helped them hide the stolen goods."
```

**Names mentioned in text:**
1. Raj Kumar ← In DB
2. Priya Singh ← In DB
3. Amit Sharma ← In DB
4. Vikram Patel ← In DB
5. **Mohit Reddy** ← NOT in DB ❌ UNDISCOVERED
6. **Anita Verma** ← NOT in DB ❌ UNDISCOVERED

---

## Processing Flow (Branch A)

### **Step 1: Process 4 DB Accused**

```python
# Loop through 4 DB accused rows
for row in valid_accused:  # 4 rows
    # Process Raj Kumar
    # Process Priya Singh
    # Process Amit Sharma
    # Process Vikram Patel
    
    → Insert 4 rows into brief_facts_ai

Result: 4 records created
```

### **Step 2: Gap-Fill (Find Undiscovered Accused)**

```python
# Extract ALL names from text
text_names = extract_accused_names_pass1(facts_text)
# Result: ["Raj Kumar", "Priya Singh", "Amit Sharma", 
#          "Vikram Patel", "Mohit Reddy", "Anita Verma"]

# Build DB name variants
db_name_variants = ["Raj Kumar", "Priya Singh", "Amit Sharma", "Vikram Patel"]

# Filter: Which text names are NOT in DB?
new_names = []
for raw in text_names:
    if _match_extracted_name_to_db_accused(raw, db_name_variants):
        continue  # Skip (already in DB)
    else:
        new_names.append(raw)  # Add (NOT in DB)

# Result: new_names = ["Mohit Reddy", "Anita Verma"]
#                      ↑ These 2 are undiscovered!
```

### **Step 3: Create Synthetic Records for Undiscovered**

```python
# For each undiscovered name, extract details and create record
for raw_name in new_names:  # ["Mohit Reddy", "Anita Verma"]
    
    # Extract Mohit Reddy details from text
    details_mohit = extract_details_pass2(facts_text, ["Mohit Reddy"])
    # → age: 28, gender: "Male", role: "handler", address: "..."
    
    # Create synthetic record
    synth_record_mohit = {
        'crime_id': 12345,
        'accused_id': 'synthetic-uuid-mohit',
        'person_id': NULL,  ← Not in DB
        'full_name': 'Mohit Reddy',
        'age': 28,
        'gender': 'Male',
        'role_in_crime': 'handler',
        'existing_accused': FALSE,  ← Key: NOT from DB
        'source_accused_fields': {'accused_id': 'SYNTHETIC_GAP_FILL'}
    }
    
    # Insert into brief_facts_ai
    insert_accused_facts(conn, synth_record_mohit)
    
    # Do same for Anita Verma
    # Insert second synthetic record
```

---

## Final Result in brief_facts_ai

```sql
SELECT * FROM brief_facts_ai WHERE crime_id = 12345;

┌──────────────────────────────────────────────────┐
│ 6 ROWS TOTAL:                                    │
├──────────────────────────────────────────────────┤
│ Row 1: Raj Kumar       (existing_accused=TRUE)   │
│ Row 2: Priya Singh     (existing_accused=TRUE)   │
│ Row 3: Amit Sharma     (existing_accused=TRUE)   │
│ Row 4: Vikram Patel    (existing_accused=TRUE)   │
│ Row 5: Mohit Reddy     (existing_accused=FALSE)  │ ← SYNTHETIC
│ Row 6: Anita Verma     (existing_accused=FALSE)  │ ← SYNTHETIC
└──────────────────────────────────────────────────┘
```

---

## How It Determines Undiscovered (The Matching Logic)

### **The Fuzzy Match Function:**

```python
def _match_extracted_name_to_db_accused(extracted_name, db_name_variants):
    """
    Check if extracted_name is ALREADY in DB.
    Returns True if match found (skip), False if new (add).
    """
    
    for db_name in db_name_variants:
        
        # Level 1: Exact match
        if extracted_name.lower() == db_name.lower():
            return True  # Found in DB
        
        # Level 2: Fuzzy match (Jaro-Winkler)
        if similarity(extracted_name, db_name) >= 0.85:
            return True  # Found in DB (with typos)
        
        # Level 3: Soundex (phonetic)
        if soundex(extracted_name) == soundex(db_name):
            return True  # Found in DB (sounds same)
        
        # Level 4: Token set
        if token_similarity(extracted_name, db_name) >= 0.80:
            return True  # Found in DB (token order different)
    
    return False  # NOT in DB (new accused!)
```

### **Applied to Our Example:**

```
DB names: ["Raj Kumar", "Priya Singh", "Amit Sharma", "Vikram Patel"]

Text name: "Raj Kumar"
  → Exact match with "Raj Kumar" in DB
  → return True (skip, already in DB) ✓

Text name: "Priya Singh"
  → Exact match with "Priya Singh" in DB
  → return True (skip, already in DB) ✓

Text name: "Amit Sharma"
  → Exact match with "Amit Sharma" in DB
  → return True (skip, already in DB) ✓

Text name: "Vikram Patel"
  → Exact match with "Vikram Patel" in DB
  → return True (skip, already in DB) ✓

Text name: "Mohit Reddy"
  → NOT in DB names list
  → Level 1: No exact match
  → Level 2: No fuzzy match
  → Level 3: No soundex match
  → Level 4: No token match
  → return False (NOT in DB, ADD!) ❌

Text name: "Anita Verma"
  → NOT in DB names list
  → All levels fail
  → return False (NOT in DB, ADD!) ❌
```

---

## Visual Flow Diagram

```
Crime #12345 arrives for processing

STEP 1: Determine branch
        └─ Does DB have person_id NOT NULL? YES
           └─ BRANCH A

STEP 2: Process DB accused (4 rows)
        ├─ Raj Kumar → Insert into brief_facts_ai
        ├─ Priya Singh → Insert into brief_facts_ai
        ├─ Amit Sharma → Insert into brief_facts_ai
        └─ Vikram Patel → Insert into brief_facts_ai
        
        Records in brief_facts_ai: 4

STEP 3: Gap-Fill - Extract ALL text names
        └─ Extract from brief_facts: 6 names
           ["Raj Kumar", "Priya Singh", "Amit Sharma", 
            "Vikram Patel", "Mohit Reddy", "Anita Verma"]

STEP 4: Match text names against DB names
        ├─ Raj Kumar → In DB ✓ (skip)
        ├─ Priya Singh → In DB ✓ (skip)
        ├─ Amit Sharma → In DB ✓ (skip)
        ├─ Vikram Patel → In DB ✓ (skip)
        ├─ Mohit Reddy → NOT in DB ✗ (ADD)
        └─ Anita Verma → NOT in DB ✗ (ADD)

STEP 5: Create synthetic records for 2 undiscovered
        ├─ Mohit Reddy → Generate UUID, extract details
        │                 → Insert into brief_facts_ai
        └─ Anita Verma → Generate UUID, extract details
                         → Insert into brief_facts_ai
        
        Records in brief_facts_ai: 6 (4 DB + 2 synthetic)
```

---

## Key Points

| Aspect | Details |
|--------|---------|
| **DB Accused Count** | 4 |
| **Text Mentioned Count** | 6 |
| **Undiscovered** | 6 - 4 = 2 (Mohit Reddy, Anita Verma) |
| **Final brief_facts_ai rows** | 6 (all captured!) |
| **How detected** | Fuzzy matching (4 levels) against DB names |
| **Gap-fill marker** | `source_accused_fields.accused_id = "SYNTHETIC_GAP_FILL"` |
| **Difference flag** | `existing_accused = FALSE` (vs TRUE for DB rows) |

---

## Code Reference

```python
# File: /data-drive/etl-process-dev/brief_facts_ai/main.py

Line 1104:  branch = _classify_db_accused(db_accused)  # → BRANCH A

Line 1114:  rows_written, branch_records = _process_branch_a(...)
            └─ Process 4 DB accused → 4 records

Line 1504:  text_names = extract_accused_names_pass1(facts_text)
            └─ Extract 6 names from text

Line 1519:  if _match_extracted_name_to_db_accused(clean, db_name_variants):
            └─ Compare text names against DB (4 match, 2 don't)

Line 1533:  for raw_name in new_names:  # 2 undiscovered
            └─ Create synthetic records for each

Line 1654:  insert_accused_facts(conn, extra_row)
            └─ Insert 2 synthetic records into brief_facts_ai

Result: 6 total rows (4 + 2)
```

---

## Summary

**Q: If DB has 4 accused (Branch A/B), but text mentions 6 people, what happens to the 2 undiscovered?**

**A:** 
1. ✓ Process 4 DB accused normally
2. ✓ Gap-fill extracts ALL 6 names from text
3. ✓ Matches text names against DB (4 match, 2 don't)
4. ✓ Creates synthetic records for 2 undiscovered
5. ✓ Inserts 2 synthetic records into brief_facts_ai
6. **Result:** All 6 appear in brief_facts_ai!
