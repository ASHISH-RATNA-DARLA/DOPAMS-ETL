# Branch Determination Logic - Step by Step

## The Decision Tree

```
For each crime_id in batch:

STEP 1: Fetch accused records from accused table
        WHERE crime_id = <this_crime_id>
        
        ↓
        
STEP 2: Check the result
        
        ┌──────────────────────────────────────────────┐
        │  Is db_accused list EMPTY?                   │
        │  (No rows returned from accused table)        │
        └──────────────────────────────────────────────┘
           │
           ├─ YES  → BRANCH C (text-only extraction)
           │        Process crime using only brief_facts text
           │        No DB accused to use as reference
           │
           └─ NO   → Go to STEP 3
                    ↓
        ┌──────────────────────────────────────────────────────┐
        │  Does ANY accused row have person_id NOT NULL?        │
        │  (At least one row has a valid FK to persons table)   │
        └──────────────────────────────────────────────────────┘
           │
           ├─ YES  → BRANCH A (DB authoritative)
           │        Use DB as source of truth
           │        LLM enriches with roles/drugs/status
           │
           └─ NO   → BRANCH B (LLM enriches DB data)
                    All person_id values are NULL
                    LLM infers identity and person details
```

---

## Code Implementation

### **The Classification Function:**

```python
def _classify_db_accused(db_accused):
    """
    Returns the processing branch for a given crime's accused rows.

    A — DB has accused rows AND at least one person_id IS NOT NULL
    B — DB has accused rows BUT ALL person_id IS NULL  (stub / orphan)
    C — DB has zero accused rows
    """
    if not db_accused:                                    # Empty list?
        return 'C'                                         # → Branch C
    
    if any(row.get('person_id') for row in db_accused):   # Any person_id NOT NULL?
        return 'A'                                         # → Branch A
    
    return 'B'                                             # → Branch B
```

### **Where It's Called:**

```python
def worker(crime):
    crime_id = crime['crime_id']
    ps_code = crime.get('ps_code')
    facts_text = crime['brief_facts']
    
    with pool.get_connection_context() as conn:
        # STEP 1: Fetch accused rows for this crime
        db_accused = fetch_existing_accused_for_crime(conn, crime_id)
        
        # STEP 2: Determine branch
        branch = _classify_db_accused(db_accused)
        
        # STEP 3: Execute appropriate branch
        if branch == 'A':
            rows_written, branch_records = _process_branch_a(
                conn, crime_id, ps_code, facts_text, db_accused, run_id
            )
        elif branch == 'B':
            rows_written, branch_records = _process_branch_b(
                conn, crime_id, ps_code, facts_text, db_accused, run_id
            )
        else:  # branch == 'C'
            rows_written, branch_records = _process_branch_c(
                conn, crime_id, ps_code, facts_text, run_id
            )
```

---

## The Key Decision: `person_id IS NOT NULL`

### **What is `person_id`?**

```sql
accused table:
┌──────────────────────────────────────────────────────┐
│ accused_id  │ crime_id │ person_id │ full_name      │
├──────────────────────────────────────────────────────┤
│ uuid-abc-1  │ 12345   │ uuid-p-1  │ Raj Kumar      │  ← person_id NOT NULL
│ uuid-abc-2  │ 12345   │ NULL      │ Priya Singh    │  ← person_id IS NULL
│ uuid-abc-3  │ 12345   │ uuid-p-3  │ Amit Sharma    │  ← person_id NOT NULL
└──────────────────────────────────────────────────────┘
```

- `person_id` = Foreign Key to `persons` table
- `person_id = NOT NULL` means: **Person record already exists, identity is confirmed**
- `person_id = NULL` means: **Person record does NOT exist, identity is unknown**

---

## Real World Examples

### **Example 1: BRANCH A**

**Scenario:** Crime #12345 (Theft case)

```sql
-- Query result from accused table:
SELECT * FROM accused WHERE crime_id = 12345;

┌──────────────────────────────────────────────────┐
│ accused_id         │ person_id          │ name   │
├──────────────────────────────────────────────────┤
│ uuid-acc-001       │ uuid-person-101    │ Raj    │  ← person_id EXISTS!
│ uuid-acc-002       │ uuid-person-102    │ Priya  │  ← person_id EXISTS!
└──────────────────────────────────────────────────┘
```

**Decision Logic:**

```python
db_accused = [
    {'accused_id': 'uuid-acc-001', 'person_id': 'uuid-person-101', 'full_name': 'Raj'},
    {'accused_id': 'uuid-acc-002', 'person_id': 'uuid-person-102', 'full_name': 'Priya'},
]

# Check 1: Is list empty?
if not db_accused:  # False (list has 2 items)
    return 'C'

# Check 2: Does ANY row have person_id NOT NULL?
if any(row.get('person_id') for row in db_accused):
    # row[0]['person_id'] = 'uuid-person-101' → Truthy ✓
    return 'A'  ← BRANCH A

return 'B'
```

**Result:** `branch = 'A'`

**Why?**
- ✓ Accused rows exist in DB (not empty)
- ✓ At least one person_id is NOT NULL
- → DB has identity confirmation, use DB as source

---

### **Example 2: BRANCH B**

**Scenario:** Crime #12346 (Drug possession arrest)

```sql
-- Query result from accused table:
SELECT * FROM accused WHERE crime_id = 12346;

┌──────────────────────────────────────────────────┐
│ accused_id         │ person_id │ name           │
├──────────────────────────────────────────────────┤
│ uuid-acc-003       │ NULL      │ Unknown Male   │  ← person_id = NULL
│ uuid-acc-004       │ NULL      │ Unknown Female │  ← person_id = NULL
└──────────────────────────────────────────────────┘
```

**Decision Logic:**

```python
db_accused = [
    {'accused_id': 'uuid-acc-003', 'person_id': None, 'full_name': 'Unknown Male'},
    {'accused_id': 'uuid-acc-004', 'person_id': None, 'full_name': 'Unknown Female'},
]

# Check 1: Is list empty?
if not db_accused:  # False (list has 2 items)
    return 'C'

# Check 2: Does ANY row have person_id NOT NULL?
if any(row.get('person_id') for row in db_accused):
    # row[0]['person_id'] = None → Falsy ✗
    # row[1]['person_id'] = None → Falsy ✗
    # any() returns False
    # Skip this return
    pass

# Fall through
return 'B'  ← BRANCH B
```

**Result:** `branch = 'B'`

**Why?**
- ✓ Accused rows exist in DB (not empty)
- ✗ NO person_id is NOT NULL (all are NULL)
- → DB has accused but person identity is unknown
- → LLM must infer identity from text

---

### **Example 3: BRANCH C**

**Scenario:** Crime #12347 (Pending investigation, arrests happening tomorrow)

```sql
-- Query result from accused table:
SELECT * FROM accused WHERE crime_id = 12347;

(empty result set)
```

**Decision Logic:**

```python
db_accused = []  # Empty list

# Check 1: Is list empty?
if not db_accused:  # True (list is empty)
    return 'C'  ← BRANCH C (early return)
```

**Result:** `branch = 'C'`

**Why?**
- ✗ No accused rows in DB at all (crime reported, still under investigation)
- → Must extract from brief_facts text only
- → LLM reads narrative to find who is involved

---

## Visual Decision Flow

```
┌─────────────────────────────────────┐
│    For crime_id = X                  │
│  Query: SELECT * FROM accused       │
│         WHERE crime_id = X          │
└─────────────────────────────────────┘
              ↓
        ┌─────────────┐
        │ Got rows?   │
        └─────────────┘
           /       \
         NO         YES
        ↓            ↓
    BRANCH C    Has person_id?
                (any row with
                 person_id NOT NULL)
                    /      \
                  YES       NO
                  ↓         ↓
              BRANCH A  BRANCH B


┌────────────────────────────────────────┐
│         BRANCH A (50-60%)               │
│  ✓ DB accused exist                    │
│  ✓ person_id confirmed                 │
│  → Use DB as source                    │
│  → LLM enriches roles/drugs/status     │
└────────────────────────────────────────┘

┌────────────────────────────────────────┐
│         BRANCH B (20-30%)               │
│  ✓ DB accused exist                    │
│  ✗ person_id NOT known                 │
│  → LLM infers identity                 │
│  → Assigns person_codes (A-1, A-2)     │
└────────────────────────────────────────┘

┌────────────────────────────────────────┐
│         BRANCH C (10-20%)               │
│  ✗ No DB accused yet                   │
│  → LLM reads text only                 │
│  → Creates synthetic records           │
│  → Invalidated when accused arrive     │
└────────────────────────────────────────┘
```

---

## Timeline: When Branches Change

```
Day 1 - Evening (7 PM)
────────────────────────────────
Crime #12348 reported
- Brief facts: "Unknown persons committed theft..."
- No arrests yet

Query: SELECT * FROM accused WHERE crime_id = 12348
Result: EMPTY ✓

Branch determination:
  if not db_accused:  # True (empty)
    return 'C'

→ BRANCH C processing
  - No accused in DB
  - LLM extracts from text: "Unknown Male 1", "Unknown Female 2"
  - Creates 2 synthetic records in brief_facts_ai
  - Marked as "TEXT_ONLY_EXTRACTION"


Day 2 - Morning (9 AM)
────────────────────────────────
Suspects arrested overnight

Query: INSERT INTO accused (crime_id, person_id, ...)
  VALUES (12348, 'uuid-person-xyz', ...)

Next ETL run:

Query: SELECT * FROM accused WHERE crime_id = 12348
Result: 1 row with person_id = 'uuid-person-xyz' ✓

Branch determination:
  if not db_accused:  # False (has 1 row)
    return 'C'
  
  if any(row.get('person_id') ...):  # True! person_id exists
    return 'A'

→ BRANCH A processing
  - Old text-only records INVALIDATED (deleted)
  - New DB records processed with LLM enrichment
  - Replaces synthetic records with confirmed identity
  - Same brief_facts_ai record, but now DB-sourced
```

---

## What Gets Checked: person_id Column

### **In accused table:**

```sql
CREATE TABLE accused (
    accused_id UUID PRIMARY KEY,
    crime_id BIGINT NOT NULL,
    person_id UUID,                     ← This column!
    accused_code VARCHAR,
    full_name VARCHAR,
    alias_name VARCHAR,
    ...
    FOREIGN KEY (person_id) REFERENCES persons(person_id)
);
```

**SQL to check for Branch A:**

```sql
-- Branch A: At least one person_id NOT NULL
SELECT branch FROM (
    SELECT
        crime_id,
        CASE
            WHEN COUNT(*) = 0 THEN 'C'
            WHEN COUNT(*) FILTER (WHERE person_id IS NOT NULL) > 0 THEN 'A'
            ELSE 'B'
        END AS branch
    FROM accused
    WHERE crime_id = 12345
    GROUP BY crime_id
) AS classification;

-- Result:
-- crime_id | branch
-- 12345    | A      (if ≥1 person_id NOT NULL)
-- 12346    | B      (if 0 person_id NOT NULL, but rows exist)
-- 12347    | C      (if no rows)
```

---

## Summary Table

| Condition | Branch | Reason |
|-----------|--------|--------|
| `db_accused = []` (empty) | C | No arrests yet, use text |
| `db_accused` has ≥1 row with `person_id NOT NULL` | A | DB identity confirmed, use DB |
| `db_accused` has rows but ALL `person_id = NULL` | B | DB person stub exists, infer identity |

---

## Code Path for Each Branch

```
worker(crime)
  ↓
fetch_existing_accused_for_crime(conn, crime_id)
  ↓ (returns db_accused list)
  ↓
_classify_db_accused(db_accused)
  ├─ if not db_accused → 'C'
  ├─ if any(person_id) → 'A'
  └─ else → 'B'
  ↓
if branch == 'A':
    _process_branch_a(conn, crime_id, ps_code, facts_text, db_accused, run_id)
elif branch == 'B':
    _process_branch_b(conn, crime_id, ps_code, facts_text, db_accused, run_id)
else:
    _process_branch_c(conn, crime_id, ps_code, facts_text, run_id)
```

---

**Key Insight:** The entire branch determination boils down to **ONE column check**: `person_id IS NOT NULL`
