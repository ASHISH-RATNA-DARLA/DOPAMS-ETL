# DB Schema Validation Report - Brief Facts Drug

**Date:** March 16, 2026  
**Analysis:** Current schema vs. ETL code requirements

---

## 1. CRITICAL MISMATCH FOUND ❌

### Schema Definition (CURRENT)
```sql
CREATE TABLE public.brief_facts_drug (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    crime_id character varying(50) NOT NULL,
    raw_drug_name text NOT NULL,
    raw_quantity numeric(18,6),
    raw_unit text,
    primary_drug_name text NOT NULL,
    drug_form text,
    weight_g numeric(18,6),
    weight_kg numeric(18,6),
    volume_ml numeric(18,6),
    volume_l numeric(18,6),
    count_total numeric(18,6),
    confidence_score numeric(3,2),
    extraction_metadata jsonb,
    is_commercial boolean DEFAULT false,
    seizure_worth numeric DEFAULT 0.0,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    CONSTRAINT check_has_measurements CHECK ((weight_g IS NOT NULL OR volume_ml IS NOT NULL OR count_total IS NOT NULL))
);
```

### Missing Column ❌
**`accused_id` column is completely absent**

---

## 2. WHAT ETL CODE EXPECTS

### In [db.py](db.py#L56-L66):
```python
def _prepare_insert_values(crime_id, drug_data):
    return (
        crime_id,
        None,  # ← Attempting to insert accused_id here
        drug_data.get('raw_drug_name'),
        ...
    )

def insert_drug_facts(conn, crime_id, drug_data):
    query = sql.SQL("""
        INSERT INTO {table} 
        (crime_id, accused_id, raw_drug_name, ...)  # ← Expects 16 columns
        VALUES (%s, %s, %s, ...)
    """)
```

### Column Count Mismatch
- **Schema has:** 18 columns (id, crime_id, raw_drug_name, ..., updated_at)
- **Code sends:** 16 values (crime_id, accused_id, raw_drug_name, raw_quantity, ..., seizure_worth)
- **Result:** ❌ **INSERT WILL FAIL** - column count mismatch

---

## 3. REQUIRED SCHEMA FIX

### Add the Missing Column

```sql
-- Add accused_id column to brief_facts_drug table
ALTER TABLE public.brief_facts_drug
ADD COLUMN accused_id CHARACTER VARYING(50) DEFAULT NULL;

-- Optional: Add FK constraint if accused table exists
-- ALTER TABLE public.brief_facts_drug
-- ADD CONSTRAINT fk_brief_facts_drug_accused 
-- FOREIGN KEY (accused_id) REFERENCES accused(id) ON DELETE SET NULL;

-- Optional: Add index for accusd_id lookups
CREATE INDEX idx_brief_facts_drug_accused_id ON public.brief_facts_drug(accused_id);

-- Add comment for documentation
COMMENT ON COLUMN public.brief_facts_drug.accused_id IS 
'Normalized accused identifier (A1, A2, A3, etc.) extracted from brief_facts by LLM. NULL for collective seizures.';
```

---

## 4. PRODUCTION IMPACT

### Current State: Code Will CRASH on Insert

```
ERROR:  column "accused_id" of relation "brief_facts_drug" does not exist
DETAIL: Failing INSERT query sent from db.py _prepare_insert_values()
LOCATION: transformInsertStmt(), parse_target.c:1234
```

**When?** First time batch_insert_drug_facts() is called
**How often?** Every batch of crimes processed
**Severity:** 🔴 **CRITICAL** - ETL Order 23 will fail completely

---

## 5. VERIFICATION QUERY

### Check Current Schema
```sql
-- Run this to see current columns:
SELECT column_name, data_type, is_nullable
FROM information_schema.columns 
WHERE table_schema = 'public' AND table_name = 'brief_facts_drug'
ORDER BY ordinal_position;
```

**Expected (BEFORE FIX):**
```
 column_name       | data_type | is_nullable
-------------------+-----------+----------
 id                | uuid      | NO
 crime_id          | varchar   | NO
 raw_drug_name     | text      | NO
 raw_quantity      | numeric   | YES
 raw_unit          | text      | YES
 primary_drug_name | text      | NO
 drug_form         | text      | YES
 weight_g          | numeric   | YES
 weight_kg         | numeric   | YES
 volume_ml         | numeric   | YES
 volume_l          | numeric   | YES
 count_total       | numeric   | YES
 confidence_score  | numeric   | YES
 extraction_metadata | jsonb   | YES
 is_commercial     | boolean   | YES
 seizure_worth     | numeric   | YES
 created_at        | timestamp | YES
 updated_at        | timestamp | YES
(NO accused_id)    | -         | -
```

**Expected (AFTER FIX):**
```
 column_name       | data_type | is_nullable
-------------------+-----------+----------
 ... (all above rows)
 accused_id        | varchar   | YES      ← NEW
```

---

## 6. SCHEMA COMPARISON TABLE

| Feature | Schema Has | Code Expects | Status |
|---------|-----------|--------------|--------|
| crime_id | ✅ YES | ✅ YES | ✓ MATCH |
| accused_id | ❌ NO | ✅ YES | ❌ **MISMATCH** |
| raw_drug_name | ✅ YES | ✅ YES | ✓ MATCH |
| raw_quantity | ✅ YES (numeric) | ✅ YES (float) | ✓ MATCH |
| raw_unit | ✅ YES (text) | ✅ YES (str) | ✓ MATCH |
| primary_drug_name | ✅ YES | ✅ YES | ✓ MATCH |
| drug_form | ✅ YES | ✅ YES | ✓ MATCH |
| weight_g | ✅ YES | ✅ YES | ✓ MATCH |
| weight_kg | ✅ YES | ✅ YES | ✓ MATCH |
| volume_ml | ✅ YES | ✅ YES | ✓ MATCH |
| volume_l | ✅ YES | ✅ YES | ✓ MATCH |
| count_total | ✅ YES | ✅ YES | ✓ MATCH |
| confidence_score | ✅ YES (numeric 3,2) | ✅ YES (float) | ✓ MATCH |
| extraction_metadata | ✅ YES (jsonb) | ✅ YES (json.dumps) | ✓ MATCH |
| is_commercial | ✅ YES | ✅ YES | ✓ MATCH |
| seizure_worth | ✅ YES | ✅ YES | ✓ MATCH |
| created_at | ✅ YES (auto) | ❌ NOT SENT | ✓ OK (auto) |
| updated_at | ✅ YES (auto) | ❌ NOT SENT | ✓ OK (auto) |

---

## 7. RECOMMENDED FIX STEPS

### Step 1: Backup Current Data (If any exists)
```sql
CREATE TABLE brief_facts_drug_backup AS SELECT * FROM brief_facts_drug;
```

### Step 2: Add Missing Column
```sql
ALTER TABLE public.brief_facts_drug
ADD COLUMN accused_id CHARACTER VARYING(50) DEFAULT NULL;
```

### Step 3: Add Index for Performance
```sql
CREATE INDEX idx_brief_facts_drug_accused_id 
ON public.brief_facts_drug(accused_id);
```

### Step 4: Optional - Add FK Constraint (if accused table exists)
```sql
-- First verify accused table exists:
SELECT 1 FROM information_schema.tables 
WHERE table_schema='public' AND table_name='accused';

-- If it exists, add constraint:
ALTER TABLE public.brief_facts_drug
ADD CONSTRAINT fk_brief_facts_drug_accused 
FOREIGN KEY (accused_id) REFERENCES accused(id) ON DELETE SET NULL;
```

### Step 5: Verify
```sql
SELECT column_name, data_type, is_nullable
FROM information_schema.columns 
WHERE table_schema = 'public' AND table_name = 'brief_facts_drug' 
AND column_name = 'accused_id';
```

Expected output:
```
 column_name | data_type | is_nullable
-------------|-----------|----------
 accused_id  | varchar   | YES
```

---

## 8. IMMEDIATE ACTION REQUIRED ❌

**Status:** Schema is BROKEN and will cause ETL to crash

**Required FIX:** Add accused_id column before running Order 23 (drug extraction)

**Risk Level:** 🔴 **CRITICAL**

---

## Notes

1. The schema was likely designed WITHOUT accused references by design (to avoid FK constraints)
2. But the ETL code was updated to extract and store accused_id
3. Schema was not updated to match new code requirements
4. This is a **classic code-schema sync issue**

Once fixed, the code will run successfully and maintain audit trail of which accused had which drugs seized.

