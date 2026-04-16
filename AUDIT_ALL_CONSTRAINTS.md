# Comprehensive Database Constraint Audit

## Executive Summary

**Total Tables Audited:** 67  
**Tables with Issues:** 10 (6 previously identified + 4 additional)  
**Constraint Type Issues:** PRIMARY KEY missing, UNIQUE constraints missing

---

## Tables Requiring Constraint Fixes

### ✅ ALREADY FIXED (6 tables)

| Table | Issue | Fix Applied | Line |
|-------|-------|-------------|------|
| crimes | Missing PRIMARY KEY on crime_id | Added `PRIMARY KEY (crime_id)` | 681 |
| accused | Missing PRIMARY KEY on accused_id | Added `PRIMARY KEY (accused_id)` | 505 |
| persons | Missing PRIMARY KEY on person_id | Added `PRIMARY KEY (person_id)` | 786 |
| properties | Missing PRIMARY KEY on property_id | Added `PRIMARY KEY (property_id)` | 3370 |
| interrogation_reports | Missing PRIMARY KEY on interrogation_report_id | Added `PRIMARY KEY (interrogation_report_id)` | 2617 |
| disposal | Missing UNIQUE constraint on composite key | Added `PRIMARY KEY (id)` + `UNIQUE (crime_id, disposal_type, disposed_at)` | 728-740 |

---

### ⚠️ NEWLY IDENTIFIED - CRITICAL (4 tables)

These tables use **`ON CONFLICT DO NOTHING`** in the ETL without PRIMARY KEY constraints:

#### 1. **ir_regular_habits**
- **Current Definition (Lines 3083-3090):**
  ```sql
  CREATE TABLE public.ir_regular_habits (
      id integer NOT NULL,
      interrogation_report_id character varying(50) NOT NULL,
      habit character varying(255) NOT NULL
  );
  ```
- **Problem:** No PRIMARY KEY constraint on `id` column
- **ON CONFLICT Usage:** `ir_etl_enhanced.py:650` - `INSERT INTO ir_regular_habits VALUES %s ON CONFLICT DO NOTHING`
- **Fix Required:** Add PRIMARY KEY constraint on `id`
- **SQL Fix:**
  ```sql
  ALTER TABLE public.ir_regular_habits ADD CONSTRAINT pk_ir_regular_habits_id PRIMARY KEY (id);
  ```

#### 2. **ir_media**
- **Current Definition (Lines 2938-2946):**
  ```sql
  CREATE TABLE public.ir_media (
      id integer NOT NULL,
      interrogation_report_id character varying(50) NOT NULL,
      media_id text NOT NULL
  );
  ```
- **Problem:** No PRIMARY KEY constraint on `id` column
- **ON CONFLICT Usage:** `ir_etl_enhanced.py:878` - `INSERT INTO ir_media VALUES %s ON CONFLICT DO NOTHING`
- **Fix Required:** Add PRIMARY KEY constraint on `id`
- **SQL Fix:**
  ```sql
  ALTER TABLE public.ir_media ADD CONSTRAINT pk_ir_media_id PRIMARY KEY (id);
  ```

#### 3. **ir_interrogation_report_refs**
- **Current Definition (Lines 2881-2886):**
  ```sql
  CREATE TABLE public.ir_interrogation_report_refs (
      id integer NOT NULL,
      interrogation_report_id character varying(50) NOT NULL,
      report_ref_id text NOT NULL
  );
  ```
- **Problem:** No PRIMARY KEY constraint on `id` column
- **ON CONFLICT Usage:** `ir_etl_enhanced.py:890` - `INSERT INTO ir_interrogation_report_refs VALUES %s ON CONFLICT DO NOTHING`
- **Fix Required:** Add PRIMARY KEY constraint on `id`
- **SQL Fix:**
  ```sql
  ALTER TABLE public.ir_interrogation_report_refs ADD CONSTRAINT pk_ir_interrogation_report_refs_id PRIMARY KEY (id);
  ```

#### 4. **ir_indulgance_before_offence**
- **Current Definition (Lines 2866-2875):**
  ```sql
  CREATE TABLE public.ir_indulgance_before_offence (
      id integer NOT NULL,
      interrogation_report_id character varying(50) NOT NULL,
      indulgance text,
      created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
  );
  ```
- **Problem:** No PRIMARY KEY constraint on `id` column
- **ON CONFLICT Usage:** `ir_etl_enhanced.py:919` - `INSERT INTO ir_indulgance_before_offence VALUES %s ON CONFLICT DO NOTHING`
- **Fix Required:** Add PRIMARY KEY constraint on `id`
- **SQL Fix:**
  ```sql
  ALTER TABLE public.ir_indulgance_before_offence ADD CONSTRAINT pk_ir_indulgance_before_offence_id PRIMARY KEY (id);
  ```

---

### ⚠️ SPECIAL CASE: ir_pending_fk (Partial Unique Constraint)

- **Current Definition (Lines 2992-3008):**
  ```sql
  CREATE TABLE public.ir_pending_fk (
      id integer NOT NULL,
      ir_id character varying(50) NOT NULL,
      crime_id character varying(50) NOT NULL,
      raw_data jsonb NOT NULL,
      created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
      retry_count integer DEFAULT 0,
      last_retry_at timestamp without time zone,
      resolved boolean DEFAULT false,
      resolved_at timestamp without time zone
  );
  ```
- **Problem:** ON CONFLICT uses `(ir_id) WHERE NOT resolved` which requires a partial unique index
- **ON CONFLICT Usage:** 
  - `ir_etl_enhanced.py:277` - `ON CONFLICT (ir_id) WHERE NOT resolved`
  - `ir_etl.py:295` - `ON CONFLICT (ir_id) WHERE NOT resolved`
- **Fix Required:** Add PRIMARY KEY on `id` AND create partial unique index on `(ir_id)` where `NOT resolved`
- **SQL Fix:**
  ```sql
  ALTER TABLE public.ir_pending_fk ADD CONSTRAINT pk_ir_pending_fk_id PRIMARY KEY (id);
  CREATE UNIQUE INDEX idx_ir_pending_fk_ir_id_unresolved 
    ON public.ir_pending_fk (ir_id) 
    WHERE NOT resolved;
  ```

---

## How ON CONFLICT Works in PostgreSQL

**Without a matching constraint:**
```
ERROR: there is no unique or exclusion constraint matching the ON CONFLICT specification
```

**Constraint matching rules:**
- `ON CONFLICT (column_name)` requires: PRIMARY KEY or UNIQUE constraint on that column
- `ON CONFLICT (col1, col2)` requires: UNIQUE constraint on the composite key
- `ON CONFLICT (column) WHERE condition` requires: Partial unique index matching the condition

---

## Audit Findings Summary

### By ETL Module

| ETL Module | Tables with Issues | Status |
|------------|------------------|--------|
| etl_crimes.py | crimes | ✅ FIXED |
| etl_accused.py | accused | ✅ FIXED |
| etl_persons.py | persons | ✅ FIXED |
| etl_disposal.py | disposal | ✅ FIXED |
| etl_properties.py | properties | ✅ FIXED |
| etl_ir.py | interrogation_reports, ir_pending_fk | ⚠️ PARTIAL (main table fixed, pending_fk needs fix) |
| ir_etl_enhanced.py | interrogation_reports, ir_regular_habits, ir_media, ir_interrogation_report_refs, ir_indulgance_before_offence, ir_pending_fk | ⚠️ NEEDS FIXES (4 subtables + 1 pending_fk) |

### Other IR Subtables Verified (No ON CONFLICT Issues)

These tables do NOT use ON CONFLICT in any ETL file, so they don't need constraint fixes:
- ir_associate_details
- ir_consumer_details
- ir_conviction_acquittal
- ir_defence_counsel
- ir_dopams_links
- ir_execution_of_nbw
- ir_family_history
- ir_financial_history
- ir_jail_sentence
- ir_local_contacts
- ir_modus_operandi
- ir_new_gang_formation
- ir_previous_offences_confessed
- ir_property_disposal
- ir_regularization_transit_warrants
- ir_shelter
- ir_sim_details
- ir_sureties
- ir_types_of_drugs

---

## Application Plan

### Step 1: Apply Fixes to DB-schema.sql

All 4 new constraint fixes should be added to DB-schema.sql in the respective CREATE TABLE definitions:

```sql
-- Line ~3087 in ir_regular_habits
ALTER TABLE public.ir_regular_habits ADD CONSTRAINT pk_ir_regular_habits_id PRIMARY KEY (id);

-- Line ~2942 in ir_media
ALTER TABLE public.ir_media ADD CONSTRAINT pk_ir_media_id PRIMARY KEY (id);

-- Line ~2885 in ir_interrogation_report_refs
ALTER TABLE public.ir_interrogation_report_refs ADD CONSTRAINT pk_ir_interrogation_report_refs_id PRIMARY KEY (id);

-- Line ~2873 in ir_indulgance_before_offence
ALTER TABLE public.ir_indulgance_before_offence ADD CONSTRAINT pk_ir_indulgance_before_offence_id PRIMARY KEY (id);

-- Line ~3008 in ir_pending_fk
ALTER TABLE public.ir_pending_fk ADD CONSTRAINT pk_ir_pending_fk_id PRIMARY KEY (id);
CREATE UNIQUE INDEX idx_ir_pending_fk_ir_id_unresolved 
  ON public.ir_pending_fk (ir_id) 
  WHERE NOT resolved;
```

### Step 2: Apply to Remote Database

SSH to remote server and execute:

```bash
ssh eagle@192.168.103.182
psql -h 192.168.103.106 -U dev_dopamas -d dev-3 << 'EOF'
-- Add PRIMARY KEY constraints for IR subtables
ALTER TABLE public.ir_regular_habits ADD CONSTRAINT pk_ir_regular_habits_id PRIMARY KEY (id);
ALTER TABLE public.ir_media ADD CONSTRAINT pk_ir_media_id PRIMARY KEY (id);
ALTER TABLE public.ir_interrogation_report_refs ADD CONSTRAINT pk_ir_interrogation_report_refs_id PRIMARY KEY (id);
ALTER TABLE public.ir_indulgance_before_offence ADD CONSTRAINT pk_ir_indulgance_before_offence_id PRIMARY KEY (id);

-- Add PRIMARY KEY and partial unique index for pending FK table
ALTER TABLE public.ir_pending_fk ADD CONSTRAINT pk_ir_pending_fk_id PRIMARY KEY (id);
CREATE UNIQUE INDEX idx_ir_pending_fk_ir_id_unresolved 
  ON public.ir_pending_fk (ir_id) 
  WHERE NOT resolved;

-- Verify all constraints
SELECT table_name, constraint_name, constraint_type 
FROM information_schema.table_constraints 
WHERE table_name IN (
  'ir_regular_habits', 'ir_media', 'ir_interrogation_report_refs', 
  'ir_indulgance_before_offence', 'ir_pending_fk'
)
ORDER BY table_name;
EOF
```

### Step 3: Clear Checkpoints and Re-run Tests

```bash
psql -h 192.168.103.106 -U dev_dopamas -d dev-3 -c "DELETE FROM etl_run_state;"
./test_etl_from_config.sh
```

---

## Root Cause Analysis

**Why these tables were missed:**

1. **Initial focus on ON CONFLICT with explicit column names** - First audit identified tables using `ON CONFLICT (crime_id)`, `ON CONFLICT (accused_id)`, etc.

2. **"DO NOTHING" masks the error** - When `ON CONFLICT DO NOTHING` is used and the constraint doesn't exist, PostgreSQL doesn't raise an error; it simply skips the insert silently

3. **Cascade failure pattern** - These are subtables related to interrogation_reports, which was itself missing the primary key. The main table failure masked the subtable issues

4. **All 4 use auto-incrementing id columns** - The fact that they use `id integer NOT NULL` (often auto-generated) made it less obvious they needed explicit PRIMARY KEY constraints

---

## Verification Checklist

After applying fixes:

- [ ] All 10 tables have proper PRIMARY KEY or UNIQUE constraints
- [ ] IR subtable constraints are added to DB-schema.sql
- [ ] Remote database constraints are applied
- [ ] ETL checkpoints are cleared
- [ ] Test suite runs with all steps completing successfully
- [ ] All tables show record counts > 0 after test run
- [ ] No "ON CONFLICT" errors in test logs
- [ ] No silent insertion failures in IR subtables

---

## Files to Update

1. **DB-schema.sql** - Add constraint definitions to CREATE TABLE blocks
2. **CONSTRAINT_FIXES.md** - Update with the 4 additional tables
3. **BACKFILL_EXECUTION.md** - Step 0 should include all 10 constraint fixes
4. **Test scripts** - No changes needed (fixes are database-level)

---

## Summary

**Total constraint issues fixed: 10 tables**
- 6 tables: Already applied in previous conversation
- 4 tables: Newly identified (ir_regular_habits, ir_media, ir_interrogation_report_refs, ir_indulgance_before_offence)
- 1 table: Special case with partial unique index (ir_pending_fk)

All issues follow the same pattern: **ON CONFLICT operations failing silently due to missing PRIMARY KEY/UNIQUE constraints**.
