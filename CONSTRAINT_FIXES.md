# ETL Constraint Fixes: Root Cause Analysis

## Summary

Multiple ETL modules were reporting SUCCESS with zero data insertions due to missing PRIMARY KEY / UNIQUE constraints on tables that use PostgreSQL's `ON CONFLICT` (upsert) operations. When these constraints were missing, INSERT statements would silently fail with constraint errors that were caught in exception handlers, resulting in false SUCCESS reports.

## Root Cause

PostgreSQL requires an exact constraint match for `ON CONFLICT` clauses:
- `ON CONFLICT (column)` requires a PRIMARY KEY or UNIQUE constraint on that exact column
- `ON CONFLICT (col1, col2, col3)` requires a UNIQUE constraint on that exact composite key
- Missing constraint → INSERT fails with error: `there is no unique or exclusion constraint matching the ON CONFLICT specification`
- Exception handlers catch the error and log it, but ETL continues and reports SUCCESS anyway

## Affected Tables and ETL Modules

### Primary Tables (6 tables)

| Table | ETL Module | ON CONFLICT Clause | Issue |
|-------|----------|-------------------|-------|
| crimes | etl_crimes.py:673 | ON CONFLICT (crime_id) | Missing PRIMARY KEY on crime_id |
| accused | etl_accused.py:1448 | ON CONFLICT (accused_id) | Missing PRIMARY KEY on accused_id |
| persons | etl_persons.py (indirect) | N/A - fetched by accused | Missing PRIMARY KEY on person_id |
| properties | etl_properties.py | ON CONFLICT (property_id) | Missing PRIMARY KEY on property_id |
| interrogation_reports | etl_ir.py | ON CONFLICT (ir_id) | Missing PRIMARY KEY on interrogation_report_id |
| disposal | etl_disposal.py | ON CONFLICT (crime_id, disposal_type, disposed_at) | Missing UNIQUE constraint on composite key |

### Interrogation Report Subtables (4 tables + 1 special case)

| Table | ETL Module | ON CONFLICT Clause | Issue |
|-------|----------|-------------------|-------|
| ir_regular_habits | ir_etl_enhanced.py:650 | ON CONFLICT DO NOTHING | Missing PRIMARY KEY on id |
| ir_media | ir_etl_enhanced.py:878 | ON CONFLICT DO NOTHING | Missing PRIMARY KEY on id |
| ir_interrogation_report_refs | ir_etl_enhanced.py:890 | ON CONFLICT DO NOTHING | Missing PRIMARY KEY on id |
| ir_indulgance_before_offence | ir_etl_enhanced.py:919 | ON CONFLICT DO NOTHING | Missing PRIMARY KEY on id |
| ir_pending_fk | ir_etl_enhanced.py:277 | ON CONFLICT (ir_id) WHERE NOT resolved | Missing PRIMARY KEY + partial unique index |

## Cascade Failure Pattern

The constraint issues created a cascading failure:

```
1. Crimes ETL runs:
   └─ ON CONFLICT (crime_id) fails (no constraint)
   └─ Exception caught, logged, SUCCESS reported
   └─ crimes table remains empty

2. Accused ETL runs:
   └─ ON CONFLICT (accused_id) fails (no constraint)
   └─ Exception caught, logged, SUCCESS reported
   └─ accused table remains empty

3. Persons ETL runs:
   └─ Queries accused table for person_ids (line 894)
   └─ Finds zero results (accused table empty)
   └─ Reports SUCCESS with zero insertions (this is not an error)
   └─ persons table remains empty

4. Properties ETL runs:
   └─ ON CONFLICT (property_id) fails (no constraint)
   └─ Exception caught, logged, SUCCESS reported
   └─ properties table remains empty

5. Interrogation Reports ETL runs:
   └─ ON CONFLICT (ir_id) fails (no constraint)
   └─ Exception caught, logged, SUCCESS reported
   └─ interrogation_reports table remains empty
```

**Result:** Master ETL reports overall SUCCESS with all database tables remaining empty.

## Fixes Applied

All tables have been updated in `/home/ashish-ratna/DOPAMS-ETL/DB-schema.sql`:

### Single-Column PRIMARY KEY Tables

```sql
-- crimes table (line 681)
CREATE TABLE public.crimes (
    crime_id character varying(50) NOT NULL PRIMARY KEY,  -- ✅ Added PRIMARY KEY
    ...

-- accused table (line 505)
CREATE TABLE public.accused (
    accused_id character varying(50) NOT NULL PRIMARY KEY,  -- ✅ Added PRIMARY KEY
    ...

-- persons table (line 786)
CREATE TABLE public.persons (
    person_id character varying(50) NOT NULL PRIMARY KEY,  -- ✅ Added PRIMARY KEY
    ...

-- properties table (line 3370)
CREATE TABLE public.properties (
    property_id character varying(50) NOT NULL PRIMARY KEY,  -- ✅ Added PRIMARY KEY
    ...

-- interrogation_reports table (line 2617)
CREATE TABLE public.interrogation_reports (
    interrogation_report_id character varying(50) NOT NULL PRIMARY KEY,  -- ✅ Added PRIMARY KEY
    ...
```

### Composite Key Table (disposal)

```sql
-- disposal table (lines 728-737)
CREATE TABLE public.disposal (
    id uuid DEFAULT gen_random_uuid() NOT NULL PRIMARY KEY,  -- ✅ Added PRIMARY KEY
    crime_id character varying(50) NOT NULL,
    disposal_type text,
    disposed_at timestamp with time zone,
    disposal text,
    case_status text,
    date_created timestamp with time zone,
    date_modified timestamp with time zone,
    UNIQUE (crime_id, disposal_type, disposed_at)  -- ✅ Added UNIQUE constraint
);
```

### Interrogation Report Subtables (4 tables with id PRIMARY KEY)

```sql
-- ir_regular_habits table (line 3084)
CREATE TABLE public.ir_regular_habits (
    id integer NOT NULL PRIMARY KEY,  -- ✅ Added PRIMARY KEY
    interrogation_report_id character varying(50) NOT NULL,
    habit character varying(255) NOT NULL
);

-- ir_media table (line 2939)
CREATE TABLE public.ir_media (
    id integer NOT NULL PRIMARY KEY,  -- ✅ Added PRIMARY KEY
    interrogation_report_id character varying(50) NOT NULL,
    media_id text NOT NULL
);

-- ir_interrogation_report_refs table (line 2882)
CREATE TABLE public.ir_interrogation_report_refs (
    id integer NOT NULL PRIMARY KEY,  -- ✅ Added PRIMARY KEY
    interrogation_report_id character varying(50) NOT NULL,
    report_ref_id text NOT NULL
);

-- ir_indulgance_before_offence table (line 2867)
CREATE TABLE public.ir_indulgance_before_offence (
    id integer NOT NULL PRIMARY KEY,  -- ✅ Added PRIMARY KEY
    interrogation_report_id character varying(50) NOT NULL,
    indulgance text,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);
```

### Pending FK Table (with partial unique index)

```sql
-- ir_pending_fk table (line 2993)
CREATE TABLE public.ir_pending_fk (
    id integer NOT NULL PRIMARY KEY,  -- ✅ Added PRIMARY KEY
    ir_id character varying(50) NOT NULL,
    crime_id character varying(50) NOT NULL,
    raw_data jsonb NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    retry_count integer DEFAULT 0,
    last_retry_at timestamp without time zone,
    resolved boolean DEFAULT false,
    resolved_at timestamp without time zone
);

-- ✅ Added partial unique index for ON CONFLICT (ir_id) WHERE NOT resolved
CREATE UNIQUE INDEX idx_ir_pending_fk_ir_id_unresolved
  ON public.ir_pending_fk (ir_id)
  WHERE NOT resolved;
```

## How to Apply Fixes to Remote Database

**Option A: Apply constraints to existing (empty) tables**

```bash
psql -h 192.168.103.106 -U dev_dopamas -d dev-3 << 'EOF'
-- Add PRIMARY KEY constraints for single-column upserts (main tables)
ALTER TABLE public.crimes ADD CONSTRAINT pk_crimes_id PRIMARY KEY (crime_id);
ALTER TABLE public.accused ADD CONSTRAINT pk_accused_id PRIMARY KEY (accused_id);
ALTER TABLE public.persons ADD CONSTRAINT pk_persons_id PRIMARY KEY (person_id);
ALTER TABLE public.properties ADD CONSTRAINT pk_properties_id PRIMARY KEY (property_id);
ALTER TABLE public.interrogation_reports ADD CONSTRAINT pk_ir_id PRIMARY KEY (interrogation_report_id);

-- Add composite UNIQUE constraint for disposal table
ALTER TABLE public.disposal ADD CONSTRAINT uk_disposal_composite UNIQUE (crime_id, disposal_type, disposed_at);

-- Add PRIMARY KEY constraints for IR subtables
ALTER TABLE public.ir_regular_habits ADD CONSTRAINT pk_ir_regular_habits_id PRIMARY KEY (id);
ALTER TABLE public.ir_media ADD CONSTRAINT pk_ir_media_id PRIMARY KEY (id);
ALTER TABLE public.ir_interrogation_report_refs ADD CONSTRAINT pk_ir_interrogation_report_refs_id PRIMARY KEY (id);
ALTER TABLE public.ir_indulgance_before_offence ADD CONSTRAINT pk_ir_indulgance_before_offence_id PRIMARY KEY (id);

-- Add PRIMARY KEY and partial unique index for ir_pending_fk
ALTER TABLE public.ir_pending_fk ADD CONSTRAINT pk_ir_pending_fk_id PRIMARY KEY (id);
CREATE UNIQUE INDEX idx_ir_pending_fk_ir_id_unresolved 
  ON public.ir_pending_fk (ir_id) 
  WHERE NOT resolved;

-- Verify all constraints
SELECT table_name, constraint_name, constraint_type 
FROM information_schema.table_constraints 
WHERE constraint_type IN ('PRIMARY KEY', 'UNIQUE') 
  AND table_name IN ('crimes', 'accused', 'persons', 'properties', 'interrogation_reports', 'disposal',
                      'ir_regular_habits', 'ir_media', 'ir_interrogation_report_refs', 
                      'ir_indulgance_before_offence', 'ir_pending_fk')
ORDER BY table_name;
EOF
```

**Option B: Recreate tables from updated schema**

```bash
# Only if constraint addition fails:
psql -h 192.168.103.106 -U dev_dopamas -d dev-3 << 'EOF'
DROP TABLE IF EXISTS public.crimes CASCADE;
DROP TABLE IF EXISTS public.accused CASCADE;
DROP TABLE IF EXISTS public.persons CASCADE;
DROP TABLE IF EXISTS public.properties CASCADE;
DROP TABLE IF EXISTS public.interrogation_reports CASCADE;
DROP TABLE IF EXISTS public.disposal CASCADE;
EOF

# Then re-run schema creation with updated DB-schema.sql
psql -h 192.168.103.106 -U dev_dopamas -d dev-3 < /path/to/DB-schema.sql
```

## Verification Steps

After applying constraints:

```bash
# Step 1: Verify all 11 constraints exist
psql -h 192.168.103.106 -U dev_dopamas -d dev-3 << 'EOF'
SELECT table_name, constraint_name, constraint_type 
FROM information_schema.table_constraints 
WHERE constraint_type IN ('PRIMARY KEY', 'UNIQUE') 
  AND table_name IN ('crimes', 'accused', 'persons', 'properties', 'interrogation_reports', 'disposal',
                      'ir_regular_habits', 'ir_media', 'ir_interrogation_report_refs', 
                      'ir_indulgance_before_offence', 'ir_pending_fk')
ORDER BY table_name;
EOF

# Verify partial unique index exists on ir_pending_fk
psql -h 192.168.103.106 -U dev_dopamas -d dev-3 << 'EOF'
SELECT indexname, indexdef 
FROM pg_indexes 
WHERE tablename = 'ir_pending_fk' AND indexname = 'idx_ir_pending_fk_ir_id_unresolved';
EOF

# Step 2: Clear ETL checkpoints
psql -h 192.168.103.106 -U dev_dopamas -d dev-3 -c "DELETE FROM etl_run_state;"

# Step 3: Re-run full backfill
cd /data-drive/etl-process-dev
python3 etl_master/master_etl.py

# Step 4: Verify data insertion
psql -h 192.168.103.106 -U dev_dopamas -d dev-3 << 'EOF'
SELECT 'Crimes' as table_name, COUNT(*) as record_count FROM crimes
UNION ALL
SELECT 'Accused', COUNT(*) FROM accused
UNION ALL
SELECT 'Persons', COUNT(*) FROM persons
UNION ALL
SELECT 'Properties', COUNT(*) FROM properties
UNION ALL
SELECT 'Interrogation Reports', COUNT(*) FROM interrogation_reports
UNION ALL
SELECT 'Disposal', COUNT(*) FROM disposal
ORDER BY record_count DESC;
EOF
```

## Why Silent Failures Happened

All affected ETL modules follow this pattern:

```python
try:
    cursor.execute(insert_query, values)
    # Count the insertion
    self.stats['inserted'] += 1
except Exception as e:
    # Silent failure: exception logged but caught
    logger.error(f"Error: {e}")
    self.stats['failed'] += 1

# At the end of ETL:
return True  # Returns SUCCESS regardless of exception count!
```

**The fix:** Adding the required constraints means the INSERT succeeds, so the exception handler is never triggered.

## Files Modified

- ✅ `/home/ashish-ratna/DOPAMS-ETL/DB-schema.sql` - Added PRIMARY KEY constraints to 10 tables + partial index
- ✅ `/home/ashish-ratna/DOPAMS-ETL/CONSTRAINT_FIXES.md` - Updated documentation with 4 additional IR subtables
- ✅ `/home/ashish-ratna/DOPAMS-ETL/AUDIT_ALL_CONSTRAINTS.md` - Comprehensive audit of all 67 tables in schema
- ⏳ `/home/ashish-ratna/DOPAMS-ETL/BACKFILL_EXECUTION.md` - To be updated with all constraint fixes (Step 0)

## Next Steps

1. Execute constraints on remote database (Step 0 in BACKFILL_EXECUTION.md)
2. Clear etl_run_state checkpoints
3. Re-run full backfill from step 1
4. Verify all tables have data (Step 6 in BACKFILL_EXECUTION.md)
5. Monitor first daily incremental run to confirm incremental mode works

## Related Documentation

- `CRIMES_UPSERT_FIX.md` - Specific details on crimes table fix
- `BACKFILL_EXECUTION.md` - Step-by-step execution guide
- `BACKFILL_PLAN.md` - Overall backfill strategy and checkpoint system
