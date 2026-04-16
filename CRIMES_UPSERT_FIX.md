# Crimes Table UPSERT Fix

## Issue Found
The crimes ETL was failing with: `there is no unique or exclusion constraint matching the ON CONFLICT specification`

**Root Cause:** The `crimes` table was missing a PRIMARY KEY constraint on `crime_id`, which is required for PostgreSQL's `ON CONFLICT` upsert logic.

## Impact
- ❌ All 14,804 crime records failed to insert in first backfill run
- ❌ Future incremental runs would also fail
- ✅ Fix: Add PRIMARY KEY constraint on `crime_id`

## Schema Issue

**Current (Broken):**
```sql
CREATE TABLE public.crimes (
    crime_id character varying(50) NOT NULL,  -- No PRIMARY KEY!
    ps_code character varying(20) NOT NULL,
    ...
);
```

**Fixed:**
```sql
CREATE TABLE public.crimes (
    crime_id character varying(50) NOT NULL PRIMARY KEY,  -- ✅ Added PRIMARY KEY
    ps_code character varying(20) NOT NULL,
    ...
);
```

## How to Apply the Fix

### Option A: Modify existing table (RECOMMENDED - for already truncated DB)

Since the database was already truncated and backfill not yet complete:

```bash
psql -h 192.168.103.106 -U dev_dopamas -d dev-3 << 'EOF'
-- Add PRIMARY KEY constraint to crimes table
ALTER TABLE public.crimes ADD CONSTRAINT pk_crimes_id PRIMARY KEY (crime_id);

-- Verify the constraint was added
SELECT constraint_name, constraint_type 
FROM information_schema.table_constraints 
WHERE table_name = 'crimes' AND constraint_type = 'PRIMARY KEY';
EOF
```

Expected output:
```
 constraint_name | constraint_type 
-----------------+-----------------
 pk_crimes_id    | PRIMARY KEY
(1 row)
```

### Option B: Recreate table from updated schema

If the constraint addition fails:

```bash
# 1. Drop the existing crimes table
psql -h 192.168.103.106 -U dev_dopamas -d dev-3 -c "DROP TABLE IF EXISTS public.crimes CASCADE;"

# 2. Recreate from updated schema file
psql -h 192.168.103.106 -U dev_dopamas -d dev-3 < /path/to/DB-schema.sql
```

## How etl_crimes.py Uses This

The upsert query (line 673-696 in etl_crimes.py):

```python
upsert_query = f"""
    INSERT INTO {CRIMES_TABLE} (
        crime_id, ps_code, fir_num, ...
    ) VALUES (...)
    ON CONFLICT (crime_id) DO UPDATE SET
        ps_code = EXCLUDED.ps_code,
        fir_num = EXCLUDED.fir_num,
        ...
"""
```

**Now works because:**
- ✅ `crime_id` has a PRIMARY KEY constraint
- ✅ `ON CONFLICT (crime_id)` matches the constraint
- ✅ Insert succeeds for new records
- ✅ Update succeeds for existing records

## What to Do Next

1. **Apply the fix:**
   ```bash
   ssh eagle@192.168.103.182
   psql -h 192.168.103.106 -U dev_dopamas -d dev-3 << 'EOF'
   ALTER TABLE public.crimes ADD CONSTRAINT pk_crimes_id PRIMARY KEY (crime_id);
   EOF
   ```

2. **Clear checkpoints:**
   ```bash
   psql -h 192.168.103.106 -U dev_dopamas -d dev-3 -c "DELETE FROM etl_run_state;"
   ```

3. **Re-run backfill from crime step:**
   ```bash
   cd /data-drive/etl-process-dev
   python3 etl_master/master_etl.py --start-order 4
   ```

4. **Verify data insertion:**
   ```bash
   psql -h 192.168.103.106 -U dev_dopamas -d dev-3 << 'EOF'
   SELECT COUNT(*) as crimes_count FROM crimes;
   SELECT COUNT(*) as inserted_count FROM crimes WHERE date_created >= NOW() - INTERVAL '1 day';
   EOF
   ```

## Files Updated
- ✅ `/home/ashish-ratna/DOPAMS-ETL/DB-schema.sql` - Added PRIMARY KEY to crimes table definition
- ✅ `/home/ashish-ratna/DOPAMS-ETL/BACKFILL_EXECUTION.md` - Added fix step before backfill

## Notes
- This is a **schema fix**, not an ETL code change
- The fix enables proper UPSERT behavior for both:
  - Initial backfill (INSERT only)
  - Daily incremental runs (INSERT + UPDATE)
- The fix is idempotent (can be run multiple times without error if constraint already exists)
