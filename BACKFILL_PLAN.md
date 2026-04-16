# ETL Backfill & Incremental Strategy (June 2022 to Present)

## Current State
- Database truncated (except Knowledge Base tables)
- config.py has start_date='2022-01-01', end_date='2025-12-31'
- ETL checkpoint system exists in: accused, persons, disposal, properties
- Master ETL has 28 sequential steps

## Backfill Strategy (One-time)

### Step 1: Update Configuration
Modify `/home/ashish-ratna/DOPAMS-ETL/etl-crimes/config.py`:
- start_date: `'2022-06-01T00:00:00+05:30'` (from June 2022 as requested)
- end_date: `'2026-04-16T23:59:59+05:30'` (today)

### Step 2: Clear Existing Checkpoints
Before backfill, reset all checkpoints to June 2022 so incremental modules start from beginning:
```sql
-- On remote server
DELETE FROM etl_run_state;
-- This forces all modules to backfill from scratch
```

### Step 3: Run Full Pipeline
```bash
cd /data-drive/etl-process-dev
python3 etl_master/master_etl.py
```

### Step 4: Verify Data Population
```sql
-- Check data in key tables
SELECT COUNT(*) FROM crimes;
SELECT COUNT(*) FROM accused;
SELECT COUNT(*) FROM persons;
SELECT COUNT(*) FROM brief_facts_ai;
SELECT COUNT(*) FROM brief_facts_drug;
```

## Post-Backfill: Daily Incremental ETL

### Tables with "Updated/Modified" Fields (UPDATE + INSERT strategy)

Need to check API documentation for these:
- [ ] crimes (may have created_date/updated_date)
- [ ] accused (may have updated fields)
- [ ] persons (may have modified_date)
- [ ] disposal (may have date fields)
- [ ] chargesheets (may have update_date)
- [ ] interrogation_reports (may have updated_date)
- [ ] properties (may have modified_date)
- [ ] mo_seizures (may have timestamp)
- [ ] arrests (may have date fields)

### Tables with Checkpoint-Only Strategy (INSERT-only)

- hierarchy (static reference data)
- brief_facts_* (derived tables - regenerate from source)
- Files/Media (based on file metadata)

## Implementation Steps

### 1. API Parameter Investigation
Run curl commands to check API responses for date fields:
```bash
curl -s "http://103.164.200.184:3000/api/DOPAMS/crimes?limit=1" | jq '.data[0]' | grep -i "date\|time\|updated\|modified"
curl -s "http://103.164.200.184:3000/api/DOPAMS/accused?limit=1" | jq '.data[0]' | grep -i "date\|time\|updated\|modified"
```

### 2. Modify ETL Scripts
For tables WITH update_date support:
- Query: `WHERE updated_date >= last_checkpoint`
- Operation: UPSERT (INSERT ON CONFLICT UPDATE)
- Update checkpoint after successful run

For tables WITHOUT update_date:
- Query: WHERE created_date/id > last_checkpoint
- Operation: INSERT only
- Update checkpoint after successful run

### 3. Checkpoint Management
After backfill completes:
```sql
-- Check final checkpoint state
SELECT * FROM etl_run_state;

-- For daily runs, each ETL will automatically:
-- 1. Read last_successful_end checkpoint
-- 2. Fetch data since that timestamp
-- 3. Update checkpoint on success
```

## Current Fix Applied
✅ Fixed etl_persons.py line 1399: removed `.isoformat()` call on already-string `resume_boundary`

## Files to Modify
1. `/home/ashish-ratna/DOPAMS-ETL/etl-crimes/config.py` - Update date range
2. ETL scripts that need update support (TBD based on API checks)
3. Remote server: clear etl_run_state table before backfill

## Timeline
1. Update config.py - 5 min
2. Clear checkpoints on remote - 2 min
3. Run backfill - depends on data volume (2-24 hours estimated)
4. Verify data - 30 min
5. API investigation for update strategies - 1-2 hours
6. Modify ETL scripts for update support - 2-4 hours

## Risk Mitigation
- Backfill is read-only API calls, safe to retry
- Checkpoints prevent duplicate inserts if backfill is re-run
- Daily runs only fetch data since last checkpoint
- etl-mongo-to-postgresql has been confirmed not running
