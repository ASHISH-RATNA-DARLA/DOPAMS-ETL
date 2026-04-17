# ETL Pipeline Audit: Incremental Processing for Daily Runs
**Date:** 2026-04-16  
**Scope:** Verify if daily ETL runs properly skip already-processed records

---

## Executive Summary

**Status:** ✅ **Incremental Processing EXISTS, but with architectural limitations**

The brief_facts_ai module (Order 23) **DOES implement per-crime incremental processing**, but:
- ✅ It correctly identifies unprocessed/modified crimes
- ✅ It maintains processing state in `etl_crime_processing_log` table
- ⚠️ It uses a **delete + re-insert pattern** that's inefficient but not duplicative
- ⚠️ It has **no integration with master checkpoint** system (unlike other ETL modules)
- ⚠️ **Daily full re-runs WILL process ALL crimes** if `etl_crime_processing_log` is cleared

---

## Detailed Findings

### 1. Per-Crime Processing Tracking

**Location:** `brief_facts_ai/db.py` lines 49-72 (`fetch_unprocessed_crimes`)

```python
def fetch_unprocessed_crimes(conn, limit=100):
    # Queries crimes that are either:
    # 1. Never processed (last_run.last_completed_at IS NULL)
    # 2. Modified since last completion (date_modified/created > last_completed_at)
    WHERE last_run.last_completed_at IS NULL
       OR COALESCE(c.date_modified, c.date_created) > last_run.last_completed_at
```

**Database Table:** `etl_crime_processing_log`
- Tracks each crime_id with status: `in_progress | complete | failed | stale`
- Records `completed_at` timestamp for each crime
- Has branch information (A, B, C) for tracking processing path

**Verdict:** ✅ Correct incremental logic in place

---

### 2. Processing Pattern: Delete + Re-Insert

**Location:** `brief_facts_ai/main.py` lines 577-620

```python
# For each unprocessed crime:
delete_brief_facts_for_crime(conn, crime_id)          # Delete all records
# ... process crime ...
bulk_upsert_brief_facts_ai(conn, enriched_rows)       # Insert updated records
complete_crime_processing_run(conn, run_id, count)    # Mark as complete
```

**Analysis:**
- Every crime reprocessing = full delete + re-insert of that crime's accused records
- This prevents duplicate records ✅
- But means full recomputation even if only 1 field changed ⚠️
- Inefficient for large crimes, but functionally correct

**Verdict:** ✅ No duplicates, but inefficient

---

### 3. Data Consistency in `brief_facts_ai` Table

**Location:** `brief_facts_ai/db.py` lines 404-456 (`bulk_upsert_brief_facts_ai`)

```python
ON CONFLICT (crime_id, accused_id) DO UPDATE SET
    person_id = EXCLUDED.person_id,
    canonical_person_id = EXCLUDED.canonical_person_id,
    # ... updates all fields ...
    date_modified = CURRENT_TIMESTAMP
```

**Key Points:**
- Composite unique key: `(crime_id, accused_id)`
- Prevents duplicate records across multiple runs ✅
- Updates all person fields if re-processing
- Timestamp `date_modified` tracks when record was last updated

**Verdict:** ✅ No duplicate key violations possible

---

### 4. CRITICAL ISSUE: No Connection to Master Checkpoint

**The Problem:**

1. **Master ETL Checkpoint** (`etl_run_state` table):
   - Tracks backfill completion status
   - Used by OTHER modules (chargesheets, fsl_case_property, etc.) to determine date ranges
   - Only updated when ALL 28 ETL steps succeed

2. **brief_facts_ai Processing Log** (`etl_crime_processing_log` table):
   - Tracks individual crime processing status
   - **Is NOT integrated with master checkpoint**
   - **Does NOT use date-range-based filtering like other modules**

3. **Consequence:**
   ```
   Other ETL modules:
   - Use config.py date ranges based on master checkpoint
   - Skip already-processed date ranges
   - Efficient daily incremental runs
   
   brief_facts_ai:
   - Uses per-crime tracking only
   - No date-range optimization
   - Must scan ALL crimes' processing logs every run
   - More I/O and CPU intensive
   ```

**Verdict:** ⚠️ Architectural mismatch with rest of pipeline

---

## Daily Run Behavior Analysis

### Scenario: Daily Run After Backfill Complete

**Configuration State:**
```
master_etl_backfill_complete = 2026-04-15 23:59:59+05:30  (in etl_run_state)
```

**What Happens with Current Code:**

1. **Brief_facts_ai starts:**
   - Looks for input file `input.txt`
   - If not found → calls `fetch_unprocessed_crimes(limit=30)`

2. **Query execution:**
   ```sql
   SELECT c.crime_id FROM crimes c
   LEFT JOIN (
     SELECT MAX(completed_at) FROM etl_crime_processing_log
     WHERE crime_id = c.crime_id AND status = 'complete'
   ) last_run
   WHERE last_run.last_completed_at IS NULL
      OR c.date_modified > last_run.last_completed_at
   LIMIT 30
   ```

3. **Processing:**
   - Each unmodified crime: ❌ RE-PROCESSED (wasteful but correct)
   - Each modified crime: ✅ RE-PROCESSED (expected)
   - No new crimes created: ❌ Entire crimes table scanned

**Result:** ❌ **FULL RE-SCAN + RE-PROCESSING OF ENTIRE CRIMES TABLE**

---

## Root Cause: Why Daily Runs Are Inefficient

### The Missing Optimization

**What other ETL modules do:**
```python
# From etl_chargesheets/etl_chargesheets.py
start_date = get_effective_start_date()  # Gets max(date_created, date_modified)
end_date = config['end_date']            # Configured based on checkpoint
# Process only charges with: date_created/date_modified between [start_date, end_date]
```

**What brief_facts_ai does:**
```python
# No date-range optimization
crimes = fetch_unprocessed_crimes(limit=30)  # Scans ENTIRE crimes table
# Must check processing_log for EVERY crime
```

### The Inefficiency Chain
```
Daily Run Start
  ↓
Loop: fetch_unprocessed_crimes(limit=30)
  ↓
For EACH batch:
  - Query: WHERE (processing_log IS NULL) OR (date_modified > last_completed_at)
  - This scans EVERY crime in crimes table
  - Index: idx_etl_log_crime_status (crime_id, status) helps somewhat
  ↓
Process crimes: delete + re-insert
  ↓
Mark complete in processing_log
  ↓
Repeat 30 crimes at a time until no more "unprocessed"
```

**Cost:** O(N) table scans daily, where N = total crimes in DB

---

## Why Order 20 Takes 10+ Minutes Daily

**Order 20: Chargesheets Module**

This module USES proper date-range optimization:
```python
# From config
ETL_CONFIG['start_date']  = effective_start_date (from checkpoint)
ETL_CONFIG['end_date']    = yesterday_end

# Processing
generate_date_ranges(start_date, end_date, chunk_size=5_days, overlap=1_day)
# Processes only: charges with date_created/date_modified in [start_date, end_date]
```

**Even with optimization:** Still takes 10+ minutes suggests:
- Chargesheets table is large
- Complex business logic in upserts
- Network latency to external APIs

**But key difference:** It only processes NEW/MODIFIED records, not re-scanning all historical data

---

## Risk Assessment: Data Duplication

### Can Daily Runs Create Duplicate Records?

**Short Answer:** ❌ **NO, due to composite unique key**

**Why:**
- Insert uses `ON CONFLICT (crime_id, accused_id)`
- Any duplicate insert → UPDATE instead
- `date_modified` timestamp gets updated
- No actual duplication occurs

**Example:**
```
Day 1: Insert crime_id=100, accused_id='A1' (person_code='A-1')
Day 2: Crime not modified, but brief_facts_ai re-processes it:
       - Deletes old records for crime_id=100
       - Re-inserts same records
       - Result: Same data, fresh timestamp
       (Wasteful but no duplication)
```

**Verdict:** ✅ **Database prevents duplicates, but CPU/I/O are wasted**

---

## Checkpoint System Status

### parse_iso_date Issue (From Plan)

**Status:** ✅ **ALREADY FIXED**

**Location:** `etl-properties/etl_properties.py` line 61-65

```python
def parse_iso_date(date_str: str) -> datetime:
    """Parse ISO 8601 date string (with optional time component) to datetime."""
    if 'T' in date_str or ' ' in date_str:  # ← Handles space-separated format
        return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
    return datetime.strptime(date_str, '%Y-%m-%d')
```

**What it does:**
- Handles both `2026-04-15T18:29:59+00:00` (ISO with T)
- AND `2026-04-15 18:29:59+00:00` (space-separated from DB varchar)
- Timestamp parsing for checkpoint recovery works ✅

---

## Summary Table: Incremental Processing Status

| Component | Implementation | Daily Efficiency | Risk Level |
|-----------|---------------|----|------------|
| **Per-Crime Tracking** | ✅ Via `etl_crime_processing_log` | Low (full table scan) | LOW |
| **Duplicate Prevention** | ✅ Composite PK + ON CONFLICT | N/A | LOW |
| **Date-Range Optimization** | ❌ Not implemented | Poor (processes all crimes) | MEDIUM |
| **Master Checkpoint Integration** | ❌ Not used by brief_facts_ai | N/A | MEDIUM |
| **Parse ISO Date** | ✅ Handles both formats | N/A | LOW |
| **Checkpoint Persistence** | ✅ `etl_run_state` table | N/A | LOW |

---

## Recommendations for Efficient Daily Processing

### Priority 1: Add Date-Range Optimization to brief_facts_ai

Replace current logic:
```python
# Current (inefficient)
crimes = fetch_unprocessed_crimes(limit=batch_size)

# Recommended
from config import ETL_CONFIG
start_date = get_effective_start_date()
end_date = ETL_CONFIG['end_date']
crimes = fetch_crimes_by_date_range(start_date, end_date, limit=batch_size)
```

**Benefit:** Only processes crimes created/modified since last checkpoint
**Effort:** Medium (new query function + config integration)

### Priority 2: Consolidate Checkpoint Systems

Align brief_facts_ai with other modules:
```python
# Use master checkpoint like chargesheets does
if is_backfill_complete():
    start_date = get_backfill_completion_date()
    end_date = yesterday_end
    # Process only new/modified crimes
else:
    # Full backfill mode
    start_date = BACKFILL_START_DATE
    end_date = tomorrow
```

**Benefit:** Consistent checkpoint strategy across all ETL modules
**Effort:** Medium (schema might need updates)

### Priority 3: Monitor Processing Log Growth

Add periodic maintenance:
```sql
-- Archive completed runs older than 90 days
DELETE FROM etl_crime_processing_log 
WHERE status = 'complete' AND completed_at < NOW() - INTERVAL '90 days'
```

**Benefit:** Prevents processing log table bloat
**Effort:** Low (periodic maintenance task)

---

## Conclusion

✅ **Brief_facts_ai WILL NOT create duplicate records on daily runs**
✅ **Per-crime tracking prevents reprocessing of unchanged data**
⚠️ **BUT full crimes table is scanned daily (inefficient)**
⚠️ **NOT integrated with master checkpoint system**

**Assessment:** System is **safe but inefficient** for daily runs. 
Recommend implementing date-range optimization for performance.

