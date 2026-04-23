# ETL Persons Execution Bottlenecks Analysis

**Execution Time (2026-04-23):**
- Start: 16:22:48
- End: 16:35:11
- **Duration: 743.28 seconds (~12.4 minutes)**

---

## Summary

etl_persons has **at least 3 significant bottlenecks** that can be addressed without logic changes. The dominant bottleneck is **per-person commits**, which duplicates the issue found in brief_facts_ai.

---

## The Bottlenecks (Priority Order)

### 1. **Per-Person Commits (CRITICAL - 25-40% gain)**
**Location:** `etl_persons.py:1900`

**Issue:**
```python
def upsert_person(self, d: Dict, table_columns: Set[str], conn, cursor):
    ...
    conn.commit()  # ← Called ONCE PER PERSON
```

Every single person processed triggers an individual commit, including all intermediate operations (SELECT for existence check, UPDATE or INSERT, schema evolution checks, new field updates).

**Impact:**
- Each commit = fsync to disk (expensive I/O operation)
- If processing 100+ persons per run, this is 100+ individual fsyncs
- Typical fsync cost: 5-50ms each = 500ms-5s overhead just on commits
- With 743s total runtime, this could add **100-500+ seconds**

**Fix:** Batch commits at the end of each date range window or collect all changes and commit once after processing all records.

**Risk:** LOW - The logic is already atomic per-person; batching just delays the commit, doesn't change semantics.

**Example gain:** If 500 persons processed:
- Current: 500 commits × ~10-20ms/fsync = 5-10 seconds overhead
- Fixed: 1 commit for entire batch = ~10-20ms overhead
- **Potential gain: 30-50% (3-5+ minutes saved)**

---

### 2. **Sequential API Calls with Retry Sleep (MODERATE - 10-20% gain)**
**Location:** `etl_persons.py:1510-1563`

**Issue:**
```python
for attempt in range(API_CONFIG['max_retries']):  # max_retries = 5
    try:
        resp = requests.get(..., timeout=180)  # 180 second timeout
        ...
    except requests.exceptions.Timeout:
        if attempt < API_CONFIG['max_retries'] - 1:
            time.sleep(2 ** attempt)  # 1s, 2s, 4s, 8s, 16s exponential backoff
```

**Problem:**
- Long retry sleeps (2^n exponential backoff) block the thread
- Even though ThreadPoolExecutor runs persons in parallel, ANY timeout on a person blocks that worker for exponential sleep
- If API is slow (close to 180s timeout), sleeps accumulate

**Potential Optimization:**
1. **Reduce initial timeout** from 180s to 60-90s (if API typically responds faster)
2. **Use shorter backoff:** Replace `2 ** attempt` with fixed delays (e.g., 5s, 10s, 15s) or jitter
3. **Concurrent retry:** Use asyncio or dedicated retry thread instead of blocking current worker

**Risk:** MEDIUM - Incorrect timeout can cause false failures if API is genuinely slow.

**Expected gain:** If 5-10% of requests timeout and retry:
- Current: Lost worker + 30s sleep overhead
- Fixed: Shorter backoff or concurrent retry
- **Potential gain: 2-3 minutes if timeouts are common**

---

### 3. **Schema Evolution Lock Contention (MINOR - 2-5% gain)**
**Location:** `etl_persons.py:1992-2006`

**Issue:**
```python
def process_person(pid, table_columns, from_date, to_date):
    if not first_record_processed and table_columns is not None and not self.person_gender_dry_run:
        with self.schema_lock:  # ← Lock acquired on FIRST record
            if not first_record_processed:
                new_fields = self.detect_new_fields(data, table_columns)
                if new_fields:
                    # Expensive schema changes
                    for api_field, db_column in new_fields.items():
                        if self.add_column_to_table(db_column):
                            table_columns.add(db_column)
                    self.update_existing_records_with_new_fields(new_fields)
                first_record_processed = True
```

**Problem:**
- First person to process acquires lock
- All other workers block waiting for schema evolution to complete
- Double-checks add overhead
- Schema evolution includes `ALTER TABLE` and `UPDATE` on existing records — potentially expensive

**Fix:**
1. Move schema detection BEFORE ThreadPoolExecutor spawns (single-threaded, no lock needed)
2. Or: make `first_record_processed` a thread-safe flag without holding lock during schema changes

**Risk:** LOW - Schema evolution is idempotent; moving it earlier doesn't change behavior.

**Expected gain:** Only impacts first person processed; if schema evolution is rare, gain is minimal (< 1-2% typically).

---

### 4. **Database Connection Pool Verification (MINOR - 1-2% gain)**
**Location:** `etl_persons.py:2028`

**Issue:**
```python
requested_workers = int(os.environ.get('MAX_WORKERS', min(32, (os.cpu_count() or 1) * 4)))
max_workers = compute_safe_workers(self.db_pool, requested_workers)
```

**Current state:** ✅ Already uses `compute_safe_workers()` to avoid pool exhaustion (unlike brief_facts_ai which had pool instantiation per-crime). This is **good design**.

**Status:** No action needed here.

---

## What's ALREADY Good

✅ **execute_batch for gender corrections** (line 1320-1334): Batches multiple updates into one statement  
✅ **Pool reuse**: DatabaseConnectionPool created once at startup, reused across all workers  
✅ **Parallel processing**: ThreadPoolExecutor with configurable MAX_WORKERS  
✅ **Safe worker count**: compute_safe_workers() prevents pool exhaustion  
✅ **Deduplication of person_ids**: Tracks processed_person_ids to avoid reprocessing  

---

## Recommended Implementation Order

1. **Fix per-person commits** (Highest impact, Low risk)
   - Collect changes in list, commit once per date range
   - Fallback: Commit every N persons (e.g., every 100)

2. **Optimize API retry backoff** (Medium impact, Medium risk)
   - Test with reduced timeout (60-90s) in next run
   - Consider shorter fixed backoff or async retry

3. **Move schema evolution detection earlier** (Low impact, Low risk)
   - Run before ThreadPoolExecutor spawns
   - Eliminates lock contention on first record

---

## Expected Improvements

**With per-person commit fix alone:**
- Baseline: 743s for ~500 persons (~1.5s per person)
- With fix: ~400-500s (~0.8-1.0s per person)
- **Gain: 30-45% runtime reduction**

**With all three fixes:**
- Estimated final: 300-350s (~0.6-0.7s per person)
- **Total gain: 50-60% runtime reduction**

---

## Testing Strategy

1. Run current baseline: measure API calls vs DB writes vs total time
2. Implement commit batching; re-run with same data
3. Compare: runtime, stats (inserted/updated counts), error rates
4. If successful, test with different date ranges to validate scalability

---

## References

- Similar bottleneck found in brief_facts_ai: per-crime commits (FIXED)
- Config: API_CONFIG['max_retries']=5, API_CONFIG['timeout']=180s
- Current parallelization: ThreadPoolExecutor with compute_safe_workers()
