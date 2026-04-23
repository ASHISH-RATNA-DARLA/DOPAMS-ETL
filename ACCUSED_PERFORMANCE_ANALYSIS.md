# ETL Accused - Performance Analysis & Bottlenecks

## Executive Summary

The `etl_accused` pipeline **currently performs well** with acceptable execution times (~7-8 minutes for 382 chunks), but has **several optimization opportunities** that could potentially improve throughput by 20-40% under high-volume scenarios.

---

## Current Performance Baseline

**Last Run Metrics (from log - 2026-03-10 execution):**
- **Total execution time**: 7 minutes 20 seconds
- **Total chunks processed**: 382 date ranges (5-day windows)
- **Average time per chunk**: ~1.15 seconds
- **Total accused records**: ~26,500+ fetched and processed
- **Parallelism**: 4 chunk-level workers + 8 row-level workers per chunk

---

## Identified Bottlenecks

### 1. **Per-Record Database Queries (HIGH IMPACT)**
**Severity**: HIGH | **Optimization Potential**: 30-40%

**Current Implementation**:
```python
# insert_accused() - lines 1243-1310
cursor.execute(f"SELECT 1 FROM {CRIMES_TABLE} WHERE crime_id = %s", (crime_id,))  # Per record
cursor.execute(f"SELECT 1 FROM {PERSONS_TABLE} WHERE person_id = %s", (person_id,))  # Per record
cursor.execute(f"SELECT date_created, date_modified FROM {CRIMES_TABLE} WHERE crime_id = %s")  # Per record
```

**Issue**: 
- Each of the 26,500+ records triggers 2-3 database queries in the critical path
- These are NOT batched - they execute one-by-one during row-level parallelism
- Even with ThreadPoolExecutor, this creates connection pool contention

**Impact Calculation**:
- 26,500 records × 2-3 queries = 53,000-79,500 individual queries
- Each query has TCP round-trip latency + connection pool overhead
- At 1ms per query, this adds ~1-1.5 minutes of latency

**Recommendations**:
1. **Batch pre-check queries** (load all crime/person IDs at chunk start)
   - Fetch all crime_ids and person_ids in the chunk at once
   - Keep in memory as sets for O(1) lookups
   - Reduces from 26,500 → ~4-5 bulk queries per chunk

2. **Cache crime metadata at chunk level**
   - Pre-fetch date_created/date_modified for all crimes in chunk
   - Avoid repeat crime queries during row processing

3. **Use PostgreSQL `VALUES()` batch lookups**
   - `WHERE crime_id = ANY(%s)` for bulk existence checks
   - Return all results in one query instead of per-row checks

---

### 2. **Sequential Chunk Processing with Schema Evolution (MEDIUM IMPACT)**
**Severity**: MEDIUM | **Optimization Potential**: 15-20%

**Current Implementation** (lines 1693-1704):
```python
for api_field, db_column in new_fields.items():
    self.add_column_to_table(db_column)  # Sequential
    # Schema lock acquired for each column addition
self.update_existing_records_with_new_fields(new_fields, to_date)  # Sequential
```

**Issue**:
- Schema changes (`ALTER TABLE`) acquire locks that block concurrent workers
- Currently done **sequentially** for each new field
- All 4 parallel chunks must wait while schema evolves

**Impact**:
- Even if rare, adds measurable delay when new fields appear
- Seen in logs but not frequent enough to dominate execution

**Recommendations**:
1. **Batch schema additions**
   - Combine multiple new fields into single `ALTER TABLE` statement
   - Reduces lock acquisition overhead

2. **Defer schema updates to end of chunk batch**
   - Process current chunk without new fields
   - Apply schema changes after all chunks in parallel batch complete

---

### 3. **Redundant Person Stub Creation Queries (MEDIUM IMPACT)**
**Severity**: MEDIUM | **Optimization Potential**: 10-15%

**Current Implementation** (lines 1258-1280):
```python
if person_id:
    cursor.execute(f"SELECT 1 FROM {PERSONS_TABLE} WHERE person_id = %s", (person_id,))  # Check
    if not person_exists:
        cursor.execute(f"INSERT ... ON CONFLICT (person_id) DO NOTHING", (person_id,))  # Insert
```

**Issue**:
- Always checks before insert (2 queries: check + insert)
- `ON CONFLICT DO NOTHING` already handles this atomically
- The CHECK query is often wasted work

**Impact**:
- 26,500 records, if 10-20% have person_id → 2,650-5,300 redundant check queries

**Recommendations**:
1. **Skip existence check, rely on ON CONFLICT**
   ```python
   # Just insert directly
   cursor.execute(
       f"INSERT INTO {PERSONS_TABLE} (person_id) VALUES (%s) ON CONFLICT DO NOTHING",
       (person_id,)
   )
   ```

2. **Batch stub creation at chunk level**
   - Collect all unique person_ids in chunk
   - Insert all stubs in one batch at chunk end

---

### 4. **Transaction Commits Per Record (LOW-MEDIUM IMPACT)**
**Severity**: LOW-MEDIUM | **Optimization Potential**: 5-10%

**Current Implementation** (lines 1409, 1415, 1522, 1547):
```python
cursor.execute(...)
self.route_accused_status(accused, cursor)  # May do more queries
conn.commit()  # Per record!
```

**Issue**:
- Each record triggers an individual transaction commit
- ThreadPoolExecutor workers each have separate connections
- Commits add fsync overhead even when batched at DB level

**Impact**:
- With 26,500 records and 8 row workers, ~3,300 commits per chunk batch
- Each commit has I/O cost

**Recommendations**:
1. **Batch commits at sub-chunk level** (e.g., every 100 records)
   - Instead of 1 commit/record → 1 commit/100 records
   - Reduces from 26,500 → 265 commits

2. **Group consecutive writes**
   ```python
   # Process 100 rows, then commit once
   if processed_count % 100 == 0:
       conn.commit()
   ```

---

### 5. **Chunk-Level Parallelism Undersized (LOW-MEDIUM IMPACT)**
**Severity**: LOW-MEDIUM | **Optimization Potential**: 10-15% potential

**Current Implementation** (lines 1929-1944):
```python
chunk_workers = get_int_env('ACCUSED_CHUNK_WORKERS', 4)
```

**Issue**:
- Default 4 workers may be conservative for a 64GB server
- Logs show ~1.15s/chunk on average, suggesting I/O wait time
- With proper API response caching, could run more chunks in parallel

**Impact**:
- 4 workers × 1.15s/chunk ≈ 4.6s utilization per second
- Server likely has spare capacity (CPU, network)

**Recommendations**:
1. **Increase to 8 chunk workers** for high-volume runs
   - Monitor API rate limits (may be 10+ req/sec)
   - Watch DB connection pool utilization

2. **Add adaptive parallelism**
   - If API is fast, increase chunk workers
   - If API slow, back off to avoid queue buildup

---

## Summary Table

| Bottleneck | Type | Impact | Current Loss | Fix Potential |
|-----------|------|--------|--------------|---------------|
| Per-record DB queries | Critical path | 1-1.5 min | 13-20% | Batch + cache |
| Schema evolution locks | Lock contention | 10-30s | 2-7% | Defer/batch |
| Redundant person checks | Wasted queries | 10-20s | 2-4% | Skip check |
| Per-record commits | I/O overhead | 20-40s | 4-9% | Batch commits |
| Undersized parallelism | Underutilization | Marginal | ~5% | Increase workers |
| **Total Potential Gain** | - | - | **35-50 sec** | **25-40%** |

---

## Recommended Implementation Order

1. **Phase 1 (Quick Wins)** - 15-20% gain, 2-3 hours
   - [ ] Batch crime/person existence checks per chunk
   - [ ] Skip redundant person existence check before insert
   - [ ] Batch person stub creation

2. **Phase 2 (Medium Effort)** - 10-15% gain, 4-6 hours
   - [ ] Batch transaction commits (every 100 records)
   - [ ] Pre-cache crime dates at chunk level

3. **Phase 3 (Complex)** - 5-10% gain, 6-8 hours
   - [ ] Defer schema evolution to end of batch
   - [ ] Add adaptive parallelism based on API response times

---

## Monitoring Recommendations

Add timing instrumentation to track:
```python
# At chunk level
chunk_start = time.time()
api_fetch_time = ...
processing_time = ...
commit_time = ...
logger.info(f"Chunk timings: API={api_fetch_time:.2f}s, Processing={processing_time:.2f}s, Commits={commit_time:.2f}s")

# At worker level (sample 10% of records)
if random.random() < 0.1:
    logger.debug(f"Record {accused_id}: checks={check_time:.3f}s, insert={insert_time:.3f}s")
```

---

## Notes

- **Current performance is acceptable** for daily incremental runs (7-8 min for 6 months of data)
- **Optimization priority**: Per-record queries (biggest bang for buck)
- **Risk level**: Low - all recommendations are internal optimizations, no API/DB schema changes
- **Testing needed**: Run full pipeline after each optimization to ensure correctness
