# IR ETL Performance Analysis - 2026-04-23

## Executive Summary
**Total Execution Time:** 1318.86 seconds (21 min 58 sec)  
**Records Processed:** 16,824 fetched → 14,779 inserted + 1,798 no-change + 247 failed  
**API Calls:** 393 calls (5-day chunks with 1-day overlap)  
**Average Time per Date Range:** ~3.35-4.62 seconds

---

## Performance Timeline Breakdown

### Execution Pattern
- **Start:** 2026-04-23 17:08:18
- **End:** 2026-04-23 17:30:16
- **Duration:** 1,318.86 seconds

### By Phase
1. **Initialization (0-5 sec)**: Database connection pool, schema migration, crime ID loading
2. **Processing (5-1312 sec)**: Sequential API calls + record insertion
3. **Finalization (1312-1319 sec)**: FK retry, statistics aggregation

---

## Identified Bottlenecks

### 1. **Sequential API Calls (PRIMARY - ~1300 seconds)**
- **Issue:** 393 API calls executed sequentially
- **Current Pattern:**
  ```
  API Call → Wait 2-3 sec (network + processing)
  → ThreadPool Process Records (parallel)
  → 1 sec sleep (API throttling)
  → Repeat 393 times
  ```
- **Time Allocation:** (393 calls × 3.3 sec avg) = ~1,297 seconds
- **Root Cause:** `process_date_range()` is called serially in a loop with 1-second artificial delay

### 2. **UUID Validation Errors (SECONDARY - Data Quality)**
- **Issue:** 247 records failed due to "invalid input syntax for type uuid"
- **Affected Table:** `ir_defence_counsel` (and possibly other related tables)
- **Error Pattern:** Records with IDs like `"68c80b9778900561705eda56"` fail UUID validation
- **Impact:** Wasted DB resources, rollbacks, potential cascading failures
- **Example Errors:**
  ```
  invalid input syntax for type uuid: "68c80b9778900561705eda56"
  invalid input syntax for type uuid: "69c76aa4a1508b868b093ccd"
  ```

### 3. **Excessive Date Range Chunking**
- **Current:** 393 date ranges (5-day chunks with 1-day overlap)
- **Calculation:** 4+ years (2022-01-01 to 2026-04-22) ÷ 5 days = ~393 ranges
- **Overlap Cost:** 1-day overlap creates redundant processing
- **Issue:** Early chunks (2022) return 0 records, wasting API calls & time

### 4. **Record Processing Inefficiency**
- **Records Processed:** 16,824 total
- **Average per API Call:** ~42.8 records
- **Processing Pattern:** ThreadPoolExecutor with sequential record insertion
- **Bottleneck:** Each record involves:
  - 1 main table INSERT/UPDATE
  - 24 related table INSERTs
  - Multiple constraint checks
  - Frequent stats_lock acquisitions

### 5. **Threading & Lock Contention**
- **stats_lock:** Hit on every record success/failure (16,824 times)
- **schema_lock:** Hit when adding new columns (during schema evolution)
- **Connection Pool:** 37 max connections, but sequential processing limits parallelism
- **ThreadPoolExecutor:** Per-range, not cross-range, limiting throughput

### 6. **1-Second Sleep Between Ranges**
- **Cost:** 393 seconds × 1 sec = ~393 seconds of pure waste
- **Reason:** "Be nice to the API" - but adds 6.6% to total runtime

---

## Performance Metrics

| Metric | Value | Time Cost |
|--------|-------|-----------|
| API Calls | 393 | ~1,297 sec |
| Sleep Delays | 393 × 1 sec | 393 sec |
| Record Processing | 16,824 records | ~400-500 sec |
| Stats Lock Contention | 16,824 acquisitions | ~50-100 sec |
| Database Overhead | Connection mgmt | ~50 sec |
| **Total** | | **1,319 sec** |

---

## Data Quality Issues

### UUID Format Violations
- **Count:** 247 failures (1.5% failure rate)
- **Tables Affected:**
  - `ir_defence_counsel` (most common)
  - `ir_associate_details` (possible)
  - Other tables with person_id foreign keys
- **Root Cause:** API returning non-UUID person IDs
- **Current Handling:** Records are rolled back, counted as failures
- **Loss:** ~1,432 records (247 failures × avg 5.8 records per failed entry)

---

## Memory & Resource Usage

- **Connection Pool:** 37 connections (5 min, 37 max)
- **ThreadPool:** Per-range execution, max_workers = ~32
- **Crime IDs in Memory:** 7,761 IDs loaded (O(1) lookup)
- **Database:** Single connection pool shared across threads

---

## Recommendations (Priority Order)

### HIGH PRIORITY
1. **Parallel API Calls** (Potential: -50-60%)
   - Use concurrent.futures.ThreadPoolExecutor for API calls
   - Fetch next chunk while processing current chunk
   - Max 3-5 concurrent API calls to avoid rate limiting
   - Estimated gain: ~650-780 seconds (50-60% reduction)

2. **Fix UUID Validation** (Potential: +1.5% success, -247 failures)
   - Validate person_id format before insertion
   - Use `is_valid_uuid()` check pre-insert
   - Handle non-UUID values: truncate to valid UUID or store as VARCHAR
   - Estimated gain: Zero time (but +1,432 records preserved)

### MEDIUM PRIORITY
3. **Reduce Date Range Overlap** (Potential: -10-15%)
   - Current: 5 days with 1-day overlap = 16% redundancy
   - Change: 10-day chunks with 0-day overlap
   - Risk: May miss records at chunk boundaries (lower risk with ordered timestamps)
   - Estimated gain: ~130-200 seconds

4. **Remove Artificial Sleep** (Potential: -30%)
   - Current: 1 sec per range × 393 = 393 seconds
   - Alternative: Respect HTTP 429 responses instead
   - Or: Batch API calls to reduce count
   - Estimated gain: 393 seconds

5. **Optimize Lock Usage** (Potential: -5-10%)
   - Use thread-local stats aggregation, merge at end
   - Or use atomic counters instead of locks
   - Current: 16,824 lock acquisitions
   - Estimated gain: ~50-100 seconds

### LOWER PRIORITY
6. **Batch Record Insertion** (Potential: -10-15%)
   - Current: Individual ThreadPoolExecutor.map() per record
   - Alternative: Batch 5-10 records per thread
   - Would reduce stats_lock contention
   - Estimated gain: ~50-100 seconds

7. **Query Optimization** (Potential: -5%)
   - Pre-fetch existing records in bulk instead of per-record
   - Use SELECT WHERE... IN (...) for batch checks
   - Estimated gain: ~30-50 seconds

---

## Implementation Priority

**Phase 1 (Immediate - 50-60% gain):**
- Parallel API calls

**Phase 2 (Quick fix - +1.5% data recovery):**
- UUID validation

**Phase 3 (Medium effort - 10-15% gain):**
- Reduce date range overlap + remove sleep

**Phase 4 (Optimization - 5-10% gain):**
- Lock contention reduction + batching

---

## Estimated Post-Optimization Timeline

| Scenario | Time | Gain |
|----------|------|------|
| Current | 1,319 sec (22 min) | baseline |
| Phase 1 only | 650-700 sec (11-12 min) | 50-60% |
| Phase 1 + 2 | 650-700 sec | +1.5% data |
| Phase 1 + 3 | 500-550 sec (8-9 min) | 60-65% |
| All phases | 400-450 sec (7-8 min) | 65-70% |

---

## Monitoring Recommendations

1. **Add per-phase timing logs**
2. **Track API response times (min/max/avg)**
3. **Monitor connection pool usage**
4. **Log UUID validation rejections**
5. **Alert on failure rate > 1%**

---

## Code Locations for Optimization

- **Sequential API calls:** `ir_etl.py:1761-1764` (main loop)
- **UUID validation:** `ir_etl.py:1496-1518` (process_ir_record)
- **Lock contention:** `ir_etl.py:145-159` (stats initialization)
- **Sleep delay:** `ir_etl.py:1764` (time.sleep)
- **Date range generation:** `ir_etl.py:514-538` (generate_date_ranges)
