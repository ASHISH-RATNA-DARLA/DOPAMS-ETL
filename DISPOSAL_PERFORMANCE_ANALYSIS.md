# Disposal ETL Performance Analysis & Optimization

## 📊 Current Performance Baseline

### Execution Time
- **Total Duration:** 374 seconds (~6.2 minutes)
- **Chunk Count:** ~80 chunks (5-day date ranges)
- **Per-Chunk Time:** ~4.7 seconds average

### Breakdown by Phase
```
API Fetch:        2.5-3.5s per chunk (network I/O)
Record Processing: 1.0-1.5s per chunk (within-chunk parallelized)
Database Commit:   0.5-1.0s per chunk (transaction overhead)
Overhead:          0.5-1.0s per chunk (logging, schema checks)
────────────────────────────────────
Total:             ~4.7s per chunk
```

---

## 🔍 Root Cause Analysis

### Sequential Bottleneck

**File:** `etl-disposal/etl_disposal.py:1548-1551`

```python
# OLD CODE (Sequential)
for from_date, to_date in tqdm(date_ranges, desc="Processing date ranges", unit="range"):
    self.process_date_range(from_date, to_date, table_columns)
    time.sleep(1)  # Be nice to the API
```

**Problem:** 
- Chunks are processed one-at-a-time despite 8 CPU cores available
- Each chunk is independent (can run in parallel)
- Only within-chunk record processing is parallelized (MAX_WORKERS=32)
- API supports concurrent requests (rate limit is per-request, not per-source)
- 1-second sleep between chunks adds ~80 seconds total

### Current Parallelization
- ✅ Within each chunk: 32 parallel record processors
- ❌ Between chunks: Sequential (single worker)

### Opportunity
- Each chunk independently:
  1. Calls API for disposal data
  2. Processes records in parallel (ThreadPoolExecutor with 32 workers)
  3. Writes to database

Since chunks are independent, they can all run in parallel!

---

## 📈 Optimization Roadmap

### Priority 1 - Parallel Chunk Processing (60-70% gain) ✅ IMPLEMENTED

**Approach:**
- Replace sequential for loop with ThreadPoolExecutor
- Use 6 concurrent chunk workers (conservative for disposal dataset)
- Auto-scale DB pool: minconn=12, maxconn=17
- Implement error recovery with exponential backoff

**Expected Improvement:**
- 80 chunks ÷ 6 workers = 13-14 batches
- Batch time = 4.7s × 6 ÷ 6 = ~4.7s per batch
- Overhead = ~20-30s
- **Total: 100-150s (60-70% reduction from 374s)**

**Implementation:**
- Added `_process_chunk_worker()` - worker thread wrapper
- Added `process_date_ranges_parallel()` - orchestrator with monitoring
- Updated `connect_db()` - auto-scale pool for 6 chunk workers
- Updated `.env` - DISPOSAL_CHUNK_PARALLEL_WORKERS=6

---

## 🛡️ Safety Measures

### Data Integrity Protections

1. **Idempotent Operations**
   - Re-fetching same API chunk produces same results
   - DB insert/update uses smart logic (only update if changed)
   - Duplicate handling: process all duplicates, smart update decides if real change

2. **Connection Pool Safety**
   - Auto-scales based on worker count
   - Reserved 5 connections for metadata operations
   - Graceful degradation if pool exhausted

3. **Error Recovery**
   - Failed chunks captured and retried sequentially
   - Exponential backoff: 1s, 2s, 4s, 8s...
   - Detailed logging of all failures

4. **Atomicity**
   - Each chunk is a transaction
   - Failure isolated to that chunk only
   - No cascading failures

---

## 📊 Performance Projections

### Calculation

```
Current: 80 chunks × 4.7s/chunk = 374s

With 6 parallel workers:
- Chunks per worker: 80 ÷ 6 = 13.3 ≈ 14 batches
- Time per batch: 14 chunks × 4.7s ÷ 6 workers = 11 seconds
- Total chunk time: 14 batches × 4.7s = 66s
- Overhead (pool setup, monitoring): 20-30s
- Removed sleep overhead: -80s

Conservative estimate: 66s + 25s = 91s
Realistic estimate: 100-150s
Expected: 100-150s (60-70% reduction)
```

### Scenario Analysis

**Scenario A: Best Case (all chunks complete on first try)**
- Batch processing: 66s (14 × 4.7)
- Overhead: 20s
- **Total: 86 seconds (77% improvement)**

**Scenario B: Realistic Case (1-2 chunks fail once)**
- Batch processing: 66s
- Failed chunk retry: 1-2 chunks × 4.7s = 4.7s
- Overhead: 25s
- **Total: 96 seconds (74% improvement)**

**Scenario C: Conservative Case (5% failure rate)**
- Batch processing: 66s
- Failed chunk retries: 4 chunks × 4.7s = 18.8s
- Overhead: 25s
- **Total: 110 seconds (71% improvement)**

---

## 🔧 Configuration Details

### Pool Sizing

```python
chunk_workers = 6
minconn = max(10, chunk_workers + 3) = 9   # Pre-allocated
maxconn = max(20, chunk_workers * 2 + 5) = 17  # Maximum
reserved = 5  # For health checks, schema operations
```

**Allocation:**
- 6 chunk workers: can grab up to 6 connections
- Each chunk may also use MAX_WORKERS=32 internally (within-chunk record processing)
- However, record processing reuses connections (doesn't hold 32 at once)
- Reserved 5: guaranteed available for metadata ops

**Key Safety:** Total workers that can grab connections = min(6, 17-5) = 6 ✅

### Environment Variables

```env
# Disposal-specific (takes precedence)
DISPOSAL_CHUNK_PARALLEL_WORKERS=6

# Fallback (if disposal-specific not set)
CHUNK_PARALLEL_WORKERS=8

# Within-chunk record processing (unchanged)
MAX_WORKERS=32
```

---

## 📊 Comparison with Arrests ETL

### Arrests ETL (Successfully Deployed)
- Chunks: 393
- Workers: 8 (larger dataset)
- Per-chunk time: 5.1s (larger API responses)
- Sequential time: 2011s (33.5 min)
- Expected parallel: 250-350s (5-7 min)
- Improvement: 75-88%

### Disposal ETL (Current Implementation)
- Chunks: 80
- Workers: 6 (smaller, conservative)
- Per-chunk time: 4.7s (smaller API responses)
- Sequential time: 374s (6.2 min)
- Expected parallel: 100-150s (1.5-2.5 min)
- Improvement: 60-70%

**Why lower improvement %?** 
Disposal chunks are already smaller (fewer API calls). The 4.7s per chunk is already relatively efficient. But absolute time savings (224 seconds) is still huge!

---

## 🔍 Bottleneck Analysis: Why 6 Workers?

### Disposal Dataset Characteristics
- Date range: ~1.5-2 years (2022-2024)
- Chunk size: 5 days
- Total chunks: ~80-100
- Records per chunk: 3-10 (very small)

### Why Not 8 (Like Arrests)?
1. **Smaller dataset** - only 80 chunks means fewer batches
   - 80 chunks ÷ 8 workers = 10 batches
   - 80 chunks ÷ 6 workers = 13 batches
   - Time difference is minimal (time limited by batch processing, not worker count)

2. **Conservative approach** - smaller dataset means less margin
   - Fewer chunks = less parallelism benefit
   - 8 workers adds pool stress without proportional gain
   - 6 workers is optimal balance

3. **Pool efficiency**
   - DB pool: 8 workers would need minconn=11, maxconn=21
   - Current: 6 workers need minconn=9, maxconn=17
   - Smaller pool = less memory, less contention

### Calculation
```
8 workers: 80 ÷ 8 = 10 batches → 10 × 4.7 = 47s + overhead = 70-90s
6 workers: 80 ÷ 6 = 13 batches → 13 × 4.7 = 61s + overhead = 85-110s
Difference: 15-20s (6.7% slower) with 6 workers

But 6 workers uses 2 fewer connections, better stability
→ Trade-off accepted: minimal performance difference, better stability
```

---

## 📋 Testing Strategy

### Pre-Deployment Testing
1. **Unit Tests** (`test_disposal_parallel.py`)
   - Pool sizing calculations
   - Worker count limits
   - Chunk isolation
   - Error recovery
   - Throughput benchmarks

2. **Integration Testing**
   - Run with small date range (1-2 weeks)
   - Verify parallel execution (check logs)
   - Validate output matches sequential run
   - Monitor resource usage

### Post-Deployment Monitoring
1. **Execution metrics**
   - Compare total time: 374s → 100-150s
   - Track per-chunk times
   - Monitor failed chunks

2. **Resource metrics**
   - DB CPU utilization
   - Connection pool usage
   - Memory consumption

3. **Data quality**
   - Row count consistency
   - Duplicate detection
   - Failed record logging

---

## 🚀 Deployment Procedure

### 1. Pre-Flight Checks
```bash
# Verify code syntax
python3 -m py_compile etl-disposal/etl_disposal.py

# Run test suite
python3 test_disposal_parallel.py
```

### 2. Staging Deployment
```bash
# Test with limited date range (7 days)
RESTART_DATE=2024-12-17 python3 etl-disposal/etl_disposal.py
# Monitor: 4-7 chunks processed
# Expected time: 20-40s total
```

### 3. Production Deployment
```bash
# Full run with standard config
python3 etl-disposal/etl_disposal.py
# Monitor: 80 chunks processed
# Expected time: 100-150s total
```

### 4. Validation
```sql
-- Verify record count unchanged
SELECT COUNT(*) as current_count FROM disposal;

-- Check for duplicates
SELECT crime_id, disposal_type, disposed_at, COUNT(*) as cnt
FROM disposal
GROUP BY crime_id, disposal_type, disposed_at
HAVING COUNT(*) > 1
LIMIT 10;

-- Should return 0 rows
```

---

## 📈 Success Criteria

✅ **Performance**
- Total execution: < 150 seconds (target)
- All 80 chunks processed successfully

✅ **Reliability**
- 0 data loss or corruption
- No duplicate processing
- 0 connection exhaustion errors

✅ **Safety**
- Row counts match sequential baseline
- No unexpected failures
- Proper error logging

---

## 🔄 Rollback Path

If any issues occur:
```bash
# Revert code to sequential
git checkout HEAD^ -- etl-disposal/etl_disposal.py

# Restore .env
DISPOSAL_CHUNK_PARALLEL_WORKERS=1

# Re-run with sequential processing
python3 etl-disposal/etl_disposal.py
```

Expected time: return to ~374 seconds (original)

---

## 📚 References

- [Arrests ETL Parallel Deployment](ARRESTS_PARALLEL_DEPLOYMENT.md) - Original framework
- [DB Pooling Module](db_pooling.py) - Connection pool implementation
- [ETL Persons Optimization](ETL_PERSONS_BOTTLENECKS.md) - Other ETL improvements
- [IR ETL Analysis](IR_ETL_PERFORMANCE_ANALYSIS.md) - Comparative analysis

---

Last Updated: 2026-04-23
Optimized By: Claude Code
Framework: Arrest ETL Parallel Processing Pattern
