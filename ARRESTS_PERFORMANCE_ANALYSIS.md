# Arrests ETL Performance Analysis

## Execution Summary
- **Duration:** 2011.19 seconds (~33 minutes)
- **Date Range:** 2022-01-01 to 2026-04-22 (4+ years)
- **Total Chunks:** 393 date ranges (5-day chunks)
- **Average Per Chunk:** 6.98s/chunk
- **Total Records:** 50,291 arrests fetched, 50,151 processed

## Root Cause: SEQUENTIAL CHUNK PROCESSING

**File:** `etl_arrests.py` Line 1575
```python
for from_date, to_date in tqdm(date_ranges, desc="Processing date ranges", unit="range"):
    self.process_date_range(from_date, to_date, table_columns)
    time.sleep(1)  # Be nice to the API
```

**Problem:** 
- Each date range (5-day chunk) is processed ONE AT A TIME
- 393 chunks × 6.98s average = ~2740 seconds expected
- Only parallelization is within each chunk (ThreadPoolExecutor processes individual records in parallel)
- Chunk-level processing is completely sequential

## Performance Breakdown per Chunk

Each chunk typically takes:
1. **API Fetch:** 3-5 seconds (~128 records per chunk)
2. **Record Processing:** 1-3 seconds (parallel ThreadPoolExecutor for records)
3. **Total:** 6-7 seconds per chunk

Example from logs:
```
2026-04-23 18:09:04 - INFO - 📅 Processing: 2026-03-21 to 2026-03-25
2026-04-23 18:09:08 - INFO - ✅ Fetched 422 arrests records    ← 4 seconds for API
2026-04-23 18:10:01 - INFO - ✅ Completed: ...                 ← 3 seconds for DB ops
```

## Configuration Analysis

From `.env`:
```
ETL_PARALLEL_WORKERS=8              # For file downloads
MAX_WORKERS=32                       # Within-chunk record parallelism  
MAX_API_WORKERS=4                    # NOT BEING USED
BATCH_COMMIT_SIZE=10               # Small batches = more commits
```

**Issue:** 
- `MAX_API_WORKERS=4` is defined but not utilized for parallel API calls across chunks
- Only within-chunk record processing is parallelized
- Chunk-level processing remains sequential despite available CPU

## Comparison: Disposal vs Arrests

| Metric | Disposal | Arrests |
|--------|----------|---------|
| Duration | 374s (6.2 min) | 2011s (33.5 min) |
| Records | Unknown (likely fewer) | 50,291 |
| Per-Record Time | ~0.0074s | ~0.040s |
| Parallelization | Unknown | Within-chunk only |

**Note:** Arrests is slower due to sequential chunk processing. If disposal processes fewer date ranges or smaller date ranges, it would complete faster.

## Optimization Opportunities

### 1. **Parallel Chunk Processing** (50-60% potential gain)
Replace sequential for loop with ThreadPoolExecutor for chunk processing:
```python
# Current: 393 chunks × 7s = 2740s
# With parallelism (8 workers): 393 chunks ÷ 8 × 7s ≈ 343s
# Realistic gain: 50-60% (accounting for thread overhead, DB pool limits)
```

### 2. **Increase Batch Commit Size** (10-15% gain)
Currently `BATCH_COMMIT_SIZE=10` - too small
- Reduce commits from ~5000+ down to ~500
- Tradeoff: slightly delayed durability, much faster throughput
- Recommended: `BATCH_COMMIT_SIZE=50-100`

### 3. **Increase Chunk Size** (20-30% potential)
Currently: 5-day chunks (393 chunks)
- Increase to 7-day chunks: ~280 chunks (28% fewer API calls)
- Per-chunk time increases slightly, but fewer total chunks
- Requires API verification it supports larger date ranges

### 4. **Reduce Per-Chunk Sleep** (minimal)
Currently: `time.sleep(1)` between chunks
- Remove or reduce to 0.1-0.5s if API allows
- Small gain: only 393 seconds max (20% of total)

### 5. **Connection Pool Optimization**
Currently:
- `ETL_DB_POOL_SIZE=10`
- `DB_POOL_MIN_CONN=10, MAX_CONN=20`

For 8 parallel chunk workers, should be:
- `ETL_DB_POOL_SIZE=16` (workers + buffer)
- `DB_POOL_MAX_CONN=20` (adequate)

## Recommended Quick Wins (in order of impact)

1. **Parallelize chunk processing** → 50-60% gain
2. **Increase BATCH_COMMIT_SIZE to 50** → 10-15% gain
3. **Increase chunk size to 7 days** → 20-30% gain (if API supports)
4. **Adjust DB pool** → 5-10% gain

**Combined potential:** 60-80% reduction (33 min → 6-13 min)

## Notes

- Server has 64GB RAM available
- ETL_PARALLEL_WORKERS=8 is reasonable, could go to 16
- Current bottleneck is architectural (sequential chunks), not resource-limited
- No LLM calls in arrests ETL, so it's pure I/O and DB bound
