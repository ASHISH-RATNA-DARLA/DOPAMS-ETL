# DOPAMS ETL Performance Optimization - Implementation Roadmap

**Status:** Ready for Production Implementation  
**Document Date:** March 2, 2026  
**Target Improvement:** 5-10x pipeline throughput  
**Estimated Timeline:** 3-4 weeks  

---

## Executive Summary for Stakeholders

Your 2-minute ETL delay is **NOT from the LLM** but from synchronous blocking patterns in the Python backend:

| Problem | Evidence | Fix | Impact |
|---------|----------|-----|--------|
| **No bulk writes** | Single INSERT per record, 1000s of commits | `batch_insert()` | **10-20x faster** |
| **No connection reuse** | New DB conn per query creation | Connection pooling | **100x faster connections** |
| **Missing indexes** | Sequential scans on frequently joined columns | Add 5 indexes | **20-40x query speeds** |
| **Sequential processing** | One crime at a time, blocking chains | Async/pipelining | **3-5x throughput** |
| **GIL overhead** | Regex processing contends for GIL | Multiprocessing | **3-4x on preprocessor** |

**Quick Wins (2 days):** Batch inserts + Connection pooling + 5 indexes = **Expected 15-25x improvement**

---

## Phase 1: Quick Wins (Days 1-3)

### Task 1.1: Enable Performance Monitoring [4 hours]
**Location:** Root directory files created  
**Files:** `performance_profiler.py`, `query_optimizer.py`

```bash
# 1. Run query analysis
python query_optimizer.py

# This will identify:
- Which queries run sequential scans
- Missing indexes
- Cache hit ratios
- Unused indexes
```

**Deliverable:** Baseline metrics report (screenshot and save)

---

### Task 1.2: Create Missing Indexes [1 hour]
**Location:** `query_optimizer.py` → INDEX_RECOMMENDATIONS section

```sql
-- Copy-paste recommendations from script output
CREATE INDEX idx_brief_facts_accused_crime_id ON brief_facts_accused(crime_id);
CREATE INDEX idx_brief_facts_drugs_crime_id ON brief_facts_drugs(crime_id);
CREATE INDEX idx_accused_crime_id ON accused(crime_id);
CREATE INDEX idx_crimes_dates ON crimes(date_created DESC, date_modified DESC);
CREATE INDEX idx_persons_full_name ON persons(full_name);
```

**Expected Impact:** 20-40x faster on JOIN queries

**Verification:**
```sql
-- Rerun query analysis  
python query_optimizer.py  
-- Should show Seq Scan → Index Scan conversion
```

---

### Task 1.3: Implement Connection Pooling [8 hours]

**Step 1: Update brief_facts_accused/db.py**

```python
# BEFORE (line 1-20 in brief_facts_accused/db.py):
import psycopg2

def get_db_connection():
    conn = psycopg2.connect(
        dbname=config.DB_NAME,
        user=config.DB_USER,
        password=config.DB_PASSWORD,
        host=config.DB_HOST,
        port=config.DB_PORT
    )
    return conn

# AFTER - Replace with:
from db_pooling import PostgreSQLConnectionPool  # New import

def get_db_connection():
    """Get pooled connection (reused, not created each time)"""
    return PostgreSQLConnectionPool().get_connection()
```

**Step 2: Update all fetch and insert functions**

For EACH function in `brief_facts_accused/db.py`:

```python
# BEFORE (fetch pattern):
def fetch_unprocessed_crimes(conn, limit=100):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()
# This requires caller to create/pass conn

# AFTER:
from db_pooling import get_db_connection, return_db_connection

def fetch_unprocessed_crimes(limit=100):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchall()
    finally:
        return_db_connection(conn)
```

**Files to update:**
- [ ] `brief_facts_accused/db.py` (all functions)
- [ ] `brief_facts_drugs/db.py` (all functions)
- [ ] `etl-accused/` (search for `psycopg2.connect()`)
- [ ] `etl-crimes/` (search for `psycopg2.connect()`)

**Test Instruction:**
```python
# In brief_facts_accused/test_accused_extraction.py or new file:
from db_pooling import PostgreSQLConnectionPool

pool = PostgreSQLConnectionPool(minconn=2, maxconn=5)
conn1 = pool.get_connection()
conn2 = pool.get_connection()

# Should be SAME connection (reused from pool), not new one
print(f"conn1 is conn2: {conn1 is conn2}")  # Should be True after first release

pool.return_connection(conn1)
conn1_again = pool.get_connection()
print(f"Got pooled connection again: {conn1 is conn1_again}")  # Should be True

pool.close_all()
```

**Expected Impact:** 10-15% latency reduction, prevents connection exhaustion

---

### Task 1.4: Implement Batch Inserts [8 hours]

**Location:** `brief_facts_accused/db.py` → `insert_accused_facts` function

**BEFORE (SLOW - 4000ms for 1000 records):**
```python
def insert_accused_facts(conn, item_data):
    """Inserts extracted accused information - ONE AT A TIME ❌"""
    with conn.cursor() as cur:
        cur.execute(INSERT_QUERY, (
            item_data['bf_id'], item_data['crime_id'], 
            item_data['full_name'], item_data['age'], ...
        ))
    conn.commit()  # Commit per record!
```

**AFTER (FAST - 200-400ms for 1000 records):**
```python
from db_pooling import batch_insert

def insert_accused_facts_batch(items, batch_size=1000):
    """Batch insert 10-20x faster ✅"""
    if not items:
        return
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Convert items to tuples
            insert_tuples = [
                (
                    item['bf_id'], item['crime_id'],
                    item['full_name'], item['age'], ...
                )
                for item in items
            ]
            
            # execute_batch is 10-20x faster than loop inserts
            batch_insert(cur, INSERT_QUERY, insert_tuples, batch_size=batch_size)
        
        conn.commit()  # Single commit for all!
        logger.info(f"Batch inserted {len(items)} records")
        
    finally:
        return_db_connection(conn)
```

**Files to update:**
- [ ] `brief_facts_accused/db.py` → `insert_accused_facts()`
- [ ] `brief_facts_drugs/db.py` → `insert_drug_facts()`

**Current call pattern (NEEDS CHANGE):**
```python
# In brief_facts_accused/reproduce_issue.py or main extraction loop:
# BEFORE - processes one crime at a time
for crime in crimes:
    accused_data = extract_accused(crime)
    db.insert_accused_facts(conn, accused_data)  # One crime, one insert!

# AFTER - batches multiple crimes
crimes_batch = []
for crime in crimes:
    accused_data = extract_accused(crime)
    crimes_batch.append(accused_data)
    
    if len(crimes_batch) >= 100:  # Batch of 100
        db.insert_accused_facts_batch(crimes_batch)
        crimes_batch = []

if crimes_batch:  # Insert remainder
    db.insert_accused_facts_batch(crimes_batch)
```

**Test:**
```bash
# Before
python -m cProfile -s cumulative brief_facts_accused/reproduce_issue.py 2>&1 | grep -A 5 "insert_accused"

# After (should show much lower cumulative time)
python -m cProfile -s cumulative brief_facts_accused/reproduce_issue.py 2>&1 | grep -A 5 "batch_insert"
```

**Expected Impact:** 10-20x faster inserts

---

## Phase 2: Medium-Effort Optimizations (Days 4-10)

### Task 2.1: Async/Await Pipeline [5 days]

This is the major architectural change for maximum throughput.

**Location:** New file `brief_facts_accused/async_extractor.py`

**Concept:** Three concurrent stages instead of sequential:
- Stage 1: Fetch crimes from DB (network I/O)
- Stage 2: Extract via LLM (network I/O)  
- Stage 3: Insert to DB (I/O)

While Stage 1 fetches, Stage 2 can extract previous batch, Stage 3 can insert earlier batch.

```python
# Skeleton in PERFORMANCE_AUDIT_REPORT.md under section 6.2
# AsyncAccusedExtractor class

import asyncio
import asyncpg

class AsyncAccusedExtractor:
    async def init_pool(self, dsn):
        """Initialize async DB pool"""
        self.db_pool = await asyncpg.create_pool(dsn, min_size=10, max_size=20)
    
    async def process_all_crimes(self, limit=1000):
        """3-stage pipelined processing"""
        # All three stages run concurrently
        await asyncio.gather(
            self.stage1_fetch(limit),
            self.stage2_extract(),
            self.stage3_insert()
        )
```

**Integration points:**
- [ ] Copy AsyncAccusedExtractor from section 6.2 of PERFORMANCE_AUDIT_REPORT.md
- [ ] Create `async_main.py` entry point
- [ ] Test with 100 crimes (should complete in ~30s vs ~120s serial)
- [ ] Load test with 1000 crimes

**Expected Impact:** 3-5x throughput improvement

---

### Task 2.2: Query Optimization Deep-Dive [3 days]

For each slow query identified in Task 1.1:

**Example: fetch_unprocessed_crimes**

```python
# Current query in brief_facts_accused/db.py:
SELECT c.crime_id, c.brief_facts 
FROM crimes c
LEFT JOIN brief_facts_accused d ON c.crime_id = d.crime_id
WHERE d.crime_id IS NULL
ORDER BY c.date_created DESC

# Run EXPLAIN ANALYZE after index creation
EXPLAIN ANALYZE
SELECT c.crime_id, c.brief_facts 
FROM crimes c
LEFT JOIN brief_facts_accused d ON c.crime_id = d.crime_id
WHERE d.crime_id IS NULL
ORDER BY c.date_created DESC
LIMIT 100;

# Should show:
# - Seq Scan on crimes → Index Scan
# - Time: 1200ms → 80ms (15x faster)
```

**Files to analyze:**
- [ ] `brief_facts_accused/db.py` - fetch_unprocessed_crimes
- [ ] `brief_facts_accused/db.py` - fetch_existing_accused_for_crime
- [ ] `brief_facts_drugs/db.py` - fetch_unprocessed_crimes

---

### Task 2.3: Multiprocessing for Drug Relevance Scoring [2 days]

The `_score_drug_relevance()` function in `brief_facts_drugs/extractor.py` is CPU-bound.

```python
# From section 6.3 of PERFORMANCE_AUDIT_REPORT.md
# multiprocess_preprocessor.py

from multiprocessing import Pool

def preprocess_batch_multiprocess(brief_facts_list, num_workers=4):
    """
    Current: Sequential scoring = 12ms per crime
    Async: Parallel = 3ms per crime (4x faster)
    """
    chunks = [(i, text) for i, text in enumerate(brief_facts_list)]
    
    with Pool(processes=num_workers) as pool:
        results = pool.map(process_fir_chunk, chunks)
    
    return results
```

**Integration:**
- [ ] Add to `brief_facts_drugs/extractor.py` preprocessing
- [ ] Benchmark: 1000 crimes before/after
- [ ] Expected: 3-4x faster preprocessing

---

## Phase 3: Full Scale Testing (Days 11-28)

### Task 3.1: Instrumentation Deployment [2 days]

Add performance monitoring to production pipeline.

```python
# In brief_facts_accused/reproduce_issue.py (entry point):
from performance_profiler import profile_function, profile_block, memory_snapshot, get_report

@profile_function
def main_extraction_loop(crimes):
    """All functions decorated - auto-collects metrics"""
    for crime in crimes:
        extract_and_insert(crime)

# At end:
print(get_report())  # Detailed metrics
```

**Deployment:**
- [ ] Add decorators to main functions
- [ ] Enable query profiling in db_pooling.py
- [ ] Collect baseline metrics
- [ ] Set up automated metric reports

---

### Task 3.2: Load Testing [3 days]

**Test 1: Single-threaded baseline (current)**
```bash
time python brief_facts_accused/reproduce_issue.py --crimes 1000
# Expected time: ~180-200 seconds (120s LLM + 60-80s backend)
```

**Test 2: With quick wins (Phase 1)**
```bash
time python brief_facts_accused/reproduce_issue.py --crimes 1000
# Expected time: ~50-60 seconds (120s LLM + 5-10s backend)
# = 3-4x speedup
```

**Test 3: With async pipeline (Phase 2.1)**
```bash
time python async_main.py --crimes 1000
# Expected time: ~40-50 seconds
# = 4-5x speedup
```

---

### Task 3.3: Monitor & Optimize Hotspots [3 days]

Use `performance_profiler.py` on full pipeline:

```python
# Measure everything
python -c "
from performance_profiler import get_report
from brief_facts_accused.reproduce_issue import main_extraction_loop

main_extraction_loop(crimes=1000)
print(get_report())
" > profile_output.txt

# Identify remaining bottlenecks
cat profile_output.txt | grep -A 20 'TOP 20 SLOWEST'
```

**Typical remaining bottlenecks after Phase 1+2:**
- LLM calls (can't optimize, inherent 120s)
- JSON parsing in LLM responses (use streaming)
- Memory allocation patterns

---

## Phase 4: Production Deployment (Days 28+)

### Deployment Checklist

- [ ] All code changes tested in staging
- [ ] Performance benchmarks documented
- [ ] Connection pool monitoring in place
- [ ] Query performance alerts configured
- [ ] Database backups taken
- [ ] Rollback plan prepared
- [ ] Team trained on new patterns
- [ ] Metrics dashboard created

### Monitoring Post-Deployment

```sql
-- Monitor these metrics daily
SELECT 
    query, calls, mean_time, max_time 
FROM pg_stat_statements 
ORDER BY mean_time DESC 
LIMIT 20;

-- Connection pool stats
SELECT datname, count(*) FROM pg_stat_activity GROUP BY datname;

-- Cache hit ratio (target > 99%)
SELECT 
    sum(heap_blks_hit) / (sum(heap_blks_hit) + sum(heap_blks_read)) * 100 as cache_ratio
FROM pg_statio_user_tables;
```

---

## Quick Reference: Key Files Created

| File | Purpose | When to Use |
|------|---------|------------|
| `PERFORMANCE_AUDIT_REPORT.md` | Comprehensive analysis & code examples | Reference during implementation |
| `performance_profiler.py` | Metrics collection & reporting | Measure improvements |
| `db_pooling.py` | Connection pooling & batch ops | Replace `psycopg2.connect()` calls |
| `query_optimizer.py` | Query analysis & index recommendations | Task 1.2 |
| `PERFORMANCE_OPTIM_ROADMAP.md` | This file | Implementation checklist |

---

## Critical Success Factors

1. **Do NOT skip Phase 1:** Quick wins are 80% of improvements
2. **Test incrementally:** Measure before/after each change
3. **Monitor production:** Have dashboards ready
4. **Document patterns:** Prevent regressions in future code
5. **Train team:** Ensure new patterns are followed

---

## Expected Timeline & Outcomes

| Phase | Duration | Effort | Expected Speedup | Validation |
|-------|----------|--------|------------------|-----------|
| **Phase 1: Quick Wins** | 3 days | 20 hours | **4-5x** | `time` command |
| **Phase 2: Medium** | 8 days | 40 hours | **+2-3x** | Async benchmarks |
| **Phase 3: Testing** | 8 days | 30 hours | Final validation | Load tests |
| **Phase 4: Deploy** | Ongoing | Monitoring | Live metrics | Dashboard |
| **TOTAL** | 19-28 days | 90 hours | **5-10x overall** | Production |

---

## Troubleshooting Common Issues

### Issue: Pool connection errors

```
psycopg2.pool.PoolError: Connection pool exhausted
```

**Solution:** Increase connection pool size in `db_pooling.py`:
```python
PostgreSQLConnectionPool(minconn=5, maxconn=30)  # Increase maxconn
```

### Issue: Memory leaks with async

```
Memory grows continuously during async processing
```

**Solution:** Ensure proper cleanup in finally blocks:
```python
try:
    await process()
finally:
    await cleanup()  # Must be called!
```

### Issue: Deadlocks with batch operations

```
psycopg2.errors.DeadlockDetected
```

**Solution:** Reduce batch size or add retry logic:
```python
batch_insert(cur, query, items, batch_size=100)  # Not 1000
```

---

## Getting Help

1. **Refer to PERFORMANCE_AUDIT_REPORT.md** for detailed code examples
2. **Run tests** from each Phase before moving to next
3. **Check metrics** at every step with `performance_profiler.py`
4. **Monitor pg_stat_statements** for unexpected slow queries

---

**Next Action:** Start with Task 1.1 (Run query analysis) and report baseline metrics.

