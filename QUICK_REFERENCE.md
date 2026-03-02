# DOPAMS ETL Performance Audit — Quick Reference Card

**Print this page and keep it on your desk during implementation**

---

## 🎯 THE PROBLEM (In 30 Seconds)

Your 2-minute ETL delay comes from **5 synchronous blocking patterns**, NOT the LLM:

1. **No connection pooling** — Creating 1000 DB connections = 100s overhead
2. **Single-record inserts** — 1000 individual INSERT statements = 30s overhead
3. **Missing indexes** — Sequential scans instead of index lookups = 20-40x slower
4. **Sequential processing** — One crime at a time = can't parallelize
5. **GIL contention** — Regex preprocessing in main thread

**Fix:** Phase 1 (2-3 days) = **4-5x improvement**

---

## 📊 QUICK METRICS

| Current | Target Phase 1 | Target Phase 1+2 |
|---------|----------------|------------------|
| **2 minutes per batch** | 30-40 seconds | 25-30 seconds |
| **4-5x slower** | **Baseline (4-5x)** | **5-10x faster** |

---

## 🚀 QUICK START (Today)

```bash
# 1. Run baseline (5 minutes)
python quick_start.py

# 2. Analyze DB (2 minutes)
python query_optimizer.py

# 3. Read summary (5 minutes)
# Open: EXECUTIVE_SUMMARY.md

# 4. Plan implementation (30 minutes)
# Open: IMPLEMENTATION_ROADMAP.md
```

**Save all metrics and timestamps.**

---

## 📋 PHASE 1: QUICK WINS (2-3 Days = 4-5x improvement)

### Task 1: Create Indexes [1 hour, ~20% improvement]
```bash
# Run query analyzer for recommendations
python query_optimizer.py

# Copy-paste SQL output and execute:
CREATE INDEX idx_brief_facts_accused_crime_id ON brief_facts_accused(crime_id);
CREATE INDEX idx_brief_facts_drugs_crime_id ON brief_facts_drugs(crime_id);
CREATE INDEX idx_accused_crime_id ON accused(crime_id);
```

### Task 2: Connection Pooling [4 hours, ~10-15% improvement]

**File 1:** Update `brief_facts_accused/db.py`
```python
# OLD (line 1-20):
import psycopg2
def get_db_connection():
    return psycopg2.connect(...)

# NEW (replace with):
from db_pooling import get_db_connection, return_db_connection

def fetch_unprocessed_crimes(limit=100):
    conn = get_db_connection()
    try:
        # ... your query ...
    finally:
        return_db_connection(conn)
```

**File 2:** Update `brief_facts_drugs/db.py` (same pattern)

**Verify:**
```bash
python -c "from db_pooling import PostgreSQLConnectionPool; print('✓ Pooling works')"
```

### Task 3: Batch Inserts [4 hours, ~10-20x on write speed]

**Update insert functions to use batching:**
```python
# OLD insert pattern (SLOW):
def insert_accused_facts(conn, item_data):
    with conn.cursor() as cur:
        cur.execute(INSERT_QUERY, (...))
    conn.commit()  # Commit per row!

# NEW batch pattern (FAST):
from db_pooling import batch_insert

def insert_accused_facts_batch(items):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            insert_tuples = [(item['col1'], ...) for item in items]
            batch_insert(cur, INSERT_QUERY, insert_tuples)
        conn.commit()
    finally:
        return_db_connection(conn)
```

**Update main loop to batch:**
```python
batch = []
for crime in crimes:
    extracted = extract_crime(crime)
    batch.append(extracted)
    if len(batch) >= 100:
        db.insert_accused_facts_batch(batch)
        batch = []
if batch:
    db.insert_accused_facts_batch(batch)
```

### ✅ Phase 1 Verification

```bash
# Before Phase 1
time python brief_facts_accused/reproduce_issue.py --crimes 100
# Expected: ~150-200 seconds

# After Phase 1 complete
time python brief_facts_accused/reproduce_issue.py --crimes 100
# Expected: ~35-50 seconds (4-5x faster!)
```

---

## 📈 PHASE 2: ADVANCED (5-7 Days = Additional 1-2x improvement)

**Only if Phase 1 doesn't meet targets**

### Option A: Async Pipeline
- Enables concurrent Fetch → Extract → Insert
- Use: `AsyncAccusedExtractor` (see PERFORMANCE_AUDIT_REPORT.md section 6.2)
- Expected: Additional 1-2x improvement
- Complexity: High (new async/await patterns)

### Option B: Multiprocessing for Preprocessing
- Parallelizes drug relevance scoring
- Use: `multiprocess_preprocessor` (see PERFORMANCE_AUDIT_REPORT.md section 6.3)
- Expected: 3-4x on preprocessing
- Complexity: Low

**Recommended:** Both = 5-10x total

---

## 🔍 DEBUGGING COMMON ISSUES

| Problem | Solution | Doc |
|---------|----------|-----|
| Pool errors | Increase maxconn in db_pooling.py | IMPLEMENTATION_ROADMAP.md |
| Batch slower? | Reduce batch_size to 100 instead of 1000 | db_pooling.py |
| Deadlocks | Add retry loop (see PERFORMANCE_AUDIT_REPORT.md) | Section 7 |
| Memory leak | Ensure cleanup() called in finally blocks | Section 2 |
| Query still slow | Run `python query_optimizer.py` again | Full analysis |

---

## 📊 TRACKING METRICS

**Create a spreadsheet with these columns:**

```
Date | Changes Applied | 100 Crimes Time (s) | 1000 Crimes Time (s) | Notes
-----|-----------------|-------------------|---------------------|--------
3/2  | Baseline        | 170              | (not tested)        | Initial measurement
3/3  | Indexes         | 140              |                     | 18% improvement
3/4  | Pooling         | 90               |                     | 47% improvement  
3/5  | Batch inserts   | 45               | 450                 | 73% improvement (4.5x total!)
3/8  | Async pipeline  | 35               | 350                 | 79% improvement (5.7x total!)
```

---

## 🛠️ ONE-PAGE IMPLEMENTATION CHECKLIST

### Day 1-2 (Indexes + Pooling)
```
[ ] Run quick_start.py → save metrics
[ ] Run query_optimizer.py → copy index recommendations
[ ] Create 5 indexes in PostgreSQL
[ ] Update brief_facts_accused/db.py with pooling
[ ] Update brief_facts_drugs/db.py with pooling
[ ] Verify with: python -c "from db_pooling import *"
[ ] Test extraction with --crimes 100
[ ] Measure time and compare to baseline
[ ] Document results
```

### Day 3 (Batch Inserts)
```
[ ] Copy batch_insert pattern to db.py insert functions
[ ] Update main extraction loop to batch items
[ ] Test with --crimes 100
[ ] Measure time (expect 4-5x now)
[ ] Test with --crimes 1000
[ ] Monitor memory usage
[ ] Document final Phase 1 metrics
```

### Day 4-10 (Phase 2 - Optional)
```
[ ] Decide: Async pipeline or multiprocessing first?
[ ] Copy code skeleton from PERFORMANCE_AUDIT_REPORT.md
[ ] Test with --crimes 100
[ ] Test with --crimes 1000
[ ] Full load test (10K records)
[ ] Production staging test
[ ] Measure Phase 2 improvement
```

---

## 📚 KEY DOCUMENTS

| Document | Read When | How Long |
|----------|-----------|----------|
| **EXECUTIVE_SUMMARY.md** | First (today) | 5 min |
| **IMPLEMENTATION_ROADMAP.md** | Planning phase | 30 min |
| **PERFORMANCE_AUDIT_REPORT.md** | Need code details | 60 min |
| **quick_start.py** | Get baseline | 5 min run |
| **query_optimizer.py** | Create indexes | 2 min run |
| **performance_profiler.py** | Measure progress | Ad-hoc |
| **db_pooling.py** | Implement pooling | Reference |

---

## 💡 PRO TIPS

1. **Always measure before and after** — Use performance_profiler.py
2. **Batch size matters** — Start with 100, not 1000
3. **Test incrementally** — One task at a time, measure each
4. **Save all metrics** — You'll need them for reporting
5. **Keep old code** — Easy rollback if issues arise
6. **Monitor production** — Have dashboards ready before deploy
7. **Database queries first** — Indexes + pooling = 80% of gains
8. **Async is optional** — Phase 1 alone might hit your target

---

## ⚡ QUICK COMMAND REFERENCE

```bash
# Baseline
python quick_start.py

# Analyze queries
python query_optimizer.py

# Measure with profiler
from performance_profiler import get_report
# ... run code ...
print(get_report())

# Verify pooling
python -c "from db_pooling import PostgreSQLConnectionPool; p = PostgreSQLConnectionPool(); print(p.stats())"

# Monitor PostgreSQL
psql -d dopams -c "SELECT datname, usename, count(*) FROM pg_stat_activity GROUP BY datname, usename;"

# See slow queries
psql -d dopams -c "SELECT query, calls, mean_time FROM pg_stat_statements ORDER BY mean_time DESC LIMIT 10;"
```

---

## 🎯 SUCCESS CRITERIA

**Phase 1 Done When:**
- ✅ Indexes created and validated
- ✅ Connection pooling implemented
- ✅ Batch inserts implemented
- ✅ 4-5x improvement measured
- ✅ All metrics documented
- ✅ No regressions on other operations

**Phase 2 Done When:**
- ✅ Async pipeline working OR multiprocessing enabled
- ✅ 5-10x total improvement measured
- ✅ Full load test passed (10K+ records)
- ✅ Memory stable throughout
- ✅ Ready for production

---

## 📞 QUICK SUPPORT

**If stuck on:** | **Check:**
---|---
Connection pooling | db_pooling.py (has migrate guide)
Batch inserts | IMPLEMENTATION_ROADMAP.md Task 1.4
Slow queries | python query_optimizer.py
Async code | PERFORMANCE_AUDIT_REPORT.md Section 6.2
Deadlocks | PERFORMANCE_AUDIT_REPORT.md Section 7
Memory issues | PERFORMANCE_AUDIT_REPORT.md Section 2
Measuring progress | performance_profiler.py --example

---

## 📌 PIN THIS TO YOUR DESK

**The 5-Minute Rule:** Before implementing any fix, measure with `performance_profiler.py`. After, measure again. If slower, revert immediately.

**The 100-Item Rule:** Always batch operations — if inserting items, batch in groups of 100+. Never insert individually in a loop.

**The Index Rule:** If a query uses LEFT JOIN or WHERE with frequent filters, create an index on that column.

**The Pool Rule:** Connection pooling is not optional. Initialize once, reuse always.

**The Async Rule:** Only use async if Phase 1 doesn't hit target (usually not needed).

---

**Status:** ✅ Ready for implementation  
**Start:** Run `python quick_start.py`  
**Estimate:** 2-3 days for 4-5x improvement  
**Questions:** Check PERFORMANCE_AUDIT_COMPLETE.md for full index

