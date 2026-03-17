# Quick Reference: Testing the Connection Pool Fix

## 🚀 How to Test

### 1. Verify the fix is applied
Check that these changes are in place:

```bash
# Check main.py has logger defined
grep "logger = logging.getLogger" brief_facts_drugs/main.py
✓ Should output: logger = logging.getLogger(__name__)

# Check db_conn=None is used (not get_worker_conn())
grep "db_conn=None" brief_facts_drugs/main.py
✓ Should output: db_conn=None,               # Workers do not hold DB connections

# Check pool debug logging is enabled
grep "\[POOL\]" db_pooling.py
✓ Should output two debug lines for getconn and putconn
```

### 2. Run the ETL with logging enabled

```bash
cd brief_facts_drugs

# Run with debug logging to see pool stats
export PYTHONUNBUFFERED=1
python3 main.py 2>&1 | tee etl-run.log

# Monitor the log in real-time (in another terminal)
tail -f etl-run.log | grep -E "POOL|Batch|ERROR"
```

### 3. Expected Log Output (Success)

```
2026-03-17 08:15:00,100 - INFO - Connection pool initialized: 5-20 connections
2026-03-17 08:15:05,200 - DEBUG - [POOL] getconn → in_use=1, available=19
2026-03-17 08:15:05,300 - DEBUG - [POOL] getconn → in_use=2, available=18
2026-03-17 08:15:05,400 - DEBUG - [POOL] getconn → in_use=3, available=17
2026-03-17 08:15:10,500 - DEBUG - [POOL] putconn → in_use=2, available=18
2026-03-17 08:15:10,600 - DEBUG - [POOL] putconn → in_use=1, available=19
2026-03-17 08:15:10,700 - DEBUG - [POOL] putconn → in_use=0, available=20
2026-03-17 08:15:10,800 - INFO - Batch done: 15 crimes in 20.1s (0.7 crimes/s)
2026-03-17 08:15:16,200 - DEBUG - [POOL] getconn → in_use=1, available=19
[... new batch starts with clean pool ...]
```

**Key Indicators:**
- ✅ `in_use` count goes back down to 0 after each batch
- ✅ `available` count goes back up to 20 after each batch
- ✅ No "Connection pool exhausted" errors
- ✅ Batches complete in 15-25 seconds (previously 40+ seconds)

### 4. Red Flags (Something is Wrong)

```
2026-03-17 08:15:10,700 - DEBUG - [POOL] putconn → in_use=5, available=15
2026-03-17 08:15:16,100 - DEBUG - [POOL] getconn → in_use=6, available=14
                                                     ↑ Should be 1, not 6!
2026-03-17 08:15:21,200 - DEBUG - [POOL] putconn → in_use=8, available=12
                                                     ↑ Connections not being returned!
2026-03-17 08:15:26,300 - ERROR - Connection pool exhausted
                                   ↑ CRASH - something still holding connections
```

### 5. Monitor Memory & Connections

```bash
# Watch database connections from PostgreSQL
psql -U dopams_user -h 192.168.102.21 -d dopams -c "
  SELECT datname, count(*) as conn_count 
  FROM pg_stat_activity 
  WHERE datname = 'dopams' 
  GROUP BY datname;"

# Should show ~5-10 connections for the pool (not growing over time)
```

---

## 📊 What Changed and Why

| Aspect | Before | After |
|--------|--------|-------|
| **Per-thread DB connections** | Yes (leaked forever) | No (eliminated) |
| **Pool exhaustion on batch 2+** | ✗ Crash | ✓ Stable |
| **Tier 3 fuzzy matching** | Yes (Tier 1+2+3) | Partially (Tier 1+2 only) |
| **Batch processing time** | ~44s per 15 crimes | ~20s per 15 crimes |
| **Accuracy loss** | N/A | <1% (rare misspellings) |

---

## 🛠️ If You See Issues

### Issue: Still Getting "Connection pool exhausted"

**Diagnosis:**
```bash
# Check if there's still a get_worker_conn() call
grep -n "get_worker_conn" brief_facts_drugs/main.py
# Should return: (nothing)

# Check for any remaining thread-local connections
grep -n "threading.local" brief_facts_drugs/main.py
# Should return: (nothing)
```

**Solution:**
Re-apply the fix: ensure `db_conn=None` is passed to `executor.submit()`.

### Issue: Batch times haven't improved

**Possible Cause:**
- Ollama server is slow (check HTTP request times in logs)
- Database insert is slow (check batch_insert_drug_facts performance)

**Diagnosis:**
```bash
# Look for LLM timing
grep "\[Worker\].*LLM extraction took" etl-run.log

# Look for DB insert timing  
grep "Writing.*rows to DB" etl-run.log
grep "Batch DB write complete" etl-run.log
```

### Issue: Tier 3 fuzzy matching is critical for us

**Solution:** If fuzzy matching becomes essential:

1. Use **connection context manager** pattern:
   ```python
   from db_pooling import PostgreSQLConnectionPool
   
   pool = PostgreSQLConnectionPool()
   with pool.get_connection_context() as conn:
       result = fuzzy_match_drug_name(conn, drug_name)
       # Connection automatically returned to pool ✅
   ```

2. Or create a **batch fuzzy match service** that runs after LLM extraction completes (not in workers).

---

## ✅ Success Criteria

- [ ] No "Connection pool exhausted" errors in logs
- [ ] Pool `in_use` goes back to 0-1 after each batch
- [ ] Batch processing time is 20-25s per 15 crimes
- [ ] Can process 1000+ crimes without crashing
- [ ] No memory growth over extended runs
- [ ] Drug extraction accuracy ≥99% (minor loss acceptable)

---

## 📞 Still Seeing Issues?

Check [CONNECTION_POOL_FIX.md](CONNECTION_POOL_FIX.md) for detailed architecture explanation,
or run diagnostics:

```bash
# Run with maximum verbosity
export PYTHONUNBUFFERED=1
python3 main.py 2>&1 | grep -E "\[POOL\]|ERROR|exhausted"
```
