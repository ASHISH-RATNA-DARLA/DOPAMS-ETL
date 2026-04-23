# 🚀 FULL ETL RUN - COMPLETE GUIDE
**Status:** Schema fix APPLIED ✅ | Ready to execute

---

## ⚡ QUICK START (Do These Now)

### 1. BACKUP .env
```bash
cp /data-drive/etl-process-dev/.env /data-drive/etl-process-dev/.env.backup.$(date +%Y%m%d_%H%M%S)
```

### 2. EDIT .env (3 changes needed)
```bash
nano /data-drive/etl-process-dev/.env
```
Change these lines:
```
RESTART=false          →  RESTART=true
LAST_RUN=              →  LAST_RUN=              (keep empty)
STEP_TIMEOUT_SEC=7200  →  STEP_TIMEOUT_SEC=28800
```
Save: `Ctrl+O` → `Enter` → `Ctrl+X`

### 3. RUN PRE-FLIGHT CHECK
```bash
bash /tmp/PRE_FULL_ETL_CHECKLIST.sh
```
✅ All checks must PASS

### 4. LAUNCH FULL ETL
```bash
# Terminal 1:
cd /data-drive/etl-process-dev/etl_master
python3 master_etl.py --config input.txt 2>&1 | tee /tmp/etl_full_run_$(date +%Y%m%d_%H%M%S).log

# Terminal 2 (monitoring):
tail -f /tmp/etl_full_run_*.log
```

### 5. WAIT 7-13 HOURS
- Orders 1-21: 2-4 hrs (load source APIs)
- **Order 22: 4-8 hrs** (brief_facts_ai - LLM bottleneck)
- Orders 23-27: 1 hr (files)

### 6. VALIDATE (Run these after completion)
```bash
# Check table counts:
PGPASSWORD="ADevingpjveD2rkdoast4s" psql -h 192.168.103.106 -U dev_dopamas -d dev-3 << 'SQL'
SELECT 'crimes', COUNT(*) FROM crimes
UNION ALL SELECT 'accused', COUNT(*) FROM accused
UNION ALL SELECT 'persons', COUNT(*) FROM persons
UNION ALL SELECT 'brief_facts_ai', COUNT(*) FROM brief_facts_ai
UNION ALL SELECT 'IR reports', COUNT(*) FROM interrogation_reports;
SQL

# Check coverage (CRITICAL):
PGPASSWORD="ADevingpjveD2rkdoast4s" psql -h 192.168.103.106 -U dev_dopamas -d dev-3 << 'SQL'
SELECT COUNT(*) as total_crimes,
       COUNT(DISTINCT bf.crime_id) as with_brief_facts,
       ROUND(100.0 * COUNT(DISTINCT bf.crime_id) / COUNT(*), 2) as coverage_pct
FROM crimes c LEFT JOIN brief_facts_ai bf ON c.crime_id = bf.crime_id;
SQL
```

✅ **Expected**: 7747 | 7747 | 100.00

---

## 📊 WHAT WILL HAPPEN

### Phase 1: Source Data Load (Orders 1-21)
- ✅ Hierarchy (from DB)
- ✅ Crimes: 7,747 records (from API)
- ✅ Classifications (from API)
- ✅ Case Status (computed)
- ✅ Accused: 26,658 records (from API)
- ✅ Persons: 26,587 records (from API)
- ✅ Address resolution (LLM)
- ✅ Domicile classification
- ✅ Name fixes
- ✅ Properties
- ✅ Interrogation Reports: 14,937 records
- ✅ Disposal, Arrests, MO_Seizures, Chargesheets, FSL
- ✅ Refresh Views

**→ At this point: All upstream data ready** ✅

### Phase 2: brief_facts_ai Extraction (Order 22) ← CRITICAL
- ✅ Fetch all 7,747 crimes (not yet in brief_facts_ai)
- ✅ For each crime:
  - Extract accused information from brief_facts field
  - Standardize drug details using LLM
  - Deduplicate accused records
  - Classify accused types
- ✅ Insert ~26,000 accused facts records
- ✅ Update etl_crime_processing_log: 7,747 complete

**Expected time: 4-8 hours** (LLM bottleneck)

### Phase 3: Finalization (Orders 23-27)
- ✅ Refresh materialized views
- ✅ Update file references
- ✅ Rebuild indexes

**Total: 7-13 hours**

---

## ✅ SUCCESS CRITERIA

After the full run completes:

- [x] Master ETL shows "completed successfully"
- [x] Crimes: 7,747 records
- [x] Brief_facts_ai coverage: **100%** (all 7,747 crimes)
- [x] LAST_RUN in .env auto-set to yesterday's date
- [x] No errors in /logs/
- [x] Post-validation queries show expected counts

---

## ⚠️ IF ERRORS OCCUR

### Error Before Order 22 (Crimes, Accused, Persons, etc.)
**Action:** Fix the error, then restart full pipeline:
```bash
RESTART=true
python3 master_etl.py --config input.txt
# Will wipe DB and reload everything cleanly
```

### Error at Order 22 (brief_facts_ai) - MOST LIKELY
**Option A - Continue (faster, if close to done):**
```bash
RESTART=false
LAST_RUN=2026-04-23  # Today's date
python3 master_etl.py --config input.txt
# Will process remaining crimes
```

**Option B - Restart (cleaner):**
```bash
RESTART=true
LAST_RUN=
python3 master_etl.py --config input.txt
# Full clean restart
```

### Error at Order 23+ (Views, Files, etc.)
**Action:** Just fix that step, upstream data is preserved:
```bash
python3 /data-drive/etl-process-dev/etl_refresh_views/views_refresh_sql.py
python3 /data-drive/etl-process-dev/etl-files/etl_pipeline_files/main_standalone.py
```

---

## 🔍 MONITORING DURING RUN

### Real-time log monitoring:
```bash
tail -f /tmp/etl_full_run_*.log
```

### Key things to watch for:
```
✅ "Order 1 completed: hierarchy"
✅ "Order 2 completed: crimes" (should load 7,747)
✅ "Order 5 completed: accused" (should load 26,658)
✅ "Order 22 IN PROGRESS: brief_facts_ai" ← Will take longest
✅ "Order 22 completed: brief_facts_ai"
✅ "All orders completed successfully"
```

### Warning signs:
```
❌ "ERROR at step X"
❌ "Timeout after 28800 seconds"
❌ "Database connection lost"
❌ "Ollama server not responding"
```

If you see warnings:
1. Check the error message
2. Decide: Option A (continue) or Option B (restart)
3. Fix underlying issue if needed
4. Re-run with appropriate settings

---

## 📝 CONFIGURATION SUMMARY

### Current .env settings after edit:
```bash
RESTART=true              # Full reload from source
RESTART_DATE=2022-06-01   # Start date
LAST_RUN=                 # Will auto-set after success
BATCH_SIZE=50
STEP_TIMEOUT_SEC=28800    # 8 hours (increased for brief_facts_ai)
PARALLEL_LLM_WORKERS=3    # For Ollama parallelism
LOG_LEVEL=INFO
```

### What these mean:
- **RESTART=true:** Wipes ALL data (except geo_* and drug_* lookup tables), reloads from source APIs
- **STEP_TIMEOUT_SEC=28800:** Each step gets 8 hours max (brief_facts_ai needs this)
- **PARALLEL_LLM_WORKERS=3:** Run 3 LLM requests in parallel (matches Ollama setup)

---

## 🔄 AFTER SUCCESSFUL COMPLETION

Once RESTART=true run completes successfully:

### 1. Switch to incremental mode
```bash
# Edit .env:
RESTART=false
# LAST_RUN will be maintained automatically
```

### 2. Schedule daily runs (optional)
```bash
# Add to crontab (daily at 6 AM IST):
0 6 * * * cd /data-drive/etl-process-dev/etl_master && python3 master_etl.py --config input.txt >> /tmp/daily_etl.log 2>&1
```

### 3. Daily incremental behavior
- Find crimes NOT in brief_facts_ai
- Find crimes modified since LAST_RUN
- Process only those (much faster - 1-2 hours instead of 13)
- LAST_RUN auto-updates

---

## 🛠️ TROUBLESHOOTING

### Issue: "Ollama not responding"
**Check:** Is Ollama running on 192.168.102.21:11434?
```bash
curl http://192.168.102.21:11434/api/tags
# Should return model list
```

### Issue: "Master ETL lock held"
**Check:** Is another ETL process running?
```bash
ps aux | grep master_etl.py
# If stuck, remove lock:
rm -f /tmp/master_etl.lock
```

### Issue: "Database pool exhausted"
**Check:** Too many parallel connections. Reduce:
```bash
# In .env:
ETL_PARALLEL_WORKERS=4    # Reduce from 8
ETL_DB_POOL_SIZE=10       # Reduce from 20
```

### Issue: "Disk space full"
**Check:** Free up space
```bash
df -h /data-drive
# Remove old logs: rm -rf /data-drive/etl-process-dev/etl_master/logs/old_run_*
```

---

## 📞 SUMMARY

| Item | Details |
|------|---------|
| **Status** | Ready to run |
| **Fix Applied** | Schema mismatch corrected ✅ |
| **Expected Runtime** | 7-13 hours |
| **Critical Step** | Order 22 (brief_facts_ai) |
| **Success Metric** | All 7,747 crimes with brief_facts_ai (100%) |
| **Next Errors Likely At** | Order 22 (LLM bottleneck) |

---

**START NOW:** 
1. Backup .env
2. Edit .env (3 changes)
3. Run pre-flight: `bash /tmp/PRE_FULL_ETL_CHECKLIST.sh`
4. Launch pipeline
5. Monitor and validate

*Good luck! The schema fix is in place and you're ready to go.* 🚀

