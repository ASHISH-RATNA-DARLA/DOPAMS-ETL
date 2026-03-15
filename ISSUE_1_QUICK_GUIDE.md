## ⚡ ISSUE #1: MISSING FILES — QUICK GUIDE

**Status:** EXPECTED BEHAVIOR (Not a bug)  
**Current:** 1,218 files found (4.2% of 28,641 expected)  
**Cause:** DOPAMS API files still downloading  
**Action:** Monitor & Re-run Order 29 after downloads complete  

---

## 🚀 QUICK STEPS

### Step 1: Check Current Status (NOW)
```bash
ssh dopams@192.168.103.182
cd ~/dopams-etl-pipelines

python3 etl-files/diagnose_missing_files.py

# Look for:
# - ✓ Mount point exists
# - Total files on disk: X
# - Total in database: 28,641
# - Status: X% downloaded
```

### Step 2: Monitor Progress (Every 6-12 hours)
```bash
# Re-run diagnostic to see if count is increasing
python3 etl-files/diagnose_missing_files.py

# If growing steadily → files downloading normally
# If flat → check DOPAMS API health
```

### Step 3: When 60%+ Files Available (48-72 hours)
```bash
# Re-run Order 29
python3 brief_facts_drugs/update_file_urls_with_extensions.py

# Expected: 15,000+ file URLs get .pdf, .docx, .xlsx extensions
```

### Step 4: Verify Extensions Added (After Step 3)
```sql
-- In psql:
SELECT COUNT(*) FROM files 
WHERE file_url LIKE '%.pdf' 
   OR file_url LIKE '%.docx';

-- Should show: 15,000+ rows
```

---

## 📊 TIMELINE

| Time | Action | Expected Result |
|------|--------|-----------------|
| Now | Run diagnostic | 1,218 files (4%) on disk |
| +24h | Check again | 5,000-10,000 files (20-40%) |
| +48h | Check again | 15,000-20,000 files (50-70%) |
| +72h | Re-run Order 29 | 22,000+ files updated with extensions |

---

## ✅ VERIFICATION

```bash
# Everything OK?
python3 etl-files/diagnose_missing_files.py

# Should show:
# ✓ Mount point exists: /mnt/shared-etl-files
# ✓ Mount is writable
# [Growing file counts]
```

---

## ⚠️ WHEN TO WORRY

Tell ops/DBAs if after 72+ hours:
- File count NOT increasing
- Mount shows I/O errors
- Diagnostic shows 0% progress
- DOPAMS API logs show failures

---

## 📁 FILES

- **Diagnostic:** `etl-files/diagnose_missing_files.py`
- **Update Script:** `brief_facts_drugs/update_file_urls_with_extensions.py` (thread-safe)
- **Trigger Fix:** Applied via `migrate_trigger_preserve_extensions.sql` (Issue #2)

---

**Status:** MONITORING MODE ✅  
**Re-run Order 29:** 48-72 hours from now  
**Expected Completion:** 96 hours from now  
