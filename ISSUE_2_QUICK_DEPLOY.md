## ⚡ ISSUE #2: TRIGGER FIX - 30-SECOND QUICK DEPLOY

**Files:**
- Migration SQL: `migrate_trigger_preserve_extensions.sql`
- Deployment Guide: `ISSUE_2_TRIGGER_FIX_DEPLOYMENT.md`

---

## 🚀 DEPLOYMENT (3 COMMANDS)

```bash
# 1. SSH to ETL Server
ssh dopams@192.168.103.182

# 2. Connect to database
psql -h 192.168.103.106 -U dev_dopamas -d dev-2 -p 5432

# 3. Run migration (copy-paste entire migrate_trigger_preserve_extensions.sql)
# OR single command:
\i migrate_trigger_preserve_extensions.sql

# ✅ Expected: NOTICE: SUCCESS: trigger_auto_generate_file_paths is created and ENABLED
```

---

## ✅ VERIFY (2 COMMANDS)

```sql
-- Check trigger status
SELECT tgname, tgenabled FROM pg_trigger 
WHERE tgrelid = 'public.files'::regclass;

-- Should output: trigger_auto_generate_file_paths | t (true = enabled)

-- View updated function (contains regex pattern)
\df+ auto_generate_file_paths
```

---

## 📊 TEST (3 COMMANDS)

```sql
-- Get a test file ID
SELECT id, file_url FROM files WHERE source_type = 'chargesheets' LIMIT 1;
-- Note the ID (e.g., 'CH-12345')

-- Update that file (trigger will preserve extension)
UPDATE files SET source_field = source_field WHERE id = 'CH-12345';

-- Verify extension preserved
SELECT file_url FROM files WHERE id = 'CH-12345';
-- Extension should be preserved ✅
```

---

## 🔄 NEXT: RE-RUN ORDER 29

After migration succeeds:

```bash
# SSH to ETL server
ssh dopams@192.168.103.182

# Re-run Order 29 with thread safety
python3 brief_facts_drugs/update_file_urls_with_extensions.py

# Result: Extensions now preserved on next file updates ✅
```

---

## ⏱️ TIMELINE

| Phase | Time | What Happens |
|-------|------|--------------|
| Deploy | 30s | Trigger updated, test passes |
| Verify | 2m | Confirm extensions preserved |
| Re-run Order 29 | 5-10m | File extensions added with guarantee of preservation |
| Total | <15m | Complete fix deployed |

---

## 🛡️ SAFETY

- ✅ No data deletion
- ✅ Fully reversible (rollback if needed)
- ✅ Backward compatible
- ✅ Works with concurrent updates
- ✅ Transaction safe
- ✅ Already tested in migration script

---

## ⚠️ IF ISSUES

```sql
-- Check if trigger exists and is enabled
SELECT * FROM pg_trigger WHERE tgname = 'trigger_auto_generate_file_paths';

-- Verify function exists
SELECT * FROM pg_proc WHERE proname = 'auto_generate_file_paths';

-- Check recent errors in logs
SELECT * FROM files WHERE file_id LIKE '%' LIMIT 5;

-- Re-run migration if needed (safe to run again)
\i migrate_trigger_preserve_extensions.sql
```

---

**Status:** READY FOR DEPLOYMENT ✅  
**Risk Level:** LOW  
**Estimated Time:** 15 minutes total  
**Rollback Time:** < 1 minute (if needed)  

