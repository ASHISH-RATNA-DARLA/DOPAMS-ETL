## Issue #2: FINAL SOLUTION - Trigger Extension Preservation Fix

**Status:** PRODUCTION READY  
**File:** `migrate_trigger_preserve_extensions.sql`  
**Deployment Time:** ~30 seconds  
**Risk Level:** LOW (trigger modification, no data deletion)  

---

## 🎯 THE PROBLEM

The existing `trigger_auto_generate_file_paths` only preserves file extensions for a hardcoded list of types:
```sql
CASE WHEN lower(split_part(NEW.file_url, '.', -1)) IN ('pdf', 'jpg', 'docx', 'xlsx', ...) 
```

**Risk:** When Update Order 29 (`update_file_urls_with_extensions.py`) adds extensions to URLs, the next UPDATE operation on those files will **strip the extensions** because the trigger overwrites the URL.

**Example:**
```
Original:  /files/chargesheet/CH001
After Update: /files/chargesheet/CH001.pdf  ← Added by Order 29
After Next UPDATE: /files/chargesheet/CH001  ← LOST! Not in hardcoded list
```

---

## ✅ THE SOLUTION

The migration creates an **enhanced trigger function** that:

1. **Preserves ANY extension** using universal regex: `\.([a-zA-Z0-9\-_]+)(?:\?|#|$)`
2. **Works for both INSERT and UPDATE** operations
3. **Handles all file types** (pdf, docx, jpg, png, mp4, xlsx, zip, etc.)
4. **Non-breaking** to existing data

---

## 📋 IMPLEMENTATION STEPS

### Step 1: Connect to Database
```bash
# SSH to ETL Server
ssh dopams@192.168.103.182

# Connect to dev-2 database
psql -h 192.168.103.106 -U dev_dopamas -d dev-2 -p 5432

# Password prompt: [enter your password]
```

### Step 2: Execute Migration
```sql
-- Copy the entire migrate_trigger_preserve_extensions.sql content
-- Paste into psql terminal

-- Or execute directly from file:
\i /path/to/migrate_trigger_preserve_extensions.sql

-- Expected output:
-- NOTICE: SUCCESS: trigger_auto_generate_file_paths is created and ENABLED
-- NOTICE: Test data: X chargesheet files with .pdf extension found
```

### Step 3: Verify Success
```sql
-- Check trigger is enabled
SELECT tgname, tgenabled 
FROM pg_trigger 
WHERE tgrelid = 'public.files'::regclass;

-- Should show: trigger_auto_generate_file_paths | t (true = enabled)

-- View the updated function
\df+ auto_generate_file_paths

-- Test with sample chargesheet
SELECT file_url FROM files 
WHERE source_type = 'chargesheets' 
LIMIT 5;
```

---

## 🔄 BEHAVIOR CHANGES

### Before Migration
| Operation | Input | Output |
|-----------|-------|--------|
| INSERT chargesheet with file_url = `.../CH001.pdf` | `.../CH001.pdf` | `.../CH001` (extension stripped) |
| UPDATE chargesheet, set file_url = `.../CH001.pdf` | `.../CH001.pdf` | `.../CH001.pdf` (if .pdf in list), else stripped |
| UPDATE file with .mp4 extension | `.../FILE.mp4` | `.../FILE` (not in hardcoded list) |

### After Migration
| Operation | Input | Output |
|-----------|-------|--------|
| INSERT chargesheet with file_url = `.../CH001.pdf` | `.../CH001.pdf` | `.../CH001.pdf` ✅ PRESERVED |
| UPDATE chargesheet, set file_url = `.../CH001.pdf` | `.../CH001.pdf` | `.../CH001.pdf` ✅ PRESERVED |
| UPDATE file with .mp4 extension | `.../FILE.mp4` | `.../FILE.mp4` ✅ PRESERVED |

---

## 🔍 EXTENSION PRESERVATION LOGIC

The trigger now works with ANY extension using this regex pattern:

```regex
\.([a-zA-Z0-9\-_]+)(?:\?|#|$)
```

**Matches:**
- `.pdf` ✅
- `.docx` ✅
- `.xlsx` ✅
- `.jpg`, `.png`, `.gif` ✅
- `.mp4`, `.avi`, `.mkv` ✅
- `.zip`, `.rar`, `.7z` ✅
- `.txt`, `.csv`, `.json` ✅
- `.zip?version=2` (with query params) ✅
- `.pdf#page=1` (with anchors) ✅

---

## 📊 INTEGRATION WITH ORDER 29

**Sequence of Operations:**
```
1. ETL Order 28: Load files → insert into files table (trigger preserves any extensions provided)
2. ETL Order 29: Update file extensions → UPDATE files SET file_url = file_url || '.' || extension
   (Trigger now preserves these extensions)
3. Future Updates: Any UPDATE on files table → extensions preserved ✅
```

**No changes needed to `update_file_urls_with_extensions.py`** - it works automatically with the new trigger.

---

## 🛡️ ROLLBACK PLAN (If Needed)

The migration is fully reversible:

```sql
-- Restore previous trigger behavior (if absolutely necessary)
DROP TRIGGER trigger_auto_generate_file_paths ON public.files;
DROP FUNCTION public.auto_generate_file_paths();

-- Then re-run the original schema creation for files table trigger
-- OR restore from backup
```

**Note:** Rollback is not recommended unless critical issues occur. The new trigger is backward-compatible.

---

## ⚠️ VERIFICATION CHECKLIST

After running the migration:

- [ ] psql shows trigger is ENABLED
- [ ] `\df+ auto_generate_file_paths` shows enhanced function (contains regex pattern)
- [ ] Test UPDATE: `UPDATE files SET source_field = source_field WHERE id = '<test_id>'`
- [ ] Verify: `SELECT file_url FROM files WHERE id = '<test_id>'` shows extension preserved
- [ ] Log shows: `NOTICE: SUCCESS: trigger_auto_generate_file_paths is created and ENABLED`

---

## 📝 NEXT ACTIONS

**Immediate (After This Migration):**
1. ✅ Run this migration on dev-2 database
2. ✅ Verify trigger is enabled
3. ✅ Re-run Order 29 (`update_file_urls_with_extensions.py`)
4. ✅ Verify extensions are preserved in subsequent file updates

**Timeline:**
- Migration execution: < 1 minute
- Verification: 5 minutes
- Can be applied during low-traffic hours or maintenance window

---

## 📖 TECHNICAL DETAILS

**Function:** `auto_generate_file_paths()`  
**Trigger:** `trigger_auto_generate_file_paths` (BEFORE INSERT OR UPDATE)  
**Table:** `public.files`  
**Dependencies:** Helper functions `generate_file_path()` and `generate_file_url()`  
**Owner:** `dev_dopamas`  

**Key Operations:**
- Lines 1-40: Extension extraction with universal regex
- Lines 41-70: UPDATE operation handling (preserves old extension)
- Lines 71-90: INSERT operation handling (preserves provided extension)
- Lines 91-110: Trigger verification and testing

---

## ❓ FAQ

**Q: Will this affect existing files?**  
A: No. The trigger only acts on INSERT/UPDATE operations going forward.

**Q: Can I run it multiple times?**  
A: Yes. It uses IF EXISTS/CREATE OR REPLACE, so re-running is safe.

**Q: What about concurrent updates?**  
A: The trigger is transaction-safe and works with concurrent multi-threaded updates (like Order 29 with ThreadPoolExecutor).

**Q: Do I need to restart the database?**  
A: No. Changes take effect immediately.

**Q: Can this break existing ETL processes?**  
A: No. It's backward-compatible and only adds functionality.

---

## 📞 SUPPORT

If issues occur:
1. Check ETL logs for trigger-related messages
2. Verify trigger is enabled: `SELECT tgenabled FROM pg_trigger WHERE tgname = 'trigger_auto_generate_file_paths'`
3. Check recent file updates: `SELECT file_url FROM files WHERE file_id LIKE '%' LIMIT 10`
4. Run verification SQL again to confirm behavior

---

**Status:** Ready for deployment  
**Last Updated:** $(date)  
**Risk Assessment:** LOW - Trigger modification, no data loss
