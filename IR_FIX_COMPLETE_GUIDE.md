## INTERROGATION REPORTS (IR) ETL FIX & ENHANCEMENT - COMPLETE GUIDE

---

## 📋 EXECUTIVE SUMMARY

This guide provides a complete fix for the Interrogation Reports ETL pipeline to:
- **Add 9 missing API fields** (currently not persisted in database)
- **Fix 7 known issues** (data quality and consistency problems)
- **Ensure 100% data lossless mapping** from API → ETL → DB
- **Maintain backward compatibility** and production safety

---

## 🎯 SCOPE: What's Being Fixed

### ❌ **9 Missing API Fields** (Currently Not Stored)
1. **INDULGANCE_BEFORE_OFFENCE** (array) → New table: `ir_indulgance_before_offence`
2. **PROPERTY_DISPOSAL** (array) → New table: `ir_property_disposal`
3. **REGULARIZATION_OF_TRANSIT_WARRANTS** (array) → New table: `ir_regularization_transit_warrants`
4. **EXECUTION_OF_NBW** (array) → New table: `ir_execution_of_nbw`
5. **PENDING_NBW** (array) → New table: `ir_pending_nbw`
6. **SURETIES** (array) → New table: `ir_sureties`
7. **JAIL_SENTENCE** (array) → New table: `ir_jail_sentence`
8. **NEW_GANG_FORMATION** (array) → New table: `ir_new_gang_formation`
9. **CONVICTION_ACQUITTAL** (array) → New table: `ir_conviction_acquittal`

### ⚠️ **7 Known Issues Fixed**

| # | Issue | Old Behavior | New Behavior | File |
|---|-------|---|---|---|
| 1 | Field mismatch | Only `PURCHASE_AMOUN_IN_INR` | Support both variants | ETL |
| 2 | Text truncation | PREVIOUS_OFFENCES fields truncated to 100 chars | No truncation (TEXT fields) | ETL |
| 3 | Date parsing | Used `parse_timestamp()` for DATE fields | Use `parse_date()` for DATE fields | ETL |
| 4 | Timezone handling | Dropped timezone info | Normalize to UTC consistently | ETL |
| 5 | Boolean defaults | Forced FALSE for unknown values | Use NULL for unknown values | Schema + ETL |
| 6 | Update detection | Skipped if DATE_MODIFIED missing | Fallback: record hash comparison | ETL |
| 7 | Partial failures | Silent skips on child insert failure | Log and continue with atomicity | ETL |

---

## 🛠️ DEPLOYMENT STEPS

### **PHASE 1: Apply Schema Changes** (Production Safe)

All schema changes use:
- `CREATE TABLE IF NOT EXISTS` - No errors if tables exist  
- `ADD COLUMN IF NOT EXISTS` - Idempotent, safe to rerun

**Copy-paste the entire SQL script:**

```sql
-- File: INTERROGATION_REPORTS_FIX.sql
-- Execute as-is in your production database
-- Safe to re-run multiple times (idempotent)
psql -U <user> -d <database> -f INTERROGATION_REPORTS_FIX.sql
```

**Expected output:**
- 9 new tables created
- 4 new columns added to `ir_previous_offences_confessed`
- 2 composite indexes created
- 2 validation views created (for data quality checks)

**Verification (after schema changes):**
```sql
-- Check new tables exist
SELECT table_name FROM information_schema.tables 
WHERE table_name LIKE 'ir_%' AND table_schema = 'public'
ORDER BY table_name;

-- Expected count: 16 existing + 9 new = 25 tables
```

---

### **PHASE 2: Deploy Enhanced ETL Code**

1. **Backup existing ETL:**
   ```bash
   cp etl-ir/ir_etl.py etl-ir/ir_etl.py.backup.$(date +%Y%m%d)
   ```

2. **Deploy enhanced version:**
   ```bash
   # Option A: Replace existing ETL (simpler)
   cp etl-ir/ir_etl_enhanced.py etl-ir/ir_etl.py
   
   # Option B: Run in parallel (safer)
   python3 etl-ir/ir_etl_enhanced.py --test-table-prefix "ir_enhanced_"
   ```

3. **Test run (recommended before prod):**
   ```bash
   # Single day test run
   python3 -c "
   from etl-ir.ir_etl import InterrogationReportsETL
   etl = InterrogationReportsETL()
   etl.run()  # Will fetch yesterday's data
   "
   ```

---

### **PHASE 3: Data Migration (for existing records)**

**Option 1: Full Resync** (Recommended)
```bash
# Clear pending FK table and reprocess all records
sqlite> --Clear and resync
psql -c "DELETE FROM ir_pending_fk"
psql -c "UPDATE interrogation_reports SET date_modified = DATE_CREATED - INTERVAL '1 day' 
         WHERE date_created > '2022-01-01'" # Force reprocessing

# Then run ETL (will reprocess all updated records)
python3 etl-ir/ir_etl.py
```

**Option 2: Incremental** (For large datasets)
```bash
# Only reprocess records after a cutoff date
psql -c "UPDATE interrogation_reports SET date_modified = CURRENT_TIMESTAMP 
         WHERE date_modified > '2025-01-01'" # Last 3 months

python3 etl-ir/ir_etl.py  # Will pick up updated records
```

---

## 📊 VALIDATION & TESTING

### **Test Scenario 1: Full Payload with All New Fields**

```python
test_payload = {
    "INTERROGATION_REPORT_ID": "test_ir_001",
    "CRIME_ID": "test_crime_001",
    "PERSON_ID": "test_person_001",
    
    # Existing fields (should still work)
    "PHYSICAL_FEATURES": {...},
    "TYPES_OF_DRUGS": [{
        "TYPE_OF_DRUG": "Ganja",
        "PURCHASE_AMOUN_IN_INR": "1488000",  # Old typo field
        # OR:NEW
        "PURCHASE_AMOUNT_IN_INR": "1488000"  # New correct field
    }],
    
    # NEW FIELDS - 9 missing fields
    "INDULGANCE_BEFORE_OFFENCE": ["Alcohol", "Drugs"],
    "PROPERTY_DISPOSAL": [{
        "MODE_OF_DISPOSAL": "Sold",
        "BUYER_NAME": "John Doe",
        "SOLD_AMOUNT_IN_INR": "50000",
        "LOCATION_OF_DISPOSAL": "Mumbai",
        "DATE_OF_DISPOSAL": "2025-01-15",
        "REMARKS": "Property sold for settlement"
    }],
    "REGULARIZATION_OF_TRANSIT_WARRANTS": [{
        "WARRANT_NUMBER": "TW-2025-001",
        "WARRANT_TYPE": "Transit",
        "ISSUED_DATE": "2025-01-10",
        "JURISDICTION_PS": "Mumbai PS",
        "CRIME_NUM": "FIR-2025-001",
        "STATUS": "Executed",
        "REMARKS": "Warrant executed successfully"
    }],
    "EXECUTION_OF_NBW": [{...}],
    "PENDING_NBW": [{...}],
    "SURETIES": [{...}],
    "JAIL_SENTENCE": [{...}],
    "NEW_GANG_FORMATION": [{...}],
    "CONVICTION_ACQUITTAL": [{...}]
}
```

**Expected Results:**
- Main record: `interrogation_reports` - 1 row
- Child records: All 9 new tables populated with corresponding array data
- No data loss
- All NULL fields allowed (not forced to FALSE)

---

### **Test Scenario 2: Partial Payload (Missing Some New Fields)**

```python
test_payload = {
    "INTERROGATION_REPORT_ID": "test_ir_002",
    "CRIME_ID": "test_crime_001",
    
    # Only some new fields present
    "INDULGANCE_BEFORE_OFFENCE": ["Chewing Pan"],
    "PROPERTY_DISPOSAL": [],  # Empty array
    # Other new fields omitted entirely
}
```

**Expected Results:**
- Records inserted successfully
- Empty arrays: Child tables not populated (safe)
- Omitted fields: Silently skipped (no error)
- No data loss for present fields

---

### **Test Scenario 3: Missing DATE_MODIFIED**

```python
test_payload = {
    "INTERROGATION_REPORT_ID": "test_ir_003",
    "CRIME_ID": "test_crime_001",
    "DATE_CREATED": "2025-01-15T10:30:00Z",
    # DATE_MODIFIED omitted
}
```

**Expected Behavior:**
1. First run: Record inserted
2. Second run: Update detection uses hash comparison (fallback)
3. If content changed: Record updated
4. If content unchanged: Record marked as "no change"

**Validation Query:**
```sql
SELECT 
    interrogation_report_id,
    date_created,
    date_modified,
    CASE WHEN date_modified IS NULL THEN 'Hash-based update' ELSE 'Timestamp-based' END as update_method
FROM interrogation_reports
WHERE interrogation_report_id LIKE 'test_%'
ORDER BY interrogation_report_id;
```

---

### **Test Scenario 4: Field Name Variants (PURCHASE_AMOUN_IN_INR vs PURCHASE_AMOUNT_IN_INR)**

```python
# Both should work
payload_variant_1 = {
    "TYPES_OF_DRUGS": [{
        "PURCHASE_AMOUN_IN_INR": "100000"  # API typo
    }]
}

payload_variant_2 = {
    "TYPES_OF_DRUGS": [{
        "PURCHASE_AMOUNT_IN_INR": "100000"  # Correct spelling
    }]
}
```

**Expected Result:**
- Both variants map to same DB column: `purchase_amount_in_inr`
- No data loss
- Backward compatible with existing API payloads

**Validation Query:**
```sql
SELECT 
    id,
    purchase_amount_in_inr,
    mode_of_payment
FROM ir_types_of_drugs
WHERE interrogation_report_id = 'test_ir_001'
ORDER BY id;
```

---

### **Test Scenario 5: Failure in Dependent Insert**

**Scenario:** Child table insert fails, but main record should be saved

```python
# Simulate by providing invalid surety data
test_payload = {
    "INTERROGATION_REPORT_ID": "test_ir_005",
    "CRIME_ID": "test_crime_001",
    "SURETIES": [{
        "SURETY_NAME": "X" * 1000,  # Exceeds field limit
        "PHONE_NUMBER": "INVALID"
    }]
}
```

**Expected Behavior:**
1. Main record: `interrogation_reports` - **INSERTED** (atomic operation)
2. Child record: `ir_sureties` - **SKIPPED** (with warning log)
3. Other child tables: Processed normally
4. ETL continues
5. Error logged for manual review

**Validation Query:**
```sql
-- Verify main record exists
SELECT * FROM interrogation_reports WHERE interrogation_report_id = 'test_ir_005';

-- Check child table status
SELECT * FROM ir_sureties WHERE interrogation_report_id = 'test_ir_005';  -- May be empty

-- Check ETL logs
tail -f etl.log | grep "test_ir_005"
```

---

## 🔍 VALIDATION QUERIES

### **1. Field Persistence Check**
```sql
-- View: ir_field_persistence_check
-- Shows which API fields are being persisted and how many records have data

SELECT * FROM public.ir_field_persistence_check
ORDER BY api_field;

-- Expected output (after running ETL with new fields):
-- INDULGANCE_BEFORE_OFFENCE | ir_indulgance_before_offence | X records | Y non-null
-- PROPERTY_DISPOSAL | ir_property_disposal | X records | Y non-null
-- ... etc ...
```

### **2. Data Coverage for All Child Tables**
```sql
-- View: ir_child_table_coverage
-- Shows how many IR records have data in each related table

SELECT * FROM public.ir_child_table_coverage
ORDER BY array_field;

-- Example output:
-- REGULAR_HABITS | 150 | 450  (150 IR records, 450 habit entries)
-- TYPES_OF_DRUGS | 200 | 250
-- INDULGANCE_BEFORE_OFFENCE | 0 | 0  (Empty if no data yet)
```

### **3. Completeness Check**
```sql
-- Find IR records with missing main fields (data quality check)

SELECT 
    ir.interrogation_report_id,
    ir.crime_id,
    ir.person_id,
    COUNT(DISTINCT CASE WHEN ir.is_in_jail IS NULL AND ir.is_on_bail IS NULL 
                          AND ir.is_absconding IS NULL THEN 1 END) as present_whereabouts_missing
FROM interrogation_reports ir
WHERE 
    ir.date_created > NOW() - INTERVAL '30 days'
    AND (
        ir.crime_id IS NULL
        OR ir.person_id IS NULL
        OR (ir.is_in_jail IS NULL AND ir.is_on_bail IS NULL 
            AND ir.is_absconding IS NULL AND ir.is_rehabilitated IS NULL 
            AND ir.is_facing_trial IS NULL AND ir.is_dead IS NULL 
            AND ir.is_normal_life IS NULL)
    )
GROUP BY ir.interrogation_report_id, ir.crime_id, ir.person_id
ORDER BY ir.date_created DESC
LIMIT 20;
```

### **4. New Fields Population Check**
```sql
-- After running enhanced ETL, check if new fields are being populated

SELECT 
    'INDULGANCE_BEFORE_OFFENCE' AS field,
    COUNT(DISTINCT ib.interrogation_report_id) AS ir_count,
    COUNT(*) AS total_entries,
    ROUND(100.0 * COUNT(*) / COUNT(DISTINCT ib.interrogation_report_id), 2) AS avg_entries_per_ir
FROM ir_indulgance_before_offence ib
UNION ALL
SELECT 'PROPERTY_DISPOSAL', COUNT(DISTINCT ipd.interrogation_report_id), COUNT(*), 
    ROUND(100.0 * COUNT(*) / COUNT(DISTINCT ipd.interrogation_report_id), 2)
FROM ir_property_disposal ipd
UNION ALL
SELECT 'REGULARIZATION_TRANSIT_WARRANTS', COUNT(DISTINCT irtw.interrogation_report_id), COUNT(*),
    ROUND(100.0 * COUNT(*) / COUNT(DISTINCT irtw.interrogation_report_id), 2)
FROM ir_regularization_transit_warrants irtw
UNION ALL
SELECT 'EXECUTION_OF_NBW', COUNT(DISTINCT ien.interrogation_report_id), COUNT(*),
    ROUND(100.0 * COUNT(*) / COUNT(DISTINCT ien.interrogation_report_id), 2)
FROM ir_execution_of_nbw ien
UNION ALL
SELECT 'PENDING_NBW', COUNT(DISTINCT ipn.interrogation_report_id), COUNT(*),
    ROUND(100.0 * COUNT(*) / COUNT(DISTINCT ipn.interrogation_report_id), 2)
FROM ir_pending_nbw ipn
UNION ALL
SELECT 'SURETIES', COUNT(DISTINCT ise.interrogation_report_id), COUNT(*),
    ROUND(100.0 * COUNT(*) / COUNT(DISTINCT ise.interrogation_report_id), 2)
FROM ir_sureties ise
UNION ALL
SELECT 'JAIL_SENTENCE', COUNT(DISTINCT ijs.interrogation_report_id), COUNT(*),
    ROUND(100.0 * COUNT(*) / COUNT(DISTINCT ijs.interrogation_report_id), 2)
FROM ir_jail_sentence ijs
UNION ALL
SELECT 'NEW_GANG_FORMATION', COUNT(DISTINCT ingf.interrogation_report_id), COUNT(*),
    ROUND(100.0 * COUNT(*) / COUNT(DISTINCT ingf.interrogation_report_id), 2)
FROM ir_new_gang_formation ingf
UNION ALL
SELECT 'CONVICTION_ACQUITTAL', COUNT(DISTINCT ica.interrogation_report_id), COUNT(*),
    ROUND(100.0 * COUNT(*) / COUNT(DISTINCT ica.interrogation_report_id), 2)
FROM ir_conviction_acquittal ica
ORDER BY ir_count DESC;
```

### **5. Boolean Field NULL Check** (After Fix Applied)
```sql
-- Verify boolean fields use NULL for unknown (not FALSE)

SELECT 
    COUNT(*) as total_records,
    COUNT(CASE WHEN is_in_jail = FALSE THEN 1 END) as in_jail_false,
    COUNT(CASE WHEN is_in_jail = TRUE THEN 1 END) as in_jail_true,
    COUNT(CASE WHEN is_in_jail IS NULL THEN 1 END) as in_jail_null,
    COUNT(CASE WHEN is_on_bail = FALSE THEN 1 END) as on_bail_false,
    COUNT(CASE WHEN is_on_bail = TRUE THEN 1 END) as on_bail_true,
    COUNT(CASE WHEN is_on_bail IS NULL THEN 1 END) as on_bail_null
FROM interrogation_reports;

-- Expected (after migration):
-- Most records should have NULL, not FALSE
-- Existing TRUE values should remain unchanged
```

### **6. Date Field Correctness Check** (DATE vs TIMESTAMP)
```sql
-- Verify DATE fields are dates and TIMESTAMP fields are timestamps

SELECT 
    interrogation_report_id,
    on_bail.date_of_bail,
    typeof(on_bail.date_of_bail) as bail_date_type,  -- Should be 'date'
    date_created,
    typeof(date_created) as created_type,  -- Should be 'timestamp'
    date_modified,
    typeof(date_modified) as modified_type  -- Should be 'timestamp'
FROM interrogation_reports
LIMIT 5;
```

---

## 📈 MIGRATION PLAN FOR PRODUCTION

### **Pre-Migration Checklist**
- [ ] Backup database: `pg_dump dopams_db > dopams_backup_$(date +%Y%m%d_%H%M%S).sql`
- [ ] Notify stakeholders of ETL changes
- [ ] Schedule maintenance window (1-2 hours)
- [ ] Test schema changes on staging environment
- [ ] Test ETL on staging with sample data

### **Day 1: Schema Deployment (Non-Breaking)**
```bash
# 1. Apply schema changes (idempotent, safe)
psql -U dopams -d dopams_production -f INTERROGATION_REPORTS_FIX.sql

# 2. Run validation queries
psql -c "SELECT COUNT(*) FROM ir_indulgance_before_offence;"  # Should be 0
psql -c "SELECT COUNT(*) FROM ir_property_disposal;"  # Should be 0

# 3. Verify no existing data lost
psql -c "SELECT COUNT(*) FROM interrogation_reports;"  # Should match before migration
```

### **Day 2: ETL Deployment**
```bash
# 1. Run enhanced ETL in test mode (if available)
python3 etl-ir/ir_etl_enhanced.py --dry-run

# 2. Deploy new ETL
cp etl-ir/ir_etl_enhanced.py etl-ir/ir_etl.py

# 3. Run single day test
python3 etl-ir/ir_etl.py  # Will fetch yesterday's data

# 4. Validate results
psql -c "SELECT * FROM ir_field_persistence_check"
```

### **Day 3+: Monitor & Verify**
```bash
# Daily ETL runs will populate new fields
# Monitor logs for any errors

tail -f etl.log | grep -E "(ERROR|INDULGANCE|PROPERTY_DISPOSAL)"

# Run weekly validation
psql -c "SELECT * FROM ir_child_table_coverage"
```

---

## 🔄 ROLLBACK PLAN (If Needed)

**Scenario 1: Schema rollback (easy)**
```sql
-- Drop new tables (keep existing data intact)
DROP TABLE IF EXISTS ir_indulgance_before_offence CASCADE;
DROP TABLE IF EXISTS ir_property_disposal CASCADE;
DROP TABLE IF EXISTS ir_regularization_transit_warrants CASCADE;
DROP TABLE IF EXISTS ir_execution_of_nbw CASCADE;
DROP TABLE IF EXISTS ir_pending_nbw CASCADE;
DROP TABLE IF EXISTS ir_sureties CASCADE;
DROP TABLE IF EXISTS ir_jail_sentence CASCADE;
DROP TABLE IF EXISTS ir_new_gang_formation CASCADE;
DROP TABLE IF EXISTS ir_conviction_acquittal CASCADE;

-- Revert boolean columns
ALTER TABLE interrogation_reports 
ALTER COLUMN is_in_jail SET DEFAULT FALSE,
ALTER COLUMN is_on_bail SET DEFAULT FALSE,
... etc;
```

**Scenario 2: ETL rollback (easy)**
```bash
# Restore previous ETL version
cp etl-ir/ir_etl.py.backup.YYYYMMDD etl-ir/ir_etl.py

# Resume with old ETL
python3 etl-ir/ir_etl.py
```

---

## 📝 COPY-PASTABLE SQL QUERIES

### **Quick Start: Apply All Schema Changes**
```sql
-- Copy entire INTERROGATION_REPORTS_FIX.sql and execute
-- Or run this command:
psql -U <user> -d <database> -f INTERROGATION_REPORTS_FIX.sql
```

### **Verify Schema Changes**
```sql
-- Count tables
SELECT COUNT(*) FROM information_schema.tables 
WHERE schema_name = 'public' AND table_name LIKE 'ir_%';
-- Expected: 25 (16 existing + 9 new)

-- Check new columns added
SELECT column_name FROM information_schema.columns 
WHERE table_name = 'ir_previous_offences_confessed'
ORDER BY column_name;
-- Expected: 18 columns (14 original + 4 new)

-- Verify no data loss
SELECT COUNT(*) as ir_records FROM interrogation_reports;
-- Should match pre-migration count
```

### **Monitor New Fields Population**
```sql
-- Check if enhanced ETL is working
SELECT COUNT(DISTINCT interrogation_report_id) as ir_records_with_indulgance
FROM ir_indulgance_before_offence;

SELECT COUNT(DISTINCT interrogation_report_id) as ir_records_with_disposal
FROM ir_property_disposal;

-- If both > 0, ETL is populating new fields successfully
```

---

## 🚨 TROUBLESHOOTING

### **Issue: "Table already exists" error**
```
ERROR: relation "ir_indulgance_before_offence" already exists
```
**Solution:** Schema changes use `IF NOT EXISTS` - this is safe, just re-run the script

### **Issue: New fields not being populated**
```
SELECT COUNT(*) FROM ir_indulgance_before_offence;  -- Returns 0
```
**Solution:** 
1. Verify enhanced ETL is deployed: `ps aux | grep ir_etl.py`
2. Check ETL logs for errors: `tail -f etl.log | grep INDULGANCE`
3. Manually trigger reprocessing: Update a record's DATE_MODIFIED in DB

### **Issue: Boolean fields showing FALSE instead of NULL**
```sql
SELECT COUNT(*) FROM interrogation_reports WHERE is_in_jail = FALSE;  -- High count
```
**Solution:**
1. Run migration query manually:
```sql
UPDATE interrogation_reports
SET is_in_jail = NULL WHERE is_in_jail = FALSE;
```

---

## 📞 SUPPORT & DOCUMENTATION

**Files Provided:**
1. ✅ `INTERROGATION_REPORTS_FIX.sql` - Schema changes (copy-paste ready)
2. ✅ `ir_etl_enhanced.py` - Enhanced ETL code (drop-in replacement)
3. ✅ This guide - Complete deployment & testing handbook

**Key Links:**
- API Endpoint: `http://103.164.200.184:3000/api/DOPAMS/interrogation-reports/v1/`
- Database: PostgreSQL 16.11
- ETL Config: `config.py`

---

## ✅ SUCCESS CRITERIA

After implementation, verify:

| Criteria | Query | Expected Result |
|----------|-------|---|
| No data loss | `SELECT COUNT(*) FROM interrogation_reports` | Same as before |
| New tables created | `SELECT COUNT(*) FROM ir_indulgance_before_offence` | Accessible table |
| NULL handling | `SELECT COUNT(*) WHERE is_in_jail IS NULL` | Increased count |
| Date parsing fixed | `SELECT date_of_bail FROM interrogation_reports LIMIT 1` | DATE type |
| 9 fields mapped | `SELECT * FROM ir_field_persistence_check` | 9 new fields listed |
| Zero errors | `SELECT COUNT(errors) FROM etl_logs` | 0 errors in new run |

---

**Last Updated:** 2026-04-09  
**Version:** 1.0 (Enhanced with 9 new fields + 7 fixes)  
**Status:** Production Ready ✅
