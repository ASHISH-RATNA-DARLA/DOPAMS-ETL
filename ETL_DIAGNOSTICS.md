# ETL Script Diagnostics Checklist

## Quick Issues to Check in Each Script

After running the validation test, use this checklist to diagnose failures in individual scripts.

### Common Patterns That Break with Single-Record Data

**1. Unhandled Empty Result Sets**
```python
# ✗ BAD - Crashes if no records found
records = cursor.fetchall()
for rec in records:
    process(rec[0])  # IndexError if empty

# ✓ GOOD - Handles empty results
records = cursor.fetchall()
if records:
    for rec in records:
        process(rec[0])
else:
    logger.info("No records to process")
```

**2. Missing Null/Optional Field Handling**
```python
# ✗ BAD - Crashes on NULL values
value = row['phone'].strip()  # AttributeError if NULL

# ✓ GOOD - Handles NULL values
value = (row['phone'] or '').strip()
value = row.get('phone', '')
```

**3. Division by Zero / Empty Aggregations**
```python
# ✗ BAD - ZeroDivisionError with 1 record
avg = total / count

# ✓ GOOD - Safe division
avg = total / count if count > 0 else 0
aggregated = sum(values) if values else 0
```

**4. Hardcoded Record Counts or Date Ranges**
```python
# ✗ BAD - Assumes historical data
WHERE date_created > '2024-01-01'
if len(records) > 100:

# ✓ GOOD - Works with any volume
WHERE crime_id = ?  # Filters by actual need
if records:
```

**5. Missing Foreign Keys on Single Records**
```python
# ✗ BAD - FK fails if parent record is deleted
INSERT INTO accused (person_id, crime_id)

# ✓ GOOD - Check parent exists first
SELECT COUNT(*) FROM persons WHERE person_id = %s
SELECT COUNT(*) FROM crimes WHERE crime_id = %s
```

---

## Script-Specific Checks

### brief_facts_accused/extractor.py
- [ ] Handles case with 0 accused extracted
- [ ] LLM extraction gracefully skips if text is empty
- [ ] doesn't crash on NULL full_name

```bash
# Test with single crime:
cd brief_facts_accused
python3 -c "
import db
results = db.fetch_accuseds_for_crime('62aa9b9ea2d2490c539be447')
print(f'Found {len(results)} accused records')
"
```

### brief_facts_drugs/main.py
- [ ] Handles case with 0 drugs extracted
- [ ] Seizure worth standardization works with NULL values
- [ ] Unknown drug names are properly handled

```bash
# Test with single crime:
cd brief_facts_drugs
python3 -c "
import db
results = db.fetch_drugs_for_crime('62aa9b9ea2d2490c539be447')
print(f'Found {len(results)} drug records')
"
```

### etl_case_status/update_crimes.py
- [ ] Works when chargesheet is deleted
- [ ] No hardcoded status assumptions
- [ ] Handles NULL chargesheet_ids

```bash
# Check what it queries:
grep -n "FROM\|WHERE\|chargesheet" etl_case_status/update_crimes.py
```

### etl-accused/etl_accused.py
- [ ] Can handle empty persons table (if persons hasn't run yet)
- [ ] Foreign key constraint respected
- [ ] Handles NULL person_ids gracefully

### etl-ir/ir_etl.py
- [ ] Handles case with 0 interrogation reports
- [ ] Nested table inserts work with minimal parent data
- [ ] No missing accused reference crashes

### etl_arrests/etl_arrests.py
- [ ] Works with minimal persons table
- [ ] Handles NULL arrest dates gracefully
- [ ] No batch processing assumptions

---

## Database Validation Queries

Run these after each ETL step to verify data integrity:

```sql
-- After Hierarchy
SELECT COUNT(*) as ps_count FROM hierarchy WHERE ps_code = '2022057';

-- After Crimes  
SELECT COUNT(*) as crime_count FROM crimes WHERE crime_id = '62aa9b9ea2d2490c539be447';
SELECT * FROM crimes WHERE crime_id = '62aa9b9ea2d2490c539be447';

-- After Accused
SELECT COUNT(*) as accused_count FROM accused 
WHERE crime_id = '62aa9b9ea2d2490c539be447';

-- After Persons
SELECT COUNT(*) as person_count FROM persons 
WHERE person_id IN (
    SELECT DISTINCT person_id FROM accused 
    WHERE crime_id = '62aa9b9ea2d2490c539be447'
);

-- After Arrests
SELECT COUNT(*) FROM arrests WHERE crime_id = '62aa9b9ea2d2490c539be447';

-- After Chargesheets
SELECT COUNT(*) FROM chargesheets WHERE id = '0294b57b-adf2-4d2a-9aa0-2808f88452fe';

-- After Brief Facts
SELECT COUNT(*) FROM brief_facts_accused WHERE crime_id = '62aa9b9ea2d2490c539be447';
SELECT COUNT(*) FROM brief_facts_drug WHERE crime_id = '62aa9b9ea2d2490c539be447';

-- Check for orphaned records (data integrity)
SELECT COUNT(*) FROM accused WHERE crime_id NOT IN (SELECT crime_id FROM crimes);
SELECT COUNT(*) FROM arrested WHERE crime_id NOT IN (SELECT crime_id FROM crimes);
```

---

## How to Use This Checklist

1. **Run the validation test:**
   ```bash
   python3 validate_etl.py 2>&1 | tee validation_results.log
   ```

2. **For each FAILED step:**
   - Note the step number and name
   - Go to the script-specific checks section above
   - Review the common patterns section
   - Run the diagnostic queries

3. **Inspect the actual error:**
   ```bash
   # Run individual script in debug mode
   cd <etl_module>
   python3 -m pdb <script_name>.py
   # Or add verbose logging:
   LOGLEVEL=DEBUG python3 <script_name>.py
   ```

4. **Fix the script:**
   - Add null checks
   - Handle empty result sets
   - Verify foreign keys exist
   - Add error handling around aggregations

5. **Re-run validation** to confirm fix

---

## Critical Dependencies to Verify

```
hierarchy
    ↓
crimes ← all other tables depend on this
    ├→ accused ← must exist before IR/Arrests
    ├→ arrests
    ├→ interrogation_reports
    ├→ disposal
    ├→ properties
    ├→ mo_seizures
    ├→ chargesheets
    ├→ fsl_case_property
    └→ brief_facts_* (optional, depend on above)
```

**Verify critical joins work:**
```bash
# These should NOT fail if ETL order is correct:
python3 -c "
import psycopg2
conn = psycopg2.connect(...)
cur = conn.cursor()

# Test each critical join
cur.execute('SELECT * FROM arrest a JOIN crimes c ON a.crime_id = c.crime_id LIMIT 1')
print('✓ arrests → crimes OK')

cur.execute('SELECT * FROM accused a JOIN crimes c ON a.crime_id = c.crime_id LIMIT 1')  
print('✓ accused → crimes OK')

cur.execute('SELECT * FROM ir i JOIN crimes c ON i.crime_id = c.crime_id LIMIT 1')
print('✓ ir → crimes OK')
"
```
