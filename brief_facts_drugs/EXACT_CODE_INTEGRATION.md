# EXACT CODE INTEGRATION POINTS
# Copy-paste ready code for extractor.py, db.py, main.py, config.py

---

## 1️⃣ UPDATE: extractor.py (Top Imports)

**Location:** Line 1-20 (imports section)  
**Action:** ADD these imports

```python
# ─────────────────────────────────────────────────────────────
# ★ NEW IMPORTS FOR KB MATCHING ★
# ─────────────────────────────────────────────────────────────
from kb_matcher_advanced import (
    DrugKBMatcherAdvanced,
    MatchResult,
    validate_commercial_quantity,
)
from extractor_integration import (
    refine_drugs_with_advanced_kb,
    apply_validation_rules,
)
```

---

## 2️⃣ UPDATE: extractor.py (extract_drug_info function)

**Location:** In extract_drug_info(), after LLM extraction and data validation  
**Action:** REPLACE post-processing section

**FIND THIS:**
```python
        # ───────────────────────────────────────────────────────────
        # POST-PROCESSING SECTION
        # ───────────────────────────────────────────────────────────
        
        collapsed = _collapse_collective_seizures(valid_drugs)
        standardized = standardize_units(collapsed)
        worth_distributed = _distribute_seizure_worth(standardized)
        commercial_checked = _apply_commercial_quantity_check(worth_distributed)
        final = deduplicate_extractions(commercial_checked)
```

**REPLACE WITH THIS:**
```python
        # ───────────────────────────────────────────────────────────
        # ★ NEW: KB FUZZY MATCHING & VALIDATION (Production-Grade) ★
        # ───────────────────────────────────────────────────────────
        
        logger.info(f"Starting advanced KB refinement for {len(valid_drugs)} entries...")
        kb_matcher = DrugKBMatcherAdvanced(drug_categories)
        
        # Stage 1-7: Fuzzy matching + validation + confidence adjustment
        refined_drugs = refine_drugs_with_advanced_kb(
            valid_drugs,
            kb_matcher,
            acts_sections=None  # Could pass crime's acts_sections if available
        )
        logger.info(f"KB refinement complete: {len(refined_drugs)} drugs")
        
        # Apply all extraction validation rules (R1-R19)
        logger.info("Applying extraction validation rules...")
        validated_drugs, rejection_stats = apply_validation_rules(refined_drugs)
        logger.info(
            f"Validation complete: {len(validated_drugs)}/{len(refined_drugs)} valid "
            f"({rejection_stats})"
        )
        
        # ───────────────────────────────────────────────────────────
        # Standard Post-Processing (corrected order)
        # ───────────────────────────────────────────────────────────
        
        collapsed = _collapse_collective_seizures(validated_drugs)
        standardized = standardize_units(collapsed)
        worth_distributed = _distribute_seizure_worth(standardized)
        commercial_checked = _apply_commercial_quantity_check(worth_distributed)
        final = deduplicate_extractions(commercial_checked)
```

---

## 3️⃣ UPDATE: db.py (Add audit logging functions)

**Location:** Add at end of file (before last function)  
**Action:** INSERT these 2 functions

```python
def log_kb_match_audit(conn, crime_id: str, drug: 'DrugExtraction'):
    """Log KB matching decision for audit trail."""
    import json
    
    meta = drug.extraction_metadata or {}
    kb_data = meta.get('kb_refinement', {})
    
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO drug_kb_match_audit 
            (crime_id, extracted_name, matched_standard_name, match_type, match_ratio, 
             is_commercial, validation_warnings, confidence_original, confidence_adjusted, 
             audit_data)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            crime_id,
            drug.raw_drug_name,
            drug.primary_drug_name,
            kb_data.get('kb_match', {}).get('match_type', 'unknown'),
            float(kb_data.get('kb_match', {}).get('match_ratio', 0)),
            drug.is_commercial,
            '|'.join(kb_data.get('warnings', [])),
            float(kb_data.get('confidence_adjustment', {}).get('original', 0)),
            float(drug.confidence_score),
            json.dumps(kb_data, default=str)
        ))


def log_drug_rejection(conn, crime_id: str, raw_name: str, 
                      reason: str, confidence: float, is_fp: bool = False):
    """Log rejected drugs for review."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO drug_extraction_rejections 
            (crime_id, raw_drug_name, rejection_reason, llm_confidence, was_false_positive)
            VALUES (%s, %s, %s, %s, %s)
        """, (crime_id, raw_name, reason, confidence, is_fp))
```

---

## 4️⃣ UPDATE: main.py (_extract_single_crime function)

**Location:** In _extract_single_crime(), validation section  
**Action:** REPLACE confidence filtering

**FIND THIS:**
```python
        valid = []
        for drug in drugs_data:
            if drug.confidence_score >= 0.50:
                valid.append(drug)
```

**REPLACE WITH THIS:**
```python
        # ★ Use validation filters instead of simple confidence check
        valid, rejection_stats = apply_validation_rules(drugs_data)
```

---

## 5️⃣ UPDATE: main.py (Add audit logging call)

**Location:** In process_crimes_parallel(), after batch insert  
**Action:** ADD audit logging

**FIND THIS:**
```python
        batch_insert_drug_facts(conn, inserts)
        logger.info(f"Batch inserted {len(inserts)} success")
```

**ADD AFTER:**
```python
        # ★ Log KB matching decisions for audit trail
        for crime_id, drugs in batch_results:
            for drug in drugs:
                try:
                    log_kb_match_audit(conn, crime_id, drug)
                except Exception as e:
                    logger.warning(f"Failed to log KB audit for {crime_id}: {e}")
```

---

## 6️⃣ UPDATE: config.py (Add KB configuration)

**Location:** End of file  
**Action:** INSERT this configuration dictionary

```python
# ─────────────────────────────────────────────────────────────
# ★ Advanced KB Matching Configuration ★
# ─────────────────────────────────────────────────────────────

KB_MATCH_CONFIG = {
    # Similarity thresholds for fuzzy matching
    'EXACT_MATCH_THRESHOLD': 0.95,
    'HIGH_CONFIDENCE_THRESHOLD': 0.82,
    'MEDIUM_CONFIDENCE_THRESHOLD': 0.72,
    'LOW_THRESHOLD': 0.60,
    
    # Confidence score adjustments
    'CONFIDENCE_BOOST_EXACT': 0.15,
    'CONFIDENCE_BOOST_HIGH': 0.08,
    'CONFIDENCE_DISCOUNT_UNCERTAIN': 0.10,
    'CONFIDENCE_DISCOUNT_SUSPICIOUS': 0.25,
    
    # Minimum confidence thresholds
    'MIN_CONFIDENCE_UNKNOWN': 0.50,
    'MIN_CONFIDENCE_PARTIAL': 0.60,
    'MIN_CONFIDENCE_CLEAR': 0.85,
    
    # Feature flags
    'ENABLE_FALSE_POSITIVE_DETECTION': True,
    'ENABLE_FORM_UNIT_VALIDATION': True,
    'ENABLE_QUANTITY_SANITY_CHECK': True,
    'ENABLE_AUDIT_LOGGING': True,
    
    # Logging level
    'LOG_LEVEL': 'INFO',
}
```

---

## 📋 Checklist Before Integration

- [ ] Copy `kb_matcher_advanced.py` to `brief_facts_drugs/` directory
- [ ] Copy `extractor_integration.py` to `brief_facts_drugs/` directory
- [ ] Copy `test_kb_matcher_advanced.py` to `brief_facts_drugs/` directory
- [ ] Run tests: `python -m pytest test_kb_matcher_advanced.py -v`
- [ ] Backup original files:
  - [ ] `cp extractor.py extractor.py.backup`
  - [ ] `cp db.py db.py.backup`
  - [ ] `cp main.py main.py.backup`
  - [ ] `cp config.py config.py.backup`
- [ ] Create database tables: `psql -d dopams_db -f deploy_migrations.sql`
- [ ] Import json in db.py: `import json` (if not already there)

---

## 🧪 Quick Test After Integration

```bash
# 1. Check imports work
python -c "from brief_facts_drugs.kb_matcher_advanced import DrugKBMatcherAdvanced; print('✅ Import OK')"

# 2. Run unit tests
cd brief_facts_drugs && python -m pytest test_kb_matcher_advanced.py::TestDrugNormalization -v

# 3. Test with single crime
python -c "
from extractor import extract_drug_info
from db import fetch_drug_categories, get_connection

conn = get_connection()
drugs = fetch_drug_categories(conn)
result = extract_drug_info('Seized 5 kg ganaj from accused', drugs)
print(f'✅ Extracted {len(result)} drugs')
for drug in result:
    print(f'  - {drug.primary_drug_name}: {drug.raw_quantity} {drug.raw_unit}')
conn.close()
"

# 4. Check audit table
psql -d dopams_db -c "SELECT COUNT(*) as total_audits FROM drug_kb_match_audit;"
```

---

## ⚠️ Common Integration Mistakes

**Mistake 1:** Forgetting import statement
```python
# ❌ WRONG
matcher = DrugKBMatcherAdvanced(drugs)  # NameError: not defined

# ✅ CORRECT
from kb_matcher_advanced import DrugKBMatcherAdvanced
matcher = DrugKBMatcherAdvanced(drugs)
```

**Mistake 2:** Wrong function call order
```python
# ❌ WRONG - validation rules called before KB matching
validated = apply_validation_rules(valid_drugs)
refined = refine_drugs_with_advanced_kb(validated, matcher)

# ✅ CORRECT - KB matching first, then validation
refined = refine_drugs_with_advanced_kb(valid_drugs, matcher)
validated, stats = apply_validation_rules(refined)
```

**Mistake 3:** Missing database migration
```python
# ❌ WRONG - tries to log but table doesn't exist
log_kb_match_audit(conn, crime_id, drug)  # Table doesn't exist!

# ✅ CORRECT - run migration first
# psql -d dopams_db -f deploy_migrations.sql
log_kb_match_audit(conn, crime_id, drug)  # Works!
```

**Mistake 4:** Incorrect matcher initialization
```python
# ❌ WRONG - passing wrong format
matcher = DrugKBMatcherAdvanced(['Ganja', 'Heroin'])  # String list

# ✅ CORRECT - pass list of dicts with required fields
drugs = [
    {'raw_name': 'Ganja', 'standard_name': 'Ganja', 'category_group': 'Cannabis'},
    {'raw_name': 'Heroin', 'standard_name': 'Heroin', 'category_group': 'Opioid'},
]
matcher = DrugKBMatcherAdvanced(drugs)
```

---

## 🚀 Integration Timeline

| Step | Time | Task |
|------|------|------|
| 1 | 5 min | Copy files to brief_facts_drugs/ |
| 2 | 10 min | Update extractor.py (imports + post-processing) |
| 3 | 5 min | Update db.py (add 2 functions) |
| 4 | 5 min | Update main.py (replace validation + add logging) |
| 5 | 5 min | Update config.py (add KB_MATCH_CONFIG) |
| 6 | 2 min | Create database tables |
| 7 | 10 min | Run tests |
| 8 | 5 min | Quick manual test |
| 9 | 30 min | Staging deployment |
| 10 | 10 min | Monitor and verify |
| **TOTAL** | **~90 min** | Full integration ready |

---

## 📊 Expected Results After Integration

**Metrics to Monitor:**

```bash
# Should see these in logs:
# ✅ "Starting advanced KB refinement for 150 entries..."
# ✅ "KB refinement complete: 145 drugs"
# ✅ "Applying extraction validation rules..."
# ✅ "Validation complete: 142/145 valid (3 rejected)"

# Check audit table:
SELECT COUNT(*) FROM drug_kb_match_audit;  # Should grow

# Check match quality:
SELECT match_type, COUNT(*) FROM drug_kb_match_audit GROUP BY match_type;
# Expected: 60% exact, 22% fuzzy_high, 8% fuzzy_medium, 10% no_match
```

---

**Integration Status:** 🟡 Ready for Copy-Paste  
**Complexity Level:** 🟢 Easy (4 files, ~30 edits total)  
**Risk Level:** 🟢 Low (with backups + tests)  
**Estimated Integration Time:** ⏱️ 90 minutes
