# PRODUCTION DEPLOYMENT GUIDE
# Advanced KB Matching Integration with NDPS Compliance

## Integration Steps for extractor.py

### Step 1: Import Advanced Matcher

```python
# At top of extractor.py

from kb_matcher_advanced import (
    DrugKBMatcherAdvanced, MatchResult, validate_commercial_quantity,
    COMMERCIAL_QUANTITY_NDPS
)
from extractor_integration import (
    refine_drugs_with_advanced_kb, apply_validation_rules
)
```

### Step 2: Update extract_drug_info() Function

**REPLACE:** Current post-processing section with CORRECTED ORDER + Advanced KB Matching

```python
def extract_drug_info(text: str, drug_categories: List[dict] = None) -> List[DrugExtraction]:
    """
    Extracts drug info with production-grade KB refinement and validation.
    
    Pipeline:
    1. Pre-process (filter non-drug FIRs)
    2. Token budget check
    3. LLM extraction
    4. Data validation & conversion
    5. ★ KB FUZZY MATCHING (NEW - Advanced)
    6. ★ VALIDATION RULES (NEW - All R1-R19 rules)
    7. Collapse collective seizures
    8. Standardize units
    9. Distribute seizure worth
    10. Commercial quantity check
    11. Deduplicate
    """
    if drug_categories is None:
        drug_categories = []

    # ───────────────────────────────────────────────────────────
    # Steps 1-4: Pre-processing, Token check, LLM extraction
    # ───────────────────────────────────────────────────────────
    
    filtered_text, preprocess_meta = preprocess_brief_facts(text)
    if not filtered_text or not filtered_text.strip():
        logger.info("Pre-processor filtered out ALL sections.")
        return []

    est_input_tokens = _estimate_tokens(filtered_text)
    CONTEXT_WINDOW = 16384
    PROMPT_OVERHEAD = 800
    kb_token_est = _estimate_tokens("\n".join(
        f"{c.get('raw_name','')}{c.get('standard_name','')}{c.get('category_group','')}"
        for c in drug_categories
    )) if drug_categories else 0
    available_for_input = CONTEXT_WINDOW - PROMPT_OVERHEAD - kb_token_est
    if est_input_tokens > available_for_input:
        logger.warning(
            f"Token budget tight: input ~{est_input_tokens} tokens, available ~{available_for_input}"
        )
    else:
        logger.info(f"Token budget OK: input ~{est_input_tokens}/{available_for_input}")
    
    # Format KB for LLM
    kb_lines = []
    if drug_categories:
        kb_lines.append("raw_name|standard_name|category")
        for cat in drug_categories:
            raw = str(cat.get('raw_name', '')).strip() or 'Unknown'
            std = str(cat.get('standard_name', '')).strip() or 'Unknown'
            grp = str(cat.get('category_group', '')).strip() or '-'
            kb_lines.append(f"{raw}|{std}|{grp}")
    else:
        kb_lines.append("(No KB provided)")
    
    formatted_kb = "\n".join(kb_lines)
    
    parser = JsonOutputParser(pydantic_object=CrimeReportExtraction)
    prompt = ChatPromptTemplate.from_template(EXTRACTION_PROMPT)
    
    try:
        llm = _get_thread_safe_llm()
        chain = prompt | llm | parser
        
        input_data = {"text": filtered_text, "drug_knowledge_base": formatted_kb}
        response = invoke_extraction_with_retry(chain, input_data, max_retries=1)
        
        if not response:
            logger.warning("LLM returned empty response.")
            return []
        
        drugs_data = response.get("drugs", [])
        if not drugs_data:
            logger.info(f"LLM returned 0 drugs.")
            return []
        
        logger.info(f"LLM returned {len(drugs_data)} raw drug entries.")
        
        # Validate and convert to DrugExtraction objects
        valid_drugs = []
        for d in drugs_data:
            try:
                if d.get('raw_quantity') is None: d['raw_quantity'] = 0.0
                if d.get('confidence_score') is None: d['confidence_score'] = 0.70
                if d.get('seizure_worth') is None: d['seizure_worth'] = 0.0
                if not d.get('raw_drug_name'): d['raw_drug_name'] = "Unknown"
                
                if d.get('is_commercial') is None: d['is_commercial'] = False
                if isinstance(d.get('is_commercial'), str):
                    d['is_commercial'] = d['is_commercial'].lower() in ('true', '1', 'yes')
                
                if str(d.get('raw_quantity')).lower() == "none": d['raw_quantity'] = 0.0
                if str(d.get('seizure_worth')).lower() == "none": d['seizure_worth'] = 0.0
                if d.get('raw_unit') is None: d['raw_unit'] = "Unknown"
                
                if isinstance(d.get('seizure_worth'), str):
                    try:
                        d['seizure_worth'] = float(str(d['seizure_worth']).replace(',', ''))
                    except:
                        d['seizure_worth'] = 0.0
                
                valid_scopes = {'individual', 'drug_total', 'overall_total'}
                ws = str(d.get('worth_scope', 'individual')).lower().strip()
                if ws not in valid_scopes:
                    d['worth_scope'] = 'individual'
                
                valid_drugs.append(DrugExtraction(**d))
            except Exception as e:
                logger.warning(f"Skipping invalid drug entry: {e}")
        
        # ───────────────────────────────────────────────────────────
        # Step 5: ★ KB FUZZY MATCHING (Production-Grade) ★
        # ───────────────────────────────────────────────────────────
        
        logger.info(f"Starting advanced KB refinement for {len(valid_drugs)} entries...")
        kb_matcher = DrugKBMatcherAdvanced(drug_categories)
        
        refined_drugs = refine_drugs_with_advanced_kb(
            valid_drugs,
            kb_matcher,
            acts_sections=None  # Could pass crime's acts_sections if available
        )
        logger.info(f"KB refinement complete: {len(refined_drugs)} drugs")
        
        # ───────────────────────────────────────────────────────────
        # Step 6: ★ VALIDATION RULES (All R1-R19) ★
        # ───────────────────────────────────────────────────────────
        
        logger.info("Applying extraction validation rules (R1-R19)...")
        validated_drugs, rejection_stats = apply_validation_rules(refined_drugs)
        logger.info(
            f"Validation complete: {len(validated_drugs)}/{len(refined_drugs)} valid "
            f"(rejected: {rejection_stats})"
        )
        
        # ───────────────────────────────────────────────────────────
        # Step 7-11: POST-PROCESSING (CORRECTED ORDER)
        # ───────────────────────────────────────────────────────────
        
        collapsed = _collapse_collective_seizures(validated_drugs)
        standardized = standardize_units(collapsed)
        worth_distributed = _distribute_seizure_worth(standardized)
        commercial_checked = _apply_commercial_quantity_check(worth_distributed)
        final = deduplicate_extractions(commercial_checked)
        
        logger.info(f"Final extraction result: {len(final)} drug entries")
        return final
        
    except Exception as e:
        logger.error(f"Drug extraction failed: {e}", exc_info=True)
        return []
```

---

## Database Tables Required

### New Audit Logging Table

```sql
-- Log all KB matching decisions for audit trail
CREATE TABLE drug_kb_match_audit (
    id SERIAL PRIMARY KEY,
    crime_id VARCHAR(50),
    extracted_name VARCHAR(255),
    matched_standard_name VARCHAR(255),
    match_type VARCHAR(50),
    match_ratio NUMERIC(3,2),
    is_commercial BOOLEAN,
    validation_warnings TEXT,
    confidence_original NUMERIC(3,2),
    confidence_adjusted NUMERIC(3,2),
    audit_data JSONB,  -- Full audit trail
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT fk_crime FOREIGN KEY (crime_id) REFERENCES crimes(crime_id) ON DELETE CASCADE
);

CREATE INDEX idx_drug_kb_audit_crime ON drug_kb_match_audit(crime_id);
CREATE INDEX idx_drug_kb_audit_match ON drug_kb_match_audit(match_type);
CREATE INDEX idx_drug_kb_audit_commercial ON drug_kb_match_audit(is_commercial);
CREATE INDEX idx_drug_kb_audit_created ON drug_kb_match_audit(created_at DESC);
```

### Rejection Log Table

```sql
-- Track rejected drugs for review
CREATE TABLE drug_extraction_rejections (
    id SERIAL PRIMARY KEY,
    crime_id VARCHAR(50),
    raw_drug_name VARCHAR(255),
    rejection_reason VARCHAR(255),
    llm_confidence NUMERIC(3,2),
    was_false_positive BOOLEAN,
    audit_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT fk_crime FOREIGN KEY (crime_id) REFERENCES crimes(crime_id) ON DELETE CASCADE
);

CREATE INDEX idx_drug_rejections_reason ON drug_extraction_rejections(rejection_reason);
CREATE INDEX idx_drug_rejections_fp ON drug_extraction_rejections(was_false_positive);
```

---

## Logging to Audit Tables

Add this function to db.py:

```python
def log_kb_match_audit(conn, crime_id: str, drug: 'DrugExtraction'):
    """Log KB matching decision for audit trail."""
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
            kb_data.get('kb_match', {}).get('match_ratio', 0),
            drug.is_commercial,
            '|'.join(kb_data.get('warnings', [])),
            kb_data.get('confidence_adjustment', {}).get('original', 0),
            kb_data.get('confidence_adjustment', {}).get('final', drug.confidence_score),
            json.dumps(kb_data, default=str)
        ))


def log_drug_rejection(conn, crime_id: str, raw_name: str, 
                      reason: str, confidence: float, is_fp: bool):
    """Log rejected drugs for review."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO drug_extraction_rejections 
            (crime_id, raw_drug_name, rejection_reason, llm_confidence, was_false_positive)
            VALUES (%s, %s, %s, %s, %s)
        """, (crime_id, raw_name, reason, confidence, is_fp))
```

---

## Configuration (config.py)

```python
# Advanced KB Matching Configuration
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

## Production Deployment Checklist

**Phase 1: Code Deployment**
- [ ] Copy `kb_matcher_advanced.py` to brief_facts_drugs/
- [ ] Copy `extractor_integration.py` to brief_facts_drugs/
- [ ] Update `extractor.py` extract_drug_info() function (see Step 2 above)
- [ ] Update `config.py` with KB_MATCH_CONFIG
- [ ] Update `db.py` with audit logging functions

**Phase 2: Database Setup**
- [ ] Create `drug_kb_match_audit` table
- [ ] Create `drug_extraction_rejections` table
- [ ] Create indexes as specified

**Phase 3: Testing**
- [ ] Run `test_kb_matcher.py` unit tests (see below)
- [ ] Test on 50 sample crimes with:
  - [ ] Typos in drug names (ganaj, gandja, etc.)
  - [ ] Regional variations (ganja tamaku, bhang, etc.)
  - [ ] Commercial quantity seizures
  - [ ] Plant/cultivation seizures
  - [ ] Multi-drug seizures with different quantities
- [ ] Verify audit logs are populated correctly
- [ ] Check rejection logs for false positives

**Phase 4: Staging**
- [ ] Deploy to staging environment
- [ ] Run full batch (1000+ crimes)
- [ ] Monitor logs for edge cases
- [ ] Review rejected drugs for patterns
- [ ] Tune thresholds if needed
- [ ] Performance test (throughput, memory)

**Phase 5: Production**
- [ ] Final sanitization check
- [ ] Deploy to production
- [ ] Monitor first 24 hours closely
- [ ] Check audit table growth
- [ ] Compare results with previous ETL run
- [ ] Publish results report

---

## Success Metrics

✅ **KB Match Rate:** >85% of extracted drugs matched to KB  
✅ **Commercial Quantity Detection:** 95%+ accuracy  
✅ **False Positive Rejection Rate:** <2%  
✅ **Audit Coverage:** 100% of decisions logged  
✅ **Processing Speed:** <50ms per drug entry  
✅ **Memory Usage:** <500MB for 10K drugs  

---

## Monitoring & Support

### Ready SQThe Alert Queries

```sql
-- Monitor KB matching quality
SELECT match_type, COUNT(*) as count, AVG(match_ratio) as avg_ratio
FROM drug_kb_match_audit
WHERE created_at > NOW() - INTERVAL '24 hours'
GROUP BY match_type
ORDER BY count DESC;

-- Find rejected drugs for manual review
SELECT raw_drug_name, COUNT(*) as rejections, rejection_reason
FROM drug_extraction_rejections
WHERE created_at > NOW() - INTERVAL '7 days'
GROUP BY raw_drug_name, rejection_reason
ORDER BY rejections DESC
LIMIT 20;

-- Confidence distribution before/after KB matching
SELECT 
    ROUND(confidence_original::numeric, 1) as orig_conf,
    ROUND(confidence_adjusted::numeric, 1) as adj_conf,
    COUNT(*) as count
FROM drug_kb_match_audit
WHERE created_at > NOW() - INTERVAL '24 hours'
GROUP BY ROUND(confidence_original::numeric, 1), ROUND(confidence_adjusted::numeric, 1)
ORDER BY count DESC;
```

### Troubleshooting

**Issue:** Low KB match rate (<80%)  
**Action:** Check if drug_categories table is populated with verified entries only

**Issue:** High false positive rejections  
**Action:** Lower thresholds in KB_MATCH_CONFIG, review audit_data for patterns

**Issue:** Slow processing  
**Action:** Check fuzzy matching algorithm, consider caching KB indices

