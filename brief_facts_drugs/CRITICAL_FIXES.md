# ETL Drug Extraction - Critical Fixes (Code-Ready)

## ISSUE #1: Accused ID Reference Broken

### Problem
LLM extracts accused_id (A1, A2, A3) but code throws it away and sets NULL.

### Current Code (BROKEN)
**File:** [db.py](db.py#L56-L66)
```python
def _prepare_insert_values(crime_id, drug_data):
    metadata = drug_data.get('extraction_metadata', {})
    llm_accused_id = drug_data.get('accused_id')
    if llm_accused_id and str(llm_accused_id).strip():
        metadata['accused_ref'] = str(llm_accused_id).strip()

    return (
        crime_id,
        None,  # ❌ DISCARDS accused_id
        drug_data.get('raw_drug_name'),
        ...
    )
```

### Fixed Code
```python
def _prepare_insert_values(crime_id, drug_data):
    metadata = drug_data.get('extraction_metadata', {})
    
    # Store accused_id: if null from LLM, still store as NULL, but don't override
    llm_accused_id = drug_data.get('accused_id')
    if llm_accused_id and str(llm_accused_id).strip():
        # Sanitize: remove whitespace, handle "null"/"None" strings
        accused_id = str(llm_accused_id).strip()
        if accused_id.lower() not in ('null', 'none', ''):
            metadata['source_accused_id'] = accused_id
        else:
            accused_id = None
    else:
        accused_id = None

    return (
        crime_id,
        accused_id,  # ✅ PRESERVE accused_id (can be NULL for collective seizures)
        drug_data.get('raw_drug_name'),
        drug_data.get('raw_quantity'),
        drug_data.get('raw_unit'),
        drug_data.get('primary_drug_name'),
        drug_data.get('drug_form'),
        round(float(drug_data.get('weight_g', 0) or 0), 6) if drug_data.get('weight_g') else None,
        round(float(drug_data.get('weight_kg', 0) or 0), 6) if drug_data.get('weight_kg') else None,
        round(float(drug_data.get('volume_ml', 0) or 0), 6) if drug_data.get('volume_ml') else None,
        round(float(drug_data.get('volume_l', 0) or 0), 6) if drug_data.get('volume_l') else None,
        round(float(drug_data.get('count_total', 0) or 0), 6),
        round(float(drug_data.get('confidence_score', 0.80) or 0.80), 2),
        json.dumps(metadata),
        bool(drug_data.get('is_commercial', False)),
        round(float(drug_data.get('seizure_worth', 0.0) or 0.0), 2)
    )
```

### DB Schema Verification Needed
**FIRST: Check if `brief_facts_drug` has accused_id column:**
```sql
-- Run this:
SELECT column_name, data_type FROM information_schema.columns 
WHERE table_name='brief_facts_drug' AND column_name='accused_id';

-- If not found, add column:
ALTER TABLE public.brief_facts_drug 
ADD COLUMN accused_id CHARACTER VARYING(50) DEFAULT NULL;

-- Optional: Add FK constraint if accused table exists
-- ALTER TABLE public.brief_facts_drug 
-- ADD CONSTRAINT fk_accused 
-- FOREIGN KEY (accused_id) REFERENCES accused(id);
```

---

## ISSUE #2: NO_DRUGS_DETECTED Placeholder Pollution

### Problem
When LLM finds no drugs, code inserts fake "NO_DRUGS_DETECTED" record.
This pollutes analytics (can't distinguish real seizures from no-drugs cases).

### Current Code (BROKEN)
**File:** [main.py](main.py#L112-L113)
```python
if len(valid_drugs) == 0:
    # No drugs found — insert placeholder
    pending_inserts.append((cid, _NO_DRUGS_PLACEHOLDER.copy()))
    total_skipped += 1
```

### Fixed Code - Option A: Don't Insert Placeholder
```python
if len(valid_drugs) == 0:
    # No drugs found — DON'T insert fake placeholder
    # Track separately in extraction_log (see below)
    logging.info(f"Crime {cid}: No drugs extracted (LLM or brief_facts filtering)")
    total_skipped += 1
    # Skip insertion
else:
    for drug_data in valid_drugs:
        pending_inserts.append((cid, drug_data))
        total_inserted += len(valid_drugs)
```

### Fixed Code - Option B: Track in Separate Log Table (RECOMMENDED)

**1. Create extraction_log table:**
```sql
CREATE TABLE extraction_status_log (
    id SERIAL PRIMARY KEY,
    crime_id VARCHAR(50) NOT NULL,
    etl_order INT,
    status VARCHAR(20),  -- 'success', 'no_drugs', 'error', 'timeout'
    num_entries INT,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY (crime_id) REFERENCES crimes(crime_id)
);

CREATE INDEX idx_extraction_status_crime ON extraction_status_log(crime_id);
```

**2. Update main.py to log status:**
```python
def log_extraction_status(conn, crime_id, status, num_entries=0, error_msg=None):
    """Log extraction status without polluting brief_facts_drug table."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO extraction_status_log (crime_id, etl_order, status, num_entries, error_message)
            VALUES (%s, 23, %s, %s, %s)
        """, (crime_id, status, num_entries, error_msg))
    conn.commit()

# In process_crimes_parallel():
if valid_drugs is None:
    # Extraction error
    log_extraction_status(conn, cid, 'error', error_msg=str(e))
    total_skipped += 1
elif len(valid_drugs) == 0:
    # No drugs found (clean)
    log_extraction_status(conn, cid, 'no_drugs', num_entries=0)
    total_skipped += 1
else:
    # Successfully extracted
    for drug_data in valid_drugs:
        pending_inserts.append((cid, drug_data))
    log_extraction_status(conn, cid, 'success', num_entries=len(valid_drugs))
    total_inserted += len(valid_drugs)
```

**3. Now analytics queries exclude no-drugs cases:**
```sql
-- Get only actual drug seizures:
SELECT * FROM brief_facts_drug 
WHERE primary_drug_name NOT IN ('NO_DRUGS_DETECTED');

-- Or better:
SELECT d.* FROM brief_facts_drug d
INNER JOIN extraction_status_log e 
  ON d.crime_id = e.crime_id AND e.status='success';
```

---

## ISSUE #3: Post-Processing Order Dependency

### Problem
Current order causes data loss:
1. Standardize units (fills weight_g/kg/volume_ml/l fields)
2. Distribute worth (looks at quantity fields)
3. Apply commercial check (sums quantities)
4. **Deduplicate (KEEPS ONLY HIGHEST CONFIDENCE)** ← Applied AFTER commercial check

If collective seizure: A1 180g, A2 180g, A3 180g:
- All marked as commercial ✅
- Only highest confidence kept ❌ (loses 2 entries)
- Should collapse to null accused first

### Current Code (BROKEN)
**File:** [extractor.py](extractor.py#L980-L994)
```python
standardized = standardize_units(valid_drugs)
worth_distributed = _distribute_seizure_worth(standardized)
commercial_checked = _apply_commercial_quantity_check(worth_distributed)
return deduplicate_extractions(commercial_checked)
```

### Fixed Code
```python
# CORRECTED ORDER:
# 1. Collapse collective seizures FIRST
# 2. Standardize units
# 3. Distribute worth
# 4. Apply commercial check
# 5. Deduplicate

collapsed = _collapse_collective_seizures(valid_drugs)
standardized = standardize_units(collapsed)
worth_distributed = _distribute_seizure_worth(standardized)
commercial_checked = _apply_commercial_quantity_check(worth_distributed)
return deduplicate_extractions(commercial_checked)
```

### Updated Function
```python
def extract_drug_info(text: str, drug_categories: List[dict] = None) -> List[DrugExtraction]:
    """
    Extracts drug information with corrected post-processing order.
    
    Order matters:
    1. Collapse: Reduces 3+ identical accused → 1 entry (accused_id=null)
    2. Standardize: Fills unit fields (weight_g/volume_ml/count_total)
    3. Distribute: Splits worth proportionally
    4. Commercial: Marks entries >= threshold
    5. Deduplicate: Removes exact duplicates, caps per crime
    """
    if drug_categories is None:
        drug_categories = []

    # ── Step 0: Pre-process ──
    filtered_text, preprocess_meta = preprocess_brief_facts(text)
    if not filtered_text or not filtered_text.strip():
        logger.info("Pre-processor filtered out ALL sections.")
        return []

    # ── Step 1: Token budget ──
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
            f"Token budget tight: input ~{est_input_tokens} tokens, "
            f"available ~{available_for_input}"
        )

    # ── Step 2: Format KB ──
    kb_lines = []
    if drug_categories:
        kb_lines.append("raw_name|standard_name|category")
        for cat in drug_categories:
            raw = cat.get('raw_name', 'Unknown')
            std = cat.get('standard_name', 'Unknown')
            grp = cat.get('category_group', '-')
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
                if d.get('confidence_score') is None: d['confidence_score'] = 90
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
                elif d.get('seizure_worth') is None:
                    d['seizure_worth'] = 0.0
                
                valid_scopes = {'individual', 'drug_total', 'overall_total'}
                ws = str(d.get('worth_scope', 'individual')).lower().strip()
                if ws not in valid_scopes:
                    d['worth_scope'] = 'individual'
                else:
                    d['worth_scope'] = ws
                
                valid_drugs.append(DrugExtraction(**d))
            except Exception as e:
                logger.warning(f"Skipping invalid drug entry: {e}")
        
        # ── POST-PROCESSING (CORRECTED ORDER) ──
        collapsed = _collapse_collective_seizures(valid_drugs)          # Step 1
        standardized = standardize_units(collapsed)                      # Step 2
        worth_distributed = _distribute_seizure_worth(standardized)      # Step 3
        commercial_checked = _apply_commercial_quantity_check(worth_distributed)  # Step 4
        final = deduplicate_extractions(commercial_checked)              # Step 5
        
        return final
        
    except Exception as e:
        logger.error(f"Drug extraction failed: {e}", exc_info=True)
        return []
```

---

## ISSUE #4: KB Format Not Escaped

### Problem
Pipe character `|` in drug names breaks CSV format for LLM.

### Current Code (BROKEN)
**File:** [extractor.py](extractor.py#L906-L915)
```python
kb_lines = []
if drug_categories:
    kb_lines.append("raw_name|standard_name|category")
    for cat in drug_categories:
        raw = cat.get('raw_name', 'Unknown')
        std = cat.get('standard_name', 'Unknown')
        grp = cat.get('category_group', '-')
        kb_lines.append(f"{raw}|{std}|{grp}")  # ❌ NO ESCAPING
```

### Fixed Code
```python
import csv
import io

def _format_drug_kb_safe(drug_categories: List[dict]) -> str:
    """
    Format drug KB as pipe-delimited CSV with proper escaping.
    Handles special characters: pipes, newlines, quotes.
    """
    if not drug_categories:
        return "(No KB provided)"
    
    # Use Python's csv module for safe escaping
    # Then replace comma delimiter with pipe
    output = io.StringIO()
    writer = csv.writer(output, delimiter='|', quoting=csv.QUOTE_MINIMAL)
    
    # Write header
    writer.writerow(['raw_name', 'standard_name', 'category'])
    
    # Write data rows
    for cat in drug_categories:
        raw_name = str(cat.get('raw_name', '')).strip() or 'Unknown'
        std_name = str(cat.get('standard_name', '')).strip() or 'Unknown'
        category = str(cat.get('category_group', '')).strip() or '-'
        
        # Sanitize: remove newlines, tabs
        raw_name = raw_name.replace('\n', ' ').replace('\t', ' ')
        std_name = std_name.replace('\n', ' ').replace('\t', ' ')
        category = category.replace('\n', ' ').replace('\t', ' ')
        
        writer.writerow([raw_name, std_name, category])
    
    result = output.getvalue()
    output.close()
    
    logger.debug(f"Formatted KB: {len(result)} chars, {len(drug_categories)} entries")
    return result

# In extract_drug_info():
formatted_kb = _format_drug_kb_safe(drug_categories)
```

---

## DEPLOYMENT CHECKLIST

- [ ] **Issue #1 Fix:** Update `db.py _prepare_insert_values()` to preserve accused_id
- [ ] **DB Schema:** Verify `brief_facts_drug.accused_id` column exists (or ADD it)
- [ ] **Issue #2 Fix:** Create `extraction_status_log` table + update main.py logging
- [ ] **Issue #3 Fix:** Correct post-processing order in `extract_drug_info()`
- [ ] **Issue #4 Fix:** Use _format_drug_kb_safe() in extract_drug_info()
- [ ] **Test:** Run test_preprocessor.py, test_seizure_worth.py
- [ ] **Staging:** Test on 100 sample crimes before production run
- [ ] **Verify:** Check brief_facts_drug for accused_id values (should not be all NULL)
- [ ] **QA:** Run analytics query to confirm no "NO_DRUGS_DETECTED" pollution

