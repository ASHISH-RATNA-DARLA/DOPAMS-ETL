# Quick Deployment Checklist

## Files Modified

### 1. brief_facts_drugs/extractor.py
- ✓ Removed KB from LLM prompt (line ~1122-1153)
- ✓ Updated token budget calculation (no KB tokens)
- ✓ Changed logging to "NO KB in prompt"
- ✓ Updated EXTRACTION_PROMPT to say "Use your trained knowledge"
- ✓ KB mapping still applied post-extraction (_apply_kb_mapping)

### 2. core/llm_service.py
- ✓ Added timeout wrapper (_invoke_with_timeout) 
- ✓ Catches TimeoutError and retries gracefully
- ✓ Timeout from LLM_TIMEOUT env var (300s)

### 3. brief_facts_drugs/extractor.py (_get_thread_safe_llm)
- ✓ Added httpx.Client with timeout to ChatOllama
- ✓ Prevents hanging on unresponsive Ollama

## Pre-Deployment Verification

Run this before restarting service:

```bash
# Check DB tables exist
sqlite3 /path/to/dopams.db "SELECT COUNT(*) FROM drug_categories;" 
sqlite3 /path/to/dopams.db "SELECT COUNT(*) FROM ignored_drugs_checklist;"

# Verify code changes
grep "NO KB in prompt" brief_facts_drugs/extractor.py
grep "Use your trained knowledge" brief_facts_drugs/extractor.py
grep "TimeoutError" core/llm_service.py

# Test imports
python -c "from brief_facts_drugs.extractor import extract_drug_info; print('✓ Imports OK')"
```

## Deployment Steps

```bash
cd /data-drive/etl-process-dev/brief_facts_drugs

# 1. Kill any running process
pkill -f "python.*main.py"
sleep 2

# 2. Verify it's dead
ps aux | grep "python.*main.py" | head -5

# 3. Restart
nohup python main.py > brief_facts_drugs.log 2>&1 &

# 4. Monitor startup
sleep 3
head -20 brief_facts_drugs.log

# 5. Watch for "NO KB in prompt" message
tail -f brief_facts_drugs.log | head -50
```

## Expected Log Output (First 30 seconds)

```
2026-03-16 15:00:00,123 - INFO - Starting Drug Extraction Service...
2026-03-16 15:00:00,124 - INFO - Parallel LLM workers: 3
2026-03-16 15:00:00,200 - INFO - Connection pool initialized: 5-20 connections
2026-03-16 15:00:00,205 - INFO - Database connection established.
2026-03-16 15:00:00,210 - INFO - Loaded 330 drug categories from knowledge base.
2026-03-16 15:00:00,212 - INFO - Loaded 144 ignored drug terms for validation.
2026-03-16 15:00:00,215 - INFO - input.txt not found. Will fetch unprocessed crimes from DB.
2026-03-16 15:00:00,220 - INFO - No input IDs provided. Starting Dynamic Batch Processing...
2026-03-16 15:00:00,225 - INFO - Fetched batch of 15 unprocessed crimes.
2026-03-16 15:00:00,300 - INFO - Token budget OK: input ~580/15584 available tokens.
2026-03-16 15:00:00,305 - INFO - Created thread-local ChatOllama for thread ThreadPoolExecutor-0_0 with 300s timeout
2026-03-16 15:00:00,310 - INFO - Created thread-local ChatOllama for thread ThreadPoolExecutor-0_1 with 300s timeout
2026-03-16 15:00:00,315 - INFO - Created thread-local ChatOllama for thread ThreadPoolExecutor-0_2 with 300s timeout
2026-03-16 15:00:00,320 - INFO - [Extractor] Sending to LLM — text_len=2345 chars (NO KB in prompt)
2026-03-16 15:00:00,325 - INFO - [Extractor] Sending to LLM — text_len=2145 chars (NO KB in prompt)
2026-03-16 15:00:00,330 - INFO - [Extractor] Sending to LLM — text_len=1890 chars (NO KB in prompt)
2026-03-16 15:00:00,335 - INFO - [LLM] Invoking chain (attempt 1)...
2026-03-16 15:00:00,340 - INFO - [LLM] Invoking chain (attempt 1)...
2026-03-16 15:00:00,345 - INFO - [LLM] Invoking chain (attempt 1)...
2026-03-16 15:00:15,500 - INFO - [LLM] chain.invoke() returned in 12.15s  ← GOOD! (was infinite hang before)
2026-03-16 15:00:16,600 - INFO - LLM returned 5 raw drug entries.
2026-03-16 15:00:16,610 - INFO - KB mapping applied: 'Dry Ganja' → 'Ganja' (category: Cannabis)
2026-03-16 15:00:16,620 - DEBUG - KB mapping applied: 'Khat' matched ignored list 'khat' (95%) → REJECTING
2026-03-16 15:00:18,700 - INFO - Crime 1234: Inserted 4 drug facts (0 skipped, 1 ignored)
2026-03-16 15:00:19,800 - INFO - [LLM] chain.invoke() returned in 11.50s
2026-03-16 15:00:20,900 - INFO - LLM returned 3 raw drug entries.
...
2026-03-16 15:00:45,000 - INFO - Batch complete. Total processed so far: 15
```

## Key Indicators

### ✓ Success Signs
- "NO KB in prompt" appears in logs
- LLM responses complete in 5-30s range
- Each "chain.invoke() returned" message has a time
- "KB mapping applied" shows drug names being standardized
- Database receives drug facts

### ✗ Failure Signs
- Process still hangs (no "chain.invoke() returned" messages)
- "Error: LLM invoke timed out" every 300 seconds
- "KB_entries=330" in logs (means KB is still being sent)
- No database inserts occurring

## Rollback (if needed)

```bash
# Switch to previous git commit
git log --oneline | head -5
git checkout <previous-commit-hash>

# Restart
pkill -f "python.*main.py"
nohup python main.py > brief_facts_drugs.log 2>&1 &
```

## Performance Targets

- **Before:** 0% CPU (hung), 0 crimes/hour
- **After:** 10-20% CPU, 200-400 crimes/hour per 3 workers

If not meeting targets, check:
1. Ollama response times (may be overloaded)
2. DB connection pool settings
3. PARALLEL_LLM_WORKERS (currently 3, can increase to 6)
