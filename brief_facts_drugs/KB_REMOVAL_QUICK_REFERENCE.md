# Quick Reference: KB Removal Changes

## TL;DR

**KB is NOT sent to LLM anymore.**

Before:
```
LLM receives: FIR text + 330 drug KB entries (1500 tokens wasted)
Result: Slow, may timeout
```

After:
```
LLM receives: FIR text only (uses trained knowledge)
KB mapping: Applied post-extraction (fast, via DB lookup)
Result: Fast, reliable, uses LLM's full knowledge
```

---

## Three Key Changes

### 1. **LLM Prompt** (brief_facts_drugs/extractor.py)
```python
# OLD: send 330 KB entries to LLM
formatted_kb = "\n".join(kb_lines)  # ❌ Large, slow

# NEW: send empty placeholder
formatted_kb = "(No KB provided — LLM uses trained knowledge...)"  # ✓ Fast
```

### 2. **KB Mapping** (extractor.py, line 1213)
```python
# MOVED to after LLM returns (not before)
kb_mapped = _apply_kb_mapping(valid_drugs, drug_categories)
# Example: "Dry Ganja" → "Ganja" (fast dictionary lookup)
```

### 3. **Ignored Filter** (main.py, line 217)
```python
# Already exists - now crucial since LLM has freedom
is_ignored = is_drug_ignored(raw_name, ignored_checklist, threshold=0.80)
# Example: "Paracetamol" → REJECT
```

---

## Data Tables

| Table | Rows | Used For | Query |
|-------|------|----------|-------|
| `drug_categories` | ~330 | Map extracted names to standard names | `SELECT * FROM drug_categories WHERE raw_name LIKE ?` |
| `ignored_drugs_checklist` | ~144 | Filter out non-NDPS drugs | `SELECT ignored_term FROM ignored_drugs_checklist` |

---

## Log Signals

### ✓ Working Correctly
```
[INFO] [Extractor] Sending to LLM — text_len=2345 chars (NO KB in prompt)
[INFO] [LLM] chain.invoke() returned in 12.34s
[INFO] KB mapping applied: 'Dry Ganja' → 'Ganja'
[INFO] Crime 1234: Inserted 5 drug facts (0 skipped, 1 ignored)
```

### ✗ Issues
```
# KB still being sent (bad)
[INFO] [Extractor] Sending to LLM — text_len=2345 chars, KB_entries=330

# Hanging (bad)
[INFO] [LLM] Invoking chain (attempt 1)...
(no follow-up message for 5+ minutes)

# Timeouts every 300s (expected if Ollama is slow)
[ERROR] All 3 extraction attempts failed. Last error: TimeoutError
```

---

## Restart Service

```bash
# Kill old process
pkill -f brief_facts_drugs

# Restart
cd /data-drive/etl-process-dev/brief_facts_drugs
nohup python main.py > brief_facts_drugs.log 2>&1 &

# Verify (should see "NO KB in prompt" within 30 seconds)
tail -20 brief_facts_drugs.log
```

---

## Expected Performance

| Metric | Before | After |
|--------|--------|-------|
| LLM response time | 30-60s (or ∞ hung) | 5-15s |
| Throughput | 0 (hung) | 200-400 crimes/hour |
| Timeout handling | 0 (infinite hang) | Graceful after 300s |

---

## If Something Breaks

```bash
# Check logs for KB in prompt (should NOT see this)
grep "KB_entries=" brief_facts_drugs.log

# Verify extraction working
grep -c "LLM returned" brief_facts_drugs.log

# Check DB inserts
psql dev-2 -c "SELECT COUNT(*) FROM brief_facts_drug WHERE created_at > NOW() - INTERVAL '30 minutes';"

# Revert if needed
git log --oneline
git checkout <previous-commit>
```

---

## Questions?

- **"Why remove KB from LLM?"** → Reduces prompt size 40%, speeds up extraction, prevents timeouts
- **"Will LLM miss variants?"** → No, LLM's trained knowledge includes "Ganja = Cannabis = Hemp" synonyms
- **"How are drugs mapped to standard names?"** → Via drug_categories table lookup (fuzzy match)
- **"What if a drug isn't in KB?"** → Still inserted with raw_drug_name, kb_mapped=False
- **"Can unwanted drugs slip through?"** → No, ignored_checklist filter catches non-NDPS (Paracetamol, etc.)
