# Fuzzy Matching & KB Learning Guide

## Overview

The ETL pipeline uses Python's `difflib.SequenceMatcher` for fuzzy string matching to:
1. **Check against ignored drug checklist** (>80% threshold)
2. **Map raw drug names to KB categories** (>80% for fuzzy match)

## How Fuzzy Matching Works

### Algorithm: SequenceMatcher

Python's `difflib.SequenceMatcher` compares two strings and returns a **similarity ratio** between 0.0 (completely different) and 1.0 (identical).

**Formula:**
```
ratio = 2.0 * len(matches) / len(string1 + string2)
```

Where `matches` are common contiguous subsequences between the strings.

### Example Calculations

```python
import difflib

# Exact match
ratio = difflib.SequenceMatcher(None, "ganja", "ganja").ratio()
# Output: 1.0 (100% match)

# Close match with typo
ratio = difflib.SequenceMatcher(None, "ganj", "ganja").ratio()
# Output: 0.888... (88.8% match)

# Partial match
ratio = difflib.SequenceMatcher(None, "unknown", "unknown drug").ratio()
# Output: 0.818... (81.8% match)

# Different strings
ratio = difflib.SequenceMatcher(None, "heroin", "cocaine").ratio()
# Output: 0.375 (37.5% match - common letters)
```

## Ignored Checklist Matching

### Threshold: ≥ 0.80 (80%)

If a drug's raw name matches ANY term in `drug_ignore_list` with **≥80% similarity**, it is **REJECTED**.

### Example: Checking "Unknown Substance" Against Ignored List

| Ignored Term | Similarity | Match? | Decision |
|---|---|---|---|
| "unknown substance" | 92% | ✅ | REJECT |
| "unknown" | 88% | ✅ | REJECT |
| "unidentified substance" | 79% | ❌ | ALLOW (below 80%) |
| "unknown drug" | 75% | ❌ | ALLOW (below 80%) |

**Result**: Drug is **REJECTED** (matches 2 terms ≥80%)

### Case Insensitivity

All comparisons are **case-insensitive**:

```python
is_drug_ignored("GANJA", ignore_list)      # Same as "ganja"
is_drug_ignored("Ganja", ignore_list)      # Same as "ganja"
is_drug_ignored("GaNjA", ignore_list)      # Same as "ganja"
```

### Whitespace Handling

All strings are **trimmed** before comparison:

```python
is_drug_ignored("  ganja  ", ignore_list)  # Same as "ganja"
is_drug_ignored("ganja\n", ignore_list)    # Same as "ganja"
```

## KB Category Mapping

### Thresholds

| Match Type | Similarity | Confidence | Decision |
|---|---|---|---|
| **Exact** | 100% | 0.95 | Map to standard_name |
| **Fuzzy** | 80-99% | 0.80-0.94 | Map to standard_name + boost confidence |
| **No Match** | <80% | 0.0 | Use raw_name + flag for ignored checklist |

### Example: Mapping "Ganj" to KB

**Database entries:**
```
raw_name='Ganja', standard_name='Ganja', category='Narcotic'
raw_name='Dry Ganja', standard_name='Ganja', category='Narcotic'
raw_name='Charas', standard_name='Charas', category='Narcotic'
```

**Matching "Ganj":**

| KB Entry | Similarity | Match Type | Result |
|---|---|---|---|
| "Ganja" | 80% | Fuzzy | ✅ Best match |
| "Dry Ganja" | 57% | None | ❌ |
| "Charas" | 38% | None | ❌ |

**Result**: Map to standard_name="Ganja", category="Narcotic", confidence=0.88

### Confidence Calculation

For fuzzy matches (80-99%):
```
confidence = 0.80 + (similarity_ratio - 0.80) * 0.15

Examples:
- 80% match → 0.80 + (0.80-0.80)*0.15 = 0.80
- 85% match → 0.80 + (0.85-0.80)*0.15 = 0.8075
- 90% match → 0.80 + (0.90-0.80)*0.15 = 0.815
- 99% match → 0.80 + (0.99-0.80)*0.15 = 0.8285
- 100% match (exact) → 0.95 (hardcoded)
```

## Common Matching Patterns

### Typos & Misspellings

```
Input: "heroin" vs KB: "heroine"  → 83% → ✅ MATCH (>80%)
Input: "ganja" vs KB: "ganja"     → 100% → ✅ EXACT MATCH

Input: "ganj" vs KB: "ganja"      → 80% → ✅ MATCH (≥80%)
Input: "gan" vs KB: "ganja"       → 60% → ❌ NO MATCH
```

### Abbreviations & Variants

```
Input: "MDMA" vs KB: "mdma"       → 100% → ✅ EXACT (case-insensitive)
Input: "E" vs KB: "ecstasy"       → 18% → ❌ NO MATCH
Input: "acid" vs KB: "lsd"        → 40% → ❌ NO MATCH
```

### Multi-word Drugs

```
Input: "Hash Oil" vs KB: "Hashish Oil"     → 78% → ❌ NO MATCH (<80%)
Input: "Hash Oil" vs KB: "Hash Oil"        → 100% → ✅ EXACT
Input: "Hashish Oil" vs KB: "Hash Oil"     → 87% → ✅ MATCH (>80%)
```

### Substrings

```
Input: "cannabis oil" vs KB: "cannabis"     → 78% → ❌ NO MATCH
Input: "cannabis oil" vs KB: "cannabis oil" → 100% → ✅ EXACT
Input: "liquid opium" vs KB: "opium"        → 60% → ❌ NO MATCH
```

## Special Cases

### Empty Strings

```python
is_drug_ignored("", ignore_list)         # Returns (False, None, 0.0)
is_drug_ignored(None, ignore_list)       # Returns (False, None, 0.0)
_map_drug_to_kb_category("", [])         # Returns ("", "Unknown", 0.0)
```

### Single Character

```python
is_drug_ignored("X", [{"term": "X"}])    # Returns (True, "x", 1.0)
is_drug_ignored("X", [{"term": "Y"}])    # Returns (False, None, 0.0)
```

### Ignored List with None/Empty Terms

```python
ignore_list = [
    {"term": None, "reason": "..."},      # Skipped (filtered)
    {"term": "", "reason": "..."},        # Skipped (filtered)
    {"term": "ganja", "reason": "..."}    # Processed
]
```

## Adjusting Thresholds

### Current Thresholds

| Context | Threshold | File | Function |
|---|---|---|---|
| Ignored checklist | 0.80 | `main.py` | `process_crimes_parallel()` |
| KB mapping | 0.80 | `extractor.py` | `_map_drug_to_kb_category()` |

### Modifying Threshold

To change ignored checklist threshold from 0.80 to 0.85:

**In `main.py`, line ~217:**
```python
# Before:
is_ignored, matched_term, similarity = is_drug_ignored(raw_name, ignored_checklist, threshold=0.80)

# After (stricter):
is_ignored, matched_term, similarity = is_drug_ignored(raw_name, ignored_checklist, threshold=0.85)
```

**Impact:**
- **Higher threshold (0.85+)**: Fewer false rejections, but some actual bad drugs might pass
- **Lower threshold (<0.80)**: More rejections, but might reject valid drugs with typos

### Recommended Values

| Use Case | Threshold |
|---|---|
| **Strict (no false positives)** | 0.90 |
| **Balanced (current)** | 0.80 |
| **Lenient (catch all variants)** | 0.70 |

## Performance Considerations

### Matching Speed

For each extracted drug:
```
Time = O(n * m)
where:
  n = length of drug_name (typically 5-20 chars)
  m = length of ignored term (typically 5-30 chars)
```

**Typical Performance:**
- Single match: ~0.001ms
- 100 terms in ignore list: ~0.1ms per drug
- 200 extracted drugs × 100 ignore terms: ~20ms total

### Optimization Strategies

1. **Pre-compute lowercase versions** (already done in code)
2. **Limit ignored list size** (<=500 terms recommended)
3. **Stop early on 100% match** (implemented in code)
4. **Cache frequent comparisons** (not implemented, but could be added)

## Debugging Fuzzy Matches

### Enable Debug Logging

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### What to Look For in Logs

```
[DEBUG] KB mapping: 'Ganj' → 'Ganja' (Narcotic) with 80% similarity (confidence 0.80)
[INFO] Drug 'Unknown Drug' matched ignore list 'unknown drug' with 86% similarity
[DEBUG] No KB match for 'xyz_drug' (will be flagged for ignored checklist)
```

### Manual Testing

```python
import difflib
from db import is_drug_ignored, fetch_ignored_checklist

# Get ignore list
ignore_list = fetch_ignored_checklist(conn)

# Test a drug
drug_name = "Unknown Substance"
is_ignored, matched, score = is_drug_ignored(drug_name, ignore_list, threshold=0.80)

print(f"Drug: {drug_name}")
print(f"Ignored: {is_ignored}")
print(f"Matched term: {matched}")
print(f"Similarity: {score:.0%}")
```

## Best Practices

1. **Keep ignored_checklist small and specific**
   - Use full phrases rather than single words
   - ✅ "unknown substance" instead of ❌ "unknown"

2. **Test before deploying**
   ```python
   # Test with a sample extraction
   test_drugs = ["unknown", "ganja", "heroin", "xyz"]
   for drug in test_drugs:
       is_ignored, matched, score = is_drug_ignored(drug, ignore_list)
       print(f"{drug}: {score:.0%} (ignored={is_ignored})")
   ```

3. **Monitor threshold changes**
   - Track rejection rate if threshold is adjusted
   - Log should show any unexpected patterns

4. **Regular audit of ignored list**
   - Review quarterly for outdated entries
   - Remove entries that cause false positives
