#!/usr/bin/env python3
"""
Diagnose whether the KB is actually being used by the LLM.
Checks:
1. KB loading from DB
2. KB formatting
3. Whether KB affects LLM response
4. Token accounting
"""

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from brief_facts_drugs.db import fetch_drug_categories, get_db_connection
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def estimate_tokens(text: str) -> int:
    """Rough token estimate."""
    return len(text) // 4

def diagnose_kb():
    """Check KB usage."""
    print("=" * 80)
    print("DOPAMS Brief Facts Drugs — KB Usage Diagnostic")
    print("=" * 80)
    
    # 1. Load KB
    try:
        conn = get_db_connection()
        drug_categories = fetch_drug_categories(conn)
        print(f"\n✓ KB loaded from DB: {len(drug_categories)} entries")
        conn.close()
    except Exception as e:
        print(f"\n✗ Failed to load KB: {e}")
        return
    
    # 2. Check KB structure
    if drug_categories:
        sample = drug_categories[0]
        print(f"\n✓ Sample KB entry structure:")
        for key, val in sample.items():
            print(f"  - {key}: {val}")
    
    # 3. Format KB as it would appear in prompt
    kb_lines = []
    if drug_categories:
        kb_lines.append("raw_name|standard_name|category")
        for cat in drug_categories:
            raw = cat.get('raw_name', 'Unknown')
            std = cat.get('standard_name', 'Unknown')
            grp = cat.get('category_group', '-')
            kb_lines.append(f"{raw}|{std}|{grp}")
    
    formatted_kb = "\n".join(kb_lines)
    kb_tokens = estimate_tokens(formatted_kb)
    
    print(f"\n✓ KB formatted into {len(kb_lines)} lines")
    print(f"  - Header + {len(kb_lines)-1} entries")
    print(f"  - Raw characters: {len(formatted_kb):,}")
    print(f"  - Estimated tokens: {kb_tokens:,} / 16384 context window = {kb_tokens*100/16384:.1f}%")
    
    # 4. Check for duplicates/gaps in KB
    raw_names = set()
    std_names = set()
    categories = set()
    for cat in drug_categories:
        raw_names.add(cat.get('raw_name', 'Unknown'))
        std_names.add(cat.get('standard_name', 'Unknown'))
        categories.add(cat.get('category_group', '-'))
    
    print(f"\n✓ KB statistics:")
    print(f"  - Unique raw names: {len(raw_names)}")
    print(f"  - Unique standard names: {len(std_names)}")
    print(f"  - Unique categories: {len(categories)}")
    print(f"  - Categories: {sorted(categories)}")
    
    # 5. Show first/last few entries to verify formatting
    print(f"\n✓ First 5 KB lines:")
    for line in kb_lines[:5]:
        print(f"  {line}")
    print(f"  ...")
    print(f"\n✓ Last 5 KB lines:")
    for line in kb_lines[-5:]:
        print(f"  {line}")
    
    # 6. Estimate prompt overhead
    extraction_prompt_header = """You are an expert forensic data analyst extracting structured drug seizure data from police brief facts.

## CORE RULES (STRICT — read carefully)
[...many rules...]

## Drug Knowledge Base
"""
    prompt_overhead = estimate_tokens(extraction_prompt_header)
    
    print(f"\n⚠ TOKEN ACCOUNTING:")
    print(f"  - Prompt template overhead: ~{prompt_overhead} tokens")
    print(f"  - KB: {kb_tokens:,} tokens ({kb_tokens*100/16384:.1f}%)")
    print(f"  - Available for input text: {16384 - prompt_overhead - kb_tokens} tokens")
    print(f"  - Input text (typical): 500-1000 tokens")
    print(f"  - TOTAL: {prompt_overhead + kb_tokens + 750} / 16384 = {(prompt_overhead + kb_tokens + 750)*100/16384:.1f}%")
    
    if kb_tokens > 3000:
        print(f"\n⚠ WARNING: KB is {kb_tokens} tokens (~{kb_tokens*100/16384:.0f}% of context window)!")
        print(f"  This may reduce LLM's ability to follow complex instructions.")
        print(f"  Consider reducing KB size or splitting into focused subsets.")
    
    # 7. Check if KB might be malformed
    print(f"\n✓ Checking for potential formatting issues:")
    for i, line in enumerate(kb_lines[:10]):
        parts = line.split('|')
        if len(parts) != 3:
            print(f"  WARNING: Line {i} has {len(parts)} fields instead of 3: {line}")
    
    print(f"\n" + "=" * 80)
    print("RECOMMENDATIONS:")
    print("=" * 80)
    print("""
If KB is loaded but not used:
1. Check if LLM is actually receiving the KB in the prompt
2. Verify KB entries have correct format (raw_name|standard_name|category)
3. Check if LLM response actually references KB entries
4. If KB is >3000 tokens, consider summarizing into top N most common drugs
5. Verify LLM isn't truncating/ignoring KB due to context window pressure

To verify LLM is using KB:
- Look for exact matches between LLM output primary_drug_name and KB standard_name
- Check extraction_metadata['kb_mapped'] field
- Compare extraction results WITH KB vs WITHOUT KB
""")

if __name__ == '__main__':
    diagnose_kb()
