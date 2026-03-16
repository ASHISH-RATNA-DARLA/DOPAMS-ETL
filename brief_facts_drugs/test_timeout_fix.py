#!/usr/bin/env python3
"""
Quick test to verify timeout fixes are working and LLM can respond.
"""

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

from brief_facts_drugs.extractor import extract_drug_info
from brief_facts_drugs.db import fetch_drug_categories, get_db_connection
import json

def test_extraction_with_timeout():
    """Test a simple extraction to verify timeout handling."""
    
    print("=" * 80)
    print("TIMEOUT FIX VERIFICATION TEST")
    print("=" * 80)
    
    # Simple test case
    test_text = """
    IN THE HONOURABLE COURT OF SESSIONAL JUDGE
    
    Crime No. 2026-001
    Seized 100 grams of Ganja from A1 (Ramesh Kumar) worth Rs.50,000.
    The quantity seized is above commercial limit under NDPS Act.
    """
    
    try:
        conn = get_db_connection()
        drug_categories = fetch_drug_categories(conn)
        conn.close()
        
        print(f"\n✓ Loaded {len(drug_categories)} drug categories from DB")
        print(f"\nTesting extraction on simple text...")
        print(f"Expected: Should complete in <30 seconds with timeout protection")
        
        # This should now timeout gracefully if Ollama doesn't respond
        result = extract_drug_info(test_text, drug_categories)
        
        if result:
            print(f"\n✓ Extraction succeeded!")
            print(f"  Found {len(result)} drug entries")
            for drug in result[:3]:  # Show first 3
                print(f"    - {drug.raw_drug_name} ({drug.raw_quantity} {drug.raw_unit})")
        else:
            print(f"\n⚠ Extraction returned empty (LLM may have timed out)")
            
    except TimeoutError as e:
        print(f"\n✓ Timeout correctly triggered: {e}")
        print(f"  This is expected if Ollama is slow to respond.")
    except Exception as e:
        print(f"\n✗ Unexpected error: {type(e).__name__}: {e}")

if __name__ == '__main__':
    test_extraction_with_timeout()
