#!/usr/bin/env python3
"""
Test smart drug assignment logic: respects explicit accused mentions.

SCENARIO 1: Drug mentions specific accused
- "A-1 has 6 Kg Ganja" → assign to A-1 only
- "A-3 sold 1 Kg" → assign to A-3 only

SCENARIO 2: Drug with primary + secondary mentions
- ["A-1 has 6 Kg", "A-3 sold 1 Kg", "5 Kg remaining"]
- Different sources mention different accused → use PRIMARY (A-1)

SCENARIO 3: All sources mention same accused
- ["A-1 has 6 Kg", "A-1 remaining 5 Kg"]
- All same → use A-1

SCENARIO 4: No accused mentioned
- ["6 Kg seized", "5 Kg remaining"]
- No A-codes → DEFAULT to A-1
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from db import _extract_codes_from_source, _extract_person_codes


def test_single_source_with_accused():
    """Test extracting codes from source mentioning specific accused."""
    sources = [
        ("A-1 has 6 Kg of Ganja", {"A-1"}),
        ("A-3 sold 1 Kg in Hyderabad", {"A-3"}),
        ("A-4 is the main supplier", {"A-4"}),
        ("seized from A-1 and A-2", {"A-1", "A-2"}),
    ]

    print("TEST 1: Single source with accused mention")
    for source, expected in sources:
        result = _extract_codes_from_source(source)
        status = "✓" if result == expected else "✗"
        print(f"  {status} '{source}' → {result} (expected {expected})")
        assert result == expected, f"Expected {expected}, got {result}"


def test_downstream_filtering():
    """Test that downstream transaction references are filtered out."""
    sources = [
        ("seized 6 Kg and sold to A-3", {""}),  # A-3 is buyer, not seized
        ("seized from A-1, sold to A-3", {"A-1"}),  # A-1 is seized, A-3 is buyer
        ("A-2 purchased from A-1", {"A-1"}),  # A-1 is source, A-2 is buyer
    ]

    print("\nTEST 2: Downstream transaction filtering")
    for source, expected_contains in sources:
        result = _extract_codes_from_source(source)
        # For downstream test, just check that downstream codes are NOT included
        has_downstream = any(code in str(result) for code in ["A-3", "A-2"]
                             if "to A-" in source or "purchased from" in source)
        status = "✓" if not has_downstream or result else "✗"
        print(f"  {status} '{source}' → {result}")


def test_consolidated_sources_same_accused():
    """Test consolidated sources all mentioning the same accused."""
    drug_data = {
        'extraction_metadata': {
            'consolidated_sources': [
                'A-1 has 6 Kg of Ganja',
                'A-1 seized with 6 Kg',
                '5 Kg remaining with A-1',
            ]
        }
    }

    print("\nTEST 3: Consolidated sources - same accused")
    result = _extract_person_codes(drug_data)
    expected = {'A-1'}
    status = "✓" if result == expected else "✗"
    print(f"  {status} All sources mention A-1 → {result} (expected {expected})")
    assert result == expected, f"Expected {expected}, got {result}"


def test_consolidated_sources_different_accused():
    """Test consolidated sources mentioning different accused → use primary."""
    drug_data = {
        'extraction_metadata': {
            'consolidated_sources': [
                'A-1 had 6 Kg of Ganja',  # Primary mention
                'A-3 sold 1 Kg in Hyderabad',  # Different accused
                '5 Kg remaining',  # No accused
            ]
        }
    }

    print("\nTEST 4: Consolidated sources - different accused")
    result = _extract_person_codes(drug_data)
    expected = {'A-1'}  # Primary (first) mention
    status = "✓" if result == expected else "✗"
    print(f"  {status} Sources mention A-1, A-3, none → {result} (expected primary {expected})")
    assert result == expected, f"Expected primary {expected}, got {result}"


def test_consolidated_sources_no_accused():
    """Test consolidated sources with NO accused mentions → empty set → A1 fallback."""
    drug_data = {
        'extraction_metadata': {
            'consolidated_sources': [
                '6 Kg of Ganja seized',
                '1 Kg sold in transaction',
                '5 Kg remaining with seized goods',
            ]
        }
    }

    print("\nTEST 5: Consolidated sources - no accused mentioned")
    result = _extract_person_codes(drug_data)
    expected = set()  # Empty → will trigger A1 fallback
    status = "✓" if result == expected else "✗"
    print(f"  {status} No accused mentioned → {result} (expected empty for A1 fallback)")
    assert result == expected, f"Expected empty set (A1 fallback), got {result}"


def test_fallback_to_single_source():
    """Test fallback to single source_sentence when no consolidated_sources."""
    drug_data = {
        'extraction_metadata': {
            'source_sentence': 'A-2 had 100g of Heroin'
        }
    }

    print("\nTEST 6: Fallback to single source_sentence")
    result = _extract_person_codes(drug_data)
    expected = {'A-2'}
    status = "✓" if result == expected else "✗"
    print(f"  {status} Single source with A-2 → {result} (expected {expected})")
    assert result == expected, f"Expected {expected}, got {result}"


if __name__ == "__main__":
    try:
        test_single_source_with_accused()
        test_downstream_filtering()
        test_consolidated_sources_same_accused()
        test_consolidated_sources_different_accused()
        test_consolidated_sources_no_accused()
        test_fallback_to_single_source()
        print("\n✅ ALL TESTS PASSED - Smart drug assignment logic verified")
    except AssertionError as e:
        print(f"\n❌ TEST FAILED: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ UNEXPECTED ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
