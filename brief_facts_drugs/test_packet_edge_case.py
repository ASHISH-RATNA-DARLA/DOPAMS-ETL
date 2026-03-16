#!/usr/bin/env python3
"""
Test script for PACKET EDGE CASE handling in drug extraction ETL

This script demonstrates:
1. Packets WITH per-packet weight (should multiply)
2. Packets WITHOUT weight (should remain as count)
3. Mixed packet scenarios
4. Decimal weights
"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from extractor import extract_drug_info


def test_packet_with_weight():
    """Test: Packets with per-packet weight → MULTIPLY"""
    print("\n" + "="*80)
    print("TEST 1: PACKETS WITH PER-PACKET WEIGHT (Should Multiply)")
    print("="*80)
    
    text = """
    Seized from A1: 8 packets of Ganja, each containing 50 grams, worth Rs.5000
    """
    
    print(f"Input: {text.strip()}")
    result = extract_drug_info(text)
    
    if result:
        drug = result[0]
        print(f"\n✓ Results:")
        print(f"  - raw_drug_name: {drug.raw_drug_name}")
        print(f"  - raw_quantity: {drug.raw_quantity} (Expected: 400, i.e., 8 × 50)")
        print(f"  - raw_unit: {drug.raw_unit} (Expected: grams)")
        print(f"  - drug_form: {drug.drug_form} (Expected: solid)")
        print(f"  - weight_g: {drug.weight_g}")
        print(f"  - weight_kg: {drug.weight_kg}")
        
        assert drug.raw_quantity == 400.0, f"Expected 400, got {drug.raw_quantity}"
        assert drug.raw_unit == "grams", f"Expected 'grams', got '{drug.raw_unit}'"
        assert drug.drug_form == "solid", f"Expected 'solid', got '{drug.drug_form}'"
        print("\n✅ TEST 1 PASSED: Packets correctly multiplied!")
    else:
        print("❌ TEST 1 FAILED: No extraction result")


def test_packet_without_weight():
    """Test: Packets without weight → KEEP AS COUNT"""
    print("\n" + "="*80)
    print("TEST 2: PACKETS WITHOUT WEIGHT (Should Keep as Count)")
    print("="*80)
    
    text = """
    Apprehended A1 with 15 packets of MDMA, but contents not analyzed or weighed
    """
    
    print(f"Input: {text.strip()}")
    result = extract_drug_info(text)
    
    if result:
        drug = result[0]
        print(f"\n✓ Results:")
        print(f"  - raw_drug_name: {drug.raw_drug_name}")
        print(f"  - raw_quantity: {drug.raw_quantity} (Expected: 15)")
        print(f"  - raw_unit: {drug.raw_unit} (Expected: packets)")
        print(f"  - drug_form: {drug.drug_form} (Expected: count)")
        print(f"  - count_total: {drug.count_total}")
        
        assert drug.raw_quantity == 15.0, f"Expected 15, got {drug.raw_quantity}"
        assert drug.raw_unit.lower() == "packets", f"Expected 'packets', got '{drug.raw_unit}'"
        assert drug.drug_form.lower() == "count", f"Expected 'count', got '{drug.drug_form}'"
        assert drug.count_total == 15.0, f"Expected count_total=15, got {drug.count_total}"
        print("\n✅ TEST 2 PASSED: Packet count correctly preserved as count form!")
    else:
        print("❌ TEST 2 FAILED: No extraction result")


def test_mixed_packets():
    """Test: Mixed packets (some with weight, some without)"""
    print("\n" + "="*80)
    print("TEST 3: MIXED PACKETS (With & Without Weight)")
    print("="*80)
    
    text = """
    From A1: 5 packets of Heroin containing 20 grams each, and 
             8 packets of Cocaine with no weights mentioned
    """
    
    print(f"Input: {text.strip()}")
    result = extract_drug_info(text)
    
    if result and len(result) >= 2:
        print(f"\n✓ Extracted {len(result)} drugs:")
        
        # Find Heroin (should be multiplied)
        heroin = next((d for d in result if 'heroin' in d.raw_drug_name.lower()), None)
        if heroin:
            print(f"\n  Heroin:")
            print(f"    - raw_quantity: {heroin.raw_quantity} (Expected: 100, i.e., 5 × 20)")
            print(f"    - raw_unit: {heroin.raw_unit} (Expected: grams)")
            print(f"    - drug_form: {heroin.drug_form}")
            assert heroin.raw_quantity == 100.0, f"Heroin: Expected 100, got {heroin.raw_quantity}"
            assert heroin.raw_unit == "grams", f"Heroin: Expected 'grams', got '{heroin.raw_unit}'"
            print("    ✅ Heroin correctly multiplied!")
        
        # Find Cocaine (should be count)
        cocaine = next((d for d in result if 'cocaine' in d.raw_drug_name.lower()), None)
        if cocaine:
            print(f"\n  Cocaine:")
            print(f"    - raw_quantity: {cocaine.raw_quantity} (Expected: 8)")
            print(f"    - raw_unit: {cocaine.raw_unit} (Expected: packets)")
            print(f"    - drug_form: {cocaine.drug_form} (Expected: count)")
            assert cocaine.raw_quantity == 8.0, f"Cocaine: Expected 8, got {cocaine.raw_quantity}"
            assert cocaine.raw_unit.lower() == "packets", f"Cocaine: Expected 'packets', got '{cocaine.raw_unit}'"
            assert cocaine.drug_form.lower() == "count", f"Cocaine: Expected 'count', got '{cocaine.drug_form}'"
            print("    ✅ Cocaine correctly kept as count!")
        
        print("\n✅ TEST 3 PASSED: Mixed packets handled correctly!")
    else:
        print("❌ TEST 3 FAILED: Expected 2+ extractions")


def test_decimal_weights():
    """Test: Decimal per-packet weights"""
    print("\n" + "="*80)
    print("TEST 4: DECIMAL PER-PACKET WEIGHTS")
    print("="*80)
    
    text = """
    Seized 2.5 packets of Ganja, each containing 10.5 grams from A2
    """
    
    print(f"Input: {text.strip()}")
    result = extract_drug_info(text)
    
    if result:
        drug = result[0]
        print(f"\n✓ Results:")
        print(f"  - raw_quantity: {drug.raw_quantity} (Expected: 26.25, i.e., 2.5 × 10.5)")
        print(f"  - raw_unit: {drug.raw_unit}")
        
        # Allow small floating point differences
        expected = 2.5 * 10.5
        if abs(drug.raw_quantity - expected) < 0.01:
            print(f"✅ TEST 4 PASSED: Decimal packet weights handled correctly!")
        else:
            print(f"❌ TEST 4 FAILED: Expected {expected}, got {drug.raw_quantity}")
    else:
        print("❌ TEST 4 FAILED: No extraction result")


def test_liquid_packets():
    """Test: Liquid drugs in packets"""
    print("\n" + "="*80)
    print("TEST 5: LIQUID DRUGS IN PACKETS")
    print("="*80)
    
    text = """
    Seized 4 packets of Hash Oil, each containing 100ml from A3
    """
    
    print(f"Input: {text.strip()}")
    result = extract_drug_info(text)
    
    if result:
        drug = result[0]
        print(f"\n✓ Results:")
        print(f"  - raw_quantity: {drug.raw_quantity} (Expected: 400, i.e., 4 × 100)")
        print(f"  - raw_unit: {drug.raw_unit} (Expected: ml or milliliter)")
        print(f"  - drug_form: {drug.drug_form} (Expected: liquid)")
        print(f"  - volume_ml: {drug.volume_ml}")
        print(f"  - volume_l: {drug.volume_l}")
        
        if drug.raw_quantity == 400.0 and ('ml' in drug.raw_unit.lower()):
            print(f"✅ TEST 5 PASSED: Liquid packets handled correctly!")
        else:
            print(f"❌ TEST 5 FAILED: Unexpected values")
    else:
        print("❌ TEST 5 FAILED: No extraction result")


def run_all_tests():
    """Run all packet edge case tests"""
    print("\n\n")
    print("#" * 80)
    print("# PACKET EDGE CASE HANDLING - TEST SUITE")
    print("#" * 80)
    
    try:
        test_packet_with_weight()
        test_packet_without_weight()
        test_mixed_packets()
        test_decimal_weights()
        test_liquid_packets()
        
        print("\n\n" + "="*80)
        print("SUMMARY: All packet edge case tests completed!")
        print("="*80)
        
    except AssertionError as e:
        print(f"\n❌ TEST FAILED: {e}")
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    run_all_tests()
