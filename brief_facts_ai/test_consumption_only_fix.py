#!/usr/bin/env python3
"""
Test: Consumption-Only Filter (Rule 13 + Post-Filter)

Verifies that drugs are NOT extracted when:
- Text mentions drug consumption (tested positive, urine test)
- But NO seizure is mentioned (no seized/confiscated/arrested with)
"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from extractor_drugs import filter_consumption_only_drugs


class MockDrugExtraction:
    """Mock DrugExtraction for testing."""
    def __init__(self, primary_drug_name, source_sentence):
        self.primary_drug_name = primary_drug_name
        self.extraction_metadata = {
            'source_sentence': source_sentence
        }


def test_case_1_consumption_only_no_seizure():
    """
    Test Case 1: Consumption-only (no seizure)
    Expected: Filter out (empty return)
    """
    print("\n" + "="*70)
    print("TEST 1: Consumption-Only (No Seizure)")
    print("="*70)

    text = "Accused tested positive for ganja in urine test. No drugs seized."

    drugs = [
        MockDrugExtraction(
            'GANJA',
            'tested positive for ganja'
        )
    ]

    result = filter_consumption_only_drugs(drugs, text)

    print(f"Input: {text}")
    print(f"Drug extracted: GANJA (tested positive)")
    print(f"Expected: FILTERED OUT (empty list)")
    print(f"Actual: {len(result)} entries")
    print(f"Status: {'✓ PASS' if len(result) == 0 else '✗ FAIL'}")

    return len(result) == 0


def test_case_2_consumption_and_seizure():
    """
    Test Case 2: Consumption + Seizure
    Expected: Keep (has seizure marker)
    """
    print("\n" + "="*70)
    print("TEST 2: Consumption + Seizure Mentioned")
    print("="*70)

    text = "Accused tested positive for heroin. 50 tablets of heroin were seized."

    drugs = [
        MockDrugExtraction(
            'HEROIN',
            'tested positive and 50 tablets of heroin were seized'
        )
    ]

    result = filter_consumption_only_drugs(drugs, text)

    print(f"Input: {text}")
    print(f"Drug extracted: HEROIN (tested positive + seized)")
    print(f"Expected: KEPT (seizure mentioned)")
    print(f"Actual: {len(result)} entries")
    print(f"Status: {'✓ PASS' if len(result) == 1 else '✗ FAIL'}")

    return len(result) == 1


def test_case_3_seizure_only():
    """
    Test Case 3: Seizure only (no consumption)
    Expected: Keep (has seizure)
    """
    print("\n" + "="*70)
    print("TEST 3: Seizure Only (No Consumption Mention)")
    print("="*70)

    text = "2kg ganja was seized from the accused's residence."

    drugs = [
        MockDrugExtraction(
            'GANJA',
            'seized from residence'
        )
    ]

    result = filter_consumption_only_drugs(drugs, text)

    print(f"Input: {text}")
    print(f"Drug extracted: GANJA (seized)")
    print(f"Expected: KEPT (seizure is clear)")
    print(f"Actual: {len(result)} entries")
    print(f"Status: {'✓ PASS' if len(result) == 1 else '✗ FAIL'}")

    return len(result) == 1


def test_case_4_multiple_drugs_mixed():
    """
    Test Case 4: Multiple drugs - some consumption-only, some seized
    Expected: Filter consumption-only, keep seized
    """
    print("\n" + "="*70)
    print("TEST 4: Multiple Drugs (Mixed Consumption + Seizure)")
    print("="*70)

    text = "Accused A tested positive for ganja. Accused B had 100 tablets of heroin seized."

    drugs = [
        MockDrugExtraction(
            'GANJA',
            'tested positive for ganja'
        ),
        MockDrugExtraction(
            'HEROIN',
            '100 tablets of heroin were seized'
        )
    ]

    result = filter_consumption_only_drugs(drugs, text)

    print(f"Input: {text}")
    print(f"Drugs extracted: GANJA (consumption-only) + HEROIN (seized)")
    print(f"Expected: 1 entry (only HEROIN, GANJA filtered)")
    print(f"Actual: {len(result)} entries")
    if result:
        print(f"Kept drugs: {[d.primary_drug_name for d in result]}")
    print(f"Status: {'✓ PASS' if len(result) == 1 and result[0].primary_drug_name == 'HEROIN' else '✗ FAIL'}")

    return len(result) == 1 and result[0].primary_drug_name == 'HEROIN'


def test_case_5_drug_test_no_seizure():
    """
    Test Case 5: Drug detection test positive, no seizure
    Expected: Filter out
    """
    print("\n" + "="*70)
    print("TEST 5: Drug Detection Test (No Seizure)")
    print("="*70)

    text = "All three accused found positive in drug detection test. No narcotics confiscated."

    drugs = [
        MockDrugExtraction(
            'NDPS_SUBSTANCES',
            'found positive in drug detection test'
        )
    ]

    result = filter_consumption_only_drugs(drugs, text)

    print(f"Input: {text}")
    print(f"Drug extracted: NDPS_SUBSTANCES (detection test)")
    print(f"Expected: FILTERED OUT (test-only, no seizure)")
    print(f"Actual: {len(result)} entries")
    print(f"Status: {'✓ PASS' if len(result) == 0 else '✗ FAIL'}")

    return len(result) == 0


def main():
    """Run all tests."""
    print("\n" + "="*70)
    print("CONSUMPTION-ONLY FILTER TEST SUITE")
    print("="*70)

    tests = [
        ("Test 1: Consumption-Only", test_case_1_consumption_only_no_seizure),
        ("Test 2: Consumption + Seizure", test_case_2_consumption_and_seizure),
        ("Test 3: Seizure Only", test_case_3_seizure_only),
        ("Test 4: Multiple Drugs Mixed", test_case_4_multiple_drugs_mixed),
        ("Test 5: Drug Test No Seizure", test_case_5_drug_test_no_seizure),
    ]

    results = []
    for name, test_func in tests:
        try:
            result = test_func()
            results.append((name, result))
        except Exception as e:
            print(f"✗ EXCEPTION: {e}")
            results.append((name, False))

    # Summary
    print("\n" + "="*70)
    print("SUMMARY")
    print("="*70)

    passed = sum(1 for _, r in results if r)
    total = len(results)

    for name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"{status} - {name}")

    print(f"\nTotal: {passed}/{total} tests passed")
    print("="*70 + "\n")

    return passed == total


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
