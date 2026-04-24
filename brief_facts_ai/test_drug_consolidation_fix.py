#!/usr/bin/env python3
"""
Test to verify that drug deduplication consolidates the same drug
across different accused mentions into a single entry for Case B, 
while preserving explicit packet mappings for Case A.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from extractor_drugs import deduplicate_extractions, DrugExtraction, _drop_redundant_total_rows


def test_case_a_explicit_mapping_kept_separate():
    """Case A: Packets explicitly associated to different accused are kept separate."""

    extractions = [
        DrugExtraction(
            primary_drug_name="Ganja",
            raw_drug_name="ganja",
            raw_quantity=3.372,
            raw_unit="kg",
            confidence_score=0.95,
            extraction_metadata={"source_sentence": "M1 3.372 kg ganja seized from A-1", "accused_ref": "A-1"},
        ),
        DrugExtraction(
            primary_drug_name="Ganja",
            raw_drug_name="ganja",
            raw_quantity=2.470,
            raw_unit="kg",
            confidence_score=0.95,
            extraction_metadata={"source_sentence": "M2 2.470 kg ganja seized from A-2", "accused_ref": "A-2"},
        ),
    ]

    print(f"Before dedup: {len(extractions)} Ganja extractions (different accused)")
    
    # Run deduplication
    deduped = deduplicate_extractions(extractions)

    print(f"\nAfter dedup: {len(deduped)} Ganja entry(ies)")
    assert len(deduped) == 2, f"Expected 2 Ganja entries (Case A), got {len(deduped)}"

    print("\n✓ TEST PASSED: Case A - Explicit per-packet mapping kept separate")
    return True


def test_case_b_consolidation():
    """Case B: Packets with no/same accused mapping are consolidated, and quantities are summed."""

    # Simulate _extract_explicit_packet_rows creating these packet rows
    packet_rows = [
        {
            'primary_drug_name': "Ganja",
            'raw_drug_name': "ganja",
            'raw_quantity': 3.372,
            'raw_unit': "kg",
            'extraction_metadata': {"source_sentence": "packet M1 was 3.372 kg", "explicit_packet_row": True, "packet_index": 1},
        },
        {
            'primary_drug_name': "Ganja",
            'raw_drug_name': "ganja",
            'raw_quantity': 2.470,
            'raw_unit': "kg",
            'extraction_metadata': {"source_sentence": "packet M2 was 2.470 kg", "explicit_packet_row": True, "packet_index": 2},
        }
    ]

    drugs = [DrugExtraction(**p) for p in packet_rows]
    
    # Simulate LLM extracting total row
    total_row = DrugExtraction(
        primary_drug_name="Ganja",
        raw_drug_name="ganja",
        raw_quantity=5.842,
        raw_unit="kg",
        confidence_score=0.90,
        extraction_metadata={"source_sentence": "total 5.842 kg ganja"}
    )
    drugs.append(total_row)

    print(f"\nBefore _drop_redundant_total_rows: {len(drugs)} drugs")
    
    # Run redundant total rows logic
    filtered_drugs = _drop_redundant_total_rows(drugs, packet_rows)
    
    print(f"After _drop_redundant_total_rows: {len(filtered_drugs)} drugs")
    assert len(filtered_drugs) == 1, f"Expected 1 Ganja entry (Case B total), got {len(filtered_drugs)}"
    
    # Run deduplication
    deduped = deduplicate_extractions(filtered_drugs)

    print(f"After dedup: {len(deduped)} Ganja entry(ies)")
    assert len(deduped) == 1, f"Expected 1 Ganja entry (Case B), got {len(deduped)}"
    assert deduped[0].raw_quantity == 5.842, f"Expected quantity 5.842, got {deduped[0].raw_quantity}"

    print("\n✓ TEST PASSED: Case B - Packets with no explicit mapping are consolidated into total")
    return True


def test_different_drugs_kept_separate():
    """Test that different drugs remain separate even without accused_ref in key."""

    extractions = [
        DrugExtraction(
            primary_drug_name="Ganja",
            raw_drug_name="ganja",
            raw_quantity=6.0,
            raw_unit="kg",
        ),
        DrugExtraction(
            primary_drug_name="Heroin",
            raw_drug_name="heroin",
            raw_quantity=100.0,
            raw_unit="grams",
        ),
        DrugExtraction(
            primary_drug_name="MDMA",
            raw_drug_name="mdma",
            raw_quantity=50.0,
            raw_unit="tablets",
        ),
    ]

    deduped = deduplicate_extractions(extractions)

    print(f"\nDifferent drugs: {len(extractions)} → {len(deduped)}")
    assert len(deduped) == 3, f"Expected 3 separate drugs, got {len(deduped)}"

    drug_names = {d.primary_drug_name for d in deduped}
    assert drug_names == {"Ganja", "Heroin", "MDMA"}

    print("✓ TEST PASSED: Different drugs remain separate")
    return True


def test_same_drug_different_suppliers():
    """Test that same drug with different suppliers remains separate."""

    extractions = [
        DrugExtraction(
            primary_drug_name="Heroin",
            raw_drug_name="heroin",
            raw_quantity=100.0,
            raw_unit="grams",
            supplier_name="A-1",
        ),
        DrugExtraction(
            primary_drug_name="Heroin",
            raw_drug_name="heroin",
            raw_quantity=50.0,
            raw_unit="grams",
            supplier_name="A-2",  # Different supplier
        ),
    ]

    deduped = deduplicate_extractions(extractions)

    print(f"\nSame drug, different suppliers: {len(extractions)} → {len(deduped)}")
    assert len(deduped) == 2, f"Expected 2 separate entries (different suppliers), got {len(deduped)}"

    print("✓ TEST PASSED: Same drug with different suppliers kept separate")
    return True


if __name__ == "__main__":
    try:
        test_case_a_explicit_mapping_kept_separate()
        test_case_b_consolidation()
        test_different_drugs_kept_separate()
        test_same_drug_different_suppliers()
        print("\n✅ ALL TESTS PASSED")
    except AssertionError as e:
        print(f"\n❌ TEST FAILED: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ UNEXPECTED ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
