#!/usr/bin/env python3
"""
Test to verify that drug deduplication consolidates the same drug
across different accused mentions into a single entry.

SCENARIO: FIR mentions "Ganja" in relation to:
- A-1: initially seized 6 Kg
- A-3: sold 1 Kg in Hyderabad
- A-4: main supplier (motorcycle owner)

EXPECTED: Single "Ganja" entry with source sentences from all 3 mentions.
PREVIOUS BUG: 5 separate entries created (different A-codes in dedup key).
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from extractor_drugs import deduplicate_extractions, DrugExtraction


def test_ganja_consolidation():
    """Test that same drug with different accused mentions gets consolidated."""

    # Simulate 5 LLM extractions of Ganja (as might come from different sentences)
    extractions = [
        DrugExtraction(
            primary_drug_name="Ganja",
            raw_drug_name="ganja",
            raw_quantity=6.0,
            raw_unit="kg",
            confidence_score=0.95,
            extraction_metadata={"source_sentence": "A-1 had 6 Kg of Ganja seized"},
        ),
        DrugExtraction(
            primary_drug_name="Ganja",
            raw_drug_name="ganja",
            raw_quantity=1.0,
            raw_unit="kg",
            confidence_score=0.90,
            extraction_metadata={"source_sentence": "A-3 sold 1 Kg of Ganja in Hyderabad"},
        ),
        DrugExtraction(
            primary_drug_name="Ganja",
            raw_drug_name="ganja",
            raw_quantity=6.0,
            raw_unit="kg",
            confidence_score=0.92,
            supplier_name="A-4",
            extraction_metadata={"source_sentence": "A-4 main supplier, motorcycle owner, 6 Kg total"},
        ),
        DrugExtraction(
            primary_drug_name="Ganja",
            raw_drug_name="ganja",
            raw_quantity=5.0,
            raw_unit="kg",
            confidence_score=0.93,
            extraction_metadata={"source_sentence": "5 Kg remaining with A-1"},
        ),
        DrugExtraction(
            primary_drug_name="Ganja",
            raw_drug_name="ganja",
            raw_quantity=0.0,
            raw_unit="unknown",
            confidence_score=0.85,
            extraction_metadata={"source_sentence": "Ganja mentioned as primary drug in NDPS charges"},
        ),
    ]

    print(f"Before dedup: {len(extractions)} Ganja extractions")
    for i, drug in enumerate(extractions, 1):
        meta = drug.extraction_metadata or {}
        supplier = drug.supplier_name or "unknown"
        print(f"  {i}. {drug.raw_quantity} {drug.raw_unit}, supplier={supplier}, "
              f"conf={drug.confidence_score}, source='{meta.get('source_sentence', '')}'")

    # Run deduplication (FIX: should now consolidate all 5 into 1)
    deduped = deduplicate_extractions(extractions)

    print(f"\nAfter dedup: {len(deduped)} Ganja entry(ies)")
    assert len(deduped) == 1, f"Expected 1 Ganja entry, got {len(deduped)}"

    drug = deduped[0]
    print(f"  Drug name: {drug.primary_drug_name}")
    print(f"  Raw quantity: {drug.raw_quantity} {drug.raw_unit}")
    print(f"  Confidence: {drug.confidence_score}")

    # Check that consolidated sources are tracked
    meta = drug.extraction_metadata or {}
    consolidated_sources = meta.get('consolidated_sources', [])
    print(f"  Consolidated sources ({len(consolidated_sources)}):")
    for source in consolidated_sources:
        print(f"    - {source}")

    # Verify we have sources from all 5 extractions
    assert len(consolidated_sources) >= 4, (
        f"Expected at least 4 consolidated sources, got {len(consolidated_sources)}"
    )

    print("\n✓ TEST PASSED: Same drug consolidated across different accused mentions")
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
        test_ganja_consolidation()
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
