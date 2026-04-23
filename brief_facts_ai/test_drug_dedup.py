#!/usr/bin/env python3
"""
Test script for drug deduplication logic.
Validates that multi-unit seizures are properly consolidated.

Example: Spasmo Proxyvon 32 tablets + 19.648 grams should create 1 entry, not 2.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from extractor_drugs import DrugExtraction, deduplicate_extractions
from db import write_drugs_by_accused_in_memory

def test_multi_unit_consolidation():
    """Test that same drug with different units is consolidated into single entry."""
    # Simulate LLM extracting the same seizure with different unit representations
    drugs = [
        DrugExtraction(
            raw_drug_name="Spasmo Proxyvon Plus tablets",
            primary_drug_name="Spasmo Proxyvon",
            raw_quantity=32.0,
            raw_unit="tablets",
            confidence_score=0.95,
            count_total=32.0,
            weight_g=None,
            extraction_metadata={
                "source_sentence": "four strips of SPASMO PROXYVON PLUS tablets were found concealed therein. Each strip contained 8 tablets, totaling 32 tablets"
            }
        ),
        DrugExtraction(
            raw_drug_name="Spasmo Proxyvon Plus tablets",
            primary_drug_name="Spasmo Proxyvon",
            raw_quantity=19.648,
            raw_unit="grams",
            confidence_score=0.95,
            weight_g=19.648,
            weight_kg=0.019648,
            extraction_metadata={
                "source_sentence": "Each tablet weighing 0.614 grams, with a total weight of 19.648 grams"
            }
        )
    ]

    result = deduplicate_extractions(drugs)

    # Should consolidate to 1 entry
    assert len(result) == 1, f"Expected 1 entry, got {len(result)}"

    consolidated = result[0]

    # Should have both measurements
    assert consolidated.count_total == 32.0, f"Expected count_total=32, got {consolidated.count_total}"
    assert consolidated.weight_g == 19.648, f"Expected weight_g=19.648, got {consolidated.weight_g}"
    assert consolidated.raw_quantity == 32.0, f"Expected raw_quantity=32, got {consolidated.raw_quantity}"
    assert consolidated.raw_unit == "tablets", f"Expected raw_unit='tablets', got {consolidated.raw_unit}"

    # Should preserve both source sentences
    metadata = consolidated.extraction_metadata or {}
    assert "source_sentence" in metadata, "Missing source_sentence"
    assert "alternate_source_sentence" in metadata, "Missing alternate_source_sentence"

    print("✅ Multi-unit consolidation test PASSED")
    print(f"   Input: 2 entries (32 tablets, 19.648g)")
    print(f"   Output: 1 entry with both measurements")
    print(f"   - count_total: {consolidated.count_total}")
    print(f"   - weight_g: {consolidated.weight_g}")


def test_different_suppliers_not_consolidated():
    """Test that same drug from different suppliers creates separate entries."""
    drugs = [
        DrugExtraction(
            raw_drug_name="Ganja",
            primary_drug_name="Ganja",
            raw_quantity=100.0,
            raw_unit="grams",
            confidence_score=0.95,
            weight_g=100.0,
            supplier_name="Raju"
        ),
        DrugExtraction(
            raw_drug_name="Ganja",
            primary_drug_name="Ganja",
            raw_quantity=50.0,
            raw_unit="grams",
            confidence_score=0.95,
            weight_g=50.0,
            supplier_name="Mohan"
        )
    ]

    result = deduplicate_extractions(drugs)

    # Should keep separate because different suppliers
    assert len(result) == 2, f"Expected 2 entries (different suppliers), got {len(result)}"

    print("✅ Different suppliers test PASSED")
    print(f"   Input: 2 Ganja entries from different suppliers")
    print(f"   Output: 2 separate entries (not consolidated)")


def test_exact_duplicates():
    """Test that exact duplicates are removed."""
    drugs = [
        DrugExtraction(
            raw_drug_name="Heroin",
            primary_drug_name="Heroin",
            raw_quantity=10.0,
            raw_unit="grams",
            confidence_score=0.90,
            weight_g=10.0
        ),
        DrugExtraction(
            raw_drug_name="Heroin",
            primary_drug_name="Heroin",
            raw_quantity=10.0,
            raw_unit="grams",
            confidence_score=0.85,
            weight_g=10.0
        )
    ]

    result = deduplicate_extractions(drugs)

    # Should consolidate to 1, keeping the higher confidence
    assert len(result) == 1, f"Expected 1 entry, got {len(result)}"
    assert result[0].confidence_score == 0.90, f"Expected confidence=0.90, got {result[0].confidence_score}"

    print("✅ Exact duplicates test PASSED")
    print(f"   Input: 2 identical Heroin entries")
    print(f"   Output: 1 entry with highest confidence (0.90)")


def test_per_accused_same_drug_not_consolidated():
    """Same drug across different accused must remain separate rows."""
    drugs = [
        DrugExtraction(
            raw_drug_name="Ganja",
            primary_drug_name="Ganja",
            raw_quantity=800.0,
            raw_unit="grams",
            confidence_score=0.95,
            weight_g=800.0,
            extraction_metadata={
                "source_sentence": "seized 800 grams of Ganja from A1"
            }
        ),
        DrugExtraction(
            raw_drug_name="Ganja",
            primary_drug_name="Ganja",
            raw_quantity=50.0,
            raw_unit="grams",
            confidence_score=0.95,
            weight_g=50.0,
            extraction_metadata={
                "source_sentence": "seized 50 grams of Ganja from A2"
            }
        )
    ]

    result = deduplicate_extractions(drugs)

    assert len(result) == 2, f"Expected 2 accused-specific rows, got {len(result)}"


def test_unattributed_fallback_prefers_a1():
    """Fallback assignment should prefer A1 even if rows arrive unsorted."""
    rows = [
        {"accused_id": "3", "person_code": "A3", "full_name": "Third Person", "seq_num": "3", "role_in_crime": "Accused", "drugs": []},
        {"accused_id": "1", "person_code": "A1", "full_name": "First Person", "seq_num": "1", "role_in_crime": "Accused", "drugs": []},
        {"accused_id": "2", "person_code": "A2", "full_name": "Second Person", "seq_num": "2", "role_in_crime": "Accused", "drugs": []},
    ]
    drug_data = [{
        "raw_drug_name": "Ganja",
        "primary_drug_name": "Ganja",
        "raw_quantity": 800.0,
        "raw_unit": "grams",
        "weight_g": 800.0,
        "extraction_metadata": {
            "source_sentence": "Later weighing person weighed the Ganja and it is 800 grams"
        }
    }]

    enriched = write_drugs_by_accused_in_memory(rows, drug_data)
    drugs_by_code = {row["person_code"]: row["drugs"] for row in enriched}

    assert len(drugs_by_code["A1"]) == 1, "Expected fallback drug on A1"
    assert drugs_by_code["A2"] == [], "Expected no fallback drug on A2"
    assert drugs_by_code["A3"] == [], "Expected no fallback drug on A3"


if __name__ == "__main__":
    try:
        test_multi_unit_consolidation()
        test_different_suppliers_not_consolidated()
        test_exact_duplicates()
        print("\n✅ All deduplication tests PASSED")
    except AssertionError as e:
        print(f"\n❌ Test FAILED: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
