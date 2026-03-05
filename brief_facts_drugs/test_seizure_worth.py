"""
Unit tests for _distribute_seizure_worth() post-processing.
Tests all 8 seizure worth distribution rules.

Usage:
  cd brief_facts_drugs
  python test_seizure_worth.py
"""
import sys
import os
import logging

logging.basicConfig(level=logging.INFO, format="%(message)s")

# Inline the DrugExtraction model and _distribute_seizure_worth for standalone testing
from pydantic import BaseModel, Field
from typing import Optional, List
from collections import defaultdict

logger = logging.getLogger(__name__)


class DrugExtraction(BaseModel):
    raw_drug_name: Optional[str] = "Unknown"
    raw_quantity: Optional[float] = 0.0
    raw_unit: Optional[str] = "Unknown"
    primary_drug_name: Optional[str] = "Unknown"
    drug_form: Optional[str] = "Unknown"
    accused_id: Optional[str] = None
    confidence_score: Optional[float] = 0.80
    seizure_worth: Optional[float] = 0.0
    worth_scope: Optional[str] = "individual"
    extraction_metadata: dict = Field(default_factory=dict)
    weight_g: Optional[float] = None
    weight_kg: Optional[float] = None
    volume_ml: Optional[float] = None
    volume_l: Optional[float] = None
    count_total: Optional[float] = None
    is_commercial: bool = False


# Import the actual function from extractor
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from extractor import _distribute_seizure_worth


def make_drug(drug_name, qty_g, accused, worth, scope="individual"):
    """Helper to create a DrugExtraction with weight_g set."""
    return DrugExtraction(
        raw_drug_name=drug_name,
        primary_drug_name=drug_name,
        raw_quantity=qty_g,
        raw_unit="grams",
        drug_form="solid",
        accused_id=accused,
        seizure_worth=worth,
        worth_scope=scope,
        weight_g=qty_g,
        weight_kg=qty_g / 1000.0 if qty_g else 0.0,
    )


def test_rule1_individual_worth():
    """Rule 1: Individual worth per accused — no change."""
    drugs = [
        make_drug("Ganja", 100, "A1", 50000, "individual"),
        make_drug("Ganja", 100, "A2", 50000, "individual"),
        make_drug("Ganja", 100, "A3", 50000, "individual"),
    ]
    result = _distribute_seizure_worth(drugs)
    assert result[0].seizure_worth == 50000
    assert result[1].seizure_worth == 50000
    assert result[2].seizure_worth == 50000
    print("✅ Rule 1: Individual worth per accused — PASSED")


def test_rule2_multiple_drugs_own_value():
    """Rule 2: Multiple drugs each with own value — no change."""
    drugs = [
        make_drug("Heroin", 50, "A1", 60000, "individual"),
        make_drug("Cocaine", 30, "A1", 40000, "individual"),
    ]
    result = _distribute_seizure_worth(drugs)
    assert result[0].seizure_worth == 60000
    assert result[1].seizure_worth == 40000
    print("✅ Rule 2: Multiple drugs each with own value — PASSED")


def test_rule3_drug_total_split_among_accused():
    """Rule 3: Drug total split among accused proportionally."""
    drugs = [
        make_drug("Ganja", 200, "A1", 15000, "drug_total"),
        make_drug("Ganja", 100, "A2", 15000, "drug_total"),
    ]
    result = _distribute_seizure_worth(drugs)
    assert result[0].seizure_worth == 10000.0, f"Expected 10000, got {result[0].seizure_worth}"
    assert result[1].seizure_worth == 5000.0, f"Expected 5000, got {result[1].seizure_worth}"
    print("✅ Rule 3: Drug total split among accused — PASSED")


def test_rule4_single_drug_total_per_accused():
    """Rule 4: Single drug total with per-accused quantities (the SR Nagar case)."""
    drugs = [
        make_drug("Ganja", 300, "A1", 20000, "drug_total"),
        make_drug("Ganja", 200, "A2", 20000, "drug_total"),
        make_drug("Ganja", 200, "A3", 20000, "drug_total"),
    ]
    result = _distribute_seizure_worth(drugs)
    # 300/700 * 20000 = 8571.43
    assert result[0].seizure_worth == 8571.43, f"Expected 8571.43, got {result[0].seizure_worth}"
    # 200/700 * 20000 = 5714.29
    assert result[1].seizure_worth == 5714.29, f"Expected 5714.29, got {result[1].seizure_worth}"
    assert result[2].seizure_worth == 5714.29, f"Expected 5714.29, got {result[2].seizure_worth}"
    print("✅ Rule 4: Single drug total with per-accused — PASSED")


def test_rule5_multiple_drugs_one_overall_total():
    """Rule 5: Multiple drugs with one overall total."""
    drugs = [
        make_drug("Heroin", 50, None, 100000, "overall_total"),
        make_drug("Cocaine", 30, None, 100000, "overall_total"),
    ]
    result = _distribute_seizure_worth(drugs)
    # 50/80 * 100000 = 62500
    assert result[0].seizure_worth == 62500.0, f"Expected 62500, got {result[0].seizure_worth}"
    # 30/80 * 100000 = 37500
    assert result[1].seizure_worth == 37500.0, f"Expected 37500, got {result[1].seizure_worth}"
    print("✅ Rule 5: Multiple drugs, one overall total — PASSED")


def test_rule6_multiple_drugs_accused_overall_total():
    """Rule 6: Multiple drugs + accused with one overall total."""
    drugs = [
        make_drug("Heroin", 20, "A1", 100000, "overall_total"),
        make_drug("Heroin", 30, "A2", 100000, "overall_total"),
        make_drug("Cocaine", 30, "A3", 100000, "overall_total"),
    ]
    result = _distribute_seizure_worth(drugs)
    # total = 80, 20/80*100000 = 25000
    assert result[0].seizure_worth == 25000.0, f"Expected 25000, got {result[0].seizure_worth}"
    # 30/80*100000 = 37500
    assert result[1].seizure_worth == 37500.0, f"Expected 37500, got {result[1].seizure_worth}"
    assert result[2].seizure_worth == 37500.0, f"Expected 37500, got {result[2].seizure_worth}"
    print("✅ Rule 6: Multiple drugs + accused, overall total — PASSED")


def test_rule7_no_quantities_overall():
    """Rule 7: Multiple drugs with overall total but no quantities."""
    drugs = [
        make_drug("Heroin", 0, None, 100000, "overall_total"),
        make_drug("Cocaine", 0, None, 100000, "overall_total"),
    ]
    # Set weight_g to 0 (simulating no quantity extracted)
    for d in drugs:
        d.weight_g = 0.0
        d.weight_kg = 0.0

    result = _distribute_seizure_worth(drugs)
    # No quantities → assign same total to each
    assert result[0].seizure_worth == 100000.0, f"Expected 100000, got {result[0].seizure_worth}"
    assert result[1].seizure_worth == 100000.0, f"Expected 100000, got {result[1].seizure_worth}"
    print("✅ Rule 7: No quantities, overall total — PASSED")


def test_rule8_no_worth():
    """Rule 8: No worth mentioned — stays 0.0."""
    drugs = [
        make_drug("Ganja", 100, "A1", 0, "individual"),
        make_drug("Ganja", 200, "A2", 0, "individual"),
    ]
    result = _distribute_seizure_worth(drugs)
    assert result[0].seizure_worth == 0.0
    assert result[1].seizure_worth == 0.0
    print("✅ Rule 8: No worth mentioned — PASSED")


def test_mixed_scopes():
    """Edge case: Mixed — some individual, some drug_total in same batch."""
    drugs = [
        make_drug("Ganja", 300, "A1", 20000, "drug_total"),
        make_drug("Ganja", 200, "A2", 20000, "drug_total"),
        make_drug("Cocaine", 50, "A3", 80000, "individual"),
    ]
    result = _distribute_seizure_worth(drugs)
    # Ganja drug_total: 300/500 * 20000 = 12000, 200/500 * 20000 = 8000
    ganja_a1 = next(d for d in result if d.accused_id == "A1")
    ganja_a2 = next(d for d in result if d.accused_id == "A2")
    cocaine_a3 = next(d for d in result if d.accused_id == "A3")
    assert ganja_a1.seizure_worth == 12000.0, f"Expected 12000, got {ganja_a1.seizure_worth}"
    assert ganja_a2.seizure_worth == 8000.0, f"Expected 8000, got {ganja_a2.seizure_worth}"
    assert cocaine_a3.seizure_worth == 80000.0, f"Expected 80000, got {cocaine_a3.seizure_worth}"
    print("✅ Mixed scopes (drug_total + individual) — PASSED")


def test_multiple_drug_totals():
    """Edge case: Multiple drugs each with their own drug_total, split among accused."""
    drugs = [
        make_drug("Heroin", 20, "A1", 50000, "drug_total"),
        make_drug("Heroin", 30, "A2", 50000, "drug_total"),
        make_drug("Cocaine", 30, "A3", 100000, "drug_total"),
    ]
    result = _distribute_seizure_worth(drugs)
    heroin_a1 = next(d for d in result if d.accused_id == "A1")
    heroin_a2 = next(d for d in result if d.accused_id == "A2")
    cocaine_a3 = next(d for d in result if d.accused_id == "A3")
    # Heroin: 20/50 * 50000 = 20000, 30/50 * 50000 = 30000
    assert heroin_a1.seizure_worth == 20000.0, f"Expected 20000, got {heroin_a1.seizure_worth}"
    assert heroin_a2.seizure_worth == 30000.0, f"Expected 30000, got {heroin_a2.seizure_worth}"
    # Cocaine: 30/30 * 100000 = 100000
    assert cocaine_a3.seizure_worth == 100000.0, f"Expected 100000, got {cocaine_a3.seizure_worth}"
    print("✅ Multiple drug_totals (Heroin + Cocaine) — PASSED")


if __name__ == "__main__":
    print("\n=== Seizure Worth Distribution Tests ===\n")
    all_tests = [
        test_rule1_individual_worth,
        test_rule2_multiple_drugs_own_value,
        test_rule3_drug_total_split_among_accused,
        test_rule4_single_drug_total_per_accused,
        test_rule5_multiple_drugs_one_overall_total,
        test_rule6_multiple_drugs_accused_overall_total,
        test_rule7_no_quantities_overall,
        test_rule8_no_worth,
        test_mixed_scopes,
        test_multiple_drug_totals,
    ]
    passed = 0
    failed = 0
    for test in all_tests:
        try:
            test()
            passed += 1
        except AssertionError as e:
            print(f"❌ {test.__name__}: {e}")
            failed += 1
        except Exception as e:
            print(f"❌ {test.__name__}: UNEXPECTED ERROR: {e}")
            failed += 1

    print(f"\n{'='*40}")
    print(f"Results: {passed} passed, {failed} failed out of {len(all_tests)}")
    if failed == 0:
        print("ALL TESTS PASSED ✅")
    else:
        print("SOME TESTS FAILED ❌")
