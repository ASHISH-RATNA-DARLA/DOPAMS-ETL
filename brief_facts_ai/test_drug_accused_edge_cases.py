#!/usr/bin/env python3
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from db import write_drugs_by_accused_in_memory
from extractor_drugs import DrugExtraction, deduplicate_extractions


def test_per_accused_same_drug_not_consolidated():
    drugs = [
        DrugExtraction(
            raw_drug_name="Ganja",
            primary_drug_name="Ganja",
            raw_quantity=800.0,
            raw_unit="grams",
            confidence_score=0.95,
            weight_g=800.0,
            extraction_metadata={
                "source_sentence": "seized 800 grams of Ganja from A1",
            },
        ),
        DrugExtraction(
            raw_drug_name="Ganja",
            primary_drug_name="Ganja",
            raw_quantity=50.0,
            raw_unit="grams",
            confidence_score=0.95,
            weight_g=50.0,
            extraction_metadata={
                "source_sentence": "seized 50 grams of Ganja from A2",
            },
        ),
    ]

    result = deduplicate_extractions(drugs)

    assert len(result) == 2


def test_metadata_accused_ref_prevents_cross_accused_merge():
    drugs = [
        DrugExtraction(
            raw_drug_name="Ganja",
            primary_drug_name="Ganja",
            raw_quantity=50.0,
            raw_unit="grams",
            confidence_score=0.95,
            weight_g=50.0,
            extraction_metadata={
                "source_sentence": "seized 50 grams dry Ganja",
                "accused_ref": "A2",
            },
        ),
        DrugExtraction(
            raw_drug_name="Ganja",
            primary_drug_name="Ganja",
            raw_quantity=50.0,
            raw_unit="grams",
            confidence_score=0.95,
            weight_g=50.0,
            extraction_metadata={
                "source_sentence": "seized 50 grams dry Ganja",
                "accused_ref": "A3",
            },
        ),
    ]

    result = deduplicate_extractions(drugs)

    assert len(result) == 2


def test_unattributed_fallback_prefers_a1():
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
            "source_sentence": "Later weighing person weighed the Ganja and it is 800 grams",
        },
    }]

    enriched = write_drugs_by_accused_in_memory(rows, drug_data)
    drugs_by_code = {row["person_code"]: row["drugs"] for row in enriched}

    assert len(drugs_by_code["A1"]) == 1
    assert drugs_by_code["A2"] == []
    assert drugs_by_code["A3"] == []
