import unittest
from unittest.mock import patch

from extractor import (
    classify_accused_type,
    clean_accused_name,
    detect_ccl,
    detect_gender,
    extract_accused_info,
    normalize_gender_value,
)


class TestExtractorRules(unittest.TestCase):

    def test_classify_accused_type(self):
        self.assertEqual(classify_accused_type("Habitually consuming ganja"), "consumer")
        self.assertEqual(classify_accused_type("Tested positive for ganja"), "consumer")
        self.assertEqual(classify_accused_type("bought for self consumption"), "consumer")

        self.assertEqual(classify_accused_type("Caught selling ganja"), "peddler")
        self.assertEqual(classify_accused_type("Transporting ganja on bike"), "peddler")
        self.assertEqual(classify_accused_type("Found in possession of 2kg ganja"), "peddler")
        self.assertEqual(classify_accused_type("Waiting for customers"), "peddler")

        self.assertEqual(classify_accused_type("Supplied ganja to multiple peddlers"), "supplier")
        self.assertEqual(classify_accused_type("Main supplier of the town"), "supplier")
        self.assertEqual(classify_accused_type("Planned the operation and directed others"), "organizer_kingpin")
        self.assertEqual(classify_accused_type("Provided funds for purchase"), "financier")
        self.assertEqual(classify_accused_type("Cultivated ganja plants in backyard"), "manufacturer")

    def test_normalize_gender_value(self):
        self.assertEqual(normalize_gender_value("male"), "Male")
        self.assertEqual(normalize_gender_value("Female"), "Female")
        self.assertEqual(normalize_gender_value("trans gender"), "Transgender")
        self.assertIsNone(normalize_gender_value("not available"))

    def test_detect_gender(self):
        self.assertEqual(detect_gender("", "Ramesh S/o Suresh"), "Male")
        self.assertEqual(detect_gender("", "Sita D/o Gita"), "Female")
        self.assertEqual(detect_gender("", "Rahul"), "Male")
        self.assertEqual(detect_gender("", "Banitha"), "Female")
        self.assertEqual(detect_gender("", "Alex"), None)
        self.assertEqual(detect_gender("", "Person Name", "transgender"), "Transgender")

    def test_detect_ccl(self):
        self.assertTrue(detect_ccl("Ravi (CCL)", "selling"))
        self.assertTrue(detect_ccl("Ravi", "Juvenile in conflict with law"))
        self.assertFalse(detect_ccl("Ravi", "selling ganja"))

    def test_clean_accused_name_handles_common_prefix_variants(self):
        self.assertEqual(clean_accused_name("A.1 Vishal Singh S/o Baldev Singh"), "Vishal Singh")
        self.assertEqual(clean_accused_name("A1/Jeeban Panigrahy s/o Ramachandra"), "Jeeban Panigrahy")

    def test_extract_accused_info_returns_none_on_pass1_failure(self):
        with patch("extractor.extract_accused_names_pass1", return_value=None):
            self.assertIsNone(extract_accused_info("sample text"))

    def test_extract_accused_info_uses_raw_name_for_gender_detection(self):
        with patch("extractor.extract_accused_names_pass1", return_value=["A.1 Sita D/o Gita"]), patch(
            "extractor.extract_details_pass2", return_value=[]
        ):
            result = extract_accused_info("sample text")

        self.assertIsNotNone(result)
        self.assertEqual(result[0].full_name, "Sita")
        self.assertEqual(result[0].gender, "Female")


if __name__ == '__main__':
    unittest.main()
