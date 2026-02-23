import unittest
from extractor import classify_accused_type, detect_gender, extract_accused_info, detect_ccl

class TestExtractorRules(unittest.TestCase):
    
    def test_classify_accused_type(self):
        # Consumer Cases
        self.assertEqual(classify_accused_type("Habitually consuming ganja"), "consumer")
        self.assertEqual(classify_accused_type("Tested positive for ganja"), "consumer")
        self.assertEqual(classify_accused_type("bought for self consumption"), "consumer")
        
        # Peddler Cases
        self.assertEqual(classify_accused_type("Caught selling ganja"), "peddler")
        self.assertEqual(classify_accused_type("Transporting ganja on bike"), "peddler")
        self.assertEqual(classify_accused_type("Found in possession of 2kg ganja"), "peddler")
        self.assertEqual(classify_accused_type("Waiting for customers"), "peddler")
        
        # Supplier Cases (Strict)
        self.assertEqual(classify_accused_type("Supplied ganja to multiple peddlers"), "supplier")
        self.assertEqual(classify_accused_type("Main supplier of the town"), "supplier")
        
        # Organizer
        self.assertEqual(classify_accused_type("Planned the operation and directed others"), "organizer_kingpin")
        
        # Financier
        self.assertEqual(classify_accused_type("Provided funds for purchase"), "financier")
        
        # Manufacturer
        self.assertEqual(classify_accused_type("Cultivated ganja plants in backyard"), "manufacturer")
        
    def test_detect_gender(self):
        self.assertEqual(detect_gender("", "Ramesh S/o Suresh"), "Male")
        self.assertEqual(detect_gender("", "Sita D/o Gita"), "Female")
        self.assertEqual(detect_gender("", "Rahul"), None)
        
    def test_detect_ccl(self):
        self.assertTrue(detect_ccl("Ravi (CCL)", "selling"))
        self.assertTrue(detect_ccl("Ravi", "Juvenile in conflict with law"))
        self.assertFalse(detect_ccl("Ravi", "selling ganja"))

if __name__ == '__main__':
    unittest.main()

