
from extractor import extract_accused_info
import json
import logging

# Configure logging to show info level
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def test_extraction():
    # Sample text from ref.txt
    sample_text = """
    On 14.09.2022, apprehended two persons. On enquiry, they revealed their details as: 
    A.1 Vishal Singh Hazari, S/o. Baldev Singh Hazari, aged about 25 yrs, Occ: Ganja Business, 
    R/o.H.No.13-2-266/38, near Gummas, Shivlal Nagar, Mangalhat. 
    A.2 Santosh Singh, S/o.Late Digamber Singh, aged 29 yrs, Occ: Ganja selling.
    Further stated that his parents A.3 Baldev Singh Hazari and A.4 Smt.Sandhya Bai are indulging in Ganja selling.
    They supply to A.5 Abhishek Singh and A.6 Vamshi who are peddlers.
    """
    
    print("Running Extraction Test...")
    results = extract_accused_info(sample_text)
    
    print(f"\nExtracted {len(results)} accused.")
    for idx, item in enumerate(results, 1):
        print(f"\n--- Accused {idx} ---")
        print(json.dumps(item.model_dump(), indent=2))
        
        # Simple assertions
        if idx == 1:
            assert "Vishal" in item.full_name
            assert item.age == 25
        if idx == 5:
            assert "Abhishek" in item.full_name
            assert item.accused_type == "peddler"

if __name__ == "__main__":
    test_extraction()

