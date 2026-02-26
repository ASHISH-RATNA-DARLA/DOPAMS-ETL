import sys
import os

# Ensure the script can import local modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))

from brief_facts_drugs.extractor import extract_drug_info
from brief_facts_drugs.main import process_crimes

test_text = """IN THE COURT OF THE HONBLE III ADDITIONAL JUNIOR CIVIL JUDGE CUM XXV ADDITIONAL JUDICIAL MAGISTRATE OF FIRST-CLASS RANGAREDDY DISTRICT AT RAJENDRANAGAR 
Honoured Madam,  

             Today i.e., on 22.02.2026 at 19:50 hrs, I have received a complaint from Sri. N. Niranjan, Sub-Inspector of Police, Attapur PS, Hyderabad. Ph. 8712567075 in which follows.

	       Facts of the case are that on 22.02.2026 at 1600 hrs, while present at Attapur Police Station, the officer received credible information that an unknown person would deliver dry ganja to needy persons between 1700–1800 hrs at the backside of Hanuman Temple, Chintalmet, Attapur. Believing the information to be true, the officer informed the Superior Officer under Section 42(2) of the NDPS Act, made a GD entry, reduced the information into writing, and summoned the Clues Team and two panch witnesses. At 1720 hrs, the officer, along with staff C. Sabitha (WPC-11360), Srinivas (HG-1679), mediators, and the Clues Team, proceeded in Government vehicles and reached the spot at 1730 hrs. At about 1750 hrs, the staff identified and detained one person carrying a blue college bag. He disclosed his identity as Md. Imtiyaz @ Imtiyaz Ali @ Faizuddin @ Faiz, aged 17 years, plumber, native of Madhubani, Bihar, presently residing at Chintalmet, Attapur. On questioning, he confessed that he was carrying dry ganja in the bag for sale, which he had purchased from Deepak @ Vakil of Madhubani, Bihar. As he stated that he was not carrying any contraband on his person, the procedure under Section 50 of the NDPS Act was not invoked. At 1830 hrs, in the presence of mediators, his confession-cum-seizure panchanama was recorded. Upon opening the bag, a black polythene cover containing flowering and fruiting tops of ganja, small transparent covers, and a weighing machine were found. The Clues Team confirmed the substance as ganja. A total of 1.030 kg of ganja was seized, packed, sealed, and marked as MO-1. The blue bag (MO-2), small covers (MO-3), weighing machine (MO-4), and empty black polythene cover (MO-5) were also seized, packed, and sealed with the signatures of the CCL, panchas, and the officer. The seized property was deposited in Form-1 with HC-3191 Ravi Kumar. The CCL was produced before the concerned authority for necessary action. The supplier, Deepak @ Vakil of Madhubani District, Bihar, is absconding. Hence, the complainant requested to take necessary legal action as per law. 

Hence the FIR
THE ORIGINAL COMPLAINT IS ENCLOSED HERE WITH
"""

print("=== STARTING EXTRACTION ===")
extractions = extract_drug_info(test_text)

print(f"Extracted {len(extractions)} drug objects:")
for drug in extractions:
    print(drug.model_dump_json(indent=2))

print("\n=== CHECKING MAIN.PY INSERTION LOGIC ===")
INVALID_DRUG_NAMES = {
    'unknown', 'unidentified', 'unknown drug', 'unknown substance',
    'unknown tablet', 'unknown powder', 'unknown liquid', 'n/a', 'none', ''
}

count = 0
for drug in extractions:
    # Guard: reject vague/placeholder drug names
    if drug.drug_name.strip().lower() in INVALID_DRUG_NAMES:
        print(f"SKIPPED -> Invalid drug name '{drug.drug_name}'")
        continue

    # User Requirement: Confidence score check (90+)
    if drug.confidence_score >= 90:
        print(f"SUCCESS -> Would insert {drug.drug_name} into DB")
        count += 1
    else:
        print(f"SKIPPED -> Low confidence extraction ({drug.confidence_score}%): {drug.drug_name}")

if count == 0:
    print("\nRESULT: This Crime ID would be marked as NO_DRUGS_DETECTED in the DB!")
else:
    print(f"\nRESULT: Successfully would insert {count} drugs into DB.")
