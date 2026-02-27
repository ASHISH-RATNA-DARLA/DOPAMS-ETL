
from typing import List, Optional
from pydantic import BaseModel, Field
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
import sys
import os

# Ensure core is accessible
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from core.llm_service import get_llm, invoke_extraction_with_retry
import config

# --- Data Models ---
# --- Data Models ---
# Controlled vocabulary for drug physical state/form.
# This drives unit validation — the form MUST be consistent with the unit used.
# solid/powder/dry/resin/paste  → kg/grams only
# liquid/syrup/oil/solution     → litres/ml only
# tablet/pill/capsule/paper/seed/count → count (nos/pieces/tablets) only
DRUG_FORM_SOLID   = {'solid', 'dry', 'powder', 'paste', 'resin', 'chunk', 'crystal', 'granule', 'leaf', 'dried', 'compressed'}
DRUG_FORM_LIQUID  = {'liquid', 'syrup', 'oil', 'solution', 'tincture', 'extract', 'concentrate', 'fluid', 'injection'}
DRUG_FORM_COUNT   = {'tablet', 'pill', 'capsule', 'paper', 'blot', 'seed', 'strip', 'sachet', 'ampule', 'vial', 'bottle'}

class DrugExtraction(BaseModel):
    raw_drug_name: Optional[str] = Field(default="Unknown")
    raw_quantity: Optional[float] = 0.0
    raw_unit: Optional[str] = Field(default="Unknown")
    primary_drug_name: str = Field(default="Unknown")
    drug_form: Optional[str] = Field(
        default="Unknown",
        description="solid, liquid, or count forms."
    )
    accused_id: Optional[str] = Field(default=None)
    confidence_score: Optional[float] = Field(default=0.80, description="Confidence out of 1.0 (e.g. 0.95)")
    seizure_worth: Optional[float] = 0.0
    extraction_metadata: dict = Field(default_factory=dict)
    
    # New Schema Calculated Fields
    weight_g: Optional[float] = None
    weight_kg: Optional[float] = None
    volume_ml: Optional[float] = None
    volume_l: Optional[float] = None
    count_total: Optional[float] = None
    is_commercial: bool = False

class CrimeReportExtraction(BaseModel):
    drugs: List[DrugExtraction]

# --- Prompt ---
EXTRACTION_PROMPT = """
You are an expert forensic data analyst. Your task is to extract structured drug seizure information from police `brief facts`.

### I. The AI ETL "Golden Rules" (Strict Constraints)
1. **Zero-Inference Extraction:** Only extract values explicitly stated or clearly implied. If a unit is missing (e.g., "caught with 500 Ganja"), flag it with a lower `confidence_score` (~60) rather than guessing.
2. **Individual Attribution:** Whenever the text describes multiple seizures (e.g., A1, A2), you must attempt to map the `accused_id` (e.g., 'A1', 'A2', 'Accused 1') to that specific drug record. If it's common or unknown, leave it null.
3. **Knowledge Base Matching:** You must use the provided Target Drug Knowledge Base constraints to map drug names natively. Look at `primary_drug_name` rules.
4. **Audit Traceability:** Every extraction must include the specific sentence or snippet of text from the source in `extraction_metadata` under the key `source_sentence`.
5. **High-Precision Preservation:** Extract exact values. Do not round numbers.

### CRITICAL RULES
1. **Container vs Content**: "3 packets, 50 grams" -> raw_quantity = 50. "3 packets of 50 grams each" -> raw_quantity = 150.
2. **Prioritize TOTAL**: Extract the total over the breakdown.
3. **Unknown Drug Names**: Do not extract "unknown", "unidentified", etc. Skip them.
4. **Drug Knowledge Base (Mandatory Reference):**
{drug_knowledge_base}
   - **Instructions:** If ANY part of the text matches a `Raw Name` or a `Standard Name` present in the knowledge base, you MUST set `primary_drug_name` to the corresponding `Standard Name`.
   - If the drug is NOT found in the knowledge base, extract its name normally and set `primary_drug_name` to the capitalized raw extraction.
5. **Drug Form (Physical State)**: Always extract `drug_form`. `solid`, `liquid`, or `count` forms.
6. **Seizure Worth**: Extract monetary value if mentioned, and extract it as float.

### Output Format
Return valid JSON only matching the schema exactly.
Example:
{{
  "drugs": [
    {{
      "raw_drug_name": "Dark green buds",
      "raw_quantity": 400.0,
      "raw_unit": "grams",
      "primary_drug_name": "Ganja",
      "drug_form": "solid",
      "accused_id": "A1",
      "seizure_worth": 10000.0,
      "confidence_score": 95,
      "extraction_metadata": {{"source_sentence": "A1 was caught with 400 grams of dark green buds of Ganja worth Rs.10,000."}}
    }}
  ]
}}


### Input Text
{text}

### FINAL INSTRUCTION
EXTRACT THE DRUGS AS VALID JSON ONLY. DO NOT SUMMARIZE. DO NOT ADD MARKDOWN formatting. JUST THE JSON OBJECT.
"""

def truncate_string(s: str, max_len: int = 50) -> str:
    """Truncates a string to max_len characters."""
    if not s:
        return ""
    if len(s) <= max_len:
        return s
    return s[:max_len]

def standardize_units(drugs: List[DrugExtraction]) -> List[DrugExtraction]:
    """
    Python logic to standardize units into Weight (Kg), Volume (ML), or Count.
    """
    for drug in drugs:
        try:
            # 1. TRUNCATE STRINGS to prevent DB errors (VARCHAR(50))
            drug.raw_unit = truncate_string(drug.raw_unit, 50)
            drug.drug_form = truncate_string(drug.drug_form, 50)
            
            qty = float(drug.raw_quantity) if drug.raw_quantity else 0.0
            unit = drug.raw_unit.lower().strip() if drug.raw_unit else "unknown"
            
            # --- Auto-Classification ---
            
            # WEIGHT UNITS
            if unit in ['g', 'gm', 'gms', 'gram', 'grams', 'grm', 'grms', 'gr']:
                drug.weight_g = qty
                drug.weight_kg = qty / 1000.0
            elif unit in ['kg', 'kgs', 'kilograms', 'kilogram', 'kilo']:
                drug.weight_g = qty * 1000.0
                drug.weight_kg = qty
            elif unit in ['mg', 'milligrams', 'milligram']:
                drug.weight_g = qty / 1000.0
                drug.weight_kg = qty / 1_000_000.0
                
            # Map to specific schema fields based on form
            # Postgres check_has_measurements requires at least ONE of these to not be null.
            if form in DRUG_FORM_SOLID:
                drug.weight_kg = (qty / 1000) if qty > 0 else 0.0
                drug.weight_g = qty if qty > 0 else 0.0
            elif form in DRUG_FORM_LIQUID:
                if unit in {'l', 'liters', 'litre'}: # Use 'unit' not 'unit_lower' as 'unit' is already lowercased
                    drug.volume_l = qty if qty > 0 else 0.0
                    drug.volume_ml = (qty * 1000) if qty > 0 else 0.0
                else:
                    drug.volume_ml = qty if qty > 0 else 0.0
                    drug.volume_l = (qty / 1000) if qty > 0 else 0.0
            elif form in DRUG_FORM_COUNT:
                drug.count_total = qty if qty > 0 else 0.0
            else:
                 # IF UNKNOWN FORM, DEFAULT TO 0 WEIGHT G TO PASS CONSTRAINT
                 if qty > 0 and not drug.weight_g and not drug.volume_ml:
                      drug.count_total = qty
                 else:
                      drug.weight_g = 0.0

            # --- Confidence Score Conversion ---
            # Convert confidence_score from percentage (e.g., 95) to ratio (e.g., 0.95) if it's >= 1.0
            if drug.confidence_score is not None and drug.confidence_score >= 1.0:
                drug.confidence_score = round(drug.confidence_score / 100, 2)

            # --- Name Standardization ---
            name = drug.raw_drug_name.lower().strip()
            # If the primary drug name hasn't been set, set it to the raw name.
            if not drug.primary_drug_name or drug.primary_drug_name == "Unknown":
                drug.primary_drug_name = drug.raw_drug_name
            is_cannabis_variant = any(x in name for x in ['kush', 'og', 'weed', 'cannabis', 'ganja', 'marijuana'])
            if is_cannabis_variant:
                drug.primary_drug_name = "Ganja"
            
            # --- Seizure Worth Conversion: Rupees to Crores ---
            # Convert seizure_worth from rupees to crores (1 crore = 10,000,000 rupees)
            # Example: 55,50,000 rupees → 0.555 crores, 1,00,00,000 rupees → 1.0 crores
            if drug.seizure_worth and drug.seizure_worth > 0:
                drug.seizure_worth = drug.seizure_worth / 10_000_000.0

            # Default form check
            if not drug.drug_form or drug.drug_form.lower() in ['unknown', 'none', 'null']:
                drug.drug_form = "Unknown"
                
        except Exception as e:
            print(f"Standardization error: {e}")
            
    return drugs

def extract_drug_info(text: str, drug_categories: List[dict] = None) -> List[DrugExtraction]:
    """
    Extracts a list of drug information objects from the given text.
    """
    if drug_categories is None:
        drug_categories = []
        
    # Format the drug categories base for the prompt
    kb_lines = []
    if drug_categories:
        kb_lines.append("   | Raw Name | Standard Name |")
        kb_lines.append("   | --- | --- |")
        for cat in drug_categories:
            raw = cat.get('raw_name', 'Unknown')
            std = cat.get('standard_name', 'Unknown')
            kb_lines.append(f"   | {raw} | {std} |")
    else:
        kb_lines.append("   (No knowledge base provided, use standard extraction)")
    
    formatted_kb = "\n".join(kb_lines)
    
    parser = JsonOutputParser(pydantic_object=CrimeReportExtraction)
    prompt = ChatPromptTemplate.from_template(EXTRACTION_PROMPT)
    
    try:
        llm_service = get_llm('extraction')
        llm = llm_service.get_langchain_model()
        chain = prompt | llm | parser
        
        response = invoke_extraction_with_retry(chain, {"text": text, "drug_knowledge_base": formatted_kb}, max_retries=1)
        
        drugs_data = response.get("drugs", []) # Changed from result to response
        valid_drugs = []
        for d in drugs_data:
            try:
                if d.get('raw_quantity') is None: d['raw_quantity'] = 0.0
                if d.get('confidence_score') is None: d['confidence_score'] = 90
                if d.get('seizure_worth') is None: d['seizure_worth'] = 0.0
                if not d.get('raw_drug_name'): d['raw_drug_name'] = "Unknown"
                
                # Check for "None" string
                if str(d.get('raw_quantity')).lower() == "none": d['raw_quantity'] = 0.0
                if str(d.get('seizure_worth')).lower() == "none": d['seizure_worth'] = 0.0
                if d.get('raw_unit') is None: d['raw_unit'] = "Unknown"
                
                # Ensure seizure_worth is a float
                if isinstance(d.get('seizure_worth'), str):
                    # Try to parse string numbers (handle commas)
                    try:
                        d['seizure_worth'] = float(str(d['seizure_worth']).replace(',', ''))
                    except:
                        d['seizure_worth'] = 0.0
                elif d.get('seizure_worth') is None:
                    d['seizure_worth'] = 0.0
                
                valid_drugs.append(DrugExtraction(**d))
            except Exception as e:
                print(f"Skipping invalid: {e}")
        
        # Post-process (Unit Calc)
        return standardize_units(valid_drugs)
        
    except Exception as e:
        print(f"Error during extraction: {e}")
        return []

if __name__ == "__main__":
    test_text = "A1 had 1 packet containing 7 grams Ganja."
    print("Testing extraction...")
    extractions = extract_drug_info(test_text)
    for d in extractions:
        print(d.model_dump_json(indent=2))

