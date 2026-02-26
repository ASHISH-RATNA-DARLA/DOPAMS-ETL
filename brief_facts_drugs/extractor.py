
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
    drug_name: str
    quantity_numeric: Optional[float] = 0.0
    quantity_unit: Optional[str] = Field(default="Unknown") # Handle None input
    drug_form: Optional[str] = Field(
        default="Unknown",
        description=(
            "Physical state/form of the drug. Must be one of: "
            "solid, dry, powder, paste, resin, chunk, crystal, leaf, compressed, "
            "liquid, syrup, oil, solution, tincture, extract, concentrate, "
            "tablet, pill, capsule, paper, blot, seed, strip, sachet, bottle. "
            "This MUST match the unit: solid forms use kg/grams, liquids use litres/ml, "
            "tablet/pill/count forms use pieces/nos/tablets."
        )
    )
    packaging_details: Optional[str] = Field(default="")
    confidence_score: Optional[int] = 80
    
    # New Schema Fields
    standardized_weight_kg: Optional[float] = None
    standardized_volume_ml: Optional[float] = None
    standardized_count: Optional[float] = None
    primary_unit_type: Optional[str] = None # 'weight', 'volume', 'count'
    is_commercial: bool = False
    seizure_worth: Optional[float] = 0.0

class CrimeReportExtraction(BaseModel):
    drugs: List[DrugExtraction]

# --- Prompt ---
EXTRACTION_PROMPT = """
You are an expert forensic data analyst. Your task is to extract structured drug seizure information from police `brief facts`.

### CRITICAL RULES
1. **Container vs Content**: 
   - Rule: **NEVER MULTIPLY** the packet count by the weight UNLESS the word "**each**" or "**per**" is explicitly used.
   - "3 packets, 50 grams" -> Quantity = **50** (Total implied).
   - "3 packets of 50 grams **each**" -> Quantity = **150** (Math required).
   - "1 packet containing 7 grams" -> Quantity = **7**.
2. **Prioritize TOTAL**: 
   - If text says "Total 555 Kg seized" and breaks it down, ONLY extract the **Total 555 Kg**. Ignore the breakdown parts.
3. **Distinct Seizures**: Extract items separately ONLY if they belong to **different suspects** or **distinctly different locations/times**.
4. **Ignore Samples**: If text says "Sample of 50g drawn from 1200g", IGNORE the sample. Extract the **ORIGINAL TOTAL** (1200g). Do not just extract the remaining bulk if the Total is known.
5. **Exact Raw Values**: Extract `quantity_numeric` and `quantity_unit` EXACTLY as written (or calculated total). Do NOT convert units yet.
6. **Written Numbers**: Convert "one" -> 1.0, "two" -> 2.0.
7. **Lists**: If the text lists multiple items (1. X, 2. Y, 3. Z), extract **EVERY SINGLE ITEM** in the list.
8. **Unknown Drug Names**: If you cannot identify the specific drug name from the text, **DO NOT** return a record for it. Never use "Unknown", "Unidentified", "Unknown Drug", "Unknown Substance", or similar vague placeholders as `drug_name`. If the drug is unidentifiable, skip it entirely.
9. **Drug Form (Physical State) — MANDATORY**: Always extract `drug_form` as the physical state of the drug itself. Use ONLY these values:
   - **Solid forms** → `dry`, `powder`, `paste`, `resin`, `chunk`, `crystal`, `compressed`, `leaf`, `solid`
   - **Liquid forms** → `liquid`, `syrup`, `oil`, `solution`, `tincture`, `extract`, `concentrate`
   - **Count forms** → `tablet`, `pill`, `capsule`, `paper`, `blot`, `seed`, `strip`, `sachet`
   - If unknown, use `solid` as default for plant-based drugs (Ganja, Heroin, Cocaine, Hashish).
10. **Form-Unit Consistency — STRICT RULE**: The `drug_form` MUST match the unit you use:
    - `dry / powder / paste / resin / leaf / solid / compressed` → unit MUST be `grams` or `kg`. NEVER litres/ml.
    - `liquid / syrup / oil / solution / tincture / extract` → unit MUST be `ml` or `litres`.
    - `tablet / pill / capsule / paper / blot / strip / sachet` → unit MUST be `nos`, `pieces`, or `tablets`.
    - **If text gives a container volume for a solid drug** (e.g. "1 litre tin of ganja"), extract the weight if stated; if only volume is given, set `drug_form=dry`, use the weight unit and set quantity to 0 with `confidence_score` lowered to 70.
11. **Seizure Worth**: Extract `seizure_worth` (monetary value) if mentioned in the text. Look for phrases like:
   - "worth Rs.X" or "worth Rs.X/-" or "worth Rs.X,XX,XXX"
   - "Rs.X" mentioned near the drug seizure
   - "value Rs.X" or "estimated value Rs.X"
   - Convert Indian number format: "Rs.55,50,000/-" -> 5550000.0, "Rs.10,000" -> 10000.0
   - If worth is mentioned for total seizure, divide proportionally if multiple drugs are listed
   - If not mentioned, set to 0.0
12. **Decimal Values**: Be extremely careful with fractional digits. If the text says "1.650 kgs" or "1.5 kgs", you MUST preserve the decimal exactness (e.g., `quantity_numeric`: 1.65, `quantity_unit`: "kgs"). DO NOT remove the decimal point (i.e. DO NOT output 1650 kgs). If the text has commas like "1,650 kgs", treat it as 1650.0.

### Output Format
Return valid JSON only.

Example:
{{
  "drugs": [
    {{
      "drug_name": "Ganja",
      "quantity_numeric": 400.0,
      "quantity_unit": "grams",
      "drug_form": "dry",
      "packaging_details": "white color cover",
      "confidence_score": 95,
      "seizure_worth": 10000.0
    }},
    {{
      "drug_name": "Heroin",
      "quantity_numeric": 2.5,
      "quantity_unit": "kg",
      "drug_form": "powder",
      "packaging_details": "brown tape wrapped",
      "confidence_score": 98,
      "seizure_worth": 500000.0
    }},
    {{
      "drug_name": "Codeine Syrup",
      "quantity_numeric": 250.0,
      "quantity_unit": "ml",
      "drug_form": "syrup",
      "packaging_details": "bottle",
      "confidence_score": 96,
      "seizure_worth": 0.0
    }},
    {{
      "drug_name": "LSD Paper",
      "quantity_numeric": 2.0,
      "quantity_unit": "pieces",
      "drug_form": "paper",
      "packaging_details": "foil",
      "confidence_score": 95,
      "seizure_worth": 0.0
    }},
    {{
      "drug_name": "Alprazolam",
      "quantity_numeric": 120.0,
      "quantity_unit": "tablets",
      "drug_form": "tablet",
      "packaging_details": "strip packing",
      "confidence_score": 97,
      "seizure_worth": 0.0
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
            drug.quantity_unit = truncate_string(drug.quantity_unit, 50)
            drug.drug_form = truncate_string(drug.drug_form, 50)
            # drug.packaging_details is TEXT, so usually safe, but good to be sane? Let's leave TEXT alone.
            
            qty = float(drug.quantity_numeric) if drug.quantity_numeric else 0.0
            unit = drug.quantity_unit.lower().strip() if drug.quantity_unit else "unknown"
            
            # --- Auto-Classification ---
            
            # WEIGHT UNITS
            if unit in ['kg', 'kgs', 'kilograms', 'kilogram', 'kilo']:
                drug.standardized_weight_kg = qty
                drug.primary_unit_type = 'weight'
            elif unit in ['g', 'gm', 'gms', 'gram', 'grams', 'grm', 'grms', 'gr']:
                drug.standardized_weight_kg = qty / 1000.0
                drug.primary_unit_type = 'weight'
            elif unit in ['mg', 'milligrams', 'milligram']:
                drug.standardized_weight_kg = qty / 1_000_000.0
                drug.primary_unit_type = 'weight'
                
            # VOLUME UNITS
            # Note: standardized_volume_ml stores values in LITERS (not milliliters)
            elif unit in ['l', 'ltr', 'liter', 'liters', 'litre', 'litres']:
                drug.standardized_volume_ml = qty  # Already in liters, store as-is
                drug.primary_unit_type = 'volume'
            elif unit in ['ml', 'ml.', 'milliliter', 'milliliters']:
                drug.standardized_volume_ml = qty / 1000.0  # Convert ml to liters
                drug.primary_unit_type = 'volume'
            elif unit in ['bottle', 'bottles']: 
                # Assumption: 1 bottle ~ 100ml? Or treat as count?
                # Safer to treat as COUNT unless size is specified.
                drug.standardized_count = qty
                drug.primary_unit_type = 'count'

            # COUNT UNITS
            elif unit in ['no', 'nos', 'number', 'numbers', 'piece', 'pieces', 'pcs', 'tablet', 'tablets', 'pill', 'pills', 'strip', 'strips', 'box', 'boxes', 'packet', 'packets', 'sachet', 'sachets', 'blot', 'blots', 'dot', 'dots']:
                drug.standardized_count = qty
                drug.primary_unit_type = 'count'
            
            # UNKNOWN / OTHER
            else:
                # Default to count if ambiguous, or leave null?
                # If quantity is present but unit is weird, assume count?
                if qty > 0 and not drug.standardized_weight_kg and not drug.standardized_volume_ml:
                     drug.standardized_count = qty
                     drug.primary_unit_type = 'count'

            # --- Form-to-Unit Cross-Validation ---
            # If the LLM returns an inconsistent form+unit pair, auto-correct it.
            # e.g. drug_form="dry" but unit was "litres" → clear the volume, flag as error.
            form_lower = drug.drug_form.lower().strip() if drug.drug_form else 'unknown'
            if form_lower in DRUG_FORM_SOLID and drug.primary_unit_type == 'volume':
                # Solid drug incorrectly given a volume unit — clear the volume
                print(f"Form-unit mismatch: {drug.drug_name} has form='{drug.drug_form}' but unit was volume. Clearing volume.")
                drug.standardized_volume_ml = None
                drug.primary_unit_type = None  # will be set correctly if weight exists, else stays None
            elif form_lower in DRUG_FORM_LIQUID and drug.primary_unit_type == 'weight':
                # Liquid drug incorrectly given a weight unit — flag but keep (weight may still be valid for resin/paste edge cases)
                print(f"Form-unit mismatch: {drug.drug_name} has form='{drug.drug_form}' but unit was weight — verify manually.")
            elif form_lower in DRUG_FORM_COUNT and drug.primary_unit_type in ('weight', 'volume'):
                # Count-form drug given weight/volume — clear and move to count
                print(f"Form-unit mismatch: {drug.drug_name} has form='{drug.drug_form}' but unit was {drug.primary_unit_type}. Moving to count.")
                count_val = drug.standardized_weight_kg or drug.standardized_volume_ml or drug.quantity_numeric or 0
                drug.standardized_weight_kg = None
                drug.standardized_volume_ml = None
                drug.standardized_count = count_val
                drug.primary_unit_type = 'count'

            # --- Name Standardization ---
            name = drug.drug_name.lower().strip()
            # Ganja/Cannabis variants: only rename to "Ganja" when it is a solid (weight/count).
            # Never rename to "Ganja" if it is a LIQUID form — those are distinct substances
            # (Cannabis Oil, Cannabis Extract, Cannabis Resin, etc.) and must keep their own name.
            is_cannabis_variant = any(x in name for x in ['kush', 'og', 'weed', 'cannabis', 'ganja', 'marijuana'])
            is_liquid_form = form_lower in DRUG_FORM_LIQUID or drug.primary_unit_type == 'volume'
            if is_cannabis_variant and not is_liquid_form:
                drug.drug_name = "Ganja"
            
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

def extract_drug_info(text: str) -> List[DrugExtraction]:
    """
    Extracts a list of drug information objects from the given text.
    """
    parser = JsonOutputParser(pydantic_object=CrimeReportExtraction)
    prompt = ChatPromptTemplate.from_template(EXTRACTION_PROMPT)
    
    try:
        llm_service = get_llm('extraction')
        llm = llm_service.get_langchain_model()
        chain = prompt | llm | parser
        
        response = invoke_extraction_with_retry(chain, {"text": text}, max_retries=1)
        
        drugs_data = response.get("drugs", []) # Changed from result to response
        valid_drugs = []
        for d in drugs_data:
            try:
                # Defaults
                if d.get('packaging_details') is None: d['packaging_details'] = ""
                if d.get('quantity_numeric') is None: d['quantity_numeric'] = 0.0
                if d.get('confidence_score') is None: d['confidence_score'] = 90
                if d.get('seizure_worth') is None: d['seizure_worth'] = 0.0
                
                # Check for "None" string
                if str(d.get('quantity_numeric')).lower() == "none": d['quantity_numeric'] = 0.0
                if str(d.get('seizure_worth')).lower() == "none": d['seizure_worth'] = 0.0
                if d.get('quantity_unit') is None: d['quantity_unit'] = "Unknown"
                
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

