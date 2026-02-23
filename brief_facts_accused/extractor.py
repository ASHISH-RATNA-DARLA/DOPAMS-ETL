from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from langchain_ollama import ChatOllama
import config
import logging

# Configure Logging
logger = logging.getLogger(__name__)

# --- Data Models ---

# Final Output Model
class AccusedExtraction(BaseModel):
    full_name: str = Field(description="Full name of the accused")
    alias_name: Optional[str] = Field(default=None, description="Alias or nickname")
    age: Optional[int] = Field(default=None)
    gender: Optional[str] = Field(default=None)
    occupation: Optional[str] = Field(default=None)
    address: Optional[str] = Field(default=None)
    phone_numbers: Optional[str] = Field(default=None, description="Comma separated phone numbers")
    
    role_in_crime: Optional[str] = Field(default=None, description="Specific action or role in this crime")
    key_details: Optional[str] = Field(default=None, description="Important context or facts regarding this person")
    
    accused_type: Optional[str] = Field(
        default="unknown", 
        description="One of: peddler, consumer, supplier, harbourer, organizer_kingpin, processor, financier, manufacturer, unknown"
    )
    status: Optional[str] = Field(default="unknown", description="arrested, absconding, or unknown")
    is_ccl: bool = Field(default=False, description="Is Child in Conflict with Law (Juvenile)")

# Pass 1 Intermediate Model
class AccusedNamesResponse(BaseModel):
    accused_names: List[str] = Field(description="List of identified accused names")

# Pass 2 Intermediate Model
import re

# Pass 2 Intermediate Model
class AccusedDetails(BaseModel):
    full_name: str = Field(description="Name of the accused")
    alias_name: Optional[str] = Field(description="Alias or nickname")
    age: Optional[int] = Field(description="Age (integer)")
    gender: Optional[str] = Field(description="Male/Female")
    occupation: Optional[str] = Field(description="Job or Profession")
    address: Optional[str] = Field(description="Full address or residence")
    phone_numbers: Optional[str] = Field(description="Phone numbers")
    role_in_crime: Optional[str] = Field(default=None, description="Factual action or role description")

def clean_accused_name(name: str) -> str:
    """
    Normalizes accused name by removing metadata, aliases, and prefixes.
    Ex: "A-1) John Doe@Rocky s/o Smith" -> "John Doe"
    """
    if not name:
        return ""
    
    # 1. Remove Prefix like "A-1", "1.", "A1)"
    name = re.sub(r'^(A-?\d+|[0-9]+)[\)\.\:\s]+', '', name, flags=re.IGNORECASE)
    
    # 2. Split at common separators causing metadata leak
    # Split at @ (alias)
    if "@" in name:
        name = name.split("@")[0]
        
    # Split at relational indicators
    indicators = [" s/o ", " d/o ", " w/o ", " h/o ", " son of ", " daughter of ", " wife of "]
    for ind in indicators:
        if ind in name.lower():
            # Use regex to split case-insensitively
            name = re.split(re.escape(ind), name, flags=re.IGNORECASE)[0]
            
    # Split at address/metadata markers
    markers = [" r/o ", " h.no ", " h no ", " age:", " caste:", " occ:", " cell:", " phone:"]
    for m in markers:
        if m in name.lower():
            name = re.split(re.escape(m), name, flags=re.IGNORECASE)[0]

    # 3. Cleanup
    name = name.strip()
    # Remove trailing parenthesis if any (e.g. "John (Absconding)")
    name = re.sub(r'\s*\(.*?\)$', '', name)
    
    return name.strip()

class AccusedDetailsResponse(BaseModel):
    accused_details: List[AccusedDetails]

# --- Prompts ---

PASS1_PROMPT = """You are a criminal law data extraction expert.

=====================================
TASK: ACCUSED IDENTIFICATION ONLY
=====================================

From the input FIR / Brief Facts text:

1. Extract ONLY persons who are ACCUSED or SUSPECTED in the crime.
2. Include:
   - Persons apprehended, arrested, confessed, absconding
   - Persons referred as A1, A2, accused, suspect, JCL/CCL
   - **Crucial**: Include Suppliers / Sources mentioned in confessions, even if not arrested or "absconding".

=====================================
STRICT EXCLUSIONS (MANDATORY)
=====================================
DO NOT extract:
- Police officers (SI, CI, Inspector, PC, HC, HG, WPC, ASI, SHO)
- Complainants
- Clues team members
- Panchas / Mediators
- Government officials (GPO, MRO, Tahsildar, GHMC, Revenue staff)
- Witnesses or drivers

If a person’s occupation is:
GPO, MRO, Panch, Official, Police -> IGNORE COMPLETELY.

=====================================
OUTPUT RULES
=====================================
- Output ONLY accused persons.
- Extract ONLY the full name string as it appears.
- DO NOT infer roles, types, gender, age, or status.
- If no accused exist, return an empty list.

Input Text:
{text}

{format_instructions}
"""

PASS2_PROMPT = """You are a criminal investigation analyst.

=====================================
TASK: DETAILED ACCUSED PROFILING
=====================================

You are given:
1. FIR / Brief Facts text
2. A list of identified accused persons

For EACH accused person in the list:
1. Search for their "A-Tag" (e.g. A-1, A-2).
2. FIND ALL MENTIONS of that Tag in the text.
3. COPY SHARED ACTIONS:
   - If text says "A-1 and A-2 purchased...", assign "Purchased..." to BOTH A-1 and A-2.
   - If text says "A-1 to A-3 were apprehended...", assign "Apprehended" to A-1, A-2, AND A-3.
   - If text says "They went to...", identify who "They" refers to (A-1, A-2, A-3) and assign the action.
4. Extract Personal Details.
5. Extract Role.

=====================================
RULES FOR LINKING ACTIONS
=====================================
1. Direct Mention: "A-1 purchased ganja" -> Role for A-1: "Purchased ganja".
2. Grouped List: "A-1 and A-2 purchased" -> Role for A-1: "Purchased". Role for A-2: "Purchased".
3. Range: "A-1 to A-3 sold drugs" -> Role for A-1: "Sold drugs". Role for A-2: "Sold drugs". Role for A-3: "Sold drugs".

=====================================
STRICT RULES
=====================================

=====================================
STRICT RULES
=====================================
- Extract strictly from the text. Do not guess.

=====================================
STRICT RULES
=====================================
- Extract strictly from the text. Do not guess.
- Address: Extract full available address (H.No, Village, Mandal, State).
- Role: Describe what they did AND their INTENT if stated (e.g. "Caught with 5kg ganja for selling", "Purchased for personal consumption", "Caught with phone").

Accused List:
{accused_names}

Input Text:
{text}

{format_instructions}
"""

# --- Rule Engine ---

def classify_accused_type(role_text: str) -> str:
    """
    Deterministically determines accused type based on role text keywords.
    """
    if not role_text:
        return "unknown"

    t = role_text.lower()

    # ------------------
    # ACTIVE PEDDLER (Explicit Selling) - Highest Priority
    # ------------------
    if any(k in t for k in [
        "selling",
        "sold",
        "retailer",
        "street dealer",
        "delivering",
        "transporting", 
        "waiting for customers",
        "commission for selling",
        "sale of",
        "business",
        "intending to sell",
        "to sell",
        "distributing"
    ]):
        return "peddler"

    # ------------------
    # CONSUMER (Priority over Possession)
    # ------------------
    if any(k in t for k in [
        "habitually consuming",
        "consuming",
        "smoking",
        "urine test",
        "tested positive",
        "for personal use",
        "for self use",
        "addicted",
        "purchased for consumption",
        "bought for consumption",
        "buy for consumption"
    ]):
        return "consumer"

    # ------------------
    # ORGANIZER / KINGPIN
    # ------------------
    if any(k in t for k in [
        "mastermind",
        "kingpin",
        "organizer",
        "planned the operation",
        "network leader",
        "head of",
        "directed",
        "controlled"
    ]):
        return "organizer_kingpin"

    # ------------------
    # SUPPLIER (Distributor/Wholesaler)
    # ------------------
    if any(k in t for k in [
        "supplied",
        "supplier",
        "distributor",
        "wholesaler",
        "bulk",
        "large quantity",
        "provided drugs to",
        "source of supply",
        "procured from",
        "procured" 
    ]):
        return "supplier"

    # ------------------
    # MANUFACTURER (Producer/Cultivator)
    # ------------------
    if any(k in t for k in [
        "cultivated",
        "grown",
        "cultivator",
        "grower",
        "producer",
        "manufactured",
        "production of"
    ]):
        return "manufacturer"

    # ------------------
    # HARBOURER
    # ------------------
    if any(k in t for k in [
        "shelter",
        "safe house",
        "lodge owner",
        "rented room",
        "harboured",
        "concealed",
        "premises used"
    ]):
        return "harbourer"

    # ------------------
    # FINANCIER (Funder/Investor)
    # ------------------
    if any(k in t for k in [
        "financed",
        "finance",
        "funded",
        "funding",
        "investor",
        "invested",
        "money launder",
        "provided capital"
    ]):
        return "financier"
        
    # ------------------
    # PROCESSOR
    # ------------------
    if any(k in t for k in [
        "processed ganja",
        "converted",
        "refined",
        "chemical processing",
        "lab"
    ]):
        return "processor"

    # ------------------
    # PASSIVE PEDDLER (Possession/Catch-all) - Lowest Priority
    # ------------------
    if any(k in t for k in [
        "caught with", 
        "possession",
        "possession of",
        "found in possession",
        "carrying",
        "purchasing", 
        "purchased",
        "bought",
        "buy",
        "small scale"
    ]):
        return "peddler"

def detect_gender(text_snippet: str, full_name: str) -> Optional[str]:
    """
    Detects gender based on honorifics and relational terms near the name.
    """
    s = full_name.lower()
    if "s/o" in s or "son of" in s or "h/o" in s or "husband of" in s or "father of" in s or "b/o" in s:
        return "Male"
    if "d/o" in s or "daughter of" in s or "w/o" in s or "wife of" in s:
        return "Female"
    return None

def detect_status(text: str, full_name: str) -> str:
    """
    Simple keyword check for status in context.
    """
    # Placeholder for more complex status logic if needed.
    return "unknown" 

def detect_ccl(full_name: str, role: str) -> bool:
    s = (full_name + " " + role).lower()
    if "ccl" in s or "child in conflict" in s or "juvenile" in s or "minor" in s:
        return True
    return False

# --- Main Extraction Logic ---

def get_llm_instance():
    return ChatOllama(
        base_url=config.LLM_ENDPOINT.replace("/api", ""),
        model=config.LLM_MODEL,
        temperature=0,
        num_ctx=config.LLM_CONTEXT_WINDOW
    )

def extract_accused_names_pass1(text: str) -> List[str]:
    llm = get_llm_instance()
    parser = JsonOutputParser(pydantic_object=AccusedNamesResponse)
    
    # Inject format instructions manually if needed, but langchain does it
    prompt = ChatPromptTemplate.from_template(PASS1_PROMPT)
    chain = prompt | llm | parser
    
    try:
        import time
        start_time = time.time()
        logger.info(f"Pass 1: Invoking LLM with model {config.LLM_MODEL}...")
        logger.info(f"Pass 1 Prompt Length: {len(text)} chars")
        
        # Debug: Invoke and print raw first if possible, but chain is convenient.
        response = chain.invoke({
            "text": text,
            "format_instructions": parser.get_format_instructions()
        })
        
        duration = time.time() - start_time
        logger.info(f"Pass 1: LLM responded in {duration:.2f} seconds.")
        logger.info(f"Pass 1 Raw LLM Parsed Response: {response}")
        
        # Handle variations
        if isinstance(response, dict) and "accused_names" in response:
            return response["accused_names"]
        if isinstance(response, list):
            return response
        return []
    except Exception as e:
        logger.error(f"Pass 1 Verification Error: {e}", exc_info=True)
        return []

def extract_details_pass2(text: str, accused_names: List[str]) -> List[AccusedDetails]:
    if not accused_names:
        return []
        
    llm = get_llm_instance()
    parser = JsonOutputParser(pydantic_object=AccusedDetailsResponse)
    prompt = ChatPromptTemplate.from_template(PASS2_PROMPT)
    chain = prompt | llm | parser
    
    try:
        # Pass 2 is often faster as the list is short, but text is same long text.
        import time
        start_time = time.time()
        logger.info("Pass 2: Invoking LLM for details...")
        
        response = chain.invoke({
            "text": text,
            "accused_names": str(accused_names),
            "format_instructions": parser.get_format_instructions()
        })
        
        duration = time.time() - start_time
        logger.info(f"Pass 2: LLM responded in {duration:.2f} seconds.")
        
        details_list = []
        raw_list = []
        
        if isinstance(response, dict):
            raw_list = response.get("accused_details", [])
        elif isinstance(response, list):
            raw_list = response
            
        for r in raw_list:
            # Flexible dict parsing
            if isinstance(r, dict):
                # Ensure full_name is present
                if not r.get('full_name'):
                    continue
                
                # Normalize phone_numbers: convert list to comma-separated string
                phone_nums = r.get('phone_numbers')
                if isinstance(phone_nums, list):
                    phone_nums = ', '.join(str(p) for p in phone_nums if p)
                elif phone_nums is None:
                    phone_nums = None
                else:
                    phone_nums = str(phone_nums)
                
                # Update the dict with normalized phone_numbers
                r_normalized = r.copy()
                r_normalized['phone_numbers'] = phone_nums
                
                try:
                    details_list.append(AccusedDetails(**r_normalized))
                except Exception as val_err:
                    logger.warning(f"Validation error for {r}: {val_err}")
                    # Try best effort
                    details_list.append(AccusedDetails(
                        full_name=r_normalized.get('full_name'),
                        role_in_crime=r_normalized.get('role_in_crime', 'Role not clearly stated'),
                        alias_name=r_normalized.get('alias_name'),
                        age=r_normalized.get('age'),
                        gender=r_normalized.get('gender'),
                        occupation=r_normalized.get('occupation'),
                        address=r_normalized.get('address'),
                        phone_numbers=phone_nums
                    ))
                
        return details_list
    except Exception as e:
        logger.error(f"Pass 2 Error: {e}")
        return []

def extract_accused_info(text: str) -> List[AccusedExtraction]:
    """
    Orchestrates the 2-pass extraction process.
    """
    if not text:
        return []

    logger.info("Starting Pass 1: Name Identification")
    names = extract_accused_names_pass1(text)
    logger.info(f"Pass 1 found: {names}")
    
    if not names:
        return []

    logger.info("Starting Pass 2: Details Extraction")
    details = extract_details_pass2(text, names)
    logger.info(f"Pass 2 found details entries: {len(details)}")
    
    final_accused = []
    
    # Create map for finding details by name
    detail_map = {d.full_name.lower().strip(): d for d in details}
    
    for name in names:
        raw_name = name.strip()
        clean_name = clean_accused_name(raw_name)
        
        # Use simple lower key for map lookup, but try both raw and clean
        # Sometimes Pass 2 returns raw name, sometimes clean.
        name_key_clean = clean_name.lower()
        name_key_raw = raw_name.lower()
        
        # Look for details
        d_obj = detail_map.get(name_key_raw) or detail_map.get(name_key_clean)
        
        # Fuzzy / Partial fallback
        if not d_obj:
            for k, v in detail_map.items():
                if k in name_key_raw or name_key_raw in k or k in name_key_clean or name_key_clean in k:
                    d_obj = v
                    break
        
        # Default empty details if missing
        role_desc = "Role not clearly stated"
        alias = None
        age = None
        gender = None
        occupation = None
        address = None
        phone = None
        
        if d_obj:
            role_desc = d_obj.role_in_crime or role_desc
            alias = d_obj.alias_name
            age = d_obj.age
            gender = d_obj.gender
            occupation = d_obj.occupation
            address = d_obj.address
            phone = d_obj.phone_numbers
        
        # Apply Logic Rules
        accused_type = classify_accused_type(role_desc)
        is_ccl = detect_ccl(clean_name, role_desc)
        
        # Helper for Gender (Logic > Extraction > None)
        logic_gender = detect_gender(text, clean_name)
        final_gender = logic_gender if logic_gender else gender
        
        # Status detection
        status = "unknown"
        if "absconding" in role_desc.lower() or "absconding" in clean_name.lower():
            status = "absconding"
        elif "arrested" in role_desc.lower() or "caught" in role_desc.lower() or "apprehended" in role_desc.lower():
            status = "arrested"
            
        obj = AccusedExtraction(
            full_name=clean_name,
            alias_name=alias,
            age=age,
            gender=final_gender,
            occupation=occupation,
            address=address,
            phone_numbers=phone,
            role_in_crime=role_desc,
            accused_type=accused_type,
            status=status,
            is_ccl=is_ccl
        )
        final_accused.append(obj)
        
    return final_accused

if __name__ == "__main__":
    # Test with a complex scenario
    test_text = "A1 Rahul @ Rocky (25) was caught selling Ganja. A2 Suresh (Supplier) is absconding. A3 Ravi bought for self consumption. Inspector Reddy arrested them."
    print("Testing extraction...")
    results = extract_accused_info(test_text)
    for r in results:
        print(r.model_dump_json(indent=2))

