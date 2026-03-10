from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
import sys
import os

# Ensure core is accessible
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from core.llm_service import get_llm, invoke_extraction_with_retry

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

EXPLICIT_GENDER_MAP = {
    "male": "Male",
    "man": "Male",
    "boy": "Male",
    "female": "Female",
    "woman": "Female",
    "girl": "Female",
    "transgender": "Transgender",
    "trans gender": "Transgender",
    "trans": "Transgender",
    "third gender": "Transgender",
}

COMMON_INDIAN_MALE_NAMES = {
    "abhishek", "akhil", "ali", "amit", "anil", "aravind", "arjun", "ashok",
    "bhanu", "charan", "dileep", "dinesh", "ganesh", "gopal", "harish",
    "imran", "jeeban", "jadhav", "karthik", "khasim", "kiran", "kishore",
    "kota", "kumar", "laxman", "mahesh", "mallappa", "manoj", "mohd",
    "mohammed", "mukarram", "muneeruddin", "naresh", "naveen", "nikhil",
    "om", "poshetty", "pradeep", "pramod", "rahul", "rajesh", "ramu",
    "ravinder", "rehan", "sairam", "santosh", "satkruth", "shahid", "shankar",
    "shiva", "srikant", "srinivas", "suresh", "teja", "uday", "vamshi",
    "venkatesh", "vijay", "vinod", "vishal",
}

COMMON_INDIAN_FEMALE_NAMES = {
    "anitha", "anjali", "banitha", "bhavani", "deepa", "divya", "gita",
    "hema", "kavitha", "lakshmi", "laxmi", "madhavi", "padma", "pooja",
    "radha", "rani", "sandhya", "savitri", "shanthi", "sita", "sunitha",
    "swathi", "uma", "vani",
}

FEMALE_NAME_SUFFIXES = ("amma", "bai", "begum", "devi", "kumari", "laxmi", "lakshmi")
MALE_NAME_SUFFIXES = ("anna", "appa", "kumar", "rao", "reddy", "singh")

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
    key_details: Optional[str] = Field(default=None, description="Quantities seized, substance type, vehicle used, or other specific investigative facts")

def clean_accused_name(name: str) -> str:
    """
    Normalizes accused name by removing metadata, aliases, and prefixes.
    Ex: "A-1) John Doe@Rocky s/o Smith" -> "John Doe"
    """
    if not name:
        return ""
    
    # 1. Remove Prefix like "A-1", "1.", "A1)"
    name = re.sub(r'^(?:A[\.\-]?\d+|[0-9]+)[\)/\.\:\s-]*', '', name, flags=re.IGNORECASE)
    
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
   - **Crucial**: Include Suppliers / Transporters / Producers / Sources mentioned in confessions, even if not arrested or "absconding".

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
- Role: Describe what they did AND their INTENT if stated (e.g. "Caught with 5kg ganja for selling", "Purchased for personal consumption", "Caught with phone", "Transporting drugs in car", "Cultivating ganja plants").
- Key Details: Note any quantities of substances, type of drugs, vehicle used, items seized, or other specific investigative facts unique to this accused. Be concise (e.g. "5kg ganja, seized in a red bag", "two mobile phones seized", "drove a blue Activa").

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
    Returns one of the 8 schema-valid categories or "unknown".
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
        "waiting for customers",
        "commission for selling",
        "sale of",
        "business",
        "intending to sell",
        "to sell",
        "distributing",
        "pushing",
        "hawking",
        "street sale",
        "spot sale",
        "trafficking",
        "peddling",
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
        "buy for consumption",
        "personal consumption",
        "consumed",
        "using drugs",
        "drug user",
        "under influence",
        "for own use",
        "for consumption",
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
        "controlled",
        "ringleader",
        "boss",
        "gang leader",
        "in-charge",
        "overseeing",
        "coordinating",
        "managing the operation",
    ]):
        return "organizer_kingpin"

    # ------------------
    # SUPPLIER (Distributor/Wholesaler/Transporter - all supply-chain roles)
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
        "procured",
        "transporting",
        "carrying",
        "delivering",
        "courier",
        "driver",
        "dispatch",
        "shipment",
        "transit",
        "source of",
    ]):
        return "supplier"

    # ------------------
    # MANUFACTURER (Producer/Cultivator - all production roles)
    # ------------------
    if any(k in t for k in [
        "manufactured",
        "production of",
        "producing",
        "growing",
        "cultivator",
        "farming",
        "cultivated",
        "grown",
        "grower",
        "farm",
        "cultivation",
        "producer",
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
        "premises used",
        "hiding",
        "hiding place",
        "stash house",
        "storing at",
        "stored at",
        "kept at",
        "concealing",
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
        "provided capital",
        "backer",
        "sponsored",
        "money for purchase",
        "provided money",
        "lender",
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
        "lab",
        "processing",
        "packaging",
        "packed",
        "repacked",
        "mixing",
        "adulteration",
        "weighing and packing",
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
        "purchasing",
        "purchased",
        "bought",
        "buy",
        "small scale",
    ]):
        return "peddler"

def normalize_gender_value(raw_gender: Optional[str]) -> Optional[str]:
    if raw_gender is None:
        return None

    value = str(raw_gender).strip().lower()
    if not value:
        return None

    value = re.sub(r'[^a-z\s]', ' ', value)
    value = re.sub(r'\s+', ' ', value).strip()

    if value in EXPLICIT_GENDER_MAP:
        return EXPLICIT_GENDER_MAP[value]

    for key, normalized in EXPLICIT_GENDER_MAP.items():
        if re.search(rf'\b{re.escape(key)}\b', value):
            return normalized

    return None


def infer_gender_from_indian_name(full_name: str) -> Optional[str]:
    tokens = [token for token in re.findall(r"[A-Za-z]+", full_name.lower()) if len(token) > 1]
    for token in tokens:
        if token in COMMON_INDIAN_FEMALE_NAMES:
            return "Female"
        if token in COMMON_INDIAN_MALE_NAMES:
            return "Male"

    for token in tokens:
        if any(token.endswith(suffix) for suffix in FEMALE_NAME_SUFFIXES):
            return "Female"
        if any(token.endswith(suffix) for suffix in MALE_NAME_SUFFIXES):
            return "Male"

    return None


def detect_gender(text_snippet: str, full_name: str, raw_gender: Optional[str] = None) -> Optional[str]:
    """
    Detect gender with a strict priority order:
    1. Explicit male/female/transgender values
    2. Relational markers near this specific name
    3. Strong Indian-name heuristics from the accused name itself
    """
    explicit_gender = normalize_gender_value(raw_gender)
    if explicit_gender:
        return explicit_gender

    name_text = full_name.lower()
    if "s/o" in name_text or "son of" in name_text or "h/o" in name_text or "husband of" in name_text or "father of" in name_text or "b/o" in name_text:
        return "Male"
    if "d/o" in name_text or "daughter of" in name_text or "w/o" in name_text or "wife of" in name_text:
        return "Female"

    lowered_text = (text_snippet or "").lower()
    candidates = [full_name.lower(), clean_accused_name(full_name).lower()]
    context_windows = []
    for candidate in candidates:
        if not candidate:
            continue
        idx = lowered_text.find(candidate)
        if idx >= 0:
            start = max(0, idx - 80)
            end = min(len(lowered_text), idx + len(candidate) + 80)
            context_windows.append(lowered_text[start:end])

    for window in context_windows:
        if any(marker in window for marker in ["s/o", "son of", "h/o", "husband of", "father of", "b/o", " mr ", " sri ", " shri "]):
            return "Male"
        if any(marker in window for marker in ["d/o", "daughter of", "w/o", "wife of", " mrs ", " ms ", " smt ", " kumari ", " begum "]):
            return "Female"
        normalized_window_gender = normalize_gender_value(window)
        if normalized_window_gender:
            return normalized_window_gender

    return infer_gender_from_indian_name(full_name)

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

def get_llm_chain(prompt_template, parser):
    llm_service = get_llm('extraction')
    llm = llm_service.get_langchain_model()
    prompt = ChatPromptTemplate.from_template(prompt_template)
    return prompt | llm | parser

def extract_accused_names_pass1(text: str) -> Optional[List[str]]:
    parser = JsonOutputParser(pydantic_object=AccusedNamesResponse)
    chain = get_llm_chain(PASS1_PROMPT, parser)

    try:
        import time
        start_time = time.time()
        logger.info(f"Pass 1: Invoking LLM with model {config.LLM_MODEL}...")
        logger.info(f"Pass 1 Prompt Length: {len(text)} chars")

        response = invoke_extraction_with_retry(
            chain,
            {
                "text": text,
                "format_instructions": parser.get_format_instructions()
            },
            max_retries=1
        )

        duration = time.time() - start_time
        logger.info(f"Pass 1: LLM responded in {duration:.2f} seconds.")
        logger.info(f"Pass 1 Raw LLM Parsed Response: {response}")

        if response in ({}, None):
            logger.error("Pass 1 returned an empty response after retries.")
            return None
        if isinstance(response, dict) and "accused_names" in response:
            return response["accused_names"]
        if isinstance(response, list):
            return response

        logger.error(f"Pass 1 returned unusable response shape: {response}")
        return None
    except Exception as e:
        logger.error(f"Pass 1 Verification Error: {e}", exc_info=True)
        return None

def extract_details_pass2(text: str, accused_names: List[str]) -> Optional[List[AccusedDetails]]:
    if not accused_names:
        return []

    parser = JsonOutputParser(pydantic_object=AccusedDetailsResponse)
    chain = get_llm_chain(PASS2_PROMPT, parser)

    try:
        import time
        start_time = time.time()
        logger.info("Pass 2: Invoking LLM for details...")

        response = invoke_extraction_with_retry(
            chain,
            {
                "text": text,
                "accused_names": str(accused_names),
                "format_instructions": parser.get_format_instructions()
            },
            max_retries=1
        )

        duration = time.time() - start_time
        logger.info(f"Pass 2: LLM responded in {duration:.2f} seconds.")

        if response in ({}, None):
            logger.error("Pass 2 returned an empty response after retries.")
            return None

        details_list = []
        raw_list = []

        if isinstance(response, dict):
            raw_list = response.get("accused_details", [])
        elif isinstance(response, list):
            raw_list = response
        else:
            logger.error(f"Pass 2 returned unusable response shape: {response}")
            return None

        for r in raw_list:
            if not isinstance(r, dict):
                continue
            if not r.get('full_name'):
                continue

            phone_nums = r.get('phone_numbers')
            if isinstance(phone_nums, list):
                phone_nums = ', '.join(str(p) for p in phone_nums if p)
            elif phone_nums is None:
                phone_nums = None
            else:
                phone_nums = str(phone_nums)

            r_normalized = r.copy()
            r_normalized['phone_numbers'] = phone_nums

            try:
                details_list.append(AccusedDetails(**r_normalized))
            except Exception as val_err:
                logger.warning(f"Validation error for {r}: {val_err}")
                safe_age = r_normalized.get('age') if isinstance(r_normalized.get('age'), int) else None
                try:
                    details_list.append(AccusedDetails(
                        full_name=str(r_normalized.get('full_name')),
                        role_in_crime=r_normalized.get('role_in_crime') or 'Role not clearly stated',
                        alias_name=r_normalized.get('alias_name'),
                        age=safe_age,
                        gender=r_normalized.get('gender') if isinstance(r_normalized.get('gender'), str) else None,
                        occupation=r_normalized.get('occupation') if isinstance(r_normalized.get('occupation'), str) else None,
                        address=r_normalized.get('address') if isinstance(r_normalized.get('address'), str) else None,
                        phone_numbers=phone_nums
                    ))
                except Exception as fallback_err:
                    logger.warning(f"Skipping invalid Pass 2 record after fallback: {fallback_err}")

        return details_list
    except Exception as e:
        logger.error(f"Pass 2 Error: {e}", exc_info=True)
        return None

def extract_accused_info(text: str) -> Optional[List[AccusedExtraction]]:
    """
    Orchestrates the 2-pass extraction process.
    Returns None when the LLM/parsing flow failed, and [] only for a valid no-accused result.
    """
    if not text:
        return []

    logger.info("Starting Pass 1: Name Identification")
    names = extract_accused_names_pass1(text)
    logger.info(f"Pass 1 found: {names}")

    if names is None:
        return None
    if not names:
        return []

    logger.info("Starting Pass 2: Details Extraction")
    details = extract_details_pass2(text, names)
    if details is None:
        return None
    logger.info(f"Pass 2 found details entries: {len(details)}")

    final_accused = []

    detail_map = {d.full_name.lower().strip(): d for d in details}

    for name in names:
        raw_name = name.strip()
        clean_name = clean_accused_name(raw_name)

        name_key_clean = clean_name.lower()
        name_key_raw = raw_name.lower()

        d_obj = detail_map.get(name_key_raw) or detail_map.get(name_key_clean)

        if not d_obj:
            # Improved matching: require at least 2 token overlap to avoid
            # 'Rahul Singh' matching 'Rahul Kumar' (audit fix)
            name_tokens_raw   = set(name_key_raw.split())
            name_tokens_clean = set(name_key_clean.split())
            best_match = None
            best_overlap = 0
            for k, v in detail_map.items():
                k_tokens = set(k.split())
                overlap = max(len(k_tokens & name_tokens_raw), len(k_tokens & name_tokens_clean))
                if overlap >= 2 and overlap > best_overlap:
                    best_overlap = overlap
                    best_match = v
            d_obj = best_match

        role_desc = "Role not clearly stated"
        alias = None
        age = None
        gender = None
        occupation = None
        address = None
        phone = None
        key_details = None

        if d_obj:
            role_desc = d_obj.role_in_crime or role_desc
            alias = d_obj.alias_name
            age = d_obj.age
            gender = d_obj.gender
            occupation = d_obj.occupation
            address = d_obj.address
            phone = d_obj.phone_numbers
            key_details = d_obj.key_details

        classification_text = role_desc + (" " + key_details if key_details else "")
        accused_type = classify_accused_type(classification_text)
        is_ccl = detect_ccl(clean_name, role_desc)

        # Gender cues usually live in the raw extracted name before cleanup strips relations.
        logic_gender = detect_gender(text, raw_name, gender)
        final_gender = logic_gender if logic_gender else gender

        _role_lower = role_desc.lower()
        _name_lower = clean_name.lower()
        _combined = _role_lower + " " + _name_lower

        _text_lower = (text or "").lower()
        for _candidate in [clean_name.lower(), raw_name.lower()]:
            if not _candidate:
                continue
            _idx = _text_lower.find(_candidate)
            if _idx >= 0:
                _start = max(0, _idx - 120)
                _end = min(len(_text_lower), _idx + len(_candidate) + 120)
                _combined += " " + _text_lower[_start:_end]
                break

        _absconding_keywords = [
            "absconding", "evading", "fled", "on the run", "not traceable",
            "not found", "missing", "could not be traced", "yet to be arrested",
            "failed to appear", "escaped",
        ]
        _arrested_keywords = [
            "arrested", "caught", "apprehended", "detained", "nabbed", "held",
            "taken into custody", "remanded", "produced before court", "surrendered",
            "confessed", "confession",
        ]

        status = "unknown"
        if any(k in _combined for k in _absconding_keywords):
            status = "absconding"
        elif any(k in _combined for k in _arrested_keywords):
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
            key_details=key_details,
            accused_type=accused_type,
            status=status,
            is_ccl=is_ccl
        )
        final_accused.append(obj)

    return final_accused


# ---------------------------------------------------------------------------
# Targeted Role Extraction for Known-Accused (Branches A & B)
# ---------------------------------------------------------------------------

PASS2_KNOWN_ACCUSED_PROMPT = """You are a criminal investigation analyst.

=====================================
TASK: ROLE EXTRACTION FOR KNOWN ACCUSED
=====================================

You are given:
1. FIR / Brief Facts text
2. A list of known accused persons already linked to the crime.
   Each entry contains the accused_code (e.g. A-1, A-2) and their name.

For EACH person in the accused list:
1. Locate all mentions of their accused_code AND their name in the text.
2. COPY SHARED ACTIONS:
   - If "A-1 and A-2 purchased...", assign "Purchased..." to BOTH.
   - If "A-1 to A-3 were apprehended...", assign to A-1, A-2, AND A-3.
3. Extract Role and Key Details.

=====================================
STRICT RULES
=====================================
- Extract strictly from the text. Do not guess.
- Role: Describe what they did AND their intent (e.g. "Caught with 5kg ganja for selling").
- Key Details: Quantities, drug type, vehicle, items seized, or other specific facts.
- If a person has no mention in the text, return role_in_crime: null, key_details: null.

Known Accused List (accused_code → name):
{accused_list}

Input Text:
{text}

{format_instructions}
"""

# Reuse Pass2 response schema
class RoleExtractionItem(BaseModel):
    accused_code: str = Field(description="The accused code from the input list, e.g. A-1, A-2")
    role_in_crime: Optional[str] = Field(default=None, description="Factual action or role description")
    key_details: Optional[str] = Field(default=None, description="Quantities seized, substance type, vehicle, or other specific investigative facts")

class RoleExtractionResponse(BaseModel):
    accused_roles: List[RoleExtractionItem]


def extract_roles_for_known_accused(text: str, accused_list: List[Dict[str, Any]]) -> Dict[str, Dict[str, Optional[str]]]:
    """
    Targeted LLM Pass 2 for Branch A / Branch B.

    Accepts a list of DB-sourced accused dicts with 'accused_code' and optionally 'full_name'.
    Returns a dict keyed by accused_code -> {'role_in_crime': ..., 'key_details': ...}.

    Used when we already know WHO the accused are from the DB, so we skip Pass 1
    (name identification) and go straight to role extraction using the accused_code
    as the deterministic pairing anchor.

    Returns {} on failure so callers can fall back gracefully.
    """
    if not text or not accused_list:
        return {}

    # Build a human-readable list for the prompt
    accused_entries = []
    for acc in accused_list:
        code = acc.get('accused_code') or '?'
        name = acc.get('full_name') or 'Unknown'
        accused_entries.append(f"{code}: {name}")
    accused_list_str = "\n".join(accused_entries)

    parser = JsonOutputParser(pydantic_object=RoleExtractionResponse)
    chain = get_llm_chain(PASS2_KNOWN_ACCUSED_PROMPT, parser)

    try:
        import time
        start_time = time.time()
        logger.info(f"extract_roles_for_known_accused: invoking LLM for {len(accused_list)} accused")

        response = invoke_extraction_with_retry(
            chain,
            {
                "text": text,
                "accused_list": accused_list_str,
                "format_instructions": parser.get_format_instructions()
            },
            max_retries=1
        )

        duration = time.time() - start_time
        logger.info(f"extract_roles_for_known_accused: LLM responded in {duration:.2f}s")

        if not response:
            logger.error("extract_roles_for_known_accused: empty response after retries")
            return {}

        raw_list = []
        if isinstance(response, dict):
            raw_list = response.get("accused_roles", [])
        elif isinstance(response, list):
            raw_list = response

        result: Dict[str, Dict[str, Optional[str]]] = {}
        for item in raw_list:
            if not isinstance(item, dict):
                continue
            code = (item.get("accused_code") or "").strip()
            if not code:
                continue
            result[code] = {
                "role_in_crime": item.get("role_in_crime"),
                "key_details": item.get("key_details"),
            }

        logger.info(f"extract_roles_for_known_accused: extracted roles for codes={list(result.keys())}")
        return result

    except Exception as e:
        logger.error(f"extract_roles_for_known_accused error: {e}", exc_info=True)
        return {}


if __name__ == "__main__":
    # Test with a complex scenario
    test_text = "A1 Rahul @ Rocky (25) was caught selling Ganja. A2 Suresh (Supplier) is absconding. A3 Ravi bought for self consumption. Inspector Reddy arrested them."
    print("Testing extraction...")
    results = extract_accused_info(test_text)
    for r in results:
        print(r.model_dump_json(indent=2))






