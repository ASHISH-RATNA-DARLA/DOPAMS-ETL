from typing import List, Optional, Dict, Any, Tuple
from pydantic import BaseModel, Field
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser  # noqa: F401
import sys
import os

# Ensure core is accessible
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from core.llm_service import get_llm, invoke_extraction_with_retry, RobustJsonOutputParser

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

# ---------------------------------------------------------------------------
# Police / official title guard — second-layer filter after LLM output
# ---------------------------------------------------------------------------
# Matches police ranks, railway police, revenue officials, and FIR support
# roles that appear near a name in the text (within 80 chars either side).
# Used to reject names the LLM extracted despite prompt exclusions.
_POLICE_TITLE_RE = re.compile(
    r'\b(?:'
    r'SI|SIP|SHO|ASI|CI|DSP|DCP|SP|ACP|Inspector|Sub[\s\-]?Inspector|'
    r'PC|HC|HG|WPC|HHC|Constable|Head[\s]?Constable|'
    r'RPC|WRPC|ARPC|RHC|SIRP|IRP|IPF|RPF|RPS|W/?Con|'
    r'FRO|Tahsildar|MRO|GPO|'
    r'Mediator|Panch(?:a|as|ayathdars?)?|'
    r'Investigating[\s]?Officer|Beat[\s]?Officer|'
    r'Complainant|Clues[\s]?Team|'
    r'Prohibition[\s]?Officer|Excise[\s]?Inspector|'
    r'Gazetted[\s]?Officer|GO\b'
    r')\b',
    re.IGNORECASE,
)


def _is_police_name(name: str, text: str) -> bool:
    """
    Returns True when `name` appears in `text` immediately adjacent to a police
    or official title — within 20 chars before (title precedes name) or 30 chars
    after (name then title, dominant Telangana FIR pattern e.g.
    "Sri P.B.Ingle, IPF/RPF/NLG" or "B Ramesh, RPC 252 of RPS Kazipet").

    Window is intentionally narrow so accused names that happen to share a
    sentence with investigating officers are NOT wrongly blocked.
    """
    if not name or not text:
        return False
    text_lower = text.lower()
    name_lower = name.lower().strip()
    idx = text_lower.find(name_lower)
    while idx >= 0:
        prefix = text[max(0, idx - 20): idx]
        suffix = text[idx + len(name_lower): min(len(text), idx + len(name_lower) + 30)]
        if _POLICE_TITLE_RE.search(prefix) or _POLICE_TITLE_RE.search(suffix):
            return True
        idx = text_lower.find(name_lower, idx + 1)
    return False


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

1. Extract ONLY persons who COMMITTED the crime — buying, selling, transporting,
   harbouring, cultivating, manufacturing, or financing drugs/contraband.
2. Include:
   - Persons apprehended, arrested, confessed, or absconding
   - Persons referred as A1, A2, A-1, A-2, accused, suspect, JCL/CCL
   - Suppliers / Transporters / Producers named in confessions, even if not arrested

=====================================
STRICT EXCLUSIONS — DO NOT EXTRACT ANY OF THESE
=====================================

POLICE / INVESTIGATING OFFICIALS (all ranks and forces):
  State Police  : SI, SIP, ASI, CI, Inspector, Sub-Inspector, SHO, DSP, DCP, SP, ACP
                  PC (Police Constable), HC (Head Constable), HG (Home Guard), WPC
  Railway Police: RPF, RPC, WRPC, ARPC, RHC, SIRP, IRP, IPF, RPS, W/Con, SIRP
  Any person described as "complainant", "investigating officer", "IO", "beat officer",
  "patrol staff", "on duty officer", or "along with his staff / team"

GOVERNMENT / REVENUE OFFICIALS:
  Tahsildar, MRO, GPO, FRO (Forest Range Officer), Revenue Officer, Excise Inspector,
  Prohibition & Excise Officer, GHMC / Municipal staff, Gazetted Officer (GO)

PANCHAS / MEDIATORS / WITNESSES:
  Panchas, Panchayathdars, Mediators, independent witnesses, mahazar witnesses,
  any person described as "1) Sri..." / "2) Sri..." in a numbered witness list

COMPLAINANT’S SUPPORT STAFF:
  Clues team, photographer, videographer, dog squad, translator / interpreter,
  weighing shop owner (called only to weigh seized material)

PERSONS NOT PRESENT OR UNNAMED:
  "Unknown person", "unidentified person", "some persons", "one person" with NO name
  Persons whose name appears ONLY on a found ID card / document but are not present
  Persons who "ran away / fled / escaped" with NO name given anywhere in the text

=====================================
CRITICAL RULE
=====================================
A name appearing in the FIR does NOT make that person an accused.
Police officers are routinely named in every FIR — IGNORE ALL OF THEM.
Extract ONLY persons who committed the drug / crime offence itself.

=====================================
OUTPUT RULES
=====================================
- Output ONLY accused persons.
- Extract ONLY the full name string as it appears in text.
- DO NOT infer roles, types, gender, age, or status.
- If no accused exist, return an empty list [].

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
        "consumption",
        "consumption of",
        "smoking",
        "urine test",
        "tested positive",
        "for personal use",
        "for self use",
        "addict",
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
    # SUPPLIER (Distributor/Wholesaler/Transporter/Receiver - all supply-chain roles)
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
        "transport",
        "transporter",
        "carrying",
        "delivering",
        "courier",
        "driver",
        "dispatch",
        "shipment",
        "transit",
        "source of",
        "receiver",
        "recipient",
        "received from",
        "owner of crime vehicle",
        "owner of the vehicle",
        "crime vehicle",
        "brought from",
        "bought from",
        "intended to hand over",
        "hand over",
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

    return "unknown"


_MEANINGFUL_ROLE_KEYWORDS = (
    "sold", "selling", "sell", "resell",
    "consumed", "consuming", "consumption", "consumer",
    "possession", "possessing", "caught with",
    "purchased", "purchase", "bought", "buy",
    "supplied", "supplying", "supplier", "supply",
    "transported", "transporting", "transporter",
    "carrying", "delivering", "courier", "driver",
    "manufactured", "manufacturing", "cultivated", "cultivation", "producer",
    "processed", "processing", "packed", "packing",
    "harbour", "harboured", "concealed", "stash", "stored",
    "financed", "financier", "funded",
    "kingpin", "mastermind", "organizer", "ringleader",
    "received", "receiver", "recipient",
    "distributor", "distributing", "peddler", "peddling", "trafficking",
    "owner of", "crime vehicle",
    "ganja", "cocaine", "heroin", "opium", "opiate", "charas", "hashish",
    "alprazolam", "benzodiazepine", "meth", "mdma", "lsd",
    "drug", "drugs", "narcotic", "contraband", "tablets",
)

_PROCEDURAL_ROLE_MARKERS = (
    "41a cr", "41 a cr", "41-a cr",
    "cr.p.c issued", "crpc issued",
    "notice issued", "notice served",
    "remanded", "arrested", "absconding", "absconded",
    "surrendered", "apprehended",
    "judicial custody", "police custody",
    "bailed", "granted bail",
)


def _is_procedural_role(role_text: Optional[str]) -> bool:
    """True when role_text is empty, 'Role not clearly stated', or purely a
    procedural/status note (e.g. '41A Cr.P.C issued', 'remanded', 'arrested')
    with no actual crime role content.

    Why: such placeholders cannot drive classification and should defer to the
    crime-wide shared role so downstream classify_accused_type has a chance.
    How to apply: called by compute_shared_role (to skip procedural rows from
    the vote) and by Branch A/Pass 2 to decide whether to inherit.
    """
    if not role_text:
        return True
    t = role_text.lower().strip()
    if not t:
        return True
    if t == "role not clearly stated" or t in {"n/a", "none", "null", "-"}:
        return True
    # Real crime-role content — not procedural.
    if any(k in t for k in _MEANINGFUL_ROLE_KEYWORDS):
        return False
    return any(m in t for m in _PROCEDURAL_ROLE_MARKERS)


def compute_shared_role(
    roles_by_code: Dict[str, Dict[str, Any]]
) -> Tuple[Optional[str], Optional[str]]:
    """Pick the dominant role_in_crime + key_details across accused in a crime.

    Edge case — some FIRs describe a collective action once ("A1 and A2
    purchased...") but the LLM assigns the role text only to one code. This
    leaves the rest with NULL role. We inherit the majority role so every
    accused in the crime gets classified consistently.

    Returns (role_in_crime, key_details) or (None, None) when no reliable
    majority exists.
    """
    if not roles_by_code:
        return None, None

    role_counts: Dict[str, int] = {}
    key_details_by_role: Dict[str, str] = {}
    for v in roles_by_code.values():
        if not isinstance(v, dict):
            continue
        rc = (v.get('role_in_crime') or '').strip()
        if not rc or _is_procedural_role(rc):
            continue
        role_counts[rc] = role_counts.get(rc, 0) + 1
        kd = v.get('key_details')
        if kd and rc not in key_details_by_role:
            key_details_by_role[rc] = kd

    if not role_counts:
        return None, None

    # Tie-break: prefer role with the highest count; on tie, pick the longest
    # (more descriptive) text.
    best_role = max(
        role_counts.items(),
        key=lambda kv: (kv[1], len(kv[0]))
    )[0]
    return best_role, key_details_by_role.get(best_role)


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
    parser = RobustJsonOutputParser(pydantic_object=AccusedNamesResponse)
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

    parser = RobustJsonOutputParser(pydantic_object=AccusedDetailsResponse)
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

    # Python-side police guard: drop any name the LLM extracted despite prompt
    # exclusions, detected by proximity to a police/official title in the text.
    filtered_names = [n for n in names if not _is_police_name(n, text)]
    dropped = set(names) - set(filtered_names)
    if dropped:
        logger.info(f"Police guard filtered out: {dropped}")
    names = filtered_names
    if not names:
        logger.info("All Pass 1 names removed by police guard — no accused.")
        return []

    logger.info("Starting Pass 2: Details Extraction")
    details = extract_details_pass2(text, names)
    if details is None:
        return None
    logger.info(f"Pass 2 found details entries: {len(details)}")

    final_accused = []

    detail_map = {d.full_name.lower().strip(): d for d in details}

    # Shared-role fallback across Pass 2 outputs: when the FIR describes a
    # collective action but the LLM only tagged one accused, inherit the
    # dominant role so every accused gets a non-null role/classification.
    synthetic_roles = {
        name: {
            'role_in_crime': d.role_in_crime,
            'key_details': d.key_details,
        }
        for name, d in detail_map.items()
        if d.role_in_crime
    }
    shared_role_text, shared_role_key_details = compute_shared_role(synthetic_roles)

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

        # Inherit dominant crime role when this accused has none or only a
        # procedural note (e.g., "41A Cr.P.C issued", "remanded").
        if shared_role_text and (
            role_desc == "Role not clearly stated"
            or _is_procedural_role(role_desc)
        ):
            role_desc = shared_role_text
            if not key_details and shared_role_key_details:
                key_details = shared_role_key_details

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
TASK: ACCUSED ANALYSIS FROM BRIEF FACTS
=====================================

You are given:
1. FIR / Brief Facts text
2. A confirmed list of accused persons linked to this crime.

For EACH person in the accused list, you MUST:
1. Search the ENTIRE text for ALL mentions of their accused_code (A-1, A-2, A1, A2, etc.)
   AND/OR their name.
2. COPY SHARED ACTIONS to EACH individual's role:
   - "A-1 and A-2 purchased..." → assign "Purchased..." to BOTH A-1 AND A-2.
   - "A-1 to A-3 were apprehended..." → assign to A-1, A-2, AND A-3.
   - "Both accused..." / "They..." / "All accused..." → assign to ALL relevant persons.
3. Extract their specific role_in_crime and key_details.
4. For fields annotated [MISSING], extract that field from the text ONLY if present.
5. For accused annotated [ASSIGN_CODE], assign a person code (A1, A2, A3...) based on
   order of first mention in the narrative text.

=====================================
CRITICAL: ROLE EXTRACTION RULES
=====================================
- EVERY accused MUST have role_in_crime describing WHAT THEY DID.
  Examples: "Selling hash oil to customers", "Transporting 120 kg ganja in vehicle",
  "Supplied ganja from Visakhapatnam", "Caught with 378g ganja for personal consumption".
- Key Details: Quantities, drug type, vehicle used, items seized, phone seized.
- Extract strictly from text. Do not guess or fabricate.
- If a person is genuinely not mentioned in text, return role_in_crime: null.

=====================================
STRICT: DO NOT CONFUSE THESE WITH ACCUSED
=====================================
The following persons appear in every FIR but are NOT accused — ignore them completely:
- Police officers of any rank: SI, ASI, SHO, Inspector, PC, HC, HG, WPC, RPC, SIRP,
  IRP, RPF, RHC, IPF, RPS, WRPC, ARPC — these are investigating / arresting officers
- Gazetted Officers (GO) present for search and seizure
- Panchas, mediators, witnesses (often listed as "1) Sri..., 2) Sri..." in text)
- Tahsildar, MRO, Excise Inspector, Prohibition Officer attending scene
- Complainant and their staff / patrol team

=====================================
Known Accused List:
{accused_list}

Input Text:
{text}

{format_instructions}
"""

# Enhanced response model with fallback fields
class RoleExtractionItem(BaseModel):
    accused_code: str = Field(description="The accused code from the input list, e.g. A-1, A-2")
    role_in_crime: Optional[str] = Field(default=None, description="Factual action or role description")
    key_details: Optional[str] = Field(default=None, description="Quantities seized, substance type, vehicle, or other specific investigative facts")
    address: Optional[str] = Field(default=None, description="Full address extracted from text, only when flagged as MISSING")
    age: Optional[int] = Field(default=None, description="Age extracted from text, only when flagged as MISSING")
    alias_name: Optional[str] = Field(default=None, description="Alias/nickname from text, only when flagged as MISSING")
    occupation: Optional[str] = Field(default=None, description="Occupation from text, only when flagged as MISSING")
    person_code_assigned: Optional[str] = Field(default=None, description="Person code (A1, A2, etc.) assigned by order of first mention, only for ASSIGN_CODE accused")

class RoleExtractionResponse(BaseModel):
    accused_roles: List[RoleExtractionItem]


def extract_roles_for_known_accused(
    text: str,
    accused_list: List[Dict[str, Any]],
    missing_fields_map: Optional[Dict[str, List[str]]] = None,
    needs_person_code: Optional[List[str]] = None,
) -> Dict[str, Dict[str, Any]]:
    """
    Targeted LLM extraction for Branch A / Branch B.

    Accepts:
      - accused_list: DB-sourced accused dicts with 'accused_code', 'full_name', etc.
      - missing_fields_map: {accused_code: [list of field names missing from DB]}
      - needs_person_code: list of accused_codes that need LLM-assigned person code

    Returns a dict keyed by accused_code -> {
        'role_in_crime': ..., 'key_details': ...,
        'address': ..., 'age': ..., 'alias_name': ..., 'occupation': ...,
        'person_code_assigned': ...
    }.

    Returns {} on failure so callers can fall back gracefully.
    """
    if not text or not accused_list:
        return {}

    missing_fields_map = missing_fields_map or {}
    needs_person_code = needs_person_code or []

    # Build a human-readable list with annotations for the prompt
    accused_entries = []
    for i, acc in enumerate(accused_list, start=1):
        code = acc.get('accused_code')
        name = acc.get('full_name') or 'Unknown'
        if not code:
            code = f'A-{i}'

        entry = f"{code}: {name}"

        # Add annotations for missing fields
        annotations = []
        missing = missing_fields_map.get(code, [])
        if missing:
            annotations.append(f"[MISSING: {', '.join(missing)}]")
        if code in needs_person_code:
            annotations.append("[ASSIGN_CODE]")

        if annotations:
            entry += " " + " ".join(annotations)

        accused_entries.append(entry)
    accused_list_str = "\n".join(accused_entries)

    parser = RobustJsonOutputParser(pydantic_object=RoleExtractionResponse)
    chain = get_llm_chain(PASS2_KNOWN_ACCUSED_PROMPT, parser)

    try:
        import time
        start_time = time.time()
        logger.info(
            f"extract_roles_for_known_accused: invoking LLM for {len(accused_list)} accused "
            f"(missing_fields={len(missing_fields_map)}, needs_code={len(needs_person_code)})"
        )

        response = invoke_extraction_with_retry(
            chain,
            {
                "text": text,
                "accused_list": accused_list_str,
                "format_instructions": parser.get_format_instructions()
            },
            max_retries=2
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

        if not raw_list:
            logger.warning("extract_roles_for_known_accused: got response but accused_roles list is empty")
            return {}

        result: Dict[str, Dict[str, Any]] = {}
        for item in raw_list:
            if not isinstance(item, dict):
                continue
            code = (item.get("accused_code") or "").strip()
            if not code:
                continue

            entry: Dict[str, Any] = {
                "role_in_crime": item.get("role_in_crime"),
                "key_details": item.get("key_details"),
            }
            # Include fallback fields if present
            if item.get("address"):
                entry["address"] = item["address"]
            if item.get("age") is not None:
                entry["age"] = item["age"]
            if item.get("alias_name"):
                entry["alias_name"] = item["alias_name"]
            if item.get("occupation"):
                entry["occupation"] = item["occupation"]
            if item.get("person_code_assigned"):
                entry["person_code_assigned"] = item["person_code_assigned"]

            result[code] = entry

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






