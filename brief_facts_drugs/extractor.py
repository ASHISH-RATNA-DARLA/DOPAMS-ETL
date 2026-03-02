
import re
import logging
import threading
from typing import List, Optional, Tuple
from pydantic import BaseModel, Field
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
import sys
import os

logger = logging.getLogger(__name__)

# Ensure core is accessible
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from core.llm_service import get_llm, invoke_extraction_with_retry
import config

# =============================================================================
# Thread-safe LLM instances
# =============================================================================
# ChatOllama uses httpx.Client internally, which is NOT thread-safe.
# When using ThreadPoolExecutor for parallel extraction, each thread MUST
# have its own ChatOllama instance.  We use threading.local() so each
# thread creates its instance once and reuses it for the thread's lifetime.
# =============================================================================
_thread_local = threading.local()

def _get_thread_safe_llm():
    """Return a per-thread ChatOllama instance (created lazily, cached per thread)."""
    if not hasattr(_thread_local, 'llm'):
        from langchain_ollama import ChatOllama
        llm_service = get_llm('extraction')
        base_url = os.getenv("OLLAMA_HOST", "http://localhost:11434")
        if base_url.endswith("/api"):
            base_url = base_url.replace("/api", "")
        _thread_local.llm = ChatOllama(
            base_url=base_url,
            model=llm_service.model,
            temperature=llm_service.temperature,
            num_ctx=llm_service.context_window,
        )
        logger.info(f"Created thread-local ChatOllama for thread {threading.current_thread().name}")
    return _thread_local.llm

# =============================================================================
# Multi-FIR Pre-processor
# =============================================================================
# Source data often contains multiple concatenated FIR cases in a single
# brief_facts field.  Only some of those FIRs are drug-related.  This
# deterministic Python pre-processor:
#   1. Splits the text at FIR boundaries ("IN THE HONOURABLE COURT …" headers)
#   2. Scores each section for drug-relevance using keyword matching
#   3. Returns ONLY the drug-relevant sections to the LLM
# No extra LLM calls — pure regex + keyword matching.
# =============================================================================

# Regex to split on FIR header boundaries
_FIR_BOUNDARY_RE = re.compile(
    r'(?=IN\s+(?:THE\s+)?HONOU?RABLE\s+(?:COURT|EXECUTIVE))',
    re.IGNORECASE
)

# Drug-relevance keywords (case-insensitive matching)
# Tier 1: Definitive drug/NDPS indicators → instantly relevant
_DRUG_KEYWORDS_TIER1 = {
    'ndps', 'narcotic', 'narcotics', 'psychotropic',
    'ganja', 'marijuana', 'cannabis', 'charas', 'hashish', 'hash',
    'heroin', 'smack', 'brown sugar', 'cocaine', 'crack',
    'opium', 'poppy', 'hemp', 'bhang',
    'mdma', 'ecstasy', 'lsd', 'methamphetamine', 'amphetamine',
    'ketamine', 'codeine', 'tramadol', 'alprazolam', 'morphine',
    'mephedrone', 'fentanyl', 'buprenorphine',
    'dry ganja', 'wet ganja',
}

# Tier 2: Contextual indicators — need at least 2 co-occurring to count
_DRUG_KEYWORDS_TIER2 = {
    'seized', 'substance', 'powder', 'tablet', 'capsule',
    'packet', 'packets', 'contraband', 'smuggling', 'transporting',
    'peddling', 'consumption', 'addiction', 'intoxicant',
}

# Section references that indicate NDPS Act
_NDPS_SECTION_RE = re.compile(
    r'\b(?:8\s*\([a-c]\)|20\s*\([a-c]\)|21|22|25|27|28|29)\b.*?NDPS|NDPS.*?\b(?:8|20|21|22|25|27|28|29)\b',
    re.IGNORECASE
)


def _estimate_tokens(text: str) -> int:
    """Rough token estimate: ~1 token per 4 characters for English text."""
    return len(text) // 4


def _score_drug_relevance(section: str) -> int:
    """
    Score a text section for drug-relevance.
    Returns:
      100+ : Definitive drug content (tier-1 keyword found)
      50-99: Probable drug content (NDPS section ref or multiple tier-2 keywords)
      0-49 : Unlikely drug content
    """
    lower = section.lower()
    score = 0

    # Tier 1 check — any single keyword is definitive
    for kw in _DRUG_KEYWORDS_TIER1:
        if kw in lower:
            score += 100
            break  # one is enough

    # NDPS section reference check
    if _NDPS_SECTION_RE.search(section):
        score += 80

    # Tier 2 — count co-occurrences
    t2_hits = sum(1 for kw in _DRUG_KEYWORDS_TIER2 if kw in lower)
    score += t2_hits * 15  # need ~4 co-occurring for threshold

    return score


def preprocess_brief_facts(text: str, relevance_threshold: int = 50) -> Tuple[str, dict]:
    """
    Pre-process brief_facts text before sending to LLM.

    1. Splits multi-FIR concatenated text into individual sections.
    2. Scores each section for drug-relevance.
    3. Returns only the drug-relevant text and metadata about what was filtered.

    Args:
        text: Raw brief_facts string (may contain 1 or many FIRs).
        relevance_threshold: Minimum drug-relevance score to keep a section.

    Returns:
        (filtered_text, metadata_dict)
        metadata_dict contains:
          - original_chars: int
          - filtered_chars: int
          - total_sections: int
          - kept_sections: int
          - dropped_sections: int
          - estimated_tokens_saved: int
          - sections_detail: list of (section_index, score, kept, first_80_chars)
    """
    if not text or not text.strip():
        return text, {"original_chars": 0, "filtered_chars": 0, "total_sections": 0,
                      "kept_sections": 0, "dropped_sections": 0, "estimated_tokens_saved": 0}

    # Split into sections
    sections = _FIR_BOUNDARY_RE.split(text)
    # Remove empty / whitespace-only sections
    sections = [s for s in sections if s and s.strip()]

    # If only 1 section (single FIR), skip filtering — pass through as-is
    if len(sections) <= 1:
        meta = {
            "original_chars": len(text),
            "filtered_chars": len(text),
            "total_sections": 1,
            "kept_sections": 1,
            "dropped_sections": 0,
            "estimated_tokens_saved": 0,
        }
        logger.info(f"Pre-processor: Single FIR detected ({_estimate_tokens(text)} est. tokens). No filtering needed.")
        return text, meta

    # Score each section
    scored = []
    for i, section in enumerate(sections):
        score = _score_drug_relevance(section)
        kept = score >= relevance_threshold
        scored.append((i, section, score, kept))

    kept_sections = [s for s in scored if s[3]]
    dropped_sections = [s for s in scored if not s[3]]

    # Build filtered text from kept sections only
    if kept_sections:
        filtered_text = "\n\n".join(s[1].strip() for s in kept_sections)
    else:
        # Edge case: no section passed the drug filter.
        # Return empty string — pipeline will insert NO_DRUGS_DETECTED placeholder.
        filtered_text = ""

    original_tokens = _estimate_tokens(text)
    filtered_tokens = _estimate_tokens(filtered_text)
    tokens_saved = original_tokens - filtered_tokens

    meta = {
        "original_chars": len(text),
        "filtered_chars": len(filtered_text),
        "total_sections": len(sections),
        "kept_sections": len(kept_sections),
        "dropped_sections": len(dropped_sections),
        "estimated_tokens_saved": tokens_saved,
        "sections_detail": [
            {
                "index": s[0],
                "score": s[2],
                "kept": s[3],
                "preview": s[1].strip()[:100].replace('\n', ' ')
            }
            for s in scored
        ],
    }

    logger.info(
        f"Pre-processor: {len(sections)} FIR sections detected → "
        f"kept {len(kept_sections)}, dropped {len(dropped_sections)} "
        f"(~{tokens_saved} tokens saved, {original_tokens}→{filtered_tokens})"
    )
    for s in scored:
        status = "KEEP" if s[3] else "DROP"
        preview = s[1].strip()[:80].replace('\n', ' ')
        logger.debug(f"  Section {s[0]}: score={s[2]:3d} [{status}] {preview}...")

    return filtered_text, meta


# --- Data Models ---
# Controlled vocabulary for drug physical state/form.
# This drives unit validation — the form MUST be consistent with the unit used.
# solid/powder/dry/resin/paste  → kg/grams only
# liquid/syrup/oil/solution     → litres/ml only
# tablet/pill/capsule/paper/seed/count → count (nos/pieces/tablets) only
DRUG_FORM_SOLID   = {'solid', 'dry', 'powder', 'paste', 'resin', 'chunk', 'crystal', 'granule', 'leaf', 'dried', 'compressed'}
DRUG_FORM_LIQUID  = {'liquid', 'syrup', 'oil', 'solution', 'tincture', 'extract', 'concentrate', 'fluid', 'injection'}
DRUG_FORM_COUNT   = {'tablet', 'pill', 'capsule', 'paper', 'blot', 'seed', 'strip', 'sachet', 'ampule', 'vial', 'bottle', 'plant', 'tree', 'sapling', 'seedling'}

class DrugExtraction(BaseModel):
    raw_drug_name: Optional[str] = Field(default="Unknown")
    raw_quantity: Optional[float] = 0.0
    raw_unit: Optional[str] = Field(default="Unknown")
    primary_drug_name: Optional[str] = Field(default="Unknown")
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
# Hybrid TOON (Token-Oriented Object Notation) prompt:
# - Critical behavioral rules kept verbose for reliability
# - Mechanical/boilerplate rules compressed to TOON shorthand
# - Knowledge base as pipe-delimited CSV
# - 1 compact example instead of 2
#
# FALLBACK: To revert to the original verbose prompt, swap EXTRACTION_PROMPT
# with EXTRACTION_PROMPT_VERBOSE below.

EXTRACTION_PROMPT_VERBOSE = """You are an expert forensic data analyst. Your task is to extract structured drug seizure information from police brief facts.
### I. Golden Rules
1. One Row Per Accused-Drug Combination: Each unique (accused, drug) pair MUST be a separate JSON entry.
2. Accused Identification: Normalize all accused references to A1, A2, A3... format.
3. Zero-Inference Extraction: Only extract explicit/implied values. Missing unit → lower confidence ~60.
4. Ignore Totals: Only per-accused quantities. Do NOT extract aggregate totals.
5. KB Matching: Map drug names using the Drug Knowledge Base.
6. Audit: extraction_metadata.source_sentence = exact source snippet.
7. Precision: Exact values, no rounding.
8. Per-accused entries are NOT duplicates. Duplicate = same accused + same drug + same qty repeated.
9. Confidence 0-100: 90-100 all clear, 70-89 partial, 50-69 missing info, <50 speculative.
10. Accused vs Customers: Only extract persons who POSSESSED/TRANSPORTED drugs at arrest. Skip customers/buyers mentioned in confessions.
11. Seized Quantity ONLY: Extract ONLY the quantity physically SEIZED at arrest. Do NOT extract purchased amounts, sold amounts, or post-sampling breakdowns (samples S1/S2, remaining property P1).
12. Collective vs Individual: If seizure is ONE TOTAL from a group with NO per-accused split → 1 entry, accused_id=null. If per-accused amounts given → separate entries.
13. Plant/Cultivation Seizures: "8 ganja plants" → raw_quantity=8, raw_unit="plants", drug_form="count". Plants ARE valid drug seizures under NDPS Act — ALWAYS extract them.
Container vs Content: "3 packets, 50g" → 50. "3 packets of 50g each" → 150.
Skip unknown/unidentified drug names. drug_form ∈ solid/liquid/count. seizure_worth = float rupees.
Drug Knowledge Base: {drug_knowledge_base}
Input: {text}
Return valid JSON matching: drugs:[{{raw_drug_name,raw_quantity,raw_unit,primary_drug_name,drug_form,accused_id,seizure_worth,confidence_score,extraction_metadata:{{source_sentence}}}}]
"""

EXTRACTION_PROMPT = """You are an expert forensic data analyst extracting structured drug seizure data from police brief facts.

## CORE RULES (STRICT — read carefully)
1. **One Row Per Accused-Drug Combination:** Each unique (accused, drug) pair MUST be a separate JSON entry.
   - A1 has Ganja AND Cocaine → 2 entries
   - A1 has Ganja AND A2 has Ganja → 2 entries
   - 6 persons each have 100g Ganja → 6 separate entries, NOT 1
   - NEVER merge accused. NEVER skip an accused because another has the same drug/quantity.
2. **Accused Identification:** Normalize ALL accused references to A1, A2, A3... by order of appearance.
   Formats: "A1"/"A-1", "Accused 1"/"Accused No. 1", numbered "1)"/"2)", or by name.
   If seizure is collective/unattributed → accused_id = null.
3. **Ignore Totals:** Only per-accused quantities. "A1 180g + A2 80g, total 260g" → 180g(A1) + 80g(A2). Do NOT add 260g entry.
4. **Per-accused entries ≠ duplicates.** 3 accused × 100g Ganja = 3 valid entries. A duplicate is ONLY same accused + same drug + same qty repeated in different sentences.

5. **Accused vs Customers/Buyers:** Only extract entries for persons who POSSESSED or TRANSPORTED drugs at the time of seizure. Do NOT create entries for customers, buyers, or associates merely mentioned in confessions as people the accused sold to. "sold to Sidhu, Karthik, Faraz" → these are NOT accused with seizures; skip them.

6. **Collective vs Individual Seizures:**
   - If the text specifies SEPARATE quantities per accused ("A1 had 180g, A2 had 80g") → create one entry per accused with their individual quantity.
   - If the text describes ONE TOTAL seizure from a GROUP without per-accused breakdown ("apprehended 6 persons... seized total 520 KGs dry ganja") → create ONLY **1 entry** with `accused_id = null` and the total quantity. Do NOT duplicate the total across each accused.
   - Example: "A1, A2, A3 caught with 520 KG ganja" → 1 entry: accused_id=null, raw_quantity=520, raw_unit="KGs"
   - Example: "seized 100g from A1 and 200g from A2" → 2 entries with individual quantities.

7. **Seized Quantity ONLY:** Extract ONLY the quantity physically SEIZED/RECOVERED at the time of arrest. Do NOT extract:
   - **Purchased quantities** — historical amounts bought before arrest ("purchased 100g" ≠ seized)
   - **Sold quantities** — amounts sold before arrest ("sold 25g to customers" ≠ seized)
   - **Post-sampling breakdowns** — forensic samples (S1/S2) and remaining property (P1) are PARTS of the total seizure; do NOT extract them as separate entries.
   - Example: "purchased 20 boxes (100g), sold 5 boxes (25g), seized 15 boxes (75g), drew 2 boxes sample (10g), remaining 13 boxes (65g) as P1" → extract ONLY **75g** (the total seized amount). Do NOT add entries for 100g, 65g, 25g, or 10g.

**REMEMBER Rule 6**: If the FIR lists multiple accused BUT the seizure is described as a SINGLE TOTAL ("seized total 520 KGs"), produce ONLY 1 entry with accused_id=null. Do NOT clone the total for each accused.

## COMPRESSED RULES
R5:zero-inference|extract only explicit/implied values|missing unit→confidence~60
R6:KB-match|text matches KB raw/standard name→primary_drug_name=Standard Name|not in KB→capitalize raw
R7:audit|extraction_metadata.source_sentence=verbatim source snippet
R8:precision|exact values,no rounding
R9:confidence(int 0-100)|90-100:name+qty+unit clear|70-89:partial|50-69:qty/unit missing|<50:speculative
R10:container-vs-content|"3 packets,50g"→50|"3×50g each"→150
R11:skip "unknown"/"unidentified" drug names
R12:drug_form∈{{solid,liquid,count}}|liquid drugs(oil,syrup,solution)→raw_unit MUST be ml/litres even if source says grams
R13:seizure_worth=float rupees|individual over collective
R14:plant/cultivation seizures|"8 ganja plants"→raw_quantity=8,raw_unit="plants",drug_form="count"|plants ARE valid drug seizures under NDPS Act—ALWAYS extract them

## Drug Knowledge Base
{drug_knowledge_base}
If text matches any raw_name or standard_name → set primary_drug_name to the corresponding standard_name.
If not in KB → set primary_drug_name to capitalized raw extraction.

## Output Schema
{{{{ "drugs": [ {{{{ "raw_drug_name":str, "raw_quantity":float, "raw_unit":str, "primary_drug_name":str, "drug_form":"solid|liquid|count", "accused_id":"A1|A2|...|null", "seizure_worth":float, "confidence_score":int, "extraction_metadata":{{{{ "source_sentence":str }}}} }}}} ] }}}}

## Examples
### Example 1 — per-accused quantities → separate entries
Input: "seized 100g Ganja from 1) Anil Kumar, 100g from 2) Jagadish, 100g from 3) Abhya Kumar"
{{{{"drugs":[
  {{{{"raw_drug_name":"Dry Ganja","raw_quantity":100.0,"raw_unit":"grams","primary_drug_name":"Ganja","drug_form":"solid","accused_id":"A1","seizure_worth":0.0,"confidence_score":95,"extraction_metadata":{{{{"source_sentence":"1) Anil Kumar 100 Grams of ganja"}}}}}}}},
  {{{{"raw_drug_name":"Dry Ganja","raw_quantity":100.0,"raw_unit":"grams","primary_drug_name":"Ganja","drug_form":"solid","accused_id":"A2","seizure_worth":0.0,"confidence_score":95,"extraction_metadata":{{{{"source_sentence":"2) Jagadish 100 grams of Ganja"}}}}}}}},
  {{{{"raw_drug_name":"Dry Ganja","raw_quantity":100.0,"raw_unit":"grams","primary_drug_name":"Ganja","drug_form":"solid","accused_id":"A3","seizure_worth":0.0,"confidence_score":95,"extraction_metadata":{{{{"source_sentence":"3) Abhya Kumar 100 grams of Ganja"}}}}}}}}
]}}}}

### Example 2 — collective seizure, NO per-accused breakdown → 1 entry, accused_id=null
Input: "apprehended A1 Sandeep, A2 Vinod, A3 Dhanaraj... Seized total 252 bundles wg 520 KGs dry ganja worth Rs.52,00,000"
{{{{"drugs":[
  {{{{"raw_drug_name":"Dry Ganja","raw_quantity":520.0,"raw_unit":"KGs","primary_drug_name":"Ganja","drug_form":"solid","accused_id":null,"seizure_worth":5200000.0,"confidence_score":95,"extraction_metadata":{{{{"source_sentence":"Seized total 252 bundles wg 520 KGs dry ganja worth about Rs.52,00,000"}}}}}}}}
]}}}}

## Input Text
{text}

EXTRACT EVERY ACCUSED-DRUG COMBINATION. If seizure is collective with NO per-accused breakdown, use accused_id=null. RETURN VALID JSON ONLY. NO MARKDOWN.
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

            # --- Strict Normalization ---
            # Step 1: lowercase + strip whitespace
            # Step 2: remove ALL non-alpha characters (dots, hyphens, spaces, etc.)
            # e.g. "Gms." -> "gms", "KG " -> "kg", "ml." -> "ml"
            raw_unit_str = drug.raw_unit if drug.raw_unit else "unknown"
            unit = re.sub(r'[^a-z]', '', raw_unit_str.lower().strip())
            form = re.sub(r'[^a-z]', '', drug.drug_form.lower().strip()) if drug.drug_form else "unknown"
            name = drug.raw_drug_name.lower().strip() if drug.raw_drug_name else ""

            # --- Auto-Classification ---

            # 1. Base classification on Unit first (most reliable)
            if unit in {'g', 'gm', 'gms', 'gram', 'grams', 'grm', 'grms', 'gr'}:
                drug.weight_g = qty
                drug.weight_kg = qty / 1000.0
            elif unit in {'kg', 'kgs', 'kilogram', 'kilograms', 'kilo', 'kilos'}:
                drug.weight_g = qty * 1000.0
                drug.weight_kg = qty
            elif unit in {'mg', 'milligram', 'milligrams'}:
                drug.weight_g = qty / 1000.0
                drug.weight_kg = qty / 1_000_000.0
            elif unit in {'l', 'ltr', 'ltrs', 'liter', 'liters', 'litre', 'litres'}:
                drug.volume_l = qty
                drug.volume_ml = qty * 1000.0
            elif unit in {'ml', 'milliliter', 'milliliters', 'millilitre', 'millilitres'}:
                drug.volume_ml = qty
                drug.volume_l = qty / 1000.0
            elif unit in {'no', 'nos', 'number', 'numbers', 'piece', 'pieces', 'pcs',
                          'tablet', 'tablets', 'pill', 'pills', 'strip', 'strips',
                          'box', 'boxes', 'packet', 'packets', 'sachet', 'sachets',
                          'blot', 'blots', 'dot', 'dots', 'bottle', 'bottles',
                          'unit', 'units', 'count', 'counts',
                          'plant', 'plants', 'tree', 'trees', 'sapling', 'saplings',
                          'seedling', 'seedlings', 'bush', 'bushes'}:
                drug.count_total = qty

            # 2. Fallback to Form if unit is unknown but qty > 0
            if qty > 0 and drug.weight_g is None and drug.volume_ml is None and drug.count_total is None:
                if form in DRUG_FORM_SOLID:
                    drug.weight_g = qty
                    drug.weight_kg = qty / 1000.0
                elif form in DRUG_FORM_LIQUID:
                    drug.volume_ml = qty
                    drug.volume_l = qty / 1000.0
                elif form in DRUG_FORM_COUNT:
                    drug.count_total = qty
                else:
                    drug.count_total = qty

            # 3. LIQUID CROSS-CHECK: If drug_form is liquid but values ended up in
            #    weight fields (because source said "grams"/"kg"), reclassify to volume.
            #    Assumption: density ≈ 1 g/ml (standard for drug seizure reporting).
            #    e.g. Hash Oil 65 grams → 65 ml, 0.065 L
            if form in DRUG_FORM_LIQUID or form == 'liquid':
                if drug.weight_g is not None and drug.weight_g > 0 and (drug.volume_ml is None or drug.volume_ml == 0):
                    logger.debug(
                        f"Liquid cross-check: {drug.raw_drug_name} — moving "
                        f"{drug.weight_g}g → {drug.weight_g}ml (density≈1)"
                    )
                    drug.volume_ml = drug.weight_g    # g → ml (1:1)
                    drug.volume_l = drug.weight_kg     # kg → L (1:1)
                    drug.weight_g = None
                    drug.weight_kg = None

            # 4. AUTO-DETECT LIQUID FORM from drug name if form was not set correctly.
            #    Some drugs are inherently liquid (oils, syrups, solutions) but LLM
            #    may still say "solid" or "Unknown".
            _LIQUID_DRUG_NAMES = {'hash oil', 'hashish oil', 'weed oil', 'cannabis oil',
                                 'opium solution', 'poppy husk solution', 'codeine syrup',
                                 'cough syrup', 'phensedyl', 'corex'}
            if name in _LIQUID_DRUG_NAMES or 'oil' in name or 'syrup' in name or 'solution' in name:
                if drug.weight_g is not None and drug.weight_g > 0 and (drug.volume_ml is None or drug.volume_ml == 0):
                    logger.debug(f"Auto-liquid: {drug.raw_drug_name} detected as liquid by name")
                    drug.volume_ml = drug.weight_g
                    drug.volume_l = drug.weight_kg
                    drug.weight_g = None
                    drug.weight_kg = None
                    drug.drug_form = "liquid"

            # 5. Ensure constraint check_has_measurements is met for 0 qty extractions
            if drug.weight_g is None and drug.weight_kg is None and drug.volume_l is None and drug.volume_ml is None and drug.count_total is None:
                drug.weight_g = 0.0
                drug.weight_kg = 0.0

            # --- Confidence Score Conversion ---
            # Convert confidence_score from percentage (e.g., 95) to ratio (e.g., 0.95) if it's >= 1.0
            if drug.confidence_score is not None and drug.confidence_score >= 1.0:
                drug.confidence_score = round(drug.confidence_score / 100, 2)

            # --- Name Standardization ---
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
            logger.error(f"Standardization error for {drug.raw_drug_name}: {e}", exc_info=True)
            
    return drugs


def _collapse_collective_seizures(drugs: List[DrugExtraction]) -> List[DrugExtraction]:
    """
    Detect and collapse collective seizures: when multiple accused have the
    EXACT SAME drug, quantity, and unit, it means the LLM duplicated a single
    collective seizure across each accused.  Collapse to 1 entry with
    accused_id = None and all accused refs stored in extraction_metadata.

    Trigger: 3+ entries share (primary_drug_name, raw_quantity, raw_unit)
    with DIFFERENT accused_ids.  This pattern only happens with collective
    seizures — individual per-accused seizures would have different quantities.
    """
    if len(drugs) < 3:
        return drugs

    from collections import defaultdict

    # Group by (drug, qty, unit) — ignore accused_id
    groups = defaultdict(list)
    for drug in drugs:
        gkey = (
            (drug.primary_drug_name or '').lower().strip(),
            round(float(drug.raw_quantity or 0), 2),
            re.sub(r'[^a-z]', '', (drug.raw_unit or '').lower().strip()),
        )
        groups[gkey].append(drug)

    result = []
    for gkey, group in groups.items():
        # Collect distinct accused_ids in this group
        accused_ids = set(
            d.accused_id.strip() for d in group
            if d.accused_id and d.accused_id.strip()
        )

        if len(accused_ids) >= 3 and len(group) == len(accused_ids):
            # Collective seizure detected — collapse to 1 entry
            best = max(group, key=lambda d: d.confidence_score or 0)
            accused_list = sorted(accused_ids)
            logger.info(
                f"Collective seizure detected: {len(accused_ids)} accused "
                f"({', '.join(accused_list)}) × {best.primary_drug_name} "
                f"{best.raw_quantity} {best.raw_unit} → collapsing to 1 entry "
                f"with accused_id=null"
            )
            best.accused_id = None
            meta = best.extraction_metadata or {}
            meta['collective_accused'] = accused_list
            meta['collapse_reason'] = (
                f"Same drug/qty/unit across {len(accused_ids)} accused "
                f"with no per-accused breakdown in source text"
            )
            best.extraction_metadata = meta
            result.append(best)
        else:
            # Individual seizures — keep all
            result.extend(group)

    if len(result) < len(drugs):
        logger.info(f"Collective collapse: {len(drugs)} → {len(result)} entries")

    return result


def deduplicate_extractions(drugs: List[DrugExtraction], max_per_crime: int = 100) -> List[DrugExtraction]:
    """
    Remove duplicate drug extractions and cap at max_per_crime.
    Also collapses collective seizures (same drug/qty/unit across 3+ accused).
    Deduplicates by (accused_id, primary_drug_name, raw_drug_name, raw_quantity, raw_unit).
    This preserves:
      - Same drug, different accused  (A1 Ganja 100g + A2 Ganja 100g → 2 entries)
      - Same accused, different drugs  (A1 Ganja 180g + A1 MDMA 10 nos → 2 entries)
      - Same accused, same drug, different quantity (A1 Ganja 180g + A1 Ganja 50g → 2 entries)
    Keeps the highest confidence entry when exact duplicates exist.
    """
    if not drugs:
        return drugs

    # Step 1: Collapse collective seizures BEFORE dedup
    drugs = _collapse_collective_seizures(drugs)

    seen = {}
    for drug in drugs:
        key = (
            (drug.accused_id or '').lower().strip(),
            (drug.primary_drug_name or '').lower().strip(),
            (drug.raw_drug_name or '').lower().strip(),
            round(float(drug.raw_quantity or 0), 2),
            (drug.raw_unit or '').lower().strip()
        )
        existing = seen.get(key)
        if not existing or (drug.confidence_score or 0) > (existing.confidence_score or 0):
            seen[key] = drug

    deduped = list(seen.values())

    if len(drugs) > len(deduped):
        logger.info(f"Deduplicated extractions: {len(drugs)} -> {len(deduped)}")

    if len(deduped) > max_per_crime:
        logger.warning(f"Capping extractions from {len(deduped)} to {max_per_crime}")
        deduped = sorted(deduped, key=lambda d: d.confidence_score or 0, reverse=True)[:max_per_crime]

    return deduped


def extract_drug_info(text: str, drug_categories: List[dict] = None) -> List[DrugExtraction]:
    """
    Extracts a list of drug information objects from the given text.
    Includes multi-FIR pre-processing to filter out non-drug-related FIR sections.
    """
    if drug_categories is None:
        drug_categories = []

    # ── Step 0: Pre-process — split multi-FIR text, keep only drug-relevant sections ──
    filtered_text, preprocess_meta = preprocess_brief_facts(text)

    if not filtered_text or not filtered_text.strip():
        logger.info("Pre-processor filtered out ALL sections (no drug content detected). Returning empty.")
        return []

    # ── Step 1: Token budget check ──
    est_input_tokens = _estimate_tokens(filtered_text)
    # Prompt template + KB overhead ≈ 800 tokens; LLM context window = 16384
    CONTEXT_WINDOW = 16384
    PROMPT_OVERHEAD = 800
    kb_token_est = _estimate_tokens("\n".join(
        f"{c.get('raw_name','')}{c.get('standard_name','')}{c.get('category_group','')}"
        for c in drug_categories
    )) if drug_categories else 0
    available_for_input = CONTEXT_WINDOW - PROMPT_OVERHEAD - kb_token_est
    if est_input_tokens > available_for_input:
        logger.warning(
            f"Token budget tight: input ~{est_input_tokens} tokens, "
            f"available ~{available_for_input} (window={CONTEXT_WINDOW}, "
            f"prompt={PROMPT_OVERHEAD}, KB={kb_token_est}). Text may be truncated by LLM."
        )
    else:
        logger.info(f"Token budget OK: input ~{est_input_tokens}/{available_for_input} available tokens.")
        
    # Format the drug categories knowledge base as pipe-delimited CSV (TOON-optimized)
    kb_lines = []
    if drug_categories:
        kb_lines.append("raw_name|standard_name|category")
        for cat in drug_categories:
            raw = cat.get('raw_name', 'Unknown')
            std = cat.get('standard_name', 'Unknown')
            grp = cat.get('category_group', '-')
            kb_lines.append(f"{raw}|{std}|{grp}")
    else:
        kb_lines.append("(No KB provided, use standard extraction)")
    
    formatted_kb = "\n".join(kb_lines)
    
    parser = JsonOutputParser(pydantic_object=CrimeReportExtraction)
    prompt = ChatPromptTemplate.from_template(EXTRACTION_PROMPT)
    
    try:
        # Use thread-safe LLM instance (each thread gets its own ChatOllama)
        llm = _get_thread_safe_llm()
        chain = prompt | llm | parser
        
        input_data = {"text": filtered_text, "drug_knowledge_base": formatted_kb}
        response = invoke_extraction_with_retry(chain, input_data, max_retries=1)
        
        if not response:
            logger.warning("LLM returned empty response (all retries failed). Returning empty.")
            return []
        
        drugs_data = response.get("drugs", [])
        if not drugs_data:
            logger.info(f"LLM returned 0 drugs from response keys: {list(response.keys())}")
            return []
        
        logger.info(f"LLM returned {len(drugs_data)} raw drug entries.")
        
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
                logger.warning(f"Skipping invalid drug entry: {e} | data: {d}")
        
        # Post-process (Unit Calc + Dedup)
        standardized = standardize_units(valid_drugs)
        return deduplicate_extractions(standardized)
        
    except Exception as e:
        logger.error(f"Drug extraction failed: {e}", exc_info=True)
        return []

if __name__ == "__main__":
    test_text = "A1 had 1 packet containing 7 grams Ganja."
    print("Testing extraction...")
    extractions = extract_drug_info(test_text)
    for d in extractions:
        print(d.model_dump_json(indent=2))

