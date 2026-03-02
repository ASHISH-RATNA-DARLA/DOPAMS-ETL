
import re
import logging
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
DRUG_FORM_COUNT   = {'tablet', 'pill', 'capsule', 'paper', 'blot', 'seed', 'strip', 'sachet', 'ampule', 'vial', 'bottle'}

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

## COMPRESSED RULES
R5:zero-inference|extract only explicit/implied values|missing unit→confidence~60
R6:KB-match|text matches KB raw/standard name→primary_drug_name=Standard Name|not in KB→capitalize raw
R7:audit|extraction_metadata.source_sentence=verbatim source snippet
R8:precision|exact values,no rounding
R9:confidence(int 0-100)|90-100:name+qty+unit clear|70-89:partial|50-69:qty/unit missing|<50:speculative
R10:container-vs-content|"3 packets,50g"→50|"3×50g each"→150
R11:skip "unknown"/"unidentified" drug names
R12:drug_form∈{{solid,liquid,count}}
R13:seizure_worth=float rupees|individual over collective

## Drug Knowledge Base
{drug_knowledge_base}
If text matches any raw_name or standard_name → set primary_drug_name to the corresponding standard_name.
If not in KB → set primary_drug_name to capitalized raw extraction.

## Output Schema
{{{{ "drugs": [ {{{{ "raw_drug_name":str, "raw_quantity":float, "raw_unit":str, "primary_drug_name":str, "drug_form":"solid|liquid|count", "accused_id":"A1|A2|...|null", "seizure_worth":float, "confidence_score":int, "extraction_metadata":{{{{ "source_sentence":str }}}} }}}} ] }}}}

## Example — 3 accused, same drug, same qty → 3 entries
Input: "seized 100g Ganja from 1) Anil Kumar, 100g from 2) Jagadish, 100g from 3) Abhya Kumar"
{{{{"drugs":[
  {{{{"raw_drug_name":"Dry Ganja","raw_quantity":100.0,"raw_unit":"grams","primary_drug_name":"Ganja","drug_form":"solid","accused_id":"A1","seizure_worth":0.0,"confidence_score":95,"extraction_metadata":{{{{"source_sentence":"1) Anil Kumar 100 Grams of ganja"}}}}}}}},
  {{{{"raw_drug_name":"Dry Ganja","raw_quantity":100.0,"raw_unit":"grams","primary_drug_name":"Ganja","drug_form":"solid","accused_id":"A2","seizure_worth":0.0,"confidence_score":95,"extraction_metadata":{{{{"source_sentence":"2) Jagadish 100 grams of Ganja"}}}}}}}},
  {{{{"raw_drug_name":"Dry Ganja","raw_quantity":100.0,"raw_unit":"grams","primary_drug_name":"Ganja","drug_form":"solid","accused_id":"A3","seizure_worth":0.0,"confidence_score":95,"extraction_metadata":{{{{"source_sentence":"3) Abhya Kumar 100 grams of Ganja"}}}}}}}}
]}}}}

## Input Text
{text}

EXTRACT EVERY ACCUSED-DRUG COMBINATION. RETURN VALID JSON ONLY. NO MARKDOWN.
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
                          'unit', 'units', 'count', 'counts'}:
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

            # 3. Ensure constraint check_has_measurements is met for 0 qty extractions
            if drug.weight_g is None and drug.weight_kg is None and drug.volume_l is None and drug.volume_ml is None and drug.count_total is None:
                drug.weight_g = 0.0
                drug.weight_kg = 0.0

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


def deduplicate_extractions(drugs: List[DrugExtraction], max_per_crime: int = 100) -> List[DrugExtraction]:
    """
    Remove duplicate drug extractions and cap at max_per_crime.
    Deduplicates by (accused_id, primary_drug_name, raw_drug_name, raw_quantity, raw_unit).
    This preserves:
      - Same drug, different accused  (A1 Ganja 100g + A2 Ganja 100g → 2 entries)
      - Same accused, different drugs  (A1 Ganja 180g + A1 MDMA 10 nos → 2 entries)
      - Same accused, same drug, different quantity (A1 Ganja 180g + A1 Ganja 50g → 2 entries)
    Keeps the highest confidence entry when exact duplicates exist.
    """
    if not drugs:
        return drugs

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
        llm_service = get_llm('extraction')
        llm = llm_service.get_langchain_model()
        chain = prompt | llm | parser
        
        response = invoke_extraction_with_retry(chain, {"text": filtered_text, "drug_knowledge_base": formatted_kb}, max_retries=1)
        
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
        
        # Post-process (Unit Calc + Dedup)
        standardized = standardize_units(valid_drugs)
        return deduplicate_extractions(standardized)
        
    except Exception as e:
        print(f"Error during extraction: {e}")
        return []

if __name__ == "__main__":
    test_text = "A1 had 1 packet containing 7 grams Ganja."
    print("Testing extraction...")
    extractions = extract_drug_info(test_text)
    for d in extractions:
        print(d.model_dump_json(indent=2))

