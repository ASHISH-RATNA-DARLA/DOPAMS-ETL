import re
import logging
import threading
from typing import List, Optional, Set, Dict, Tuple
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
#
# CHANGE: _DRUG_KEYWORDS_TIER1 is now a static FALLBACK only.
# The real keyword set is built dynamically from drug_categories KB via
# build_drug_keywords() at pipeline startup and passed into
# preprocess_brief_facts() as `dynamic_drug_keywords`.
# This ensures ALL 330+ KB entries (Nitrazepam, Tramadol, Spasmo Proxyvon,
# Pentazocine, Butorphanol, Tapentadol, THC, all precursors, etc.) are
# covered in section scoring — not just the 25 hardcoded names.
# =============================================================================

# Regex to split on FIR header boundaries
_FIR_BOUNDARY_RE = re.compile(
    r'(?=IN\s+(?:THE\s+)?HONOU?RABLE\s+(?:COURT|EXECUTIVE))',
    re.IGNORECASE
)

# Static fallback Tier 1 keywords — used ONLY when no dynamic set is provided
# (e.g. unit tests, standalone runs).  In production the dynamic set from
# build_drug_keywords() replaces this entirely.
_DRUG_KEYWORDS_TIER1_FALLBACK = {
    'ndps', 'narcotic', 'narcotics', 'psychotropic',
    'ganja', 'marijuana', 'cannabis', 'charas', 'hashish', 'hash',
    'heroin', 'smack', 'brown sugar', 'cocaine', 'crack',
    'opium', 'poppy', 'hemp', 'bhang',
    'mdma', 'ecstasy', 'lsd', 'methamphetamine', 'amphetamine',
    'ketamine', 'codeine', 'tramadol', 'alprazolam', 'morphine',
    'mephedrone', 'fentanyl', 'buprenorphine',
    'dry ganja', 'wet ganja',
    # Extended fallback to cover common KB drugs missing from original set
    'nitravet', 'nitrazepam', 'tydol', 'fortwin', 'pentazocine',
    'tapentadol', 'butorphanol', 'mephentermine', 'spasmo', 'proxyvon',
    'thc', 'charas', 'mandrax', 'methaqualone', 'phencyclidine',
    'etizolam', 'clonazepam', 'diazepam', 'midazolam', 'zolpidem',
    'chlordiazepoxide', 'barbiturate', 'meow', 'mephedrone',
    'ganga chocolate', 'magic mushroom', 'toddy',
}

# Tier 2: Contextual indicators — need co-occurrences to count
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


def build_drug_keywords(drug_categories: List[dict]) -> Set[str]:
    """
    Build a comprehensive drug-detection keyword set from the drug_categories KB.

    Called once at startup in main.py. The returned set is passed into
    preprocess_brief_facts() as `dynamic_drug_keywords` to replace the static
    _DRUG_KEYWORDS_TIER1_FALLBACK.

    Strategy:
    - Add every raw_name and standard_name (lowercased, full phrase) as a keyword
    - Also add individual tokens of length >= 4 (avoids noise from very short
      abbreviations like 'md', 'or', etc.)
    - Always union with the static fallback to retain NDPS/narcotic/etc. terms
      that are not drug names per se but are strong relevance signals

    Thread-safety: the returned set is read-only once built — safe to share
    across all worker threads without locking.
    """
    keywords = set(_DRUG_KEYWORDS_TIER1_FALLBACK)  # start with static fallback

    for row in drug_categories:
        for field in ('raw_name', 'standard_name'):
            val = (row.get(field) or '').lower().strip()
            if not val:
                continue
            keywords.add(val)  # full phrase match (e.g. "dry mixed heroin powder")
            # Individual tokens for partial matching
            for token in val.split():
                if len(token) >= 4:
                    keywords.add(token)

    logger.info(f"build_drug_keywords: {len(keywords)} keywords built from {len(drug_categories)} KB entries.")
    return keywords


def _estimate_tokens(text: str) -> int:
    """Rough token estimate: ~1 token per 4 characters for English text."""
    return len(text) // 4


def _score_drug_relevance(section: str, dynamic_drug_keywords: Set[str] = None) -> int:
    """
    Score a text section for drug-relevance.

    Uses dynamic_drug_keywords (from KB) when provided, otherwise falls back
    to the static _DRUG_KEYWORDS_TIER1_FALLBACK.

    Returns:
      100+ : Definitive drug content (tier-1 keyword found)
      50-99: Probable drug content (NDPS section ref or multiple tier-2 keywords)
      0-49 : Unlikely drug content
    """
    tier1 = dynamic_drug_keywords if dynamic_drug_keywords else _DRUG_KEYWORDS_TIER1_FALLBACK
    lower = section.lower()
    score = 0

    # Tier 1 check — any single keyword match is definitive
    for kw in tier1:
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


def preprocess_brief_facts(
    text: str,
    relevance_threshold: int = 50,
    dynamic_drug_keywords: Set[str] = None,
) -> Tuple[str, dict]:
    """
    Pre-process brief_facts text before sending to LLM.

    1. Splits multi-FIR concatenated text into individual sections.
    2. Scores each section for drug-relevance using dynamic_drug_keywords
       (built from the full KB) or the static fallback if not provided.
    3. Returns only the drug-relevant text and metadata about what was filtered.

    Args:
        text:                   Raw brief_facts string (may contain 1 or many FIRs).
        relevance_threshold:    Minimum drug-relevance score to keep a section.
        dynamic_drug_keywords:  KB-derived keyword set from build_drug_keywords().
                                When provided, ALL drugs in drug_categories are
                                detectable — not just the 25 static ones.

    Returns:
        (filtered_text, metadata_dict)
    """
    if not text or not text.strip():
        return text, {"original_chars": 0, "filtered_chars": 0, "total_sections": 0,
                      "kept_sections": 0, "dropped_sections": 0, "estimated_tokens_saved": 0}

    # Split into sections
    sections = _FIR_BOUNDARY_RE.split(text)
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
        score = _score_drug_relevance(section, dynamic_drug_keywords)
        kept = score >= relevance_threshold
        scored.append((i, section, score, kept))

    kept_sections = [s for s in scored if s[3]]
    dropped_sections = [s for s in scored if not s[3]]

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


# =============================================================================
# Data Models
# =============================================================================
# Controlled vocabulary for drug physical state/form.
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
    confidence_score: Optional[float] = Field(default=0.80, description="Confidence out of 1.0 (e.g. 0.95)")
    seizure_worth: Optional[float] = 0.0
    worth_scope: Optional[str] = Field(
        default="individual",
        description="Scope of seizure_worth: 'individual' (per accused-drug), 'drug_total' (total for this drug type), 'overall_total' (total for all drugs)"
    )
    extraction_metadata: dict = Field(default_factory=dict)

    # Calculated measurement fields
    weight_g: Optional[float] = None
    weight_kg: Optional[float] = None
    volume_ml: Optional[float] = None
    volume_l: Optional[float] = None
    count_total: Optional[float] = None
    is_commercial: bool = False


class CrimeReportExtraction(BaseModel):
    drugs: List[DrugExtraction]


# =============================================================================
# Extraction Prompt
# =============================================================================
# FALLBACK verbose prompt kept for reference / rollback.
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
12. Collective vs Individual: If seizure is ONE TOTAL from a group with NO per-accused split → 1 entry. If per-accused amounts given → separate entries.
13. Plant/Cultivation Seizures: "8 ganja plants" → raw_quantity=8, raw_unit="plants", drug_form="count". Plants ARE valid drug seizures under NDPS Act — ALWAYS extract them.
Container vs Content: "3 packets, 50g" → 50. "3 packets of 50g each" → 150.
Skip unknown/unidentified drug names. drug_form ∈ solid/liquid/count. seizure_worth = float rupees.
Drug Knowledge Base: {drug_knowledge_base}
Input: {text}
Return valid JSON matching: drugs:[{{raw_drug_name,raw_quantity,raw_unit,primary_drug_name,drug_form,seizure_worth,worth_scope,confidence_score,extraction_metadata:{{source_sentence}}}}]
"""

EXTRACTION_PROMPT = """You are an expert forensic data analyst extracting structured drug seizure data from police brief facts.

## CORE RULES (STRICT — read carefully)
1. **One Row Per Drug Seizure:** Each unique drug seizure incident MUST be a separate JSON entry.
   - A1 has Ganja AND Cocaine → 2 entries
   - A1 has Ganja AND A2 has Ganja → 2 entries (same drug, different persons, seized separately)
   - 6 persons each have 100g Ganja seized together → 1 entry (collective seizure)
   - NEVER merge or combine seizures that should be separate.
2. **Collective vs Attributed Seizures:** Normalize the context:
   - Attributed seizures (per-person amounts clearly separate) → create multiple entries
   - Collective seizures (GROUP total, no per-person breakdown) → 1 entry for the total
3. **Ignore Totals:** Only per-accused quantities. "A1 180g + A2 80g, total 260g" → 180g(A1) + 80g(A2). Do NOT add 260g entry.
4. **Per-accused entries ≠ duplicates.** 3 accused × 100g Ganja = 3 valid entries. A duplicate is ONLY same accused + same drug + same qty repeated in different sentences.

5. **Accused vs Customers/Buyers:** Only extract entries for persons who POSSESSED or TRANSPORTED drugs at the time of seizure. Do NOT create entries for customers, buyers, or associates merely mentioned in confessions as people the accused sold to. "sold to Sidhu, Karthik, Faraz" → these are NOT accused with seizures; skip them.

6. **Collective vs Individual Seizures:**
   - If the text specifies SEPARATE quantities per person ("A1 had 180g, A2 had 80g") → create one entry per person with their individual quantity.
   - If the text describes ONE TOTAL seizure from a GROUP without per-person breakdown ("apprehended 6 persons... seized total 520 KGs dry ganja") → create ONLY **1 entry** with the total quantity. Do NOT duplicate the total across each person.
   - Example: "A1, A2, A3 caught with 520 KG ganja" → 1 entry: raw_quantity=520, raw_unit="KGs"
   - Example: "seized 100g from A1 and 200g from A2" → 2 entries with individual quantities.

7. **Seized Quantity ONLY:** Extract ONLY the quantity physically SEIZED/RECOVERED at the time of arrest. Do NOT extract:
   - **Purchased quantities** — historical amounts bought before arrest ("purchased 100g" ≠ seized)
   - **Sold quantities** — amounts sold before arrest ("sold 25g to customers" ≠ seized)
   - **Post-sampling breakdowns** — forensic samples (S1/S2) and remaining property (P1) are PARTS of the total seizure; do NOT extract them as separate entries.
   - Example: "purchased 20 boxes (100g), sold 5 boxes (25g), seized 15 boxes (75g), drew 2 boxes sample (10g), remaining 13 boxes (65g) as P1" → extract ONLY **75g** (the total seized amount). Do NOT add entries for 100g, 65g, 25g, or 10g.

**REMEMBER Rule 6**: If the FIR lists multiple accused BUT the seizure is described as a SINGLE TOTAL ("seized total 520 KGs"), produce ONLY 1 entry with that total. Do NOT clone the total for each person.

8. **Seizure Worth (MANDATORY):** Extract the monetary value ("worth") of EACH drug as `seizure_worth` in **rupees (float)**.
   - Look for patterns: "worth Rs.", "W/Rs:", "valued at Rs.", "worth about Rs.", "worth approximately", "market value", "valued", "costing Rs.", "worth of Rs."
   - Parse Indian number formats: Rs.52,00,000 = 5200000.0 | Rs.5,00,000 = 500000.0 | Rs.10,000 = 10000.0 | Rs.1,00,00,000 = 10000000.0
   - **Per-drug mapping:** If worth is mentioned alongside a specific drug, map it to THAT drug only.
     Example: "seized 500g Ganja worth Rs.5,00,000 and 100g Charas worth Rs.2,00,000" → Ganja gets 500000.0, Charas gets 200000.0
   - If a single "worth" covers all drugs collectively → assign the FULL total value to EVERY entry. Post-processing will distribute proportionally.
   - If NO worth/value is mentioned in the text → seizure_worth = 0.0
   - NEVER default to 0.0 when worth IS mentioned in the text.
   - **worth_scope (MANDATORY):** Indicates the scope of the seizure_worth value:
     - `"individual"` → worth is explicitly stated FOR THIS specific accused-drug pair (e.g., "A1 had 200g Ganja worth Rs.5,000")
     - `"drug_total"` → worth is the TOTAL for this drug type across all accused (e.g., "total Ganja 700g worth Rs.20,000" but quantities are per-accused). Assign the FULL total to each entry.
     - `"overall_total"` → worth is ONE combined total for ALL drugs in the seizure (e.g., "total seizure worth Rs.1,00,000"). Assign the FULL total to each entry.
     - If no worth is mentioned → worth_scope = "individual" and seizure_worth = 0.0

## COMPRESSED RULES
R5:zero-inference|extract only explicit/implied values|missing unit→confidence~60
R6:KB-match|text matches KB raw/standard name→primary_drug_name=Standard Name|not in KB→capitalize raw
R7:audit|extraction_metadata.source_sentence=verbatim source snippet
R8:precision|exact values,no rounding
R9:confidence(int 0-100)|90-100:name+qty+unit clear|70-89:partial|50-69:qty/unit missing|<50:speculative
R10:container-vs-content|"3 packets,50g"→50|"3×50g each"→150
R11:skip "unknown"/"unidentified" drug names
R12:drug_form∈{{solid,liquid,count}}|liquid drugs(oil,syrup,solution)→raw_unit MUST be ml/litres even if source says grams
R13:plant/cultivation seizures|"8 ganja plants"→raw_quantity=8,raw_unit="plants",drug_form="count"|plants ARE valid drug seizures under NDPS Act—ALWAYS extract them
R14:is_commercial(bool)|if brief facts explicitly says "commercial quantity" or "above commercial quantity"→true|if not mentioned→false|do NOT guess—only set true when TEXT states it
R15:decimal-quantity|"1.200 Kg" or "1.500 Kgs"→the dot is a DECIMAL separator→raw_quantity=1.2 or 1.5, NOT 1200 or 1500|Indian FIR quantities under 100 Kg use decimals, not thousands separators|same for grams: "6.585 grams"→6.585
R16:cash-is-NOT-worth|"seized Rs.500/- from his possession" or "amount of Rs 500/-"→this is CASH/CURRENCY seized, NOT drug seizure worth|do NOT assign cash amounts to seizure_worth|seizure_worth is ONLY the estimated VALUE of the DRUG itself
R17:W/Rs-is-always-worth|"W/Rs:" "W/Rs." "W/Rs" are abbreviations for "Worth Rupees" written by the Investigating Officer→ALWAYS extract the number following this pattern as seizure_worth|this is the officer's official worth estimate, NOT a purchase price|even if the same rupee amount appeared earlier as a purchase price, the W/Rs: notation is the authoritative worth entry|common formats: "W/Rs: 10,000/-", "W/Rs. 80,000/-", "W/Rs 2,52,800/-"
R18:purchase-price-is-NOT-worth|"purchased at Rs.10,000/- per KG" or "bought for Rs.5,000/-"→this is PURCHASE PRICE, NOT seizure worth|seizure_worth must come from "worth Rs.", "W/Rs:", "valued at", "worth of Rs.", "worth about Rs." patterns ONLY|if ONLY a purchase price appears with NO W/Rs or worth pattern → seizure_worth=0.0
R19:per-kg-rate-is-NOT-worth|"at the rate of Rs.10,000/- per KG"→this is a RATE, not a value for specific seized quantity|do NOT multiply rate × quantity to compute worth—only extract worth when explicitly stated
R20:non-drug-seizures|NEVER create entries for co-seized property that is NOT a narcotic/psychotropic substance under NDPS Act|SKIP entries for: vehicles (motorcycle, car, scooter, truck, auto, bike, two-wheeler, tractor), mobile phones, SIM cards, cash/currency (seized cash is NOT drug worth), weighing scales, packaging material, lighters, match boxes, rolling papers, OCB papers, empty sachets, empty covers, alcohol brands (whisky, beer, rum, vodka), cigarettes, tobacco products, chillum, bong, paraphernalia|RULE: if the item cannot be charged under NDPS Act as a narcotic or psychotropic substance — DO NOT extract it
R21:multi-drug-in-one-sentence|if a single sentence mentions multiple distinct substances, each substance MUST be a separate entry|Example: "seized 60g Ganja, 5g Charas and 10 tablets Alprazolam" → 3 entries, NOT 1

## Drug Knowledge Base
{drug_knowledge_base}
If text matches any raw_name or standard_name → set primary_drug_name to the corresponding standard_name.
If not in KB → set primary_drug_name to capitalized raw extraction.

## Output Schema
{{{{ "drugs": [ {{{{ "raw_drug_name":str, "raw_quantity":float, "raw_unit":str, "primary_drug_name":str, "drug_form":"solid|liquid|count", "seizure_worth":float, "worth_scope":"individual|drug_total|overall_total", "is_commercial":bool, "confidence_score":int, "extraction_metadata":{{{{ "source_sentence":str }}}} }}}} ] }}}}

## Examples
### Example 1 — per-accused with individual worth, commercial mentioned in text
Input: "seized 100g Ganja worth Rs.50,000 from 1) Anil Kumar, 100g worth Rs.50,000 from 2) Jagadish, 100g worth Rs.50,000 from 3) Abhya Kumar. The total seized quantity is above commercial quantity under NDPS Act."
{{{{"drugs":[
  {{{"raw_drug_name":"Dry Ganja","raw_quantity":100.0,"raw_unit":"grams","primary_drug_name":"Ganja","drug_form":"solid","seizure_worth":50000.0,"worth_scope":"individual","is_commercial":true,"confidence_score":95,"extraction_metadata":{{{{"source_sentence":"1) Anil Kumar 100 Grams of ganja worth Rs.50,000"}}}}}}}}},
  {{{"raw_drug_name":"Dry Ganja","raw_quantity":100.0,"raw_unit":"grams","primary_drug_name":"Ganja","drug_form":"solid","seizure_worth":50000.0,"worth_scope":"individual","is_commercial":true,"confidence_score":95,"extraction_metadata":{{{{"source_sentence":"2) Jagadish 100 grams of Ganja worth Rs.50,000"}}}}}}}}},
  {{{"raw_drug_name":"Dry Ganja","raw_quantity":100.0,"raw_unit":"grams","primary_drug_name":"Ganja","drug_form":"solid","seizure_worth":50000.0,"worth_scope":"individual","is_commercial":true,"confidence_score":95,"extraction_metadata":{{{{"source_sentence":"3) Abhya Kumar 100 grams of Ganja worth Rs.50,000"}}}}}}}}
]}}}}

### Example 2 — collective seizure with worth → 1 entry (group total)
Input: "apprehended Sandeep, Vinod, Dhanaraj... Seized total 252 bundles wg 520 KGs dry ganja worth Rs.52,00,000"
{{{{{"drugs":[
  {{{{"raw_drug_name":"Dry Ganja","raw_quantity":520.0,"raw_unit":"KGs","primary_drug_name":"Ganja","drug_form":"solid","seizure_worth":5200000.0,"worth_scope":"individual","is_commercial":false,"confidence_score":95,"extraction_metadata":{{{{"source_sentence":"Seized total 252 bundles wg 520 KGs dry ganja worth about Rs.52,00,000"}}}}}}}}
]}}}}

### Example 3 — multiple drugs, each with its own worth
Input: "seized 500g Ganja worth Rs.5,00,000 and 50g Charas worth Rs.2,00,000"
{{{{{"drugs":[
  {{{{"raw_drug_name":"Ganja","raw_quantity":500.0,"raw_unit":"grams","primary_drug_name":"Ganja","drug_form":"solid","seizure_worth":500000.0,"worth_scope":"individual","is_commercial":false,"confidence_score":95,"extraction_metadata":{{{{"source_sentence":"seized 500g Ganja worth Rs.5,00,000"}}}}}}}},
  {{{{"raw_drug_name":"Charas","raw_quantity":50.0,"raw_unit":"grams","primary_drug_name":"Charas","drug_form":"solid","seizure_worth":200000.0,"worth_scope":"individual","is_commercial":false,"confidence_score":95,"extraction_metadata":{{{{"source_sentence":"50g Charas worth Rs.2,00,000"}}}}}}}}
]}}}}

### Example 4 — per-accused quantities with collective total worth (drug_total)
Input: "found 300 Grms of Ganja from A1, 200 grms from A2 and 200 grms from A3. The seized total Ganja of 700 Grms worth of Rs.20,000/-"
{{{{"drugs":[
  {{{"raw_drug_name":"Ganja","raw_quantity":300.0,"raw_unit":"grams","primary_drug_name":"Ganja","drug_form":"solid","seizure_worth":20000.0,"worth_scope":"drug_total","is_commercial":false,"confidence_score":95,"extraction_metadata":{{{{"source_sentence":"found 300 Grms of Ganja from A1"}}}}}}}}},
  {{{"raw_drug_name":"Ganja","raw_quantity":200.0,"raw_unit":"grams","primary_drug_name":"Ganja","drug_form":"solid","seizure_worth":20000.0,"worth_scope":"drug_total","is_commercial":false,"confidence_score":95,"extraction_metadata":{{{{"source_sentence":"200 grms from A2"}}}}}}}}},
  {{{"raw_drug_name":"Ganja","raw_quantity":200.0,"raw_unit":"grams","primary_drug_name":"Ganja","drug_form":"solid","seizure_worth":20000.0,"worth_scope":"drug_total","is_commercial":false,"confidence_score":95,"extraction_metadata":{{{{"source_sentence":"200 grms from A3"}}}}}}}}
]}}}}

### Example 5 — multiple drugs + accused with one overall total worth
Input: "seized 20g Heroin from A1, 30g Heroin from A2, 30g Cocaine from A3. Total seizure worth Rs.1,00,000"
{{{{"drugs":[
  {{{"raw_drug_name":"Heroin","raw_quantity":20.0,"raw_unit":"grams","primary_drug_name":"Heroin","drug_form":"solid","seizure_worth":100000.0,"worth_scope":"overall_total","is_commercial":false,"confidence_score":95,"extraction_metadata":{{{{"source_sentence":"seized 20g Heroin from A1"}}}}}}}}},
  {{{"raw_drug_name":"Heroin","raw_quantity":30.0,"raw_unit":"grams","primary_drug_name":"Heroin","drug_form":"solid","seizure_worth":100000.0,"worth_scope":"overall_total","is_commercial":false,"confidence_score":95,"extraction_metadata":{{{{"source_sentence":"30g Heroin from A2"}}}}}}}}},
  {{{"raw_drug_name":"Cocaine","raw_quantity":30.0,"raw_unit":"grams","primary_drug_name":"Cocaine","drug_form":"solid","seizure_worth":100000.0,"worth_scope":"overall_total","is_commercial":false,"confidence_score":95,"extraction_metadata":{{{{"source_sentence":"30g Cocaine from A3"}}}}}}}}
]}}}}

### Example 6 — W/Rs pattern with prior purchase price mention (R17)
Input: "purchased 60g Ganja from Durgam Rajkumar for Rs.4000/- and while proceeding to sell it, police seized 60 Grams dry Ganja and Hero HF Deluxe motorcycle B No TS 19G 4409 from possession. W/Rs 4000/-"
{{{{"drugs":[
  {{{{"raw_drug_name":"Dry Ganja","raw_quantity":60.0,"raw_unit":"grams","primary_drug_name":"Ganja","drug_form":"solid","seizure_worth":4000.0,"worth_scope":"individual","is_commercial":false,"confidence_score":95,"extraction_metadata":{{{{"source_sentence":"seized 60 Grams dry Ganja ... W/Rs 4000/-"}}}}}}}}
]}}}}
NOTE: Motorcycle is NOT extracted (R20). W/Rs 4000/- is seizure_worth even though Rs.4000/- appeared earlier as purchase price (R17).

## Input Text
{text}

EXTRACT EVERY DRUG SEIZURE. If seizure is collective with NO per-person breakdown, produce ONE entry with the total quantity. Extract seizure_worth from "worth Rs.", "W/Rs:", "valued at", "worth of Rs." mentions — map each worth to its specific drug. Set worth_scope to indicate if the value is individual, drug_total, or overall_total. Set is_commercial=true ONLY if the text explicitly mentions "commercial quantity". NEVER extract vehicles, phones, cash, paraphernalia, or alcohol as drug entries (R20). RETURN VALID JSON ONLY. NO MARKDOWN.
"""


def _safe_prompt_template(template: str) -> str:
    escaped = template.replace("{", "{{").replace("}", "}}")
    escaped = escaped.replace("{{text}}", "{text}")
    escaped = escaped.replace("{{drug_knowledge_base}}", "{drug_knowledge_base}")
    return escaped


# =============================================================================
# Post-processing Step 1 (NEW): Resolve primary_drug_name via KB lookup
# =============================================================================
def resolve_primary_drug_name(drugs: List[DrugExtraction], kb_lookup: Dict[str, str]) -> List[DrugExtraction]:
    """
    Deterministic KB-based name resolution — runs AFTER LLM extraction.

    Why: The LLM's KB matching is probabilistic. This step makes it deterministic:
    if raw_drug_name exactly matches a KB raw_name, we OVERRIDE primary_drug_name
    with the authoritative standard_name regardless of what the LLM returned.

    Matching priority:
    1. Exact lowercase match:  raw_drug_name.lower() == kb_raw_name
    2. Substring match:        any kb_raw_name is contained in raw_drug_name.lower()
       (handles cases like "60 Grams floating and flowering dry Ganja" containing "dry ganja")
    3. Reverse substring:      raw_drug_name.lower() is contained in any kb_raw_name
       (handles abbreviations / short names)
    4. No match → keep LLM's primary_drug_name as-is

    Also removes the fragile hardcoded cannabis-variant check that was in
    standardize_units() — the KB lookup handles it properly via the 11 Ganja
    entries in drug_categories.

    Thread-safety: kb_lookup is a read-only dict — safe for concurrent use.
    """
    if not kb_lookup:
        return drugs

    for drug in drugs:
        raw = (drug.raw_drug_name or '').lower().strip()
        if not raw or raw == 'unknown':
            continue

        resolved = None

        # 1. Exact match
        if raw in kb_lookup:
            resolved = kb_lookup[raw]

        # 2. Any KB key is a substring of the raw name
        if not resolved:
            for kb_raw, kb_std in kb_lookup.items():
                if kb_raw in raw:
                    resolved = kb_std
                    break

        # 3. Raw name is a substring of any KB key (short name / abbreviation)
        if not resolved and len(raw) >= 4:
            for kb_raw, kb_std in kb_lookup.items():
                if raw in kb_raw:
                    resolved = kb_std
                    break

        if resolved and resolved != drug.primary_drug_name:
            logger.debug(
                f"KB resolve: '{drug.raw_drug_name}' → '{resolved}' "
                f"(was '{drug.primary_drug_name}')"
            )
            drug.primary_drug_name = resolved

    return drugs


# =============================================================================
# Post-processing Step 2 (NEW): Filter non-drug entries via ignore list
# =============================================================================
def filter_non_drug_entries(
    drugs: List[DrugExtraction],
    ignore_set: Set[str],
) -> List[DrugExtraction]:
    """
    Drop entries whose primary_drug_name exactly matches a term in ignore_set.

    IMPORTANT DESIGN DECISION — exact match on primary_drug_name ONLY:
    - Applied AFTER resolve_primary_drug_name() so primary_drug_name is already
      standardized (e.g. "Morphine", "Ganja", "Motorcycle").
    - NEVER applied as substring against raw_drug_name — analysis showed this
      causes false positives: ignore term 'powder' would drop 'dry mixed heroin
      powder' (Heroin), 'rumorf' would drop 'rumorf-30' (Morphine), etc.
    - Exact match on standardized primary_drug_name is safe because:
        * Real drugs resolve to clean names like "Ganja", "Heroin", "Tramadol"
        * Non-drug items resolve to names like "Motorcycle", "Alcohol" which
          are then caught by the ignore list

    Hardcoded safety-net (SEIZED_NON_DRUG_ITEMS):
    - Independent of the DB table — catches items even if ignore list is empty
    - Only contains tokens that can NEVER be a drug name
    - Checked via substring against primary_drug_name to catch composed names
      (e.g. "Hero HF Deluxe Motorcycle" contains "motorcycle")
    """
    # Hardcoded safety net — items that are NEVER drugs under NDPS Act
    SEIZED_NON_DRUG_ITEMS = {
        'motorcycle', 'motor cycle', 'motorbike', 'scooter', 'moped',
        'car', 'truck', 'lorry', 'tractor', 'auto', 'vehicle', 'two-wheeler',
        'mobile', 'mobile phone', 'cell phone', 'smartphone', 'sim card', 'sim',
        'cash', 'currency', 'rupees', 'money', 'notes',
        'weighing scale', 'weighing machine', 'digital scale', 'balance',
        'kite string', 'manja', 'chinese manja',
    }

    kept = []
    for drug in drugs:
        primary = (drug.primary_drug_name or '').lower().strip()

        # Check 1: exact match against DB ignore list
        if primary in ignore_set:
            reason = 'DB ignore list'
            logger.info(
                f"[IgnoreFilter] Dropped '{drug.raw_drug_name}' "
                f"(primary='{drug.primary_drug_name}') — matched ignore term '{primary}' [{reason}]"
            )
            continue

        # Check 2: substring match against hardcoded non-drug safety net
        matched_safetynet = next(
            (item for item in SEIZED_NON_DRUG_ITEMS if item in primary or item in (drug.raw_drug_name or '').lower()),
            None
        )
        if matched_safetynet:
            logger.info(
                f"[IgnoreFilter] Dropped '{drug.raw_drug_name}' "
                f"(primary='{drug.primary_drug_name}') — matched safety-net term '{matched_safetynet}'"
            )
            continue

        kept.append(drug)

    dropped_count = len(drugs) - len(kept)
    if dropped_count > 0:
        logger.info(f"[IgnoreFilter] Dropped {dropped_count} non-drug entries, kept {len(kept)}.")

    return kept


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

    CHANGE: Removed the cannabis-variant hardcoded check:
        is_cannabis_variant = any(x in name for x in ['kush', 'og', 'weed', ...])
    This is now handled properly by resolve_primary_drug_name() via KB lookup,
    which maps all 11 cannabis raw_name variants in drug_categories to "Ganja".
    """
    for drug in drugs:
        try:
            # 1. TRUNCATE STRINGS to prevent DB errors (VARCHAR(50))
            drug.raw_unit = truncate_string(drug.raw_unit, 50)
            drug.drug_form = truncate_string(drug.drug_form, 50)

            qty = float(drug.raw_quantity) if drug.raw_quantity else 0.0

            # Strict normalization: lowercase, strip, remove non-alpha
            raw_unit_str = drug.raw_unit if drug.raw_unit else "unknown"
            unit = re.sub(r'[^a-z]', '', raw_unit_str.lower().strip())
            form = re.sub(r'[^a-z]', '', drug.drug_form.lower().strip()) if drug.drug_form else "unknown"
            name = drug.raw_drug_name.lower().strip() if drug.raw_drug_name else ""

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
            elif unit in {
                'no', 'nos', 'number', 'numbers', 'piece', 'pieces', 'pcs',
                'tablet', 'tablets', 'pill', 'pills', 'strip', 'strips',
                'box', 'boxes', 'packet', 'packets', 'sachet', 'sachets',
                'blot', 'blots', 'dot', 'dots', 'bottle', 'bottles',
                'unit', 'units', 'count', 'counts',
                'plant', 'plants', 'tree', 'trees', 'sapling', 'saplings',
                'seedling', 'seedlings', 'bush', 'bushes',
                # Additional FIR-specific container units seen in NDPS cases
                'cover', 'covers', 'polythene', 'wrap', 'bundle', 'bundles',
                'puri', 'puris', 'katta', 'kattas', 'pouch', 'pouches',
                'vial', 'vials', 'ampule', 'ampules', 'ampoule', 'ampoules',
                'injection', 'injections', 'capsule', 'capsules',
            }:
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
            if form in DRUG_FORM_LIQUID or form == 'liquid':
                if drug.weight_g is not None and drug.weight_g > 0 and (drug.volume_ml is None or drug.volume_ml == 0):
                    logger.debug(
                        f"Liquid cross-check: {drug.raw_drug_name} — moving "
                        f"{drug.weight_g}g → {drug.weight_g}ml (density≈1)"
                    )
                    drug.volume_ml = drug.weight_g
                    drug.volume_l = drug.weight_kg
                    drug.weight_g = None
                    drug.weight_kg = None

            # 4. AUTO-DETECT LIQUID FORM from drug name if form was not set correctly.
            _LIQUID_DRUG_NAMES = {
                'hash oil', 'hashish oil', 'weed oil', 'cannabis oil',
                'opium solution', 'poppy husk solution', 'codeine syrup',
                'cough syrup', 'phensedyl', 'corex',
            }
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

            # Confidence Score Conversion: percentage → ratio
            if drug.confidence_score is not None and drug.confidence_score >= 1.0:
                drug.confidence_score = round(drug.confidence_score / 100, 2)

            # Name fallback: if primary_drug_name still unknown after KB resolve, use raw
            if not drug.primary_drug_name or drug.primary_drug_name == "Unknown":
                drug.primary_drug_name = drug.raw_drug_name

            # NOTE: cannabis variant override REMOVED — handled by resolve_primary_drug_name()

            # Default form check
            if not drug.drug_form or drug.drug_form.lower() in ['unknown', 'none', 'null']:
                drug.drug_form = "Unknown"

        except Exception as e:
            logger.error(f"Standardization error for {drug.raw_drug_name}: {e}", exc_info=True)

    return drugs


# =============================================================================
# NDPS Commercial Quantity Thresholds
# Source: NDPS Act, 1985 — Schedule notification by Government of India.
# =============================================================================
COMMERCIAL_QUANTITY_KG = {
    'ganja':           20.0,
    'charas':           1.0,
    'hashish':          1.0,
    'heroin':           0.250,
    'cocaine':          0.500,
    'opium':            2.5,
    'morphine':         0.250,
    'methamphetamine':  0.050,
    'amphetamine':      0.050,
    'mdma':             0.050,
    'mdm':              0.050,
    'ecstasy':          0.050,
    'ephedrine':        1.0,
    'pseudoephedrine':  1.0,
    'ketamine':         0.500,
    'mephedrone':       0.050,
    'codeine':          1.0,
    'buprenorphine':    0.050,
    'fentanyl':         0.050,
    'poppy straw':     50.0,
    'poppy husk':      50.0,
}
COMMERCIAL_QUANTITY_L = {
    'hash oil':         1.0,
    'hashish oil':      1.0,
    'hashish/weed oil': 1.0,
    'cannabis oil':     1.0,
    'liquid opium':     2.5,
}
COMMERCIAL_QUANTITY_COUNT = {
    'lsd':          100.0,
    'alprazolam':  1000.0,
    'tramadol':    1000.0,
    'diazepam':    1000.0,
    'nitrazepam':  1000.0,
    'clonazepam':  1000.0,
}


def _apply_commercial_quantity_check(drugs: List[DrugExtraction]) -> List[DrugExtraction]:
    """
    Post-processing: Check if the TOTAL seized quantity per drug meets or
    exceeds the NDPS commercial quantity threshold. Marks all entries for
    that drug as is_commercial=True if threshold is met.
    """
    if not drugs:
        return drugs

    from collections import defaultdict

    drug_groups = defaultdict(list)
    for drug in drugs:
        key = (drug.primary_drug_name or '').lower().strip()
        drug_groups[key].append(drug)

    for drug_name, group in drug_groups.items():
        # If any entry already marked commercial by LLM, propagate to all
        if any(d.is_commercial for d in group):
            for d in group:
                d.is_commercial = True
            logger.info(f"is_commercial: '{drug_name}' — LLM flagged as commercial, applied to all {len(group)} entries")
            continue

        total_kg    = sum(float(d.weight_kg or 0) for d in group)
        total_l     = sum(float(d.volume_l or 0) for d in group)
        total_count = sum(float(d.count_total or 0) for d in group)

        is_comm = False
        threshold_info = ""

        if total_kg > 0 and drug_name in COMMERCIAL_QUANTITY_KG:
            threshold = COMMERCIAL_QUANTITY_KG[drug_name]
            if total_kg >= threshold:
                is_comm = True
                threshold_info = f"weight {total_kg:.3f}kg >= {threshold}kg"

        if not is_comm and total_l > 0 and drug_name in COMMERCIAL_QUANTITY_L:
            threshold = COMMERCIAL_QUANTITY_L[drug_name]
            if total_l >= threshold:
                is_comm = True
                threshold_info = f"volume {total_l:.3f}L >= {threshold}L"

        if not is_comm and total_count > 0 and drug_name in COMMERCIAL_QUANTITY_COUNT:
            threshold = COMMERCIAL_QUANTITY_COUNT[drug_name]
            if total_count >= threshold:
                is_comm = True
                threshold_info = f"count {total_count:.0f} >= {threshold:.0f}"

        if is_comm:
            logger.info(
                f"is_commercial: '{drug_name}' — total {threshold_info} "
                f"(across {len(group)} entries) → marking ALL as commercial"
            )
            for d in group:
                d.is_commercial = True

    return drugs


def _distribute_seizure_worth(drugs: List[DrugExtraction]) -> List[DrugExtraction]:
    """
    Post-processing: Distribute seizure_worth proportionally based on worth_scope.

    Rules (in priority order):
    1. individual    → keep as-is
    2. drug_total    → split proportionally within the same drug group by quantity
    3. overall_total → split proportionally across ALL entries by quantity
    4. No worth (0.0) → keep as 0.0
    """
    if not drugs:
        return drugs

    from collections import defaultdict

    individual_entries    = []
    drug_total_entries    = []
    overall_total_entries = []
    zero_worth_entries    = []

    for drug in drugs:
        scope = (drug.worth_scope or 'individual').lower().strip()
        worth = float(drug.seizure_worth or 0)

        if worth == 0.0:
            zero_worth_entries.append(drug)
        elif scope == 'individual':
            individual_entries.append(drug)
        elif scope == 'drug_total':
            drug_total_entries.append(drug)
        elif scope == 'overall_total':
            overall_total_entries.append(drug)
        else:
            individual_entries.append(drug)

    # drug_total → split proportionally within each drug group
    if drug_total_entries:
        drug_groups = defaultdict(list)
        for d in drug_total_entries:
            key = (d.primary_drug_name or '').lower().strip()
            drug_groups[key].append(d)

        for drug_name, group in drug_groups.items():
            total_worth = max(float(d.seizure_worth or 0) for d in group)
            quantities  = [
                float(d.weight_g or 0) or float(d.volume_ml or 0) or float(d.count_total or 0)
                for d in group
            ]
            total_qty = sum(quantities)

            if total_qty > 0 and total_worth > 0:
                for d, qty in zip(group, quantities):
                    d.seizure_worth = round((qty / total_qty) * total_worth, 2)
                    logger.info(
                        f"Worth distribution (drug_total): {drug_name} — "
                        f"{qty}g/{total_qty}g × ₹{total_worth} = ₹{d.seizure_worth}"
                    )
            elif total_qty == 0 and total_worth > 0:
                equal_share = round(total_worth / len(group), 2)
                for d in group:
                    d.seizure_worth = equal_share
                    logger.info(
                        f"Worth distribution (drug_total, equal): {drug_name} — "
                        f"₹{equal_share} (1/{len(group)} of ₹{total_worth})"
                    )

    # overall_total → split proportionally across ALL entries
    if overall_total_entries:
        total_worth = max(float(d.seizure_worth or 0) for d in overall_total_entries)
        quantities  = [
            float(d.weight_g or 0) or float(d.volume_ml or 0) or float(d.count_total or 0)
            for d in overall_total_entries
        ]
        total_qty = sum(quantities)

        if total_qty > 0 and total_worth > 0:
            for d, qty in zip(overall_total_entries, quantities):
                d.seizure_worth = round((qty / total_qty) * total_worth, 2)
                logger.info(
                    f"Worth distribution (overall_total): "
                    f"{d.primary_drug_name}: "
                    f"{qty}/{total_qty} × ₹{total_worth} = ₹{d.seizure_worth}"
                )
        elif total_qty == 0 and total_worth > 0:
            for d in overall_total_entries:
                d.seizure_worth = total_worth
                logger.info(
                    f"Worth distribution (overall_total, no qty): "
                    f"{d.primary_drug_name} — keeping ₹{total_worth}"
                )

    # Recombine (preserve original order)
    all_processed = set(
        id(d) for d in
        individual_entries + drug_total_entries + overall_total_entries + zero_worth_entries
    )
    result = [d for d in drugs if id(d) in all_processed]
    return result


def _collapse_collective_seizures(drugs: List[DrugExtraction]) -> List[DrugExtraction]:
    """
    No-op: collective seizure detection was based on accused_id which no longer
    exists in the schema. Accused information is not stored in the database.
    """
    return drugs


def deduplicate_extractions(drugs: List[DrugExtraction], max_per_crime: int = 100) -> List[DrugExtraction]:
    """
    Remove duplicate drug extractions and cap at max_per_crime.
    Deduplicates by (primary_drug_name, raw_drug_name, raw_quantity, raw_unit).
    Keeps the highest confidence entry when exact duplicates exist.
    """
    if not drugs:
        return drugs

    seen = {}
    for drug in drugs:
        key = (
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


# =============================================================================
# Main extraction entry point
# =============================================================================
def extract_drug_info(
    text: str,
    drug_categories: List[dict] = None,
    ignore_set: Set[str] = None,
    kb_lookup: Dict[str, str] = None,
    dynamic_drug_keywords: Set[str] = None,
) -> List[DrugExtraction]:
    """
    Extracts a list of drug information objects from the given text.

    Full pipeline (in order):
      Step 0  — Preprocessor: split multi-FIR text, keep only drug-relevant
                sections using dynamic_drug_keywords (KB-driven, not static).
      Step 1  — Token budget check.
      Step 2  — LLM extraction via EXTRACTION_PROMPT (with R20/R21 added).
      Step 3  — Input sanitization (None guards, worth_scope validation).
      Step 4  — resolve_primary_drug_name(): deterministic KB override of
                primary_drug_name — fixes LLM mis-mappings.
      Step 5  — filter_non_drug_entries(): drop vehicles, alcohol, paraphernalia
                etc. using DB ignore_set + hardcoded safety net.
      Step 6  — standardize_units(): unit → weight_g/kg, volume_ml/l, count_total.
      Step 7  — _distribute_seizure_worth(): proportional worth distribution.
      Step 8  — _apply_commercial_quantity_check(): NDPS threshold → is_commercial.
      Step 9  — deduplicate_extractions(): dedup + cap at 100.

    Args:
        text:                   Raw brief_facts text for one crime.
        drug_categories:        List of KB dicts (raw_name, standard_name, category_group).
        ignore_set:             Set of lowercased terms from drug_ignore_list DB table.
                                Exact-matched against primary_drug_name post-KB-resolve.
        kb_lookup:              Dict {raw_name_lower: standard_name} for deterministic
                                name resolution (Step 4).
        dynamic_drug_keywords:  KB-derived token set for preprocessor scoring (Step 0).

    Returns:
        List of DrugExtraction objects. Empty list if no drugs found or error.
    """
    if drug_categories is None:
        drug_categories = []
    if ignore_set is None:
        ignore_set = set()
    if kb_lookup is None:
        kb_lookup = {}

    # ── Step 0: Pre-process — split multi-FIR text, keep only drug-relevant sections ──
    filtered_text, preprocess_meta = preprocess_brief_facts(
        text,
        dynamic_drug_keywords=dynamic_drug_keywords,
    )

    if not filtered_text or not filtered_text.strip():
        logger.info("Pre-processor filtered out ALL sections (no drug content detected). Returning empty.")
        return []

    # ── Step 1: Token budget check ──
    est_input_tokens = _estimate_tokens(filtered_text)
    CONTEXT_WINDOW   = 16384
    PROMPT_OVERHEAD  = 900   # slightly higher now with R20/R21 additions
    kb_token_est     = _estimate_tokens("\n".join(
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

    # ── Step 2: Format KB for prompt ──
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
    prompt  = ChatPromptTemplate.from_template(_safe_prompt_template(EXTRACTION_PROMPT))

    try:
        # ── Step 2 cont: LLM call (thread-safe per-thread instance) ──
        llm   = _get_thread_safe_llm()
        chain = prompt | llm | parser

        input_data = {"text": filtered_text, "drug_knowledge_base": formatted_kb}
        response   = invoke_extraction_with_retry(chain, input_data, max_retries=1)

        if not response:
            logger.warning("LLM returned empty response (all retries failed). Returning empty.")
            return []

        drugs_data = response.get("drugs", [])
        if not drugs_data:
            logger.info(f"LLM returned 0 drugs from response keys: {list(response.keys())}")
            return []

        logger.info(f"LLM returned {len(drugs_data)} raw drug entries.")

        # ── Step 3: Input sanitization ──
        valid_drugs = []
        for d in drugs_data:
            try:
                if d.get('raw_quantity') is None: d['raw_quantity'] = 0.0
                if d.get('confidence_score') is None: d['confidence_score'] = 90
                if d.get('seizure_worth') is None: d['seizure_worth'] = 0.0
                if not d.get('raw_drug_name'): d['raw_drug_name'] = "Unknown"

                if d.get('is_commercial') is None: d['is_commercial'] = False
                if isinstance(d.get('is_commercial'), str):
                    d['is_commercial'] = d['is_commercial'].lower() in ('true', '1', 'yes')

                if str(d.get('raw_quantity')).lower() == "none": d['raw_quantity'] = 0.0
                if str(d.get('seizure_worth')).lower() == "none": d['seizure_worth'] = 0.0
                if d.get('raw_unit') is None: d['raw_unit'] = "Unknown"

                if isinstance(d.get('seizure_worth'), str):
                    try:
                        d['seizure_worth'] = float(str(d['seizure_worth']).replace(',', ''))
                    except Exception:
                        d['seizure_worth'] = 0.0
                elif d.get('seizure_worth') is None:
                    d['seizure_worth'] = 0.0

                valid_scopes = {'individual', 'drug_total', 'overall_total'}
                ws = str(d.get('worth_scope', 'individual')).lower().strip()
                d['worth_scope'] = ws if ws in valid_scopes else 'individual'

                valid_drugs.append(DrugExtraction(**d))
            except Exception as e:
                logger.warning(f"Skipping invalid drug entry: {e} | data: {d}")

        # ── Step 4: Deterministic KB name resolution ──
        kb_resolved = resolve_primary_drug_name(valid_drugs, kb_lookup)

        # ── Step 5: Drop non-drug entries (ignore list + safety net) ──
        filtered = filter_non_drug_entries(kb_resolved, ignore_set)

        # ── Steps 6-9: Unit standardization → Worth distribution → Commercial check → Dedup ──
        standardized       = standardize_units(filtered)
        worth_distributed  = _distribute_seizure_worth(standardized)
        commercial_checked = _apply_commercial_quantity_check(worth_distributed)
        return deduplicate_extractions(commercial_checked)

    except Exception as e:
        logger.error(f"Drug extraction failed: {e}", exc_info=True)
        return []


if __name__ == "__main__":
    test_text = "A1 had 1 packet containing 7 grams Ganja."
    print("Testing extraction...")
    extractions = extract_drug_info(test_text)
    for d in extractions:
        print(d.model_dump_json(indent=2))