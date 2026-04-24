import re
import logging
import threading
from typing import List, Optional, Set, Dict, Tuple
from pydantic import BaseModel, Field
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser  # noqa: F401  (kept for back-compat)
import sys
import os
import re
import httpx

def _safe_prompt_template(template: str) -> str:
    """Escape all braces except those intended for LangChain formatting."""
    # First, escape everything by doubling all { and }
    escaped = template.replace("{", "{{").replace("}", "}}")
    # Then restore the intended placeholders (only {text} is used in current EXTRACTION_PROMPT)
    # The restore logic must use the now-escaped forms {{ and }}
    return escaped.replace("{{text}}", "{text}")

logger = logging.getLogger(__name__)

# Ensure core is accessible
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from core.llm_service import get_llm, invoke_extraction_with_retry, RobustJsonOutputParser
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
        
        timeout_seconds = float(os.getenv("LLM_TIMEOUT", "300"))
        http_client = httpx.Client(timeout=timeout_seconds)
        
        _thread_local.llm = ChatOllama(
            base_url=base_url,
            model=llm_service.model,
            temperature=llm_service.temperature,
            num_ctx=llm_service.context_window,
            num_predict=llm_service.max_tokens,
            keep_alive=os.getenv("OLLAMA_KEEP_ALIVE", "60m"),
            client=http_client,
        )
        logger.info(f"Created thread-local ChatOllama for thread {threading.current_thread().name} (HTTP timeout: {timeout_seconds}s)")
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
    supplier_name: Optional[str] = None
    source_location: Optional[str] = None
    destination: Optional[str] = None
    purchase_price_per_unit: Optional[float] = None
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


_ACCUSED_REF_PATTERN = re.compile(r'\bA\s*[-.]?\s*(\d+)\b', flags=re.IGNORECASE)
_SEGMENT_QUANTITY_PATTERN = re.compile(
    r'(?P<qty>\d+(?:\.\d+)?)\s*(?P<unit>kg|kgs|kilograms?|g|gm|gms|gram|grams|grm|grms|mg|ml|l|ltr|litre|litres)\b',
    flags=re.IGNORECASE,
)
_PACKET_QUANTITY_PATTERN = re.compile(
    r'(?P<idx>\d+)\s*[\)\.:\-]?\s*(?P<qty>\d+(?:\.\d+)?)\s*(?P<unit>kg|kgs|kilograms?|g|gm|gms|gram|grams|grm|grms|mg|ml|l|ltr|litre|litres|packet|packets|piece|pieces|cover|covers|bundle|bundles)\b',
    flags=re.IGNORECASE,
)
_PACKET_CONTEXT_RE = re.compile(r'\b(packet|packets|cover|covers|bundle|bundles|transparent|polythene|plastic)\b', re.IGNORECASE)


def _best_drug_keyword_match(text: str, kb_lookup: Dict[str, str]) -> Tuple[Optional[str], Optional[str]]:
    if not text:
        return None, None

    lowered = text.lower()
    best_raw = None
    best_standard = None
    best_len = 0

    for raw_name, standard_name in (kb_lookup or {}).items():
        raw = (raw_name or '').lower().strip()
        std = (standard_name or '').strip()
        if not raw:
            continue
        if raw in lowered and len(raw) > best_len:
            best_raw = raw_name
            best_standard = standard_name
            best_len = len(raw)

    return best_raw, best_standard


def _infer_unique_accused_ref(text: str) -> Optional[str]:
    if not text:
        return None

    matches = []
    for match in _ACCUSED_REF_PATTERN.finditer(text):
        normalized = f"A-{int(match.group(1))}"
        if normalized not in matches:
            matches.append(normalized)

    if len(matches) == 1:
        return matches[0]
    return None


def _extract_explicit_packet_rows(text: str, kb_lookup: Dict[str, str]) -> List[dict]:
    """
    Deterministically expand packet lists like "1) 2.130 KGs 2) 3.090 KGs" into
    explicit packet rows so the extractor does not collapse them into a single total.
    """
    if not text:
        return []

    rows = []
    sentences = re.split(r'(?<=[.!?])\s+|\n+', text)
    for sentence in sentences:
        if not sentence:
            continue
        qty_matches = list(_PACKET_QUANTITY_PATTERN.finditer(sentence))
        if len(qty_matches) < 2:
            continue
        if not _PACKET_CONTEXT_RE.search(sentence):
            continue

        drug_raw, drug_standard = _best_drug_keyword_match(sentence, kb_lookup)
        if not drug_standard and not drug_raw:
            continue

        accused_ref = _infer_unique_accused_ref(sentence)

        seen_quantities = set()
        for packet_index, match in enumerate(qty_matches, start=1):
            qty = float(match.group('qty'))
            unit = match.group('unit')
            qty_key = (round(qty, 3), unit.lower())
            if qty_key in seen_quantities:
                continue
            seen_quantities.add(qty_key)

            raw_name = drug_raw or drug_standard or 'Unknown'
            standard_name = drug_standard or drug_raw or 'Unknown'
            if isinstance(raw_name, str):
                raw_name = raw_name.title() if raw_name.islower() else raw_name
            if isinstance(standard_name, str):
                standard_name = standard_name.title() if standard_name.islower() else standard_name

            rows.append({
                'raw_drug_name': raw_name,
                'raw_quantity': qty,
                'raw_unit': unit,
                'primary_drug_name': standard_name,
                'drug_form': 'solid',
                'confidence_score': 95,
                'seizure_worth': 0.0,
                'worth_scope': 'individual',
                'supplier_name': None,
                'source_location': None,
                'destination': None,
                'purchase_price_per_unit': None,
                'extraction_metadata': {
                    'source_sentence': sentence.strip(),
                    'packet_index': packet_index,
                    'accused_ref': accused_ref,
                    'explicit_packet_row': True,
                },
            })

    return rows


def _extract_segmented_accused_rows(text: str, kb_lookup: Dict[str, str]) -> List[dict]:
    """
    Expand clauses that explicitly mention an accused code (A1, A2, ...) and a
    seizure quantity into one row per accused. This keeps per-accused packet
    quantities separate even when the narrative is written as a single paragraph.
    """
    if not text:
        return []

    accused_matches = list(_ACCUSED_REF_PATTERN.finditer(text))
    if not accused_matches:
        return []

    rows = []
    for index, match in enumerate(accused_matches):
        start = match.start()
        end = accused_matches[index + 1].start() if index + 1 < len(accused_matches) else len(text)
        segment = text[start:end].strip()
        if not segment:
            continue

        if not _PACKET_CONTEXT_RE.search(segment) and not re.search(r'\bganja\b|\bcannabis\b|\bcharas\b|\bmarijuana\b', segment, re.IGNORECASE):
            continue

        qty_match = _SEGMENT_QUANTITY_PATTERN.search(segment)
        if not qty_match:
            continue

        drug_raw, drug_standard = _best_drug_keyword_match(segment, kb_lookup)
        if not drug_standard and not drug_raw:
            continue

        qty = float(qty_match.group('qty'))
        unit = qty_match.group('unit')
        accused_ref = f"A-{int(match.group(1))}"

        raw_name = drug_raw or drug_standard or 'Unknown'
        standard_name = drug_standard or drug_raw or 'Unknown'
        if isinstance(raw_name, str):
            raw_name = raw_name.title() if raw_name.islower() else raw_name
        if isinstance(standard_name, str):
            standard_name = standard_name.title() if standard_name.islower() else standard_name

        rows.append({
            'raw_drug_name': raw_name,
            'raw_quantity': qty,
            'raw_unit': unit,
            'primary_drug_name': standard_name,
            'drug_form': 'solid',
            'confidence_score': 96,
            'seizure_worth': 0.0,
            'worth_scope': 'individual',
            'supplier_name': None,
            'source_location': None,
            'destination': None,
            'purchase_price_per_unit': None,
            'extraction_metadata': {
                'source_sentence': segment,
                'accused_ref': accused_ref,
                'explicit_segment_row': True,
            },
        })

    return rows


def _drop_redundant_total_rows(drugs: List[DrugExtraction], packet_rows: List[dict]) -> List[DrugExtraction]:
    """
    Remove confession-style total rows when explicit packet rows already exist for
    the same drug and their quantities sum to the total.
    """
    if not drugs or not packet_rows:
        return drugs

    from collections import defaultdict

    packet_sums = defaultdict(float)
    packet_counts = defaultdict(int)
    for packet in packet_rows:
        key = (str(packet.get('primary_drug_name') or '').lower().strip())
        packet_sums[key] += float(packet.get('raw_quantity') or 0.0)
        packet_counts[key] += 1

    kept = []
    for drug in drugs:
        drug_key = (drug.primary_drug_name or '').lower().strip()
        total_qty = float(drug.raw_quantity or 0.0)
        source_sentence = str((drug.extraction_metadata or {}).get('source_sentence') or '').lower()

        if packet_counts.get(drug_key, 0) >= 2:
            packet_total = round(packet_sums[drug_key], 3)
            if packet_total > 0 and abs(round(total_qty, 3) - packet_total) <= 0.01:
                if any(marker in source_sentence for marker in ('total', 'about', 'approx', 'approximately', 'purchased', 'came to', 'came with', 'confessed')):
                    logger.info(
                        f"Dropped redundant total drug row: {drug.raw_drug_name} qty={drug.raw_quantity} "
                        f"because explicit packet rows total {packet_total} already exist"
                    )
                    continue

        kept.append(drug)

    return kept


def _normalize_accused_ref(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None

    text = " ".join(str(value).strip().split())
    if not text:
        return None

    match = _ACCUSED_REF_PATTERN.search(text)
    if match:
        return f"A-{int(match.group(1))}"

    return text.upper()


def _extract_dedup_accused_ref(drug: DrugExtraction) -> Optional[str]:
    meta = drug.extraction_metadata if isinstance(drug.extraction_metadata, dict) else {}

    accused_ref = _normalize_accused_ref(meta.get("accused_ref"))
    if accused_ref:
        return accused_ref

    source_sentence = str(meta.get("source_sentence") or "")
    matches = []
    for match in _ACCUSED_REF_PATTERN.finditer(source_sentence):
        normalized = f"A-{int(match.group(1))}"
        if normalized not in matches:
            matches.append(normalized)

    if len(matches) == 1:
        return matches[0]

    return None


_MONEY_HINT_RE = re.compile(r'(?i)(?:\b(?:rs\.?|rupees|₹)\b|for\s+rs\.?|purchased\s+for|bought\s+for|sold\s+for|price|cost|worth)')


def _looks_like_monetary_amount(item: dict) -> bool:
    if not isinstance(item, dict):
        return False

    raw_unit = str(item.get('raw_unit') or '').strip().lower()
    source_sentence = str((item.get('extraction_metadata') or {}).get('source_sentence') or '').strip().lower()

    if raw_unit in {'rs', 'rs.', 'rupees', 'rupee', 'inr', '₹'}:
        return True
    return bool(_MONEY_HINT_RE.search(source_sentence))


def _parse_monetary_amount(item: dict) -> Optional[float]:
    if not isinstance(item, dict):
        return None

    for field in ('purchase_price_per_unit', 'seizure_worth'):
        value = item.get(field)
        if isinstance(value, (int, float)) and float(value) > 0:
            return float(value)

    source_sentence = str((item.get('extraction_metadata') or {}).get('source_sentence') or '')
    match = re.search(r'(?:rs\.?|rupees|₹)\s*([0-9][0-9,]*(?:\.\d+)?)', source_sentence, flags=re.IGNORECASE)
    if match:
        return float(match.group(1).replace(',', ''))

    match = re.search(r'([0-9][0-9,]*(?:\.\d+)?)\s*(?:rs\.?|rupees|₹)', source_sentence, flags=re.IGNORECASE)
    if match:
        return float(match.group(1).replace(',', ''))

    return None


# =============================================================================
# Extraction Prompt
# =============================================================================
# FALLBACK verbose prompt kept for reference / rollback.
EXTRACTION_PROMPT_VERBOSE = """You are an expert forensic data analyst. Your task is to extract structured drug seizure information from police brief facts.
# =============================================================
# DO NOT USE IN PRODUCTION — KEPT FOR REFERENCE ONLY
# {drug_knowledge_base} placeholder is no longer populated.
# Using this prompt will raise KeyError and break extraction.
# Active prompt is: EXTRACTION_PROMPT (below)
# =============================================================
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
Drug Knowledge Base: {{drug_knowledge_base}}
Input: {text}
Return valid JSON matching: drugs:[{{raw_drug_name,raw_quantity,raw_unit,primary_drug_name,drug_form,seizure_worth,worth_scope,confidence_score,extraction_metadata:{{source_sentence}}}}]
"""

EXTRACTION_PROMPT = """You are an expert forensic data analyst extracting structured drug seizure data from police brief facts.

## CORE RULES (STRICT — read carefully)
0. **Zero Miss Policy:** You are acting as a 30-year NDPS officer. Every substance that could be charged under the NDPS Act MUST be extracted. A missed drug entry is a worse error than a false positive. When in doubt — extract it and set confidence_score accordingly.
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
R6:drug-name|set primary_drug_name to the most precise standard drug name you know from your training|use exact NDPS/pharmacological names (e.g. Alprazolam not "tablet", Tramadol not "painkiller", Ganja not "contraband")|capitalize properly|if truly unidentifiable→use capitalized raw text from FIR|know common aliases (extract if seen): Ganja=marijuana/weed/bhang/grass/pot/dope/maal/stuff; Heroin=smack/brown sugar/H/no.4/harry; Cocaine=coke/snow/charlie/white; Methamphetamine=meth/ice/crystal/shabu/yaba; MDMA=ecstasy/molly/mandy/crystal mdma/mdma crystal; Alprazolam=xanax/niravam/alprax; Tramadol=ultram/tramazac/contramal/tydol; Pentazocine=fortwin/sosegon/talwin; Mephedrone=meow meow/M-cat/drone/4-MMC; Buprenorphine=buprenex/subutex/temgesic; Ketamine=special K/vitamin K/ket; Codeine=lean/purple drank (syrup); Opium=afeem/amal/chandu/doda; Poppy=khus khus/post/bhukki/doda/poppy husk/poppy straw; Nitrazepam=nitravet/nitavan/alodorm; Zolpidem=ambien/stilnox; Charas=hash/hashish/cream/resin/black/slate
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
R22:brand-names|extract brands as generic names: Spasmo Proxyvon/Spasmo-Proxylon→Pentazocine; Rumorf/Rumrof/Rumorf-30/Rumrof CR→Morphine; Nitravet/Nitavan→Nitrazepam; Tydol/Tramazac/Contramal→Tramadol; Fortwin/Sosegon→Pentazocine; Corex/Phensedyl→Codeine Syrup; any "SP capsule"/"SP tablet"→Pentazocine|raw_drug_name=brand from FIR, primary_drug_name=generic name
R23:cannabis-edibles|chocolates/cookies/brownies/laddoos or any food described as cannabis/ganja/bhang-infused are valid NDPS seizures|extract with an appropriate edible form name (e.g. "Ganja Chocolates")|drug_form=count if pieces, solid if bulk weight|NEVER skip edibles
R24:precursors|acetic anhydride, ephedrine, pseudoephedrine, phenylacetic acid, and other NDPS Table I/II precursors are extractable|primary_drug_name must be the chemical name exactly|do NOT confuse with cutting agents (sugar, starch)
R25:source-sentence|extraction_metadata.source_sentence MUST be the verbatim sentence/clause that contains the drug mention|never summarize|if quantity spans two sentences, include both verbatim
R26:supplier-name|if text says accused "purchased from X" or "brought from X"→set supplier_name=X|extract only the SUPPLIER (person they bought from), NOT the accused themselves|if no supplier mentioned→null
R26A:RULE 15 CLARIFICATION|supplier is CONTEXT ONLY, never assign drugs to supplier as accused|Example: "A1 confessed he purchased ganja from Raju for Rs.5000"→assign drug to A1 (who possessed it), set supplier_name=Raju (context)|Raju is NOT in crime facts unless arrested; only use Raju's name if Raju is explicitly charged in the FIR|if supplier not in accused list, assign drug to the actual possessor (e.g. A1) not supplier
R27:source-location|if text explicitly names a city/state/country as origin of drugs (e.g. "brought from Goa"→"Goa", "from Delhi supplier"→"Delhi")→set source_location|only when PLACE is explicitly named|if not present→null
R27A:RULE 16 CLARIFICATION|location context used only if explicitly present in brief facts|Example: "contraband recovered from A1's car parked at Market Street"→location="Market Street" (explicit)|Example: "A1 apprehended in Hyderabad"→location could be Hyderabad (explicitly named)|NEVER infer location from non-explicit clues|location is for context and consolidation, never for accusation inference
R28:destination|if text states intended delivery city/location (e.g. "to be sold in Hyderabad")→set destination|only when EXPLICIT→null if not mentioned
R29:purchase-price|if text states "purchased for Rs.X per packet/kg" or "bought at Rs.X"→set purchase_price_per_unit=X (float rupees per unit)|this is NOT seizure_worth|if not mentioned→null

## Drug Name Instruction
Use your training knowledge to identify the drug. Set primary_drug_name to the correct pharmacological or NDPS standard name (e.g. "Ganja", "Heroin", "Alprazolam", "Tramadol", "Charas", "Methamphetamine", "Cocaine", "Opium"). Do NOT write vague terms like "tablet", "powder", "contraband", "narcotic" as the primary_drug_name — use the actual drug name. If the drug is genuinely unidentifiable, use the capitalized raw text from the FIR. Post-processing will standardise names against the verified drug database — your job is to extract every drug present.

## Output Schema
{{{{ "drugs": [ {{{{ "raw_drug_name":str, "raw_quantity":float, "raw_unit":str, "primary_drug_name":str, "drug_form":"solid|liquid|count", "seizure_worth":float, "worth_scope":"individual|drug_total|overall_total", "is_commercial":bool, "confidence_score":int, "supplier_name":str, "source_location":str, "destination":str, "purchase_price_per_unit":float, "extraction_metadata":{{{{ "source_sentence":str }}}} }}}} ] }}}}

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


# Prompt override: keep the legacy prompt above for reference, but use this
# shorter production prompt to preserve per-accused seizures and accused refs.
EXTRACTION_PROMPT = """You are an expert forensic analyst extracting NDPS drug seizure rows from police brief facts.

Return VALID JSON ONLY with this exact shape:
{"drugs":[{"raw_drug_name":str,"raw_quantity":float,"raw_unit":str,"primary_drug_name":str,"drug_form":"solid|liquid|count","seizure_worth":float,"worth_scope":"individual|drug_total|overall_total","is_commercial":bool,"confidence_score":int,"supplier_name":str|null,"source_location":str|null,"destination":str|null,"purchase_price_per_unit":float|null,"extraction_metadata":{"source_sentence":str,"accused_ref":str|null}}]}

Rules:
0. **SEIZURE-ONLY RULE (CRITICAL)**: Extract ONLY drugs physically seized at the crime spot during arrest/apprehension.
   DO NOT extract: sold quantities (e.g., "sold 1kg to A-3"), consumed quantities (e.g., "tested positive"),
   or historical purchases (e.g., "purchased 6kg before arrest"). Only extract what was seized/confiscated/recovered
   from accused's possession at the time of arrest.

1. One row per actual seizure incident. Different accused with the same drug are separate rows.
2. If per-person quantities are stated, create one row per person. Example: A1 has 800g Ganja and A2 has 50g Ganja -> 2 rows.
3. If multiple people share one common total with NO per-person split, create exactly 1 collective row for that total.
4. Per-accused rows are NOT duplicates. Example: 6 accused each having 50g Ganja from their own possession -> 6 rows.
5. Skip customers or buyers mentioned only in confession history when nothing is seized from them.
6. Critical edge case: if a person is called a buyer/customer but is later apprehended and contraband is seized from that person's possession, that person IS a valid seizure row and must be extracted.
6b. Joint possession: If A1 and CCL jointly purchased/transported drugs and they were seized as a group, create ONE row with accused_ref=null (collective seizure). DO NOT create separate rows for each person or duplicate the same quantity for multiple accused. Do NOT include the downstream buyer/seller in the seizure row (they are NOT part of the seizure event).
7. Extract only the quantity physically seized at arrest. Skip historical purchase quantities, already-sold quantities, samples S1/S2, and remaining property breakdowns like P1 when they are subsets of the seized total.
8. When a row belongs to one accused, extraction_metadata.accused_ref MUST contain the accused code from the roster (A1, A2, etc.). If no code exists, use the exact accused name. For collective unattributed totals, set accused_ref to null.
9. extraction_metadata.source_sentence must be ONLY the verbatim clause describing the SEIZURE event (who had/possessed the drug at arrest). EXCLUDE downstream transactions like "and sold to A-3" or "buyer was X" which are not part of the seizure. This prevents misattributing drugs to downstream sellers/buyers.
10. Extract seizure_worth from worth phrases such as "worth Rs.", "W/Rs:", "market value", or "valued at". If one worth covers all rows of the same drug, use worth_scope="drug_total". If one worth covers all drugs, use worth_scope="overall_total". If no worth is stated, set seizure_worth=0 and worth_scope="individual".
11. Never extract vehicles, phones, SIM cards, cash, alcohol, empty covers, weighing scales, or other non-drug property as drug rows.
12. Use the actual NDPS drug name for primary_drug_name whenever identifiable.
13. CRITICAL SEIZURE-VERIFICATION FILTER:
    - **Consumption-only (NO seizure)**: If text mentions drug consumption/detection (tested positive, drug test,
      positive test, urine test, detected in test, found positive, smoked, consumed, ingestion) BUT does NOT contain
      seizure indicators (seized, confiscated, recovered, found with, apprehended with, caught with, arrested with,
      possessed of, in possession of) → return EMPTY drugs array: {"drugs":[]}
    - **Sold-only (NO seizure)**: If text mentions sold/transaction (sold to, sold by, buyer, customers, purchased by,
      transaction, dealt to, distributed to) BUT does NOT contain seizure indicators → return EMPTY drugs array
    - Examples to SKIP (return empty):
      - "Accused tested positive for ganja in urine test. No drugs seized."
      - "A-1 sold 1kg to A-3. No seizure at arrest location."
      - "All accused purchased drugs 2 weeks before arrest. Nothing seized during apprehension."
    - Examples to EXTRACT (seizure mentioned):
      - "Tested positive AND 2kg ganja was seized."
      - "Apprehended with 1kg Ganja, seized at arrest."
      - "A-1 caught with 50g, A-2 apprehended with 30g."

Input text:
{text}
"""

# =============================================================================
# Post-processing Step 1 (NEW): Resolve primary_drug_name via KB lookup
# =============================================================================
def resolve_primary_drug_name(
    drugs: List[DrugExtraction],
    kb_lookup: Dict[str, str],
    conn=None,
) -> List[DrugExtraction]:
    """
    Three-tier KB name resolution — runs AFTER LLM extraction.

    The LLM now extracts freely (no KB in prompt) using its own training
    knowledge. This step standardises primary_drug_name against drug_categories
    so downstream analytics use consistent canonical names.

    Matching tiers (applied in order, first match wins):

    Tier 1 — Exact lowercase match against kb_lookup dict (in-memory, O(1))
        raw_drug_name.lower() == any kb_raw_name
        e.g. "dry ganja" → "Ganja"

    Tier 2 — Substring match against kb_lookup (in-memory, O(n))
        any kb_raw_name found inside raw_drug_name.lower(), or vice versa
        e.g. "60 Grams floating and flowering dry Ganja" contains "dry ganja"
        e.g. "nitravet" is contained in "nitravet 10 mg tablets"

    Tier 3 — pg_trgm fuzzy match via DB (only when conn provided, only on miss)
        Calls fuzzy_match_drug_name(conn, raw_drug_name, threshold=0.35)
        Uses the GIN index on drug_categories(raw_name) — fast single lookup
        Catches misspellings and transliterations the LLM got right but KB
        doesn't have as exact text: "ganza"→"Ganja", "heroien"→"Heroin",
        "kokain"→"Cocaine", "smak"→"Heroin", "alprazolam tab"→"Alprazolam"

    No match on any tier → keep LLM's primary_drug_name unchanged.
    This is the correct behaviour: the LLM may have identified a valid drug
    (e.g. a new synthetic) that isn't in the KB yet.

    Args:
        drugs:      List of DrugExtraction objects post-LLM.
        kb_lookup:  {raw_name_lower: standard_name} dict — read-only, thread-safe.
        conn:       Optional DB connection for Tier 3 pg_trgm fallback.
                    Pass None to skip Tier 3 (name stays as LLM output).

    Thread-safety: kb_lookup is read-only. conn is per-thread (from thread-local
    pool or passed explicitly) — do not share across threads.
    """
    if not kb_lookup and conn is None:
        return drugs

    # Import here to avoid circular import — db.py imports from extractor indirectly
    fuzzy_fn = None
    if conn is not None:
        try:
            from db import fuzzy_match_drug_name
            fuzzy_fn = fuzzy_match_drug_name
        except ImportError:
            logger.debug("fuzzy_match_drug_name not importable — Tier 3 disabled")

    for drug in drugs:
        raw = (drug.raw_drug_name or '').lower().strip()
        if not raw or raw == 'unknown':
            continue

        resolved = None
        tier_used = None

        # ── Tier 1: Exact match ──
        if kb_lookup and raw in kb_lookup:
            resolved = kb_lookup[raw]
            tier_used = "exact"

        # ── Tier 2: Substring match ──
        if not resolved and kb_lookup:
            # KB key inside raw name (e.g. "dry ganja" in "floating dry ganja 60g")
            for kb_raw, kb_std in kb_lookup.items():
                if kb_raw in raw:
                    resolved = kb_std
                    tier_used = "substring(kb-in-raw)"
                    break
            # Raw name inside KB key (e.g. "nitravet" in "nitravet 10 mg tablets")
            if not resolved and len(raw) >= 4:
                for kb_raw, kb_std in kb_lookup.items():
                    if raw in kb_raw:
                        resolved = kb_std
                        tier_used = "substring(raw-in-kb)"
                        break

        # ── Tier 3: pg_trgm fuzzy match ──
        if not resolved and fuzzy_fn is not None:
            fuzzy_result = fuzzy_fn(conn, drug.raw_drug_name)
            if fuzzy_result:
                resolved = fuzzy_result
                tier_used = "pgtrgm"

        if resolved and resolved != drug.primary_drug_name:
            logger.debug(
                f"KB resolve [{tier_used}]: '{drug.raw_drug_name}' → '{resolved}' "
                f"(was '{drug.primary_drug_name}')"
            )
            drug.primary_drug_name = resolved
        elif not resolved:
            # No KB match — LLM's primary_drug_name is kept as-is.
            # This is correct: the LLM may know drugs not yet in the KB.
            logger.debug(
                f"KB resolve [no match]: '{drug.raw_drug_name}' "
                f"kept as '{drug.primary_drug_name}' (LLM knowledge)"
            )

    return drugs


# =============================================================================
# Post-processing Step 1b (NEW): Filter consumption-only entries (no seizure)
# =============================================================================
def filter_consumption_only_drugs(
    drugs: List[DrugExtraction],
    text: str,
) -> List[DrugExtraction]:
    """
    Filter out drug entries where the source_sentence indicates CONSUMPTION or SOLD
    (tested positive, drug test, urine test, sold to buyer) BUT NO SEIZURE occurred.

    RULE: Extract ONLY physically seized drugs at crime spot. Filter out:
    - Consumption-only (tested positive, smoked, ingested) without seizure
    - Sold/transaction drugs (sold to, buyer, transaction) without seizure mention
    - Historical purchases without seizure mention

    This is a safety net to prevent false extraction of drug references that
    are not actual seizures. The LLM should follow Rule 7 in EXTRACTION_PROMPT,
    but this post-filter catches any misses.

    Seizure-based examples (KEEP):
    - "seized 2kg ganja" → KEEP (explicit seizure)
    - "tested positive AND 2kg ganja was seized" → KEEP (seizure mentioned)
    - "A-1 apprehended with 50g" → KEEP (possession at arrest)
    - "A-3 caught with drug contraband" → KEEP (caught with implies seizure)

    Non-seizure examples (FILTER):
    - "tested positive for ganja" (no seizure) → SKIP
    - "sold 1kg to buyer" (no seizure) → SKIP
    - "purchased 6kg before arrest" (no seizure) → SKIP
    - "buyer was A-3" (downstream transaction only) → SKIP

    Args:
        drugs:  List of DrugExtraction objects from LLM.
        text:   Original brief_facts text (for context).

    Returns:
        Filtered list with non-seizure entries removed.
    """
    # Consumption/test markers (no seizure = consumption-only)
    consumption_markers = {
        'tested positive', 'positive for', 'urine test', 'drug test',
        'detected in test', 'found positive', 'positive in test',
        'smoked', 'consumed', 'ingested', 'consumption'
    }

    # Sold/transaction markers (no seizure = sold, not seized)
    sold_markers = {
        'sold to', 'sold by', 'sold for', 'selling to',
        'buyer', 'customers', 'purchased by', 'transaction',
        'transacted', 'dealt to', 'given to', 'distributed to'
    }

    # Seizure indicators (presence = this IS a seizure)
    seizure_markers = {
        'seized', 'confiscated', 'recovered', 'found with',
        'apprehended with', 'caught with', 'arrested with',
        'in possession of', 'possessed of', 'possession',
        'contraband', 'apprehended', 'confiscated'
    }

    kept = []
    text_lower = text.lower()

    for drug in drugs:
        metadata = (drug.extraction_metadata or {}) if hasattr(drug, 'extraction_metadata') else {}
        source_sentence = (metadata.get('source_sentence') or '').lower() if isinstance(metadata, dict) else ''

        # Check markers in source_sentence
        has_consumption_marker = any(marker in source_sentence for marker in consumption_markers)
        has_sold_marker = any(marker in source_sentence for marker in sold_markers)
        has_seizure_marker = any(marker in source_sentence for marker in seizure_markers)

        # Additional context check: look in full text for seizure indicators
        # This handles cases where source_sentence is incomplete
        has_seizure_in_text = any(marker in text_lower for marker in seizure_markers)

        # FILTER LOGIC:
        # Only KEEP if it has a seizure marker (in source OR in broader text)
        # SKIP if:
        #  1. Has consumption marker AND no seizure marker (consumption-only)
        #  2. Has sold marker AND no seizure marker (sold, not seized)
        #  3. No seizure marker anywhere (no seizure evidence)

        is_consumption_only = has_consumption_marker and not has_seizure_marker and not has_seizure_in_text
        is_sold_only = has_sold_marker and not has_seizure_marker and not has_seizure_in_text
        is_no_seizure = not has_seizure_marker and not has_seizure_in_text

        if is_consumption_only:
            drug_name = getattr(drug, 'primary_drug_name', '?')
            logger.info(
                f"[SeizureFilter] Filtered '{drug_name}': "
                f"consumption-only (no seizure mentioned). "
                f"source_sentence='{source_sentence}'"
            )
            continue

        if is_sold_only:
            drug_name = getattr(drug, 'primary_drug_name', '?')
            logger.info(
                f"[SeizureFilter] Filtered '{drug_name}': "
                f"sold/transaction (no seizure at crime spot). "
                f"source_sentence='{source_sentence}'"
            )
            continue

        if is_no_seizure and (source_sentence or '').strip():
            # Has source_sentence but no seizure markers — likely not a seizure
            drug_name = getattr(drug, 'primary_drug_name', '?')
            logger.info(
                f"[SeizureFilter] Filtered '{drug_name}': "
                f"no seizure markers found. "
                f"source_sentence='{source_sentence}'"
            )
            continue

        # If we get here, entry has seizure markers → KEEP it
        kept.append(drug)

    filtered_count = len(drugs) - len(kept)
    if filtered_count > 0:
        logger.info(
            f"[SeizureFilter] Removed {filtered_count} non-seizure drug entries "
            f"(consumption-only, sold, or no seizure evidence). "
            f"Kept {len(kept)} crime-spot seizure entries."
        )

    return kept


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
                # FIR unit sanity: Some FIRs write "Wg. 24.925 grms" when the value is
                # effectively kilograms (worth and context imply kg, not grams).
                #
                # NDPS officer policy: prefer avoiding false negatives. If a grams unit
                # would imply an absurd Rs/gram (e.g. > 1000 Rs/g) AND the sentence is a
                # weight ("Wg.") style seizure line, treat qty as kilograms.
                source = (drug.extraction_metadata or {}).get("source_sentence", "") if isinstance(drug.extraction_metadata, dict) else ""
                source_l = str(source).lower()
                worth = float(drug.seizure_worth or 0.0)
                rs_per_gram = (worth / qty) if qty > 0 else 0.0
                if qty > 0 and worth > 0 and rs_per_gram > 1000 and ('wg' in source_l or 'w/g' in source_l):
                    logger.warning(
                        f"[UnitSanity] Interpreting '{drug.raw_drug_name}' qty={qty} as KG (not grams) "
                        f"based on worth Rs.{worth:.0f} and source_sentence='{source}'."
                    )
                    drug.raw_unit = "KGs"
                    drug.weight_g = qty * 1000.0
                    drug.weight_kg = qty
                else:
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
        # Backward-compat normalization: legacy DB rows used 'MDM' for MDMA.
        # Canonical NDPS name is 'MDMA' (standard_name). Keep dict key as 'mdma'.
        key = 'mdma' if key == 'mdm' else key
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
    Remove duplicate drug extractions and consolidate multi-unit seizures into single entries.

    Deduplication strategy:
    1. PRIMARY DEDUP: By (primary_drug_name, raw_drug_name, supplier, location) — ignores quantity/unit AND accused.
       Rationale: Same drug with different unit representations (32 tablets vs 19.648g)
       are the SAME seizure, just measured differently. Multiple mentions of the same drug
       across different accused (e.g., "A-1 has 6 Kg Ganja, A-3 sold 1 Kg, A-4 is supplier")
       are quantity EVENTS for the same drug, not separate drugs.
       Accused assignment happens later in write_drugs_by_accused_in_memory().

    2. CONSOLIDATION: For duplicates, merge measurements and keep highest confidence.
       - weight_g, weight_kg, volume_ml, volume_l, count_total are normalized forms.
       - Keep the entry with highest confidence_score.
       - Preserve all source data for audit trail (raw_quantity, raw_unit, extraction_metadata).
       - Collect all source sentences from each extraction (different mentions) for audit.

    3. EDGE CASES:
       - Different suppliers/locations for same drug → keep separate (different seizures).
       - Same drug, same quantities, different units (e.g., 32 tablets = 19.648g) → consolidate.
       - Same drug mentioned with different accused → consolidate here, assign later.

    Example:
       Input:  [{primary_drug_name: 'Ganja', raw_quantity: 6, raw_unit: 'kg', source: "A-1 has 6 Kg"},
                {primary_drug_name: 'Ganja', raw_quantity: 1, raw_unit: 'kg', source: "A-3 sold 1 Kg"},
                {primary_drug_name: 'Ganja', raw_quantity: 5, raw_unit: 'kg', source: "5 Kg remaining"}]
       Output: [{primary_drug_name: 'Ganja', raw_quantity: 6 (from highest confidence), all_sources: [...]
                 consolidated_sources track all 3 mentions}]
    """
    if not drugs:
        return drugs

    seen = {}
    for drug in drugs:
        meta = drug.extraction_metadata if isinstance(drug.extraction_metadata, dict) else {}
        # Dedup key: (drug_name, raw_name, supplier, location) — NO accused_ref.
        # Accused assignment happens in write_drugs_by_accused_in_memory() AFTER dedup,
        # so that the same drug consolidated here can be assigned to the right accused(s)
        # based on source_sentence analysis.
        key = (
            (drug.primary_drug_name or '').lower().strip(),
            (drug.raw_drug_name or '').lower().strip(),
            (drug.supplier_name or '').lower().strip(),        # Different supplier = different seizure
            (drug.source_location or '').lower().strip(),      # Different location = different seizure
        )

        existing = seen.get(key)
        if not existing:
            # Initialize consolidated_sources list for audit trail
            if isinstance(drug.extraction_metadata, dict):
                source_sentence = drug.extraction_metadata.get('source_sentence', '')
                if source_sentence:
                    drug.extraction_metadata['consolidated_sources'] = [source_sentence]
            seen[key] = drug
        else:
            # Consolidation logic: keep higher confidence, merge measurements and source sentences
            if (drug.confidence_score or 0) > (existing.confidence_score or 0):
                # Preserve measurement data from new entry, but keep raw fields from existing if missing
                if not drug.raw_quantity or drug.raw_quantity == 0:
                    drug.raw_quantity = existing.raw_quantity
                    drug.raw_unit = existing.raw_unit
                # Merge standardized measurements (weight_g, weight_kg, volume_ml, volume_l, count_total)
                # Prefer non-null values from either entry
                if drug.weight_g is None:
                    drug.weight_g = existing.weight_g
                if drug.weight_kg is None:
                    drug.weight_kg = existing.weight_kg
                if drug.volume_ml is None:
                    drug.volume_ml = existing.volume_ml
                if drug.volume_l is None:
                    drug.volume_l = existing.volume_l
                if drug.count_total is None:
                    drug.count_total = existing.count_total
                # Merge metadata: collect all source sentences for audit trail
                if isinstance(drug.extraction_metadata, dict) and isinstance(existing.extraction_metadata, dict):
                    existing_sources = (existing.extraction_metadata or {}).get('consolidated_sources', [])
                    drug_source = (drug.extraction_metadata or {}).get('source_sentence', '')
                    if drug_source:
                        if not existing_sources:
                            existing_source = (existing.extraction_metadata or {}).get('source_sentence', '')
                            if existing_source:
                                existing_sources = [existing_source]
                        if drug_source not in existing_sources:
                            existing_sources.append(drug_source)
                        drug.extraction_metadata['consolidated_sources'] = existing_sources
                seen[key] = drug
            else:
                # Existing entry has higher/equal confidence, merge new measurements into it
                if drug.raw_quantity and drug.raw_quantity > 0 and (not existing.raw_quantity or existing.raw_quantity == 0):
                    existing.raw_quantity = drug.raw_quantity
                    existing.raw_unit = drug.raw_unit
                # Merge standardized measurements
                if drug.weight_g is not None and (existing.weight_g is None or existing.weight_g == 0):
                    existing.weight_g = drug.weight_g
                if drug.weight_kg is not None and (existing.weight_kg is None or existing.weight_kg == 0):
                    existing.weight_kg = drug.weight_kg
                if drug.volume_ml is not None and (existing.volume_ml is None or existing.volume_ml == 0):
                    existing.volume_ml = drug.volume_ml
                if drug.volume_l is not None and (existing.volume_l is None or existing.volume_l == 0):
                    existing.volume_l = drug.volume_l
                if drug.count_total is not None and (existing.count_total is None or existing.count_total == 0):
                    existing.count_total = drug.count_total
                # Merge metadata: collect all source sentences for audit trail
                if isinstance(drug.extraction_metadata, dict) and isinstance(existing.extraction_metadata, dict):
                    existing_sources = (existing.extraction_metadata or {}).get('consolidated_sources', [])
                    drug_source = (drug.extraction_metadata or {}).get('source_sentence', '')
                    if drug_source:
                        if not existing_sources:
                            existing_source = (existing.extraction_metadata or {}).get('source_sentence', '')
                            if existing_source:
                                existing_sources = [existing_source]
                        if drug_source not in existing_sources:
                            existing_sources.append(drug_source)
                        existing.extraction_metadata['consolidated_sources'] = existing_sources

    deduped = list(seen.values())

    if len(drugs) > len(deduped):
        logger.info(f"Deduplicated extractions: {len(drugs)} -> {len(deduped)} (consolidated multi-unit seizures)")

    if len(deduped) > max_per_crime:
        logger.warning(f"Capping extractions from {len(deduped)} to {max_per_crime}")
        deduped = sorted(deduped, key=lambda d: d.confidence_score or 0, reverse=True)[:max_per_crime]

    return deduped


# =============================================================================
# Sample/Aliquot Detection (Rule 13)
# =============================================================================
def _mark_sample_entries(drugs: List[DrugExtraction]) -> List[DrugExtraction]:
    """
    RULE 13: Samples vs Bulk Seizure.

    Detect if a drug entry is a sample/aliquot (small quantity drawn for testing)
    vs bulk seizure. Samples should be marked with metadata to prevent
    double-counting with parent seizure.

    Detection keywords:
      "sample", "aliquot", "drawn for testing", "for FSL analysis",
      "portion", "subsample", "test portion", "sub-sample"

    Marked entries are tracked but not merged (they are part of the bulk quantity).

    Args:
        drugs: List of DrugExtraction objects

    Returns:
        Same list with sample metadata added where applicable
    """
    if not drugs:
        return drugs

    sample_keywords = {
        'sample', 'aliquot', 'drawn for', 'drawn to', 'for testing',
        'for fsl', 'for analysis', 'portion', 'sub portion', 'subsample',
        'test portion', 'sub-sample', 'subsampl', 'for examination',
        'for chemical examination', 'for laboratory'
    }

    for drug in drugs:
        # Get source sentence from extraction metadata
        meta = drug.extraction_metadata or {}
        source_sentence = str(meta.get('source_sentence', '')).lower()

        # Check if any sample keyword appears in source sentence
        is_sample = any(keyword in source_sentence for keyword in sample_keywords)

        if is_sample:
            # Mark as sample with audit trail
            if not isinstance(drug.extraction_metadata, dict):
                drug.extraction_metadata = {}

            drug.extraction_metadata['is_sample_or_aliquot'] = True
            drug.extraction_metadata['sample_reason'] = 'Detected keywords in source_sentence'

            # Log for audit trail
            logger.info(
                f"[RuleCheck] SAMPLE DETECTED: {drug.primary_drug_name} "
                f"({drug.raw_quantity} {drug.raw_unit}) - marked as sample/aliquot"
            )

    return drugs


# =============================================================================
# Main extraction entry point
# =============================================================================
def extract_drug_info(
    text: str,
    drug_categories: List[dict] = None,
    ignore_set: Set[str] = None,
    kb_lookup: Dict[str, str] = None,
    dynamic_drug_keywords: Set[str] = None,
    conn=None,
) -> List[DrugExtraction]:
    """
    Extracts a list of drug information objects from the given text.

    Full pipeline (in order):
      Step 0  — Preprocessor: split multi-FIR text, keep only drug-relevant
                sections using dynamic_drug_keywords (KB-driven, not static).
      Step 1  — Token budget check.
      Step 2  — LLM extraction via EXTRACTION_PROMPT (with R20/R21 added).
      Step 3  — Input sanitization (None guards, worth_scope validation).
      Step 4  — resolve_primary_drug_name(): 3-tier KB standardisation:
                Tier1 exact match, Tier2 substring, Tier3 pg_trgm fuzzy (DB).
      Step 5  — filter_non_drug_entries(): drop vehicles, alcohol, paraphernalia
                etc. using DB ignore_set + hardcoded safety net.
      Step 6  — standardize_units(): unit → weight_g/kg, volume_ml/l, count_total.
      Step 7  — _distribute_seizure_worth(): proportional worth distribution.
      Step 8  — _apply_commercial_quantity_check(): NDPS threshold → is_commercial.
      Step 9  — deduplicate_extractions(): dedup + cap at 100.

    Args:
        text:                   Raw brief_facts text for one crime.
        drug_categories:        List of KB dicts (raw_name, standard_name) — used only
                                to build kb_lookup and dynamic_drug_keywords at startup.
                                NOT injected into the LLM prompt.
        ignore_set:             Set of lowercased terms from drug_ignore_list DB table.
                                Exact-matched against primary_drug_name post-KB-resolve.
        kb_lookup:              Dict {raw_name_lower: standard_name} for deterministic
                                name resolution (Step 4, Tier 1+2).
        dynamic_drug_keywords:  KB-derived token set for preprocessor scoring (Step 0).
        conn:                   Optional DB connection for Step 4 Tier 3 pg_trgm fallback.
                                Pass per-thread connection — not shared across threads.

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
    # KB is no longer injected into the prompt — LLM extracts freely using its
    # own training knowledge. This frees ~3,700 tokens (the 330-row KB table),
    # giving 15,484 tokens for brief_facts text vs 11,763 previously.
    est_input_tokens    = _estimate_tokens(filtered_text)
    CONTEXT_WINDOW      = get_llm('extraction').context_window
    PROMPT_OVERHEAD     = 900   # rules (R1-R21) + examples + output schema
    available_for_input = CONTEXT_WINDOW - PROMPT_OVERHEAD
    if est_input_tokens > available_for_input:
        logger.warning(
            f"Token budget tight: input ~{est_input_tokens} tokens, "
            f"available ~{available_for_input} (window={CONTEXT_WINDOW}, "
            f"prompt={PROMPT_OVERHEAD}). Text may be truncated by LLM."
        )
    else:
        logger.info(f"Token budget OK: input ~{est_input_tokens}/{available_for_input} available tokens.")

    # ── Step 2: LLM call — no KB injected into prompt ──
    # The KB is NOT sent to the LLM. The LLM uses its own training knowledge
    # to identify drugs and set primary_drug_name. Post-processing
    # (resolve_primary_drug_name + fuzzy_match_drug_name) standardises names
    # against drug_categories using exact match, substring, and pg_trgm.
    parser = RobustJsonOutputParser(pydantic_object=CrimeReportExtraction)
    prompt  = ChatPromptTemplate.from_template(_safe_prompt_template(EXTRACTION_PROMPT))

    try:
        # LLM call (thread-safe per-thread instance)
        llm   = _get_thread_safe_llm()
        chain = prompt | llm | parser

        input_data = {"text": filtered_text}
        response   = invoke_extraction_with_retry(chain, input_data, max_retries=1)

        if not response:
            logger.warning("LLM returned empty response (all retries failed). Returning empty.")
            return []

        if isinstance(response, list):
            drugs_data = response
        elif isinstance(response, dict):
            drugs_data = response.get("drugs", [])
            if not drugs_data:
                logger.info(f"LLM returned 0 drugs from response keys: {list(response.keys())}")
                return []
        else:
            logger.warning(f"LLM returned unexpected type {type(response).__name__}. Returning empty.")
            return []

        if not drugs_data:
            logger.info("LLM returned 0 drugs.")
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

                # Monetary values are transaction metadata, not seizure quantities.
                if _looks_like_monetary_amount(d):
                    amount = _parse_monetary_amount(d)
                    if amount is not None:
                        d['purchase_price_per_unit'] = amount if d.get('purchase_price_per_unit') is None else d.get('purchase_price_per_unit')
                    d['raw_quantity'] = 0.0
                    if not d.get('raw_unit') or str(d.get('raw_unit')).strip().lower() in {'rs', 'rs.', 'rupees', 'rupee', 'inr', '₹'}:
                        d['raw_unit'] = 'Unknown'
                    # Monetary-only rows are not seizure-worth rows.
                    d['seizure_worth'] = 0.0
                    d['worth_scope'] = 'individual'

                valid_scopes = {'individual', 'drug_total', 'overall_total'}
                ws = str(d.get('worth_scope', 'individual')).lower().strip()
                d['worth_scope'] = ws if ws in valid_scopes else 'individual'

                valid_drugs.append(DrugExtraction(**d))
            except Exception as e:
                logger.warning(f"Skipping invalid drug entry: {e} | data: {d}")

        # Expand explicit accused-linked clauses first so per-accused quantities
        # remain separate even when the LLM collapses them into a total row.
        segmented_packet_rows = _extract_segmented_accused_rows(filtered_text, kb_lookup)
        if segmented_packet_rows:
            logger.info(f"Segmented accused expansion found {len(segmented_packet_rows)} explicit rows.")
            for packet_row in segmented_packet_rows:
                try:
                    valid_drugs.append(DrugExtraction(**packet_row))
                except Exception as e:
                    logger.warning(f"Skipping invalid segmented accused row: {e} | data: {packet_row}")

        # Expand numbered packet lists found in the source text so packet-level
        # quantities survive even when the LLM collapses them into a total row.
        explicit_packet_rows = _extract_explicit_packet_rows(filtered_text, kb_lookup)
        if explicit_packet_rows:
            logger.info(f"Packet expansion found {len(explicit_packet_rows)} explicit packet rows.")
            for packet_row in explicit_packet_rows:
                try:
                    valid_drugs.append(DrugExtraction(**packet_row))
                except Exception as e:
                    logger.warning(f"Skipping invalid explicit packet row: {e} | data: {packet_row}")

            valid_drugs = _drop_redundant_total_rows(valid_drugs, explicit_packet_rows)

        # ── Step 4: Deterministic KB name resolution ──
        kb_resolved = resolve_primary_drug_name(valid_drugs, kb_lookup, conn=conn)

        # ── Step 4b: Filter consumption-only entries (no seizure) ──
        consumption_filtered = filter_consumption_only_drugs(kb_resolved, text)

        # ── Step 5: Drop non-drug entries (ignore list + safety net) ──
        filtered = filter_non_drug_entries(consumption_filtered, ignore_set)

        # ── Steps 6-9: Unit standardization → Worth distribution → Commercial check → Sample detection → Dedup ──
        standardized       = standardize_units(filtered)
        worth_distributed  = _distribute_seizure_worth(standardized)
        commercial_checked = _apply_commercial_quantity_check(worth_distributed)
        sample_marked      = _mark_sample_entries(commercial_checked)
        return deduplicate_extractions(sample_marked)

    except Exception as e:
        logger.error(f"Drug extraction failed: {e}", exc_info=True)
        return []


if __name__ == "__main__":
    test_text = "A1 had 1 packet containing 7 grams Ganja."
    print("Testing extraction...")
    extractions = extract_drug_info(test_text)
    for d in extractions:
        print(d.model_dump_json(indent=2))
