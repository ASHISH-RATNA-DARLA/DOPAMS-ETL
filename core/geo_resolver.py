"""
Hybrid geographic resolver — embedding cascade with LLM fallback.

Cascade:
  Pass 1 (caller): pg_trgm fuzzy match (existing in update-state-country)
  Pass 2 (this module): sentence-transformer embedding cosine vs KB
  Pass 3 (this module): LLM disambiguation if top-2 embeddings within margin

Used by:
  - update-state-country/update-state-country.py (Phase 3 fallback)
"""

import os
import logging
import threading
import json
from functools import lru_cache
from typing import Optional, Tuple, List, Dict

logger = logging.getLogger(__name__)

INDIAN_STATES = [
    "Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar", "Chhattisgarh",
    "Goa", "Gujarat", "Haryana", "Himachal Pradesh", "Jharkhand", "Karnataka",
    "Kerala", "Madhya Pradesh", "Maharashtra", "Manipur", "Meghalaya", "Mizoram",
    "Nagaland", "Odisha", "Punjab", "Rajasthan", "Sikkim", "Tamil Nadu",
    "Telangana", "Tripura", "Uttar Pradesh", "Uttarakhand", "West Bengal",
    "Andaman and Nicobar Islands", "Chandigarh",
    "Dadra and Nagar Haveli and Daman and Diu", "Delhi",
    "Jammu and Kashmir", "Ladakh", "Lakshadweep", "Puducherry",
]

# Common aliases that pg_trgm misses
STATE_ALIASES = {
    "bombay": "Maharashtra",
    "madras": "Tamil Nadu",
    "calcutta": "West Bengal",
    "bangalore": "Karnataka",
    "mysore": "Karnataka",
    "hyderabad": "Telangana",
    "secunderabad": "Telangana",
    "ap": "Andhra Pradesh",
    "tn": "Tamil Nadu",
    "ts": "Telangana",
    "mp": "Madhya Pradesh",
    "up": "Uttar Pradesh",
    "wb": "West Bengal",
    "j&k": "Jammu and Kashmir",
    "jk": "Jammu and Kashmir",
    "ncr": "Delhi",
    "ncr delhi": "Delhi",
    "new delhi": "Delhi",
    "pondicherry": "Puducherry",
    "orissa": "Odisha",
    "uttaranchal": "Uttarakhand",
}

EMBED_MATCH_THRESHOLD = float(os.getenv("GEO_EMBED_MATCH_THRESHOLD", "0.85"))
EMBED_AMBIG_MARGIN    = float(os.getenv("GEO_EMBED_AMBIG_MARGIN", "0.05"))
EMBED_MIN_THRESHOLD   = float(os.getenv("GEO_EMBED_MIN_THRESHOLD", "0.55"))
ENABLED               = os.getenv("ENABLE_EMBEDDINGS", "false").lower() == "true"

_state_vectors = None
_country_vectors = None
_country_names: List[str] = []
_model = None
_init_lock = threading.Lock()


def _get_model():
    """Lazy-load sentence-transformers model. Thread-safe singleton."""
    global _model
    if _model is not None:
        return _model
    with _init_lock:
        if _model is not None:
            return _model
        from sentence_transformers import SentenceTransformer
        model_name = os.getenv("EMBEDDING_MODEL", "all-MiniLM-L6-v2")
        logger.info(f"geo_resolver: loading {model_name}")
        _model = SentenceTransformer(model_name)
        logger.info("geo_resolver: model loaded")
    return _model


def _ensure_state_vectors():
    global _state_vectors
    if _state_vectors is not None:
        return
    with _init_lock:
        if _state_vectors is not None:
            return
        model = _get_model()
        _state_vectors = model.encode(INDIAN_STATES, normalize_embeddings=True)
        logger.info(f"geo_resolver: encoded {len(INDIAN_STATES)} state vectors")


def _ensure_country_vectors(conn):
    """Load country names from geo_countries table once and embed them."""
    global _country_vectors, _country_names
    if _country_vectors is not None:
        return
    with _init_lock:
        if _country_vectors is not None:
            return
        names = []
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT DISTINCT country_name FROM geo_countries WHERE country_name IS NOT NULL")
                names = [r[0] for r in cur.fetchall() if r[0]]
        except Exception as e:
            logger.warning(f"geo_resolver: could not load geo_countries: {e}")
            return
        if not names:
            logger.warning("geo_resolver: geo_countries returned no rows")
            return
        model = _get_model()
        _country_vectors = model.encode(names, normalize_embeddings=True)
        _country_names = names
        logger.info(f"geo_resolver: encoded {len(names)} country vectors")


def _normalize(raw: str) -> str:
    return (raw or "").strip().lower()


def _alias_lookup(raw: str) -> Optional[str]:
    return STATE_ALIASES.get(_normalize(raw))


@lru_cache(maxsize=4096)
def _embed_query(raw_normalized: str):
    model = _get_model()
    return model.encode([raw_normalized], normalize_embeddings=True)[0]


def _cosine_topk(query_vec, kb_vectors, kb_names, k=3):
    import numpy as np
    scores = kb_vectors @ query_vec  # both already normalized = cosine
    idx = np.argsort(-scores)[:k]
    return [(kb_names[i], float(scores[i])) for i in idx]


def _llm_disambiguate(raw: str, candidates: List[Tuple[str, float]], task: str) -> Optional[str]:
    """Call LLM only when embeddings are ambiguous. Returns picked name or None."""
    try:
        import sys
        sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from core.llm_service import get_llm
    except Exception as e:
        logger.warning(f"geo_resolver: LLM import failed: {e}")
        return None

    candidate_list = [c[0] for c in candidates]
    prompt = (
        f"Pick the correct {task} for this raw input. "
        f"Output ONLY a single name from the candidates list, or NONE if no match.\n\n"
        f"Raw input: {raw}\n"
        f"Candidates: {candidate_list}\n\n"
        f"Output (single name only, no explanation):"
    )
    try:
        svc = get_llm("classification")
        response = svc.generate(prompt)
        if not response:
            return None
        response = response.strip().split("\n")[0].strip()
        if response.upper() == "NONE":
            return None
        # Validate response is in candidates (prevent hallucination)
        for c, _ in candidates:
            if response.lower() == c.lower():
                return c
        # Loose match — sometimes LLM adds quotes or punctuation
        cleaned = response.strip("\"'.,").lower()
        for c, _ in candidates:
            if cleaned == c.lower():
                return c
        logger.warning(f"geo_resolver: LLM returned non-candidate '{response}' for '{raw}'")
        return None
    except Exception as e:
        logger.warning(f"geo_resolver: LLM call failed: {e}")
        return None


def resolve_state(raw: str, conn=None) -> Tuple[Optional[str], float, str]:
    """
    Resolve raw text to canonical Indian state name.

    Returns: (state_name, confidence, source) where source ∈
        {'alias', 'embedding', 'llm', 'unresolved'}.
    Confidence is 1.0 for alias hits, cosine score for embedding,
    and embedding score for LLM (LLM choice is gated by candidates).
    """
    if not raw or not raw.strip():
        return (None, 0.0, "unresolved")

    norm = _normalize(raw)

    aliased = _alias_lookup(norm)
    if aliased:
        return (aliased, 1.0, "alias")

    if not ENABLED:
        return (None, 0.0, "disabled")

    _ensure_state_vectors()
    if _state_vectors is None:
        return (None, 0.0, "unresolved")

    query = _embed_query(norm)
    top = _cosine_topk(query, _state_vectors, INDIAN_STATES, k=3)
    if not top:
        return (None, 0.0, "unresolved")

    best_name, best_score = top[0]

    if best_score >= EMBED_MATCH_THRESHOLD:
        return (best_name, best_score, "embedding")

    if best_score < EMBED_MIN_THRESHOLD:
        return (None, best_score, "unresolved")

    # Ambiguous middle band — use LLM to pick from top-3
    second_score = top[1][1] if len(top) > 1 else 0.0
    if (best_score - second_score) > EMBED_AMBIG_MARGIN:
        return (best_name, best_score, "embedding")

    picked = _llm_disambiguate(raw, top, task="Indian state")
    if picked:
        return (picked, best_score, "llm")
    return (None, best_score, "unresolved")


def resolve_country(raw: str, conn) -> Tuple[Optional[str], float, str]:
    """
    Resolve raw text to canonical country from geo_countries.

    Requires conn to load country list on first call.
    """
    if not raw or not raw.strip():
        return (None, 0.0, "unresolved")
    if conn is None:
        return (None, 0.0, "unresolved")
    if not ENABLED:
        return (None, 0.0, "disabled")

    norm = _normalize(raw)
    _ensure_country_vectors(conn)
    if _country_vectors is None or not _country_names:
        return (None, 0.0, "unresolved")

    query = _embed_query(norm)
    top = _cosine_topk(query, _country_vectors, _country_names, k=3)
    if not top:
        return (None, 0.0, "unresolved")

    best_name, best_score = top[0]

    if best_score >= EMBED_MATCH_THRESHOLD:
        return (best_name, best_score, "embedding")
    if best_score < EMBED_MIN_THRESHOLD:
        return (None, best_score, "unresolved")

    second_score = top[1][1] if len(top) > 1 else 0.0
    if (best_score - second_score) > EMBED_AMBIG_MARGIN:
        return (best_name, best_score, "embedding")

    picked = _llm_disambiguate(raw, top, task="country")
    if picked:
        return (picked, best_score, "llm")
    return (None, best_score, "unresolved")
