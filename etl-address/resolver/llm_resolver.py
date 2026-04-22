from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from typing import Optional

import requests

from .kb_cache import GeoKB
from .types import AddressCandidate, ResolvedAddress

logger = logging.getLogger(__name__)


DEFAULT_MODEL = "qwen2.5:14b-instruct"
DEFAULT_FALLBACK = "llama3.1:8b"


def _env(key: str, default: str) -> str:
    v = os.environ.get(key)
    return v if v else default


def _ollama_host() -> str:
    return (
        os.environ.get("OLLAMA_HOST")
        or os.environ.get("OLLAMA_BASE_URL")
        or "http://localhost:11434"
    ).rstrip("/")


def _summary(value: Optional[str]) -> str:
    return value if value else "-"


class LLMCallBudget:
    def __init__(self, limit: int) -> None:
        self._limit = max(0, int(limit))
        self._calls = 0
        self._lock = threading.Lock()

    def take(self) -> bool:
        with self._lock:
            if self._calls >= self._limit:
                return False
            self._calls += 1
            return True

    @property
    def used(self) -> int:
        return self._calls

    @property
    def limit(self) -> int:
        return self._limit


class LLMAddressResolver:
    _instance: Optional["LLMAddressResolver"] = None
    _lock = threading.Lock()

    def __init__(self) -> None:
        self.model = _env("LLM_MODEL_ADDRESS", DEFAULT_MODEL)
        self.fallback = _env("LLM_MODEL_ADDRESS_FALLBACK", DEFAULT_FALLBACK)
        self.timeout = int(_env("ADDRESS_LLM_TIMEOUT", "20"))
        self.primary_timeout = int(_env("ADDRESS_LLM_TIMEOUT_PRIMARY", str(self.timeout)))
        self.fallback_timeout = int(_env("ADDRESS_LLM_TIMEOUT_FALLBACK", str(self.timeout)))
        inflight = int(_env("ADDRESS_LLM_MAX_INFLIGHT", "2"))
        self.sem = threading.Semaphore(max(1, inflight))
        self.budget = LLMCallBudget(int(_env("ADDRESS_LLM_MAX_CALLS_PER_RUN", "50000")))
        self.keep_alive = os.environ.get("OLLAMA_KEEP_ALIVE", "60m")
        self.host = _ollama_host()
        self._primary_failures = 0
        self._primary_failures_to_cooldown = int(_env("ADDRESS_LLM_PRIMARY_FAILS_TO_COOLDOWN", "4"))
        self._primary_cooldown_sec = int(_env("ADDRESS_LLM_PRIMARY_COOLDOWN_SEC", "180"))
        self._primary_cooldown_until = 0.0
        self._primary_state_lock = threading.Lock()

    @classmethod
    def instance(cls) -> "LLMAddressResolver":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    # ---------------------------------------------------------------

    def resolve(self, cand: AddressCandidate, kb_partial: ResolvedAddress) -> ResolvedAddress:
        if not self.budget.take():
            logger.warning("LLM call budget exhausted (%d). Skipping LLM.", self.budget.limit)
            return kb_partial

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "LLM resolve slot=%s model=%s fallback=%s timeout=%s partial=%s/%s/%s/%s signal=%s",
                cand.slot,
                self.model,
                self.fallback,
                self.timeout,
                _summary(kb_partial.country),
                _summary(kb_partial.state),
                _summary(kb_partial.district),
                _summary(kb_partial.mandal),
                cand.has_any_signal,
            )

        with self.sem:
            raw = self._call_with_fallback(cand, kb_partial)

        if raw is None:
            return kb_partial

        parsed = _safe_json(raw)
        if not parsed:
            logger.warning("LLM returned unparseable output: %r", raw[:200])
            return kb_partial

        return _validate_against_kb(parsed, cand, kb_partial)

    def _call_with_fallback(self, cand: AddressCandidate, kb_partial: ResolvedAddress) -> Optional[str]:
        prompt = _build_prompt(cand, kb_partial)

        model_chain = (self.model, self.fallback)
        with self._primary_state_lock:
            if time.time() < self._primary_cooldown_until:
                model_chain = (self.fallback,)
                remaining = max(0, int(self._primary_cooldown_until - time.time()))
                logger.debug("Primary model cooldown active; using fallback only for %ds", remaining)

        for attempt, model in enumerate(model_chain, start=1):
            try:
                timeout = self.primary_timeout if model == self.model else self.fallback_timeout
                logger.debug("LLM call attempt %d model=%s timeout=%s", attempt, model, timeout)
                out = self._call(model, prompt, timeout=timeout)
                if out:
                    if model == self.model:
                        self._note_primary_success()
                    logger.debug("LLM call attempt %d model=%s returned %d chars", attempt, model, len(out))
                    return out
            except Exception as exc:
                logger.warning("LLM call attempt %d model=%s failed: %s", attempt, model, exc)
                if model == self.model:
                    self._note_primary_failure(exc)
                time.sleep(min(2 ** (attempt - 1), 3))
        return None

    def _call(self, model: str, prompt: str, timeout: Optional[int] = None) -> Optional[str]:
        url = f"{self.host}/api/generate"
        payload = {
            "model": model,
            "prompt": prompt,
            "stream": False,
            "format": "json",
            "keep_alive": self.keep_alive,
            "options": {
                "temperature": 0.0,
                "num_ctx": 4096,
                "num_predict": 256,
            },
        }
        r = requests.post(url, json=payload, timeout=timeout or self.timeout)
        r.raise_for_status()
        data = r.json()
        return (data.get("response") or "").strip() or None

    def _note_primary_success(self) -> None:
        with self._primary_state_lock:
            self._primary_failures = 0
            self._primary_cooldown_until = 0.0
        logger.debug("Primary model succeeded; cooldown cleared")

    def _note_primary_failure(self, exc: Exception) -> None:
        if not isinstance(exc, requests.exceptions.Timeout):
            return
        with self._primary_state_lock:
            self._primary_failures += 1
            if self._primary_failures < self._primary_failures_to_cooldown:
                return
            self._primary_cooldown_until = time.time() + self._primary_cooldown_sec
            self._primary_failures = 0
            logger.warning(
                "Primary model %s timed out repeatedly; using fallback %s for %ds",
                self.model,
                self.fallback,
                self._primary_cooldown_sec,
            )


# -----------------------------------------------------------------
# Prompt construction + validation
# -----------------------------------------------------------------

_SYSTEM = (
    "You are an expert in Indian and global administrative geography. "
    "Given partial address fields including ward_colony and street text, return strict JSON: "
    '{"country": string|null, "state": string|null, "district": string|null, '
    '"mandal": string|null, "confidence": number between 0 and 1, "reason": string}. '
    "Use canonical English names. Prefer Indian official names "
    "(e.g., 'Telangana', 'Andhra Pradesh', 'Bengaluru', 'Mumbai'). "
    "If unknown, set the field to null. Do not invent names. "
    "For Indian states always set country to 'India'. Output ONLY valid JSON."
)


def _build_prompt(cand: AddressCandidate, partial: ResolvedAddress) -> str:
    payload = {
        "input": {
            "state": cand.raw_state,
            "district": cand.raw_district,
            "mandal": cand.raw_mandal,
            "country": cand.raw_country,
            "ward_colony": cand.raw_ward,
            "street": cand.raw_street,
            "pin_code": cand.raw_pin,
            "locality": cand.raw_locality,
            "landmark": cand.raw_landmark,
            "nationality": cand.raw_nationality,
        },
        "kb_partial": {
            "country": partial.country,
            "state": partial.state,
            "district": partial.district,
            "mandal": partial.mandal,
        },
    }
    return (
        _SYSTEM
        + "\n\nINPUT:\n"
        + json.dumps(payload, ensure_ascii=False)
        + "\n\nReturn JSON now."
    )


_JSON_RE = re.compile(r"\{.*\}", re.DOTALL)


def _safe_json(raw: str) -> Optional[dict]:
    try:
        return json.loads(raw)
    except Exception:
        pass
    m = _JSON_RE.search(raw)
    if not m:
        return None
    try:
        return json.loads(m.group(0))
    except Exception:
        return None


def _validate_against_kb(
    parsed: dict, cand: AddressCandidate, partial: ResolvedAddress
) -> ResolvedAddress:
    kb = GeoKB.instance()
    out = ResolvedAddress(slot=cand.slot, path="kb+llm", confidence=float(parsed.get("confidence") or 0.0))

    country = _s(parsed.get("country"))
    state = _s(parsed.get("state"))
    district = _s(parsed.get("district"))
    mandal = _s(parsed.get("mandal"))

    # state must exist in KB (Indian) or be a foreign state known via geo_countries
    canon_state = kb.canon_state(state)
    if not canon_state and state:
        # could be a foreign state; accept only if geo_countries knows it
        if kb.country_of_foreign_state(state):
            canon_state = state  # accept as-is; foreign state names aren't normalized here
    out.state = canon_state or partial.state

    # district must exist under state
    canon_district = kb.canon_district(out.state, district) if out.state else None
    out.district = canon_district or partial.district

    # mandal must exist under (state, district)
    canon_mandal = kb.canon_mandal(out.state, out.district, mandal) if out.state and out.district else None
    out.mandal = canon_mandal or partial.mandal

    # country: KB canon, or derive from state
    canon_country = kb.canon_country(country)
    if not canon_country:
        canon_country = kb.country_of_indian_state(out.state) or kb.country_of_foreign_state(out.state)
    out.country = canon_country or partial.country

    # confidence floor
    if out.confidence < 0.0:
        out.confidence = 0.0
    if out.confidence > 1.0:
        out.confidence = 1.0

    # notes — flag LLM fields that were rejected
    rejected = []
    if state and not canon_state:
        rejected.append(f"state={state!r}")
    if district and not canon_district:
        rejected.append(f"district={district!r}")
    if mandal and not canon_mandal:
        rejected.append(f"mandal={mandal!r}")
    if country and not canon_country:
        rejected.append(f"country={country!r}")
    if rejected:
        out.notes = "llm_rejected: " + ", ".join(rejected)

    return out


def _s(v) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s or None
