"""
Quick test for the multi-FIR pre-processor.
Can run standalone — only imports the preprocessor function (no LLM/DB deps).

Usage:
  cd brief_facts_drugs
  python test_preprocessor.py
"""
import sys, os, re, logging

logging.basicConfig(level=logging.INFO, format="%(message)s")

# --- Inline the preprocessor logic so this test has ZERO external deps ---
# (mirrors extractor.py's preprocess_brief_facts exactly)

_FIR_BOUNDARY_RE = re.compile(
    r'(?=IN\s+(?:THE\s+)?HONOU?RABLE\s+(?:COURT|EXECUTIVE))',
    re.IGNORECASE
)

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

_DRUG_KEYWORDS_TIER2 = {
    'seized', 'substance', 'powder', 'tablet', 'capsule',
    'packet', 'packets', 'contraband', 'smuggling', 'transporting',
    'peddling', 'consumption', 'addiction', 'intoxicant',
}

_NDPS_SECTION_RE = re.compile(
    r'\b(?:8\s*\([a-c]\)|20\s*\([a-c]\)|21|22|25|27|28|29)\b.*?NDPS|NDPS.*?\b(?:8|20|21|22|25|27|28|29)\b',
    re.IGNORECASE
)

def _estimate_tokens(text):
    return len(text) // 4

def _score_drug_relevance(section):
    lower = section.lower()
    score = 0
    for kw in _DRUG_KEYWORDS_TIER1:
        if kw in lower:
            score += 100
            break
    if _NDPS_SECTION_RE.search(section):
        score += 80
    t2_hits = sum(1 for kw in _DRUG_KEYWORDS_TIER2 if kw in lower)
    score += t2_hits * 15
    return score

def preprocess_brief_facts(text, relevance_threshold=50):
    if not text or not text.strip():
        return text, {"original_chars": 0, "filtered_chars": 0, "total_sections": 0,
                      "kept_sections": 0, "dropped_sections": 0, "estimated_tokens_saved": 0}
    sections = _FIR_BOUNDARY_RE.split(text)
    sections = [s for s in sections if s and s.strip()]
    if len(sections) <= 1:
        return text, {"original_chars": len(text), "filtered_chars": len(text),
                      "total_sections": 1, "kept_sections": 1, "dropped_sections": 0,
                      "estimated_tokens_saved": 0}
    scored = []
    for i, section in enumerate(sections):
        sc = _score_drug_relevance(section)
        scored.append((i, section, sc, sc >= relevance_threshold))
    kept = [s for s in scored if s[3]]
    dropped = [s for s in scored if not s[3]]
    filtered_text = "\n\n".join(s[1].strip() for s in kept) if kept else ""
    tokens_saved = _estimate_tokens(text) - _estimate_tokens(filtered_text)
    meta = {
        "original_chars": len(text), "filtered_chars": len(filtered_text),
        "total_sections": len(sections), "kept_sections": len(kept),
        "dropped_sections": len(dropped), "estimated_tokens_saved": tokens_saved,
        "sections_detail": [
            {"index": s[0], "score": s[2], "kept": s[3],
             "preview": s[1].strip()[:100].replace('\n', ' ')}
            for s in scored
        ],
    }
    return filtered_text, meta

test_text = """IN THE HONOURABLE COURT OF SPECIAL JUDICIAL MAGISTRATE OF FIRST-CLASS PROHIBITION AND EXCISE OFFENCES COURT AT SANGAREDDY.
HONOURED MADAM/SIR
Facts of the case are that on 07.09.2023 some persons are transporting the dry ganja from Aruku. 1) Dheeraj Munnala Jaiswal and 2) Prashanth Sanjay Shinde purchased 58.5 K.G (39 Packets) from Vinay Mandal. Seized Dry Ganja of MO-1 to MO-39 of 58.5 KGS which cost about Rs 11,70,000/-. registered a case in Cr No 558/2023 U/s 8(c) r/w 20(b)(ii)(c) NDPS Act-1985

IN THE HONOURABLE COURT OF SPECIAL JUDICIAL MAGISTRATE OF FIRST CLASS PROHIBITION AND EXCISE OFFENCES COURT AT SANGAREDDY.
HONOURED MADAM/SIR
Facts: one person illegally filled gas in small gas cylinders. seized HP Full Domestic Gas cylinders-3. case in Cr.No 557/2023 U/s 285 IPC and Sec 7(1) EC Act

IN THE HONOURABLE EXECUTIVE CUM THASILDAR OF RC PURAM MANDAL.
HONOURED MADAM,
Facts: S Kavitha committed suicide by hanging. case in Cr.No. 556/2023 U/s 174 Cr.P.C

IN THE HONOURABLE COURT AT SANGAREDDY.
HONOURED MADAM/SIR,
Facts: some persons playing three cards game by betting money. case in Cr.No. 555/2023 U/s 4 of TS Gaming Act.

IN THE HONOURABLE COURT AT SANGAREDDY.
Facts: HF Deluxe bike was stolen. case in Cr.No. 554/2023 U/s 379 IPC

IN THE HONOURABLE COURT AT SANGAREDDY.
Facts: road accident with rash negligent driving. case in Cr.No. 553/2023 U/s 279,504 IPC"""

filtered, meta = preprocess_brief_facts(test_text)

print("\n--- RESULT ---")
print("Total sections:", meta["total_sections"])
print("Kept:", meta["kept_sections"])
print("Dropped:", meta["dropped_sections"])
print("Tokens saved:", meta["estimated_tokens_saved"])

for s in meta.get("sections_detail", []):
    tag = "KEEP" if s["kept"] else "DROP"
    preview = s["preview"][:70]
    score = s["score"]
    print(f"  [{tag}] score={score:3d} | {preview}")

print()
has_ganja = "ganja" in filtered.lower()
no_gas = "gas cylinders" not in filtered.lower()
no_suicide = "suicide" not in filtered.lower()
no_cards = "three cards" not in filtered.lower()
no_bike = "bike was stolen" not in filtered.lower()
no_accident = "rash negligent" not in filtered.lower()

print("Drug section kept:", has_ganja)
print("Gas cylinders dropped:", no_gas)
print("Suicide dropped:", no_suicide)
print("Cards game dropped:", no_cards)
print("Theft dropped:", no_bike)
print("Accident dropped:", no_accident)
print()

if all([has_ganja, no_gas, no_suicide, no_cards, no_bike, no_accident]):
    print("ALL TESTS PASSED")
else:
    print("SOME TESTS FAILED")

# Test single-FIR passthrough
single_text = "On 01.01.2024 accused had 5 grams of ganja seized under NDPS Act."
filtered_single, meta_single = preprocess_brief_facts(single_text)
assert meta_single["total_sections"] == 1, "Single FIR should have 1 section"
assert meta_single["kept_sections"] == 1, "Single FIR should be kept"
assert filtered_single == single_text, "Single FIR should pass through unchanged"
print("Single-FIR passthrough: PASSED")

# Test empty text
filtered_empty, meta_empty = preprocess_brief_facts("")
assert filtered_empty == "", "Empty text should return empty"
assert meta_empty["total_sections"] == 0, "Empty text should have 0 sections"
print("Empty text handling: PASSED")
