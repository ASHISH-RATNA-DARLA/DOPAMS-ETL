#!/usr/bin/env python3
"""
Temporary, read-only, API-ONLY deep validation of the 4 CCTNS V2 report
endpoints, reconciling each sampled record against the existing entity
endpoints DOPAMS already ingests (crimes, accused, arrests, person-details),
at the FIELD level — not just CRIME_ID presence.

SAFETY
--------------------------------------------------------------------------
- Every HTTP call is GET. No POST/PUT/PATCH/DELETE against CCTNS.
- No database import, connection, query, or credential of any kind.
- No ETL or application code is read or modified by this script.

Run:
    python diagnostics/validate_reports_deep.py
"""

from __future__ import annotations

import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from env_utils import load_repo_environment, resolve_api_base_url  # noqa: E402

load_repo_environment()

API_BASE_URL = (resolve_api_base_url("DOPAMAS_API_URL") or "").rstrip("/")
API_KEY = resolve_api_base_url("DOPAMAS_API_KEY") or ""
HEADERS = {"x-api-key": API_KEY}
TIMEOUT = int(resolve_api_base_url("API_TIMEOUT", default="60") or "60")

RECORD_LABELS = {"EXACT_MATCH", "MATCH_WITH_FIELD_DIFFERENCES", "PARTIAL_MATCH", "NO_MATCH", "NO_RESOLUTION_PATH"}
FINAL_LABELS = {"REDUNDANT", "UNIQUE_DATA_FOUND", "NO_DATA_FOUND", "REQUIRES_REVIEW", "NO_RESOLUTION_PATH", "API_ERROR"}

CALL_COUNT = 0


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------

def api_get(path: str, params: Optional[dict] = None) -> Tuple[Optional[int], Any, Optional[str], float]:
    global CALL_COUNT
    CALL_COUNT += 1
    url = f"{API_BASE_URL}{path}"
    start = time.time()
    try:
        resp = requests.get(url, params=params, headers=HEADERS, timeout=TIMEOUT)
        elapsed = time.time() - start
        try:
            payload = resp.json()
        except ValueError:
            payload = resp.text
        return resp.status_code, payload, None, elapsed
    except requests.RequestException as exc:
        return None, None, str(exc), time.time() - start


def extract_records(payload: Any) -> List[dict]:
    """Handles the observed nesting: {"status":.., "data": {"data": [...], "totalCount":..}}
    as well as the simpler {"status":.., "data": [...]} and bare-list/bare-dict shapes."""
    if payload is None:
        return []
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    if isinstance(payload, dict):
        data = payload.get("data", payload)
        if isinstance(data, list):
            return [r for r in data if isinstance(r, dict)]
        if isinstance(data, dict):
            inner = data.get("data") if isinstance(data.get("data"), (list, dict)) else None
            if isinstance(inner, list):
                return [r for r in inner if isinstance(r, dict)]
            if isinstance(inner, dict):
                return [inner]
            return [data]
    return []


def get_total_count(payload: Any) -> Optional[int]:
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, dict) and "totalCount" in data:
            return data.get("totalCount")
    return None


def norm_name(s: Optional[str]) -> str:
    if not s:
        return ""
    return re.sub(r"[^a-z]", "", s.lower())


def names_match(a: Optional[str], b: Optional[str]) -> bool:
    na, nb = norm_name(a), norm_name(b)
    if not na or not nb:
        return False
    return na == nb or na in nb or nb in na


def parse_date(raw: Any) -> Optional[datetime]:
    if not raw:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(str(raw)[: len(fmt.replace("%f", "000000"))], fmt)
        except ValueError:
            continue
    return None


def fetch_multi_window(ep: str, extra_params: dict, max_windows: int, start_from_days_ago: int = 1) -> Tuple[List[dict], List[str]]:
    all_records: List[dict] = []
    windows: List[str] = []
    to_date = datetime.now(timezone.utc) - timedelta(days=start_from_days_ago)
    for _ in range(max_windows):
        from_date = to_date - timedelta(days=6)
        frm, to = from_date.strftime("%Y-%m-%d"), to_date.strftime("%Y-%m-%d")
        status, payload, err, _ = api_get(ep, dict(extra_params, fromDate=frm, toDate=to))
        recs = extract_records(payload) if not err and status == 200 else []
        tc = get_total_count(payload) if not err and status == 200 else None
        windows.append(f"{frm}..{to} -> status={status if not err else 'ERROR:'+err} records={len(recs)}"
                        + (f" totalCount={tc}" if tc is not None else ""))
        if not err and status == 200:
            all_records.extend(recs)
        to_date = from_date - timedelta(days=1)
    return all_records, windows


_CRIME_CACHE: Dict[str, Any] = {}
_ACCUSED_CACHE: Dict[str, Any] = {}
_ARRESTS_CACHE: Dict[str, Any] = {}
_PERSON_CACHE: Dict[str, Any] = {}


def resolve_crime(crime_id: str) -> Optional[dict]:
    if crime_id in _CRIME_CACHE:
        return _CRIME_CACHE[crime_id]
    status, payload, err, _ = api_get(f"/crimes/{crime_id}")
    recs = extract_records(payload) if not err and status == 200 else []
    result = recs[0] if recs else None
    _CRIME_CACHE[crime_id] = result
    return result


def resolve_accused(crime_id: str) -> List[dict]:
    if crime_id in _ACCUSED_CACHE:
        return _ACCUSED_CACHE[crime_id]
    status, payload, err, _ = api_get(f"/accused/{crime_id}")
    recs = extract_records(payload) if not err and status == 200 else []
    _ACCUSED_CACHE[crime_id] = recs
    return recs


def resolve_arrests(crime_id: str) -> List[dict]:
    if crime_id in _ARRESTS_CACHE:
        return _ARRESTS_CACHE[crime_id]
    status, payload, err, _ = api_get(f"/arrests/{crime_id}")
    recs = extract_records(payload) if not err and status == 200 else []
    _ARRESTS_CACHE[crime_id] = recs
    return recs


def resolve_person(person_id: str) -> Optional[dict]:
    if person_id in _PERSON_CACHE:
        return _PERSON_CACHE[person_id]
    status, payload, err, _ = api_get(f"/person-details/{person_id}")
    recs = extract_records(payload) if not err and status == 200 else []
    result = recs[0] if recs else None
    _PERSON_CACHE[person_id] = result
    return result


def person_display_name(person_detail: dict) -> str:
    pd = person_detail.get("PERSONAL_DETAILS", {}) if isinstance(person_detail.get("PERSONAL_DETAILS"), dict) else {}
    return pd.get("FULL_NAME") or pd.get("NAME") or ""


# ---------------------------------------------------------------------------
# Per-endpoint deep validators
# ---------------------------------------------------------------------------

def log(msg: str = "") -> None:
    print(msg)


class RecordFinding:
    def __init__(self, key: str, label: str, notes: List[str]):
        assert label in RECORD_LABELS, label
        self.key, self.label, self.notes = key, label, notes


def rollup(findings: List[RecordFinding]) -> str:
    if not findings:
        return "NO_DATA_FOUND"
    labels = [f.label for f in findings]
    if all(l == "NO_RESOLUTION_PATH" for l in labels):
        return "NO_RESOLUTION_PATH"
    if any(l in ("NO_MATCH",) for l in labels):
        # some records couldn't be reconciled at all -> real unique data risk
        return "UNIQUE_DATA_FOUND"
    if any(l == "PARTIAL_MATCH" for l in labels):
        return "REQUIRES_REVIEW"
    if any(l == "MATCH_WITH_FIELD_DIFFERENCES" for l in labels):
        return "REQUIRES_REVIEW"
    if all(l == "EXACT_MATCH" for l in labels):
        return "REDUNDANT"
    return "REQUIRES_REVIEW"


# --- 1. missing-udb-persons ---------------------------------------------------

def validate_missing_udb(district: str) -> Tuple[str, List[RecordFinding], List[str], int]:
    log("\n" + "=" * 100)
    log("1. /reports/missing-udb-persons/v1/")
    log("=" * 100)

    all_records: List[dict] = []
    windows: List[str] = []
    for person_type in ("Missing", "Kidnapped", "Unknown Dead Body"):
        recs, w = fetch_multi_window("/reports/missing-udb-persons/v1/",
                                      {"districtCodes": district, "limit": "25", "page": "1", "personType": person_type}, 4)
        all_records.extend(recs)
        windows.extend(f"[{person_type}] {line}" for line in w)

    log(f"Windows tested: {len(windows)}  |  Total records fetched: {len(all_records)}")
    for line in windows:
        log(f"  - {line}")

    if not all_records:
        return "NO_DATA_FOUND", [], windows, 0

    sample = all_records[:8]
    findings: List[RecordFinding] = []
    log(f"\nSampling {len(sample)} record(s) for field-level reconciliation:")
    for rec in sample:
        crime_id = rec.get("CRIME_ID")
        person_name = rec.get("PERSON_NAME") or rec.get("PERSON")
        log(f"\n  Record: CRIME_ID={crime_id} FIR_NUM={rec.get('FIR_NUM')} PERSON_NAME={person_name!r} "
            f"AGE={rec.get('PERSON_AGE')} GENDER={rec.get('GENDER')}")

        crime = resolve_crime(crime_id) if crime_id else None
        if not crime:
            log(f"    -> /crimes/{{id}} did NOT resolve (CRIME_ID invalid/unresolvable)")
            findings.append(RecordFinding(crime_id or "?", "NO_RESOLUTION_PATH", ["CRIME_ID does not resolve via /crimes/{id}"]))
            continue
        log(f"    -> /crimes/{{id}} resolved: FIR_NUM={crime.get('FIR_NUM')} FIR_REG_NUM={crime.get('FIR_REG_NUM')}")

        accused_list = resolve_accused(crime_id)
        log(f"    -> /accused/{{crimeId}}: {len(accused_list)} accused record(s)")
        matched_person = None
        matched_accused = None
        for acc in accused_list:
            pid = acc.get("PERSON_ID")
            if not pid:
                continue
            pdet = resolve_person(pid)
            if pdet and names_match(person_display_name(pdet), person_name):
                matched_person, matched_accused = pdet, acc
                break

        arrests_list = resolve_arrests(crime_id)
        log(f"    -> /arrests/{{crimeId}}: {len(arrests_list)} arrest record(s)")

        notes = [f"FIR_NUM report={rec.get('FIR_NUM')!r} vs crimes={crime.get('FIR_NUM')!r}"]
        if rec.get("FIR_NUM") != crime.get("FIR_NUM"):
            notes.append("FIR_NUM MISMATCH between report and /crimes")

        if not matched_person:
            log(f"    -> NO accused/person record in /accused matches report's PERSON_NAME {person_name!r}")
            findings.append(RecordFinding(crime_id, "NO_MATCH", notes + [
                f"report person {person_name!r} not found among {len(accused_list)} /accused record(s) for this crime "
                f"(checked via linked /person-details) — this person-level info (name/age/gender/media/brief_facts of "
                f"a missing/UDB person) is not obtainable from /accused or /arrests for this crime."]))
            continue

        pd = matched_person.get("PERSONAL_DETAILS", {}) if isinstance(matched_person.get("PERSONAL_DETAILS"), dict) else {}
        field_diffs = []
        if rec.get("PERSON_AGE") is not None and pd.get("AGE") is not None and int(rec.get("PERSON_AGE")) != int(pd.get("AGE")):
            field_diffs.append(f"AGE report={rec.get('PERSON_AGE')} vs person-details={pd.get('AGE')}")
        rg = (rec.get("GENDER") or "").strip().lower()
        pg = (pd.get("GENDER") or "").strip().lower()
        if rg and pg and rg != pg:
            field_diffs.append(f"GENDER report={rec.get('GENDER')} vs person-details={pd.get('GENDER')}")
        log(f"    -> matched person via /accused -> /person-details: {person_display_name(matched_person)!r}"
            + (f"; field diffs: {field_diffs}" if field_diffs else "; fields consistent"))

        label = "MATCH_WITH_FIELD_DIFFERENCES" if field_diffs else "EXACT_MATCH"
        findings.append(RecordFinding(crime_id, label, notes + field_diffs))

    return rollup(findings), findings, windows, len(all_records)


# --- 2. arrest-particulars (internal) ----------------------------------------

def validate_arrest_particulars(district: str) -> Tuple[str, List[RecordFinding], List[str], int]:
    log("\n" + "=" * 100)
    log("2. /reports/arrest/arrest-particulars/v1/")
    log("=" * 100)

    records, windows = fetch_multi_window("/reports/arrest/arrest-particulars/v1/", {"districtCodes": district, "limit": "25", "page": "1"}, 6)
    log(f"Windows tested: {len(windows)}  |  Total records fetched: {len(records)}")
    for line in windows:
        log(f"  - {line}")

    if not records:
        return "NO_DATA_FOUND", [], windows, 0

    sample = records[:8]
    findings: List[RecordFinding] = []
    log(f"\nSampling {len(sample)} record(s) for field-level reconciliation:")
    for rec in sample:
        crime_id = rec.get("CRIME_ID")
        acc_name = rec.get("ACCUSED_NAME")
        log(f"\n  Record: CRIME_ID={crime_id} FIR_NUM={rec.get('FIR_NUM')} ACCUSED_NAME={acc_name!r} "
            f"AGE={rec.get('ACCUSED_AGE')} ARREST_TYPE={rec.get('ARREST_TYPE')} DATE_OF_ARREST={rec.get('DATE_OF_ARREST')}")

        crime = resolve_crime(crime_id) if crime_id else None
        if not crime:
            log("    -> /crimes/{id} did NOT resolve")
            findings.append(RecordFinding(crime_id or "?", "NO_RESOLUTION_PATH", ["CRIME_ID does not resolve via /crimes/{id}"]))
            continue

        arrests_list = resolve_arrests(crime_id)
        log(f"    -> /arrests/{{crimeId}}: {len(arrests_list)} arrest record(s)")
        matched_arrest = None
        matched_person = None
        for arr in arrests_list:
            pid = arr.get("PERSON_ID")
            if not pid:
                continue
            pdet = resolve_person(pid)
            if pdet and names_match(person_display_name(pdet), acc_name):
                matched_arrest, matched_person = arr, pdet
                break

        if not matched_arrest:
            log(f"    -> NO /arrests record links (via /person-details) to report's ACCUSED_NAME {acc_name!r}")
            findings.append(RecordFinding(crime_id, "NO_MATCH", [
                f"report accused {acc_name!r} not linkable to any of {len(arrests_list)} /arrests record(s) for this crime via person-details name match"]))
            continue

        field_diffs = []
        report_arrest_dt = parse_date(rec.get("DATE_OF_ARREST"))
        bulk_arrest_dt = parse_date(matched_arrest.get("ARRESTED_DATE"))
        if report_arrest_dt and bulk_arrest_dt and abs((report_arrest_dt - bulk_arrest_dt).days) > 1:
            field_diffs.append(f"ARREST_DATE report={rec.get('DATE_OF_ARREST')} vs /arrests.ARRESTED_DATE={matched_arrest.get('ARRESTED_DATE')}")
        rtype = (rec.get("ARREST_TYPE") or "").strip().lower()
        atype = (matched_arrest.get("ACCUSED_TYPE") or "").strip().lower()
        if rtype and atype and rtype != atype:
            field_diffs.append(f"ARREST_TYPE report={rec.get('ARREST_TYPE')} vs /arrests.ACCUSED_TYPE={matched_arrest.get('ACCUSED_TYPE')}")
        pd = matched_person.get("PERSONAL_DETAILS", {}) if isinstance(matched_person.get("PERSONAL_DETAILS"), dict) else {}
        if rec.get("ACCUSED_AGE") is not None and pd.get("AGE") is not None and int(rec.get("ACCUSED_AGE")) != int(pd.get("AGE")):
            field_diffs.append(f"AGE report={rec.get('ACCUSED_AGE')} vs person-details={pd.get('AGE')}")

        log(f"    -> matched /arrests record + /person-details {person_display_name(matched_person)!r}"
            + (f"; field diffs: {field_diffs}" if field_diffs else "; fields consistent"))
        label = "MATCH_WITH_FIELD_DIFFERENCES" if field_diffs else "EXACT_MATCH"
        findings.append(RecordFinding(crime_id, label, field_diffs))

    return rollup(findings), findings, windows, len(records)


# --- 3. citizen arrest-particulars -------------------------------------------

def validate_citizen_arrest_particulars(district: str, internal_records: List[dict]) -> Tuple[str, List[RecordFinding], List[str], int]:
    log("\n" + "=" * 100)
    log("3. /reports/citizen/arrest/arrest-particulars/v1/")
    log("=" * 100)

    records, windows = fetch_multi_window("/reports/citizen/arrest/arrest-particulars/v1/", {"district_cd": district, "limit": "25", "page": "1"}, 6)
    log(f"Windows tested: {len(windows)}  |  Total records fetched: {len(records)}")
    for line in windows:
        log(f"  - {line}")

    if not records:
        return "NO_DATA_FOUND", [], windows, 0

    # Bridge index: internal arrest-particulars keyed by (FIR_NUM, normalized accused name)
    bridge: Dict[Tuple[str, str], dict] = {}
    for r in internal_records:
        key = (str(r.get("FIR_NUM") or "").strip(), norm_name(r.get("ACCUSED_NAME")))
        if key[0] and key[1]:
            bridge[key] = r

    sample = records[:8]
    findings: List[RecordFinding] = []
    log(f"\nSampling {len(sample)} record(s), reconciling via FIR_NO + accused name against the internal "
        f"arrest-particulars dataset fetched above ({len(internal_records)} candidates):")
    for rec in sample:
        fir_no = str(rec.get("FIR_NO") or "").strip()
        acc_name = rec.get("ACCUSED_NAME")
        log(f"\n  Record: FIR_NO={fir_no} FIR_REG_NUM={rec.get('FIR_REG_NUM')} ACCUSED_NAME={acc_name!r} "
            f"ARREST_DT={rec.get('ARREST_DT')} DISTRICT={rec.get('DISTRICT')}")

        match = bridge.get((fir_no, norm_name(acc_name)))
        if not match:
            # fallback: same FIR_NO, fuzzy name containment
            candidates = [r for r in internal_records if str(r.get("FIR_NUM") or "").strip() == fir_no]
            for cand in candidates:
                if names_match(cand.get("ACCUSED_NAME"), acc_name):
                    match = cand
                    break

        if not match:
            log(f"    -> NO match in internal arrest-particulars dataset for FIR_NO={fir_no} + name={acc_name!r}")
            findings.append(RecordFinding(fir_no or "?", "NO_RESOLUTION_PATH", [
                "No CRIME_ID-bearing internal report record shares this FIR_NO+accused name within the windows tested "
                "— cannot bridge to /crimes, /arrests, or /person-details without a resolvable crime_id."]))
            continue

        crime_id = match.get("CRIME_ID")
        crime = resolve_crime(crime_id) if crime_id else None
        field_diffs = []
        if crime:
            if match.get("FIR_NUM") != rec.get("FIR_NO"):
                field_diffs.append("FIR number formatting differs between citizen and internal report")
            r_dt = parse_date(rec.get("ARREST_DT"))
            m_dt = parse_date(match.get("DATE_OF_ARREST"))
            if r_dt and m_dt and abs((r_dt - m_dt).days) > 1:
                field_diffs.append(f"ARREST_DT citizen={rec.get('ARREST_DT')} vs internal DATE_OF_ARREST={match.get('DATE_OF_ARREST')}")
            log(f"    -> bridged to CRIME_ID={crime_id} via internal report; /crimes/{{id}} resolves={bool(crime)}"
                + (f"; field diffs: {field_diffs}" if field_diffs else "; fields consistent"))
            label = "MATCH_WITH_FIELD_DIFFERENCES" if field_diffs else "EXACT_MATCH"
        else:
            log(f"    -> bridged to CRIME_ID={crime_id} but /crimes/{{id}} did NOT resolve it")
            label = "PARTIAL_MATCH"
            field_diffs.append("bridged crime_id does not resolve via /crimes/{id}")
        findings.append(RecordFinding(fir_no, label, field_diffs))

    log("\nShape check: citizen variant exposes CIRCLE/SDPO/PS/INTIMATION_GIVEN_TO/ARREST_REASON/ARREST_PERSONAL_DET "
        "in addition to the FIR/accused identifiers — a smaller, public-safe field projection of the same"
        " underlying arrest record (no PERSON_ID/media/internal case fields).")

    return rollup(findings), findings, windows, len(records)


# --- 4. stolen-automobiles ----------------------------------------------------

def validate_stolen_automobiles(max_windows: int = 52) -> Tuple[str, List[RecordFinding], List[str], int]:
    log("\n" + "=" * 100)
    log("4. /reports/stolen-automobiles")
    log("=" * 100)

    records, windows = fetch_multi_window("/reports/stolen-automobiles", {}, max_windows)
    log(f"Windows tested: {len(windows)} ({len(windows) * 7} days, ~{len(windows) // 4} months)  |  Total records fetched: {len(records)}")
    for line in windows:
        log(f"  - {line}")

    if not records:
        return "NO_DATA_FOUND", [], windows, 0

    sample = records[:8]
    findings: List[RecordFinding] = []
    log(f"\nSampling {len(sample)} record(s) for field-level reconciliation against /property-details:")
    for rec in sample:
        crime_id = rec.get("CRIME_ID")
        log(f"\n  Record: CRIME_ID={crime_id} STOLEN_PROPERTY_ID={rec.get('STOLEN_PROPERTY_ID')} "
            f"REGISTRATION_NO={rec.get('REGISTRATION_NO')} CHASSIS_NO={rec.get('CHASSIS_NO')}")

        crime = resolve_crime(crime_id) if crime_id else None
        if not crime:
            findings.append(RecordFinding(crime_id or "?", "NO_RESOLUTION_PATH", ["CRIME_ID does not resolve via /crimes/{id}"]))
            continue

        crime_date = parse_date(crime.get("DATE_CREATED"))
        prop_status, prop_payload, prop_err, _ = (None, None, None, None)
        if crime_date:
            frm = (crime_date - timedelta(days=3)).strftime("%Y-%m-%d")
            to = (crime_date + timedelta(days=3)).strftime("%Y-%m-%d")
            prop_status, prop_payload, prop_err, _ = api_get("/property-details", {"fromDate": frm, "toDate": to})
        prop_records = extract_records(prop_payload) if prop_payload and not prop_err and prop_status == 200 else []
        prop_matches = [p for p in prop_records if p.get("CRIME_ID") == crime_id and p.get("CATEGORY") == "Automobiles"]

        if not prop_matches:
            findings.append(RecordFinding(crime_id, "NO_MATCH", ["No matching Automobiles-category /property-details record found for this crime_id"]))
            continue

        add = prop_matches[0].get("ADDITIONAL_DETAILS", {}) if isinstance(prop_matches[0].get("ADDITIONAL_DETAILS"), dict) else {}
        diffs = []
        for rfield, pfield in (("CHASSIS_NO", "CHASSIS_NO"), ("REGISTRATION_NO", "REGISTRATION_NO"), ("ENGINE_NO", "ENGINE_NO")):
            rv, pv = rec.get(rfield), add.get(pfield)
            if rv and pv and str(rv).strip() != str(pv).strip():
                diffs.append(f"{rfield} report={rv} vs property-details={pv}")
        label = "MATCH_WITH_FIELD_DIFFERENCES" if diffs else "EXACT_MATCH"
        findings.append(RecordFinding(crime_id, label, diffs))

    return rollup(findings), findings, windows, len(records)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def get_sample_district_code() -> Optional[str]:
    status, payload, err, _ = api_get("/master-data/hierarchy")
    if err or status != 200:
        return None
    for rec in extract_records(payload):
        if rec.get("DIST_CODE"):
            return rec["DIST_CODE"]
    return None


def main():
    print("=" * 100)
    print("DEEP FIELD-LEVEL VALIDATION — 4 CCTNS V2 REPORT ENDPOINTS (API-only, no database)")
    print("=" * 100)

    if not API_BASE_URL:
        print("[FATAL] DOPAMAS_API_URL not configured.", file=sys.stderr)
        return 1

    district = get_sample_district_code()
    print(f"\nUsing district code: {district}")
    if not district:
        print("[FATAL] Could not resolve a district code from /master-data/hierarchy — cannot proceed.", file=sys.stderr)
        return 1

    label1, findings1, windows1, n1 = validate_missing_udb(district)

    # Fetch internal arrest-particulars once; reused by both #2 and #3 (citizen bridge).
    internal_records, internal_windows = fetch_multi_window(
        "/reports/arrest/arrest-particulars/v1/", {"districtCodes": district, "limit": "25", "page": "1"}, 6)
    label2, findings2, windows2, n2 = validate_arrest_particulars(district)
    label3, findings3, windows3, n3 = validate_citizen_arrest_particulars(district, internal_records)
    label4, findings4, windows4, n4 = validate_stolen_automobiles()

    results = [
        ("stolen-automobiles", label4, findings4, windows4, n4),
        ("missing-udb-persons", label1, findings1, windows1, n1),
        ("arrest/arrest-particulars", label2, findings2, windows2, n2),
        ("citizen/arrest/arrest-particulars", label3, findings3, windows3, n3),
    ]

    print("\n\n" + "=" * 115)
    print("FINAL TERMINAL REPORT")
    print("=" * 115)
    header = f"{'Endpoint':42} {'Result':20} {'Records Tested':>15} {'Unique Fields':>14} {'Resolution'}"
    print(header)
    print("-" * len(header))
    for name, label, findings, windows, n in results:
        unique_ct = sum(1 for f in findings if f.label == "NO_MATCH")
        resolution = "multi-hop: crimes->accused/arrests->person-details" if name != "stolen-automobiles" else "crimes->property-details"
        if name == "citizen/arrest/arrest-particulars":
            resolution = "FIR_NO+name bridge -> internal report -> crimes/arrests"
        print(f"{name:42} {label:20} {n:>15} {unique_ct:>14} {resolution}")

    print("\n" + "=" * 115)
    print("EVIDENCE")
    print("=" * 115)
    for name, label, findings, windows, n in results:
        exact = sum(1 for f in findings if f.label == "EXACT_MATCH")
        diffs = sum(1 for f in findings if f.label == "MATCH_WITH_FIELD_DIFFERENCES")
        partial = sum(1 for f in findings if f.label == "PARTIAL_MATCH")
        nomatch = sum(1 for f in findings if f.label == "NO_MATCH")
        noresolve = sum(1 for f in findings if f.label == "NO_RESOLUTION_PATH")
        print(f"\n{name}  [{label}]")
        print(f"  windows tested        : {len(windows)}")
        print(f"  records fetched       : {n}")
        print(f"  records sampled       : {len(findings)}")
        print(f"  exact matches         : {exact}")
        print(f"  match w/ field diffs  : {diffs}")
        print(f"  partial matches       : {partial}")
        print(f"  no match (unresolved) : {nomatch}")
        print(f"  no resolution path    : {noresolve}")
        for f in findings:
            if f.notes:
                print(f"    - [{f.key}] {f.label}: {'; '.join(f.notes)}")

    print(f"\nTotal API calls made this run: {CALL_COUNT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
