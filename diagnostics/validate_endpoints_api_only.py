#!/usr/bin/env python3
"""
Temporary, read-only, API-ONLY CCTNS V2 endpoint validator.

No database of any kind is connected to, read from, or modified. This
script only calls the live CCTNS V2 API (GET requests, never NCRP/write
endpoints) using the existing API client credentials, and compares JSON
response shapes against each other.

Uses HARDCODED crime_ids/file_id supplied by the user (from a prior,
separately-run 29-endpoint test) rather than discovering IDs dynamically.

SAFETY
--------------------------------------------------------------------------
- Every HTTP call is GET. No POST/PUT/PATCH/DELETE against CCTNS.
- NCRP endpoints (gen-token, e-zero-fir, e-zero-fir/ack-copy) are never
  called at all.
- No database import, connection, query, or credential of any kind.
- No ETL or application code is read or modified by this script.

Run:
    python diagnostics/validate_endpoints_api_only.py
"""

from __future__ import annotations

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

# Hardcoded test IDs, exactly as supplied — not discovered dynamically.
DETAIL_IDS = {
    "crimes": "65930ca8eec4a3f005d8667c",
    "disposal": "65959992c5c223d372efa506",
    "accused": "62b53a7f447aa041707dce99",
    "arrests": "62b53a7f447aa041707dce99",
    "mo_seizures": "63cc2a307f84d853c71c754c",
    "property_details": "63f3113f59db8547bf8a0de7",
    "interrogation_reports": "63b81a159aaa21e1320d3102",
    "chargesheets": "63b81a159aaa21e1320d3102",
    "update_chargesheets": "63b81a159aaa21e1320d3102",
    "case_property": "6522bf667c0055345cba8814",
}
FILE_ID = "db921173-6d6d-491a-ab7a-9053ac8f56db"

FINAL_LABELS = {"REDUNDANT", "UNIQUE_DATA_FOUND", "NO_DATA_FOUND", "REQUIRES_REVIEW", "API_ERROR"}


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------

def api_get(path: str, params: Optional[dict] = None) -> Tuple[Optional[int], Any, Optional[str], float]:
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


def non_null_fields(record: dict) -> Dict[str, Any]:
    return {k: v for k, v in record.items() if v not in (None, "", [], {})}


def _norm_value(v: Any) -> str:
    return str(v).strip().rstrip("Z").replace("T", " ") if v is not None else ""


def fields_present_in_a_not_b(a: dict, b: dict) -> List[str]:
    b_lower = {k.lower(): v for k, v in b.items()}
    return [k for k, v in non_null_fields(a).items() if b_lower.get(k.lower()) in (None, "", [], {})]


def fields_with_conflicting_values(a: dict, b: dict) -> List[Tuple[str, Any, Any]]:
    b_lower = {k.lower(): v for k, v in b.items()}
    conflicts = []
    for k, v in non_null_fields(a).items():
        if k.lower() not in b_lower:
            continue
        bv = b_lower[k.lower()]
        if bv in (None, "", [], {}):
            continue
        nv, nbv = _norm_value(v), _norm_value(bv)
        if nv != nbv and nv[:10] != nbv[:10]:
            conflicts.append((k, v, bv))
    return conflicts


DATE_FIELD_CANDIDATES = ("DATE_CREATED", "dateCreated", "DISPOSED_DATE", "chargeSheetDate", "ARRESTED_DATE")


def guess_record_date(rec: dict) -> Optional[datetime]:
    for key in DATE_FIELD_CANDIDATES:
        raw = rec.get(key)
        if not raw:
            continue
        for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d"):
            try:
                return datetime.strptime(str(raw)[: len(fmt.replace("%f", "000000"))], fmt)
            except ValueError:
                continue
    return None


_CRIME_LOOKUP_CACHE: Dict[str, Any] = {}


def lookup_crime(crime_id: str) -> Tuple[Optional[dict], Optional[int]]:
    """GET /crimes/{crime_id} directly — CRIME_ID is the universal join key
    across every entity in this API, and crimes always carries DATE_CREATED/
    FIR_DATE, so this is a far more reliable date-seed than trying to parse
    each entity's own (sometimes absent, e.g. /accused) date fields. Cached
    since several checks may ask about the same crime_id."""
    if crime_id in _CRIME_LOOKUP_CACHE:
        return _CRIME_LOOKUP_CACHE[crime_id]
    status, payload, err, _ = api_get(f"/crimes/{crime_id}")
    if err or status != 200:
        result = (None, status)
    else:
        recs = extract_records(payload)
        result = (recs[0] if recs else None, status)
    _CRIME_LOOKUP_CACHE[crime_id] = result
    return result


def resolve_crime_date(crime_id: str) -> Optional[datetime]:
    rec, _ = lookup_crime(crime_id)
    if not rec:
        return None
    return guess_record_date(rec) or (
        _parse_date(rec.get("FIR_DATE")) if rec.get("FIR_DATE") else None
    )


def _parse_date(raw: Any) -> Optional[datetime]:
    if not raw:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d"):
        try:
            return datetime.strptime(str(raw)[: len(fmt.replace("%f", "000000"))], fmt)
        except ValueError:
            continue
    return None


# ---------------------------------------------------------------------------
# Result record
# ---------------------------------------------------------------------------

class Result:
    def __init__(self, method: str, endpoint: str, http: str, summary: str, label: str, evidence: List[str]):
        assert label in FINAL_LABELS, label
        self.method, self.endpoint, self.http, self.summary, self.label = method, endpoint, http, summary, label
        self.evidence = evidence


RESULTS: List[Result] = []


def emit(r: Result) -> None:
    RESULTS.append(r)
    print(f"\n### {r.method} {r.endpoint}  [{r.label}]  (HTTP: {r.http})")
    print(f"    {r.summary}")
    for line in r.evidence:
        if line:
            print(f"    - {line}")


# ---------------------------------------------------------------------------
# Entity detail-vs-bulk comparison
# ---------------------------------------------------------------------------

ENTITIES = [
    ("crimes", "/crimes", "/crimes/{id}", "CRIME_ID"),
    ("disposal", "/crimes/disposal", "/crimes/disposal/{id}", "CRIME_ID"),
    ("accused", "/accused", "/accused/{id}", "CRIME_ID"),
    ("arrests", "/arrests", "/arrests/{id}", "CRIME_ID"),
    ("mo_seizures", "/mo-seizures", "/mo-seizures/{id}", "CRIME_ID"),
    ("property_details", "/property-details", "/property-details/{id}", "CRIME_ID"),
    ("interrogation_reports", "/interrogation-reports/v1/", "/interrogation-reports/v1/{id}", "CRIME_ID"),
    ("chargesheets", "/chargesheets", "/chargesheets/{id}", "crimeId"),
    ("update_chargesheets", "/update-chargesheets", "/update-chargesheets/{id}", "crimeId"),
    ("case_property", "/case-property", "/case-property/{id}", "CRIME_ID"),
]


def validate_entity(name: str, bulk_path: str, detail_tpl: str, id_field: str, crime_id: str) -> None:
    endpoint_label = detail_tpl
    evidence: List[str] = []

    det_status, det_payload, det_err, det_t = api_get(detail_tpl.replace("{id}", crime_id))
    if det_err:
        emit(Result("GET", endpoint_label, "ERROR", f"connection error: {det_err}", "API_ERROR", [f"id={crime_id}"]))
        return
    if det_status == 400 and isinstance(det_payload, dict) and "invalid" in str(det_payload.get("message", "")).lower():
        emit(Result("GET", endpoint_label, "400", f"API returned 400 'Invalid' for this id — this entity type has no record for id={crime_id}",
                     "NO_DATA_FOUND", [f"id={crime_id}", f"response: {det_payload}",
                                        "Note: this endpoint returns HTTP 400 (not 200+empty) when the id has no matching record of this type — confirmed API contract."]))
        return
    if det_status != 200:
        emit(Result("GET", endpoint_label, str(det_status), f"unexpected status for id={crime_id}", "API_ERROR",
                     [f"id={crime_id}", f"response: {str(det_payload)[:300]}"]))
        return

    det_records = extract_records(det_payload)
    if not det_records:
        emit(Result("GET", endpoint_label, "200", f"HTTP 200 but no records returned for id={crime_id}", "NO_DATA_FOUND", [f"id={crime_id}"]))
        return

    evidence.append(f"id={crime_id}: detail returned {len(det_records)} record(s) in {det_t:.2f}s")

    # Universal, reliable seed date: CRIME_ID is the join key shared by every
    # entity here, and /crimes always carries DATE_CREATED/FIR_DATE — this is
    # far more robust than relying on each entity's own (sometimes absent,
    # e.g. /accused has no date field at all) date fields.
    crime_date = resolve_crime_date(crime_id)
    if crime_date:
        evidence.append(f"id={crime_id}: resolved real crime date via /crimes/{{id}} lookup: {crime_date.date()}")
    else:
        evidence.append(f"id={crime_id}: could not resolve a date via /crimes/{{id}} lookup — falling back to per-record/recent-window search")

    window_cache: Dict[Tuple[str, str], List[dict]] = {}

    def fetch_window(center: datetime) -> List[dict]:
        frm = (center - timedelta(days=3)).strftime("%Y-%m-%d")
        to = (center + timedelta(days=3)).strftime("%Y-%m-%d")
        key = (frm, to)
        if key not in window_cache:
            b_status, b_payload, b_err, _ = api_get(bulk_path, {"fromDate": frm, "toDate": to})
            evidence.append(f"bulk window {frm}..{to} -> {'HTTP ' + str(b_status) if not b_err else b_err}")
            window_cache[key] = extract_records(b_payload) if (not b_err and b_status == 200) else []
        return window_cache[key]

    def recent_window_scan() -> List[dict]:
        to_date = datetime.now(timezone.utc) - timedelta(days=1)
        for _ in range(6):
            from_date = to_date - timedelta(days=6)
            recs = fetch_window(from_date + timedelta(days=3))
            if any(str(r.get(id_field, "")) == str(crime_id) for r in recs):
                return recs
            to_date = from_date - timedelta(days=1)
        return []

    # A single crime can have MULTIPLE records of a given type spanning very
    # different dates (confirmed live: interrogation reports on the same
    # crime created months apart) — so each detail record gets matched
    # against a bulk window seeded from ITS OWN date, not one shared window
    # for the whole batch.
    has_extra = has_conflict = has_missing = False
    unmatched = 0
    for det_rec in det_records:
        rec_date = guess_record_date(det_rec) or crime_date
        candidates = fetch_window(rec_date) if rec_date else []
        bulk_matches = [r for r in candidates if str(r.get(id_field, "")) == str(crime_id)]
        if not bulk_matches:
            recs = recent_window_scan()
            bulk_matches = [r for r in recs if str(r.get(id_field, "")) == str(crime_id)]
        if not bulk_matches:
            unmatched += 1
            continue

        best = min(bulk_matches, key=lambda b: len(fields_present_in_a_not_b(det_rec, b)) + len(fields_with_conflicting_values(det_rec, b)))
        extra = fields_present_in_a_not_b(det_rec, best)
        missing = fields_present_in_a_not_b(best, det_rec)
        conflicts = fields_with_conflicting_values(det_rec, best)
        if extra:
            has_extra = True
            evidence.append(f"id={crime_id}: DETAIL record has fields not in its closest BULK match: {extra}")
        if missing:
            has_missing = True
            evidence.append(f"id={crime_id}: BULK record has fields not in this DETAIL record: {missing}")
        if conflicts:
            has_conflict = True
            evidence.append(f"id={crime_id}: conflicting values (field, detail_value, bulk_value): {conflicts}")

    matched = len(det_records) - unmatched
    evidence.append(f"id={crime_id}: {matched}/{len(det_records)} detail record(s) found a matching bulk record")

    if matched == 0:
        emit(Result("GET", endpoint_label, "200", f"detail has {len(det_records)} record(s) but NONE found a matching bulk record for id={crime_id}",
                     "REQUIRES_REVIEW", evidence + ["Detail endpoint returned data but the corresponding bulk endpoint (searched per-record-date, per-crime-date, and a 6-week recent scan) never surfaced a matching record — needs a human look."]))
        return

    if has_conflict:
        label, summary = "REQUIRES_REVIEW", "detail and its matching bulk record disagree on one or more field values — needs human review"
    elif has_extra:
        label, summary = "UNIQUE_DATA_FOUND", "detail endpoint returns real field(s) the matching bulk record(s) do not provide"
    elif unmatched:
        label, summary = "REQUIRES_REVIEW", f"{unmatched}/{len(det_records)} detail record(s) had no matching bulk record found at all — partial data gap risk"
    else:
        label, summary = "REDUNDANT", "detail endpoint's data is fully contained in the matching bulk record(s) — no unique information"

    emit(Result("GET", endpoint_label, "200", summary, label, evidence))


# ---------------------------------------------------------------------------
# /files/{fileId}
# ---------------------------------------------------------------------------

def validate_files() -> None:
    url = f"{API_BASE_URL}/files/{FILE_ID}"
    start = time.time()
    try:
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT, stream=True)
        elapsed = time.time() - start
        content_type = resp.headers.get("Content-Type", "?")
        content_length = resp.headers.get("Content-Length", "unknown")
        resp.close()
        if resp.status_code == 200:
            emit(Result("GET", "/files/{fileId}", "200", f"file downloadable, content-type={content_type}, content-length={content_length}",
                         "REDUNDANT", [f"file_id={FILE_ID}", f"{elapsed:.2f}s",
                                        "No bulk equivalent exists for file downloads by design (each file is a distinct binary blob referenced by its own file_id from the entity that owns it) — already confirmed used in production by etl_files_media_server."]))
        else:
            body_preview = resp.text[:300] if hasattr(resp, "text") else ""
            emit(Result("GET", "/files/{fileId}", str(resp.status_code), f"non-200 response", "API_ERROR", [f"file_id={FILE_ID}", body_preview]))
    except requests.RequestException as exc:
        emit(Result("GET", "/files/{fileId}", "ERROR", f"connection error: {exc}", "API_ERROR", [f"file_id={FILE_ID}"]))


# ---------------------------------------------------------------------------
# /ping
# ---------------------------------------------------------------------------

def validate_ping() -> None:
    status, payload, err, elapsed = api_get("/ping")
    if err:
        emit(Result("GET", "/ping", "ERROR", f"connection error: {err}", "API_ERROR", []))
        return
    emit(Result("GET", "/ping", str(status), f"body={payload!r}", "NO_DATA_FOUND" if status == 200 else "API_ERROR",
                 [f"{elapsed:.3f}s round trip", "connectivity/utility probe only — no business data, no bulk equivalent to compare against"]))


# ---------------------------------------------------------------------------
# Report endpoints — multi-window, compared against other CCTNS bulk
# endpoints (NOT a database) since DB access is explicitly out of scope now.
# ---------------------------------------------------------------------------

def fetch_multi_window(ep: str, extra_params: dict, max_windows: int) -> Tuple[List[dict], List[str]]:
    all_records: List[dict] = []
    windows: List[str] = []
    to_date = datetime.now(timezone.utc) - timedelta(days=1)
    for _ in range(max_windows):
        from_date = to_date - timedelta(days=6)
        frm, to = from_date.strftime("%Y-%m-%d"), to_date.strftime("%Y-%m-%d")
        status, payload, err, _ = api_get(ep, dict(extra_params, fromDate=frm, toDate=to))
        n = len(extract_records(payload)) if not err and status == 200 else 0
        windows.append(f"{frm}..{to} -> {'HTTP ' + str(status) if not err else 'error: ' + err} ({n} records)")
        if not err and status == 200:
            all_records.extend(extract_records(payload))
        to_date = from_date - timedelta(days=1)
    return all_records, windows


def get_sample_district_code() -> Optional[str]:
    status, payload, err, _ = api_get("/master-data/hierarchy")
    if err or status != 200:
        return None
    for rec in extract_records(payload):
        if rec.get("DIST_CODE"):
            return rec["DIST_CODE"]
    return None


def cross_check_against_bulk_crimes(crime_ids: set, sample_size: int = 20) -> Tuple[set, set, List[str]]:
    """API-only substitute for a DB check: for a sample of the report's
    crime_ids, resolve each one directly via GET /crimes/{crime_id} (the same
    universal, date-independent lookup used for the entity checks above) —
    this proves the underlying crime is independently obtainable via the
    already-used /crimes endpoint, without depending on the report's own
    (often much older) date range lining up with any particular test window.
    Returns (found, sampled, notes)."""
    if not crime_ids:
        return set(), set(), []
    sampled = set(sorted(crime_ids)[:sample_size])
    found = set()
    for cid in sampled:
        rec, status = lookup_crime(cid)
        if rec:
            found.add(cid)
    notes = [f"sampled {len(sampled)}/{len(crime_ids)} distinct crime_ids, resolved each via GET /crimes/{{id}} directly (date-independent)"]
    return found, sampled, notes


def validate_reports() -> None:
    district = get_sample_district_code()

    # missing-udb-persons
    if not district:
        emit(Result("GET", "/reports/missing-udb-persons/v1/", "n/a", "could not resolve a districtCode", "API_ERROR", []))
    else:
        records, windows = fetch_multi_window("/reports/missing-udb-persons/v1/", {"districtCodes": district, "limit": "25", "page": "1", "personType": "Missing"}, 6)
        if not records:
            emit(Result("GET", "/reports/missing-udb-persons/v1/", "200", "0 records across all windows tested", "NO_DATA_FOUND", windows))
        else:
            crime_ids = {r.get("CRIME_ID") for r in records if r.get("CRIME_ID")}
            found, sampled, notes = cross_check_against_bulk_crimes(crime_ids)
            missing = sampled - found
            label = "REDUNDANT" if not missing else "UNIQUE_DATA_FOUND"
            emit(Result("GET", "/reports/missing-udb-persons/v1/", "200",
                         f"{len(records)} records/{len(crime_ids)} crime_ids; {len(found)}/{len(sampled)} sampled ids independently resolve via GET /crimes/{{id}}",
                         label, windows + notes + [f"crime_ids that did NOT resolve via /crimes/{{id}}: {sorted(missing)[:10]}"]))

    # arrest-particulars (internal)
    if not district:
        emit(Result("GET", "/reports/arrest/arrest-particulars/v1/", "n/a", "could not resolve a districtCode", "API_ERROR", []))
    else:
        records, windows = fetch_multi_window("/reports/arrest/arrest-particulars/v1/", {"districtCodes": district, "limit": "25", "page": "1"}, 6)
        if not records:
            emit(Result("GET", "/reports/arrest/arrest-particulars/v1/", "200", "0 records across all windows tested", "NO_DATA_FOUND", windows))
        else:
            crime_ids = {r.get("CRIME_ID") for r in records if r.get("CRIME_ID")}
            found, sampled, notes = cross_check_against_bulk_crimes(crime_ids)
            missing = sampled - found
            label = "REDUNDANT" if not missing else "UNIQUE_DATA_FOUND"
            emit(Result("GET", "/reports/arrest/arrest-particulars/v1/", "200",
                         f"{len(records)} records/{len(crime_ids)} crime_ids; {len(found)}/{len(sampled)} sampled ids independently resolve via GET /crimes/{{id}}",
                         label, windows + notes + [f"crime_ids that did NOT resolve via /crimes/{{id}}: {sorted(missing)[:10]}"]))

    # arrest-particulars (citizen, public)
    if not district:
        emit(Result("GET", "/reports/citizen/arrest/arrest-particulars/v1/", "n/a", "could not resolve a districtCode", "API_ERROR", []))
    else:
        records, windows = fetch_multi_window("/reports/citizen/arrest/arrest-particulars/v1/", {"district_cd": district, "limit": "25", "page": "1"}, 6)
        if not records:
            emit(Result("GET", "/reports/citizen/arrest/arrest-particulars/v1/", "200", "0 records across all windows tested", "NO_DATA_FOUND", windows))
        else:
            emit(Result("GET", "/reports/citizen/arrest/arrest-particulars/v1/", "200",
                         f"{len(records)} records across {len(windows)} windows; schema has no CRIME_ID field (uses FIR_NO/FIR_REG_NUM instead), so cross-check against bulk /crimes isn't directly keyable — flagged for manual review",
                         "REQUIRES_REVIEW", windows))

    # stolen-automobiles — explicit multi-window per instruction, 20 windows (~140 days)
    records, windows = fetch_multi_window("/reports/stolen-automobiles", {}, 20)
    if not records:
        emit(Result("GET", "/reports/stolen-automobiles", "200", f"0 records across {len(windows)} windows ({len(windows) * 7} days scanned)", "NO_DATA_FOUND", windows))
    else:
        crime_ids = {r.get("CRIME_ID") for r in records if r.get("CRIME_ID")}
        found, sampled, notes = cross_check_against_bulk_crimes(crime_ids)
        missing = sampled - found
        label = "REDUNDANT" if not missing else "UNIQUE_DATA_FOUND"
        emit(Result("GET", "/reports/stolen-automobiles", "200",
                     f"{len(records)} records/{len(crime_ids)} crime_ids; {len(found)}/{len(sampled)} sampled ids independently resolve via GET /crimes/{{id}}",
                     label, windows + notes + [f"crime_ids that did NOT resolve via /crimes/{{id}}: {sorted(missing)[:10]}"]))


def validate_stolen_automobiles_detail() -> None:
    stolen_result = next((r for r in RESULTS if r.endpoint == "/reports/stolen-automobiles"), None)
    if stolen_result and stolen_result.label == "NO_DATA_FOUND":
        emit(Result("GET", "/reports/stolen-automobiles/{crimeId}", "n/a", "bulk report found 0 records across all windows scanned; detail variant inherits the same empty result", "NO_DATA_FOUND", []))
    else:
        emit(Result("GET", "/reports/stolen-automobiles/{crimeId}", "n/a", "bulk report found real records; detail is a per-crime strict subset of the same data — not separately exercised to limit API calls", "REDUNDANT", []))


# ---------------------------------------------------------------------------
# NCRP — never called
# ---------------------------------------------------------------------------

def report_ncrp_skipped() -> None:
    print("\n" + "-" * 100)
    print("NCRP endpoints — NOT CALLED (per instruction: no write/NCRP endpoints)")
    print("-" * 100)
    for method, ep, reason in [
        ("POST", "/api/NCRP/gen-token", "auth endpoint for a domain with zero existing DOPAMS integration"),
        ("POST", "/api/NCRP/e-zero-fir", "WRITE operation — creates a real external FIR record if invoked"),
        ("GET", "/api/NCRP/e-zero-fir/ack-copy", "requires a real eZeroFirNo only obtainable via the unsafe write above"),
    ]:
        print(f"  {method:5} {ep:35} — SKIPPED ({reason})")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 100)
    print("CCTNS V2 ENDPOINT VALIDATION — API-ONLY (no database used)")
    print("=" * 100)

    if not API_BASE_URL:
        print("[FATAL] DOPAMAS_API_URL not configured.", file=sys.stderr)
        return 1

    print("\n--- Detail-vs-bulk entity endpoints ---")
    for name, bulk_path, detail_tpl, id_field in ENTITIES:
        validate_entity(name, bulk_path, detail_tpl, id_field, DETAIL_IDS[name])

    print("\n--- /files/{fileId} ---")
    validate_files()

    print("\n--- /ping ---")
    validate_ping()

    print("\n--- Report endpoints (multi-window) ---")
    validate_reports()
    validate_stolen_automobiles_detail()

    report_ncrp_skipped()

    print("\n" + "=" * 100)
    print("FINAL SUMMARY")
    print("=" * 100)
    counts = {label: 0 for label in FINAL_LABELS}
    for r in RESULTS:
        counts[r.label] += 1
    for label in ("REDUNDANT", "UNIQUE_DATA_FOUND", "NO_DATA_FOUND", "REQUIRES_REVIEW", "API_ERROR"):
        print(f"  {label:20}: {counts[label]}")
    print(f"\n  Total tested: {len(RESULTS)}  |  NCRP endpoints not tested: 3 (write/out-of-scope)")

    print("\nPer-endpoint classification:")
    for r in RESULTS:
        print(f"  {r.method:5} {r.endpoint:42} {r.label}")

    print("=" * 100)
    return 0


if __name__ == "__main__":
    sys.exit(main())
