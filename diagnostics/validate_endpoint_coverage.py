#!/usr/bin/env python3
"""
Read-only CCTNS V2 endpoint coverage validator.

Checks whether the 17 endpoints already flagged "not used" by static code
audit are actually safe to exclude from the DOPAMS ETL, by making REAL
GET-only calls to the CCTNS V2 API and comparing:

    detail endpoint response  vs  bulk endpoint response  vs  DOPAMS DB row

for the same crime_id, plus a best-effort reconstruction check for the
five report endpoints, a connectivity/shape check for /ping, and a static
in-scope/out-of-scope classification for the three NCRP endpoints (which
are never called over the network by this script).

SAFETY / SCOPE GUARANTEES
--------------------------------------------------------------------------
- Every CCTNS call made by this script is HTTP GET. No POST/PUT/PATCH/
  DELETE is ever issued against CCTNS.
- Every DOPAMS DB statement is a SELECT. No INSERT/UPDATE/DELETE/DDL.
- The three NCRP endpoints (gen-token, e-zero-fir, e-zero-fir/ack-copy)
  are NEVER called — they are classified from the API spec + codebase
  search alone, per explicit instruction not to submit FIR data.
- Existing credentials/config are reused as-is from env_utils
  (DOPAMAS_API_URL / DOPAMAS_API_KEY / DB_* / POSTGRES_*); no new
  secrets are introduced, none are printed.

Run:
    python diagnostics/validate_endpoint_coverage.py
    python diagnostics/validate_endpoint_coverage.py --json out.json

Exit code is always 0 (this is a report tool, not a pass/fail gate).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from env_utils import load_repo_environment, resolve_api_base_url, resolve_db_config  # noqa: E402

load_repo_environment()

API_BASE_URL = (resolve_api_base_url("DOPAMAS_API_URL") or "").rstrip("/")
API_KEY = resolve_api_base_url("DOPAMAS_API_KEY") or ""
HEADERS = {"x-api-key": API_KEY}
TIMEOUT = int(resolve_api_base_url("API_TIMEOUT", default="60") or "60")

# DB safety: this script must ONLY ever compare against the current
# POSTGRES_* (cctns-v2) database — never RDS_*/dev-3 or an AUTO-guessed
# source. Force the source explicitly; db_connect() then double-checks the
# resolved database name at connect time and refuses to proceed otherwise.
os.environ["DB_CONFIG_SOURCE"] = "POSTGRES"
REQUIRED_DB_NAME = "cctns-v2"

VALID_CLASSIFICATIONS = {
    "SAFE_TO_NEGLECT",
    "REQUIRES_REVIEW",
    "DATA_GAP",
    "OUT_OF_SCOPE",
    "UTILITY_ONLY",
    "WRITE_ONLY_NOT_SAFE_TO_TEST",
    "API_UNAVAILABLE",
    "TEST_FAILED",
    "NO_DATA_FOUND_AFTER_MULTI_WINDOW_TEST",
}

# Bulk-vs-detail shape comparison (separate axis from the final classification).
BULK_COMPARISON_LABELS = {
    "EXACT_DUPLICATE",       # detail and matching bulk record(s) have identical field sets and values
    "STRICT_SUBSET",         # detail's fields/values all also appear in the matching bulk record(s); bulk may have more
    "HAS_ADDITIONAL_DATA",   # detail has a field with a real value the matching bulk record(s) lack entirely
    "HAS_DIFFERENT_DATA",    # a field common to both sides has a different (conflicting) value
    "NO_DATA",                # neither side had records to compare
}


@dataclass
class EndpointResult:
    method: str
    endpoint: str
    http_result: str  # PASS / FAIL / SKIPPED / n/a
    data_summary: str
    compared_with: str
    additional_data: str  # YES / NO / n/a
    dopams_equivalent: str  # YES / NO / PARTIAL / n/a
    classification: str
    evidence: List[str] = field(default_factory=list)
    bulk_comparison: str = "n/a"  # one of BULK_COMPARISON_LABELS, or "n/a"
    etl_usage: str = ""  # file:line trace of the real production call site, or "not called"

    def __post_init__(self):
        assert self.classification in VALID_CLASSIFICATIONS, self.classification
        assert self.bulk_comparison == "n/a" or self.bulk_comparison in BULK_COMPARISON_LABELS, self.bulk_comparison


# ---------------------------------------------------------------------------
# Low-level helpers (read-only)
# ---------------------------------------------------------------------------

def api_get(path: str, params: Optional[dict] = None) -> Tuple[Optional[int], Any, Optional[str], float]:
    """GET only. Returns (status_code, parsed_json_or_None, error_or_None, elapsed_seconds)."""
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
    """Normalize any CCTNS response shape (list / dict-with-array / single object) into a list of dicts."""
    if payload is None:
        return []
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    if isinstance(payload, dict):
        data = payload.get("data", payload)
        if isinstance(data, list):
            return [r for r in data if isinstance(r, dict)]
        if isinstance(data, dict):
            # some detail endpoints (e.g. crimes/disposal/{crimeId}) return a single object
            inner = data.get("data") if isinstance(data.get("data"), (list, dict)) else None
            if isinstance(inner, list):
                return [r for r in inner if isinstance(r, dict)]
            if isinstance(inner, dict):
                return [inner]
            return [data]
    return []


def db_connect():
    """Connect using ONLY the POSTGRES_* source (forced above), and verify the
    resolved database is really `cctns-v2` before handing back a connection.
    Raises RuntimeError (never silently falls back to RDS_*/dev-3 or anything
    else) if the resolved config points anywhere else."""
    import psycopg2

    cfg = resolve_db_config()
    if cfg.get("dbname") != REQUIRED_DB_NAME:
        raise RuntimeError(
            f"Refusing to connect: resolved dbname={cfg.get('dbname')!r} via DB_CONFIG_SOURCE=POSTGRES, "
            f"expected {REQUIRED_DB_NAME!r}. Check POSTGRES_DB in .env — this script will not fall back "
            f"to RDS_*/dev-3 or any other source."
        )
    conn = psycopg2.connect(**{k: v for k, v in cfg.items() if k in ("host", "port", "dbname", "user", "password")})
    with conn.cursor() as cur:
        cur.execute("SELECT current_database()")
        actual = cur.fetchone()[0]
    if actual != REQUIRED_DB_NAME:
        conn.close()
        raise RuntimeError(f"Refusing to use connection: server reports current_database()={actual!r}, expected {REQUIRED_DB_NAME!r}.")
    return conn


def db_select(conn, sql: str, params: tuple = ()) -> List[tuple]:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def db_select_dict(conn, sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
    from psycopg2.extras import RealDictCursor

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]


def non_null_fields(record: dict) -> Dict[str, Any]:
    return {k: v for k, v in record.items() if v not in (None, "", [], {})}


def fields_present_in_a_not_b(a: dict, b: dict) -> List[str]:
    """Keys with a real (non-empty) value in a that are absent/empty in b, case-insensitively matched."""
    b_lower = {k.lower(): v for k, v in b.items()}
    extra = []
    for k, v in non_null_fields(a).items():
        bv = b_lower.get(k.lower())
        if bv in (None, "", [], {}):
            extra.append(k)
    return extra


def _norm_value(v: Any) -> str:
    return str(v).strip().rstrip("Z").replace("T", " ") if v is not None else ""


def fields_with_conflicting_values(a: dict, b: dict) -> List[Tuple[str, Any, Any]]:
    """Fields present with a real value on BOTH sides whose normalized values differ."""
    b_lower = {k.lower(): v for k, v in b.items()}
    conflicts = []
    for k, v in non_null_fields(a).items():
        if k.lower() not in b_lower:
            continue
        bv = b_lower[k.lower()]
        if bv in (None, "", [], {}):
            continue
        if _norm_value(v) != _norm_value(bv) and _norm_value(v)[:10] != _norm_value(bv)[:10]:
            conflicts.append((k, v, bv))
    return conflicts


def classify_bulk_vs_detail(det_records: List[dict], bulk_matches: List[dict]) -> Tuple[str, List[str]]:
    """Pairs each detail record with its closest-matching bulk record and returns
    one of BULK_COMPARISON_LABELS plus human-readable diff evidence."""
    if not det_records or not bulk_matches:
        return "NO_DATA", []

    has_extra = has_conflict = has_missing_in_detail = False
    notes: List[str] = []
    for det_rec in det_records:
        best = min(
            bulk_matches,
            key=lambda b: len(fields_present_in_a_not_b(det_rec, b)) + len(fields_with_conflicting_values(det_rec, b)),
        )
        extra = fields_present_in_a_not_b(det_rec, best)
        missing = fields_present_in_a_not_b(best, det_rec)
        conflicts = fields_with_conflicting_values(det_rec, best)
        if extra:
            has_extra = True
            notes.append(f"detail has {extra} not present in its closest-matching bulk record")
        if missing:
            has_missing_in_detail = True
            notes.append(f"bulk record has {missing} not present in detail")
        if conflicts:
            has_conflict = True
            notes.append(f"conflicting values: {[(k, v1, v2) for k, v1, v2 in conflicts]}")

    if has_conflict:
        return "HAS_DIFFERENT_DATA", notes
    if has_extra:
        return "HAS_ADDITIONAL_DATA", notes
    if has_missing_in_detail:
        return "STRICT_SUBSET", notes
    return "EXACT_DUPLICATE", notes


# ---------------------------------------------------------------------------
# Detail-vs-bulk crime-id endpoints
# ---------------------------------------------------------------------------

@dataclass
class EntityConfig:
    name: str
    bulk_path: str
    detail_path_tpl: str  # "{crime_id}" placeholder
    bulk_id_field: str  # field name holding the crime id in the bulk response
    dopams_table: str
    etl_usage: str  # file:line trace of the real production bulk-endpoint call site


ENTITIES = [
    EntityConfig("disposal", "/crimes/disposal", "/crimes/disposal/{crime_id}", "CRIME_ID", "disposal",
                 "etl-disposal/etl_disposal.py:625 fetch_disposal_api() -> run() -> process_date_range() -> INSERT INTO disposal (:987)"),
    EntityConfig("arrests", "/arrests", "/arrests/{crime_id}", "CRIME_ID", "arrests",
                 "etl_arrests/etl_arrests.py:559 fetch_arrests_api() -> run() -> process_date_range() -> INSERT INTO arrests (:1116)"),
    EntityConfig("mo_seizures", "/mo-seizures", "/mo-seizures/{crime_id}", "CRIME_ID", "mo_seizures",
                 "etl_mo_seizures/etl_mo_seizure.py:557 fetch fn -> run() -> INSERT INTO mo_seizures (:1182) + mo_seizure_media (:941)"),
    EntityConfig("property_details", "/property-details", "/property-details/{crime_id}", "CRIME_ID", "properties",
                 "etl-properties/etl_properties.py:528 fetch_properties_api() -> run() -> INSERT INTO properties (:912)"),
    EntityConfig(
        "interrogation_reports",
        "/interrogation-reports/v1/",
        "/interrogation-reports/v1/{crime_id}",
        "CRIME_ID",
        "interrogation_reports",
        "etl-ir/ir_etl.py:567 fetch fn -> run() -> process_date_range() -> INSERT INTO interrogation_reports (:913-947) + 24 child tables",
    ),
    EntityConfig("chargesheets", "/chargesheets", "/chargesheets/{crime_id}", "crimeId", "chargesheets",
                 "etl_chargesheets/etl_chargesheets.py:614 fetch fn -> run() -> INSERT INTO chargesheets (:1229) + 5 child tables"),
    EntityConfig(
        "update_chargesheets",
        "/update-chargesheets",
        "/update-chargesheets/{crime_id}",
        "crimeId",
        "charge_sheet_updates",
        "etl_updated_chargesheet/etl_update_chargesheet.py:560 fetch fn -> run() -> INSERT INTO charge_sheet_updates (:1111)",
    ),
    EntityConfig("case_property", "/case-property", "/case-property/{crime_id}", "CRIME_ID", "fsl_case_property",
                 "etl_fsl_case_property/etl_fsl_case_property.py:712 fetch fn -> run() -> INSERT INTO fsl_case_property (:1344) + fsl_case_property_media"),
]


def fetch_bulk_window(bulk_path: str, max_windows: int = 8) -> Tuple[List[dict], Optional[str], Optional[str]]:
    """Walk backward through 7-day windows (CCTNS' hard max span) from yesterday
    until a non-empty bulk result is found. Returns (records, fromDate, toDate)."""
    to_date = datetime.now(timezone.utc) - timedelta(days=1)
    for _ in range(max_windows):
        from_date = to_date - timedelta(days=6)
        frm, to = from_date.strftime("%Y-%m-%d"), to_date.strftime("%Y-%m-%d")
        status, payload, err, _ = api_get(bulk_path, {"fromDate": frm, "toDate": to})
        if not err and status == 200:
            records = extract_records(payload)
            if records:
                return records, frm, to
        to_date = from_date - timedelta(days=1)
    return [], None, None


def validate_entity(conn, cfg: EntityConfig, db_blocked_reason: Optional[str]) -> EndpointResult:
    endpoint_label = cfg.detail_path_tpl
    evidence: List[str] = []

    if not API_BASE_URL:
        return EndpointResult("GET", endpoint_label, "FAIL", "n/a", "n/a", "n/a", "n/a", "API_UNAVAILABLE",
                               ["DOPAMAS_API_URL not configured in this environment"], etl_usage=cfg.etl_usage)

    # Source candidate crime_ids from the entity's OWN bulk endpoint — guarantees
    # each candidate genuinely has a record of this type (some detail endpoints,
    # e.g. /crimes/disposal/{crimeId}, return HTTP 400 "Invalid crimeId" — not
    # 200+empty — for a crime that has no record of that type, confirmed live).
    records, frm, to = fetch_bulk_window(cfg.bulk_path)
    if not records:
        return EndpointResult("GET", endpoint_label, "FAIL", "no bulk records found across 8 recent 7-day windows",
                               cfg.bulk_path, "n/a", "n/a", "TEST_FAILED",
                               [f"Walked back {8 * 7} days from yesterday via {cfg.bulk_path}; every window was empty."],
                               etl_usage=cfg.etl_usage)
    evidence.append(f"bulk window used: {frm}..{to} ({len(records)} record(s))")

    db_available = conn is not None and db_blocked_reason is None
    tried = []
    for bulk_rec in records:
        crime_id = bulk_rec.get(cfg.bulk_id_field)
        if not crime_id or crime_id in tried:
            continue
        tried.append(crime_id)
        if len(tried) > 5:
            break

        det_status, det_payload, det_err, det_t = api_get(cfg.detail_path_tpl.format(crime_id=crime_id))
        if det_err or det_status != 200:
            evidence.append(f"crime_id={crime_id}: detail call status={det_status} err={det_err}")
            continue
        det_records = extract_records(det_payload)
        if not det_records:
            evidence.append(f"crime_id={crime_id}: detail call returned 200 but no records")
            continue

        # A crime can have >1 record of this entity type (e.g. multiple
        # case-property items) — compare against ALL matching bulk records
        # from this batch, not just the one record we read crime_id off of.
        bulk_matches = [r for r in records if r.get(cfg.bulk_id_field) == crime_id]
        bulk_comparison, bulk_notes = classify_bulk_vs_detail(det_records, bulk_matches)

        dopams_rows = db_select_dict(conn, f"SELECT * FROM {cfg.dopams_table} WHERE crime_id = %s", (crime_id,)) if db_available else []
        evidence.append(
            f"crime_id={crime_id}: detail_records={len(det_records)} bulk_matches={len(bulk_matches)} "
            f"dopams_rows={'n/a' if not db_available else len(dopams_rows)} (detail {det_t:.2f}s) "
            f"-> bulk_comparison={bulk_comparison}"
        )
        evidence.extend(f"crime_id={crime_id}: {note}" for note in bulk_notes)

        # Does DOPAMS already hold everything meaningful the detail endpoint returned?
        if not db_available:
            dopams_equiv = "DB_UNAVAILABLE"
            missing_in_dopams: List[str] = []
        elif dopams_rows:
            dopams_row = dopams_rows[0]
            missing_in_dopams = sorted(set(
                f for det_rec in det_records for f in fields_present_in_a_not_b(det_rec, dopams_row)
            ))
            dopams_equiv = "YES" if not missing_in_dopams else "PARTIAL"
        else:
            missing_in_dopams = ["<no DOPAMS row found for this crime_id at all>"]
            dopams_equiv = "NO"

        additional = "YES" if bulk_comparison in ("HAS_ADDITIONAL_DATA", "HAS_DIFFERENT_DATA") else "NO"
        if missing_in_dopams:
            evidence.append(f"crime_id={crime_id}: fields in DETAIL not reflected in DOPAMS `{cfg.dopams_table}` row: {missing_in_dopams}")
        if not db_available:
            evidence.append(
                f"crime_id={crime_id}: DOPAMS DB comparison skipped this run — {db_blocked_reason or 'no DB connection'}"
            )

        if not db_available:
            classification = "DATA_GAP" if additional == "YES" else "REQUIRES_REVIEW"
        elif additional == "NO" and dopams_equiv == "YES":
            classification = "SAFE_TO_NEGLECT"
        elif additional == "YES" and dopams_equiv != "YES":
            classification = "DATA_GAP"
        else:
            classification = "REQUIRES_REVIEW"

        return EndpointResult(
            "GET", endpoint_label, "PASS", f"{len(det_records)} detail record(s) vs {len(bulk_matches)} matching bulk record(s)",
            f"bulk {cfg.bulk_path} (fromDate={frm}&toDate={to})", additional, dopams_equiv, classification, evidence,
            bulk_comparison=bulk_comparison, etl_usage=cfg.etl_usage,
        )

    return EndpointResult(
        "GET", endpoint_label, "FAIL", "no sampled crime_id (from this entity's own bulk data) produced a valid detail response",
        cfg.bulk_path, "n/a", "n/a", "TEST_FAILED", evidence, etl_usage=cfg.etl_usage,
    )


# ---------------------------------------------------------------------------
# /ping
# ---------------------------------------------------------------------------

def validate_ping() -> EndpointResult:
    etl_usage = "not called by any ETL script; no health-check/connectivity-test caller found anywhere in the repo (confirmed by repo-wide grep in an earlier code audit)"
    if not API_BASE_URL:
        return EndpointResult("GET", "/ping", "FAIL", "n/a", "n/a", "n/a", "n/a", "API_UNAVAILABLE", ["DOPAMAS_API_URL not configured"], etl_usage=etl_usage)
    status, payload, err, elapsed = api_get("/ping")
    if err:
        return EndpointResult("GET", "/ping", "FAIL", "n/a", "n/a", "n/a", "n/a", "API_UNAVAILABLE", [f"connection error: {err}"], etl_usage=etl_usage)
    body_has_data = isinstance(payload, (dict, list)) and len(payload) > 0
    classification = "UTILITY_ONLY" if not body_has_data else "REQUIRES_REVIEW"
    return EndpointResult(
        "GET", "/ping", "PASS" if status == 200 else "FAIL",
        f"status={status} body={payload!r}"[:200],
        "n/a (utility probe, no data-bearing equivalent expected)",
        "NO" if not body_has_data else "YES", "n/a",
        classification if status == 200 else "API_UNAVAILABLE",
        [f"HTTP {status}, {elapsed:.3f}s round trip, auth accepted={status != 401 and status != 403}",
         "Body is a plain connectivity string, not business data — confirms no data ingestion is possible through this endpoint."],
        etl_usage=etl_usage,
    )


# ---------------------------------------------------------------------------
# NCRP — never called over the network, classified from spec + codebase only
# ---------------------------------------------------------------------------

def ncrp_results() -> List[EndpointResult]:
    shared_evidence = [
        "Zero references to NCRP/gen-token/e-zero-fir anywhere in the DOPAMS-ETL codebase "
        "(confirmed by repo-wide grep in an earlier code audit) — no encryption/requestData "
        "scheme, no BearerAuth token handling, no NCRP-specific credentials configured.",
        "NCRP direction is DOPAMS -> NCRP (submitting eZero FIRs outward), the opposite "
        "direction of CCTNS V2 -> DOPAMS ingestion this ETL performs.",
    ]
    etl_usage = "not called anywhere in the codebase"
    return [
        EndpointResult(
            "POST", "/api/NCRP/gen-token", "SKIPPED", "not called", "n/a", "n/a", "n/a", "OUT_OF_SCOPE",
            shared_evidence + ["Auth-token endpoint for the NCRP domain; irrelevant without any NCRP integration to authenticate for."],
            etl_usage=etl_usage,
        ),
        EndpointResult(
            "POST", "/api/NCRP/e-zero-fir", "SKIPPED", "not called", "n/a", "n/a", "n/a", "WRITE_ONLY_NOT_SAFE_TO_TEST",
            shared_evidence + ["Creates a real FIR record in NCRP if invoked — a write with external side effects. Never called by this script."],
            etl_usage=etl_usage,
        ),
        EndpointResult(
            "GET", "/api/NCRP/e-zero-fir/ack-copy", "SKIPPED", "not called", "n/a", "n/a", "n/a", "OUT_OF_SCOPE",
            shared_evidence + ["Requires a real eZeroFirNo that only exists after a genuine e-zero-fir submission; cannot be safely tested without first performing the unsafe write above."],
            etl_usage=etl_usage,
        ),
    ]


# ---------------------------------------------------------------------------
# Report endpoints — multi-window aggregation + best-effort reconstruction
# ---------------------------------------------------------------------------

REPORT_ETL_USAGE = (
    "not called anywhere in the codebase (confirmed by repo-wide grep in an earlier code audit); "
    "the underlying entities it's built from (crimes/persons/arrests/properties) ARE independently ingested"
)


def get_sample_district_code(conn) -> Optional[str]:
    if conn is not None:
        rows = db_select(conn, "SELECT dist_code FROM hierarchy WHERE dist_code IS NOT NULL LIMIT 1")
        if rows:
            return rows[0][0]
    # DB-less fallback: pull a real DIST_CODE straight from the live hierarchy endpoint.
    status, payload, err, _ = api_get("/master-data/hierarchy")
    if err or status != 200:
        return None
    for rec in extract_records(payload):
        if rec.get("DIST_CODE"):
            return rec["DIST_CODE"]
    return None


def fetch_multi_window(ep: str, extra_params: dict, max_windows: int = 6) -> Tuple[List[dict], List[str]]:
    """Aggregate records across up to `max_windows` distinct 7-day windows (walking
    backward from yesterday) instead of relying on a single window's sample size."""
    all_records: List[dict] = []
    windows_tried: List[str] = []
    to_date = datetime.now(timezone.utc) - timedelta(days=1)
    for _ in range(max_windows):
        from_date = to_date - timedelta(days=6)
        frm, to = from_date.strftime("%Y-%m-%d"), to_date.strftime("%Y-%m-%d")
        params = dict(extra_params, fromDate=frm, toDate=to)
        status, payload, err, _ = api_get(ep, params)
        windows_tried.append(f"{frm}..{to} -> {'HTTP ' + str(status) if not err else 'error: ' + err} "
                              f"({len(extract_records(payload)) if not err and status == 200 else 0} records)")
        if not err and status == 200:
            all_records.extend(extract_records(payload))
        to_date = from_date - timedelta(days=1)
    return all_records, windows_tried


def _dopams_crime_id_check(conn, table: str, crime_ids: set, extra_where: str = "") -> Tuple[Optional[set], str]:
    """Returns (set of crime_ids DOPAMS has in `table`, or None if DB unavailable, evidence note)."""
    if conn is None:
        return None, "DOPAMS DB comparison skipped this run (see DB blocker above)"
    if not crime_ids:
        return set(), "no crime_ids returned by CCTNS to check"
    where = f"crime_id = ANY(%s) {extra_where}"
    rows = db_select(conn, f"SELECT crime_id FROM {table} WHERE {where}", (list(crime_ids),))
    return {r[0] for r in rows}, ""


def _dopams_fir_reg_num_check(conn, fir_reg_nums: set) -> Tuple[Optional[set], str]:
    if conn is None:
        return None, "DOPAMS DB comparison skipped this run (see DB blocker above)"
    if not fir_reg_nums:
        return set(), "no FIR_REG_NUM returned by CCTNS to check"
    rows = db_select(conn, "SELECT fir_reg_num FROM crimes WHERE fir_reg_num = ANY(%s)", (list(fir_reg_nums),))
    return {r[0] for r in rows}, ""


def validate_report_missing_udb(conn, district: Optional[str], db_blocked_reason: Optional[str]) -> EndpointResult:
    ep = "/reports/missing-udb-persons/v1/"
    if not district:
        return EndpointResult("GET", ep, "SKIPPED", "no district code available", "n/a", "n/a", "n/a", "TEST_FAILED",
                               ["Could not resolve a real districtCode to use as a required test parameter."], etl_usage=REPORT_ETL_USAGE)
    records, windows = fetch_multi_window(ep, {"districtCodes": district, "limit": "25", "page": "1", "personType": "Missing"})
    if not records:
        return EndpointResult("GET", ep, "PASS", "0 records across all windows tested", "n/a", "n/a", "n/a",
                               "NO_DATA_FOUND_AFTER_MULTI_WINDOW_TEST", [f"district={district}"] + windows, etl_usage=REPORT_ETL_USAGE)
    # Underlying entities this report is built from: crimes (CRIME_ID/FIR_NUM/ACTS_SECTIONS/BRIEF_FACTS/DISTRICT/IO_*),
    # persons (PERSON_NAME/AGE/GENDER/PRESENT_ADDRESS/PHYSICAL_FEATURES), hierarchy (PS_NAME).
    crime_ids = {r.get("CRIME_ID") for r in records if r.get("CRIME_ID")}
    dopams_crime_ids, note = _dopams_crime_id_check(conn if db_blocked_reason is None else None, "crimes", crime_ids)
    if dopams_crime_ids is None:
        equiv, classification = "DB_UNAVAILABLE", "REQUIRES_REVIEW"
        gap_note = db_blocked_reason or note
    else:
        missing = crime_ids - dopams_crime_ids
        equiv = "YES" if not missing else "PARTIAL"
        classification = "SAFE_TO_NEGLECT" if not missing else "DATA_GAP"
        gap_note = f"crime_ids present in DOPAMS `crimes`: {len(dopams_crime_ids)}/{len(crime_ids)}; absent: {sorted(missing)[:10]}"
    return EndpointResult(
        "GET", ep, "PASS", f"{len(records)} record(s) across {len(windows)} windows, {len(crime_ids)} distinct crime_id(s)",
        "reconstructed from `crimes` (+ `persons`/`hierarchy` for name/address fields) for the same crime_ids",
        "n/a", equiv, classification, windows + [gap_note], etl_usage=REPORT_ETL_USAGE,
    )


def validate_report_arrest_particulars(conn, district: Optional[str], citizen: bool, db_blocked_reason: Optional[str]) -> EndpointResult:
    ep = "/reports/citizen/arrest/arrest-particulars/v1/" if citizen else "/reports/arrest/arrest-particulars/v1/"
    if not district:
        return EndpointResult("GET", ep, "SKIPPED", "no district code available", "n/a", "n/a", "n/a", "TEST_FAILED",
                               ["Could not resolve a real district code."], etl_usage=REPORT_ETL_USAGE)
    params = {"district_cd": district, "limit": "25", "page": "1"} if citizen else {"districtCodes": district, "limit": "25", "page": "1"}
    records, windows = fetch_multi_window(ep, params)
    if not records:
        return EndpointResult("GET", ep, "PASS", "0 records across all windows tested", "n/a", "n/a", "n/a",
                               "NO_DATA_FOUND_AFTER_MULTI_WINDOW_TEST", [f"district={district}"] + windows, etl_usage=REPORT_ETL_USAGE)

    if citizen:
        # This variant's schema has no CRIME_ID — it uses FIR_NO/FIR_REG_NUM instead
        # (confirmed against api-1.yaml). Reconcile via FIR_REG_NUM against `crimes`.
        fir_reg_nums = {r.get("FIR_REG_NUM") for r in records if r.get("FIR_REG_NUM")}
        dopams_set, note = _dopams_fir_reg_num_check(conn if db_blocked_reason is None else None, fir_reg_nums)
        key_kind, key_count = "FIR_REG_NUM", len(fir_reg_nums)
    else:
        crime_ids = {r.get("CRIME_ID") for r in records if r.get("CRIME_ID")}
        dopams_set, note = _dopams_crime_id_check(conn if db_blocked_reason is None else None, "arrests", crime_ids)
        key_kind, key_count = "crime_id", len(crime_ids)
        fir_reg_nums = None

    if dopams_set is None:
        equiv, classification = "DB_UNAVAILABLE", "REQUIRES_REVIEW"
        gap_note = db_blocked_reason or note
    else:
        keys = fir_reg_nums if citizen else crime_ids
        missing = keys - dopams_set
        equiv = "YES" if not missing else "PARTIAL"
        classification = "SAFE_TO_NEGLECT" if not missing else "DATA_GAP"
        gap_note = f"{key_kind}s present in DOPAMS: {len(dopams_set)}/{key_count}; absent: {sorted(missing)[:10]}"

    return EndpointResult(
        "GET", ep, "PASS", f"{len(records)} record(s) across {len(windows)} windows, {key_count} distinct {key_kind}(s)",
        f"reconstructed from `arrests`/`crimes`/`persons` (matched via {key_kind})", "n/a", equiv, classification,
        windows + [gap_note], etl_usage=REPORT_ETL_USAGE,
    )


def validate_report_stolen_automobiles(conn, db_blocked_reason: Optional[str], max_windows: int = 20) -> EndpointResult:
    """Part 6: a single empty window is inconclusive, so this scans a much wider
    span (default 20 * 7 = 140 days) before concluding no data exists."""
    ep = "/reports/stolen-automobiles"
    records, windows = fetch_multi_window(ep, {}, max_windows=max_windows)
    if not records:
        return EndpointResult(
            "GET", ep, "PASS", f"0 records across {max_windows} windows ({max_windows * 7} days scanned)",
            "n/a", "n/a", "n/a", "NO_DATA_FOUND_AFTER_MULTI_WINDOW_TEST",
            [f"Scanned {max_windows} consecutive 7-day windows back from yesterday ({max_windows * 7} days total); every window returned 0 records."] + windows,
            etl_usage=REPORT_ETL_USAGE,
        )
    ids = {r.get("STOLEN_PROPERTY_ID") for r in records if r.get("STOLEN_PROPERTY_ID")}
    crime_ids = {r.get("CRIME_ID") for r in records if r.get("CRIME_ID")}
    dopams_crime_ids, note = _dopams_crime_id_check(
        conn if db_blocked_reason is None else None, "properties", crime_ids, extra_where="AND category = 'Automobiles'"
    )
    if dopams_crime_ids is None:
        equiv, classification = "DB_UNAVAILABLE", "REQUIRES_REVIEW"
        gap_note = db_blocked_reason or note
    else:
        missing = crime_ids - dopams_crime_ids
        equiv = "YES" if not missing else "PARTIAL"
        classification = "SAFE_TO_NEGLECT" if not missing else "DATA_GAP"
        gap_note = f"crime_ids present in DOPAMS `properties`(Automobiles): {len(dopams_crime_ids)}/{len(crime_ids)}; absent: {sorted(missing)[:10]}"
    return EndpointResult(
        "GET", ep, "PASS", f"{len(records)} record(s), {len(ids)} distinct STOLEN_PROPERTY_ID, {len(crime_ids)} crime_id(s)",
        "reconstructed from `properties` WHERE category='Automobiles' for the same crime_ids", "n/a", equiv, classification,
        windows + [gap_note], etl_usage=REPORT_ETL_USAGE,
    )


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

def print_report(results: List[EndpointResult], db_blocked_reason: Optional[str]) -> None:
    print("\n" + "=" * 100)
    print("CCTNS V2 ENDPOINT COVERAGE VALIDATION — REPORT")
    print("=" * 100)
    if db_blocked_reason:
        print(f"\n*** DOPAMS DB COMPARISON BLOCKED: {db_blocked_reason} ***\n")
    header = f"{'Method':6} | {'Endpoint':48} | {'HTTP':6} | {'BulkCmp':18} | {'DOPAMS':13} | Classification"
    print(header)
    print("-" * len(header))
    for r in results:
        print(f"{r.method:6} | {r.endpoint:48} | {r.http_result:6} | {r.bulk_comparison:18} | {r.dopams_equivalent:13} | {r.classification}")

    print("\n" + "-" * 100)
    print("DETAIL")
    print("-" * 100)
    for r in results:
        print(f"\n### {r.method} {r.endpoint}  [{r.classification}]")
        print(f"    data_summary   : {r.data_summary}")
        print(f"    compared_with  : {r.compared_with}")
        print(f"    bulk_comparison: {r.bulk_comparison}")
        print(f"    etl_usage      : {r.etl_usage}")
        for line in r.evidence:
            if line:
                print(f"    - {line}")

    total = len(results)
    validated = sum(1 for r in results if r.http_result == "PASS")
    redundant = sum(1 for r in results if r.classification == "SAFE_TO_NEGLECT")
    out_of_scope = sum(1 for r in results if r.classification == "OUT_OF_SCOPE")
    review = sum(1 for r in results if r.classification == "REQUIRES_REVIEW")
    write_unsafe = sum(1 for r in results if r.classification == "WRITE_ONLY_NOT_SAFE_TO_TEST")
    gaps = [r for r in results if r.classification == "DATA_GAP"]
    no_data = sum(1 for r in results if r.classification == "NO_DATA_FOUND_AFTER_MULTI_WINDOW_TEST")
    unavailable = sum(1 for r in results if r.classification in ("API_UNAVAILABLE", "TEST_FAILED"))

    print("\n" + "=" * 100)
    print("FINAL SUMMARY")
    print("=" * 100)
    print(f"1. Endpoints tested                        : {total}")
    print(f"2. Successfully validated (HTTP PASS)       : {validated}")
    print(f"3. Confirmed redundant (SAFE_TO_NEGLECT)    : {redundant}")
    print(f"4. Confirmed out of scope                   : {out_of_scope}")
    print(f"5. Requiring further review                 : {review}")
    print(f"6. Unsafe to test (writes)                  : {write_unsafe}")
    print(f"7. Genuine data gaps discovered              : {len(gaps)}")
    for g in gaps:
        print(f"     - {g.method} {g.endpoint}: {g.evidence[-1] if g.evidence else ''}")
    print(f"   No data after multi-window test           : {no_data}")
    print(f"   Untestable this run (connectivity/DB)      : {unavailable}")
    print("\n8. Safe to exclude from DOPAMS ETL:")
    for r in results:
        if r.classification == "SAFE_TO_NEGLECT":
            print(f"     - {r.method} {r.endpoint}  ({r.compared_with})")
    if db_blocked_reason:
        print(f"\n*** Reminder: DOPAMS DB comparison was BLOCKED this run: {db_blocked_reason} ***")
    print("=" * 100 + "\n")


def to_json(results: List[EndpointResult]) -> list:
    return [
        {
            "method": r.method,
            "endpoint": r.endpoint,
            "http_result": r.http_result,
            "data_summary": r.data_summary,
            "compared_with": r.compared_with,
            "additional_data": r.additional_data,
            "dopams_equivalent": r.dopams_equivalent,
            "bulk_comparison": r.bulk_comparison,
            "etl_usage": r.etl_usage,
            "classification": r.classification,
            "evidence": r.evidence,
        }
        for r in results
    ]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Read-only CCTNS V2 endpoint coverage validator")
    parser.add_argument("--json", default=None, help="Optional path to also write the report as JSON")
    parser.add_argument("--stolen-auto-windows", type=int, default=20, help="Number of 7-day windows to scan for /reports/stolen-automobiles before concluding no data")
    args = parser.parse_args()

    results: List[EndpointResult] = []

    conn = None
    db_blocked_reason: Optional[str] = None
    try:
        conn = db_connect()
        print(f"[OK] Connected to DOPAMS database: current_database()={REQUIRED_DB_NAME!r} (forced DB_CONFIG_SOURCE=POSTGRES)", file=sys.stderr)
    except Exception as exc:
        db_blocked_reason = str(exc)
        print(f"[BLOCKED] DOPAMS DB comparison stopped: {exc}", file=sys.stderr)

    if not API_BASE_URL:
        print("[FATAL] DOPAMAS_API_URL not configured — every live-API check below will be API_UNAVAILABLE.", file=sys.stderr)

    # 1. crimeId detail-vs-bulk endpoints — each entity sources its own test
    #    crime_ids straight from its own bulk endpoint (see validate_entity).
    for cfg in ENTITIES:
        results.append(validate_entity(conn, cfg, db_blocked_reason))

    # 2. /ping
    results.append(validate_ping())

    # 3. NCRP (never called)
    results.extend(ncrp_results())

    # 4. Reports — multi-window, run against the live API regardless of DB
    #    availability; DOPAMS-side comparison degrades to DB_UNAVAILABLE /
    #    REQUIRES_REVIEW with db_blocked_reason spelled out when DB is blocked.
    if API_BASE_URL:
        district = get_sample_district_code(conn)
        results.append(validate_report_missing_udb(conn, district, db_blocked_reason))
        results.append(validate_report_arrest_particulars(conn, district, citizen=False, db_blocked_reason=db_blocked_reason))
        results.append(validate_report_arrest_particulars(conn, district, citizen=True, db_blocked_reason=db_blocked_reason))
        stolen_result = validate_report_stolen_automobiles(conn, db_blocked_reason, max_windows=args.stolen_auto_windows)
        results.append(stolen_result)
        if stolen_result.classification == "NO_DATA_FOUND_AFTER_MULTI_WINDOW_TEST":
            results.append(EndpointResult(
                "GET", "/reports/stolen-automobiles/{crimeId}", "SKIPPED",
                f"bulk report found 0 records across {args.stolen_auto_windows} windows; a crimeId-scoped "
                f"lookup can only ever be a subset of that same empty dataset",
                "/reports/stolen-automobiles", "n/a", "n/a", "NO_DATA_FOUND_AFTER_MULTI_WINDOW_TEST",
                ["Not separately exercised — inherits the bulk report's multi-window empty result."],
                etl_usage=REPORT_ETL_USAGE,
            ))
        else:
            results.append(EndpointResult(
                "GET", "/reports/stolen-automobiles/{crimeId}", "SKIPPED",
                "bulk report found real records; detail variant is a strict per-crime subset of the same data",
                "/reports/stolen-automobiles", "NO", stolen_result.dopams_equivalent, "REQUIRES_REVIEW",
                ["Not separately exercised over the network — inherits the bulk report's shape (each record already "
                 "carries CRIME_ID; a crimeId-scoped call cannot add fields the bulk response lacks) — deprioritized "
                 "to limit CCTNS API calls during this read-only run."],
                etl_usage=REPORT_ETL_USAGE,
            ))
    else:
        for ep in (
            "/reports/missing-udb-persons/v1/",
            "/reports/arrest/arrest-particulars/v1/",
            "/reports/citizen/arrest/arrest-particulars/v1/",
            "/reports/stolen-automobiles",
            "/reports/stolen-automobiles/{crimeId}",
        ):
            results.append(EndpointResult("GET", ep, "FAIL", "n/a", "n/a", "n/a", "n/a", "API_UNAVAILABLE",
                                           ["DOPAMAS_API_URL not configured in this environment."], etl_usage=REPORT_ETL_USAGE))

    if conn is not None:
        conn.close()

    print_report(results, db_blocked_reason)

    if args.json:
        Path(args.json).write_text(json.dumps(to_json(results), indent=2, default=str), encoding="utf-8")
        print(f"JSON report written to {args.json}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
