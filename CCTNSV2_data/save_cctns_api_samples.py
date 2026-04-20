#!/usr/bin/env python3
"""
Fetch 1 sample record from every active CCTNS/DOPAMAS API.

Run:  python CCTNSV2_data/save_cctns_api_samples.py
      (from repo root, or cd CCTNSV2_data && python save_cctns_api_samples.py)

Outputs: CCTNSV2_data/<API_NAME>.json for each API.
Does NOT modify any ETL tables or checkpoints.
"""

import sys
import os
import json
import logging
from datetime import datetime
from pathlib import Path

import requests

# ── Path setup ────────────────────────────────────────────────────────────────
OUTPUT_DIR = Path(__file__).resolve().parent          # CCTNSV2_data/
REPO_ROOT = OUTPUT_DIR.parent                          # repo root where env_utils.py lives
sys.path.insert(0, str(REPO_ROOT))

from env_utils import load_repo_environment
load_repo_environment()

# ── Config (read env after load_repo_environment) ─────────────────────────────
API1 = os.environ.get('DOPAMAS_API_URL', '').rstrip('/')
API2 = (os.environ.get('DOPAMAS_API_URL2', '') or API1).rstrip('/')
API_KEY = os.environ.get('DOPAMAS_API_KEY', '')
TIMEOUT = int(os.environ.get('API_TIMEOUT', '60'))
HEADERS = {'x-api-key': API_KEY}

# API enforces max 7-day window
SAMPLE_FROM = '2022-07-01'
SAMPLE_TO = '2022-07-07'

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger('cctns_sampler')


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_api2_url(endpoint_key: str, default_path: str) -> str:
    """
    Mirrors get_api_endpoint() logic from etl_mo_seizures/config.py etc.
    Prefers DOPAMAS_API_URL2 (port 3001) over API1 (port 3000).
    Allows per-endpoint override via <KEY>_API_BASE_URL / <KEY>_API_ENDPOINT env vars.
    """
    override_base = os.environ.get(f'{endpoint_key.upper()}_API_BASE_URL', '').strip()
    base = (override_base or os.environ.get('DOPAMAS_API_URL2', '') or API1).rstrip('/')
    path = os.environ.get(f'{endpoint_key.upper()}_API_ENDPOINT', default_path).strip()
    if path and not path.startswith('/'):
        path = '/' + path
    return f"{base}{path}"


def extract_first(data) -> object:
    """Return only the first record from any response shape."""
    if isinstance(data, list):
        return data[:1]
    if isinstance(data, dict):
        for key in ('data', 'records', 'results', 'items', 'rows',
                    'crimes', 'cases', 'content', 'list'):
            val = data.get(key)
            if isinstance(val, list) and val:
                return {**data, key: val[:1]}
    return data


def save_sample(api_name: str, url: str, params: dict = None) -> tuple:
    """
    GET url with params, save first record to OUTPUT_DIR/<api_name>.json.
    Returns (success: bool, error_msg: str|None).
    """
    out = OUTPUT_DIR / f'{api_name}.json'
    log.info(f'Sampling  {api_name:<35} → {url}')
    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
        payload = extract_first(data)
        if not payload and payload != 0:
            payload = {'_meta': 'no_data_returned', 'url': url, 'params': params}
        out.write_text(json.dumps(payload, indent=2, default=str), encoding='utf-8')
        log.info(f'  ✓  {out.name}')
        return True, None
    except Exception as exc:
        msg = str(exc)
        log.error(f'  ✗  {api_name}: {msg}')
        out.write_text(
            json.dumps({'_meta': 'error', 'api': api_name, 'url': url,
                        'params': params, 'error': msg}, indent=2),
            encoding='utf-8',
        )
        return False, msg


def resolve_crime_id() -> str | None:
    """Fetch crimes list → extract first crime_id for /id endpoints."""
    try:
        r = requests.get(
            f'{API1}/crimes',
            params={'fromDate': SAMPLE_FROM, 'toDate': SAMPLE_TO},
            headers=HEADERS, timeout=TIMEOUT,
        )
        r.raise_for_status()
        data = r.json()
        rec = None
        if isinstance(data, list) and data:
            rec = data[0]
        elif isinstance(data, dict):
            for key in ('data', 'records', 'results', 'items', 'rows', 'crimes', 'cases', 'content'):
                val = data.get(key)
                if isinstance(val, list) and val:
                    rec = val[0]
                    break
        if rec:
            for field in ('crimeId', 'crime_id', 'id', 'caseId', 'case_id', '_id'):
                if rec.get(field):
                    cid = str(rec[field])
                    log.info(f'  Resolved crime_id={cid} (field={field})')
                    return cid
    except Exception as exc:
        log.warning(f'Could not resolve crime_id: {exc}')
    return None


def resolve_person_id() -> str | None:
    """Query persons DB table for first id. Falls back to None (no crash)."""
    try:
        import psycopg2
        from env_utils import resolve_db_config
        cfg = resolve_db_config()
        conn = psycopg2.connect(**{k: v for k, v in cfg.items()
                                   if k in ('host', 'port', 'dbname', 'user', 'password')})
        cur = conn.cursor()
        cur.execute("SELECT id FROM persons LIMIT 1")
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row:
            pid = str(row[0])
            log.info(f'  Resolved person_id={pid} from DB')
            return pid
    except Exception as exc:
        log.warning(f'Could not resolve person_id from DB: {exc}')
    return None


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    OUTPUT_DIR.mkdir(exist_ok=True)

    if not API1:
        log.error('DOPAMAS_API_URL not set. Check your .env file.')
        sys.exit(1)
    if not API_KEY:
        log.warning('DOPAMAS_API_KEY is empty — requests may be rejected.')

    log.info(f'API (port 3000): {API1}')
    log.info(f'Sample window : {SAMPLE_FROM} → {SAMPLE_TO}')
    log.info(f'Output dir    : {OUTPUT_DIR}\n')

    date_params = {'fromDate': SAMPLE_FROM, 'toDate': SAMPLE_TO}
    results: list[tuple[str, bool, str | None]] = []

    # ── Pre-fetch shared IDs (needed for /id endpoints) ───────────────────────
    log.info('--- Resolving shared IDs ---')
    crime_id = resolve_crime_id()
    person_id = resolve_person_id()
    log.info('')

    # ── API1 endpoints (DOPAMAS_API_URL / port 3000) ──────────────────────────
    log.info('--- API1 endpoints ---')

    results.append(('CRIMES_API',
                    *save_sample('CRIMES_API', f'{API1}/crimes', date_params)))

    if crime_id:
        results.append(('CRIMES_BY_ID_API',
                        *save_sample('CRIMES_BY_ID_API', f'{API1}/crimes/{crime_id}')))
    else:
        log.warning('CRIMES_BY_ID_API skipped — crime_id unresolved')
        results.append(('CRIMES_BY_ID_API', False, 'crime_id unresolved'))

    results.append(('ACCUSED_API',
                    *save_sample('ACCUSED_API', f'{API1}/accused', date_params)))

    if crime_id:
        results.append(('ACCUSED_BY_CRIME_ID_API',
                        *save_sample('ACCUSED_BY_CRIME_ID_API', f'{API1}/accused/{crime_id}')))
    else:
        log.warning('ACCUSED_BY_CRIME_ID_API skipped — crime_id unresolved')
        results.append(('ACCUSED_BY_CRIME_ID_API', False, 'crime_id unresolved'))

    if person_id:
        results.append(('PERSON_DETAILS_API',
                        *save_sample('PERSON_DETAILS_API',
                                     f'{API1}/person-details/{person_id}', date_params)))
    else:
        log.warning('PERSON_DETAILS_API skipped — person_id unresolved (DB unavailable?)')
        results.append(('PERSON_DETAILS_API', False, 'person_id unresolved'))

    results.append(('PROPERTY_DETAILS_API',
                    *save_sample('PROPERTY_DETAILS_API', f'{API1}/property-details', date_params)))

    results.append(('IR_API',
                    *save_sample('IR_API', f'{API1}/interrogation-reports/v1/', date_params)))

    results.append(('HIERARCHY_API',
                    *save_sample('HIERARCHY_API', f'{API1}/master-data/hierarchy', date_params)))

    # ── API1 endpoints (continued) ───────────────────────────────────────────
    log.info('\n--- Additional API1 endpoints ---')

    results.append(('DISPOSAL_API',
                    *save_sample('DISPOSAL_API', f'{API1}/crimes/disposal', date_params)))

    results.append(('ARRESTS_API',
                    *save_sample('ARRESTS_API', f'{API1}/arrests', date_params)))

    results.append(('MO_SEIZURES_API',
                    *save_sample('MO_SEIZURES_API', f'{API1}/mo-seizures', date_params)))

    results.append(('CHARGESHEETS_API',
                    *save_sample('CHARGESHEETS_API', f'{API1}/chargesheets', date_params)))

    results.append(('UPDATE_CHARGESHEETS_API',
                    *save_sample('UPDATE_CHARGESHEETS_API', f'{API1}/update-chargesheets', date_params)))

    results.append(('CASE_PROPERTY_API',
                    *save_sample('CASE_PROPERTY_API', f'{API1}/case-property', date_params)))

    # ── FILES endpoint (binary PDF — save probe metadata only) ────────────────
    log.info('\n--- FILES endpoint (binary — HEAD probe) ---')
    files_out = OUTPUT_DIR / 'FILES_API.json'
    try:
        probe_url = f'{API1}/files/probe-check'
        r = requests.head(probe_url, headers=HEADERS, timeout=TIMEOUT)
        payload = {
            '_meta': 'HEAD probe — actual endpoint returns binary PDF',
            'endpoint': f'{API1}/files/{{fileId}}',
            'probe_url': probe_url,
            'http_status': r.status_code,
            'response_headers': dict(r.headers),
            'usage': 'GET /files/{fileId} streams PDF binary — not JSON-samplable',
        }
        files_out.write_text(json.dumps(payload, indent=2), encoding='utf-8')
        log.info(f'  ✓  FILES_API.json (HEAD {r.status_code})')
        results.append(('FILES_API', True, None))
    except Exception as exc:
        files_out.write_text(
            json.dumps({'_meta': 'error', 'error': str(exc)}, indent=2), encoding='utf-8')
        results.append(('FILES_API', False, str(exc)))

    # ── Summary ───────────────────────────────────────────────────────────────
    ok_count = sum(1 for _, ok, _ in results if ok)
    fail_count = len(results) - ok_count
    width = 55

    print(f'\n{"─" * width}')
    print(f'  CCTNS API Samples  —  {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print(f'{"─" * width}')
    for name, ok, err in results:
        tag = '✓ OK  ' if ok else '✗ FAIL'
        detail = f'  ({err})' if err else ''
        print(f'  {tag}  {name}{detail}')
    print(f'{"─" * width}')
    print(f'  Total: {len(results)}   OK: {ok_count}   Failed: {fail_count}')
    print(f'  Output: {OUTPUT_DIR}')
    print(f'{"─" * width}\n')

    sys.exit(0 if fail_count == 0 else 1)


if __name__ == '__main__':
    main()
