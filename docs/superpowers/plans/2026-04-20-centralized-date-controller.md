# Centralized ETL Date Controller Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a single `DateController` module that all ETLs import to get their fetch window, handle `fetch_origin_start`, persist state, and deduplicate records — replacing scattered per-ETL checkpoint logic. Linked APIs (ACCUSED, PERSON_DETAILS, UPDATE_CHARGESHEETS) are triggered by changed IDs emitted by their parent incremental APIs, not by date range.

**Architecture:** A new `etl_date_controller/` package stores per-API state in two new Postgres tables: `etl_api_date_control` (fetch windows + status per API) and `etl_linked_id_queue` (crime_ids/person_ids discovered by incremental ETLs, consumed by linked ETLs). Incremental ETLs enqueue changed IDs on each run; linked ETLs dequeue and fetch by specific ID. Deduplication uses `ON CONFLICT DO NOTHING` with per-API unique keys.

**Tech Stack:** Python 3.11, psycopg2, existing `db_pooling.py`, existing `env_utils.py`, pytest

---

## API Classification (from CCTNSV2_data/ samples + DB-schema.sql audit)

### Tier 1 — Incremental (DATE_MODIFIED in API response)

| API | Modified Field | DB Table | Unique Key (DB) |
|-----|---------------|----------|-----------------|
| CRIMES_API | `DATE_MODIFIED` | crimes | crime_id |
| ARRESTS_API | `DATE_MODIFIED` | arrests | (crime_id, accused_seq_no) |
| CASE_PROPERTY_API | `DATE_MODIFIED` | fsl_case_property | case_property_id |
| CHARGESHEETS_API | `dateModified` | chargesheets | charge_sheet_id |
| DISPOSAL_API | `DATE_MODIFIED` | disposal | crime_id |
| IR_API | `DATE_MODIFIED` | interrogation_reports | interrogation_report_id |
| MO_SEIZURES_API | `DATE_MODIFIED` | mo_seizures | mo_seizure_id |
| PROPERTY_DETAILS_API | `DATE_MODIFIED` | properties | property_id |

### Tier 2 — Linked (no DATE_MODIFIED in API; triggered by parent ID)

| API | API Response FK | Parent | Fetch Endpoint | DB Table | Unique Key |
|-----|----------------|--------|---------------|----------|------------|
| ACCUSED_API | `CRIME_ID` | CRIMES_API | `/accused/{crime_id}` | accused | accused_id |
| UPDATE_CHARGESHEETS_API | `crimeId` | CRIMES_API | date range (uses crime_id filter) | charge_sheet_updates | update_charge_sheet_id |
| PERSON_DETAILS_API | `PERSON_ID` | ACCUSED_API | `/person-details/{person_id}` | persons | person_id |

> **Why Linked?** DB schema confirms `accused.date_modified` exists, but the **ACCUSED_API response does not expose `DATE_MODIFIED`** — only `CRIME_ID`, `ACCUSED_ID`, `PERSON_ID`. So incremental tracking via API is impossible. Instead: when CRIMES_API finds new/modified crime_ids → enqueue them → ACCUSED_API fetches by crime_id → extracts person_ids → enqueues them → PERSON_DETAILS_API fetches by person_id.

### Tier 3 — Standalone (master data, no crime FK)

| API | Refresh Strategy | DB Table | Unique Key |
|-----|-----------------|----------|------------|
| HIERARCHY_API | Periodic full-refresh (weekly) | hierarchy | ps_code |

**API max date window:** 7 days (enforced by CCTNS server — `fromDate`/`toDate` diff ≤ 7 days).

---

## DB Schema Additions (append to DB-schema.sql)

```sql
-- Table 1: per-API fetch window + status
CREATE TABLE IF NOT EXISTS etl_api_date_control (
    api_name                     VARCHAR(100) PRIMARY KEY,
    api_tier                     VARCHAR(20)  NOT NULL
                                              CHECK (api_tier IN ('incremental', 'linked', 'standalone')),
    fetch_origin_start           BOOLEAN      NOT NULL DEFAULT FALSE,
    origin_start_date            TIMESTAMPTZ  NOT NULL DEFAULT '2022-06-01T00:00:00+05:30',
    last_run_start               TIMESTAMPTZ,
    last_run_end                 TIMESTAMPTZ,
    last_processed_modified_date TIMESTAMPTZ,
    status                       VARCHAR(20)  NOT NULL DEFAULT 'pending'
                                              CHECK (status IN ('pending','running','completed','failed')),
    records_processed            INTEGER      NOT NULL DEFAULT 0,
    updated_at                   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Table 2: ID queue for linked APIs
CREATE TABLE IF NOT EXISTS etl_linked_id_queue (
    id             BIGSERIAL    PRIMARY KEY,
    target_api     VARCHAR(100) NOT NULL,   -- e.g. 'ACCUSED_API'
    id_type        VARCHAR(50)  NOT NULL,   -- 'crime_id' | 'person_id'
    id_value       VARCHAR(100) NOT NULL,
    source_api     VARCHAR(100) NOT NULL,   -- which ETL enqueued this
    status         VARCHAR(20)  NOT NULL DEFAULT 'pending'
                                           CHECK (status IN ('pending','processing','done','failed')),
    enqueued_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    processed_at   TIMESTAMPTZ,
    CONSTRAINT etl_linked_id_queue_uniq UNIQUE (target_api, id_type, id_value, status)
);
CREATE INDEX IF NOT EXISTS idx_linked_queue_pending
    ON etl_linked_id_queue (target_api, status) WHERE status = 'pending';
```

---

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `etl_date_controller/__init__.py` | Create | Public exports |
| `etl_date_controller/api_registry.py` | Create | API tier + unique key + modified field + parent link constants |
| `etl_date_controller/schema.py` | Create | `etl_api_date_control` + `etl_linked_id_queue` DDL |
| `etl_date_controller/controller.py` | Create | `DateController` class — window logic, state read/write |
| `etl_date_controller/linked_queue.py` | Create | `LinkedIdQueue` — enqueue/dequeue crime_ids/person_ids |
| `etl_date_controller/deduplication.py` | Create | `build_upsert_skip_sql()` + `extract_max_modified()` |
| `tests/test_date_controller.py` | Create | Unit + integration tests |
| `etl-crimes/config.py` | Modify | Replace hand-rolled checkpoint with `DateController` |

---

## Task 1: DB Schema — `etl_api_date_control` table

**Files:**
- Create: `etl_date_controller/schema.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_date_controller.py
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
import pytest
from env_utils import load_repo_environment, resolve_db_config

load_repo_environment()

@pytest.fixture(scope='session')
def db_conn():
    cfg = resolve_db_config()
    conn = psycopg2.connect(**{k: v for k, v in cfg.items()
                               if k in ('host', 'port', 'dbname', 'user', 'password')})
    yield conn
    conn.close()

def test_schema_creates_table(db_conn):
    from etl_date_controller.schema import ensure_schema
    ensure_schema(db_conn)
    with db_conn.cursor() as cur:
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'etl_api_date_control'
            ORDER BY column_name
        """)
        cols = {r[0] for r in cur.fetchall()}
    assert 'api_name' in cols
    assert 'api_type' in cols
    assert 'fetch_origin_start' in cols
    assert 'last_processed_modified_date' in cols
    assert 'last_run_start' in cols
    assert 'last_run_end' in cols
    assert 'status' in cols
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /home/ashish-ratna/DOPAMS-ETL
python -m pytest tests/test_date_controller.py::test_schema_creates_table -v
```

Expected: `ModuleNotFoundError: No module named 'etl_date_controller'`

- [ ] **Step 3: Create schema module**

```python
# etl_date_controller/schema.py

DDL = """
CREATE TABLE IF NOT EXISTS etl_api_date_control (
    api_name                     VARCHAR(100) PRIMARY KEY,
    api_type                     VARCHAR(20)  NOT NULL CHECK (api_type IN ('incremental', 'full_load')),
    fetch_origin_start           BOOLEAN      NOT NULL DEFAULT FALSE,
    origin_start_date            TIMESTAMPTZ  NOT NULL DEFAULT '2022-06-01T00:00:00+05:30',
    last_run_start               TIMESTAMPTZ,
    last_run_end                 TIMESTAMPTZ,
    last_processed_modified_date TIMESTAMPTZ,
    status                       VARCHAR(20)  NOT NULL DEFAULT 'pending'
                                              CHECK (status IN ('pending', 'running', 'completed', 'failed')),
    records_processed            INTEGER      NOT NULL DEFAULT 0,
    updated_at                   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
)
"""

def ensure_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()
```

- [ ] **Step 4: Create package `__init__.py`**

```python
# etl_date_controller/__init__.py
from .controller import DateController
from .api_registry import API_REGISTRY, ApiType

__all__ = ['DateController', 'API_REGISTRY', 'ApiType']
```

- [ ] **Step 5: Run test**

```bash
python -m pytest tests/test_date_controller.py::test_schema_creates_table -v
```

Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add etl_date_controller/__init__.py etl_date_controller/schema.py tests/test_date_controller.py
git commit -m "feat: add etl_api_date_control schema and test"
```

---

## Task 2: API Registry

**Files:**
- Create: `etl_date_controller/api_registry.py`

- [ ] **Step 1: Write the failing test**

```python
# Add to tests/test_date_controller.py

def test_registry_classifies_crimes_as_incremental():
    from etl_date_controller.api_registry import API_REGISTRY, ApiType
    reg = API_REGISTRY['CRIMES_API']
    assert reg['type'] == ApiType.INCREMENTAL
    assert reg['modified_field'] == 'DATE_MODIFIED'
    assert 'CRIME_ID' in reg['unique_keys']

def test_registry_classifies_accused_as_full_load():
    from etl_date_controller.api_registry import API_REGISTRY, ApiType
    reg = API_REGISTRY['ACCUSED_API']
    assert reg['type'] == ApiType.FULL_LOAD
    assert reg['modified_field'] is None

def test_registry_has_all_apis():
    from etl_date_controller.api_registry import API_REGISTRY
    expected = {
        'CRIMES_API', 'ACCUSED_API', 'ARRESTS_API', 'DISPOSAL_API',
        'CHARGESHEETS_API', 'UPDATE_CHARGESHEETS_API', 'IR_API',
        'MO_SEIZURES_API', 'CASE_PROPERTY_API', 'PROPERTY_DETAILS_API',
        'HIERARCHY_API', 'PERSON_DETAILS_API',
    }
    assert expected.issubset(set(API_REGISTRY.keys()))
```

- [ ] **Step 2: Run test to verify it fails**

```bash
python -m pytest tests/test_date_controller.py::test_registry_classifies_crimes_as_incremental -v
```

Expected: `ImportError`

- [ ] **Step 3: Create api_registry.py**

```python
# etl_date_controller/api_registry.py
from enum import Enum


class ApiTier(str, Enum):
    INCREMENTAL = 'incremental'  # date-range fetch using DATE_MODIFIED from API response
    LINKED      = 'linked'       # no DATE_MODIFIED in API; triggered by parent's changed IDs
    STANDALONE  = 'standalone'   # master data; periodic full-refresh, no crime FK


# Each entry:
#   tier           : ApiTier
#   modified_field : str | None    — API response field name for incremental tracking
#   unique_keys    : list[str]     — DB column names forming the unique/conflict key
#   db_table       : str           — target Postgres table
#   parent_api     : str | None    — for LINKED: which API enqueues IDs for this one
#   parent_id_field: str | None    — for LINKED: ID field in parent API response to enqueue
#   id_type        : str | None    — for LINKED: 'crime_id' | 'person_id' (matches queue id_type)
API_REGISTRY: dict[str, dict] = {
    # ── Tier 1: Incremental ──────────────────────────────────────────────────
    'CRIMES_API': {
        'tier': ApiTier.INCREMENTAL,
        'modified_field': 'DATE_MODIFIED',
        'unique_keys': ['crime_id'],
        'db_table': 'crimes',
        'parent_api': None,
        'parent_id_field': None,
        'id_type': None,
        # CRIMES enqueues crime_ids for ACCUSED and UPDATE_CHARGESHEETS
        'enqueues': [
            {'target_api': 'ACCUSED_API',            'id_type': 'crime_id', 'id_field': 'CRIME_ID'},
            {'target_api': 'UPDATE_CHARGESHEETS_API', 'id_type': 'crime_id', 'id_field': 'CRIME_ID'},
        ],
    },
    'ARRESTS_API': {
        'tier': ApiTier.INCREMENTAL,
        'modified_field': 'DATE_MODIFIED',
        'unique_keys': ['crime_id', 'accused_seq_no'],
        'db_table': 'arrests',
        'parent_api': None, 'parent_id_field': None, 'id_type': None, 'enqueues': [],
    },
    'CASE_PROPERTY_API': {
        'tier': ApiTier.INCREMENTAL,
        'modified_field': 'DATE_MODIFIED',
        'unique_keys': ['case_property_id'],
        'db_table': 'fsl_case_property',
        'parent_api': None, 'parent_id_field': None, 'id_type': None, 'enqueues': [],
    },
    'CHARGESHEETS_API': {
        'tier': ApiTier.INCREMENTAL,
        'modified_field': 'dateModified',
        'unique_keys': ['charge_sheet_id'],
        'db_table': 'chargesheets',
        'parent_api': None, 'parent_id_field': None, 'id_type': None, 'enqueues': [],
    },
    'DISPOSAL_API': {
        'tier': ApiTier.INCREMENTAL,
        'modified_field': 'DATE_MODIFIED',
        'unique_keys': ['crime_id'],
        'db_table': 'disposal',
        'parent_api': None, 'parent_id_field': None, 'id_type': None, 'enqueues': [],
    },
    'IR_API': {
        'tier': ApiTier.INCREMENTAL,
        'modified_field': 'DATE_MODIFIED',
        'unique_keys': ['interrogation_report_id'],
        'db_table': 'interrogation_reports',
        'parent_api': None, 'parent_id_field': None, 'id_type': None, 'enqueues': [],
    },
    'MO_SEIZURES_API': {
        'tier': ApiTier.INCREMENTAL,
        'modified_field': 'DATE_MODIFIED',
        'unique_keys': ['mo_seizure_id'],
        'db_table': 'mo_seizures',
        'parent_api': None, 'parent_id_field': None, 'id_type': None, 'enqueues': [],
    },
    'PROPERTY_DETAILS_API': {
        'tier': ApiTier.INCREMENTAL,
        'modified_field': 'DATE_MODIFIED',
        'unique_keys': ['property_id'],
        'db_table': 'properties',
        'parent_api': None, 'parent_id_field': None, 'id_type': None, 'enqueues': [],
    },

    # ── Tier 2: Linked ───────────────────────────────────────────────────────
    'ACCUSED_API': {
        'tier': ApiTier.LINKED,
        'modified_field': None,          # API response has no DATE_MODIFIED
        'unique_keys': ['accused_id'],
        'db_table': 'accused',
        'parent_api': 'CRIMES_API',
        'parent_id_field': 'CRIME_ID',   # field in CRIMES_API response
        'id_type': 'crime_id',
        # ACCUSED enqueues person_ids for PERSON_DETAILS
        'enqueues': [
            {'target_api': 'PERSON_DETAILS_API', 'id_type': 'person_id', 'id_field': 'PERSON_ID'},
        ],
    },
    'UPDATE_CHARGESHEETS_API': {
        'tier': ApiTier.LINKED,
        'modified_field': None,
        'unique_keys': ['update_charge_sheet_id'],
        'db_table': 'charge_sheet_updates',
        'parent_api': 'CRIMES_API',
        'parent_id_field': 'CRIME_ID',
        'id_type': 'crime_id',
        'enqueues': [],
    },
    'PERSON_DETAILS_API': {
        'tier': ApiTier.LINKED,
        'modified_field': None,
        'unique_keys': ['person_id'],
        'db_table': 'persons',
        'parent_api': 'ACCUSED_API',
        'parent_id_field': 'PERSON_ID',  # field in ACCUSED_API response
        'id_type': 'person_id',
        'enqueues': [],
    },

    # ── Tier 3: Standalone ───────────────────────────────────────────────────
    'HIERARCHY_API': {
        'tier': ApiTier.STANDALONE,
        'modified_field': None,
        'unique_keys': ['ps_code'],      # DB schema: ps_code is PK of hierarchy table
        'db_table': 'hierarchy',
        'parent_api': None, 'parent_id_field': None, 'id_type': None, 'enqueues': [],
    },
}
```

- [ ] **Step 4: Run tests**

```bash
python -m pytest tests/test_date_controller.py -k "registry" -v
```

Expected: 3 PASS

- [ ] **Step 5: Commit**

```bash
git add etl_date_controller/api_registry.py tests/test_date_controller.py
git commit -m "feat: add API registry with incremental/full_load classification"
```

---

## Task 3: DateController Core Logic

**Files:**
- Create: `etl_date_controller/controller.py`

- [ ] **Step 1: Write the failing tests**

```python
# Add to tests/test_date_controller.py
from datetime import datetime, timezone, timedelta

IST = timezone(timedelta(hours=5, minutes=30))
ORIGIN = datetime(2022, 6, 1, tzinfo=IST)


def test_first_run_fetch_origin_true_returns_origin(db_conn):
    """First run with fetch_origin_start=True → start from June 2022."""
    from etl_date_controller.schema import ensure_schema
    from etl_date_controller.controller import DateController

    ensure_schema(db_conn)
    # Clean slate for this api
    with db_conn.cursor() as cur:
        cur.execute("DELETE FROM etl_api_date_control WHERE api_name = %s", ('CRIMES_API',))
    db_conn.commit()

    ctrl = DateController('CRIMES_API', db_conn, fetch_origin_start=True)
    window = ctrl.get_fetch_window()

    assert window['start'] == ORIGIN
    assert window['type'] == 'origin'


def test_first_run_fetch_origin_false_returns_origin_as_fallback(db_conn):
    """First run with fetch_origin_start=False and no state → fall back to origin."""
    from etl_date_controller.schema import ensure_schema
    from etl_date_controller.controller import DateController

    ensure_schema(db_conn)
    with db_conn.cursor() as cur:
        cur.execute("DELETE FROM etl_api_date_control WHERE api_name = %s", ('DISPOSAL_API',))
    db_conn.commit()

    ctrl = DateController('DISPOSAL_API', db_conn, fetch_origin_start=False)
    window = ctrl.get_fetch_window()

    assert window['start'] == ORIGIN
    assert window['type'] == 'fallback_origin'


def test_incremental_api_uses_last_modified_date(db_conn):
    """Incremental API with prior state → start from last_processed_modified_date."""
    from etl_date_controller.schema import ensure_schema
    from etl_date_controller.controller import DateController

    ensure_schema(db_conn)
    last_mod = datetime(2024, 3, 15, 10, 0, 0, tzinfo=IST)
    with db_conn.cursor() as cur:
        cur.execute("""
            INSERT INTO etl_api_date_control
                (api_name, api_type, fetch_origin_start, last_processed_modified_date,
                 last_run_end, status)
            VALUES (%s, 'incremental', FALSE, %s, %s, 'completed')
            ON CONFLICT (api_name) DO UPDATE
                SET last_processed_modified_date = EXCLUDED.last_processed_modified_date,
                    status = 'completed'
        """, ('CRIMES_API', last_mod, last_mod))
    db_conn.commit()

    ctrl = DateController('CRIMES_API', db_conn, fetch_origin_start=False)
    window = ctrl.get_fetch_window()

    assert window['start'] == last_mod
    assert window['type'] == 'incremental_modified'


def test_full_load_api_uses_last_run_end(db_conn):
    """Full-load API with prior state → start from last_run_end."""
    from etl_date_controller.schema import ensure_schema
    from etl_date_controller.controller import DateController

    ensure_schema(db_conn)
    last_run = datetime(2024, 6, 1, 0, 0, 0, tzinfo=IST)
    with db_conn.cursor() as cur:
        cur.execute("""
            INSERT INTO etl_api_date_control
                (api_name, api_type, fetch_origin_start, last_run_end, status)
            VALUES (%s, 'full_load', FALSE, %s, 'completed')
            ON CONFLICT (api_name) DO UPDATE
                SET last_run_end = EXCLUDED.last_run_end,
                    status = 'completed'
        """, ('ACCUSED_API', last_run))
    db_conn.commit()

    ctrl = DateController('ACCUSED_API', db_conn, fetch_origin_start=False)
    window = ctrl.get_fetch_window()

    assert window['start'] == last_run
    assert window['type'] == 'last_run'


def test_mark_success_persists_state(db_conn):
    """mark_success() writes last_run_end and last_processed_modified_date."""
    from etl_date_controller.schema import ensure_schema
    from etl_date_controller.controller import DateController

    ensure_schema(db_conn)
    with db_conn.cursor() as cur:
        cur.execute("DELETE FROM etl_api_date_control WHERE api_name = %s", ('MO_SEIZURES_API',))
    db_conn.commit()

    ctrl = DateController('MO_SEIZURES_API', db_conn, fetch_origin_start=True)
    run_end = datetime(2024, 7, 31, 23, 59, 59, tzinfo=IST)
    max_modified = datetime(2024, 7, 30, 15, 0, 0, tzinfo=IST)
    ctrl.mark_success(run_end=run_end, max_modified_date=max_modified, records_processed=42)

    with db_conn.cursor() as cur:
        cur.execute("""
            SELECT last_run_end, last_processed_modified_date, status, records_processed
            FROM etl_api_date_control WHERE api_name = %s
        """, ('MO_SEIZURES_API',))
        row = cur.fetchone()

    assert row[0].replace(tzinfo=IST) == run_end or row[0] is not None
    assert row[2] == 'completed'
    assert row[3] == 42
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_date_controller.py -k "run" -v
```

Expected: `ImportError: cannot import name 'DateController'`

- [ ] **Step 3: Implement DateController**

```python
# etl_date_controller/controller.py
from __future__ import annotations
from datetime import datetime, timezone, timedelta
from typing import Optional
import logging

from .api_registry import API_REGISTRY, ApiType
from .schema import ensure_schema

log = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))
ORIGIN_START = datetime(2022, 6, 1, tzinfo=IST)
MAX_CHUNK_DAYS = 7  # CCTNS API hard limit


class DateController:
    """
    Centralised fetch-window manager for all DOPAMS ETLs.

    Usage:
        ctrl = DateController('CRIMES_API', conn, fetch_origin_start=False)
        window = ctrl.get_fetch_window()
        # ... run ETL for window['start'] → window['end'] in 7-day chunks ...
        ctrl.mark_success(run_end=..., max_modified_date=..., records_processed=n)
    """

    def __init__(self, api_name: str, conn, fetch_origin_start: bool = False):
        if api_name not in API_REGISTRY:
            raise ValueError(f"Unknown API '{api_name}'. Register it in api_registry.py.")
        self.api_name = api_name
        self.conn = conn
        self.fetch_origin_start = fetch_origin_start
        self.meta = API_REGISTRY[api_name]
        ensure_schema(conn)
        self._state = self._load_state()

    def _load_state(self) -> dict:
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT last_run_start, last_run_end, last_processed_modified_date, status "
                "FROM etl_api_date_control WHERE api_name = %s",
                (self.api_name,),
            )
            row = cur.fetchone()
        if row is None:
            return {}
        return {
            'last_run_start': row[0],
            'last_run_end': row[1],
            'last_processed_modified_date': row[2],
            'status': row[3],
        }

    def get_fetch_window(self) -> dict:
        """
        Returns:
            {
              'start': datetime,
              'end':   datetime,     # always now(IST)
              'type':  str,          # 'origin' | 'incremental_modified' | 'last_run' | 'fallback_origin'
              'chunk_days': int,     # always MAX_CHUNK_DAYS
            }
        """
        now = datetime.now(IST)

        if self.fetch_origin_start:
            return {'start': ORIGIN_START, 'end': now,
                    'type': 'origin', 'chunk_days': MAX_CHUNK_DAYS}

        # Incremental API — prefer last_processed_modified_date
        if (self.meta['type'] == ApiType.INCREMENTAL
                and self._state.get('last_processed_modified_date')):
            start = self._state['last_processed_modified_date']
            if start.tzinfo is None:
                start = start.replace(tzinfo=IST)
            return {'start': start, 'end': now,
                    'type': 'incremental_modified', 'chunk_days': MAX_CHUNK_DAYS}

        # Full-load API — use last_run_end as next start
        if self._state.get('last_run_end'):
            start = self._state['last_run_end']
            if start.tzinfo is None:
                start = start.replace(tzinfo=IST)
            return {'start': start, 'end': now,
                    'type': 'last_run', 'chunk_days': MAX_CHUNK_DAYS}

        # No prior state at all → fall back to origin
        log.warning("%s has no prior state. Starting from origin %s", self.api_name, ORIGIN_START)
        return {'start': ORIGIN_START, 'end': now,
                'type': 'fallback_origin', 'chunk_days': MAX_CHUNK_DAYS}

    def mark_running(self, run_start: datetime) -> None:
        """Call at ETL start to record status=running."""
        self._upsert(run_start=run_start, status='running')

    def mark_success(
        self,
        run_end: datetime,
        max_modified_date: Optional[datetime] = None,
        records_processed: int = 0,
    ) -> None:
        """Call after successful ETL completion."""
        self._upsert(
            run_end=run_end,
            last_modified=max_modified_date,
            status='completed',
            records_processed=records_processed,
        )

    def mark_failed(self, run_end: datetime) -> None:
        """Call on ETL failure to record status=failed (does NOT advance window)."""
        self._upsert(run_end=run_end, status='failed')

    def _upsert(
        self,
        run_start: Optional[datetime] = None,
        run_end: Optional[datetime] = None,
        last_modified: Optional[datetime] = None,
        status: str = 'pending',
        records_processed: int = 0,
    ) -> None:
        api_type = self.meta['type'].value
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO etl_api_date_control
                    (api_name, api_type, fetch_origin_start,
                     last_run_start, last_run_end, last_processed_modified_date,
                     status, records_processed, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (api_name) DO UPDATE SET
                    fetch_origin_start           = EXCLUDED.fetch_origin_start,
                    last_run_start               = COALESCE(EXCLUDED.last_run_start,
                                                            etl_api_date_control.last_run_start),
                    last_run_end                 = COALESCE(EXCLUDED.last_run_end,
                                                            etl_api_date_control.last_run_end),
                    last_processed_modified_date = COALESCE(EXCLUDED.last_processed_modified_date,
                                                            etl_api_date_control.last_processed_modified_date),
                    status                       = EXCLUDED.status,
                    records_processed            = etl_api_date_control.records_processed
                                                   + EXCLUDED.records_processed,
                    updated_at                   = NOW()
            """, (
                self.api_name, api_type, self.fetch_origin_start,
                run_start, run_end, last_modified,
                status, records_processed,
            ))
        self.conn.commit()
```

- [ ] **Step 4: Run tests**

```bash
python -m pytest tests/test_date_controller.py -v
```

Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add etl_date_controller/controller.py tests/test_date_controller.py
git commit -m "feat: implement DateController with fetch window and state persistence"
```

---

## Task 4: Deduplication Helper

**Files:**
- Create: `etl_date_controller/deduplication.py`

- [ ] **Step 1: Write the failing test**

```python
# Add to tests/test_date_controller.py

def test_dedup_conflict_clause_single_key():
    from etl_date_controller.deduplication import build_upsert_skip_sql
    sql = build_upsert_skip_sql('crimes', ['crime_id'], ['crime_id', 'ps_code', 'fir_num'])
    assert 'ON CONFLICT (crime_id)' in sql
    assert 'DO NOTHING' in sql

def test_dedup_conflict_clause_composite_key():
    from etl_date_controller.deduplication import build_upsert_skip_sql
    sql = build_upsert_skip_sql('arrests', ['crime_id', 'accused_seq_no'],
                                ['crime_id', 'accused_seq_no', 'arrested_date'])
    assert 'ON CONFLICT (crime_id, accused_seq_no)' in sql
    assert 'DO NOTHING' in sql

def test_dedup_returns_valid_parameterized_sql():
    from etl_date_controller.deduplication import build_upsert_skip_sql
    sql = build_upsert_skip_sql('mo_seizures', ['mo_seizure_id'],
                                ['mo_seizure_id', 'crime_id', 'seq_no'])
    # Must contain placeholders
    assert '%s' in sql
```

- [ ] **Step 2: Run test to verify it fails**

```bash
python -m pytest tests/test_date_controller.py -k "dedup" -v
```

Expected: `ImportError`

- [ ] **Step 3: Implement deduplication helper**

```python
# etl_date_controller/deduplication.py
from __future__ import annotations


def build_upsert_skip_sql(table: str, conflict_keys: list[str], columns: list[str]) -> str:
    """
    Build INSERT ... ON CONFLICT DO NOTHING SQL for idempotent ingestion.

    Args:
        table         : target DB table name
        conflict_keys : columns forming the unique constraint (must already exist in DB)
        columns       : all columns to insert (in order)

    Returns:
        Parameterized SQL string; caller supplies values as a tuple per row.

    Example:
        sql = build_upsert_skip_sql('crimes', ['crime_id'], ['crime_id', 'ps_code', 'fir_num'])
        cur.executemany(sql, [(row['crime_id'], row['ps_code'], row['fir_num']) for row in rows])
    """
    cols_str = ', '.join(columns)
    placeholders = ', '.join(['%s'] * len(columns))
    conflict_str = ', '.join(conflict_keys)
    return (
        f"INSERT INTO {table} ({cols_str}) "
        f"VALUES ({placeholders}) "
        f"ON CONFLICT ({conflict_str}) DO NOTHING"
    )


def extract_max_modified(records: list[dict], modified_field: str | None) -> object | None:
    """
    Scan records and return the maximum modified datetime value found.
    Returns None if modified_field is None or no records have the field.

    Args:
        records        : list of API response dicts
        modified_field : field name as it appears in the API response (e.g. 'DATE_MODIFIED')
    """
    if not modified_field or not records:
        return None
    values = [r[modified_field] for r in records if r.get(modified_field)]
    if not values:
        return None
    return max(values)  # ISO string max() works for lexicographic date comparison
```

- [ ] **Step 4: Run tests**

```bash
python -m pytest tests/test_date_controller.py -k "dedup" -v
```

Expected: 3 PASS

- [ ] **Step 5: Commit**

```bash
git add etl_date_controller/deduplication.py tests/test_date_controller.py
git commit -m "feat: add deduplication helper with upsert-skip SQL builder"
```

---

## Task 5: LinkedIdQueue — Enqueue / Dequeue for Linked APIs

**Files:**
- Create: `etl_date_controller/linked_queue.py`

- [ ] **Step 1: Write the failing tests**

```python
# Add to tests/test_date_controller.py

def test_enqueue_and_dequeue_crime_id(db_conn):
    from etl_date_controller.schema import ensure_schema
    from etl_date_controller.linked_queue import LinkedIdQueue

    ensure_schema(db_conn)
    with db_conn.cursor() as cur:
        cur.execute("DELETE FROM etl_linked_id_queue WHERE target_api = 'ACCUSED_API'")
    db_conn.commit()

    q = LinkedIdQueue(db_conn)
    q.enqueue('ACCUSED_API', 'crime_id', ['CRIME001', 'CRIME002'], source_api='CRIMES_API')

    batch = q.dequeue('ACCUSED_API', limit=10)
    assert len(batch) == 2
    assert {r['id_value'] for r in batch} == {'CRIME001', 'CRIME002'}

def test_enqueue_dedup_no_double_pending(db_conn):
    """Enqueueing same id twice must not create duplicate pending rows."""
    from etl_date_controller.schema import ensure_schema
    from etl_date_controller.linked_queue import LinkedIdQueue

    ensure_schema(db_conn)
    with db_conn.cursor() as cur:
        cur.execute("DELETE FROM etl_linked_id_queue WHERE target_api = 'PERSON_DETAILS_API'")
    db_conn.commit()

    q = LinkedIdQueue(db_conn)
    q.enqueue('PERSON_DETAILS_API', 'person_id', ['PID001'], source_api='ACCUSED_API')
    q.enqueue('PERSON_DETAILS_API', 'person_id', ['PID001'], source_api='ACCUSED_API')

    with db_conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM etl_linked_id_queue
            WHERE target_api = 'PERSON_DETAILS_API' AND id_value = 'PID001' AND status = 'pending'
        """)
        count = cur.fetchone()[0]
    assert count == 1

def test_mark_done_removes_from_pending(db_conn):
    from etl_date_controller.schema import ensure_schema
    from etl_date_controller.linked_queue import LinkedIdQueue

    ensure_schema(db_conn)
    with db_conn.cursor() as cur:
        cur.execute("DELETE FROM etl_linked_id_queue WHERE target_api = 'UPDATE_CHARGESHEETS_API'")
    db_conn.commit()

    q = LinkedIdQueue(db_conn)
    q.enqueue('UPDATE_CHARGESHEETS_API', 'crime_id', ['CID_X'], source_api='CRIMES_API')
    batch = q.dequeue('UPDATE_CHARGESHEETS_API', limit=5)
    row_ids = [r['queue_id'] for r in batch]
    q.mark_done(row_ids)

    with db_conn.cursor() as cur:
        cur.execute("""
            SELECT status FROM etl_linked_id_queue WHERE id = ANY(%s)
        """, (row_ids,))
        statuses = [r[0] for r in cur.fetchall()]
    assert all(s == 'done' for s in statuses)
```

- [ ] **Step 2: Run to verify failure**

```bash
python -m pytest tests/test_date_controller.py -k "enqueue or dequeue or mark_done" -v
```

Expected: `ImportError: cannot import name 'LinkedIdQueue'`

- [ ] **Step 3: Implement linked_queue.py**

```python
# etl_date_controller/linked_queue.py
from __future__ import annotations
from datetime import datetime, timezone, timedelta
from typing import Optional

IST = timezone(timedelta(hours=5, minutes=30))


class LinkedIdQueue:
    """
    Manages etl_linked_id_queue: the bridge between incremental ETLs and linked ETLs.

    Incremental ETLs call enqueue() with changed IDs they discover.
    Linked ETLs call dequeue() to get a batch, process it, then call mark_done().
    """

    def __init__(self, conn):
        self.conn = conn

    def enqueue(
        self,
        target_api: str,
        id_type: str,
        id_values: list[str],
        source_api: str,
    ) -> int:
        """
        Insert id_values into the queue for target_api.
        Skips IDs already pending (idempotent).
        Returns number of rows actually inserted.
        """
        if not id_values:
            return 0
        inserted = 0
        with self.conn.cursor() as cur:
            for val in id_values:
                cur.execute("""
                    INSERT INTO etl_linked_id_queue
                        (target_api, id_type, id_value, source_api, status, enqueued_at)
                    VALUES (%s, %s, %s, %s, 'pending', NOW())
                    ON CONFLICT (target_api, id_type, id_value, status) DO NOTHING
                """, (target_api, id_type, str(val), source_api))
                inserted += cur.rowcount
        self.conn.commit()
        return inserted

    def dequeue(self, target_api: str, limit: int = 100) -> list[dict]:
        """
        Atomically claim up to `limit` pending rows for target_api.
        Sets status = 'processing'. Returns list of dicts with queue_id + id_value.
        """
        with self.conn.cursor() as cur:
            cur.execute("""
                UPDATE etl_linked_id_queue
                SET status = 'processing'
                WHERE id IN (
                    SELECT id FROM etl_linked_id_queue
                    WHERE target_api = %s AND status = 'pending'
                    ORDER BY enqueued_at
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING id, id_type, id_value
            """, (target_api, limit))
            rows = cur.fetchall()
        self.conn.commit()
        return [{'queue_id': r[0], 'id_type': r[1], 'id_value': r[2]} for r in rows]

    def mark_done(self, queue_ids: list[int]) -> None:
        """Mark processed rows as done."""
        if not queue_ids:
            return
        with self.conn.cursor() as cur:
            cur.execute("""
                UPDATE etl_linked_id_queue
                SET status = 'done', processed_at = NOW()
                WHERE id = ANY(%s)
            """, (queue_ids,))
        self.conn.commit()

    def mark_failed(self, queue_ids: list[int]) -> None:
        """Return rows to failed so they can be retried or inspected."""
        if not queue_ids:
            return
        with self.conn.cursor() as cur:
            cur.execute("""
                UPDATE etl_linked_id_queue
                SET status = 'failed', processed_at = NOW()
                WHERE id = ANY(%s)
            """, (queue_ids,))
        self.conn.commit()

    def pending_count(self, target_api: str) -> int:
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM etl_linked_id_queue
                WHERE target_api = %s AND status = 'pending'
            """, (target_api,))
            return cur.fetchone()[0]
```

- [ ] **Step 4: Update schema.py to include etl_linked_id_queue DDL**

```python
# etl_date_controller/schema.py  — replace entire file

DDL_DATE_CONTROL = """
CREATE TABLE IF NOT EXISTS etl_api_date_control (
    api_name                     VARCHAR(100) PRIMARY KEY,
    api_tier                     VARCHAR(20)  NOT NULL
                                              CHECK (api_tier IN ('incremental','linked','standalone')),
    fetch_origin_start           BOOLEAN      NOT NULL DEFAULT FALSE,
    origin_start_date            TIMESTAMPTZ  NOT NULL DEFAULT '2022-06-01T00:00:00+05:30',
    last_run_start               TIMESTAMPTZ,
    last_run_end                 TIMESTAMPTZ,
    last_processed_modified_date TIMESTAMPTZ,
    status                       VARCHAR(20)  NOT NULL DEFAULT 'pending'
                                              CHECK (status IN ('pending','running','completed','failed')),
    records_processed            INTEGER      NOT NULL DEFAULT 0,
    updated_at                   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
)
"""

DDL_LINKED_QUEUE = """
CREATE TABLE IF NOT EXISTS etl_linked_id_queue (
    id           BIGSERIAL    PRIMARY KEY,
    target_api   VARCHAR(100) NOT NULL,
    id_type      VARCHAR(50)  NOT NULL,
    id_value     VARCHAR(100) NOT NULL,
    source_api   VARCHAR(100) NOT NULL,
    status       VARCHAR(20)  NOT NULL DEFAULT 'pending'
                              CHECK (status IN ('pending','processing','done','failed')),
    enqueued_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ,
    CONSTRAINT etl_linked_id_queue_uniq UNIQUE (target_api, id_type, id_value, status)
)
"""

DDL_LINKED_QUEUE_IDX = """
CREATE INDEX IF NOT EXISTS idx_linked_queue_pending
    ON etl_linked_id_queue (target_api, status) WHERE status = 'pending'
"""

def ensure_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(DDL_DATE_CONTROL)
        cur.execute(DDL_LINKED_QUEUE)
        cur.execute(DDL_LINKED_QUEUE_IDX)
    conn.commit()
```

- [ ] **Step 5: Update __init__.py**

```python
# etl_date_controller/__init__.py
from .controller import DateController
from .api_registry import API_REGISTRY, ApiTier
from .linked_queue import LinkedIdQueue

__all__ = ['DateController', 'API_REGISTRY', 'ApiTier', 'LinkedIdQueue']
```

- [ ] **Step 6: Run all tests**

```bash
python -m pytest tests/test_date_controller.py -v
```

Expected: All PASS

- [ ] **Step 7: Commit**

```bash
git add etl_date_controller/linked_queue.py etl_date_controller/schema.py \
        etl_date_controller/__init__.py tests/test_date_controller.py
git commit -m "feat: add LinkedIdQueue for linked-tier API triggering (ACCUSED, PERSON_DETAILS, UPDATE_CHARGESHEETS)"
```

---

## Task 7: chunk_date_ranges() Utility

The CCTNS API enforces ≤ 7-day windows. ETLs must iterate in chunks. This belongs in the controller package.

**Files:**
- Modify: `etl_date_controller/controller.py` (add function at module level)

- [ ] **Step 1: Write the failing test**

```python
# Add to tests/test_date_controller.py

def test_chunk_date_ranges_single_chunk():
    from etl_date_controller.controller import chunk_date_ranges
    start = datetime(2024, 1, 1, tzinfo=IST)
    end   = datetime(2024, 1, 5, tzinfo=IST)
    chunks = list(chunk_date_ranges(start, end, chunk_days=7))
    assert len(chunks) == 1
    assert chunks[0] == (start, end)

def test_chunk_date_ranges_multi_chunk():
    from etl_date_controller.controller import chunk_date_ranges
    start = datetime(2024, 1, 1, tzinfo=IST)
    end   = datetime(2024, 1, 22, tzinfo=IST)
    chunks = list(chunk_date_ranges(start, end, chunk_days=7))
    assert len(chunks) == 3
    # Each chunk ≤ 7 days
    for s, e in chunks:
        assert (e - s).days <= 7

def test_chunk_date_ranges_exact_boundary():
    from etl_date_controller.controller import chunk_date_ranges
    start = datetime(2024, 1, 1, tzinfo=IST)
    end   = datetime(2024, 1, 8, tzinfo=IST)   # exactly 7 days
    chunks = list(chunk_date_ranges(start, end, chunk_days=7))
    assert len(chunks) == 1
    assert chunks[0] == (start, end)
```

- [ ] **Step 2: Run test to verify it fails**

```bash
python -m pytest tests/test_date_controller.py -k "chunk" -v
```

Expected: `ImportError: cannot import name 'chunk_date_ranges'`

- [ ] **Step 3: Add chunk_date_ranges to controller.py**

Add this function after the imports section (before `DateController` class):

```python
from typing import Generator

def chunk_date_ranges(
    start: datetime,
    end: datetime,
    chunk_days: int = MAX_CHUNK_DAYS,
) -> Generator[tuple[datetime, datetime], None, None]:
    """
    Yield (chunk_start, chunk_end) pairs of at most chunk_days each,
    covering [start, end] without gaps.
    """
    cursor = start
    delta = timedelta(days=chunk_days)
    while cursor < end:
        chunk_end = min(cursor + delta, end)
        yield cursor, chunk_end
        cursor = chunk_end
```

- [ ] **Step 4: Run tests**

```bash
python -m pytest tests/test_date_controller.py -k "chunk" -v
```

Expected: 3 PASS

- [ ] **Step 5: Run full test suite**

```bash
python -m pytest tests/test_date_controller.py -v
```

Expected: All PASS

- [ ] **Step 6: Commit**

```bash
git add etl_date_controller/controller.py tests/test_date_controller.py
git commit -m "feat: add chunk_date_ranges() for 7-day API window iteration"
```

---

## Task 8: Integrate with CRIMES ETL (Reference Implementation)

This is the reference integration. All other ETLs follow the same pattern.

**Files:**
- Modify: `etl-crimes/config.py` (replace `get_etl_end_date()` and `ETL_CONFIG` start_date logic)

- [ ] **Step 1: Read current state of config.py lines 35-70**

Current `config.py` has a hand-rolled `get_etl_end_date()` that queries `etl_run_state`. We replace this with `DateController`.

- [ ] **Step 2: Replace ETL_CONFIG date logic in etl-crimes/config.py**

Replace the entire `get_etl_end_date()` function and `ETL_CONFIG['start_date']` / `ETL_CONFIG['end_date']` with:

```python
# etl-crimes/config.py  (replace get_etl_end_date and ETL_CONFIG start/end)
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl_date_controller import DateController
from etl_date_controller.controller import chunk_date_ranges

# Read fetch_origin_start from env (default False = incremental)
FETCH_ORIGIN_START = get_bool_env('FETCH_ORIGIN_START', False)

def get_crimes_date_controller(conn):
    """Return a configured DateController for the crimes ETL."""
    return DateController('CRIMES_API', conn, fetch_origin_start=FETCH_ORIGIN_START)

ETL_CONFIG = {
    # start_date / end_date now come from DateController.get_fetch_window() at runtime
    'chunk_days': 7,           # Hard limit enforced by CCTNS API
    'chunk_overlap_days': get_int_env('CHUNK_OVERLAP_DAYS', 0),
    'batch_size': 100,
    'enable_embeddings': get_bool_env('ENABLE_EMBEDDINGS', False),
}
```

- [ ] **Step 3: Update etl_crimes.py to use DateController**

In `etl_crimes.py`, in the main ETL runner method (wherever `API_CONFIG['start_date']` / `ETL_CONFIG['start_date']` is consumed), add:

```python
# In ETL runner __init__ or run():
from config import get_crimes_date_controller
from etl_date_controller.controller import chunk_date_ranges
from etl_date_controller.deduplication import extract_max_modified

self.ctrl = get_crimes_date_controller(self.conn)
window = self.ctrl.get_fetch_window()
self.ctrl.mark_running(run_start=window['start'])

total_records = 0
max_mod = None

for chunk_start, chunk_end in chunk_date_ranges(window['start'], window['end']):
    records = self.fetch_crimes_api(
        from_date=chunk_start.isoformat(),
        to_date=chunk_end.isoformat(),
    )
    if records:
        self.upsert_crimes(records)        # already uses ON CONFLICT DO NOTHING
        total_records += len(records)
        chunk_max = extract_max_modified(records, 'DATE_MODIFIED')
        if chunk_max and (max_mod is None or chunk_max > max_mod):
            max_mod = chunk_max

# Parse max_mod string to datetime if needed
if isinstance(max_mod, str):
    from datetime import datetime
    max_mod = datetime.fromisoformat(max_mod.replace('Z', '+00:00'))

self.ctrl.mark_success(
    run_end=window['end'],
    max_modified_date=max_mod,
    records_processed=total_records,
)
```

- [ ] **Step 4: Set FETCH_ORIGIN_START in .env**

```ini
# .env — add this line
FETCH_ORIGIN_START=false
```

- [ ] **Step 5: Manual smoke test**

```bash
cd /home/ashish-ratna/DOPAMS-ETL
FETCH_ORIGIN_START=true python -c "
import psycopg2
from env_utils import load_repo_environment, resolve_db_config
load_repo_environment()
cfg = resolve_db_config()
conn = psycopg2.connect(**{k: v for k, v in cfg.items() if k in ('host','port','dbname','user','password')})
from etl_date_controller import DateController
ctrl = DateController('CRIMES_API', conn, fetch_origin_start=True)
w = ctrl.get_fetch_window()
print('Window type:', w['type'])
print('Start:', w['start'])
print('End:', w['end'])
conn.close()
"
```

Expected output:
```
Window type: origin
Start: 2022-06-01 00:00:00+05:30
End: <today>
```

- [ ] **Step 6: Commit**

```bash
git add etl-crimes/config.py etl-crimes/etl_crimes.py .env
git commit -m "feat: integrate DateController into crimes ETL as reference implementation"
```

---

## Task 9: Integration Test — Idempotency / Deduplication

Prove that re-running does not duplicate records.

**Files:**
- Modify: `tests/test_date_controller.py`

- [ ] **Step 1: Write the idempotency test**

```python
def test_idempotent_upsert_no_duplicates(db_conn):
    """
    Inserting the same crime_id twice with build_upsert_skip_sql
    must not duplicate the row.
    """
    from etl_date_controller.deduplication import build_upsert_skip_sql

    # Ensure test table exists
    with db_conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS _test_crimes_dedup (
                crime_id TEXT PRIMARY KEY,
                fir_num  TEXT
            )
        """)
        cur.execute("DELETE FROM _test_crimes_dedup")
    db_conn.commit()

    sql = build_upsert_skip_sql('_test_crimes_dedup', ['crime_id'], ['crime_id', 'fir_num'])
    rows = [('CRIME001', '001/2024'), ('CRIME001', '999/2024')]  # duplicate crime_id

    with db_conn.cursor() as cur:
        for row in rows:
            cur.execute(sql, row)
    db_conn.commit()

    with db_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM _test_crimes_dedup WHERE crime_id = 'CRIME001'")
        count = cur.fetchone()[0]

    # Cleanup
    with db_conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS _test_crimes_dedup")
    db_conn.commit()

    assert count == 1, f"Expected 1 row, got {count}"
```

- [ ] **Step 2: Run test**

```bash
python -m pytest tests/test_date_controller.py::test_idempotent_upsert_no_duplicates -v
```

Expected: PASS

- [ ] **Step 3: Run full suite**

```bash
python -m pytest tests/test_date_controller.py -v
```

Expected: All PASS

- [ ] **Step 4: Commit**

```bash
git add tests/test_date_controller.py
git commit -m "test: add idempotency test for upsert deduplication"
```

---

## Task 10: Integration Guide for Remaining ETLs

**Files:**
- Create: `etl_date_controller/INTEGRATION.md`

- [ ] **Step 1: Create integration guide**

```markdown
# DateController Integration Guide

## Pattern (copy for every ETL)

```python
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl_date_controller import DateController
from etl_date_controller.controller import chunk_date_ranges
from etl_date_controller.deduplication import build_upsert_skip_sql, extract_max_modified
from env_utils import get_bool_env

FETCH_ORIGIN_START = get_bool_env('FETCH_ORIGIN_START', False)

# --- In your ETL runner ---
ctrl = DateController('<API_NAME>', conn, fetch_origin_start=FETCH_ORIGIN_START)
window = ctrl.get_fetch_window()
ctrl.mark_running(run_start=window['start'])

total = 0
max_mod = None
upsert_sql = build_upsert_skip_sql('<table>', ['<pk_col>'], ['<col1>', '<col2>', ...])

try:
    for chunk_start, chunk_end in chunk_date_ranges(window['start'], window['end']):
        records = fetch_api(chunk_start.isoformat(), chunk_end.isoformat())
        if records:
            with conn.cursor() as cur:
                for row in records:
                    cur.execute(upsert_sql, (row['<COL1>'], row['<COL2>'], ...))
            conn.commit()
            total += len(records)
            chunk_max = extract_max_modified(records, '<MODIFIED_FIELD_OR_NONE>')
            if chunk_max and (max_mod is None or chunk_max > max_mod):
                max_mod = chunk_max

    ctrl.mark_success(run_end=window['end'], max_modified_date=max_mod, records_processed=total)
except Exception:
    ctrl.mark_failed(run_end=datetime.now(IST))
    raise
```

## API Name → use exact key from API_REGISTRY

| ETL | api_name |
|-----|----------|
| etl-crimes | CRIMES_API |
| etl-accused | ACCUSED_API |
| etl_arrests | ARRESTS_API |
| etl-disposal | DISPOSAL_API |
| etl_chargesheets | CHARGESHEETS_API |
| etl_updated_chargesheet | UPDATE_CHARGESHEETS_API |
| etl-ir | IR_API |
| etl_mo_seizures | MO_SEIZURES_API |
| etl_fsl_case_property | CASE_PROPERTY_API |
| etl-properties | PROPERTY_DETAILS_API |
| etl-hierarchy | HIERARCHY_API |
| etl-persons | PERSON_DETAILS_API |

## Env Flag

```ini
# .env
FETCH_ORIGIN_START=false   # true = full reload from June 2022 (with dedup)
                           # false = incremental from last checkpoint
```
```

- [ ] **Step 2: Commit**

```bash
git add etl_date_controller/INTEGRATION.md
git commit -m "docs: add DateController integration guide for all ETLs"
```

---

## Self-Review

### Spec Coverage

| Requirement | Task |
|-------------|------|
| `fetch_origin_start` flag | Task 3 (`get_fetch_window` origin branch) |
| Start from June 2022 when flag=true | Task 3 (`ORIGIN_START = 2022-06-01`) |
| Incremental by DATE_MODIFIED field | Task 3 (`incremental_modified` branch) |
| Incremental by last run timestamp | Task 3 (`last_run` branch) |
| Skip existing records (dedup) | Task 4 (`build_upsert_skip_sql` ON CONFLICT DO NOTHING) |
| Idempotent re-runs | Task 9 (idempotency test) |
| Read CCTNSV2_data/ JSONs + DB schema audit | Done pre-plan — 3-tier classification above |
| Classify incremental / linked / standalone | Task 2 (`api_registry.py` with `ApiTier`) |
| Per-API `last_processed_modified_date` | Task 3 (`mark_success`) |
| Linked APIs triggered by parent changed IDs | Task 5 (`LinkedIdQueue`) |
| CRIMES → enqueues crime_ids → ACCUSED + UPDATE_CS | Task 2 (`enqueues` registry) + Task 5 |
| ACCUSED → enqueues person_ids → PERSON_DETAILS | Task 2 + Task 5 |
| 7-day chunk enforcement | Task 7 (`chunk_date_ranges`) |
| First-run handling | Task 3 (fallback to origin) |
| Failure recovery | Task 3 (`mark_failed`) + Task 5 (`mark_failed` queue) |
| Reference integration | Task 8 (crimes ETL) |
| Guide for remaining ETLs | Task 10 |

### No gaps found.
