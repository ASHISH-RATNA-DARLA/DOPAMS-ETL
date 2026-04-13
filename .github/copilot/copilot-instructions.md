# GitHub Copilot Instructions

## Priority Guidelines

When generating code for this repository:

1. Version compatibility first. Use only language and library features compatible with versions declared in repository dependency files.
2. Follow repo context files first when present under .github/copilot.
3. If a rule is missing, copy patterns from similar modules already in this repo.
4. Preserve the established ETL module boundaries and sequencing conventions (ETL-first priority).
5. Prioritize maintainability, performance, security, and testability, in that order observed in existing code.

## Technology And Version Baseline

Use these observed constraints as the minimum compatibility baseline.

### Language

- Python is the primary language.
- Scripts use python3 shebangs and standard library typing/concurrency features compatible with modern Python 3.
- Keep Python compatibility dependency-file driven. Do not lock to a specific Python minor version unless a version file or project config explicitly requires it.

### Core Dependencies (Observed)

From root requirements and module requirements:

- psycopg2-binary >=2.9.9 or >=2.9.10,<3.0
- pymongo >=4.6.0
- python-dotenv >=1.0.0
- requests >=2.31.0
- pandas >=2.1.0
- numpy >=1.26.0
- tqdm >=4.66.1
- colorlog >=6.8.0
- Flask >=3.0.0
- flask-cors >=4.0.0
- flask-limiter >=3.5.0
- SQLAlchemy >=2.0.23
- redis >=5.0.1
- langchain >=0.2.0
- langgraph >=0.0.62
- openai >=1.3.0

Generate code that remains compatible with these ranges and avoids requiring unobserved frameworks.

## Architecture Consistency

This repository is a Python ETL monorepo with module-based jobs and shared utilities.

### Top-Level Structure

- API ETL modules are folder-scoped, each usually containing config.py, etl_*.py, and optional requirements.txt.
- SQL assets include materialized view definitions, migration scripts, and repair scripts.
- A master orchestrator executes ordered ETL blocks from config text inputs.
- There is a chatbot service in chatbot/ with Flask plus DB and agent layers.

### Boundaries To Preserve

- Keep each ETL module self-contained; prefer local config.py imports and module-specific constants.
- Reuse shared DB pooling utilities from db_pooling.py instead of introducing alternate pooling abstractions.
- Keep orchestration logic in etl_master/ and module execution logic in each module folder.
- Keep migration logic idempotent and safe for repeated execution.

## Codebase Patterns To Follow

### Python Module Conventions

Observed recurring conventions:

- Module-level constants in uppercase (for table names, timezone offsets, API defaults).
- Config dictionaries loaded via dotenv and os.getenv in config.py.
- ETL implemented as classes with:
  - setup/connect methods
  - process/fetch/transform/load methods
  - stats dict for counters
  - optional threading locks for shared mutable stats
- Entry points guarded with if __name__ == '__main__':
- Logging initialized near top of file, often with colorlog and structured formats.

### Logging Patterns

- Use logger.info/warning/error/debug consistently.
- Existing code includes clear operational markers and progress logs.
- Keep log messages actionable and tied to ETL steps (chunk, table, record counts, API calls, failures).

### Error Handling Patterns

- Use try/except around external boundaries (HTTP calls, DB operations, file IO).
- Continue processing where safe (chunk-level or record-level) and increment failure counters.
- Roll back DB transactions on SQL errors; commit successful units explicitly.
- Use defensive fallbacks for optional files/paths and missing env vars when existing code does so.

### Concurrency And Throughput Patterns

- Existing ETLs use ThreadPoolExecutor for API and processing concurrency.
- Existing DB access prefers connection pooling and batched inserts (execute_batch/execute_values).
- Keep concurrency bounded and tied to pool capacity where applicable.

### SQL And Migration Patterns

Observed migration style:

- Use BEGIN/COMMIT blocks for transactional changes.
- Prefer idempotent DDL:
  - ADD COLUMN IF NOT EXISTS
  - CREATE INDEX IF NOT EXISTS
  - CREATE OR REPLACE VIEW
  - guarded DO $$ checks for constraints
- Normalize blanks to NULL using NULLIF(BTRIM(col), '').
- Keep production-safe, non-destructive semantics unless explicitly requested.

## Security And Data Safety

- Read credentials from environment variables; do not hardcode secrets.
- Validate and normalize API payload fields before DB writes.
- Preserve primary/unique key behavior and overwrite semantics already present.
- Avoid widening write scope unintentionally; keep updates targeted.

## Documentation Expectations

- Match existing docstring style: concise module/class/function docstrings with practical context.
- Add comments only for non-obvious logic (branching, compatibility, migration safety).
- Prefer operationally useful markdown updates when behavior changes.

## Testing And Validation Patterns

Testing in this repo is mixed and script-heavy.

- Existing tests include script-style test_*.py files with inline assertions.
- Shell-based validation scripts and diagnostics are common.
- For ETL changes, validate using:
  - reproducible command/query
  - row count checks
  - sample-record spot checks
  - dependent module/view smoke checks when relevant

Default validation policy for ETL edits: use script-style validation checks and diagnostics already common in this repo. Add automated tests only when explicitly requested.

## File Selection Rules For New Changes

Before writing code:

1. Find a sibling module with similar ETL purpose.
2. Copy import organization, class layout, logging shape, and error-handling style from that sibling.
3. Keep naming aligned with existing patterns:
   - folders like etl-*/etl_*
   - classes like *ETL
   - methods like connect_db, fetch_*, process_*, ensure_*
4. Keep changes minimal and localized to the affected module and its immediate dependencies.

## Conflict Resolution Rules

If patterns conflict across old and new code:

1. Prefer the pattern used in the nearest module folder.
2. Prefer newer migration style using idempotent DDL and explicit safety comments.
3. Prefer patterns that preserve backward compatibility and non-destructive behavior.

## Explicit Do And Do Not

Do:

- Generate code that works with existing dependency ranges.
- Keep ETL logic chunked, resumable, and observable through logs and counters.
- Maintain schema compatibility and idempotent migration behavior.

Do not:

- Introduce unobserved frameworks or architectural rewrites.
- Replace module-level config patterns with incompatible global configuration systems.
- Introduce destructive SQL defaults in migration scripts.
- Assume standards that are not visible in this repository.

## Practical Examples To Emulate

Use these as pattern references when generating code:

- ETL orchestration and ordered execution: etl_master/master_etl.py
- Shared DB pooling and batch write patterns: db_pooling.py
- Module config pattern with dotenv: etl-ir/config.py
- Chunked ETL with stats and logging: etl-disposal/etl_disposal.py
- Idempotent SQL migration style: etl_updated_chargesheet/migrations/2026-04-10_update_chargesheets_alignment.sql
- Flask service bootstrap pattern: chatbot/app.py

## Final Generation Rule

When in doubt, prioritize consistency with surrounding repository code over external best practices or newer language/library features.
