1️⃣ Repository Tree
Top-level structure (ETL + infra focus)

Root
README.md, EXECUTIVE_SUMMARY.md, ETL-EXECUTIVE_SUMMARY_DESIGN.md, ETL-SOLUTION_DESIGN_DOCUMENT.md, ETL_DIAGNOSTICS.md, PERFORMANCE_AUDIT_REPORT.md, PERFORMANCE_AUDIT_COMPLETE.md, QUICK_REFERENCE.md, IMPLEMENTATION_ROADMAP.md, guide.md, nohup-cmd.md, shared-LLMService.md
requirements.txt (consolidated deps for all ETL + chatbot)
db_pooling.py (shared DB connection-pool and batch utilities)
quick_start.py, validate_etl.py, test_issue.py, query_optimizer.py
Master orchestrator
etl_master/
master_etl.py (master ETL runner)
input.txt (full daily schedule, Orders 1–29)
input-2back.txt (minimal test / alternate run, Order 1 only)
input-resume-19.txt (resume schedule starting at Order 19)
run.sh
__pycache__/master_etl.cpython-313.pyc
Core API-based ETLs
etl-crimes/
etl_crimes.py
config.py
requirements.txt
migrate_crimes_fir_copy.py
test_date_range.sh
__pycache__/config.cpython-313.pyc, __pycache__/etl_crimes.cpython-313.pyc, __pycache__/migrate_crimes_fir_copy.cpython-313.pyc
etl-accused/
etl_accused.py
etl_accused-original-working-fine.py
config.py
requirements.txt
1, 2 (scratch / data files)
__pycache__/*
etl-persons/
etl_persons.py
config.py
requirements.txt
1 (scratch)
__pycache__/*
etl-properties/
etl_properties.py
config.py
requirements.txt
__pycache__/*
etl-ir/
ir_etl.py
config.py
requirements.txt
__pycache__/*
etl-disposal/
etl_disposal.py
config.py
__pycache__/*
etl_arrests/
etl_arrests.py
config.py
__pycache__/*
etl_mo_seizures/
etl_mo_seizure.py
config.py
__pycache__/* (from Glob)
Schema / master-data ETLs
etl-hierarchy/
etl_hierarchy.py
config.py
requirements.txt
__pycache__/*
etl_case_status/
update_crimes.py
requirements.txt
__pycache__/*
section-wise-case-clarification/
process_sections.py
requirements.txt
__pycache__/*
update-state-country/
update-state-country.py
ref.txt, 1
__pycache__/*
domicile_classification/
domicile_classifier.py
config.py (has its own)
requirements.txt
__pycache__/*
Chargesheets ETLs
etl_chargesheets/
etl_chargesheets.py
config.py
(requirements not explicitly seen but implied)
etl_updated_chargesheet/
etl_update_chargesheet.py
config.py
(no local requirements file; uses root requirements.txt)
File-based ETL infrastructure
etl-files/
config.py (more advanced shared config, includes disposal/arrests/seizures)
etl-files.py (orchestration wrapper for file-based ETL)
diagnose_new_apis.py
etl_pipeline_files/ (package-like, already close to proper Python package)
__init__.py
requirements.txt
config/
__init__.py
database.py
api_config.py
extract/
__init__.py
crimes_extractor.py
chargesheets_extractor.py
persons_extractor.py
property_extractor.py
mo_seizures_extractor.py
fsl_case_property_extractor.py
base_extractor.py
interrogation_extractor.py (from earlier Glob)
transform/
__init__.py
load/
__init__.py
files_loader.py
utils/
__init__.py
date_utils.py
idempotency.py
logger.py
main.py
main_standalone.py
check_setup.py
api-ref.txt
etl_files_media_server/
config.py
etl_files_media_server/__init__.py
etl_files_media_server/main.py (already used as python -m etl_files_media_server.main)
test_file_api.py
diagnose_new_apis.py
start_downloader.sh
requirements.txt
update_file_urls_with_extensions/
update_file_urls_with_extensions.py
requirements.txt
“Brief facts” / derived ETLs
brief_facts_accused/
config.py
db.py
extracted logic (extractor.py, test_accused_extraction.py, reproduce_issue.py, llm_factory.py from Glob)
log files brief_facts_accused-*.log
requirements.txt
brief_facts_drugs/
config.py
db.py
main.py, extractor.py, migrate_data.py, update_schema.py, process_seizure_worth_backfill.py, debug_long_text.py, others
data files (drug-types.txt, ref.txt)
requirements.txt
Data cleaning / enrichment jobs
fix_fullname/
fix_person_names.py
fix_all_fullnames.py
fix_name_field.py
fix_surname_field.py
data_cleanup.py
create_person_deduplication_table.py
requirements.txt
__pycache__/*, scratch 1
drug_standardization/
drug_standardization.py
drug_mappings.json
requirements.txt
Refresh / views / support
etl_refresh_views/
views_refresh_sql.py
grant_privileges_helper.py
requirements.txt
run_etl.sh
Other support scripts
etl_mongo-to-postgresql/
etl_migration.py
export_mongodb_records.py
update_crimes_dates.py
requirements.txt
state-districts.csv
etl_fsl_case_property/
etl_fsl_case_property.py
config.py
(no separate requirements file observed; uses root)
Chatbot / non-ETL
chatbot/ (full FastAPI/Flask-style app with agents, database layer, etc.)
core/llm_service.py
Other analytic tools (performance_profiler.py, etc.)
.env and logs

No .env file is checked into the repo (no matches for *.env). All ETL configs use python-dotenv to load environment variables at runtime from an external .env.
ETL scripts create per-job logs/ directories in their own working directory (e.g., logs/crimes_*.log, logs/accused_*.log, etc.) on execution; these are runtime artifacts, not core source.
2️⃣ ETL Execution Order (Master Runner)
Master runner

File: etl_master/master_etl.py
Execution: typically from the master directory, e.g.:
cd etl_master
python3 master_etl.py --config input.txt (or alternate config file)
Execution model

master_etl.py:
Reads a configuration file (default input.txt, overridable by --config).
Parses it into an ordered list of “process blocks” labeled [Order N], each with:
Optional human-readable name line (e.g. crimes).
One or more shell commands (usually cd, source venv, python3 ...).
For each block, it:
Joins commands with && into a single shell string.
Wraps it as:
set -o pipefail; ( <commands && commands> ) 2>&1 | tee output.log
Executes using:
subprocess.run(..., shell=True, executable='/bin/bash', check=True, text=True)
Logs high-level status to master_etl.log.
Waits 5 seconds between blocks.
Primary daily schedule (etl_master/input.txt)

Execution order (logical job names and commands):

hierarchy
cd /data-drive/etl-process-dev/etl-hierarchy
source /data-drive/etl-process-dev/venv/bin/activate
python3 etl_hierarchy.py
crimes
cd /data-drive/etl-process-dev/etl-crimes
source /data-drive/etl-process-dev/venv/bin/activate
python3 etl_crimes.py
class_classification (section-wise-case-clarification)
cd /data-drive/etl-process-dev/section-wise-case-clarification
source /data-drive/etl-process-dev/venv/bin/activate
python3 process_sections.py
case_status
cd /data-drive/etl-process-dev/etl_case_status
source .../venv/bin/activate
python3 update_crimes.py
accused
cd /data-drive/etl-process-dev/etl-accused
source .../venv/bin/activate
python3 etl_accused.py
persons
cd /data-drive/etl-process-dev/etl-persons
source .../venv/bin/activate
python3 etl_persons.py
update-state-country
cd /data-drive/etl-process-dev/update-state-country
source .../venv/bin/activate
python3 update-state-country.py
domicile_classification
cd /data-drive/etl-process-dev/domicile_classification
source .../venv/bin/activate
python3 domicile_classifier.py
fix_person_names
cd /data-drive/etl-process-dev/fix_fullname
source .../venv/bin/activate
python3 fix_person_names.py
full_name_fix
same dir; python3 fix_all_fullnames.py
name_fix
same dir; python3 fix_name_field.py
surname_fix
same dir; python3 fix_surname_field.py
properties
cd /data-drive/etl-process-dev/etl-properties
source .../venv/bin/activate
python3 etl_properties.py
IR (interrogation reports)
cd /data-drive/etl-process-dev/etl-ir
source .../venv/bin/activate
python3 ir_etl.py
disposal
cd /data-drive/etl-process-dev/etl-disposal
source .../venv/bin/activate
python3 etl_disposal.py
arrests
cd /data-drive/etl-process-dev/etl_arrests
source .../venv/bin/activate
python3 etl_arrests.py
mo_seizures
cd /data-drive/etl-process-dev/etl_mo_seizures
source .../venv/bin/activate
python3 etl_mo_seizure.py
chargesheets
cd /data-drive/etl-process-dev/etl_chargesheets
source .../venv/bin/activate
python3 etl_chargesheets.py
updated_chargesheet
cd /data-drive/etl-process-dev/etl_updated_chargesheet
source .../venv/bin/activate
python3 etl_update_chargesheet.py
fsl_case_property
cd /data-drive/etl-process-dev/etl_fsl_case_property
source .../venv/bin/activate
python3 etl_fsl_case_property.py
refresh_views (1st time)
cd /data-drive/etl-process-dev/etl_refresh_views
source .../venv/bin/activate
python3 views_refresh_sql.py
brief_facts_accused
cd /data-drive/etl-process-dev/brief_facts_accused
source .../venv/bin/activate
python3 accused_type.py
brief_facts_drugs
cd /data-drive/etl-process-dev/brief_facts_drugs
source .../venv/bin/activate
python3 main.py
drug_standardization
cd /data-drive/etl-process-dev/drug_standardization
source .../venv/bin/activate
python3 drug_standardization.py
refresh_views (2nd time)
same as Order 21
update_file_id (file URL metadata ETL)
cd /data-drive/etl-process-dev/etl-files/etl_pipeline_files
source .../venv/bin/activate
python3 main_standalone.py
files_download_media_server
cd /data-drive/etl-process-dev/etl-files/etl_files_media_server
source .../venv/bin/activate
python3 -m etl_files_media_server.main
update_file_extensions
cd /data-drive/etl-process-dev/etl-files/update_file_urls_with_extensions
source .../venv/bin/activate
python3 update_file_urls_with_extensions.py
refresh_views (3rd time)
same as Order 21
Alternate configs

input-2back.txt: Only Order 1 (persons ETL) – for focused reruns.
input-resume-19.txt: Resume from Order 19 to 29 after a partial run.
3️⃣ Shared Modules
Key shared modules

db_pooling.py (root)
Provides:
PostgreSQLConnectionPool (singleton, ThreadedConnectionPool-backed).
get_connection_context() context manager.
Convenience functions get_db_connection(), return_db_connection().
compute_safe_workers(pool, requested_workers, reserved=5).
ConnectionLimiter semaphore wrapper.
BatchInsertOptimizer and batch_insert/batch_update/batch_upsert helpers.
config.py (per ETL + etl-files/config.py)
A family of nearly-identical modules (see duplication section).
Define DB_CONFIG, API_CONFIG, ETL_CONFIG, LOG_CONFIG, TABLE_CONFIG.
ETLs importing config (local to their folder)

Imports: from config import DB_CONFIG, API_CONFIG, ETL_CONFIG, LOG_CONFIG, TABLE_CONFIG (or subset):

etl-crimes/etl_crimes.py, etl-crimes/migrate_crimes_fir_copy.py
etl-accused/etl_accused.py, etl-accused/etl_accused-original-working-fine.py
etl-persons/etl_persons.py
etl-properties/etl_properties.py
etl-ir/ir_etl.py
etl-disposal/etl_disposal.py
etl-hierarchy/etl_hierarchy.py
etl_arrests/etl_arrests.py
etl-files/etl-files.py (imports DB_CONFIG, API_CONFIG, TABLE_CONFIG, LOG_CONFIG)
etl-files/diagnose_new_apis.py (imports DB_CONFIG)
Each of these folders has its own config.py file; the content is mostly copy-pasted with small evolution (the etl-files/config.py variant knows about disposal/arrests/seizures and extra API endpoints).

ETLs importing db_pooling

Imports: from db_pooling import PostgreSQLConnectionPool and/or compute_safe_workers:

etl-crimes/etl_crimes.py
etl-accused/etl_accused.py
etl-persons/etl_persons.py
etl-properties/etl_properties.py
etl-ir/ir_etl.py
etl-disposal/etl_disposal.py
etl_mo_seizures/etl_mo_seizure.py
etl_arrests/etl_arrests.py
etl-hierarchy/etl_hierarchy.py
etl_case_status/update_crimes.py
section-wise-case-clarification/process_sections.py
update-state-country/update-state-country.py
domicile_classification/domicile_classifier.py
fix_fullname/fix_person_names.py, fix_fullname/fix_name_field.py, fix_fullname/fix_surname_field.py, fix_fullname/fix_all_fullnames.py
Other support scripts (e.g., quick_start.py references recommended usage)
All of these use the same root db_pooling.py; no duplicates of that module.

Other shared-ish utilities

etl-files/etl_pipeline_files/utils/date_utils.py, idempotency.py, logger.py – shared within the file ETL package.
etl-files/etl_pipeline_files/config/database.py, api_config.py – shared config for file ETL.
These are not yet used by the “core” API ETLs (crimes/accused/persons/etc.), but represent a more modular, package-style subproject.
4️⃣ Import Patterns
Patterns inside core ETLs

Dynamic sys.path hack (to reach root-level shared modules):

In all the “dash” ETLs (etl-crimes, etl-accused, etl-persons, etl-properties, etl-ir, etl-disposal, etl-hierarchy:

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_pooling import PostgreSQLConnectionPool
from config import DB_CONFIG, API_CONFIG, ETL_CONFIG, LOG_CONFIG, TABLE_CONFIG
This assumes:

The ETL folder is one level under a root where db_pooling.py and its config.py live.
The script is executed as python etl-*/etl_*.py with __file__ inside that folder.
Local config.py imports:

Each ETL expects its own config.py in the same directory; no package prefix:
from config import DB_CONFIG, API_CONFIG, ETL_CONFIG, LOG_CONFIG, TABLE_CONFIG
This hard-wires all imports to “same folder” semantics, not a(package)-relative import.
Direct root-relative imports do not exist; everything uses either:

sys.path.append(...) + bare module import (db_pooling, config), or
Local imports within nested packages (e.g., etl-files/etl_pipeline_files using from .config import database etc.)
Implications

Import assumptions:

ETL scripts are tightly coupled to a particular directory layout:
Top-level root:
db_pooling.py
Several etl-* and etl_* folders, each with its own config.py and main script.
Scripts break if:
Run with a different working directory without __file__ being under etl-*.
Directory layout changes (e.g., moved into dopams_etl_pipelines/ package) without adjusting imports.
Path hacks:

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) is used instead of package-relative imports.
This is the key source of ModuleNotFoundError when:
Running from different working directories.
Trying to execute scripts via python -m package.module without adjusting sys.path.
5️⃣ Configuration System
Config module behavior

Each ETL’s config.py (for crimes, accused, persons, properties, IR, disposal, hierarchy, arrests) follows the same pattern:

Loads .env via load_dotenv().
Builds DB_CONFIG from:
POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_PORT.
Builds API_CONFIG from:
DOPAMAS_API_URL, DOPAMAS_API_KEY, API_TIMEOUT, API_MAX_RETRIES.
Convenience URLs: crimes_url, accused_url, persons_url, hierarchy_url, ir_url, sometimes files_url, disposal_url, arrests_url, seizures_url depending on variant.
Builds ETL_CONFIG:
start_date (fixed string '2022-01-01T00:00:00+05:30' for logging only).
end_date (placeholder '2025-12-31T23:59:59+05:30').
chunk_days (5).
chunk_overlap_days (from env CHUNK_OVERLAP_DAYS).
batch_size (100).
enable_embeddings (from env ENABLE_EMBEDDINGS).
LOG_CONFIG:
LOG_LEVEL, log format and date format.
TABLE_CONFIG:
Maps logical table names (crimes, accused, persons, hierarchy, properties, disposal, arrests, many ir_* tables) to:
os.getenv('<TABLE>_TABLE', default_name).
etl-files/config.py is a superset variant:

Adds flexible endpoint logic via get_api_endpoint helper and supports DOPAMAS_API_URL2, per-endpoint base URLs and endpoint overrides (*_API_BASE_URL, *_API_ENDPOINT).
Adds disposal / arrests / seizures endpoints and table names.
Where config is loaded and used

Loaded in every ETL script via from config import ....
Used for:
DB connections (passed into PostgreSQLConnectionPool, or in older modules, into direct psycopg2.connect – though in this repo, ETLs use pooling).
API base URLs and keys for upstream API calls.
Date range and chunking parameters (e.g., ETL_CONFIG['chunk_days'], ETL_CONFIG['chunk_overlap_days']).
Logging level and fmt for colorlog in each ETL.
Table names for SELECT and INSERT/UPDATE operations across all ETLs.
Environment variables relied upon

Database
POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_PORT
API
DOPAMAS_API_URL (and optionally DOPAMAS_API_URL2)
DOPAMAS_API_KEY
API_TIMEOUT, API_MAX_RETRIES
*_API_BASE_URL / *_API_ENDPOINT (for flexible per-endpoint routing)
ETL behavior
CHUNK_OVERLAP_DAYS
ENABLE_EMBEDDINGS
EMBEDDING_MODEL
Job-specific:
MAX_WORKERS (global concurrency cap in some ETLs)
CRIMES_CHUNK_WORKERS, ACCUSED_CHUNK_WORKERS, ACCUSED_INTER_CHUNK_SLEEP
ACCUSED_RUN_MODE (1 incremental, 0 full reset)
Table overrides
CRIMES_TABLE, ACCUSED_TABLE, PERSONS_TABLE, HIERARCHY_TABLE, PROPERTIES_TABLE, DISPOSAL_TABLE, ARRESTS_TABLE, and many IR_*_TABLE envs.
.env files in repo

None checked in. All configs expect a .env in the runtime environment (e.g. on the server) but it is not part of this git repo.
6️⃣ Dependency Graph Between ETLs
Core data flow (logical)

etl-hierarchy

Populates hierarchy table.
Used by:
etl-crimes (checks PS_CODE in hierarchy before inserting crimes).
etl-accused indirectly (through crimes date fallbacks and PS_CODE validation in crime-level fallback).
Other modules that validate PS_CODEs (e.g. IR, properties, disposal indirectly rely on CRIMES_TABLE, which itself references PS_CODE).
etl-crimes

Populates crimes table (CRIMES_TABLE / crimes).
Used by:
section-wise-case-clarification/process_sections.py (classification on crimes).
etl_case_status/update_crimes.py (case status updates on crimes).
etl-accused (requires crimes row to exist for each CRIME_ID).
etl-properties (FK to CRIMES_TABLE via crime_id).
etl-ir (FK to CRIMES_TABLE via CRIME_ID).
etl-disposal (FK to CRIMES_TABLE via crime_id).
etl_arrests (FK to CRIMES_TABLE via CRIME_ID).
etl_mo_seizures (FK to CRIMES_TABLE).
etl_chargesheets and etl_updated_chargesheet (chargesheet data for each crime).
etl_refresh_views (views over all of the above).
Downstream “brief facts” ETLs (operating on crimes and related tables).
etl-accused

Writes accused table.
Ensures stub rows in persons table for referenced PERSON_IDs (if they do not exist).
Has fallback to insert / update crimes if CRIME_ID missing in crimes table.
Used by:
etl-persons (derives PERSON_IDs to pull detailed person info).
Brief facts and downstream analytics on accused.
etl-persons

Uses accused table to discover PERSON_IDs requiring enrichment.
Writes detailed person rows into persons table.
Used by:
etl_arrests (PERSON_ID FK checks).
domicile_classification and other analytics that rely on person attributes.
IR ETL, which references PERSON_IDs across several nested entities.
section-wise-case-clarification

Reads from crimes (and likely hierarchy) to classify cases by section.
Feeds classification labels back into the DB (same crimes or related tables).
etl_case_status

Reads crimes and updates case_status field in crimes.
update-state-country

Updates address country/state info, likely in persons and/or hierarchy-related tables.
domicile_classification

Depends on persons (addresses, nationality, etc.) and possibly crimes, to classify domicile.
fix_fullname suite

Operates primarily on persons (name fields normalization).
Sequenced after persons ETL and domicile-related updates.
etl-properties

Requires crimes (FK on crime_id) and uses an in-memory list of crime_ids to enforce FK.
Inserts into properties and properties_pending_fk to handle missing CRIME_IDs.
Retries pending FKs once more after full run.
etl-ir (Interrogation Reports)

Requires crimes (FK on CRIME_ID).
Writes to interrogation_reports and multiple normalized IR tables.
Heavy relationships to persons via PERSON_ID fields, but does not enforce person FK in all cases; mainly ensures CRIME_ID exists and logs missing FKs into ir_pending_fk.
etl-disposal

Requires crimes (FK on crime_id) – logs and skips disposals with unknown CRIME_IDs.
Writes to disposal table.
etl_arrests

Requires crimes (FK on CRIME_ID) – skips records with unknown CRIME_IDs.
PERSON_ID is optional: if unknown in persons, record is still inserted with person_id = NULL and logged.
Writes to arrests table.
etl_mo_seizures

Similar to properties and IR:
Uses pooled connections via db_pooling.
Targets mo_seizures-like tables; uses API_CONFIG['seizures_url'] and TABLE_CONFIG.
etl_chargesheets / etl_updated_chargesheet

Operate on chargesheet-level data tied to crimes.
etl_updated_chargesheet is sequenced after etl_chargesheets in master flow.
etl_refresh_views

End-of-pipeline; runs database SQL to refresh materialized views and set privileges.
Executed 3 times across the master schedule (after core structural ETLs and after “brief facts” and file ETLs).
brief_facts_* and drug_standardization

Depend on crimes, accused, persons, IR, properties, etc., to derive analytic datasets.
Sequenced after all core ETLs and at least one refresh_views.
etl-files family

Depends on CRIMES_TABLE, PROPERTIES_TABLE, and other base data to build file metadata and download actual media.
etl_files_media_server is already run as python3 -m etl_files_media_server.main (module-style execution).
High-level graph (simplified)

hierarchy ↓
crimes ↓
section-wise-case-clarification, case_status ↓
accused ↓
persons ↓
update-state-country, domicile_classification, fix_fullname suite ↓
Branch A: properties → brief_facts_* → drug_standardization
Branch B: IR → IR-derived analytics
Branch C: disposal
Branch D: arrests
Branch E: mo_seizures
Branch F: chargesheets → updated_chargesheet
Branch G: etl-files (file IDs, media server, URL updating)
Periodic: refresh_views (x3) over all branches.
7️⃣ Code Duplication Analysis
Configuration duplication

Each ETL directory (etl-crimes, etl-accused, etl-persons, etl-properties, etl-ir, etl-disposal, etl-hierarchy, etl_arrests, etl_mo_seizures, etl_chargesheets, etl_updated_chargesheet, plus etl-files, brief_facts_*, etc.) has a copy of a nearly identical config.py.
Variations:
Some know about disposal/arrests/seizures endpoints.
Others only define core endpoints.
This leads to:
Multiple places to change when updating API URLs, timeouts, or table names.
Inconsistent behavior (some ETLs know about DOPAMAS_API_URL2 and per-endpoint overrides, others don’t).
Logging setup duplication

All major ETLs define nearly identical logging boilerplate:

Import logging, colorlog.
Create StreamHandler.
Configure ColoredFormatter(LOG_CONFIG['format'], LOG_CONFIG['date_format'], log_colors=...).
Attach to root logger.
Optionally add custom TRACE level and Logger.trace monkey-patch (in crimes, accused, disposal, arrests).
This pattern is repeated in:

etl_crimes.py, etl_accused.py, etl_persons.py, etl_properties.py, etl_ir.py, etl_disposal.py, etl_arrests.py.
Other analytic scripts (e.g. domicile classifier) have their own similar logging setup.
Connection pool / DB access patterns

PostgreSQLConnectionPool usage pattern is repeated:

Each ETL:
Creates internal self.db_pool = PostgreSQLConnectionPool(minconn=..., maxconn=..., **DB_CONFIG) in its own way.
Implements its own get_table_columns (using information_schema.columns).
Implements its own get_effective_start_date (same SQL pattern for GREATEST(MAX(date_created), MAX(date_modified)), defaulting to 2022-01-01).
These functions (same signature) exist in:
etl_crimes.py, etl_accused.py, etl_persons.py, etl_properties.py, etl_ir.py, etl_disposal.py, etl_arrests.py, etl_mo_seizures.py, etl_hierarchy.py, etc.
Chunking, date range logic, and retry patterns

Chunk generation (generate_date_ranges) is implemented separately in each ETL, with the same logic:
Accepts ISO start/end.
Generates ranges of ETL_CONFIG['chunk_days'] with ETL_CONFIG['chunk_overlap_days'] overlap.
Comments and examples are extremely similar; only minor differences (some functions return YYYY-MM-DD, others operate with timezone).
API fetch with retry is repeated in each ETL, with slight variation in:
URL path (/crimes, /accused, /person-details, /property-details, /interrogation-reports/v1, /crimes/disposal, /arrests, /mo-seizures).
Logging and error handling messages.
Some ETLs log chunk JSON to file, some don’t.
ETL control-flow duplication

Almost every ETL follows similar high-level steps:

Setup logging and stats dict.
Connect DB and create PostgreSQLConnectionPool.
Compute effective start date from DB.
Generate overlapped date ranges.
For each date range:
Fetch API data with retries.
Validate FKs (e.g., CRIME_ID, PS_CODE, PERSON_ID).
Transform records.
Insert/update with smart upsert logic.
Log chunk-level stats to files.
Print final statistics and log summaries.
These patterns are repeated with customizations per entity (field mappings, target tables), but the control-flow and utility functions are nearly identical across:

etl_crimes.py, etl_accused.py, etl_persons.py, etl_properties.py, etl_ir.py, etl_disposal.py, etl_arrests.py, etl_mo_seizures.py.
Where duplication is concentrated

Config duplication:
etl-crimes/config.py, etl-accused/config.py, etl-persons/config.py, etl-properties/config.py, etl-ir/config.py, etl-disposal/config.py, etl_hierarchy/config.py, etl_arrests/config.py, etl_mo_seizures/config.py, etl_chargesheets/config.py, etl_updated_chargesheet/config.py, etl-files/config.py, brief_facts_* /config.py, etc.
Logging + chunk logger setup:
Within all main ETL scripts listed above.
DB pooling helpers (get_table_columns, get_effective_start_date):
Repeated in most ETL scripts.
Chunk range generation and API retry loops:
Repeated with small variations across all ETLs.
8️⃣ Infrastructure Components
Database infrastructure

Connection pooling:
Centralized in db_pooling.py as PostgreSQLConnectionPool.
All major ETLs use this, but each re-specifies minconn/maxconn based on:
MAX_WORKERS, ACCUSED_CHUNK_WORKERS, CRIMES_CHUNK_WORKERS, etc.
Batch operations:
BatchInsertOptimizer is available but only explicitly used in advisory docs (quick_start.py, db_pooling.py comments).
Most ETLs still insert one record at a time, often within executor workers.
Logging

Per-ETL loggers:
Each ETL:
Configures colorlog logger with LOG_CONFIG.
Writes chunk-wise logs (*_api_chunks_*.log, *_db_chunks_*.log) in a logs/ directory under its working directory.
Maintains failed-record logs and duplicates logs.
This yields a consistent logging pattern, but the implementation is repeated per ETL.
API client

Ad-hoc requests usage:
Each ETL calls requests.get directly, using API_CONFIG['base_url'] or more specific *_url fields.
Retries use per-ETL loops with exponential backoff, often with API_CONFIG['max_retries'].
No shared API client abstraction today—just repeated patterns.
Retry / idempotency logic

Fetch retries:
Implemented per ETL with similar patterns.
Idempotent DB behavior:
“Smart upsert” logic in each ETL script:
Reads existing row.
Compares each field:
Non-null → prefer new vs old.
Null new vs non-null old → keep old.
Always updates date fields from API.
Separate “pending FK” tables:
properties_pending_fk for properties.
ir_pending_fk for IR.
Both queue records where CRIME_ID isn’t yet present, then retry after upstream ETLs.
Threading / concurrency

Many ETLs use:
ThreadPoolExecutor for:
Chunk-level concurrency (across date ranges).
Record-level concurrency (within chunk).
Concurrency tuned via:
Env vars MAX_WORKERS, ACCUSED_CHUNK_WORKERS, CRIMES_CHUNK_WORKERS.
compute_safe_workers(self.db_pool, requested_workers) used in some ETLs to avoid exhausting DB pool (etl-persons, etl-properties, etl-ir, etl_mo_seizures, etl_arrests, properties ETL, etc.).
ConnectionLimiter is available but not widely used in these ETLs (current control is via executor workers and compute_safe_workers).
Existing package-like infra

etl-files/etl_pipeline_files is already structured as a Python package:
Proper __init__.py.
Internal imports like from .config import database, from .extract import crimes_extractor etc.
A single main.py/main_standalone.py driving orchestrated extract-transform-load behavior.
This subproject provides a useful reference for how to structure the rest of the ETL system as a proper Python package.
9️⃣ Execution Environment
Environment management

Virtualenv-based:
The master config uses:
source /data-drive/etl-process-dev/venv/bin/activate before running each ETL.
No conda or docker configuration is present in this repo (no environment.yml, Dockerfile, docker-compose.yml, etc.).
Python dependencies

Root requirements.txt consolidates dependencies across all subprojects, including ETLs and chatbot:

DB / drivers: psycopg2-binary, pymongo.
Config: python-dotenv, python-dateutil.
Data: pandas, numpy, openpyxl.
HTTP: requests.
Logging / utilities: tqdm, colorlog.
LLM / AI stack: langchain, langchain-community, langchain-ollama, langchain-core, pydantic, sentence-transformers, torch, openai, langgraph.
Web API: Flask, flask-cors, flask-limiter, SQLAlchemy, Werkzeug, gunicorn.
Caching: redis.
NLP/text: spacy, fuzzywuzzy, python-Levenshtein, dedupe.
Additional per-module requirements.txt (some partially overlapping):

etl-crimes/requirements.txt
etl-accused/requirements.txt
etl-persons/requirements.txt
etl-properties/requirements.txt
etl-ir/requirements.txt
etl-hierarchy/requirements.txt
etl_refresh_views/requirements.txt
etl_case_status/requirements.txt
etl-files/etl_pipeline_files/requirements.txt
etl-files/etl_files_media_server/requirements.txt
etl-files/update_file_urls_with_extensions/requirements.txt
brief_facts_accused/requirements.txt
brief_facts_drugs/requirements.txt
fix_fullname/requirements.txt
drug_standardization/requirements.txt
domicile_classification/requirements.txt
etl-mongo-to-postgresql/requirements.txt
section-wise-case-clarification/requirements.txt
chatbot/requirements.txt
Working-directory assumptions

Master config always runs ETLs by:
cd /data-drive/etl-process-dev/<subfolder> then python3 <script>.py.
Every ETL:
Creates logs/ directory relative to current working directory (the ETL folder).
Uses relative paths for log file names (logs/...).
Many ETLs rely on __file__ and sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) to locate db_pooling.py and their config.py in the parent-of-parent directory.
Environment variables required

Global:
POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_PORT.
DOPAMAS_API_URL (and optionally DOPAMAS_API_URL2).
DOPAMAS_API_KEY.
API_TIMEOUT, API_MAX_RETRIES.
CHUNK_OVERLAP_DAYS.
LOG_LEVEL.
ENABLE_EMBEDDINGS, EMBEDDING_MODEL.
Per-table overrides:
CRIMES_TABLE, ACCUSED_TABLE, PERSONS_TABLE, HIERARCHY_TABLE, PROPERTIES_TABLE, DISPOSAL_TABLE, ARRESTS_TABLE, and all IR_*_TABLE envs.
Per-endpoint overrides (in more advanced config modules):
<NAME>_API_BASE_URL, <NAME>_API_ENDPOINT (e.g., DISPOSAL_API_BASE_URL, DISPOSAL_API_ENDPOINT).
Concurrency and run-mode control:
MAX_WORKERS (caps global concurrency).
CRIMES_CHUNK_WORKERS, ACCUSED_CHUNK_WORKERS, ACCUSED_INTER_CHUNK_SLEEP.
ACCUSED_RUN_MODE (0=full historical, 1=incremental).
🔟 Summary (Technical Architecture & Problems)
1️⃣ Repository architecture summary

A monorepo-style layout: one root directory containing many focused ETL subprojects (etl-*, etl_*), plus analytics/dedup tasks (fix_fullname, domicile_classification, brief_facts_*, drug_standardization), a file-ETL package (etl-files/etl_pipeline_files), and a chatbot backend.
ETL subsystems are script-first, each with its own config.py and main script, orchestrated via etl_master/master_etl.py using a text-based workflow (input.txt).
2️⃣ Import structure problems

ETLs rely on sys.path hacks and bare imports (from config import ..., from db_pooling import ...) instead of proper package-relative imports.
Each ETL copies config.py instead of importing a shared config package, causing divergence and making dependency management harder.
The design assumes:
Specific folder names (etl-crimes, etl-accused, etc.).
Execution via python script.py from within each ETL directory.
A shared FS root /data-drive/etl-process-dev (baked into input.txt), making relocation or packaging brittle.
3️⃣ Shared module structure

Single shared DB infra module: db_pooling.py is already centralized and widely used; it’s the natural candidate for a core package module (dopams_etl.db.pooling-style).
Configs: logically shared but physically duplicated in each ETL directory.
File ETL package: etl-files/etl_pipeline_files already behaves like a proper Python package, with __init__.py, subpackages, and internal imports.
4️⃣ Execution model (master ETL runner)

A single master orchestrator (master_etl.py) reads declarative process blocks from input.txt and runs them as shell commands in order.
ETLs run in a single process each, but internal concurrency (ThreadPoolExecutor) and DB connection pooling are heavily used.
Each job is side-effectful at the DB and filesystem level (logs), controlled by environment variables and per-job configs.
5️⃣ Dependency relationships between ETLs

Strong, explicit data dependencies:
hierarchy → crimes → (classification, case_status) → accused → persons → domicile/fix_fullname → properties/IR/disposal/arrests/chargesheets → brief_facts_* → file ETLs → views refresh.
FK and data integrity constraints are enforced in code (via checks); some ETLs (properties/IR) use pending FK tables to defer inserts until upstream ETLs finish.