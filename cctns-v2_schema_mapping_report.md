# cctns-v2 Schema — Endpoint → Response File → Table → Field Mapping Report

Companion to `cctns-v2_schema.sql`. Source data: CCTNS V2 API response samples under
`.../cctnsv2/response/` (mapped by `00_summary.json`), cross-checked against the current
pure-CCTNS ETL pipeline (`etl_master/input.cctns-pure.txt`, `master_etl.py --pure-cctns`)
and the existing production schema this same codebase already targets (`DB-schema.sql`).

**Revision 4 (this version) — full audit/fix/validate pass.** Revisions 2 and 3 (an
automated consolidation pass not authored in this session) left the schema and three ETL
modules in a broken state: 12 tables had two fabricated JSONB columns
(`acts_and_sections`/`accused_particulars`) copy-pasted onto them with no basis in their
actual CCTNS response data; 11 `COMMENT ON TABLE` statements referenced tables that had
already been dropped; `etl_chargesheets.py` had a Python `IndentationError` (would not
even import); `etl-properties/etl_properties.py`'s UPSERT referenced a `properties.media`
column that no longer existed in the schema (parameter-count mismatch, would fail on
every call); `etl-ir/ir_etl.py` was missing its `process_date_range` method entirely
(`AttributeError` on every run) and called five nonexistent per-entity table constants;
`etl-ir/config.py` had a Python `SyntaxError` (would not even import). This revision:
fixed all of the above, re-derived every interrogation-report and chargesheet sub-field's
storage representation from the actual captured JSON (not from the broken JSONB dump),
verified every remaining table's purpose and every ETL module's SQL against the final
schema, and removed 11 leftover one-shot migration scripts from the repo root. See §4 for
the full before/after mapping and the JSONB justification, §5 for the table
classification, and §6 for the validation results.

**Do not create or modify any actual database from this report or the accompanying
SQL file** — both are design/validation artifacts only, per the task's explicit
instruction.

## 1. Endpoint → Response File → Table(s) → ETL-used fields

| # | CCTNS Endpoint | Response File | PostgreSQL Table(s) | ETL-used fields (source: transform function in the named ETL module) |
|---|---|---|---|---|
| 1 | `GET /ping` | `01_ping_endpoint_to_test_api.json` | — (no data table; connectivity smoke test only) | n/a |
| 2 | `GET /crimes/disposal` (bulk) | `02_...` | `disposal` | `CRIME_ID, DISPOSAL_TYPE, DISPOSED_DATE→disposed_at, DISPOSAL, CASE_STATUS, DATE_CREATED, DATE_MODIFIED` — `etl-disposal/etl_disposal.py:transform_disposal()` |
| 3 | `GET /crimes/disposal/{crimeId}` | `03_...` | `disposal` | same as #2 |
| 4 | `GET /crimes` (bulk) | `04_...` | `crimes` | `CRIME_ID, PS_CODE, FIR_NUM, FIR_REG_NUM, FIR_TYPE, ACTS_SECTIONS, FIR_DATE, CASE_STATUS, MAJOR_HEAD, MINOR_HEAD, CRIME_TYPE, IO_NAME, IO_RANK, BRIEF_FACTS, FIR_COPY, DATE_CREATED, DATE_MODIFIED` mapped to columns; everything else (`OCCURRENCE_DATE`, `PLACE_OF_OFFENCE`, `GD`, `COMPLAINANT_ID`, `COURT_NAME`, `IO_MOBILE`, ...) captured whole into `additional_json_data` JSONB — `etl-crimes/etl_crimes.py:transform_crime()` |
| 5 | `GET /crimes/{crimeId}` | `05_...` | `crimes` | same as #4 |
| 6 | `GET /master-data/hierarchy` | `06_...` | `hierarchy` | all 16 fields 1:1 — `etl-hierarchy/etl_hierarchy.py:transform_hierarchy()` |
| 7 | `GET /accused` (bulk) | `07_...` | `accused` | `ACCUSED_ID, CRIME_ID, PERSON_ID, ACCUSED_CODE, TYPE, SEQ_NUM, IS_CCL, PHYSICAL_FEATURES.*, ACCUSED_STATUS, DATE_CREATED, DATE_MODIFIED` — `etl-accused/etl_accused.py:transform_accused()` |
| 8 | `GET /accused/{crimeId}` | `08_...` | `accused` | same as #7 |
| 9 | `GET /arrests` (bulk) | `09_...` | `arrests` | `CRIME_ID, PERSON_ID, ACCUSED_SEQ_NO, ACCUSED_CODE, ACCUSED_TYPE, IS_ARRESTED, ARRESTED_DATE, IS_41A_CRPC, IS_41A_EXPLAIN_SUBMITTED, DATE_OF_ISSUE_41A, IS_CCL, IS_APPREHENDED, IS_ABSCONDING, IS_DIED, DATE_CREATED, DATE_MODIFIED` — `etl_arrests/etl_arrests.py:transform_arrests()` |
| 10 | `GET /arrests/{crimeId}` | `10_...` | `arrests` | same as #9 |
| 11 | `GET /mo-seizures` (bulk) | `11_...` | `mo_seizures`, `file_media_bookkeeping` (source_type=`mo_seizures`, source_field=`MO_MEDIA`) | `MO_SEIZURE_ID, CRIME_ID, SEQ_NO, MO_ID, TYPE, SUB_TYPE, DESCRIPTION, SEIZED_FROM, SEIZED_DATE→seized_at, SEIZED_BY, STRENGTH_OF_EVIDENCE, POS_ADDRESS1/2, POS_CITY, POS_DISTRICT, POS_PINCODE, POS_LANDMARK, POS_DESCRIPTION, POS_LATITUDE, POS_LONGITUDE, MO_MEDIA_URL/NAME/FILE_ID (primary media, flattened), DATE_CREATED, DATE_MODIFIED` → `mo_seizures`; per-item media entries → `file_media_bookkeeping` — `etl_mo_seizures/etl_mo_seizure.py`. `MO_MEDIA_CATEGORY`/`POS_ADDRESS_ID` are not currently consumed. |
| 12 | `GET /mo-seizures/{crimeId}` | `12_...` | `mo_seizures`, `file_media_bookkeeping` | same as #11 |
| 13 | `GET /person-details/{personId}` | `13_...` | `persons`, `file_media_bookkeeping` (source_type=`person`) | `PERSON_ID`, `PERSONAL_DETAILS.*`, `PRESENT_ADDRESS.*`, `PERMANENT_ADDRESS.*`, `CONTACT_DETAILS.*`, `DATE_CREATED`, `DATE_MODIFIED` → `persons`; `IDENTITY_DETAILS[]`/`MEDIA[]` → `file_media_bookkeeping` — `etl-persons/etl_persons.py`. `geo_resolution_source/confidence`/`domicile_classification` are derived by later pipeline steps. |
| 14 | `GET /property-details` (bulk) | `14_...` | `properties`, `file_media_bookkeeping` (source_type=`property`, source_field=`MEDIA`) | `PROPERTY_ID, CRIME_ID, CASE_PROPERTY_ID, PROPERTY_STATUS, RECOVERED_FROM, PLACE_OF_RECOVERY, DATE_OF_SEIZURE, NATURE, BELONGS, ESTIMATE_VALUE, RECOVERED_VALUE, PARTICULAR_OF_PROPERTY, CATEGORY, ADDITIONAL_DETAILS (jsonb, shape varies by CATEGORY), DATE_CREATED, DATE_MODIFIED` → `properties`; `MEDIA[]` → `file_media_bookkeeping` — `etl-properties/etl_properties.py:transform_property()`. Rows whose `crime_id` isn't yet in `crimes` are queued in `etl_bookkeeping` (kind='fk_retry'). |
| 15 | `GET /property-details/{crimeId}` | `15_...` | same as #14 | same as #14 |
| 16 | `GET /interrogation-reports/v1/` (bulk) | `16_...` | `interrogation_reports`, `file_media_bookkeeping` (source_type=`interrogation`) | Main record: `PHYSICAL_FEATURES.*`, `SOCIO_ECONOMIC_PROFILE.*`, `COMMISSION_OF_OFFENCE.*`, `SHARE_OF_AMOUNT_SPENT.*`, `PRESENT_WHEREABOUTS.*` flattened into columns; 11 sub-entities with confirmed stable real-data schemas (`FAMILY_HISTORY`, `ASSOCIATE_DETAILS`, `LOCAL_CONTACTS`, `MODUS_OPERANDI`, `SHELTER`, `DOPAMS_LINKS`, `TYPES_OF_DRUGS`, `CONSUMER_DETAILS`, `FINANCIAL_HISTORY`, `SIM_DETAILS`, `REGULAR_HABITS`) → parallel array columns; 10 sub-entities never observed populated in any capture → retained JSONB (see §4.2); `MEDIA[]`/`INTERROGATION_REPORT[]` → `file_media_bookkeeping` — `etl-ir/ir_etl.py:insert_main_record()`. Rows whose `crime_id` isn't yet in `crimes` are queued in `etl_bookkeeping` (kind='fk_retry', module_name='interrogation_reports'). |
| 17 | `GET /interrogation-reports/v1/{crimeId}` | `17_...` | same as #16 | same as #16 |
| 18 | `GET /chargesheets` (bulk) | `18_...` | `chargesheets`, `file_media_bookkeeping` (source_type=`chargesheets`, source_field=`uploadChargeSheet`) | `chargeSheetId, crimeId, chargeSheetNo, chargeSheetDate, chargeSheetType, courtName, isCcl, isEsigned, chargeSheetNoForIcjs, dateCreated, dateModified` → `chargesheets` columns; `actsAndSections[]`/`accusedParticulars[]` → parallel array columns on `chargesheets` (see §4.2); `uploadChargeSheet.fileId` → one `file_media_bookkeeping` row — `etl_chargesheets/etl_chargesheets.py`. Note: camelCase API, unlike almost every other CCTNS endpoint. |
| 19 | `GET /chargesheets/{crimeId}` | `19_...` | same as #18 | same as #18 |
| 20 | `GET /update-chargesheets` (bulk) | `20_...` | `charge_sheet_updates` | `updateChargeSheetId, crimeId, chargeSheetNo, chargeSheetDate, chargeSheetStatus, dateCreated, takenOnFile.{date,caseType,courtCaseNo}` — `etl_updated_chargesheet/etl_update_chargesheet.py` |
| 21 | `GET /update-chargesheets/{crimeId}` | `21_...` | same as #20 | same as #20 |
| 22 | `GET /case-property` (bulk) | `22_...` | `fsl_case_property`, `file_media_bookkeeping` (source_type=`case_property`, source_field=`MEDIA`) | `CASE_PROPERTY_ID, CASE_TYPE, CRIME_ID, MO_ID, STATUS, SEND_DATE, ..., DATE_CREATED, DATE_MODIFIED` → `fsl_case_property`; `MEDIA[]` → `file_media_bookkeeping` — `etl_fsl_case_property/etl_fsl_case_property.py`. CCTNS calls this endpoint "case-property"; DOPAMS models it as `fsl_case_property`. |
| 23 | `GET /case-property/{crimeId}` | `23_...` | same as #22 | same as #22 |
| 24 | `GET /files/{fileId}` | `24_...` | `file_media_bookkeeping` (download-status columns) | Binary content only; `fileId` is captured by every other endpoint's ETL into `file_media_bookkeeping.file_id` — `etl-files/etl_pipeline_files` (discovery) and `etl-files/etl_files_media_server` (download + status update). |
| 25–29 | Reports/citizen-portal endpoints (`missing-udb-persons`, `arrest-particulars` x2, `stolen-automobiles` x2) | `25_...`–`29_...` | **not ingested** | Held pending CCTNS-provider clarification; no ETL module reads these, so no table is proposed. |

## 2. Tables not tied to a single endpoint

| Table(s) | Populated by | Notes |
|---|---|---|
| `etl_bookkeeping` (`kind='fk_retry'`) | `etl-disposal`, `etl_arrests`, `etl_chargesheets`, `etl_fsl_case_property`, `etl_updated_chargesheet` (via shared `etl_fk_retry_queue.py`), plus `etl-properties` and `etl-ir` (via their own `queue_pending_fk`/`push_fk_failure` calls, `module_name='properties'`/`'interrogation_reports'`) | Retry queue for rows whose `crime_id`/`mo_id` FK isn't resolvable yet. Formerly 6 separate tables: `etl_fk_retry_queue`, `properties_pending_fk`, `ir_pending_fk` (all fk_retry-shaped), plus `etl_checkpoint`/`etl_run_state`/`etl_address_failures` (other kinds). |
| `etl_bookkeeping` (`kind='checkpoint'`, `kind='failure'`) | `etl-address` | Crash-resume checkpoint and per-record failure log for the (LLM-off) address-resolution step. |
| `etl_bookkeeping` (`kind='run_state'`) | `etl_master/checkpoint_manager.py`, `etl-persons`, `etl-properties`, `etl-disposal`, `etl-accused` | Per-module incremental-run watermark. |
| `geo_countries`, `geo_reference` | loaded/maintained separately (not CCTNS-sourced) | Static reference data for the non-LLM address resolver. |

## 3. Schema validation against current ETL requirements

Every column was derived directly from a confirmed `INSERT`/`UPDATE` column list read from
the corresponding ETL module's source code, cross-checked against the actual captured
CCTNS response JSON and, for the two "confirmed-real-data" nested-array redesigns
(chargesheets, interrogation_reports — see §4), against a field-by-field union/count over
every sampled record. Programmatic column-parity checks (schema columns vs. ETL
INSERT/UPDATE column lists) were run for `interrogation_reports`, `chargesheets`, and
`mo_seizures` and found **zero mismatches** in either direction.

### Known data-shape caveats carried into the schema (not gaps, just worth flagging)

- **Empty string vs. NULL**: `crimes`, `accused`, most of `arrests`, and almost all of
  `fsl_case_property`'s optional string fields use `""` as their API-level null-sentinel
  rather than JSON `null`. Columns are left nullable; the ETL is responsible for any
  `"" -> NULL` normalization it already performs.
- **Inconsistent field casing across endpoints**: chargesheets/update-chargesheets use
  camelCase; every other endpoint uses SCREAMING_SNAKE_CASE. Already handled per-module in
  the ETL code.
- **`ARRESTED_DATE`/`DATE_OF_ISSUE_41A`** use a `"YYYY-MM-DD HH:MM:SS"` (no timezone)
  string format, unlike every other `DATE_CREATED`/`DATE_MODIFIED` field's ISO-8601-with-Z
  format. Columns are `timestamp with time zone` to match the rest of `arrests`.
- **`ACCUSED_CODE`/`IS_ABSCONDING`** on the arrests bulk endpoint can be entirely absent as
  JSON keys on ~3% of sampled records — both columns are nullable.

## 4. Interrogation-report and chargesheet field-level redesign (this revision)

### 4.1 Why this needed re-auditing

The schema inherited from the prior (broken) automated pass stored **every** IR sub-entity
and the chargesheets `acts_and_sections`/`accused_particulars` fields as raw JSONB, with no
per-field justification, and had fabricated the same two JSONB columns onto 11 unrelated
tables (`hierarchy`, `crimes`, `persons`, `accused`, `arrests`, `disposal`, `properties`,
`mo_seizures`, `charge_sheet_updates`, `fsl_case_property`, `file_media_bookkeeping`,
`etl_bookkeeping`) that have no such data in their actual CCTNS responses at all. Both
problems are fixed in this revision: the fabricated columns were removed, and every real
IR/chargesheet nested field was individually re-evaluated against the captured response
JSON (`16_...`/`17_...` for IR, `18_...`/`19_...` for chargesheets) before deciding its
storage representation.

### 4.2 Per-field decisions

**Chargesheets** — both nested arrays have a confirmed stable schema from 21 real captured
chargesheet records (287 `actsAndSections[]` items, 90 `accusedParticulars[]` items) →
stored as parallel PostgreSQL arrays directly on `chargesheets` (one array position per
source API array item), replacing the former `chargesheet_acts`/
`chargesheet_acts_sections`/`chargesheet_accused` tables:
`acts_descriptions, acts_sections, acts_section_descriptions, acts_grave_particulars,
acts_rw_required, accused_person_ids, accused_charge_statuses,
accused_reasons_for_no_charge, accused_requested_for_nbw`. (The former
`chargesheet_accused.is_person_master_present` column was **not** recreated — the old ETL
code's own comment says "API doesn't seem to have isPersonMasterPresent, default to True",
i.e. it was a DOPAMS-invented default with no basis in the actual CCTNS field, so keeping
it would be inventing structure.)

**Interrogation reports** — 11 sub-entities have a confirmed stable schema from real
captured data and are stored as parallel array columns (see the full list in
`cctns-v2_schema.sql`'s `interrogation_reports` definition):

| Sub-entity | Real items observed | Fields |
|---|---|---|
| `FAMILY_HISTORY[]` | 233 | person_id, relation, family_member_peculiarity, criminal_background, is_alive, family_stay_together |
| `SIM_DETAILS[]` | 90 | phone_number, sdr, imei, true_caller_name, person_id |
| `TYPES_OF_DRUGS[]` | 83 | type_of_drug, quantity, purchase_amount_in_inr, mode_of_payment, mode_of_transport, supplier_person_id, receivers_person_id |
| `CONSUMER_DETAILS[]` | 75 | consumer_person_id, place_of_consumption, other_sources, other_sources_phone_no, aadhar_card_number, aadhar_card_number_phone_no |
| `DOPAMS_LINKS[]` | 71 | phone_number, dopams_data[] (comma-joined per phone number — see column comment) |
| `FINANCIAL_HISTORY[]` | 70 | account_holder_person_id, pan_no, upi_id, name_of_bank, account_number, branch_name, ifsc_code, immovable_property_acquired, movable_property_acquired |
| `REGULAR_HABITS[]` | 203 | flat string array (not an array of objects) |
| `MODUS_OPERANDI[]` | 9 | crime_head, crime_sub_head, modus_operandi |
| `ASSOCIATE_DETAILS[]` | 4 | person_id, gang, relation |
| `LOCAL_CONTACTS[]` | 3 | person_id, town, address, jurisdiction_ps |
| `SHELTER[]` | 3 | preparation_of_offence, after_offence, regular_residency, remarks, other_regular_residency |

`INDULGANCE_BEFORE_OFFENCE` is a mixed-type API field (empty array in 187/191 sampled
records, a plain string in the other 4/191, e.g. "Consuming ganja") — stored as a nullable
`text` scalar, not an array/JSONB.

`MEDIA[]` and `INTERROGATION_REPORT[]` are file/document reference UUID lists, not
business data — routed through `file_media_bookkeeping` (source_type='interrogation'),
matching how every other entity's file/media fields are handled, not stored as columns on
`interrogation_reports`.

### IR JSONB retained

```
conviction_acquittal, defence_counsel, execution_of_nbw, jail_sentence,
new_gang_formation, pending_nbw, previous_offences_confessed, property_disposal,
regularization_transit_warrants, sureties
```

Reason (same for all 10): **zero real records** for any of these fields were observed in
any of the 191 sampled IR records across both endpoint captures (bulk + by-crimeId). Their
structure cannot be verified against the actual CCTNS V2 API contract from the evidence
available. The pre-existing production database (`DB-schema.sql`) does have column lists
for the equivalent legacy tables, but that reflects a different, older system's schema —
using it to invent ~9–17 columns per field here would risk fabricating a structure the
current CCTNS V2 API may not actually send, which is a worse violation of "do not invent
structure" than storing the raw array/object as JSONB. If real data is ever observed for
any of these fields, re-run this same field-by-field audit against that data and flatten
into columns/arrays using the same method as the 11 fields above.

### IR fields converted to normal columns/arrays

The 11 sub-entities listed in §4.2's table (51 array columns total), plus
`indulgance_before_offence` (scalar text). `MEDIA[]`/`INTERROGATION_REPORT[]` moved to
`file_media_bookkeeping` (not a column on `interrogation_reports` at all).

### IR child tables

**0** (down from 23). All IR business data lives in the single `interrogation_reports`
table; file/media references live in the single `file_media_bookkeeping` table.

## 5. Full table classification

`CCTNS_BUSINESS_DATA` = populated from a CCTNS endpoint (top-level record and/or its
nested arrays/objects, now flattened into the same table). `REFERENCE_DATA` = static
lookup, not CCTNS-sourced. `ETL_BOOKKEEPING`/`FILE_MEDIA_BOOKKEEPING` = the two
consolidated operational tables.

| Table | Type | CCTNS Endpoint/Source | Why Required | ETL Modules Using It |
|---|---|---|---|---|
| `hierarchy` | CCTNS_BUSINESS_DATA | `GET /master-data/hierarchy` | PS→ADG org hierarchy; `crimes.ps_code` join target | `etl-hierarchy` |
| `geo_countries` | REFERENCE_DATA | not CCTNS-sourced | country/state/timezone lookup for the non-LLM address resolver | `etl-address` (read-only) |
| `geo_reference` | REFERENCE_DATA | not CCTNS-sourced | India state/district/sub-district/village lookup for the non-LLM address resolver | `etl-address` (read-only) |
| `crimes` | CCTNS_BUSINESS_DATA | `GET /crimes` (+detail) | core FIR/crime record; join key for nearly every other entity | `etl-crimes`; `section-wise-case-clarification` (class_classification); `etl_case_status` (case_status) |
| `persons` | CCTNS_BUSINESS_DATA | `GET /person-details/{personId}` | person master; referenced by accused/arrests/IR/chargesheets | `etl-persons`; `etl-address`; `domicile_classification`; `fix_fullname/*` |
| `accused` | CCTNS_BUSINESS_DATA | `GET /accused` (+detail) | links a person to a crime as accused, with physical features | `etl-accused` |
| `arrests` | CCTNS_BUSINESS_DATA | `GET /arrests` (+detail) | arrest status per accused-per-crime | `etl_arrests` |
| `disposal` | CCTNS_BUSINESS_DATA | `GET /crimes/disposal` (+detail) | case disposal outcome | `etl-disposal` |
| `properties` | CCTNS_BUSINESS_DATA | `GET /property-details` (+detail) | seized/recovered property, incl. CATEGORY-dependent `additional_details` jsonb | `etl-properties` |
| `mo_seizures` | CCTNS_BUSINESS_DATA | `GET /mo-seizures` (+detail) | modus-operandi seizure records | `etl_mo_seizures` |
| `chargesheets` | CCTNS_BUSINESS_DATA | `GET /chargesheets` (+detail) | chargesheet record + actsAndSections[]/accusedParticulars[] as parallel arrays | `etl_chargesheets` |
| `charge_sheet_updates` | CCTNS_BUSINESS_DATA | `GET /update-chargesheets` (+detail) | chargesheet-status update / takenOnFile record | `etl_updated_chargesheet` |
| `fsl_case_property` | CCTNS_BUSINESS_DATA | `GET /case-property` (+detail) | forensic/case-property register entry | `etl_fsl_case_property` |
| `interrogation_reports` | CCTNS_BUSINESS_DATA | `GET /interrogation-reports/v1/` (+detail) | IR main record + 11 sub-entities as parallel arrays + 10 as justified JSONB | `etl-ir` |
| `file_media_bookkeeping` | FILE_MEDIA_BOOKKEEPING | every endpoint's file/media fields + `GET /files/{fileId}` | single consolidated file/media reference + download-tracking table (replaces 7 former tables) | `etl-files/*`, `etl-properties`, `etl_mo_seizures`, `etl_chargesheets`, `etl_fsl_case_property`, `etl-ir` |
| `etl_bookkeeping` | ETL_BOOKKEEPING | derived (not CCTNS) | single consolidated ETL checkpoint/watermark/retry/failure table (replaces 6 former tables) | `etl-address`, `etl_master`, `etl-persons`, `etl-properties`, `etl-disposal`, `etl-accused`, `etl_arrests`, `etl_chargesheets`, `etl_fsl_case_property`, `etl_updated_chargesheet`, `etl-ir` |

**16 tables total.**

## 6. Validation results (this revision's audit)

### 6.1 Stale references

- Runtime SQL references to `etl_checkpoint`, `etl_run_state`, `etl_fk_retry_queue`,
  `etl_address_failures`, `properties_pending_fk`, `ir_pending_fk`: **0**.
- Runtime SQL references to `files`, `property_media`, `mo_seizure_media`,
  `chargesheet_files`, `chargesheet_media`, `fsl_case_property_media`, `ir_media`: **0**.
- Runtime SQL references to `chargesheet_acts`, `chargesheet_acts_sections`,
  `chargesheet_accused`, and all 23 former `ir_*` child tables: **0**.
- Orphaned `COMMENT ON TABLE`/`COMMENT ON COLUMN` for removed tables: **0** (11 removed).
- Orphaned indexes/triggers/constraints referencing non-existent tables or columns: **0**
  (verified programmatically: every `CREATE INDEX ... ON public.X` and trigger target `X`
  is one of the 16 tables the schema actually creates).

### 6.2 Bugs found and fixed (not present before this revision's automated pass, but present
in the schema/ETL state this revision started from)

1. **`etl_chargesheets.py`: Python `IndentationError`** — an empty `for accused_data in
   accused_list:` loop body (the `insert_chargesheet_accused(...)` call inside it had been
   deleted without removing the loop). The module could not be imported at all.
2. **`etl-ir/config.py`: Python `SyntaxError`** — a malformed `TABLE_CONFIG` dict literal
   (dangling `),` left over from a partially-applied removal). The module could not be
   imported at all, which also broke `ir_etl.py` (imports `config`).
3. **`etl-ir/ir_etl.py`: missing `process_date_range` method** — called from `run()` via
   `ThreadPoolExecutor`, but not defined anywhere in the file. Every run would fail with
   `AttributeError` before processing a single record. Reconstructed from the surrounding
   helper methods (`fetch_ir_data_from_api`, `get_existing_ir_record`,
   `should_update_record`, `insert_main_record`, `queue_pending_fk`, `detect_new_fields`).
4. **`etl-ir/ir_etl.py`: `CRIMES_TABLE` used but never defined** — `load_crime_ids()` would
   raise `NameError` on first use. Added `CRIMES_TABLE = TABLE_CONFIG.get('crimes',
   'crimes')`.
5. **`etl-ir/ir_etl.py`: `delete_related_records()` referenced 13 undefined `IR_*_TABLE`
   constants** — dead code (never called), removed; its one correct piece of behavior
   (clearing stale `file_media_bookkeeping` rows before reprocessing) was folded directly
   into `insert_main_record()`, extended to cover `INTERROGATION_REPORT[]` as well as
   `MEDIA[]`.
6. **`etl-properties/etl_properties.py`: UPSERT referenced `properties.media`**, a column
   that no longer exists in the schema (media moved to `file_media_bookkeeping`) — the
   `WHERE` clause would raise `UndefinedColumn`, and the parameter tuple had one more value
   than the column list had placeholders (would raise a binding error even if the column
   existed). Both removed.
7. **`etl-properties/etl_properties.py`: `self.has_property_additional_details_table` /
   `PROPERTY_ADDITIONAL_DETAILS_TABLE` referenced but never defined** — `AttributeError`/
   `NameError` on every run, immediately after connecting. Dead code (the
   `property_additional_details` mirror table was removed; `properties.additional_details`
   is now the only copy of this data), removed.
8. **`etl-properties/etl_properties.py`: `ensure_pending_table()` referenced the undefined
   `PENDING_FK_TABLE`** and created a `properties_pending_fk`-shaped table the schema no
   longer has — dead/broken code from an incomplete migration (the real, working mechanism
   is `queue_pending_fk()` → `push_fk_failure()` → `etl_bookkeeping`), removed along with
   its call site.
9. **`etl_chargesheets.py`/`etl-properties/etl_properties.py`: 11 unrelated tables**
   (see §4.1) had two fabricated JSONB columns with no ETL code ever writing to them.
   Removed everywhere except `chargesheets`, where the underlying data is real (redesigned
   as parallel arrays, see §4.2).

All 9 were confirmed via `python -m py_compile` (syntax) and `python -m pyflakes`
(undefined names) across every `*.py` file in the pure-CCTNS ETL codebase, then fixed and
re-verified with the same tools — **0 remaining syntax errors, 0 remaining undefined-name
errors** repo-wide (excluding `brief_facts_ai/`, the AI module explicitly out of pure-CCTNS
scope, which has one pre-existing unrelated syntax error not touched by this audit).

Also removed: 11 one-shot migration scripts left in the repo root
(`fix_etl_ir.py`, `fix_etl_properties.py`, `fix_etl_again.py`, `fix_chargesheets.py`,
`fix_mo_seizures.py`, `fix_ir_insert.py`, `fix_properties.py`, `remove_tables.py`,
`clean_schema.py`, `fix_mapping_report.py`, `finalize_mapping_report.py`) — debris from the
automated pass that produced the broken state this revision fixed; none are imported by
any ETL module.

### 6.3 ETL reconciliation

Programmatic column-parity checks (every schema column vs. every ETL INSERT/UPDATE column
reference) for `interrogation_reports`, `chargesheets`, and `mo_seizures`: **0 mismatches**
in either direction. `INSERT`/`UPDATE`/`SELECT`/`DELETE`/`ON CONFLICT`/upsert logic,
checkpoint/retry behavior, provenance columns, and the 7-day API chunking / date-range
logic were **not** redesigned — only the persistence code for the tables actually
consolidated in this revision was touched, per the task's explicit instruction not to
change ETL flow.
