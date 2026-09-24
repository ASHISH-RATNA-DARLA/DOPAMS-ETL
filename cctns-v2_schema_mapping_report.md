# cctns-v2 Schema — Endpoint → Response File → Table → Field Mapping Report

Companion to `cctns-v2_schema.sql`. Source data: CCTNS V2 API response samples under
`C:\Users\DELL\Downloads\cctns\cctns\cctnsv2\response\` (mapped by `00_summary.json`),
cross-checked against the current pure-CCTNS ETL pipeline
(`etl_master/input.cctns-pure.txt`, `master_etl.py --pure-cctns`) and the existing
production schema this same codebase already targets (`DB-schema.sql`).

**Revision 2 (this version):** consolidated 4 separate ETL bookkeeping tables into one
(`etl_bookkeeping`) and 7 separate file/media reference tables into one
(`file_media_bookkeeping`), and updated the ETL code that wrote to the 11 old tables to
write to the 2 new ones instead. See §4 for the full before/after mapping, the updated
table classification, and the reference-search validation. Revision 1's per-endpoint
mapping in §1 below is otherwise unchanged; table names have been updated in place where
they were affected by the consolidation.

**Do not create or modify any actual database from this report or the accompanying
SQL file** — both are design/validation artifacts only, per the task's explicit
instruction.

## 1. Endpoint → Response File → Table(s) → ETL-used fields

| # | CCTNS Endpoint | Response File | PostgreSQL Table(s) | ETL-used fields (source: transform function in the named ETL module) |
|---|---|---|---|---|
| 1 | `GET /ping` | `01_ping_endpoint_to_test_api.json` | — (no data table; connectivity smoke test only) | n/a |
| 2 | `GET /crimes/disposal` (bulk) | `02_crimes_endpoint_to_get_disposal_cases_data_betw.json` | `disposal` | `CRIME_ID, DISPOSAL_TYPE, DISPOSED_DATE→disposed_at, DISPOSAL, CASE_STATUS, DATE_CREATED, DATE_MODIFIED` — `etl-disposal/etl_disposal.py:transform_disposal()` |
| 3 | `GET /crimes/disposal/{crimeId}` | `03_crimes_endpoint_to_get_disposal_case_data_using.json` | `disposal` | same as #2 (identical schema, single object vs array) |
| 4 | `GET /crimes` (bulk) | `04_crimes_endpoint_to_get_crimes_data_updated_betw.json` | `crimes` | `CRIME_ID, PS_CODE, FIR_NUM, FIR_REG_NUM, FIR_TYPE, ACTS_SECTIONS, FIR_DATE, CASE_STATUS, MAJOR_HEAD, MINOR_HEAD, CRIME_TYPE, IO_NAME, IO_RANK, BRIEF_FACTS, FIR_COPY, DATE_CREATED, DATE_MODIFIED` mapped to columns; everything else (`OCCURRENCE_DATE`, `PLACE_OF_OFFENCE`, `GD`, `COMPLAINANT_ID`, `COURT_NAME`, `IO_MOBILE`, ...) captured whole into `additional_json_data` JSONB — `etl-crimes/etl_crimes.py:transform_crime()` |
| 5 | `GET /crimes/{crimeId}` | `05_crimes_endpoint_to_get_crime_data_using_crimeid.json` | `crimes` | same as #4 |
| 6 | `GET /master-data/hierarchy` | `06_master_data_endpoint_to_get_hierarchy_master_data.json` | `hierarchy` | all 16 fields 1:1 — `etl-hierarchy/etl_hierarchy.py:transform_hierarchy()` |
| 7 | `GET /accused` (bulk) | `07_accused_endpoint_to_get_accused_data_updated_bet.json` | `accused` | `ACCUSED_ID, CRIME_ID, PERSON_ID, ACCUSED_CODE, TYPE, SEQ_NUM, IS_CCL, PHYSICAL_FEATURES.{BEARD,BUILD,COLOR,EAR,EYES,FACE,HAIR,HEIGHT,LEUCODERMA,MOLE,MUSTACHE,NOSE,TEETH}, ACCUSED_STATUS, DATE_CREATED, DATE_MODIFIED` — `etl-accused/etl_accused.py:transform_accused()` |
| 8 | `GET /accused/{crimeId}` | `08_accused_endpoint_to_get_accused_data_using_crime.json` | `accused` | same as #7 |
| 9 | `GET /arrests` (bulk) | `09_arrests_endpoint_to_get_arrests_data_updated_bet.json` | `arrests` | `CRIME_ID, PERSON_ID, ACCUSED_SEQ_NO, ACCUSED_CODE, ACCUSED_TYPE, IS_ARRESTED, ARRESTED_DATE, IS_41A_CRPC, IS_41A_EXPLAIN_SUBMITTED, DATE_OF_ISSUE_41A, IS_CCL, IS_APPREHENDED, IS_ABSCONDING, IS_DIED, DATE_CREATED, DATE_MODIFIED` — `etl_arrests/etl_arrests.py:transform_arrests()` |
| 10 | `GET /arrests/{crimeId}` | `10_arrests_endpoint_to_get_arrests_data_using_crime.json` | `arrests` | same as #9 |
| 11 | `GET /mo-seizures` (bulk) | `11_mo_seizures_endpoint_to_get_mo_seizures_created_or_u.json` | `mo_seizures`, `file_media_bookkeeping` (source_type=`mo_seizures`, source_field=`MO_MEDIA`) | `MO_SEIZURE_ID, CRIME_ID, SEQ_NO, MO_ID, TYPE, SUB_TYPE, DESCRIPTION, SEIZED_FROM, SEIZED_DATE→seized_at, SEIZED_BY, STRENGTH_OF_EVIDENCE, POS_ADDRESS1/2, POS_CITY, POS_DISTRICT, POS_PINCODE, POS_LANDMARK, POS_DESCRIPTION, POS_LATITUDE, POS_LONGITUDE, MO_MEDIA_URL/NAME/FILE_ID (primary media, flattened), DATE_CREATED, DATE_MODIFIED` → `mo_seizures`; per-item media entries → `file_media_bookkeeping` (formerly the dedicated `mo_seizure_media` table) — `etl_mo_seizures/etl_mo_seizure.py:transform_seizure()`, `sync_seizure_media()`. `MO_MEDIA_CATEGORY` and `POS_ADDRESS_ID` (present in raw API, always null/optional) are **not** currently consumed by the ETL. |
| 12 | `GET /mo-seizures/{crimeId}` | `12_mo_seizures_endpoint_to_get_mo_seizures_using_crimei.json` | `mo_seizures`, `file_media_bookkeeping` | same as #11 |
| 13 | `GET /person-details/{personId}` | `13_person_details_endpoint_to_get_person_details_using_per.json` | `persons`, `file_media_bookkeeping` (source_type=`person`) | `PERSON_ID`, `PERSONAL_DETAILS.*` (NAME, SURNAME, ALIAS, FULL_NAME, RELATION_TYPE, RELATIVE_NAME, GENDER, IS_DIED, DATE_OF_BIRTH, AGE, OCCUPATION, EDUCATION_QUALIFICATION, CASTE, SUB_CASTE, RELIGION, NATIONALITY, DESIGNATION, PLACE_OF_WORK), `PRESENT_ADDRESS.*`, `PERMANENT_ADDRESS.*`, `CONTACT_DETAILS.*` (PHONE_NUMBER, COUNTRY_CODE, EMAIL_ID), `DATE_CREATED`, `DATE_MODIFIED` → `persons`; `IDENTITY_DETAILS[]` and `MEDIA[]` (both empty in the sampled record — inner shape unconfirmed) → `file_media_bookkeeping` rows with `source_type='person'`, `source_field IN ('IDENTITY_DETAILS','MEDIA')` (formerly the generic `files` table) — `etl-persons/etl_persons.py`. `geo_resolution_source/confidence` and `domicile_classification` are **derived** by later pure-pipeline steps (`etl-address`, `domicile_classification`), not read directly from this endpoint. |
| 14 | `GET /property-details` (bulk) | `14_property_details_endpoint_to_get_property_details_created.json` | `properties`, `properties_pending_fk`, `property_additional_details`, `file_media_bookkeeping` (source_type=`property`, source_field=`MEDIA`) | `PROPERTY_ID, CRIME_ID, CASE_PROPERTY_ID, PROPERTY_STATUS, RECOVERED_FROM, PLACE_OF_RECOVERY, DATE_OF_SEIZURE, NATURE, BELONGS, ESTIMATE_VALUE, RECOVERED_VALUE, PARTICULAR_OF_PROPERTY, CATEGORY, ADDITIONAL_DETAILS (shape varies by CATEGORY), MEDIA, DATE_CREATED, DATE_MODIFIED` — `etl-properties/etl_properties.py:transform_property()`; rows whose `crime_id` isn't yet in `crimes` are queued in `properties_pending_fk`; `additional_details` is mirrored into `property_additional_details` and `media` into `file_media_bookkeeping` (formerly the dedicated `property_media` table), both in overwrite mode. |
| 15 | `GET /property-details/{crimeId}` | `15_property_details_endpoint_to_get_property_details_using_c.json` | same as #14 | same as #14 |
| 16 | `GET /interrogation-reports/v1/` (bulk) | `16_interrogation_reports_endpoint_to_get_interrogation_reports_cr.json` | `interrogation_reports` + 23 `ir_*` child tables + `ir_pending_fk` + `file_media_bookkeeping` (source_type=`interrogation`) | Main record: `PHYSICAL_FEATURES.*`, `SOCIO_ECONOMIC_PROFILE.*`, `COMMISSION_OF_OFFENCE.*`, `SHARE_OF_AMOUNT_SPENT.*`, `PRESENT_WHEREABOUTS.*` (7 sub-objects) flattened into ~55 `interrogation_reports` columns; each of `FAMILY_HISTORY[]`, `ASSOCIATE_DETAILS[]`, `LOCAL_CONTACTS[]`, `TYPES_OF_DRUGS[]`, `CONSUMER_DETAILS[]`, `FINANCIAL_HISTORY[]`, `SIM_DETAILS[]`, `MODUS_OPERANDI[]`, `SHELTER[]`, `DOPAMS_LINKS[]` (→ `ir_dopams_links`), `REGULAR_HABITS[]`, `INTERROGATION_REPORT[]` (report/doc refs → `ir_interrogation_report_refs`) → each its own retained `ir_*` child table (unaffected by the consolidation); root `MEDIA[]` → `file_media_bookkeeping` (`source_type='interrogation'`, `source_field='MEDIA'`; formerly the dedicated `ir_media` table) — `etl-ir/ir_etl.py`. `INDULGANCE_BEFORE_OFFENCE` (mixed array/string type in API) → `other_indulgence_before_offence`/`ir_indulgance_before_offence` as free text. Rows whose `crime_id` isn't yet in `crimes` are queued in `ir_pending_fk`. Separately, the `etl-files/etl_pipeline_files` discovery pipeline also registers `INTERROGATION_REPORT`/`DOPAMS_DATA` file references from this same response into `file_media_bookkeeping` for download tracking (source_field `INTERROGATION_REPORT`/`DOPAMS_DATA`) — that is additive bookkeeping alongside, not instead of, `ir_interrogation_report_refs`/`ir_dopams_links`. |
| 17 | `GET /interrogation-reports/v1/{crimeId}` | `17_interrogation_reports_endpoint_to_get_interrogation_reports_us.json` | same as #16 | same as #16 |
| 18 | `GET /chargesheets` (bulk) | `18_chargesheets_endpoint_to_get_chargesheets_updated_bet.json` | `chargesheets`, `chargesheet_acts`, `chargesheet_acts_sections`, `chargesheet_accused`, `file_media_bookkeeping` (source_type=`chargesheets`, source_field=`uploadChargeSheet`) | `chargeSheetId, crimeId, chargeSheetNo, chargeSheetDate, chargeSheetType, courtName, isCcl, isEsigned, chargeSheetNoForIcjs, dateCreated, dateModified` → `chargesheets`; `uploadChargeSheet.fileId` → one `file_media_bookkeeping` row keyed by `charge_sheet_id` (formerly written to both the dedicated `chargesheet_files` and `chargesheet_media` tables, which stored the same event under two different keys — now written once); `actsAndSections[]` → `chargesheet_acts` and `chargesheet_acts_sections`; `accusedParticulars[]` → `chargesheet_accused` — `etl_chargesheets/etl_chargesheets.py:transform_chargesheet()`, `insert_chargesheet_file()`. Note: this endpoint uses **camelCase**, unlike almost every other CCTNS endpoint (SCREAMING_SNAKE_CASE). |
| 19 | `GET /chargesheets/{crimeId}` | `19_chargesheets_endpoint_to_get_chargesheets_using_crime.json` | same as #18 | same as #18 |
| 20 | `GET /update-chargesheets` (bulk) | `20_update_chargesheets_endpoint_to_get_update-chargesheets_upda.json` | `charge_sheet_updates` | `updateChargeSheetId, crimeId, chargeSheetNo, chargeSheetDate, chargeSheetStatus, dateCreated, takenOnFile.{date,caseType,courtCaseNo}` (flattened to `taken_on_file_date/case_type/court_case_no`) — `etl_updated_chargesheet/etl_update_chargesheet.py` |
| 21 | `GET /update-chargesheets/{crimeId}` | `21_update_chargesheets_endpoint_to_get_update-chargesheets_usin.json` | same as #20 | same as #20 |
| 22 | `GET /case-property` (bulk) | `22_case_property_get_case_property_details_between_date_r.json` | `fsl_case_property`, `file_media_bookkeeping` (source_type=`case_property`, source_field=`MEDIA`) | `CASE_PROPERTY_ID, CASE_TYPE, CRIME_ID, MO_ID, STATUS, SEND_DATE, FORWARDING_THROUGH, COURT_NAME, FSL_COURT_NAME, FSL_REQUEST_ID, CPR_NO, DIRECTION_BY_COURT, DETAILS_DISPOSAL, CPR_COURT_NAME, PLACE_DISPOSAL, DATE_DISPOSAL, RELEASE_ORDER_NO, RELEASE_DATE, RETURN_DATE, PLACE_CUSTODY, ASSIGN_CUSTODY, DATE_CUSTODY, FSL_NO, FSL_DATE, REPORT_RECEIVED, OPINION, OPINION_FURNISHED, STRENGTH_OF_EVIDENCE, PROPERTY_RECEIVED_BACK, EXPERT_TYPE, OTHER_EXPERT_TYPE, DATE_SENT_TO_EXPERT, COURT_ORDER_NUMBER, COURT_ORDER_DATE, DATE_CREATED, DATE_MODIFIED` → `fsl_case_property`; `MEDIA[]` → `file_media_bookkeeping` (replace-all per `case_property_id`; formerly the dedicated `fsl_case_property_media` table) — `etl_fsl_case_property/etl_fsl_case_property.py:transform_fsl_case_property()`, `insert_media_files()`. Note: CCTNS names this endpoint "case-property"; DOPAMS models the same entity as `fsl_case_property`. Empty-string is used as the null-sentinel for most optional fields here rather than JSON `null`. |
| 23 | `GET /case-property/{crimeId}` | `23_case_property_get_case_property_details_by_crime_id.json` | same as #22 | same as #22 |
| 24 | `GET /files/{fileId}` | `24_files_endpoint_to_download_file_using_fileid_n.json` | `file_media_bookkeeping` (download-status columns: `is_downloaded`, `downloaded_at`, `download_error`, `download_attempts`; formerly the dedicated `files` table) | Binary content only (no structured metadata in the response); `fileId` is the path param already captured in `file_media_bookkeeping.file_id` by every other endpoint's ETL (`FIR_COPY`, `MEDIA`, `uploadChargeSheet`, `MO_MEDIA`, `IDENTITY_DETAILS`, `INTERROGATION_REPORT`, `DOPAMS_DATA`) — `etl-files/etl_pipeline_files` (discovery/registration) and `etl-files/etl_files_media_server` (download + status update). Rate-limited by CCTNS (noted in the captured sample). |
| 25 | `GET /reports/missing-udb-persons/v1/` | `25_reports_endpoint_to_get_missing_and_udb_persons_.json` | **not ingested** | Held pending CCTNS-provider clarification (see prior session's endpoint-coverage audit); no ETL module currently consumes this endpoint, so no table is proposed for it. |
| 26 | `GET /reports/arrest/arrest-particulars/v1/` | `26_reports_endpoint_to_get_arrest_particulars_data_.json` | **not ingested** | same status as #25 |
| 27 | `GET /reports/citizen/arrest/arrest-particulars/v1/` | `27_citizen_portal_public_endpoint_for_the_citizen_portal_t.json` | **not ingested** | same status as #25 |
| 28 | `GET /reports/stolen-automobiles` (bulk) | `28_reports_endpoint_to_get_stolen_automobiles_creat.json` | **not ingested** | same status as #25. Sample data itself is a synthetic schema placeholder (API returned an empty result set for this tenant), so field values are not representative even if this endpoint is adopted later. |
| 29 | `GET /reports/stolen-automobiles/{crimeId}` | `29_reports_endpoint_to_get_stolen_automobiles_using.json` | **not ingested** | same status as #28 |

Endpoints #25–29 are intentionally out of scope for `cctns-v2_schema.sql`: no current ETL module reads them, so no table is proposed. If/when the business decision is made to ingest them, `stolen-automobiles` alone would need a new ~68-column table (field list captured in the earlier endpoint-coverage session output) and the two "arrest-particulars" report endpoints would need their own tables since they return pre-joined/denormalized report rows, not raw entity records.

## 2. Tables not tied to a single endpoint

| Table(s) | Populated by | Notes |
|---|---|---|
| `properties_pending_fk`, `ir_pending_fk` | `etl-properties`, `etl-ir` | Per-entity retry queues for rows whose `crime_id` FK isn't resolvable yet. Distinct from the shared `etl_bookkeeping` (`kind='fk_retry'`) queue below; not part of this revision's consolidation (not named in the request). |
| `etl_bookkeeping` (`kind='fk_retry'`) | `etl-disposal`, `etl_arrests`, `etl_chargesheets`, `etl_fsl_case_property`, `etl_updated_chargesheet` (via the shared `etl_fk_retry_queue.py` module) | Shared FK-retry queue for rows whose `crime_id`/`mo_id` FK isn't resolvable yet. Formerly the dedicated `etl_fk_retry_queue` table. |
| `etl_bookkeeping` (`kind='checkpoint'`, `kind='failure'`) | `etl-address` | Crash-resume checkpoint and per-record failure log for the (LLM-off in pure-CCTNS mode) address-resolution step that runs after `persons` ingestion. Formerly the dedicated `etl_checkpoint` and `etl_address_failures` tables. |
| `etl_bookkeeping` (`kind='run_state'`) | `etl_master/checkpoint_manager.py`, `etl-persons`, `etl-properties`, `etl-disposal`, `etl-accused` | Per-module incremental-run watermark. Formerly the dedicated `etl_run_state` table. |
| `geo_countries`, `geo_reference` | loaded/maintained separately (not CCTNS-sourced) | Static reference data consumed read-only by the knowledge-base (non-LLM) address/geo resolver. |

## 3. Schema validation against current ETL requirements

Every column in `cctns-v2_schema.sql` was derived directly from a confirmed `INSERT`/`UPDATE`
column list read from the corresponding ETL module's source code (not guessed from the API
response alone), then cross-checked against the columns already present in the existing
production schema (`DB-schema.sql`) this same ETL codebase targets. Result: **no gaps found** —
every field the current pure-CCTNS ETL flow reads, transforms, or writes has a corresponding
column in the proposed schema, for all of:

`hierarchy, crimes, persons, accused, arrests, disposal, properties (+ properties_pending_fk,
property_additional_details), mo_seizures, chargesheets (+ chargesheet_acts,
chargesheet_acts_sections, chargesheet_accused), charge_sheet_updates, fsl_case_property,
interrogation_reports (+ its 23 remaining child tables and ir_pending_fk),
file_media_bookkeeping, etl_bookkeeping, geo_countries, geo_reference`.

(As of this revision, `file_media_bookkeeping` and `etl_bookkeeping` are consolidated
tables replacing 7 and 4 former dedicated tables respectively — see §4.)

Two deliberate, non-destructive additions beyond a literal copy of the production dump (both
already explained inline as comments in `cctns-v2_schema.sql`):

1. **Missing PRIMARY KEY constraints added** for `hierarchy.ps_code`, `chargesheets.id`,
   `files.id`, `fsl_case_property.case_property_id`, `mo_seizures.mo_seizure_id`, and
   `geo_reference.id` — these are the natural/upsert keys the ETL already treats as unique,
   but production does not currently enforce them as real `PRIMARY KEY` constraints. This adds
   integrity checking only; it does not change what the ETL writes or how it upserts.
2. **`persons.geo_resolution_source` / `geo_resolution_confidence`** were added per
   `migrations/add_geo_resolution_audit.sql`, since the `etl-address` step (part of the pure
   pipeline) writes them, even though the specific `DB-schema.sql` dump examined predates that
   migration being applied.

No case was found where the ETL writes to a column that this schema omits, and no case was
found where this schema requires a `NOT NULL` value the ETL cannot guarantee (all
`NOT NULL` constraints above were copied as-is from the columns production already declares
`NOT NULL`, which the ETL therefore already satisfies today).

### Known data-shape caveats carried into the schema (not gaps, just worth flagging)

- **Empty string vs. NULL**: `crimes`, `accused`, most of `arrests`, and almost all of
  `fsl_case_property`'s optional string fields use `""` as their API-level null-sentinel
  rather than JSON `null`. All affected columns are left nullable; `""` values simply pass
  through as empty strings unless the ETL already normalizes them (no schema-level coercion
  is imposed here, to avoid changing ETL behavior).
- **Inconsistent field casing across endpoints**: chargesheets/update-chargesheets use
  camelCase; every other endpoint sampled (crimes, accused, arrests, mo-seizures,
  person-details, property-details, interrogation-reports, case-property, and all report
  endpoints) uses SCREAMING_SNAKE_CASE. This is already handled per-module in the existing ETL
  code and required no schema change.
- **`ARRESTED_DATE` / `DATE_OF_ISSUE_41A`** on the arrests endpoint use a
  `"YYYY-MM-DD HH:MM:SS"` (no timezone) string format, unlike every other `DATE_CREATED` /
  `DATE_MODIFIED` field's full ISO-8601-with-`Z` format. Columns are typed
  `timestamp with time zone` to match the rest of `arrests`, consistent with what the existing
  ETL already parses into.
- **`ACCUSED_CODE` and `IS_ABSCONDING`** on the arrests bulk endpoint can be entirely absent as
  JSON keys (not merely `null`) on ~3% of sampled records — both columns are nullable.
- **Ten `ir_*` child tables were always empty in the sampled data**
  (`ir_conviction_acquittal`, `ir_defence_counsel`, `ir_execution_of_nbw`, `ir_jail_sentence`,
  `ir_new_gang_formation`, `ir_pending_nbw`, `ir_previous_offences_confessed`,
  `ir_property_disposal`, `ir_regularization_transit_warrants`, `ir_sureties`). Their column
  lists come from the existing production schema (which the ETL code already targets), not
  from the sampled response data — flagged here in case a future, larger sample surfaces
  additional fields CCTNS sends for these sub-entities that today's ETL doesn't yet map.

## 4. Bookkeeping consolidation (revision 2)

### 4.1 Why

The schema had one bookkeeping table per concern, added independently as each ETL module
grew. Every table in each family stored the same underlying *kind* of row:

- **ETL bookkeeping** (`etl_checkpoint`, `etl_run_state`, `etl_fk_retry_queue`,
  `etl_address_failures`): each is "which ETL module, what state, when" — a resume
  cursor, a watermark, a parked failed record, or a quarantine entry. Only the specific
  fields populated and the upsert key differed per table.
- **File/media bookkeeping** (`files`, `property_media`, `mo_seizure_media`,
  `chargesheet_files`, `chargesheet_media`, `fsl_case_property_media`, `ir_media`): each
  is "which parent CCTNS record, which API field, which file/media item, has it been
  downloaded" — `files` already had this exact shape (`source_type`/`source_field`
  enums existed specifically to describe *all* of these entities' file/media fields, see
  `source_type_enum`/`source_field_enum` in `cctns-v2_schema.sql`), the 6 per-entity
  tables were simply never migrated onto it.

Both were merged into one table per family, with a `kind` (`etl_bookkeeping`) or
`source_type`/`source_field` (`file_media_bookkeeping`, reusing the enums the `files`
table already had) discriminator column.

### 4.2 Before → after mapping

| Former table | Consolidated into | Discriminator | Key columns renamed |
|---|---|---|---|
| `etl_checkpoint` | `etl_bookkeeping` | `kind = 'checkpoint'` | `last_seen_id` → `checkpoint_value` |
| `etl_run_state` | `etl_bookkeeping` | `kind = 'run_state'` | `last_successful_end` → `watermark` |
| `etl_fk_retry_queue` | `etl_bookkeeping` | `kind = 'fk_retry'` | `queue_id` → `id`; `error_detail` → `reason` |
| `etl_address_failures` | `etl_bookkeeping` | `kind = 'failure'` | `person_id` → `record_key` (scoped by `module_name = 'etl-address'`); `attempted` → `attempt_count`; `last_try` → `last_attempted_at` |
| `files` | `file_media_bookkeeping` | (unchanged: `source_type`/`source_field`) | unchanged |
| `property_media` | `file_media_bookkeeping` | `source_type='property'`, `source_field='MEDIA'` | `property_id` → `parent_id`; `media_index` → `file_index`; `media_file_id` → `file_id` |
| `mo_seizure_media` | `file_media_bookkeeping` | `source_type='mo_seizures'`, `source_field='MO_MEDIA'` | `mo_seizure_id` → `parent_id`; `media_index` → `file_index`; `media_file_id` → `file_id` |
| `chargesheet_files` + `chargesheet_media` (formerly 2 rows per upload — same `uploadChargeSheet.fileId` event, keyed differently) | `file_media_bookkeeping` | `source_type='chargesheets'`, `source_field='uploadChargeSheet'` | `chargesheet_id` → `parent_id` (now always the natural `charge_sheet_id`, not the synthetic `chargesheets.id`); now written **once** per upload instead of twice |
| `fsl_case_property_media` | `file_media_bookkeeping` | `source_type='case_property'`, `source_field='MEDIA'` | `case_property_id` → `parent_id`; `media_index` → `file_index` |
| `ir_media` | `file_media_bookkeeping` | `source_type='interrogation'`, `source_field='MEDIA'` | `interrogation_report_id` → `parent_id`; `media_id` → `file_id` |

New columns added to `file_media_bookkeeping` beyond the original `files` table to hold
the extra data the 6 folded-in tables carried that `files` didn't: `media_url` (raw
API-provided URL — kept **separate** from the trigger-managed `file_url` so it's never
silently overwritten), `media_name`, `media_payload` (jsonb), `updated_at`.

### 4.3 ETL code updated (minimum required for compatibility)

| File | Change |
|---|---|
| `etl-address/io_layer/checkpoint.py` | `etl_checkpoint` → `etl_bookkeeping` (`kind='checkpoint'`) |
| `etl-address/io_layer/failures.py` | `etl_address_failures` → `etl_bookkeeping` (`kind='failure'`), added `MODULE_NAME` constant |
| `etl-address/io_layer/reader.py` | quarantine sub-query repointed to `etl_bookkeeping` |
| `etl_fk_retry_queue.py` (shared module used by disposal/arrests/chargesheets/updated_chargesheet/fsl_case_property) | `etl_fk_retry_queue` → `etl_bookkeeping` (`kind='fk_retry'`); `queue_id`→`id`, `error_detail`→`reason` |
| `etl_master/checkpoint_manager.py` | `etl_run_state` → `etl_bookkeeping` (`kind='run_state'`) |
| `etl-persons/etl_persons.py`, `etl-properties/etl_properties.py`, `etl-disposal/etl_disposal.py`, `etl-accused/etl_accused.py` | each module's own duplicated `ensure_run_state_table`/`get_run_checkpoint`/`update_run_checkpoint` repointed to `etl_bookkeeping` (`kind='run_state'`) |
| `etl-properties/etl_properties.py` | `replace_property_media()` → writes `file_media_bookkeeping` |
| `etl_mo_seizures/etl_mo_seizure.py` | `sync_seizure_media()`/`get_existing_seizure_media()` → `file_media_bookkeeping` |
| `etl_chargesheets/etl_chargesheets.py` | `insert_chargesheet_file()` → single write to `file_media_bookkeeping` (was 2 writes to `chargesheet_files`+`chargesheet_media`); `delete_related_tables()` updated; `get_effective_start_date()`'s chargesheet_files date-check entry removed (see note below) |
| `etl_fsl_case_property/etl_fsl_case_property.py` | `insert_media_files()` → `file_media_bookkeeping`; `ensure_media_table_ready()` simplified to an existence check (the former per-module auto-create/backfill/FK-add logic is superseded by the centrally-provisioned schema) |
| `etl-ir/ir_etl.py` | `MEDIA[]` insert → `file_media_bookkeeping`; `delete_related_records()` updated (the generic per-table delete loop can't target `file_media_bookkeeping`, which has no `interrogation_report_id` column — a real behavior gap that had to be fixed, not just a rename: without this, stale IR media rows would never be cleared on re-ingest) |
| `etl-files/etl_pipeline_files/load/files_loader.py` | `INSERT INTO files` → `INSERT INTO file_media_bookkeeping` (hardcoded literal) |
| `etl-files/etl_pipeline_files/utils/idempotency.py` | both `FROM files` queries → `file_media_bookkeeping` (hardcoded literal) |
| `etl-files/etl_pipeline_files/main.py`, `main_standalone.py` | hardcoded resume-date query → `file_media_bookkeeping` |
| `etl-files/update_file_urls_with_extensions/update_file_urls_with_extensions.py` | all hardcoded `files` SQL/trigger references → `file_media_bookkeeping` |
| `etl-files/diagnose_missing_files.py`, `etl-files/diagnose_new_apis.py`, `etl-files/etl_files_media_server/diagnose_new_apis.py` | hardcoded `FROM files` → `file_media_bookkeeping` (diagnostic/read-only scripts) |
| `.env` | `FILES_TABLE=files` → `FILES_TABLE=file_media_bookkeeping` — this alone repoints every module that already resolved the table name from this env var (`etl-files/etl_files_media_server/main.py`, both `sync_files_state.py` copies) with no code change needed |

Not touched (out of scope — not named in the consolidation request, and each is a
distinct mechanism): `properties_pending_fk`, `ir_pending_fk` (per-entity FK retry,
separate from the *shared* `etl_fk_retry_queue`/`etl_bookkeeping kind='fk_retry'`).

**Note on `etl_chargesheets/etl_chargesheets.py`'s `get_effective_start_date()`:** this
method scans up to 4 chargesheet-related tables for `MAX(date)` to pick a resume date.
`file_media_bookkeeping` is now shared by every entity, so an unfiltered `MAX(date)` scan
of it would pick up dates from unrelated entities (persons, crimes, ...) and could push
the chargesheets resume date too far forward. Rather than risk that behavior change, the
`chargesheet_files` entry was dropped from this specific scan; `chargesheets`,
`chargesheet_acts`, and `chargesheet_accused` (written in the same batch per chargesheet)
remain as signal.

### 4.4 Validation: reference search

Full-repository search (`grep -rniE`, all `*.py`, comments distinguished from live SQL):

- `etl_checkpoint`, `etl_address_failures`, `etl_fk_retry_queue`, `etl_run_state`: **zero
  remaining live SQL/table references.** The only remaining text matches are (a) the
  Python import `from etl_fk_retry_queue import ...` — the shared module's *filename*,
  which still describes what it does (a retry-queue helper) and was not renamed, since
  the task asked to update table references, not module filenames — and (b) explanatory
  comments documenting the consolidation itself.
- `files` as a bare SQL table name (`FROM files`, `INTO files`, `UPDATE files`,
  `ALTER TABLE files`): **zero remaining occurrences.** All confirmed repointed to
  `file_media_bookkeeping`, either directly or via the `FILES_TABLE` env var.
- `property_media`, `mo_seizure_media`, `chargesheet_files`, `chargesheet_media`,
  `fsl_case_property_media`, `ir_media` as live SQL: **zero remaining occurrences** in
  the ETL modules that own them. The only remaining text matches repo-wide are (a) inert
  `TABLE_CONFIG` dict entries copied as shared boilerplate into ~15 modules' `config.py`
  files that never actually query that table (e.g. `etl-crimes/config.py` defines an
  `'ir_media'` key purely as unused boilerplate — `etl-crimes` never touches IR tables)
  and (b) explanatory comments.

### 4.5 Full table classification

`CCTNS_ENDPOINT_DATA` = populated directly from one CCTNS endpoint's top-level record.
`CCTNS_NESTED_DATA` = populated from a nested array/object within a CCTNS endpoint's
response, normalized into its own table. `ETL_BOOKKEEPING` / `FILE_MEDIA_BOOKKEEPING` =
the two consolidated tables. `REFERENCE_DATA` = static lookup data, not CCTNS-sourced.

| Table | Type | CCTNS Endpoint/Source | Why Required | ETL Modules Using It |
|---|---|---|---|---|
| `hierarchy` | CCTNS_ENDPOINT_DATA | `GET /master-data/hierarchy` | PS→ADG org hierarchy; `crimes.ps_code` FK target | `etl-hierarchy` |
| `geo_countries` | REFERENCE_DATA | not CCTNS-sourced | country/state/timezone lookup for the non-LLM address resolver | `etl-address` (read-only) |
| `geo_reference` | REFERENCE_DATA | not CCTNS-sourced | India state/district/sub-district/village lookup for the non-LLM address resolver | `etl-address` (read-only) |
| `crimes` | CCTNS_ENDPOINT_DATA | `GET /crimes` (+detail) | core FIR/crime record; join key for nearly every other entity | `etl-crimes`; `section-wise-case-clarification` (writes `class_classification`); `etl_case_status` (writes `case_status`) |
| `persons` | CCTNS_ENDPOINT_DATA | `GET /person-details/{personId}` | person master; referenced by accused/arrests/IR/chargesheets | `etl-persons`; `etl-address` (geo columns); `domicile_classification`; `fix_fullname/*` (name-field cleanup) |
| `accused` | CCTNS_ENDPOINT_DATA | `GET /accused` (+detail) | links a person to a crime as accused, with physical features | `etl-accused` |
| `arrests` | CCTNS_ENDPOINT_DATA | `GET /arrests` (+detail) | arrest status per accused-per-crime | `etl_arrests` |
| `disposal` | CCTNS_ENDPOINT_DATA | `GET /crimes/disposal` (+detail) | case disposal outcome | `etl-disposal` |
| `properties` | CCTNS_ENDPOINT_DATA | `GET /property-details` (+detail) | seized/recovered property | `etl-properties` |
| `properties_pending_fk` | ETL_BOOKKEEPING | derived (not CCTNS) | retry queue for property rows whose `crime_id` isn't resolvable yet | `etl-properties` |
| `property_additional_details` | CCTNS_NESTED_DATA | `GET /property-details` (`ADDITIONAL_DETAILS`) | normalized queryable snapshot of the CATEGORY-specific details object | `etl-properties` |
| `mo_seizures` | CCTNS_ENDPOINT_DATA | `GET /mo-seizures` (+detail) | modus-operandi seizure records | `etl_mo_seizures` |
| `chargesheets` | CCTNS_ENDPOINT_DATA | `GET /chargesheets` (+detail) | chargesheet record | `etl_chargesheets` |
| `chargesheet_acts` | CCTNS_NESTED_DATA | `GET /chargesheets` (`actsAndSections[]`) | acts/sections charged, keyed by `chargesheets.id` | `etl_chargesheets` |
| `chargesheet_acts_sections` | CCTNS_NESTED_DATA | `GET /chargesheets` (`actsAndSections[]`, alt. normalized form) | same data keyed by the natural `charge_sheet_id` with explicit ordinals | `etl_chargesheets` |
| `chargesheet_accused` | CCTNS_NESTED_DATA | `GET /chargesheets` (`accusedParticulars[]`) | accused-level charge status per chargesheet | `etl_chargesheets` |
| `charge_sheet_updates` | CCTNS_ENDPOINT_DATA | `GET /update-chargesheets` (+detail) | chargesheet-status update / takenOnFile record | `etl_updated_chargesheet` |
| `fsl_case_property` | CCTNS_ENDPOINT_DATA | `GET /case-property` (+detail) | forensic/case-property register entry | `etl_fsl_case_property` |
| `interrogation_reports` | CCTNS_ENDPOINT_DATA | `GET /interrogation-reports/v1/` (+detail) | interrogation report main record | `etl-ir` |
| `ir_associate_details` | CCTNS_NESTED_DATA | `ASSOCIATE_DETAILS[]` | associate/gang links per IR | `etl-ir` |
| `ir_consumer_details` | CCTNS_NESTED_DATA | `CONSUMER_DETAILS[]` | drug-consumption details per IR | `etl-ir` |
| `ir_conviction_acquittal` | CCTNS_NESTED_DATA | `CONVICTION_ACQUITTAL[]` | prior conviction/acquittal history per IR | `etl-ir` |
| `ir_defence_counsel` | CCTNS_NESTED_DATA | `DEFENCE_COUNSEL[]` | defence counsel details per IR | `etl-ir` |
| `ir_dopams_links` | CCTNS_NESTED_DATA | `DOPAMS_LINKS[]` | phone-number-linked DOPAMS data refs per IR | `etl-ir` |
| `ir_execution_of_nbw` | CCTNS_NESTED_DATA | `EXECUTION_OF_NBW[]` | non-bailable-warrant execution history per IR | `etl-ir` |
| `ir_family_history` | CCTNS_NESTED_DATA | `FAMILY_HISTORY[]` | family member details per IR | `etl-ir` |
| `ir_financial_history` | CCTNS_NESTED_DATA | `FINANCIAL_HISTORY[]` | bank/financial details per IR | `etl-ir` |
| `ir_indulgance_before_offence` | CCTNS_NESTED_DATA | `INDULGANCE_BEFORE_OFFENCE` | free-text pre-offence indulgence note per IR | `etl-ir` |
| `ir_interrogation_report_refs` | CCTNS_NESTED_DATA | `INTERROGATION_REPORT[]` | report/document reference ids per IR | `etl-ir` |
| `ir_jail_sentence` | CCTNS_NESTED_DATA | `JAIL_SENTENCE[]` | jail sentence history per IR | `etl-ir` |
| `ir_local_contacts` | CCTNS_NESTED_DATA | `LOCAL_CONTACTS[]` | local contact details per IR | `etl-ir` |
| `ir_modus_operandi` | CCTNS_NESTED_DATA | `MODUS_OPERANDI[]` | crime-head/sub-head MO details per IR | `etl-ir` |
| `ir_new_gang_formation` | CCTNS_NESTED_DATA | `NEW_GANG_FORMATION[]` | new gang formation details per IR | `etl-ir` |
| `ir_pending_nbw` | CCTNS_NESTED_DATA | `PENDING_NBW[]` | pending non-bailable-warrant details per IR | `etl-ir` |
| `ir_previous_offences_confessed` | CCTNS_NESTED_DATA | `PREVIOUS_OFFENCES_CONFESSED[]` | confessed prior offences per IR | `etl-ir` |
| `ir_property_disposal` | CCTNS_NESTED_DATA | `PROPERTY_DISPOSAL[]` | stolen/seized property disposal details per IR | `etl-ir` |
| `ir_regular_habits` | CCTNS_NESTED_DATA | `REGULAR_HABITS[]` | flat habit-enum list per IR | `etl-ir` |
| `ir_regularization_transit_warrants` | CCTNS_NESTED_DATA | `REGULARIZATION_OF_TRANSIT_WARRANTS[]` | transit warrant regularization details per IR | `etl-ir` |
| `ir_shelter` | CCTNS_NESTED_DATA | `SHELTER[]` | shelter/residency details per IR | `etl-ir` |
| `ir_sim_details` | CCTNS_NESTED_DATA | `SIM_DETAILS[]` | phone/SIM details per IR | `etl-ir` |
| `ir_sureties` | CCTNS_NESTED_DATA | `SURETIES[]` | surety/bail details per IR | `etl-ir` |
| `ir_types_of_drugs` | CCTNS_NESTED_DATA | `TYPES_OF_DRUGS[]` | drug type/quantity/source details per IR | `etl-ir` |
| `ir_pending_fk` | ETL_BOOKKEEPING | derived (not CCTNS) | retry queue for IR rows whose `crime_id` isn't resolvable yet | `etl-ir` |
| `file_media_bookkeeping` | FILE_MEDIA_BOOKKEEPING | every endpoint's file/media fields (`FIR_COPY`, crime `MEDIA`, person `MEDIA`/`IDENTITY_DETAILS`, property `MEDIA`, mo-seizures `MO_MEDIA`, chargesheets `uploadChargeSheet`, case-property `MEDIA`, interrogation `MEDIA`/`INTERROGATION_REPORT`/`DOPAMS_DATA`) + `GET /files/{fileId}` (download status) | single consolidated file/media reference + download-tracking table (see §4.2) | `etl-files/etl_pipeline_files` (discovery), `etl-files/etl_files_media_server` (download), `etl-properties`, `etl_mo_seizures`, `etl_chargesheets`, `etl_fsl_case_property`, `etl-ir` |
| `etl_bookkeeping` | ETL_BOOKKEEPING | derived (not CCTNS) | single consolidated ETL checkpoint/watermark/retry/failure table (see §4.2) | `etl-address` (checkpoint, failure), `etl_master`/`etl-persons`/`etl-properties`/`etl-disposal`/`etl-accused` (run_state), `etl-disposal`/`etl_arrests`/`etl_chargesheets`/`etl_fsl_case_property`/`etl_updated_chargesheet` via `etl_fk_retry_queue.py` (fk_retry) |

45 tables total (previously 53): 43 unchanged CCTNS entity/nested/reference tables, plus
the 2 consolidated bookkeeping tables replacing the former 11 (7 file/media + 4 ETL).
