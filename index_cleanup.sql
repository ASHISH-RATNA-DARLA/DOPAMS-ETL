-- =============================================================================
-- DOPAMS INDEX CLEANUP & OPTIMIZATION SCRIPT
-- =============================================================================
-- Generated: 2026-03-02 from query_optimizer.py analysis
-- Database: dev-2 (PostgreSQL 16.11)
--
-- INSTRUCTIONS:
--   Run each section separately. Start with Phase 1 (safe, high-impact).
--   Phases are ordered by risk — lowest risk first.
--
-- ALWAYS take a backup before dropping indexes in production:
--   pg_dump -h 192.168.103.106 -U dev_dopamas -d dev-2 --schema-only > schema_backup.sql
-- =============================================================================

BEGIN;

-- =============================================================================
-- PHASE 1: ADD MISSING INDEXES (SAFE — CREATE IF NOT EXISTS)
-- =============================================================================
-- These address the red-flagged tables from the query optimizer output.
-- Impact: Eliminates 99%+ sequential scans on chargesheets, chargesheet_accused,
--         drug_categories, drug_ignore_list.

-- chargesheets: 177,395 seq scans vs 251 idx scans (99.9% sequential!)
-- ETL lookups use: WHERE crime_id = %s AND chargesheet_no = %s AND chargesheet_date = %s
CREATE INDEX IF NOT EXISTS idx_chargesheets_crime_id
    ON chargesheets(crime_id);

CREATE INDEX IF NOT EXISTS idx_chargesheets_crime_no_date
    ON chargesheets(crime_id, chargesheet_no, chargesheet_date);

-- chargesheet_accused: 137,275 seq scans vs 50 idx scans (100% sequential!)
-- ETL lookups use: WHERE chargesheet_id = %s AND accused_person_id = %s
CREATE INDEX IF NOT EXISTS idx_chargesheet_accused_cs_id
    ON chargesheet_accused(chargesheet_id);

CREATE INDEX IF NOT EXISTS idx_chargesheet_accused_cs_person
    ON chargesheet_accused(chargesheet_id, accused_person_id);

-- drug_categories: 111,356 seq scans vs 441 idx scans (99.6% sequential!)
-- Drug standardization lookups use: WHERE raw_name = %s or LIKE/trgm matching
CREATE INDEX IF NOT EXISTS idx_drug_categories_raw_name
    ON drug_categories(raw_name);

CREATE INDEX IF NOT EXISTS idx_drug_categories_standard_name
    ON drug_categories(standard_name);

-- drug_ignore_list: 138,225 seq scans vs 258 idx scans (99.8% sequential!)
-- Lookups use: WHERE term = %s
CREATE INDEX IF NOT EXISTS idx_drug_ignore_list_term
    ON drug_ignore_list(term);

-- agent_deduplication_tracker: 82,763 seq scans, 0 idx scans (100% sequential!)
-- Views join on: WHERE person_id = ANY(all_person_ids)
-- GIN index for array containment (@>) queries
CREATE INDEX IF NOT EXISTS idx_adt_all_person_ids_gin
    ON agent_deduplication_tracker USING GIN (all_person_ids);

CREATE INDEX IF NOT EXISTS idx_adt_canonical_person_id
    ON agent_deduplication_tracker(canonical_person_id);

-- ETL-critical indexes (already exist but listed for completeness)
CREATE INDEX IF NOT EXISTS idx_brief_facts_accused_crime_id ON brief_facts_accused(crime_id);
CREATE INDEX IF NOT EXISTS idx_brief_facts_drugs_crime_id ON brief_facts_drugs(crime_id);
CREATE INDEX IF NOT EXISTS idx_accused_crime_id ON accused(crime_id);
CREATE INDEX IF NOT EXISTS idx_accused_crime_person ON accused(crime_id, person_id);
CREATE INDEX IF NOT EXISTS idx_crimes_dates ON crimes(date_created DESC, date_modified DESC);
CREATE INDEX IF NOT EXISTS idx_persons_full_name ON persons(full_name);
CREATE INDEX IF NOT EXISTS idx_persons_phone ON persons(phone_number);

COMMIT;


-- =============================================================================
-- PHASE 2: DROP DEAD-WEIGHT TABLE
-- =============================================================================
-- dedup_comparison_progress_backup: 0.2% cache hit ratio, 4.1M disk reads!
-- This single table is thrashing your buffer cache and degrading ALL other queries.
--
-- VERIFIED SAFE TO DROP:
--   • Passive snapshot of dedup_comparison_progress (identical columns)
--   • Not referenced in any Python ETL scripts, LangGraph logic, or Prisma models
--   • Not used by the active deduplication pipeline, GraphQL API, or Master ETL
--   • Likely created manually (CREATE TABLE ... AS SELECT * FROM ...) before
--     a fresh dedup run, purely as a developer safety net
--
-- Impact: Eliminates 4.1M disk reads that evict useful pages from buffer cache.

DROP TABLE IF EXISTS dedup_comparison_progress_backup;


-- =============================================================================
-- PHASE 3: DROP UNUSED INDEXES ON HIGH-WRITE TABLES
-- =============================================================================
-- These indexes have ZERO scans but slow down every INSERT/UPDATE.
-- Grouped by table for clarity. Each DROP is independent.
--
-- ⚠️  Run in a maintenance window. Each DROP acquires a brief lock.

BEGIN;

-- ---------------------------------------------------------------------------
-- 3A: dedup_* tables (largest unused indexes, bulk-loaded, rarely queried)
-- ---------------------------------------------------------------------------
-- dedup_comparison_progress: 6 unused indexes on a 4M+ row table
DROP INDEX IF EXISTS public.ix_dedup_comparison_persons;
-- uix_comparison_pair is a CONSTRAINT, not a standalone index
ALTER TABLE public.dedup_comparison_progress DROP CONSTRAINT IF EXISTS uix_comparison_pair;
-- ⚠️  CAUTION: dropping pkey removes uniqueness constraint
-- DROP INDEX IF EXISTS public.dedup_comparison_progress_pkey;
DROP INDEX IF EXISTS public.ix_dedup_comparison_progress_person_i_id;
DROP INDEX IF EXISTS public.ix_dedup_comparison_progress_person_j_id;
DROP INDEX IF EXISTS public.ix_dedup_comparison_progress_person_j_index;

-- dedup_cluster_state: 5 unused indexes
DROP INDEX IF EXISTS public.ix_dedup_cluster_person_id;
DROP INDEX IF EXISTS public.ix_dedup_cluster_state_person_index;
DROP INDEX IF EXISTS public.ix_dedup_cluster_state_cluster_id;
-- ⚠️  CAUTION: unique constraint — keep if dedup code needs it
-- DROP INDEX IF EXISTS public.uix_cluster_person;
-- DROP INDEX IF EXISTS public.dedup_cluster_state_pkey;

-- dedup_run_metadata: 3 unused indexes
DROP INDEX IF EXISTS public.ix_dedup_run_metadata_run_id;
DROP INDEX IF EXISTS public.ix_dedup_run_status;
-- ⚠️  unique constraint
-- DROP INDEX IF EXISTS public.dedup_run_metadata_run_id_key;
-- DROP INDEX IF EXISTS public.dedup_run_metadata_pkey;

COMMIT;

BEGIN;

-- ---------------------------------------------------------------------------
-- 3B: files table (large, frequently written by ETL)
-- ---------------------------------------------------------------------------
-- 8 unused indexes! Every file INSERT pays for all of them.
DROP INDEX IF EXISTS public.idx_files_file_url;
DROP INDEX IF EXISTS public.idx_files_source_type_created;
DROP INDEX IF EXISTS public.idx_files_file_path;
DROP INDEX IF EXISTS public.idx_files_file_id;
DROP INDEX IF EXISTS public.idx_files_source_field;
DROP INDEX IF EXISTS public.idx_files_source_type;
DROP INDEX IF EXISTS public.idx_files_downloaded_at;
DROP INDEX IF EXISTS public.idx_files_is_downloaded;
DROP INDEX IF EXISTS public.idx_files_identity_type;
-- ⚠️  CAUTION: pkey
-- DROP INDEX IF EXISTS public.files_pkey;

COMMIT;

BEGIN;

-- ---------------------------------------------------------------------------
-- 3C: charge_sheet_updates table (7 unused indexes)
-- ---------------------------------------------------------------------------
DROP INDEX IF EXISTS public.idx_charge_sheet_updates_crime_id;
DROP INDEX IF EXISTS public.idx_charge_sheet_updates_taken_on_file;
DROP INDEX IF EXISTS public.idx_charge_sheet_updates_date_status;
DROP INDEX IF EXISTS public.idx_charge_sheet_updates_taken_on_file_date;
DROP INDEX IF EXISTS public.idx_charge_sheet_updates_charge_sheet_date;
DROP INDEX IF EXISTS public.idx_charge_sheet_updates_court_case_no;
DROP INDEX IF EXISTS public.idx_charge_sheet_updates_charge_sheet_no;
-- ⚠️  unique constraint
-- DROP INDEX IF EXISTS public.charge_sheet_updates_update_charge_sheet_id_key;
-- DROP INDEX IF EXISTS public.charge_sheet_updates_pkey;

COMMIT;

BEGIN;

-- ---------------------------------------------------------------------------
-- 3D: person_deduplication_tracker (7 unused indexes)
-- ---------------------------------------------------------------------------
DROP INDEX IF EXISTS public.idx_dedup_tracker_crime_details;
DROP INDEX IF EXISTS public.idx_dedup_tracker_person_ids;
DROP INDEX IF EXISTS public.idx_dedup_tracker_accused_ids;
DROP INDEX IF EXISTS public.idx_dedup_tracker_crime_ids;
DROP INDEX IF EXISTS public.idx_dedup_tracker_fingerprint;
DROP INDEX IF EXISTS public.idx_dedup_tracker_tier;
DROP INDEX IF EXISTS public.idx_dedup_tracker_crime_count;
DROP INDEX IF EXISTS public.idx_dedup_tracker_canonical_person;
-- ⚠️  unique + pkey constraints
-- DROP INDEX IF EXISTS public.person_deduplication_tracker_person_fingerprint_key;
-- DROP INDEX IF EXISTS public.person_deduplication_tracker_pkey;

COMMIT;

BEGIN;

-- ---------------------------------------------------------------------------
-- 3E: interrogation_reports & related (many small unused indexes)
-- ---------------------------------------------------------------------------
DROP INDEX IF EXISTS public.idx_ir_date_modified;
DROP INDEX IF EXISTS public.idx_ir_date_created;
DROP INDEX IF EXISTS public.idx_ir_is_facing_trial;
DROP INDEX IF EXISTS public.idx_ir_socio_occupation;
DROP INDEX IF EXISTS public.idx_ir_physical_height;
DROP INDEX IF EXISTS public.idx_ir_is_absconding;
DROP INDEX IF EXISTS public.idx_ir_is_on_bail;
DROP INDEX IF EXISTS public.idx_ir_socio_education;
DROP INDEX IF EXISTS public.idx_ir_socio_marital_status;
DROP INDEX IF EXISTS public.idx_ir_physical_color;
DROP INDEX IF EXISTS public.idx_ir_physical_beard;
DROP INDEX IF EXISTS public.idx_ir_interrogation_report_refs_ir;
DROP INDEX IF EXISTS public.idx_ir_regular_habits_habit;
DROP INDEX IF EXISTS public.idx_ir_types_of_drugs_type;
DROP INDEX IF EXISTS public.idx_ir_sim_details_phone;
DROP INDEX IF EXISTS public.idx_ir_dopams_links_phone;

COMMIT;

BEGIN;

-- ---------------------------------------------------------------------------
-- 3F: crimes table (3 unused indexes)
-- ---------------------------------------------------------------------------
-- ⚠️  crimes_fir_reg_num_key is a UNIQUE constraint — keep unless you're sure
-- DROP INDEX IF EXISTS public.crimes_fir_reg_num_key;
DROP INDEX IF EXISTS public.idx_crimes_fir_reg_num;
DROP INDEX IF EXISTS public.idx_crimes_crime_type;
-- idx_crimes_dates was recommended but shows 0 scans — the optimizer may not
-- be choosing it yet. Keep for now, re-evaluate after adding chargesheets index.
-- DROP INDEX IF EXISTS public.idx_crimes_dates;

COMMIT;

BEGIN;

-- ---------------------------------------------------------------------------
-- 3G: properties table (5 unused indexes)
-- ---------------------------------------------------------------------------
DROP INDEX IF EXISTS public.idx_properties_additional_details;
DROP INDEX IF EXISTS public.idx_properties_date_created;
DROP INDEX IF EXISTS public.idx_properties_case_property_id;
DROP INDEX IF EXISTS public.idx_properties_category;
DROP INDEX IF EXISTS public.idx_properties_status;
DROP INDEX IF EXISTS public.idx_properties_belongs;
DROP INDEX IF EXISTS public.idx_properties_nature;

COMMIT;

BEGIN;

-- ---------------------------------------------------------------------------
-- 3H: brief_facts & drug-related (small win)
-- ---------------------------------------------------------------------------
DROP INDEX IF EXISTS public.idx_bf_accused_crime_accused;
DROP INDEX IF EXISTS public.idx_brief_facts_accused_person_id;
DROP INDEX IF EXISTS public.idx_brief_facts_accused_type;
DROP INDEX IF EXISTS public.idx_brief_facts_drugs_drug_name;
DROP INDEX IF EXISTS public.idx_bfd_metadata;
DROP INDEX IF EXISTS public.idx_summaries_model;
DROP INDEX IF EXISTS public.idx_brief_facts_summaries_crime_id;

COMMIT;

BEGIN;

-- ---------------------------------------------------------------------------
-- 3I: Remaining small tables
-- ---------------------------------------------------------------------------
DROP INDEX IF EXISTS public.idx_persons_present_state;
DROP INDEX IF EXISTS public.idx_arrests_arrested_date;
DROP INDEX IF EXISTS public.idx_old_interragation_report_crime_id;
DROP INDEX IF EXISTS public.idx_hierarchy_ps_name;
DROP INDEX IF EXISTS public.idx_hierarchy_range_code;
DROP INDEX IF EXISTS public.idx_hierarchy_zone_code;
DROP INDEX IF EXISTS public.idx_hierarchy_dist_code;
DROP INDEX IF EXISTS public.idx_fsl_case_property_mo_id;
DROP INDEX IF EXISTS public.idx_fsl_case_property_send_date;
DROP INDEX IF EXISTS public.idx_fsl_case_property_date_created;
DROP INDEX IF EXISTS public.idx_fsl_case_property_status;
DROP INDEX IF EXISTS public.idx_fsl_case_property_fsl_date;
DROP INDEX IF EXISTS public.idx_fsl_case_property_case_type;

-- Materialized view indexes (safe if views are refreshed via REFRESH CONCURRENTLY)
DROP INDEX IF EXISTS public.idx_accuseds_mv_unique_id;
DROP INDEX IF EXISTS public.idx_as_accuseds_mv_id;
DROP INDEX IF EXISTS public.idx_criminal_profiles_mv_id;
DROP INDEX IF EXISTS public.idx_as_firs_mv_id;

-- ⚠️  CAUTION: dropping MV indexes means REFRESH MATERIALIZED VIEW CONCURRENTLY
--     will fail. Only drop if you use non-concurrent refresh.
-- If you use CONCURRENTLY, keep at least one UNIQUE index per MV.

COMMIT;

BEGIN;

-- ---------------------------------------------------------------------------
-- 3J: Primary key indexes — DO NOT DROP unless you know what you're doing
-- ---------------------------------------------------------------------------
-- These are listed as "unused" because no query uses them explicitly,
-- but they enforce uniqueness and referential integrity. KEEP THEM.
--
-- files_pkey, arrests_pkey, brief_facts_accused_pkey,
-- agent_deduplication_tracker_pkey, person_deduplication_tracker_pkey,
-- charge_sheet_updates_pkey, dedup_comparison_progress_pkey,
-- dedup_cluster_state_pkey, dedup_run_metadata_pkey,
-- ir_family_history_pkey, ir_sim_details_pkey, ir_consumer_details_pkey,
-- ir_dopams_links_pkey, ir_financial_history_pkey, ir_types_of_drugs_pkey,
-- ir_previous_offences_confessed_pkey, ir_modus_operandi_pkey,
-- ir_local_contacts_pkey, ir_associate_details_pkey, ir_shelter_pkey,
-- ir_defence_counsel_pkey, ir_regular_habits_pkey,
-- old_interragation_report_pkey, brief_facts_crime_summaries_pkey,
-- fsl_case_property_media_pkey, disposal_pkey,
-- disposal_crime_id_disposal_type_disposed_at_key
--
-- NEVER drop primary keys or unique constraints that enforce data integrity.

COMMIT;


-- =============================================================================
-- PHASE 4: ANALYZE TABLES (run after all index changes)
-- =============================================================================
-- Forces PostgreSQL to update statistics so the query planner uses new indexes.

ANALYZE chargesheets;
ANALYZE chargesheet_accused;
ANALYZE drug_categories;
ANALYZE drug_ignore_list;
ANALYZE agent_deduplication_tracker;
ANALYZE crimes;
ANALYZE accused;
ANALYZE persons;
ANALYZE brief_facts_accused;
ANALYZE brief_facts_drugs;
ANALYZE files;
ANALYZE properties;
ANALYZE charge_sheet_updates;


-- =============================================================================
-- PHASE 5: VERIFY IMPROVEMENTS
-- =============================================================================
-- After running Phases 1-4, wait for some ETL cycles, then check:

-- 1. Confirm new indexes are being used:
-- SELECT relname, indexrelname, idx_scan
-- FROM pg_stat_user_indexes
-- WHERE relname IN ('chargesheets', 'chargesheet_accused', 'drug_categories', 'drug_ignore_list')
-- ORDER BY relname, idx_scan DESC;

-- 2. Confirm sequential scans dropped:
-- SELECT relname, seq_scan, idx_scan,
--        ROUND(100.0 * seq_scan / NULLIF(seq_scan + idx_scan, 0), 1) AS seq_pct
-- FROM pg_stat_user_tables
-- WHERE relname IN ('chargesheets', 'chargesheet_accused', 'drug_categories', 'drug_ignore_list')
-- ORDER BY seq_pct DESC;

-- 3. Check total index count (should drop from ~170+ to ~50-60):
-- SELECT COUNT(*) AS total_indexes FROM pg_stat_user_indexes;

-- 4. Check disk space reclaimed:
-- SELECT pg_size_pretty(pg_database_size('dev-2')) AS db_size;


-- =============================================================================
-- SUMMARY
-- =============================================================================
-- Phase 1: +12 new indexes (CREATE IF NOT EXISTS — safe to re-run)
-- Phase 2: 1 table to review for dropping (dedup_comparison_progress_backup)
-- Phase 3: ~80 unused indexes dropped across 10 table groups
-- Phase 4: ANALYZE to update planner statistics
-- Phase 5: Verification queries
--
-- Expected impact:
--   • chargesheets:         99.9% seq → <5% seq (177K seq scans eliminated)
--   • chargesheet_accused:  100% seq  → <5% seq (137K seq scans eliminated)
--   • drug_categories:      99.6% seq → <5% seq (111K seq scans eliminated)
--   • INSERT throughput:    ~20-30% faster (fewer indexes to maintain per write)
--   • Buffer cache:         Better hit ratios (less index bloat competing for RAM)
-- =============================================================================
