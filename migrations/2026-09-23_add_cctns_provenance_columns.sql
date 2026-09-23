-- Migration: add CCTNS V2 source-provenance columns to core ingestion tables
--
-- Today no core table records where a row came from beyond its own natural
-- CCTNS ID (crime_id, accused_id, person_id, ...), which already serves as
-- source_id. This adds the remaining minimum needed to answer "where did
-- this record originate": which CCTNS V2 endpoint produced it, which ETL
-- run fetched it, and when.
--
--   source_system    TEXT      constant 'CCTNS_V2' (room for a future source)
--   source_endpoint  TEXT      the CCTNS V2 API path this row was fetched from
--   fetched_at       TIMESTAMPTZ  when the ETL fetched/wrote this row
--   etl_run_id       UUID      master_etl.py run identifier (ETL_RUN_ID env var)
--
-- source_created_at/source_updated_at are intentionally NOT added: every
-- table below already has date_created/date_modified populated directly
-- from the CCTNS API's DATE_CREATED/DATE_MODIFIED fields, so they already
-- serve that role.
--
-- Run once on target database. Safe to rerun — uses IF NOT EXISTS throughout.
-- source_system gets a DEFAULT so existing rows are backfilled for free
-- (cheap metadata-only change in Postgres 11+, no table rewrite/lock).
-- source_endpoint/fetched_at/etl_run_id are left NULL on pre-existing rows
-- (we don't know retroactively when/how they were first fetched) and get
-- populated going forward by each ETL script.

DO $$
DECLARE
    t TEXT;
BEGIN
    FOREACH t IN ARRAY ARRAY[
        'hierarchy',
        'crimes',
        'accused',
        'persons',
        'properties',
        'interrogation_reports',
        'disposal',
        'arrests',
        'mo_seizures',
        'chargesheets',
        'charge_sheet_updates',
        'fsl_case_property',
        'files'
    ]
    LOOP
        EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS source_system TEXT DEFAULT %L', t, 'CCTNS_V2');
        EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS source_endpoint TEXT', t);
        EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS fetched_at TIMESTAMPTZ', t);
        EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS etl_run_id UUID', t);
    END LOOP;
END $$;

-- Verification query
-- SELECT table_name, column_name, data_type
-- FROM information_schema.columns
-- WHERE column_name IN ('source_system', 'source_endpoint', 'fetched_at', 'etl_run_id')
--   AND table_schema = 'public'
-- ORDER BY table_name, column_name;

-- Audit query after running the pure-CCTNS pipeline:
-- SELECT source_system, source_endpoint, etl_run_id, COUNT(*), MAX(fetched_at)
-- FROM accused
-- GROUP BY source_system, source_endpoint, etl_run_id
-- ORDER BY MAX(fetched_at) DESC;
