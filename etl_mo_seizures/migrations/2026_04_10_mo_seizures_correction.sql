-- MO Seizures correction migration
-- Purpose:
-- 1. Normalize media handling into a child table
-- 2. Clean blank values to NULL for optional fields
-- 3. Convert coordinate columns to numeric types
-- 4. Add incremental sync index on date_modified
--
-- Safe to re-run:
-- - Uses IF NOT EXISTS where supported
-- - Uses catalog checks before type changes
-- - Backfills child media rows from the existing parent table

BEGIN;

-- -----------------------------------------------------------------------------
-- 1) Create normalized child table for media references
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.mo_seizure_media (
    id BIGSERIAL PRIMARY KEY,
    mo_seizure_id VARCHAR(50) NOT NULL,
    media_index INTEGER NOT NULL DEFAULT 0,
    media_file_id TEXT,
    media_url TEXT,
    media_name TEXT,
    date_created TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    date_modified TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT mo_seizure_media_unique_entry UNIQUE (mo_seizure_id, media_index)
);

COMMENT ON TABLE public.mo_seizure_media IS 'Normalized media references for mo_seizures. One row per media item.';
COMMENT ON COLUMN public.mo_seizure_media.mo_seizure_id IS 'Foreign key to mo_seizures.mo_seizure_id';
COMMENT ON COLUMN public.mo_seizure_media.media_index IS 'Zero-based ordering of media items in the source payload';
COMMENT ON COLUMN public.mo_seizure_media.media_file_id IS 'Media file identifier from API';
COMMENT ON COLUMN public.mo_seizure_media.media_url IS 'Media URL from API';
COMMENT ON COLUMN public.mo_seizure_media.media_name IS 'Media file name from API';
COMMENT ON COLUMN public.mo_seizure_media.date_created IS 'Record creation timestamp (source timestamp or load timestamp)';
COMMENT ON COLUMN public.mo_seizure_media.date_modified IS 'Record modification timestamp (source timestamp or load timestamp)';

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'mo_seizure_media_mo_seizure_id_fkey'
          AND conrelid = 'public.mo_seizure_media'::regclass
    ) THEN
        ALTER TABLE public.mo_seizure_media
            ADD CONSTRAINT mo_seizure_media_mo_seizure_id_fkey
            FOREIGN KEY (mo_seizure_id)
            REFERENCES public.mo_seizures (mo_seizure_id)
            ON DELETE CASCADE;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_mo_seizure_media_mo_seizure_id
    ON public.mo_seizure_media USING btree (mo_seizure_id);

CREATE INDEX IF NOT EXISTS idx_mo_seizure_media_media_file_id
    ON public.mo_seizure_media USING btree (media_file_id);

-- -----------------------------------------------------------------------------
-- 2) Add/verify performance index on mo_seizures.date_modified
-- -----------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_mo_seizures_date_modified
    ON public.mo_seizures USING btree (date_modified);

-- -----------------------------------------------------------------------------
-- 3) Convert coordinate columns to numeric types safely
-- -----------------------------------------------------------------------------
DO $$
-- Handle dependency: firs_mv materialized view may depend on mo_seizures coordinates.
-- Backup definition + indexes, drop view, alter columns, then recreate.
DECLARE
    v_firs_mv_exists BOOLEAN;
    v_firs_mv_def TEXT;
    v_firs_mv_indexes TEXT[];
    v_idx_sql TEXT;
BEGIN
    v_firs_mv_exists := to_regclass('public.firs_mv') IS NOT NULL;

    IF v_firs_mv_exists THEN
        SELECT definition
        INTO v_firs_mv_def
        FROM pg_matviews
        WHERE schemaname = 'public'
          AND matviewname = 'firs_mv';

        SELECT array_agg(indexdef ORDER BY indexname)
        INTO v_firs_mv_indexes
        FROM pg_indexes
        WHERE schemaname = 'public'
          AND tablename = 'firs_mv';

        EXECUTE 'DROP MATERIALIZED VIEW public.firs_mv';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'mo_seizures'
          AND column_name = 'pos_latitude'
          AND data_type <> 'double precision'
    ) THEN
        ALTER TABLE public.mo_seizures
            ALTER COLUMN pos_latitude TYPE double precision
            USING CASE
                WHEN NULLIF(BTRIM(pos_latitude), '') IS NULL THEN NULL
                WHEN NULLIF(BTRIM(pos_latitude), '') ~ '^[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?$' THEN NULLIF(BTRIM(pos_latitude), '')::double precision
                ELSE NULL
            END;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'mo_seizures'
          AND column_name = 'pos_longitude'
          AND data_type <> 'double precision'
    ) THEN
        ALTER TABLE public.mo_seizures
            ALTER COLUMN pos_longitude TYPE double precision
            USING CASE
                WHEN NULLIF(BTRIM(pos_longitude), '') IS NULL THEN NULL
                WHEN NULLIF(BTRIM(pos_longitude), '') ~ '^[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?$' THEN NULLIF(BTRIM(pos_longitude), '')::double precision
                ELSE NULL
            END;
    END IF;

    IF v_firs_mv_exists AND v_firs_mv_def IS NOT NULL THEN
        EXECUTE 'CREATE MATERIALIZED VIEW public.firs_mv AS ' || v_firs_mv_def;

        IF v_firs_mv_indexes IS NOT NULL THEN
            FOREACH v_idx_sql IN ARRAY v_firs_mv_indexes LOOP
                EXECUTE v_idx_sql;
            END LOOP;
        END IF;
    END IF;
END $$;

COMMENT ON COLUMN public.mo_seizures.pos_latitude IS 'Latitude in decimal degrees';
COMMENT ON COLUMN public.mo_seizures.pos_longitude IS 'Longitude in decimal degrees';

-- -----------------------------------------------------------------------------
-- 4) Normalize blank optional values in the parent table
-- -----------------------------------------------------------------------------
UPDATE public.mo_seizures
   SET seq_no = NULLIF(BTRIM(seq_no), ''),
       mo_id = NULLIF(BTRIM(mo_id), ''),
       type = NULLIF(BTRIM(type), ''),
       sub_type = NULLIF(BTRIM(sub_type), ''),
       description = NULLIF(BTRIM(description), ''),
       seized_from = NULLIF(BTRIM(seized_from), ''),
       seized_by = NULLIF(BTRIM(seized_by), ''),
       strength_of_evidence = NULLIF(BTRIM(strength_of_evidence), ''),
       pos_address1 = NULLIF(BTRIM(pos_address1), ''),
       pos_address2 = NULLIF(BTRIM(pos_address2), ''),
       pos_city = NULLIF(BTRIM(pos_city), ''),
       pos_district = NULLIF(BTRIM(pos_district), ''),
       pos_pincode = NULLIF(BTRIM(pos_pincode), ''),
       pos_landmark = NULLIF(BTRIM(pos_landmark), ''),
       pos_description = NULLIF(BTRIM(pos_description), ''),
       mo_media_url = NULLIF(BTRIM(mo_media_url), ''),
       mo_media_name = NULLIF(BTRIM(mo_media_name), ''),
       mo_media_file_id = NULLIF(BTRIM(mo_media_file_id), '')
 WHERE seq_no = ''
    OR mo_id = ''
    OR type = ''
    OR sub_type = ''
    OR description = ''
    OR seized_from = ''
    OR seized_by = ''
    OR strength_of_evidence = ''
    OR pos_address1 = ''
    OR pos_address2 = ''
    OR pos_city = ''
    OR pos_district = ''
    OR pos_pincode = ''
    OR pos_landmark = ''
    OR pos_description = ''
    OR mo_media_url = ''
    OR mo_media_name = ''
    OR mo_media_file_id = '';

-- -----------------------------------------------------------------------------
-- 5) Backfill normalized media rows from existing parent media columns
-- -----------------------------------------------------------------------------
INSERT INTO public.mo_seizure_media (
    mo_seizure_id,
    media_index,
    media_file_id,
    media_url,
    media_name,
    date_created,
    date_modified
)
SELECT
    mo_seizure_id,
    0 AS media_index,
    NULLIF(BTRIM(mo_media_file_id), '') AS media_file_id,
    NULLIF(BTRIM(mo_media_url), '') AS media_url,
    NULLIF(BTRIM(mo_media_name), '') AS media_name,
    COALESCE(date_created, CURRENT_TIMESTAMP) AS date_created,
    COALESCE(date_modified, CURRENT_TIMESTAMP) AS date_modified
FROM public.mo_seizures
WHERE NULLIF(BTRIM(mo_media_file_id), '') IS NOT NULL
   OR NULLIF(BTRIM(mo_media_url), '') IS NOT NULL
   OR NULLIF(BTRIM(mo_media_name), '') IS NOT NULL
ON CONFLICT (mo_seizure_id, media_index) DO UPDATE
SET media_file_id = EXCLUDED.media_file_id,
    media_url = EXCLUDED.media_url,
    media_name = EXCLUDED.media_name,
    date_modified = EXCLUDED.date_modified;

COMMIT;
