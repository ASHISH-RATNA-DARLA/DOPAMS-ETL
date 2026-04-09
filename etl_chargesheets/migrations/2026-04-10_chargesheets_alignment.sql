-- MODULE: ChargeSheets
-- Migration: API / ETL / DB alignment with legacy compatibility
-- Safety: idempotent, production-safe, non-destructive, preserves existing tables

BEGIN;

-- -----------------------------------------------------------------------------
-- 1) Parent table alignment
-- -----------------------------------------------------------------------------
ALTER TABLE public.chargesheets
    ADD COLUMN IF NOT EXISTS charge_sheet_id character varying(50);

COMMENT ON COLUMN public.chargesheets.charge_sheet_id IS
    'API chargeSheetId. Natural key used by the chargesheets ETL for overwrite semantics.';

-- Keep legacy surrogate key for compatibility, but make the API key searchable.
CREATE UNIQUE INDEX IF NOT EXISTS idx_chargesheets_charge_sheet_id
    ON public.chargesheets USING btree (charge_sheet_id)
    WHERE charge_sheet_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_chargesheets_date_modified
    ON public.chargesheets USING btree (date_modified);

-- Preserve firs_mv once, then rebuild after all dependent column changes complete.
CREATE TEMP TABLE IF NOT EXISTS tmp_firs_mv_restore (
    definition text,
    indexes text[]
) ON COMMIT DROP;

TRUNCATE tmp_firs_mv_restore;

DO $$
DECLARE
    v_firs_mv_def TEXT;
    v_firs_mv_indexes TEXT[];
BEGIN
    IF to_regclass('public.firs_mv') IS NOT NULL THEN
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

        INSERT INTO tmp_firs_mv_restore(definition, indexes)
        VALUES (v_firs_mv_def, v_firs_mv_indexes);

        EXECUTE 'DROP MATERIALIZED VIEW public.firs_mv';
    END IF;
END;
$$;

-- Convert date columns to timestamptz, treating existing naive values as UTC.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'chargesheets'
          AND column_name = 'chargesheet_date' AND data_type = 'timestamp without time zone'
    ) THEN
        EXECUTE 'ALTER TABLE public.chargesheets ALTER COLUMN chargesheet_date TYPE timestamptz USING CASE WHEN chargesheet_date IS NULL THEN NULL ELSE chargesheet_date AT TIME ZONE ''UTC'' END';
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'chargesheets'
          AND column_name = 'date_created' AND data_type = 'timestamp without time zone'
    ) THEN
        EXECUTE 'ALTER TABLE public.chargesheets ALTER COLUMN date_created TYPE timestamptz USING CASE WHEN date_created IS NULL THEN NULL ELSE date_created AT TIME ZONE ''UTC'' END';
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'chargesheets'
          AND column_name = 'date_modified' AND data_type = 'timestamp without time zone'
    ) THEN
        EXECUTE 'ALTER TABLE public.chargesheets ALTER COLUMN date_modified TYPE timestamptz USING CASE WHEN date_modified IS NULL THEN NULL ELSE date_modified AT TIME ZONE ''UTC'' END';
    END IF;
END;
$$;

-- Normalize blanks to NULL for optional parent fields.
UPDATE public.chargesheets
   SET charge_sheet_id = NULLIF(BTRIM(charge_sheet_id), ''),
       chargesheet_no = NULLIF(BTRIM(chargesheet_no), ''),
       chargesheet_no_icjs = NULLIF(BTRIM(chargesheet_no_icjs), ''),
       chargesheet_type = NULLIF(BTRIM(chargesheet_type), ''),
       court_name = NULLIF(BTRIM(court_name), '')
 WHERE charge_sheet_id = ''
    OR chargesheet_no = ''
    OR chargesheet_no_icjs = ''
    OR chargesheet_type = ''
    OR court_name = '';

-- -----------------------------------------------------------------------------
-- 2) Legacy child tables: convert timestamps to timestamptz and remove blanks
-- -----------------------------------------------------------------------------
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'chargesheet_files'
          AND column_name = 'created_at' AND data_type = 'timestamp without time zone'
    ) THEN
        EXECUTE 'ALTER TABLE public.chargesheet_files ALTER COLUMN created_at TYPE timestamptz USING CASE WHEN created_at IS NULL THEN NULL ELSE created_at AT TIME ZONE ''UTC'' END';
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'chargesheet_acts'
          AND column_name = 'created_at' AND data_type = 'timestamp without time zone'
    ) THEN
        EXECUTE 'ALTER TABLE public.chargesheet_acts ALTER COLUMN created_at TYPE timestamptz USING CASE WHEN created_at IS NULL THEN NULL ELSE created_at AT TIME ZONE ''UTC'' END';
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'chargesheet_accused'
          AND column_name = 'created_at' AND data_type = 'timestamp without time zone'
    ) THEN
        EXECUTE 'ALTER TABLE public.chargesheet_accused ALTER COLUMN created_at TYPE timestamptz USING CASE WHEN created_at IS NULL THEN NULL ELSE created_at AT TIME ZONE ''UTC'' END';
    END IF;
END;
$$;

ALTER TABLE public.chargesheet_acts
    ALTER COLUMN section TYPE text USING NULLIF(BTRIM(section), '');

UPDATE public.chargesheet_files
   SET file_id = NULLIF(BTRIM(file_id), '')
 WHERE file_id = '';

UPDATE public.chargesheet_acts
   SET act_description = NULLIF(BTRIM(act_description), ''),
       section = NULLIF(BTRIM(section), ''),
       section_description = NULLIF(BTRIM(section_description), ''),
       grave_particulars = NULLIF(BTRIM(grave_particulars), '')
 WHERE act_description = ''
    OR section = ''
    OR section_description = ''
    OR grave_particulars = '';

UPDATE public.chargesheet_accused
   SET charge_status = NULLIF(BTRIM(charge_status), ''),
       reason_for_no_charge = NULLIF(BTRIM(reason_for_no_charge), '')
 WHERE charge_status = ''
    OR reason_for_no_charge = '';

-- Recreate firs_mv after all dependent column type changes are complete.
DO $$
DECLARE
    v_firs_mv_def TEXT;
    v_firs_mv_indexes TEXT[];
    v_idx_sql TEXT;
BEGIN
    SELECT definition, indexes
      INTO v_firs_mv_def, v_firs_mv_indexes
      FROM tmp_firs_mv_restore
     LIMIT 1;

    IF v_firs_mv_def IS NOT NULL THEN
        EXECUTE 'CREATE MATERIALIZED VIEW public.firs_mv AS ' || v_firs_mv_def;

        IF v_firs_mv_indexes IS NOT NULL THEN
            FOREACH v_idx_sql IN ARRAY v_firs_mv_indexes LOOP
                EXECUTE v_idx_sql;
            END LOOP;
        END IF;
    END IF;
END;
$$;

-- -----------------------------------------------------------------------------
-- 3) New API-truth normalized tables
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.chargesheet_media (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    chargesheet_id character varying(50) NOT NULL,
    media_index integer NOT NULL DEFAULT 0,
    file_id character varying(100),
    media_payload jsonb,
    created_at timestamptz,
    date_modified timestamptz,
    CONSTRAINT chargesheet_media_pkey PRIMARY KEY (id),
    CONSTRAINT chargesheet_media_unique_entry UNIQUE (chargesheet_id, media_index)
);

COMMENT ON TABLE public.chargesheet_media IS
    'Normalized media references for chargesheets. One row per uploadChargeSheet item.';
COMMENT ON COLUMN public.chargesheet_media.chargesheet_id IS
    'API chargeSheetId used as the logical parent key.';
COMMENT ON COLUMN public.chargesheet_media.file_id IS
    'uploadChargeSheet.fileId from the API payload.';

CREATE INDEX IF NOT EXISTS idx_chargesheet_media_chargesheet_id
    ON public.chargesheet_media USING btree (chargesheet_id);

CREATE INDEX IF NOT EXISTS idx_chargesheet_media_file_id
    ON public.chargesheet_media USING btree (file_id);

CREATE TABLE IF NOT EXISTS public.chargesheet_acts_sections (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    chargesheet_id character varying(50) NOT NULL,
    act_index integer NOT NULL DEFAULT 0,
    section_index integer NOT NULL DEFAULT 0,
    act_description text,
    section text,
    rw_required boolean DEFAULT false,
    section_description text,
    grave_particulars text,
    created_at timestamptz,
    date_modified timestamptz,
    CONSTRAINT chargesheet_acts_sections_pkey PRIMARY KEY (id),
    CONSTRAINT chargesheet_acts_sections_unique_entry UNIQUE (chargesheet_id, act_index, section_index)
);

COMMENT ON TABLE public.chargesheet_acts_sections IS
    'Normalized sections for chargesheets. One row per section entry extracted from actsAndSections[].';
COMMENT ON COLUMN public.chargesheet_acts_sections.chargesheet_id IS
    'API chargeSheetId used as the logical parent key.';

CREATE INDEX IF NOT EXISTS idx_chargesheet_acts_sections_chargesheet_id
    ON public.chargesheet_acts_sections USING btree (chargesheet_id);

CREATE INDEX IF NOT EXISTS idx_chargesheet_acts_sections_section
    ON public.chargesheet_acts_sections USING btree (section);

-- -----------------------------------------------------------------------------
-- 4) Backfill API-truth tables from legacy data when the API key is available
-- -----------------------------------------------------------------------------
INSERT INTO public.chargesheet_media (
    id,
    chargesheet_id,
    media_index,
    file_id,
    media_payload,
    created_at,
    date_modified
)
SELECT
    public.uuid_generate_v4(),
    cs.charge_sheet_id,
    ROW_NUMBER() OVER (PARTITION BY cs.charge_sheet_id ORDER BY cf.created_at, cf.id) - 1,
    NULLIF(BTRIM(cf.file_id), ''),
    jsonb_build_object(
        'fileId', NULLIF(BTRIM(cf.file_id), ''),
        'legacyChargesheetId', cf.chargesheet_id,
        'legacyFileId', cf.file_id
    ),
    cf.created_at,
    cf.created_at
FROM public.chargesheet_files cf
JOIN public.chargesheets cs
  ON cs.id = cf.chargesheet_id
WHERE cs.charge_sheet_id IS NOT NULL
ON CONFLICT (chargesheet_id, media_index) DO UPDATE
SET file_id = EXCLUDED.file_id,
    media_payload = EXCLUDED.media_payload,
    created_at = EXCLUDED.created_at,
    date_modified = EXCLUDED.date_modified;

INSERT INTO public.chargesheet_acts_sections (
    id,
    chargesheet_id,
    act_index,
    section_index,
    act_description,
    section,
    rw_required,
    section_description,
    grave_particulars,
    created_at,
    date_modified
)
SELECT
    public.uuid_generate_v4(),
    cs.charge_sheet_id,
    ROW_NUMBER() OVER (PARTITION BY cs.charge_sheet_id ORDER BY ca.created_at, ca.id) - 1,
    0,
    ca.act_description,
    NULLIF(BTRIM(ca.section), ''),
    ca.rw_required,
    NULLIF(BTRIM(ca.section_description), ''),
    NULLIF(BTRIM(ca.grave_particulars), ''),
    ca.created_at,
    ca.created_at
FROM public.chargesheet_acts ca
JOIN public.chargesheets cs
  ON cs.id = ca.chargesheet_id
WHERE cs.charge_sheet_id IS NOT NULL
ON CONFLICT (chargesheet_id, act_index, section_index) DO UPDATE
SET act_description = EXCLUDED.act_description,
    section = EXCLUDED.section,
    rw_required = EXCLUDED.rw_required,
    section_description = EXCLUDED.section_description,
    grave_particulars = EXCLUDED.grave_particulars,
    created_at = EXCLUDED.created_at,
    date_modified = EXCLUDED.date_modified;

COMMIT;
