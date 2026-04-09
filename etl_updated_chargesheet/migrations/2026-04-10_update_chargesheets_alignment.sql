-- MODULE: Update-Chargesheets
-- Migration: API / ETL / DB alignment with null-safe overwrite semantics
-- Safety: idempotent, production-safe, non-destructive

BEGIN;

-- -----------------------------------------------------------------------------
-- 1) Core table alignment
-- -----------------------------------------------------------------------------
ALTER TABLE public.charge_sheet_updates
    ADD COLUMN IF NOT EXISTS date_modified timestamptz;

COMMENT ON COLUMN public.charge_sheet_updates.date_modified IS
    'Timestamp when the update record was last modified in the API system.';

CREATE INDEX IF NOT EXISTS idx_charge_sheet_updates_date_modified
    ON public.charge_sheet_updates USING btree (date_modified);

-- Normalize blanks to NULL for nullable text fields.
UPDATE public.charge_sheet_updates
   SET update_charge_sheet_id = NULLIF(BTRIM(update_charge_sheet_id), ''),
       charge_sheet_no = NULLIF(BTRIM(charge_sheet_no), ''),
       charge_sheet_status = NULLIF(BTRIM(charge_sheet_status), ''),
       taken_on_file_case_type = NULLIF(BTRIM(taken_on_file_case_type), ''),
       taken_on_file_court_case_no = NULLIF(BTRIM(taken_on_file_court_case_no), '')
 WHERE update_charge_sheet_id = ''
    OR charge_sheet_no = ''
    OR charge_sheet_status = ''
    OR taken_on_file_case_type = ''
    OR taken_on_file_court_case_no = '';

-- Keep the API identifier as the overwrite key.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'charge_sheet_updates_update_charge_sheet_id_key'
          AND conrelid = 'public.charge_sheet_updates'::regclass
    ) THEN
        -- Existing unique constraint already covers overwrite semantics.
        NULL;
    ELSE
        ALTER TABLE ONLY public.charge_sheet_updates
            ADD CONSTRAINT charge_sheet_updates_update_charge_sheet_id_key UNIQUE (update_charge_sheet_id);
    END IF;
END;
$$;

-- -----------------------------------------------------------------------------
-- 2) Backward-compatible alias for consumer code that expects the module name
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW public.update_chargesheets AS
SELECT
    id,
    update_charge_sheet_id,
    crime_id,
    charge_sheet_no,
    charge_sheet_date,
    charge_sheet_status,
    taken_on_file_date,
    taken_on_file_case_type,
    taken_on_file_court_case_no,
    date_created,
    date_modified
FROM public.charge_sheet_updates;

COMMENT ON VIEW public.update_chargesheets IS
    'API-facing alias for charge_sheet_updates. Read-only compatibility layer.';

COMMIT;
