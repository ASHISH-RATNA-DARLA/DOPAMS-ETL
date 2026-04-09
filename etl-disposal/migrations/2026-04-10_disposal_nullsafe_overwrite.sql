-- MODULE: Disposal
-- Migration: align disposal storage with API overwrite semantics
-- Safety: idempotent, non-destructive, fails fast if duplicate keys already exist

BEGIN;

-- Allow API nulls to overwrite existing values.
ALTER TABLE public.disposal
    ALTER COLUMN disposal_type DROP NOT NULL,
    ALTER COLUMN date_created DROP NOT NULL,
    ALTER COLUMN date_modified DROP NOT NULL;

-- Guardrail: make sure there are no duplicate logical keys before switching
-- the unique constraint to NULLS NOT DISTINCT.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM (
            SELECT crime_id, disposal_type, disposed_at, COUNT(*) AS row_count
            FROM public.disposal
            GROUP BY crime_id, disposal_type, disposed_at
            HAVING COUNT(*) > 1
        ) dup
    ) THEN
        RAISE EXCEPTION 'Duplicate disposal keys exist; clean them up before applying the NULL-safe unique constraint.';
    END IF;
END $$;

-- Recreate the logical unique key with NULL-safe semantics.
ALTER TABLE public.disposal
    DROP CONSTRAINT IF EXISTS disposal_crime_id_disposal_type_disposed_at_key;

ALTER TABLE public.disposal
    ADD CONSTRAINT disposal_crime_id_disposal_type_disposed_at_key
    UNIQUE NULLS NOT DISTINCT (crime_id, disposal_type, disposed_at);

-- Ensure the incremental ETL path can seek by modification timestamp.
CREATE INDEX IF NOT EXISTS idx_disposal_crime
    ON public.disposal USING btree (crime_id);

CREATE INDEX IF NOT EXISTS idx_disposal_date_modified
    ON public.disposal USING btree (date_modified);

COMMIT;
