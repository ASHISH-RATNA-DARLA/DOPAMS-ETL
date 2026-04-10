-- Safe schema extension for PERSON gender enrichment metadata.
-- Non-destructive: adds columns only if they do not already exist.

ALTER TABLE public.persons
    ADD COLUMN IF NOT EXISTS gender_confidence NUMERIC(4,3),
    ADD COLUMN IF NOT EXISTS gender_source VARCHAR(20);

-- Optional follow-up (run only after cleanup/backfill):
-- ALTER TABLE public.persons
--     ADD CONSTRAINT persons_gender_check
--     CHECK (gender IN ('Male', 'Female', 'Transgender', 'Unknown'));
