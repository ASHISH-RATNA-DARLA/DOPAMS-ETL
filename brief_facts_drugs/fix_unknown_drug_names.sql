-- ============================================================
-- Fix: Remove / suppress "Unknown" drug name records
-- Run once on production after deploying the code fixes.
-- ============================================================

-- Step 1: Add "unknown" variants to the drug_ignore_list so that
--         drug_standardization.py sets primary_drug_name = NULL for them.
INSERT INTO public.drug_ignore_list (term)
VALUES
    ('unknown'),
    ('unidentified'),
    ('unknown drug'),
    ('unknown substance'),
    ('unknown tablet'),
    ('unknown powder'),
    ('unknown liquid'),
    ('unknown material')
ON CONFLICT (term) DO NOTHING;

-- Step 2: Null out primary_drug_name for already-existing "Unknown" rows
--         (the standardization pipeline will have set it to 'Unknown' raw value).
UPDATE public.brief_facts_drug
SET primary_drug_name = NULL
WHERE LOWER(TRIM(drug_name)) IN (
    'unknown', 'unidentified', 'unknown drug', 'unknown substance',
    'unknown tablet', 'unknown powder', 'unknown liquid', 'unknown material'
)
   OR LOWER(TRIM(primary_drug_name)) IN (
    'unknown', 'unidentified', 'unknown drug', 'unknown substance',
    'unknown tablet', 'unknown powder', 'unknown liquid', 'unknown material'
);

-- Step 3 (optional): Hard-delete the rows entirely if you don't want them at all.
-- WARNING: Only run this if you are sure — it removes the DB rows permanently.
-- These rows were written with confidence ≥ 90 but had a bad drug_name.
-- Uncomment to apply:
--
-- DELETE FROM public.brief_facts_drug
-- WHERE LOWER(TRIM(drug_name)) IN (
--     'unknown', 'unidentified', 'unknown drug', 'unknown substance',
--     'unknown tablet', 'unknown powder', 'unknown liquid', 'unknown material'
-- );
