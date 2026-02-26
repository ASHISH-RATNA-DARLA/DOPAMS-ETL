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

-- ============================================================
-- Fix: Ganja incorrectly classified as liquid (shows "26 L" in dashboard)
-- Root cause: Cannabis variants with volume units were renamed to "Ganja",
--             and/or LLM returned litre units for solid ganja records.
-- ============================================================

-- Step 4: For existing Ganja rows that have standardized_volume_ml set
--         (and NO weight) — these are misclassified. 
--         Two sub-cases:
--           a) drug_name is a liquid variant (Cannabis Oil, Resin, etc.) — 
--              they were wrongly renamed to "Ganja". Restore primary_drug_name to NULL
--              so standardization re-evaluates them.
--           b) drug_name is plain "Ganja" but unit was "l"/"ltr" (LLM error) —
--              move the value to weight (treat liters as kg approximation is wrong,
--              so NULL it out and flag for re-extraction).

-- Audit first (run this SELECT to review before updating):
-- SELECT id, crime_id, drug_name, quantity_numeric, quantity_unit,
--        standardized_weight_kg, standardized_volume_ml, primary_drug_name
-- FROM public.brief_facts_drug
-- WHERE primary_drug_name = 'Ganja'
--   AND standardized_volume_ml IS NOT NULL
--   AND standardized_volume_ml > 0;

-- Fix 4a: NULL out primary_drug_name for Ganja rows with volume 
--         so they are excluded from the "Ganja" aggregation in views.
UPDATE public.brief_facts_drug
SET primary_drug_name = NULL
WHERE primary_drug_name = 'Ganja'
  AND standardized_volume_ml IS NOT NULL
  AND standardized_volume_ml > 0
  AND (standardized_weight_kg IS NULL OR standardized_weight_kg = 0);

-- Fix 4b: Also NULL out the volume value itself so it doesn't pollute 
--         the "26 L" total shown in the dashboard.
UPDATE public.brief_facts_drug
SET standardized_volume_ml = NULL,
    primary_unit_type = NULL
WHERE drug_name = 'Ganja'
  AND standardized_volume_ml IS NOT NULL
  AND standardized_volume_ml > 0
  AND (standardized_weight_kg IS NULL OR standardized_weight_kg = 0);

