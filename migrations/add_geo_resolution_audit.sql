-- Migration: add geo resolution audit columns to persons
-- Tracks how addresses were standardized (pg_trgm, alias, embedding, llm)
-- and the confidence score of the match.
--
-- Run once on target database. Safe to rerun — uses IF NOT EXISTS.

ALTER TABLE persons
    ADD COLUMN IF NOT EXISTS geo_resolution_source TEXT;

ALTER TABLE persons
    ADD COLUMN IF NOT EXISTS geo_resolution_confidence REAL;

-- Optional index for audit queries filtering by source
CREATE INDEX IF NOT EXISTS idx_persons_geo_resolution_source
    ON persons (geo_resolution_source)
    WHERE geo_resolution_source IS NOT NULL;

-- Verification query
-- SELECT column_name, data_type
-- FROM information_schema.columns
-- WHERE table_name = 'persons'
--   AND column_name IN ('geo_resolution_source', 'geo_resolution_confidence');

-- Audit query after running ETL:
-- SELECT geo_resolution_source, COUNT(*), AVG(geo_resolution_confidence)
-- FROM persons
-- WHERE geo_resolution_source IS NOT NULL
-- GROUP BY geo_resolution_source;

-- Flag LLM/embedding low-confidence rows for manual review:
-- SELECT person_id, permanent_state_ut, permanent_country,
--        geo_resolution_source, geo_resolution_confidence
-- FROM persons
-- WHERE geo_resolution_source IN ('embedding', 'llm')
--   AND geo_resolution_confidence < 0.80
-- ORDER BY geo_resolution_confidence ASC
-- LIMIT 100;
