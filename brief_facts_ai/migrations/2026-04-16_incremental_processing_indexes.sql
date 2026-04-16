-- Migration: Add indexes to support efficient incremental processing in brief_facts_ai
--            and fix etl_crime_processing_log status constraint to allow 'stale'.
-- Date: 2026-04-16
-- Purpose:
--   fetch_unprocessed_crimes_since() filters crimes by COALESCE(date_modified, date_created) >= cutoff.
--   Without an index, every daily run performs a full sequential scan of the crimes table.
--   These indexes allow the query planner to use index scans instead.
--
--   ALSO: the code sets status = 'stale' (in invalidate_branch_c_log_for_crimes) but the
--   existing CHECK constraint only allows in_progress | complete | failed.  This migration
--   expands the constraint to include 'stale'.

-- ---------------------------------------------------------------------------
-- 0. Fix status CHECK constraint to allow 'stale'
-- ---------------------------------------------------------------------------
-- Drop old constraint, recreate with 'stale' included.
ALTER TABLE public.etl_crime_processing_log
    DROP CONSTRAINT IF EXISTS etl_crime_processing_log_status_check;

ALTER TABLE public.etl_crime_processing_log
    ADD CONSTRAINT etl_crime_processing_log_status_check
    CHECK (status IN ('in_progress', 'complete', 'failed', 'stale'));

-- ---------------------------------------------------------------------------
-- 1. Index on crimes for date-range filtering (supports incremental mode)
-- ---------------------------------------------------------------------------
-- date_modified takes priority over date_created in COALESCE.
-- Index covers both columns so the planner can use a bitmap scan with either.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_crimes_date_modified_created
    ON public.crimes (date_modified DESC NULLS LAST, date_created DESC NULLS LAST);

-- A separate index on date_created alone helps queries that filter on it directly.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_crimes_date_created
    ON public.crimes (date_created DESC NULLS LAST);

-- ---------------------------------------------------------------------------
-- 2. Index on etl_crime_processing_log for LATERAL subquery performance
-- ---------------------------------------------------------------------------
-- The LATERAL subquery in fetch_unprocessed_crimes_since evaluates:
--   WHERE crime_id = c.crime_id AND status = 'complete'
-- The existing idx_etl_log_crime_status covers (crime_id, status).
-- Adding completed_at as an INCLUDE column lets the planner resolve MAX(completed_at)
-- from the index without a heap fetch (index-only scan).
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_etl_log_crime_status_completed
    ON public.etl_crime_processing_log (crime_id, status, completed_at DESC NULLS LAST);

-- ---------------------------------------------------------------------------
-- 3. Verify: after applying, run EXPLAIN ANALYZE on the query to confirm
--    index scans are used instead of sequential scans on crimes.
-- ---------------------------------------------------------------------------
-- EXPLAIN (ANALYZE, BUFFERS)
-- SELECT c.crime_id, COALESCE(c.date_modified, c.date_created) AS src_date
-- FROM crimes c
-- LEFT JOIN LATERAL (
--     SELECT MAX(l.completed_at) AS last_completed_at
--     FROM public.etl_crime_processing_log l
--     WHERE l.crime_id = c.crime_id AND l.status = 'complete'
-- ) last_run ON TRUE
-- WHERE COALESCE(c.date_modified, c.date_created) >= NOW() - INTERVAL '2 days'
--   AND (last_run.last_completed_at IS NULL
--        OR COALESCE(c.date_modified, c.date_created) > last_run.last_completed_at)
-- ORDER BY COALESCE(c.date_modified, c.date_created) DESC NULLS LAST
-- LIMIT 30;
