-- =====================================================================
-- etl-address :: bootstrap migration (001)
-- Run ONCE via pgAdmin (or psql) on the target DB BEFORE master_etl.
-- Idempotent: safe to re-run.
--
-- Purpose:
--   1. Ensure pg_trgm + unaccent extensions.
--   2. Create trgm GIN indexes on geo_reference + geo_countries (fast fuzzy).
--   3. Create etl_checkpoint + etl_address_failures tables.
--   4. Create partial index on persons for the pending predicate (fast scan).
--   5. One-shot backfill: fill permanent_country / present_country = 'India'
--      for rows whose state is a known Indian state but country is empty.
--      Kills the infinite-pending loop left by the legacy ETLs.
--
-- NOTE: statements run individually; no outer transaction so each step
-- commits on its own. If one fails, earlier ones stay applied (all
-- guarded by IF NOT EXISTS / IS DISTINCT FROM so re-run is safe).
-- =====================================================================


-- ---------------------------------------------------------------------
-- 1. Extensions
-- ---------------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS unaccent;


-- ---------------------------------------------------------------------
-- 2. Trigram GIN indexes for KB fuzzy matching
--    (matches the lookups performed by resolver/kb_resolver.py)
-- ---------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_geo_reference_state_trgm
    ON geo_reference USING gin (lower(state_name) gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_geo_reference_district_trgm
    ON geo_reference USING gin (lower(district_name) gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_geo_reference_subdistrict_trgm
    ON geo_reference USING gin (lower(sub_district_name) gin_trgm_ops)
    WHERE sub_district_name IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_geo_reference_village_trgm
    ON geo_reference USING gin (lower(village_name_english) gin_trgm_ops)
    WHERE village_name_english IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_geo_countries_country_trgm
    ON geo_countries USING gin (lower(country_name) gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_geo_countries_state_trgm
    ON geo_countries USING gin (lower(state_name) gin_trgm_ops)
    WHERE state_name IS NOT NULL;

-- Exact-match helpers used by kb_resolver parent-bounded queries
CREATE INDEX IF NOT EXISTS idx_geo_reference_state_lower
    ON geo_reference (lower(state_name));

CREATE INDEX IF NOT EXISTS idx_geo_reference_state_district_lower
    ON geo_reference (lower(state_name), lower(district_name));


-- ---------------------------------------------------------------------
-- 3. etl_checkpoint -- resumable keyset pagination
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS etl_checkpoint (
    etl_name      TEXT PRIMARY KEY,
    last_seen_id  TEXT,
    run_id        TEXT,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);


-- ---------------------------------------------------------------------
-- 4. etl_address_failures -- quarantine + audit (no silent drops)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS etl_address_failures (
    person_id  TEXT PRIMARY KEY,
    reason     TEXT NOT NULL,
    details    JSONB,
    attempted  INT NOT NULL DEFAULT 1,
    last_try   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_etl_address_failures_reason
    ON etl_address_failures (reason);

CREATE INDEX IF NOT EXISTS idx_etl_address_failures_attempted
    ON etl_address_failures (attempted);


-- ---------------------------------------------------------------------
-- 5. Partial index on persons for pending predicate
--    Tuned to the exact predicate in etl-address/io_layer/reader.py
-- ---------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_persons_address_pending
    ON persons (person_id)
    WHERE (
          TRIM(COALESCE(permanent_state_ut,''))           <> ''
       OR TRIM(COALESCE(present_state_ut,''))             <> ''
       OR TRIM(COALESCE(permanent_district,''))           <> ''
       OR TRIM(COALESCE(present_district,''))             <> ''
       OR TRIM(COALESCE(permanent_area_mandal,''))        <> ''
       OR TRIM(COALESCE(present_area_mandal,''))          <> ''
       OR TRIM(COALESCE(permanent_locality_village,''))   <> ''
       OR TRIM(COALESCE(present_locality_village,''))     <> ''
       OR TRIM(COALESCE(permanent_landmark_milestone,'')) <> ''
       OR TRIM(COALESCE(present_landmark_milestone,''))   <> ''
       OR TRIM(COALESCE(permanent_ward_colony,''))        <> ''
       OR TRIM(COALESCE(present_ward_colony,''))          <> ''
       OR TRIM(COALESCE(permanent_street_road_no,''))     <> ''
       OR TRIM(COALESCE(present_street_road_no,''))       <> ''
       OR TRIM(COALESCE(permanent_pin_code,''))           <> ''
       OR TRIM(COALESCE(present_pin_code,''))             <> ''
       OR (
             TRIM(COALESCE(nationality,'')) <> ''
         AND (
                TRIM(COALESCE(permanent_country,'')) = ''
             OR TRIM(COALESCE(present_country,''))   = ''
         )
       )
    )
    AND NOT (
           TRIM(COALESCE(permanent_country,''))   <> ''
       AND TRIM(COALESCE(permanent_state_ut,''))  <> ''
       AND TRIM(COALESCE(permanent_district,''))  <> ''
         AND TRIM(COALESCE(present_country,''))     <> ''
         AND TRIM(COALESCE(present_state_ut,''))    <> ''
         AND TRIM(COALESCE(present_district,''))    <> ''
    );


-- ---------------------------------------------------------------------
-- 6. One-shot backfill: fill country='India' when state is an Indian
--    state-name in geo_reference. This clears the pending-loop residue
--    produced by the legacy update-state-country.py that wrote state+
--    district but never populated country.
-- ---------------------------------------------------------------------
UPDATE persons p
   SET permanent_country = 'India'
 WHERE TRIM(COALESCE(p.permanent_country,'')) = ''
   AND TRIM(COALESCE(p.permanent_state_ut,'')) <> ''
   AND EXISTS (
        SELECT 1
          FROM geo_reference gr
         WHERE lower(TRIM(gr.state_name)) = lower(TRIM(p.permanent_state_ut))
   );

UPDATE persons p
   SET present_country = 'India'
 WHERE TRIM(COALESCE(p.present_country,'')) = ''
   AND TRIM(COALESCE(p.present_state_ut,'')) <> ''
   AND EXISTS (
        SELECT 1
          FROM geo_reference gr
         WHERE lower(TRIM(gr.state_name)) = lower(TRIM(p.present_state_ut))
   );


-- ---------------------------------------------------------------------
-- 7. ANALYZE so planner picks up new indexes
-- ---------------------------------------------------------------------
ANALYZE geo_reference;
ANALYZE geo_countries;
ANALYZE persons;
ANALYZE etl_checkpoint;
ANALYZE etl_address_failures;


-- ---------------------------------------------------------------------
-- 8. Verification (read-only; safe to keep in paste)
-- ---------------------------------------------------------------------
SELECT 'pg_trgm'   AS ext, extversion FROM pg_extension WHERE extname='pg_trgm'
UNION ALL
SELECT 'unaccent', extversion FROM pg_extension WHERE extname='unaccent';

SELECT indexname
  FROM pg_indexes
 WHERE indexname IN (
        'idx_geo_reference_state_trgm',
        'idx_geo_reference_district_trgm',
        'idx_geo_reference_subdistrict_trgm',
        'idx_geo_reference_village_trgm',
        'idx_geo_countries_country_trgm',
        'idx_geo_countries_state_trgm',
        'idx_geo_reference_state_lower',
        'idx_geo_reference_state_district_lower',
        'idx_persons_address_pending',
        'idx_etl_address_failures_reason',
        'idx_etl_address_failures_attempted'
 )
 ORDER BY indexname;

SELECT to_regclass('public.etl_checkpoint')        AS etl_checkpoint,
       to_regclass('public.etl_address_failures')  AS etl_address_failures;

SELECT COUNT(*) AS pending_rows
  FROM persons
 WHERE (
          TRIM(COALESCE(permanent_state_ut,''))           <> ''
       OR TRIM(COALESCE(present_state_ut,''))             <> ''
       OR TRIM(COALESCE(permanent_district,''))           <> ''
       OR TRIM(COALESCE(present_district,''))             <> ''
       OR TRIM(COALESCE(permanent_area_mandal,''))        <> ''
       OR TRIM(COALESCE(present_area_mandal,''))          <> ''
       OR TRIM(COALESCE(permanent_locality_village,''))   <> ''
       OR TRIM(COALESCE(present_locality_village,''))     <> ''
       OR TRIM(COALESCE(permanent_landmark_milestone,'')) <> ''
       OR TRIM(COALESCE(present_landmark_milestone,''))   <> ''
             OR TRIM(COALESCE(permanent_ward_colony,''))        <> ''
             OR TRIM(COALESCE(present_ward_colony,''))          <> ''
             OR TRIM(COALESCE(permanent_street_road_no,''))     <> ''
             OR TRIM(COALESCE(present_street_road_no,''))       <> ''
             OR TRIM(COALESCE(permanent_pin_code,''))           <> ''
             OR TRIM(COALESCE(present_pin_code,''))             <> ''
             OR (
                         TRIM(COALESCE(nationality,'')) <> ''
                 AND (
                                TRIM(COALESCE(permanent_country,'')) = ''
                         OR TRIM(COALESCE(present_country,''))   = ''
                 )
             )
   )
   AND NOT (
           TRIM(COALESCE(permanent_country,''))   <> ''
       AND TRIM(COALESCE(permanent_state_ut,''))  <> ''
       AND TRIM(COALESCE(permanent_district,''))  <> ''
       AND TRIM(COALESCE(present_country,''))     <> ''
       AND TRIM(COALESCE(present_state_ut,''))    <> ''
       AND TRIM(COALESCE(present_district,''))    <> ''
   );

-- =====================================================================
-- End of migration 001. After success:
--   1. Run master_etl.py (Order 7 = etl-address).
--   2. Verify pending_rows shrinks each run until stable.
--   3. Then delete legacy dirs:
--        rm -rf /data-drive/etl-process-dev/update-mandal
--        rm -rf /data-drive/etl-process-dev/update-state-country
-- =====================================================================
