-- =====================================================================================
-- IR ETL SCHEMA FIX - Convert UUID columns back to VARCHAR(50)
-- =====================================================================================
-- Issue: Mixed UUID and VARCHAR column types causing insert failures
-- Solution: Standardize all person_id and interrogation_report_id to VARCHAR(50)
-- This allows storing non-UUID string values from the API
-- =====================================================================================

BEGIN TRANSACTION;

-- Drop and recreate tables with inconsistent UUID types
-- These tables have UUID columns that should be VARCHAR(50)

-- 1. ir_defence_counsel - Convert UUID to VARCHAR
ALTER TABLE ir_defence_counsel DROP CONSTRAINT IF EXISTS ir_defence_counsel_interrogation_report_id_fkey;
ALTER TABLE ir_defence_counsel ALTER COLUMN interrogation_report_id TYPE VARCHAR(50);
ALTER TABLE ir_defence_counsel ALTER COLUMN defence_counsel_person_id TYPE VARCHAR(50);
ALTER TABLE ir_defence_counsel
  ADD CONSTRAINT ir_defence_counsel_interrogation_report_id_fkey
  FOREIGN KEY (interrogation_report_id) REFERENCES interrogation_reports(interrogation_report_id) ON DELETE CASCADE;

-- 2. ir_conviction_acquittal - Convert UUID to VARCHAR
ALTER TABLE ir_conviction_acquittal DROP CONSTRAINT IF EXISTS ir_conviction_acquittal_interrogation_report_id_fkey;
ALTER TABLE ir_conviction_acquittal ALTER COLUMN interrogation_report_id TYPE VARCHAR(50);
ALTER TABLE ir_conviction_acquittal
  ADD CONSTRAINT ir_conviction_acquittal_interrogation_report_id_fkey
  FOREIGN KEY (interrogation_report_id) REFERENCES interrogation_reports(interrogation_report_id) ON DELETE CASCADE;

-- 3. ir_execution_of_nbw - Convert UUID to VARCHAR
ALTER TABLE ir_execution_of_nbw DROP CONSTRAINT IF EXISTS ir_execution_of_nbw_interrogation_report_id_fkey;
ALTER TABLE ir_execution_of_nbw ALTER COLUMN interrogation_report_id TYPE VARCHAR(50);
ALTER TABLE ir_execution_of_nbw
  ADD CONSTRAINT ir_execution_of_nbw_interrogation_report_id_fkey
  FOREIGN KEY (interrogation_report_id) REFERENCES interrogation_reports(interrogation_report_id) ON DELETE CASCADE;

-- 4. ir_jail_sentence - Convert UUID to VARCHAR
ALTER TABLE ir_jail_sentence DROP CONSTRAINT IF EXISTS ir_jail_sentence_interrogation_report_id_fkey;
ALTER TABLE ir_jail_sentence ALTER COLUMN interrogation_report_id TYPE VARCHAR(50);
ALTER TABLE ir_jail_sentence
  ADD CONSTRAINT ir_jail_sentence_interrogation_report_id_fkey
  FOREIGN KEY (interrogation_report_id) REFERENCES interrogation_reports(interrogation_report_id) ON DELETE CASCADE;

-- 5. ir_new_gang_formation - Convert UUID to VARCHAR
ALTER TABLE ir_new_gang_formation DROP CONSTRAINT IF EXISTS ir_new_gang_formation_interrogation_report_id_fkey;
ALTER TABLE ir_new_gang_formation ALTER COLUMN interrogation_report_id TYPE VARCHAR(50);
ALTER TABLE ir_new_gang_formation ALTER COLUMN leader_person_id TYPE VARCHAR(50);
ALTER TABLE ir_new_gang_formation
  ADD CONSTRAINT ir_new_gang_formation_interrogation_report_id_fkey
  FOREIGN KEY (interrogation_report_id) REFERENCES interrogation_reports(interrogation_report_id) ON DELETE CASCADE;

-- 6. ir_pending_nbw - Convert UUID to VARCHAR
ALTER TABLE ir_pending_nbw DROP CONSTRAINT IF EXISTS ir_pending_nbw_interrogation_report_id_fkey;
ALTER TABLE ir_pending_nbw ALTER COLUMN interrogation_report_id TYPE VARCHAR(50);
ALTER TABLE ir_pending_nbw
  ADD CONSTRAINT ir_pending_nbw_interrogation_report_id_fkey
  FOREIGN KEY (interrogation_report_id) REFERENCES interrogation_reports(interrogation_report_id) ON DELETE CASCADE;

-- 7. ir_property_disposal - Convert UUID to VARCHAR
ALTER TABLE ir_property_disposal DROP CONSTRAINT IF EXISTS ir_property_disposal_interrogation_report_id_fkey;
ALTER TABLE ir_property_disposal ALTER COLUMN interrogation_report_id TYPE VARCHAR(50);
ALTER TABLE ir_property_disposal
  ADD CONSTRAINT ir_property_disposal_interrogation_report_id_fkey
  FOREIGN KEY (interrogation_report_id) REFERENCES interrogation_reports(interrogation_report_id) ON DELETE CASCADE;

-- 8. ir_regularization_transit_warrants - Convert UUID to VARCHAR
ALTER TABLE ir_regularization_transit_warrants DROP CONSTRAINT IF EXISTS ir_regularization_transit_warrants_interrogation_report_id_fkey;
ALTER TABLE ir_regularization_transit_warrants ALTER COLUMN interrogation_report_id TYPE VARCHAR(50);
ALTER TABLE ir_regularization_transit_warrants
  ADD CONSTRAINT ir_regularization_transit_warrants_interrogation_report_id_fkey
  FOREIGN KEY (interrogation_report_id) REFERENCES interrogation_reports(interrogation_report_id) ON DELETE CASCADE;

-- 9. ir_sureties - Convert UUID to VARCHAR
ALTER TABLE ir_sureties DROP CONSTRAINT IF EXISTS ir_sureties_interrogation_report_id_fkey;
ALTER TABLE ir_sureties ALTER COLUMN interrogation_report_id TYPE VARCHAR(50);
ALTER TABLE ir_sureties ALTER COLUMN surety_person_id TYPE VARCHAR(50);
ALTER TABLE ir_sureties
  ADD CONSTRAINT ir_sureties_interrogation_report_id_fkey
  FOREIGN KEY (interrogation_report_id) REFERENCES interrogation_reports(interrogation_report_id) ON DELETE CASCADE;

-- Verify all person_id columns are VARCHAR(50)
ALTER TABLE interrogation_reports ALTER COLUMN person_id TYPE VARCHAR(50) USING person_id::VARCHAR(50);

COMMIT;

-- Verify fix
SELECT table_name, column_name, data_type
FROM information_schema.columns
WHERE table_name LIKE 'ir_%' AND (column_name LIKE '%person_id%' OR column_name LIKE '%interrogation_report_id%')
ORDER BY table_name, column_name;
