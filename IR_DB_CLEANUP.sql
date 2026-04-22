-- ===================================================================
-- IR ETL DATABASE CLEANUP & RECOVERY SCRIPT
-- ===================================================================
-- Run this script in PgAdmin if you need to fix sequence issues
-- BACKUP YOUR DATABASE FIRST before running these commands!

-- ===================================================================
-- STEP 1: CLEAR EXISTING INVALID DATA IF NEEDED
-- ===================================================================
-- Use these only if you need to clear corrupted data entries
-- This will DELETE all IR-related data (WARNING: DESTRUCTIVE)

-- DO NOT RUN unless absolutely necessary!
-- BACKUP DATABASE FIRST!

/*
-- Disable triggers and constraints temporarily
ALTER TABLE public.interrogation_reports DISABLE TRIGGER ALL;
ALTER TABLE public.ir_family_history DISABLE TRIGGER ALL;
ALTER TABLE public.ir_local_contacts DISABLE TRIGGER ALL;
-- ... repeat for all child tables

-- Clear data (DESTRUCTIVE!)
TRUNCATE TABLE public.ir_family_history CASCADE;
TRUNCATE TABLE public.ir_local_contacts CASCADE;
TRUNCATE TABLE public.ir_regular_habits CASCADE;
-- ... repeat for all child tables
TRUNCATE TABLE public.interrogation_reports CASCADE;

-- Re-enable triggers
ALTER TABLE public.interrogation_reports ENABLE TRIGGER ALL;
ALTER TABLE public.ir_family_history ENABLE TRIGGER ALL;
-- ... repeat for all child tables
*/

-- ===================================================================
-- STEP 2: RESET ALL SEQUENCES TO CURRENT MAX VALUES
-- ===================================================================
-- Safe way to sync all sequences with current data

\echo 'Syncing all IR table sequences...'

SELECT setval('ir_family_history_id_seq', (SELECT COALESCE(MAX(id), 1) + 1 FROM public.ir_family_history));
SELECT setval('ir_local_contacts_id_seq', (SELECT COALESCE(MAX(id), 1) + 1 FROM public.ir_local_contacts));
SELECT setval('ir_regular_habits_id_seq', (SELECT COALESCE(MAX(id), 1) + 1 FROM public.ir_regular_habits));
SELECT setval('ir_types_of_drugs_id_seq', (SELECT COALESCE(MAX(id), 1) + 1 FROM public.ir_types_of_drugs));
SELECT setval('ir_sim_details_id_seq', (SELECT COALESCE(MAX(id), 1) + 1 FROM public.ir_sim_details));
SELECT setval('ir_financial_history_id_seq', (SELECT COALESCE(MAX(id), 1) + 1 FROM public.ir_financial_history));
SELECT setval('ir_consumer_details_id_seq', (SELECT COALESCE(MAX(id), 1) + 1 FROM public.ir_consumer_details));
SELECT setval('ir_modus_operandi_id_seq', (SELECT COALESCE(MAX(id), 1) + 1 FROM public.ir_modus_operandi));
SELECT setval('ir_previous_offences_confessed_id_seq', (SELECT COALESCE(MAX(id), 1) + 1 FROM public.ir_previous_offences_confessed));
SELECT setval('ir_defence_counsel_id_seq', (SELECT COALESCE(MAX(id), 1) + 1 FROM public.ir_defence_counsel));
SELECT setval('ir_associate_details_id_seq', (SELECT COALESCE(MAX(id), 1) + 1 FROM public.ir_associate_details));
SELECT setval('ir_shelter_id_seq', (SELECT COALESCE(MAX(id), 1) + 1 FROM public.ir_shelter));
SELECT setval('ir_media_id_seq', (SELECT COALESCE(MAX(id), 1) + 1 FROM public.ir_media));
SELECT setval('ir_interrogation_report_refs_id_seq', (SELECT COALESCE(MAX(id), 1) + 1 FROM public.ir_interrogation_report_refs));
SELECT setval('ir_dopams_links_id_seq', (SELECT COALESCE(MAX(id), 1) + 1 FROM public.ir_dopams_links));
SELECT setval('ir_indulgance_before_offence_id_seq', (SELECT COALESCE(MAX(id), 1) + 1 FROM public.ir_indulgance_before_offence));
SELECT setval('ir_property_disposal_id_seq', (SELECT COALESCE(MAX(id), 1) + 1 FROM public.ir_property_disposal));
SELECT setval('ir_regularization_transit_warrants_id_seq', (SELECT COALESCE(MAX(id), 1) + 1 FROM public.ir_regularization_transit_warrants));
SELECT setval('ir_execution_of_nbw_id_seq', (SELECT COALESCE(MAX(id), 1) + 1 FROM public.ir_execution_of_nbw));
SELECT setval('ir_pending_nbw_id_seq', (SELECT COALESCE(MAX(id), 1) + 1 FROM public.ir_pending_nbw));
SELECT setval('ir_sureties_id_seq', (SELECT COALESCE(MAX(id), 1) + 1 FROM public.ir_sureties));
SELECT setval('ir_jail_sentence_id_seq', (SELECT COALESCE(MAX(id), 1) + 1 FROM public.ir_jail_sentence));
SELECT setval('ir_new_gang_formation_id_seq', (SELECT COALESCE(MAX(id), 1) + 1 FROM public.ir_new_gang_formation));
SELECT setval('ir_conviction_acquittal_id_seq', (SELECT COALESCE(MAX(id), 1) + 1 FROM public.ir_conviction_acquittal));

\echo 'All sequences synced successfully!'

-- ===================================================================
-- STEP 3: VERIFY SYNC WAS SUCCESSFUL
-- ===================================================================

\echo ''
\echo 'Verifying sequence sync...'

WITH seq_check AS (
  SELECT
    'ir_family_history' as table_name,
    (SELECT MAX(id) FROM public.ir_family_history) as max_id,
    (SELECT last_value FROM public.ir_family_history_id_seq) as seq_value
  UNION ALL
  SELECT 'ir_local_contacts',
    (SELECT MAX(id) FROM public.ir_local_contacts),
    (SELECT last_value FROM public.ir_local_contacts_id_seq)
  UNION ALL
  SELECT 'ir_regular_habits',
    (SELECT MAX(id) FROM public.ir_regular_habits),
    (SELECT last_value FROM public.ir_regular_habits_id_seq)
  UNION ALL
  SELECT 'ir_types_of_drugs',
    (SELECT MAX(id) FROM public.ir_types_of_drugs),
    (SELECT last_value FROM public.ir_types_of_drugs_id_seq)
  UNION ALL
  SELECT 'ir_sim_details',
    (SELECT MAX(id) FROM public.ir_sim_details),
    (SELECT last_value FROM public.ir_sim_details_id_seq)
  UNION ALL
  SELECT 'ir_financial_history',
    (SELECT MAX(id) FROM public.ir_financial_history),
    (SELECT last_value FROM public.ir_financial_history_id_seq)
  UNION ALL
  SELECT 'ir_consumer_details',
    (SELECT MAX(id) FROM public.ir_consumer_details),
    (SELECT last_value FROM public.ir_consumer_details_id_seq)
  UNION ALL
  SELECT 'ir_modus_operandi',
    (SELECT MAX(id) FROM public.ir_modus_operandi),
    (SELECT last_value FROM public.ir_modus_operandi_id_seq)
  UNION ALL
  SELECT 'ir_previous_offences_confessed',
    (SELECT MAX(id) FROM public.ir_previous_offences_confessed),
    (SELECT last_value FROM public.ir_previous_offences_confessed_id_seq)
  UNION ALL
  SELECT 'ir_defence_counsel',
    (SELECT MAX(id) FROM public.ir_defence_counsel),
    (SELECT last_value FROM public.ir_defence_counsel_id_seq)
  UNION ALL
  SELECT 'ir_associate_details',
    (SELECT MAX(id) FROM public.ir_associate_details),
    (SELECT last_value FROM public.ir_associate_details_id_seq)
  UNION ALL
  SELECT 'ir_shelter',
    (SELECT MAX(id) FROM public.ir_shelter),
    (SELECT last_value FROM public.ir_shelter_id_seq)
  UNION ALL
  SELECT 'ir_media',
    (SELECT MAX(id) FROM public.ir_media),
    (SELECT last_value FROM public.ir_media_id_seq)
  UNION ALL
  SELECT 'ir_interrogation_report_refs',
    (SELECT MAX(id) FROM public.ir_interrogation_report_refs),
    (SELECT last_value FROM public.ir_interrogation_report_refs_id_seq)
  UNION ALL
  SELECT 'ir_dopams_links',
    (SELECT MAX(id) FROM public.ir_dopams_links),
    (SELECT last_value FROM public.ir_dopams_links_id_seq)
  UNION ALL
  SELECT 'ir_indulgance_before_offence',
    (SELECT MAX(id) FROM public.ir_indulgance_before_offence),
    (SELECT last_value FROM public.ir_indulgance_before_offence_id_seq)
  UNION ALL
  SELECT 'ir_property_disposal',
    (SELECT MAX(id) FROM public.ir_property_disposal),
    (SELECT last_value FROM public.ir_property_disposal_id_seq)
  UNION ALL
  SELECT 'ir_regularization_transit_warrants',
    (SELECT MAX(id) FROM public.ir_regularization_transit_warrants),
    (SELECT last_value FROM public.ir_regularization_transit_warrants_id_seq)
  UNION ALL
  SELECT 'ir_execution_of_nbw',
    (SELECT MAX(id) FROM public.ir_execution_of_nbw),
    (SELECT last_value FROM public.ir_execution_of_nbw_id_seq)
  UNION ALL
  SELECT 'ir_pending_nbw',
    (SELECT MAX(id) FROM public.ir_pending_nbw),
    (SELECT last_value FROM public.ir_pending_nbw_id_seq)
  UNION ALL
  SELECT 'ir_sureties',
    (SELECT MAX(id) FROM public.ir_sureties),
    (SELECT last_value FROM public.ir_sureties_id_seq)
  UNION ALL
  SELECT 'ir_jail_sentence',
    (SELECT MAX(id) FROM public.ir_jail_sentence),
    (SELECT last_value FROM public.ir_jail_sentence_id_seq)
  UNION ALL
  SELECT 'ir_new_gang_formation',
    (SELECT MAX(id) FROM public.ir_new_gang_formation),
    (SELECT last_value FROM public.ir_new_gang_formation_id_seq)
  UNION ALL
  SELECT 'ir_conviction_acquittal',
    (SELECT MAX(id) FROM public.ir_conviction_acquittal),
    (SELECT last_value FROM public.ir_conviction_acquittal_id_seq)
)
SELECT table_name, max_id, seq_value,
       CASE WHEN max_id IS NULL OR max_id < seq_value THEN '✓ OK'
            ELSE '✗ MISMATCH' END as sync_status
FROM seq_check
ORDER BY table_name;

\echo ''
\echo '==== CLEANUP COMPLETE ===='
