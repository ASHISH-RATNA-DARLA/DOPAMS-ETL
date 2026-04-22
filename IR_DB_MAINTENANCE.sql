-- ===================================================================
-- IR ETL DATABASE MAINTENANCE & VALIDATION SCRIPT
-- ===================================================================
-- This script performs comprehensive checks and fixes for IR tables
-- Run this in PgAdmin to verify and fix database issues

-- ===================================================================
-- SECTION 1: VERIFY SEQUENCE SYNCHRONIZATION
-- ===================================================================
-- Check if sequences are synchronized with actual table data

\echo '==== SECTION 1: SEQUENCE SYNCHRONIZATION CHECK ===='

-- Family History
SELECT 'ir_family_history' as table_name,
       (SELECT MAX(id) FROM public.ir_family_history) as max_id,
       (SELECT last_value FROM public.ir_family_history_id_seq) as seq_value,
       CASE
           WHEN (SELECT MAX(id) FROM public.ir_family_history) >= (SELECT last_value FROM public.ir_family_history_id_seq)
           THEN 'NEEDS SYNC'
           ELSE 'OK'
       END as status;

-- Local Contacts
SELECT 'ir_local_contacts' as table_name,
       (SELECT MAX(id) FROM public.ir_local_contacts) as max_id,
       (SELECT last_value FROM public.ir_local_contacts_id_seq) as seq_value,
       CASE
           WHEN (SELECT MAX(id) FROM public.ir_local_contacts) >= (SELECT last_value FROM public.ir_local_contacts_id_seq)
           THEN 'NEEDS SYNC'
           ELSE 'OK'
       END as status;

-- Regular Habits
SELECT 'ir_regular_habits' as table_name,
       (SELECT MAX(id) FROM public.ir_regular_habits) as max_id,
       (SELECT last_value FROM public.ir_regular_habits_id_seq) as seq_value,
       CASE
           WHEN (SELECT MAX(id) FROM public.ir_regular_habits) >= (SELECT last_value FROM public.ir_regular_habits_id_seq)
           THEN 'NEEDS SYNC'
           ELSE 'OK'
       END as status;

-- Types of Drugs
SELECT 'ir_types_of_drugs' as table_name,
       (SELECT MAX(id) FROM public.ir_types_of_drugs) as max_id,
       (SELECT last_value FROM public.ir_types_of_drugs_id_seq) as seq_value,
       CASE
           WHEN (SELECT MAX(id) FROM public.ir_types_of_drugs) >= (SELECT last_value FROM public.ir_types_of_drugs_id_seq)
           THEN 'NEEDS SYNC'
           ELSE 'OK'
       END as status;

-- SIM Details
SELECT 'ir_sim_details' as table_name,
       (SELECT MAX(id) FROM public.ir_sim_details) as max_id,
       (SELECT last_value FROM public.ir_sim_details_id_seq) as seq_value,
       CASE
           WHEN (SELECT MAX(id) FROM public.ir_sim_details) >= (SELECT last_value FROM public.ir_sim_details_id_seq)
           THEN 'NEEDS SYNC'
           ELSE 'OK'
       END as status;

-- Financial History
SELECT 'ir_financial_history' as table_name,
       (SELECT MAX(id) FROM public.ir_financial_history) as max_id,
       (SELECT last_value FROM public.ir_financial_history_id_seq) as seq_value,
       CASE
           WHEN (SELECT MAX(id) FROM public.ir_financial_history) >= (SELECT last_value FROM public.ir_financial_history_id_seq)
           THEN 'NEEDS SYNC'
           ELSE 'OK'
       END as status;

-- Consumer Details
SELECT 'ir_consumer_details' as table_name,
       (SELECT MAX(id) FROM public.ir_consumer_details) as max_id,
       (SELECT last_value FROM public.ir_consumer_details_id_seq) as seq_value,
       CASE
           WHEN (SELECT MAX(id) FROM public.ir_consumer_details) >= (SELECT last_value FROM public.ir_consumer_details_id_seq)
           THEN 'NEEDS SYNC'
           ELSE 'OK'
       END as status;

-- All other child tables in compact form
WITH seq_check AS (
  SELECT
    'ir_modus_operandi' as table_name,
    (SELECT MAX(id) FROM public.ir_modus_operandi) as max_id,
    (SELECT last_value FROM public.ir_modus_operandi_id_seq) as seq_value
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
       CASE WHEN max_id IS NULL THEN 'TABLE EMPTY'
            WHEN max_id >= seq_value THEN 'NEEDS SYNC'
            ELSE 'OK' END as status
FROM seq_check
ORDER BY table_name;

-- ===================================================================
-- SECTION 2: CHECK FOR DATA ANOMALIES
-- ===================================================================

\echo ''
\echo '==== SECTION 2: DATA ANOMALIES CHECK ===='

-- Check for records with missing interrogation_report_id
SELECT 'ir_family_history' as table_name, COUNT(*) as null_ir_count
FROM public.ir_family_history WHERE interrogation_report_id IS NULL
UNION ALL
SELECT 'ir_local_contacts', COUNT(*)
FROM public.ir_local_contacts WHERE interrogation_report_id IS NULL
UNION ALL
SELECT 'ir_types_of_drugs', COUNT(*)
FROM public.ir_types_of_drugs WHERE interrogation_report_id IS NULL
ORDER BY table_name;

-- ===================================================================
-- SECTION 3: RECORD COUNT BY TABLE
-- ===================================================================

\echo ''
\echo '==== SECTION 3: IR TABLE RECORD COUNTS ===='

SELECT 'interrogation_reports' as table_name, COUNT(*) as record_count FROM public.interrogation_reports
UNION ALL
SELECT 'ir_family_history', COUNT(*) FROM public.ir_family_history
UNION ALL
SELECT 'ir_local_contacts', COUNT(*) FROM public.ir_local_contacts
UNION ALL
SELECT 'ir_regular_habits', COUNT(*) FROM public.ir_regular_habits
UNION ALL
SELECT 'ir_types_of_drugs', COUNT(*) FROM public.ir_types_of_drugs
UNION ALL
SELECT 'ir_sim_details', COUNT(*) FROM public.ir_sim_details
UNION ALL
SELECT 'ir_financial_history', COUNT(*) FROM public.ir_financial_history
UNION ALL
SELECT 'ir_consumer_details', COUNT(*) FROM public.ir_consumer_details
UNION ALL
SELECT 'ir_modus_operandi', COUNT(*) FROM public.ir_modus_operandi
UNION ALL
SELECT 'ir_previous_offences_confessed', COUNT(*) FROM public.ir_previous_offences_confessed
UNION ALL
SELECT 'ir_defence_counsel', COUNT(*) FROM public.ir_defence_counsel
UNION ALL
SELECT 'ir_associate_details', COUNT(*) FROM public.ir_associate_details
UNION ALL
SELECT 'ir_shelter', COUNT(*) FROM public.ir_shelter
UNION ALL
SELECT 'ir_media', COUNT(*) FROM public.ir_media
UNION ALL
SELECT 'ir_interrogation_report_refs', COUNT(*) FROM public.ir_interrogation_report_refs
UNION ALL
SELECT 'ir_dopams_links', COUNT(*) FROM public.ir_dopams_links
UNION ALL
SELECT 'ir_indulgance_before_offence', COUNT(*) FROM public.ir_indulgance_before_offence
UNION ALL
SELECT 'ir_property_disposal', COUNT(*) FROM public.ir_property_disposal
UNION ALL
SELECT 'ir_regularization_transit_warrants', COUNT(*) FROM public.ir_regularization_transit_warrants
UNION ALL
SELECT 'ir_execution_of_nbw', COUNT(*) FROM public.ir_execution_of_nbw
UNION ALL
SELECT 'ir_pending_nbw', COUNT(*) FROM public.ir_pending_nbw
UNION ALL
SELECT 'ir_sureties', COUNT(*) FROM public.ir_sureties
UNION ALL
SELECT 'ir_jail_sentence', COUNT(*) FROM public.ir_jail_sentence
UNION ALL
SELECT 'ir_new_gang_formation', COUNT(*) FROM public.ir_new_gang_formation
UNION ALL
SELECT 'ir_conviction_acquittal', COUNT(*) FROM public.ir_conviction_acquittal
ORDER BY table_name;

-- ===================================================================
-- SECTION 4: SYNC SEQUENCES (RUN IF NEEDED - UNCOMMENT)
-- ===================================================================
-- If sequences are out of sync, uncomment and run these commands:

\echo ''
\echo '==== SECTION 4: SEQUENCE SYNC COMMANDS (if needed) ===='
\echo '-- Uncomment and run these if sequences are out of sync:'

-- SELECT setval('ir_family_history_id_seq', (SELECT COALESCE(MAX(id), 0) FROM public.ir_family_history));
-- SELECT setval('ir_local_contacts_id_seq', (SELECT COALESCE(MAX(id), 0) FROM public.ir_local_contacts));
-- SELECT setval('ir_regular_habits_id_seq', (SELECT COALESCE(MAX(id), 0) FROM public.ir_regular_habits));
-- SELECT setval('ir_types_of_drugs_id_seq', (SELECT COALESCE(MAX(id), 0) FROM public.ir_types_of_drugs));
-- SELECT setval('ir_sim_details_id_seq', (SELECT COALESCE(MAX(id), 0) FROM public.ir_sim_details));
-- SELECT setval('ir_financial_history_id_seq', (SELECT COALESCE(MAX(id), 0) FROM public.ir_financial_history));
-- SELECT setval('ir_consumer_details_id_seq', (SELECT COALESCE(MAX(id), 0) FROM public.ir_consumer_details));
-- SELECT setval('ir_modus_operandi_id_seq', (SELECT COALESCE(MAX(id), 0) FROM public.ir_modus_operandi));
-- SELECT setval('ir_previous_offences_confessed_id_seq', (SELECT COALESCE(MAX(id), 0) FROM public.ir_previous_offences_confessed));
-- SELECT setval('ir_defence_counsel_id_seq', (SELECT COALESCE(MAX(id), 0) FROM public.ir_defence_counsel));
-- SELECT setval('ir_associate_details_id_seq', (SELECT COALESCE(MAX(id), 0) FROM public.ir_associate_details));
-- SELECT setval('ir_shelter_id_seq', (SELECT COALESCE(MAX(id), 0) FROM public.ir_shelter));
-- SELECT setval('ir_media_id_seq', (SELECT COALESCE(MAX(id), 0) FROM public.ir_media));
-- SELECT setval('ir_interrogation_report_refs_id_seq', (SELECT COALESCE(MAX(id), 0) FROM public.ir_interrogation_report_refs));
-- SELECT setval('ir_dopams_links_id_seq', (SELECT COALESCE(MAX(id), 0) FROM public.ir_dopams_links));
-- SELECT setval('ir_indulgance_before_offence_id_seq', (SELECT COALESCE(MAX(id), 0) FROM public.ir_indulgance_before_offence));
-- SELECT setval('ir_property_disposal_id_seq', (SELECT COALESCE(MAX(id), 0) FROM public.ir_property_disposal));
-- SELECT setval('ir_regularization_transit_warrants_id_seq', (SELECT COALESCE(MAX(id), 0) FROM public.ir_regularization_transit_warrants));
-- SELECT setval('ir_execution_of_nbw_id_seq', (SELECT COALESCE(MAX(id), 0) FROM public.ir_execution_of_nbw));
-- SELECT setval('ir_pending_nbw_id_seq', (SELECT COALESCE(MAX(id), 0) FROM public.ir_pending_nbw));
-- SELECT setval('ir_sureties_id_seq', (SELECT COALESCE(MAX(id), 0) FROM public.ir_sureties));
-- SELECT setval('ir_jail_sentence_id_seq', (SELECT COALESCE(MAX(id), 0) FROM public.ir_jail_sentence));
-- SELECT setval('ir_new_gang_formation_id_seq', (SELECT COALESCE(MAX(id), 0) FROM public.ir_new_gang_formation));
-- SELECT setval('ir_conviction_acquittal_id_seq', (SELECT COALESCE(MAX(id), 0) FROM public.ir_conviction_acquittal));

-- ===================================================================
-- SECTION 5: AUTOMATED SEQUENCE SYNC (UNCOMMENT TO RUN)
-- ===================================================================
-- This single command will sync ALL sequences automatically

\echo ''
\echo '==== SECTION 5: AUTO-SYNC ALL SEQUENCES ===='
\echo '-- Uncomment below to auto-sync all IR table sequences:'

/*
DO $$
DECLARE
  tables RECORD;
BEGIN
  FOR tables IN
    SELECT tablename FROM pg_tables
    WHERE schemaname = 'public' AND tablename LIKE 'ir_%'
  LOOP
    EXECUTE 'SELECT setval(''' || tables.tablename || '_id_seq'', (SELECT COALESCE(MAX(id), 0) FROM public.' || tables.tablename || '))';
  END LOOP;
END $$;
*/

-- ===================================================================
-- SECTION 6: VERIFY INDEXES
-- ===================================================================

\echo ''
\echo '==== SECTION 6: VERIFY INDEXES ===='

SELECT schemaname, tablename, indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'public' AND tablename LIKE 'ir_%'
ORDER BY tablename, indexname;

\echo ''
\echo '==== DATABASE REVIEW COMPLETE ===='
