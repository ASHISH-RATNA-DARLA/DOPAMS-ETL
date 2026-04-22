-- ===================================================================
-- QUICK COMMANDS FOR PGADMIN - IR ETL DATABASE
-- ===================================================================
-- Copy & paste sections directly into PgAdmin Query Tool

-- ===================================================================
-- 1. CHECK ALL SEQUENCES - PASTE THIS INTO PGADMIN
-- ===================================================================

SELECT * FROM (
  SELECT 'ir_family_history', MAX(id), (SELECT last_value FROM ir_family_history_id_seq) FROM ir_family_history
  UNION ALL SELECT 'ir_local_contacts', MAX(id), (SELECT last_value FROM ir_local_contacts_id_seq) FROM ir_local_contacts
  UNION ALL SELECT 'ir_regular_habits', MAX(id), (SELECT last_value FROM ir_regular_habits_id_seq) FROM ir_regular_habits
  UNION ALL SELECT 'ir_types_of_drugs', MAX(id), (SELECT last_value FROM ir_types_of_drugs_id_seq) FROM ir_types_of_drugs
  UNION ALL SELECT 'ir_sim_details', MAX(id), (SELECT last_value FROM ir_sim_details_id_seq) FROM ir_sim_details
  UNION ALL SELECT 'ir_financial_history', MAX(id), (SELECT last_value FROM ir_financial_history_id_seq) FROM ir_financial_history
  UNION ALL SELECT 'ir_consumer_details', MAX(id), (SELECT last_value FROM ir_consumer_details_id_seq) FROM ir_consumer_details
  UNION ALL SELECT 'ir_modus_operandi', MAX(id), (SELECT last_value FROM ir_modus_operandi_id_seq) FROM ir_modus_operandi
  UNION ALL SELECT 'ir_previous_offences_confessed', MAX(id), (SELECT last_value FROM ir_previous_offences_confessed_id_seq) FROM ir_previous_offences_confessed
  UNION ALL SELECT 'ir_defence_counsel', MAX(id), (SELECT last_value FROM ir_defence_counsel_id_seq) FROM ir_defence_counsel
  UNION ALL SELECT 'ir_associate_details', MAX(id), (SELECT last_value FROM ir_associate_details_id_seq) FROM ir_associate_details
  UNION ALL SELECT 'ir_shelter', MAX(id), (SELECT last_value FROM ir_shelter_id_seq) FROM ir_shelter
  UNION ALL SELECT 'ir_media', MAX(id), (SELECT last_value FROM ir_media_id_seq) FROM ir_media
  UNION ALL SELECT 'ir_interrogation_report_refs', MAX(id), (SELECT last_value FROM ir_interrogation_report_refs_id_seq) FROM ir_interrogation_report_refs
  UNION ALL SELECT 'ir_dopams_links', MAX(id), (SELECT last_value FROM ir_dopams_links_id_seq) FROM ir_dopams_links
  UNION ALL SELECT 'ir_indulgance_before_offence', MAX(id), (SELECT last_value FROM ir_indulgance_before_offence_id_seq) FROM ir_indulgance_before_offence
  UNION ALL SELECT 'ir_property_disposal', MAX(id), (SELECT last_value FROM ir_property_disposal_id_seq) FROM ir_property_disposal
  UNION ALL SELECT 'ir_regularization_transit_warrants', MAX(id), (SELECT last_value FROM ir_regularization_transit_warrants_id_seq) FROM ir_regularization_transit_warrants
  UNION ALL SELECT 'ir_execution_of_nbw', MAX(id), (SELECT last_value FROM ir_execution_of_nbw_id_seq) FROM ir_execution_of_nbw
  UNION ALL SELECT 'ir_pending_nbw', MAX(id), (SELECT last_value FROM ir_pending_nbw_id_seq) FROM ir_pending_nbw
  UNION ALL SELECT 'ir_sureties', MAX(id), (SELECT last_value FROM ir_sureties_id_seq) FROM ir_sureties
  UNION ALL SELECT 'ir_jail_sentence', MAX(id), (SELECT last_value FROM ir_jail_sentence_id_seq) FROM ir_jail_sentence
  UNION ALL SELECT 'ir_new_gang_formation', MAX(id), (SELECT last_value FROM ir_new_gang_formation_id_seq) FROM ir_new_gang_formation
  UNION ALL SELECT 'ir_conviction_acquittal', MAX(id), (SELECT last_value FROM ir_conviction_acquittal_id_seq) FROM ir_conviction_acquittal
) t(table_name, max_id, seq_value)
ORDER BY table_name;


-- ===================================================================
-- 2. COUNT RECORDS IN ALL TABLES - PASTE THIS INTO PGADMIN
-- ===================================================================

SELECT * FROM (
  SELECT 'interrogation_reports', COUNT(*) FROM interrogation_reports
  UNION ALL SELECT 'ir_family_history', COUNT(*) FROM ir_family_history
  UNION ALL SELECT 'ir_local_contacts', COUNT(*) FROM ir_local_contacts
  UNION ALL SELECT 'ir_regular_habits', COUNT(*) FROM ir_regular_habits
  UNION ALL SELECT 'ir_types_of_drugs', COUNT(*) FROM ir_types_of_drugs
  UNION ALL SELECT 'ir_sim_details', COUNT(*) FROM ir_sim_details
  UNION ALL SELECT 'ir_financial_history', COUNT(*) FROM ir_financial_history
  UNION ALL SELECT 'ir_consumer_details', COUNT(*) FROM ir_consumer_details
  UNION ALL SELECT 'ir_modus_operandi', COUNT(*) FROM ir_modus_operandi
  UNION ALL SELECT 'ir_previous_offences_confessed', COUNT(*) FROM ir_previous_offences_confessed
  UNION ALL SELECT 'ir_defence_counsel', COUNT(*) FROM ir_defence_counsel
  UNION ALL SELECT 'ir_associate_details', COUNT(*) FROM ir_associate_details
  UNION ALL SELECT 'ir_shelter', COUNT(*) FROM ir_shelter
  UNION ALL SELECT 'ir_media', COUNT(*) FROM ir_media
  UNION ALL SELECT 'ir_interrogation_report_refs', COUNT(*) FROM ir_interrogation_report_refs
  UNION ALL SELECT 'ir_dopams_links', COUNT(*) FROM ir_dopams_links
  UNION ALL SELECT 'ir_indulgance_before_offence', COUNT(*) FROM ir_indulgance_before_offence
  UNION ALL SELECT 'ir_property_disposal', COUNT(*) FROM ir_property_disposal
  UNION ALL SELECT 'ir_regularization_transit_warrants', COUNT(*) FROM ir_regularization_transit_warrants
  UNION ALL SELECT 'ir_execution_of_nbw', COUNT(*) FROM ir_execution_of_nbw
  UNION ALL SELECT 'ir_pending_nbw', COUNT(*) FROM ir_pending_nbw
  UNION ALL SELECT 'ir_sureties', COUNT(*) FROM ir_sureties
  UNION ALL SELECT 'ir_jail_sentence', COUNT(*) FROM ir_jail_sentence
  UNION ALL SELECT 'ir_new_gang_formation', COUNT(*) FROM ir_new_gang_formation
  UNION ALL SELECT 'ir_conviction_acquittal', COUNT(*) FROM ir_conviction_acquittal
) t(table_name, count)
ORDER BY table_name;


-- ===================================================================
-- 3. RESET SEQUENCES IF OUT OF SYNC - PASTE THIS INTO PGADMIN
-- ===================================================================
-- Run this ONLY if Step 1 showed sequence values less than max_id

SELECT setval('ir_family_history_id_seq', (SELECT COALESCE(MAX(id), 1) + 1 FROM ir_family_history));
SELECT setval('ir_local_contacts_id_seq', (SELECT COALESCE(MAX(id), 1) + 1 FROM ir_local_contacts));
SELECT setval('ir_regular_habits_id_seq', (SELECT COALESCE(MAX(id), 1) + 1 FROM ir_regular_habits));
SELECT setval('ir_types_of_drugs_id_seq', (SELECT COALESCE(MAX(id), 1) + 1 FROM ir_types_of_drugs));
SELECT setval('ir_sim_details_id_seq', (SELECT COALESCE(MAX(id), 1) + 1 FROM ir_sim_details));
SELECT setval('ir_financial_history_id_seq', (SELECT COALESCE(MAX(id), 1) + 1 FROM ir_financial_history));
SELECT setval('ir_consumer_details_id_seq', (SELECT COALESCE(MAX(id), 1) + 1 FROM ir_consumer_details));
SELECT setval('ir_modus_operandi_id_seq', (SELECT COALESCE(MAX(id), 1) + 1 FROM ir_modus_operandi));
SELECT setval('ir_previous_offences_confessed_id_seq', (SELECT COALESCE(MAX(id), 1) + 1 FROM ir_previous_offences_confessed));
SELECT setval('ir_defence_counsel_id_seq', (SELECT COALESCE(MAX(id), 1) + 1 FROM ir_defence_counsel));
SELECT setval('ir_associate_details_id_seq', (SELECT COALESCE(MAX(id), 1) + 1 FROM ir_associate_details));
SELECT setval('ir_shelter_id_seq', (SELECT COALESCE(MAX(id), 1) + 1 FROM ir_shelter));
SELECT setval('ir_media_id_seq', (SELECT COALESCE(MAX(id), 1) + 1 FROM ir_media));
SELECT setval('ir_interrogation_report_refs_id_seq', (SELECT COALESCE(MAX(id), 1) + 1 FROM ir_interrogation_report_refs));
SELECT setval('ir_dopams_links_id_seq', (SELECT COALESCE(MAX(id), 1) + 1 FROM ir_dopams_links));
SELECT setval('ir_indulgance_before_offence_id_seq', (SELECT COALESCE(MAX(id), 1) + 1 FROM ir_indulgance_before_offence));
SELECT setval('ir_property_disposal_id_seq', (SELECT COALESCE(MAX(id), 1) + 1 FROM ir_property_disposal));
SELECT setval('ir_regularization_transit_warrants_id_seq', (SELECT COALESCE(MAX(id), 1) + 1 FROM ir_regularization_transit_warrants));
SELECT setval('ir_execution_of_nbw_id_seq', (SELECT COALESCE(MAX(id), 1) + 1 FROM ir_execution_of_nbw));
SELECT setval('ir_pending_nbw_id_seq', (SELECT COALESCE(MAX(id), 1) + 1 FROM ir_pending_nbw));
SELECT setval('ir_sureties_id_seq', (SELECT COALESCE(MAX(id), 1) + 1 FROM ir_sureties));
SELECT setval('ir_jail_sentence_id_seq', (SELECT COALESCE(MAX(id), 1) + 1 FROM ir_jail_sentence));
SELECT setval('ir_new_gang_formation_id_seq', (SELECT COALESCE(MAX(id), 1) + 1 FROM ir_new_gang_formation));
SELECT setval('ir_conviction_acquittal_id_seq', (SELECT COALESCE(MAX(id), 1) + 1 FROM ir_conviction_acquittal));


-- ===================================================================
-- 4. VERIFY ALL INDEXES EXIST - PASTE THIS INTO PGADMIN
-- ===================================================================

SELECT schemaname, tablename, indexname
FROM pg_indexes
WHERE schemaname = 'public' AND tablename LIKE 'ir_%'
ORDER BY tablename, indexname;


-- ===================================================================
-- 5. CHECK FOR RECENT DATA ISSUES - PASTE THIS INTO PGADMIN
-- ===================================================================

-- Find records from last 24 hours
SELECT COUNT(*) as last_24h_count,
       MIN(date_created) as oldest,
       MAX(date_created) as newest
FROM interrogation_reports
WHERE date_created > NOW() - INTERVAL '24 hours';

-- Check for NULL interrogation_report_id in child tables
SELECT 'ir_family_history' as table_name, COUNT(*) as null_count
FROM ir_family_history WHERE interrogation_report_id IS NULL
UNION ALL
SELECT 'ir_local_contacts', COUNT(*)
FROM ir_local_contacts WHERE interrogation_report_id IS NULL
UNION ALL
SELECT 'ir_types_of_drugs', COUNT(*)
FROM ir_types_of_drugs WHERE interrogation_report_id IS NULL;


-- ===================================================================
-- 6. VERIFY FIX WORKED - PASTE THIS AFTER RUNNING ETL AGAIN
-- ===================================================================

-- Get recent insert stats
SELECT
  (SELECT COUNT(*) FROM interrogation_reports WHERE date_created > NOW() - INTERVAL '1 hour') as new_ir_count,
  (SELECT COUNT(*) FROM ir_family_history WHERE interrogation_report_id IN
    (SELECT interrogation_report_id FROM interrogation_reports WHERE date_created > NOW() - INTERVAL '1 hour')
  ) as family_history_count,
  (SELECT COUNT(*) FROM ir_local_contacts WHERE interrogation_report_id IN
    (SELECT interrogation_report_id FROM interrogation_reports WHERE date_created > NOW() - INTERVAL '1 hour')
  ) as local_contacts_count;


-- ===================================================================
-- INSTRUCTIONS FOR PGADMIN
-- ===================================================================

/*
1. Open PgAdmin and connect to your database (dev-3)

2. STEP 1: Check sequences
   - Click Tools > Query Tool
   - Copy/paste SECTION 1 above
   - Click Execute (F5)
   - Look at seq_value column - should match or exceed max_id
   - If seq_value < max_id: sequences are OUT OF SYNC

3. STEP 2: Count records
   - Copy/paste SECTION 2
   - Click Execute
   - Shows how many records in each table

4. STEP 3: Fix if needed (only if Step 1 showed out of sync)
   - Copy/paste SECTION 3
   - Click Execute
   - Wait for completion

5. STEP 4: Verify indexes
   - Copy/paste SECTION 4
   - Should see indexes for all ir_* tables

6. AFTER RUNNING ETL AGAIN:
   - Copy/paste SECTION 6
   - Should show new records inserted successfully
   - If counts are > 0, the fix worked!
*/
