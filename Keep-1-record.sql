BEGIN;

-- LEVEL 3: Deepest Child Tables
DELETE FROM ir_associate_details WHERE interrogation_report_id != '63787aaf50929367f95f1ec6';
DELETE FROM ir_consumer_details WHERE interrogation_report_id != '63787aaf50929367f95f1ec6';
DELETE FROM ir_family_history WHERE interrogation_report_id != '63787aaf50929367f95f1ec6';
DELETE FROM ir_financial_history WHERE interrogation_report_id != '63787aaf50929367f95f1ec6';
DELETE FROM ir_local_contacts WHERE interrogation_report_id != '63787aaf50929367f95f1ec6';
DELETE FROM ir_modus_operandi WHERE interrogation_report_id != '63787aaf50929367f95f1ec6';
DELETE FROM ir_previous_offences_confessed WHERE interrogation_report_id != '63787aaf50929367f95f1ec6';
DELETE FROM ir_sim_details WHERE interrogation_report_id != '63787aaf50929367f95f1ec6';
DELETE FROM ir_types_of_drugs WHERE interrogation_report_id != '63787aaf50929367f95f1ec6';
DELETE FROM ir_defence_counsel WHERE interrogation_report_id != '63787aaf50929367f95f1ec6';
DELETE FROM ir_shelter WHERE interrogation_report_id != '63787aaf50929367f95f1ec6';
DELETE FROM ir_regular_habits WHERE interrogation_report_id != '63787aaf50929367f95f1ec6';
DELETE FROM ir_dopams_links WHERE interrogation_report_id != '63787aaf50929367f95f1ec6';
DELETE FROM ir_media WHERE interrogation_report_id != '63787aaf50929367f95f1ec6';
DELETE FROM ir_interrogation_report_refs WHERE interrogation_report_id != '63787aaf50929367f95f1ec6';
DELETE FROM chargesheet_acts WHERE chargesheet_id != '0294b57b-adf2-4d2a-9aa0-2808f88452fe';
DELETE FROM chargesheet_accused WHERE chargesheet_id != '0294b57b-adf2-4d2a-9aa0-2808f88452fe';
DELETE FROM chargesheet_files WHERE chargesheet_id != '0294b57b-adf2-4d2a-9aa0-2808f88452fe';
DELETE FROM fsl_case_property_media WHERE case_property_id NOT IN (SELECT case_property_id FROM fsl_case_property WHERE crime_id = '62aa9b9ea2d2490c539be447');

-- LEVEL 2: Bridge Table
DELETE FROM accused WHERE crime_id != '62aa9b9ea2d2490c539be447';

-- LEVEL 1: Case Entities
DELETE FROM interrogation_reports WHERE crime_id != '62aa9b9ea2d2490c539be447';
DELETE FROM chargesheets WHERE id != '0294b57b-adf2-4d2a-9aa0-2808f88452fe';
DELETE FROM arrests WHERE crime_id != '62aa9b9ea2d2490c539be447';
DELETE FROM properties WHERE crime_id != '62aa9b9ea2d2490c539be447';
DELETE FROM mo_seizures WHERE crime_id != '62aa9b9ea2d2490c539be447';
DELETE FROM disposal WHERE crime_id != '62aa9b9ea2d2490c539be447';
DELETE FROM fsl_case_property WHERE crime_id != '62aa9b9ea2d2490c539be447';
DELETE FROM charge_sheet_updates WHERE crime_id != '62aa9b9ea2d2490c539be447';
DELETE FROM brief_facts_accused WHERE crime_id != '62aa9b9ea2d2490c539be447';
DELETE FROM brief_facts_crime_summaries WHERE crime_id != '62aa9b9ea2d2490c539be447';
DELETE FROM brief_facts_drug WHERE crime_id != '62aa9b9ea2d2490c539be447';

-- LEVEL 0: Foundation
-- Keeps your target person + anyone still referenced by a surviving accused row
DELETE FROM persons 
WHERE person_id NOT IN (
    SELECT DISTINCT person_id FROM accused WHERE person_id IS NOT NULL
)
AND person_id != '62ab45de447aa0823c735af1';

DELETE FROM crimes WHERE crime_id != '62aa9b9ea2d2490c539be447';
DELETE FROM hierarchy WHERE ps_code != '2022057';

-- DATE MODIFICATION FOR KEPT RECORD
-- Set dates back to 2022-01-01 to force ETL into processing it as an UPDATE from that date
UPDATE crimes SET date_modified = '2022-01-01', date_created = '2022-01-01' WHERE crime_id = '62aa9b9ea2d2490c539be447';
UPDATE arrests SET date_modified = '2022-01-01', date_created = '2022-01-01' WHERE crime_id = '62aa9b9ea2d2490c539be447';
UPDATE properties SET date_modified = '2022-01-01', date_created = '2022-01-01' WHERE crime_id = '62aa9b9ea2d2490c539be447';
UPDATE mo_seizures SET date_modified = '2022-01-01', date_created = '2022-01-01' WHERE crime_id = '62aa9b9ea2d2490c539be447';
UPDATE disposal SET date_modified = '2022-01-01', date_created = '2022-01-01' WHERE crime_id = '62aa9b9ea2d2490c539be447';
UPDATE fsl_case_property SET date_modified = '2022-01-01', date_created = '2022-01-01' WHERE crime_id = '62aa9b9ea2d2490c539be447';
UPDATE charge_sheet_updates SET date_created = '2022-01-01' WHERE crime_id = '62aa9b9ea2d2490c539be447';
UPDATE brief_facts_accused SET date_modified = '2022-01-01', date_created = '2022-01-01' WHERE crime_id = '62aa9b9ea2d2490c539be447';
UPDATE brief_facts_crime_summaries SET date_modified = '2022-01-01', date_created = '2022-01-01' WHERE crime_id = '62aa9b9ea2d2490c539be447';
UPDATE brief_facts_drug SET updated_at = '2022-01-01', created_at = '2022-01-01' WHERE crime_id = '62aa9b9ea2d2490c539be447';
UPDATE persons SET date_modified = '2022-01-01', date_created = '2022-01-01' WHERE person_id = '62ab45de447aa0823c735af1';
UPDATE interrogation_reports SET date_modified = '2022-01-01', date_created = '2022-01-01' WHERE interrogation_report_id = '63787aaf50929367f95f1ec6';
UPDATE chargesheets SET date_modified = '2022-01-01', date_created = '2022-01-01' WHERE id = '0294b57b-adf2-4d2a-9aa0-2808f88452fe';

COMMIT;