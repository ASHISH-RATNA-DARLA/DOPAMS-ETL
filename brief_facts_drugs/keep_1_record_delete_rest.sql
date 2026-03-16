-- ============================================================================
-- KEEP 1 RECORD AND DELETE ALL OTHERS FROM brief_facts_drug
-- ============================================================================
-- THIS SCRIPT: Keeps ONE specific record and DELETES all others
-- RECORD TO KEEP: crime_id = '64b065bfe8e8a718bfdcbad8'
-- WARNING: This operation is DESTRUCTIVE - will delete ~1.27M+ records
-- ============================================================================

-- STEP 1: Create a backup before we delete (RECOMMENDED)
-- Uncomment if you want to backup:
-- CREATE TABLE brief_facts_drug_backup_before_keep_1 AS 
-- SELECT * FROM brief_facts_drug;

-- STEP 2: Verify the record we're keeping exists
SELECT 
    crime_id, 
    primary_drug_name, 
    quantity, 
    quantity_unit,
    created_at
FROM brief_facts_drug 
WHERE crime_id = '64b065bfe8e8a718bfdcbad8'
LIMIT 1;

-- STEP 3: Count records before deletion
SELECT COUNT(*) as total_records_before FROM brief_facts_drug;

-- STEP 4: DELETE ALL EXCEPT the one record we want to keep
DELETE FROM brief_facts_drug 
WHERE crime_id <> '64b065bfe8e8a718bfdcbad8';

-- STEP 5: Verify deletion - should return exactly 1 record
SELECT COUNT(*) as total_records_after FROM brief_facts_drug;

-- STEP 6: Verify the kept record is correct
SELECT * FROM brief_facts_drug 
WHERE crime_id = '64b065bfe8e8a718bfdcbad8';

-- ============================================================================
-- RECOVERY (if you need to restore from backup):
-- DROP TABLE brief_facts_drug;
-- ALTER TABLE brief_facts_drug_backup_before_keep_1 
-- RENAME TO brief_facts_drug;
-- ============================================================================
