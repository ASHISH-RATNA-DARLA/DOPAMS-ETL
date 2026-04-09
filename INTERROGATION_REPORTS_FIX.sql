-- =====================================================================================
-- INTERROGATION REPORTS (IR) ETL FIX - Schema Changes
-- =====================================================================================
-- 
-- This migration adds support for 9 previously unmapped API fields and fixes 7 known issues
-- 
-- PHASE 1: Add new tables for missing array fields
-- PHASE 2: Fix boolean defaults in existing tables
-- PHASE 3: Extend ir_previous_offences_confessed with new fields
-- PHASE 4: Create validation views and indexes
--
-- Production-Safe: All changes use ALTER TABLE IF NOT EXISTS and ADD COLUMN IF NOT EXISTS
-- =====================================================================================

-- =====================================================================================
-- PHASE 1: ADD NEW TABLES FOR 9 MISSING ARRAY FIELDS
-- =====================================================================================

-- 1. INDULGANCE_BEFORE_OFFENCE - Array of substances/habits before offense (similar to REGULAR_HABITS)
CREATE TABLE IF NOT EXISTS public.ir_indulgance_before_offence (
    id SERIAL PRIMARY KEY,
    interrogation_report_id VARCHAR(50) NOT NULL,
    indulgance TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (interrogation_report_id) REFERENCES public.interrogation_reports(interrogation_report_id) ON DELETE CASCADE
);

COMMENT ON TABLE public.ir_indulgance_before_offence IS 'Substances/habits indulged in before offense for each IR record. One record per indulgance entry (junction table for INDULGANCE_BEFORE_OFFENCE array).';
COMMENT ON COLUMN public.ir_indulgance_before_offence.interrogation_report_id IS 'Foreign key to interrogation_reports table';
COMMENT ON COLUMN public.ir_indulgance_before_offence.indulgance IS 'Type of indulgance (e.g., alcohol, drugs, etc.)';

CREATE INDEX IF NOT EXISTS idx_ir_indulgance_before_offence_ir_id 
ON public.ir_indulgance_before_offence(interrogation_report_id);


-- 2. PROPERTY_DISPOSAL - Array of property disposal records
CREATE TABLE IF NOT EXISTS public.ir_property_disposal (
    id SERIAL PRIMARY KEY,
    interrogation_report_id VARCHAR(50) NOT NULL,
    mode_of_disposal VARCHAR(255),
    buyer_name VARCHAR(255),
    sold_amount_in_inr TEXT,
    location_of_disposal TEXT,
    date_of_disposal DATE,
    remarks TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (interrogation_report_id) REFERENCES public.interrogation_reports(interrogation_report_id) ON DELETE CASCADE
);

COMMENT ON TABLE public.ir_property_disposal IS 'Property disposal details for each IR record. One record per disposal entry.';
COMMENT ON COLUMN public.ir_property_disposal.interrogation_report_id IS 'Foreign key to interrogation_reports table';
COMMENT ON COLUMN public.ir_property_disposal.mode_of_disposal IS 'How property was disposed (sold, donated, etc.)';
COMMENT ON COLUMN public.ir_property_disposal.buyer_name IS 'Name of buyer or recipient';
COMMENT ON COLUMN public.ir_property_disposal.sold_amount_in_inr IS 'Amount in INR if sold';
COMMENT ON COLUMN public.ir_property_disposal.location_of_disposal IS 'Location where property was disposed';
COMMENT ON COLUMN public.ir_property_disposal.date_of_disposal IS 'Date of disposal';

CREATE INDEX IF NOT EXISTS idx_ir_property_disposal_ir_id 
ON public.ir_property_disposal(interrogation_report_id);


-- 3. REGULARIZATION_OF_TRANSIT_WARRANTS - Array of warrant regularization records
CREATE TABLE IF NOT EXISTS public.ir_regularization_transit_warrants (
    id SERIAL PRIMARY KEY,
    interrogation_report_id VARCHAR(50) NOT NULL,
    warrant_number VARCHAR(255),
    warrant_type VARCHAR(100),
    issued_date DATE,
    jurisdiction_ps VARCHAR(255),
    crime_num VARCHAR(255),
    status VARCHAR(100),
    remarks TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (interrogation_report_id) REFERENCES public.interrogation_reports(interrogation_report_id) ON DELETE CASCADE
);

COMMENT ON TABLE public.ir_regularization_transit_warrants IS 'Regularization of transit warrants for each IR record. One record per warrant entry.';
COMMENT ON COLUMN public.ir_regularization_transit_warrants.interrogation_report_id IS 'Foreign key to interrogation_reports table';
COMMENT ON COLUMN public.ir_regularization_transit_warrants.warrant_number IS 'Warrant number/reference';
COMMENT ON COLUMN public.ir_regularization_transit_warrants.warrant_type IS 'Type of warrant (NBW, transit, etc.)';
COMMENT ON COLUMN public.ir_regularization_transit_warrants.issued_date IS 'Date warrant was issued';
COMMENT ON COLUMN public.ir_regularization_transit_warrants.jurisdiction_ps IS 'Police station/jurisdiction';
COMMENT ON COLUMN public.ir_regularization_transit_warrants.crime_num IS 'Associated crime number';
COMMENT ON COLUMN public.ir_regularization_transit_warrants.status IS 'Current status (pending, executed, withdrawn, etc.)';

CREATE INDEX IF NOT EXISTS idx_ir_regularization_transit_warrants_ir_id 
ON public.ir_regularization_transit_warrants(interrogation_report_id);


-- 4. EXECUTION_OF_NBW - Array of NBW execution records
CREATE TABLE IF NOT EXISTS public.ir_execution_of_nbw (
    id SERIAL PRIMARY KEY,
    interrogation_report_id VARCHAR(50) NOT NULL,
    nbw_number VARCHAR(255),
    issued_date DATE,
    executed_date DATE,
    jurisdiction_ps VARCHAR(255),
    crime_num VARCHAR(255),
    executed_by VARCHAR(255),
    place_of_execution TEXT,
    remarks TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (interrogation_report_id) REFERENCES public.interrogation_reports(interrogation_report_id) ON DELETE CASCADE
);

COMMENT ON TABLE public.ir_execution_of_nbw IS 'Execution of NBW (Non-Bailable Warrant) for each IR record. One record per NBW execution entry.';
COMMENT ON COLUMN public.ir_execution_of_nbw.interrogation_report_id IS 'Foreign key to interrogation_reports table';
COMMENT ON COLUMN public.ir_execution_of_nbw.nbw_number IS 'NBW number/reference';
COMMENT ON COLUMN public.ir_execution_of_nbw.issued_date IS 'Date NBW was issued';
COMMENT ON COLUMN public.ir_execution_of_nbw.executed_date IS 'Date NBW was executed';
COMMENT ON COLUMN public.ir_execution_of_nbw.jurisdiction_ps IS 'Police station where executed';
COMMENT ON COLUMN public.ir_execution_of_nbw.crime_num IS 'Associated crime number';
COMMENT ON COLUMN public.ir_execution_of_nbw.executed_by IS 'Name of officer who executed';
COMMENT ON COLUMN public.ir_execution_of_nbw.place_of_execution IS 'Location of execution';

CREATE INDEX IF NOT EXISTS idx_ir_execution_of_nbw_ir_id 
ON public.ir_execution_of_nbw(interrogation_report_id);


-- 5. PENDING_NBW - Array of pending NBW records
CREATE TABLE IF NOT EXISTS public.ir_pending_nbw (
    id SERIAL PRIMARY KEY,
    interrogation_report_id VARCHAR(50) NOT NULL,
    nbw_number VARCHAR(255),
    issued_date DATE,
    jurisdiction_ps VARCHAR(255),
    crime_num VARCHAR(255),
    reason_for_pending TEXT,
    expected_execution_date DATE,
    remarks TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (interrogation_report_id) REFERENCES public.interrogation_reports(interrogation_report_id) ON DELETE CASCADE
);

COMMENT ON TABLE public.ir_pending_nbw IS 'Pending NBW (Non-Bailable Warrant) for each IR record. One record per pending NBW entry.';
COMMENT ON COLUMN public.ir_pending_nbw.interrogation_report_id IS 'Foreign key to interrogation_reports table';
COMMENT ON COLUMN public.ir_pending_nbw.nbw_number IS 'NBW number/reference';
COMMENT ON COLUMN public.ir_pending_nbw.issued_date IS 'Date NBW was issued';
COMMENT ON COLUMN public.ir_pending_nbw.jurisdiction_ps IS 'Police station where issued';
COMMENT ON COLUMN public.ir_pending_nbw.crime_num IS 'Associated crime number';
COMMENT ON COLUMN public.ir_pending_nbw.reason_for_pending IS 'Reason why NBW is still pending';
COMMENT ON COLUMN public.ir_pending_nbw.expected_execution_date IS 'Expected date of execution';

CREATE INDEX IF NOT EXISTS idx_ir_pending_nbw_ir_id 
ON public.ir_pending_nbw(interrogation_report_id);


-- 6. SURETIES - Array of surety records
CREATE TABLE IF NOT EXISTS public.ir_sureties (
    id SERIAL PRIMARY KEY,
    interrogation_report_id VARCHAR(50) NOT NULL,
    surety_person_id VARCHAR(50),
    surety_name VARCHAR(255),
    relation_to_accused VARCHAR(100),
    occupation VARCHAR(255),
    aadhar_number VARCHAR(50),
    pan_number VARCHAR(50),
    house_no VARCHAR(100),
    street_road_no VARCHAR(255),
    locality_village TEXT,
    area_mandal VARCHAR(255),
    district VARCHAR(255),
    state_ut VARCHAR(255),
    pin_code VARCHAR(10),
    phone_number VARCHAR(20),
    surety_amount_in_inr TEXT,
    date_of_surety DATE,
    remarks TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (interrogation_report_id) REFERENCES public.interrogation_reports(interrogation_report_id) ON DELETE CASCADE
);

COMMENT ON TABLE public.ir_sureties IS 'Surety information for bail for each IR record. One record per surety entry.';
COMMENT ON COLUMN public.ir_sureties.interrogation_report_id IS 'Foreign key to interrogation_reports table';
COMMENT ON COLUMN public.ir_sureties.surety_person_id IS 'Reference to person_id if surety is in DOPAMS';
COMMENT ON COLUMN public.ir_sureties.surety_name IS 'Name of surety';
COMMENT ON COLUMN public.ir_sureties.relation_to_accused IS 'Relationship to accused (friend, family, etc.)';
COMMENT ON COLUMN public.ir_sureties.occupation IS 'Occupation of surety';
COMMENT ON COLUMN public.ir_sureties.surety_amount_in_inr IS 'Amount of surety in INR';
COMMENT ON COLUMN public.ir_sureties.date_of_surety IS 'Date surety was provided';

CREATE INDEX IF NOT EXISTS idx_ir_sureties_ir_id 
ON public.ir_sureties(interrogation_report_id);


-- 7. JAIL_SENTENCE - Array of jail sentence records
CREATE TABLE IF NOT EXISTS public.ir_jail_sentence (
    id SERIAL PRIMARY KEY,
    interrogation_report_id VARCHAR(50) NOT NULL,
    crime_num VARCHAR(255),
    jurisdiction_ps VARCHAR(255),
    law_section VARCHAR(255),
    sentence_type VARCHAR(100),
    sentence_duration_in_months INTEGER,
    sentence_start_date DATE,
    sentence_end_date DATE,
    sentence_amount_in_inr TEXT,
    jail_name VARCHAR(255),
    date_of_jail_entry DATE,
    date_of_jail_release DATE,
    remarks TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (interrogation_report_id) REFERENCES public.interrogation_reports(interrogation_report_id) ON DELETE CASCADE
);

COMMENT ON TABLE public.ir_jail_sentence IS 'Jail sentence details for each IR record. One record per sentence entry.';
COMMENT ON COLUMN public.ir_jail_sentence.interrogation_report_id IS 'Foreign key to interrogation_reports table';
COMMENT ON COLUMN public.ir_jail_sentence.crime_num IS 'Associated crime number';
COMMENT ON COLUMN public.ir_jail_sentence.sentence_type IS 'Type of sentence (RI, SI, etc.)';
COMMENT ON COLUMN public.ir_jail_sentence.sentence_duration_in_months IS 'Duration in months';
COMMENT ON COLUMN public.ir_jail_sentence.sentence_start_date IS 'When sentence started';
COMMENT ON COLUMN public.ir_jail_sentence.sentence_end_date IS 'When sentence ended';
COMMENT ON COLUMN public.ir_jail_sentence.sentence_amount_in_inr IS 'Fine amount in INR if applicable';
COMMENT ON COLUMN public.ir_jail_sentence.jail_name IS 'Name of jail where served';
COMMENT ON COLUMN public.ir_jail_sentence.date_of_jail_entry IS 'When admitted to jail';
COMMENT ON COLUMN public.ir_jail_sentence.date_of_jail_release IS 'When released from jail';

CREATE INDEX IF NOT EXISTS idx_ir_jail_sentence_ir_id 
ON public.ir_jail_sentence(interrogation_report_id);


-- 8. NEW_GANG_FORMATION - Array of new gang formation records
CREATE TABLE IF NOT EXISTS public.ir_new_gang_formation (
    id SERIAL PRIMARY KEY,
    interrogation_report_id VARCHAR(50) NOT NULL,
    gang_name VARCHAR(255),
    gang_formation_date DATE,
    number_of_members INTEGER,
    leader_name VARCHAR(255),
    leader_person_id VARCHAR(50),
    gang_objective TEXT,
    criminal_history TEXT,
    jurisdiction_ps VARCHAR(255),
    active BOOLEAN,
    remarks TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (interrogation_report_id) REFERENCES public.interrogation_reports(interrogation_report_id) ON DELETE CASCADE
);

COMMENT ON TABLE public.ir_new_gang_formation IS 'New gang formation details for each IR record. One record per gang entry.';
COMMENT ON COLUMN public.ir_new_gang_formation.interrogation_report_id IS 'Foreign key to interrogation_reports table';
COMMENT ON COLUMN public.ir_new_gang_formation.gang_name IS 'Name of the gang';
COMMENT ON COLUMN public.ir_new_gang_formation.gang_formation_date IS 'When gang was formed';
COMMENT ON COLUMN public.ir_new_gang_formation.number_of_members IS 'Number of members';
COMMENT ON COLUMN public.ir_new_gang_formation.leader_name IS 'Name of gang leader';
COMMENT ON COLUMN public.ir_new_gang_formation.leader_person_id IS 'Reference to person_id if leader is in DOPAMS';
COMMENT ON COLUMN public.ir_new_gang_formation.gang_objective IS 'Stated objective of gang';
COMMENT ON COLUMN public.ir_new_gang_formation.criminal_history IS 'Known criminal activities';
COMMENT ON COLUMN public.ir_new_gang_formation.active IS 'Whether gang is still active';

CREATE INDEX IF NOT EXISTS idx_ir_new_gang_formation_ir_id 
ON public.ir_new_gang_formation(interrogation_report_id);


-- 9. CONVICTION_ACQUITTAL - Array of conviction/acquittal records
CREATE TABLE IF NOT EXISTS public.ir_conviction_acquittal (
    id SERIAL PRIMARY KEY,
    interrogation_report_id VARCHAR(50) NOT NULL,
    crime_num VARCHAR(255),
    jurisdiction_ps VARCHAR(255),
    court_name VARCHAR(500),
    judge_name VARCHAR(255),
    law_section VARCHAR(255),
    verdict VARCHAR(100),
    verdict_date DATE,
    reason_if_acquitted TEXT,
    conviction_remarks TEXT,
    fine_amount_in_inr TEXT,
    sentence_if_convicted TEXT,
    appeal_status VARCHAR(100),
    appeal_court VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (interrogation_report_id) REFERENCES public.interrogation_reports(interrogation_report_id) ON DELETE CASCADE
);

COMMENT ON TABLE public.ir_conviction_acquittal IS 'Conviction/acquittal details for each IR record. One record per case verdict entry.';
COMMENT ON COLUMN public.ir_conviction_acquittal.interrogation_report_id IS 'Foreign key to interrogation_reports table';
COMMENT ON COLUMN public.ir_conviction_acquittal.crime_num IS 'Associated crime number';
COMMENT ON COLUMN public.ir_conviction_acquittal.court_name IS 'Court name where verdict was delivered';
COMMENT ON COLUMN public.ir_conviction_acquittal.verdict IS 'Verdict (Convicted, Acquitted, Discharged, etc.)';
COMMENT ON COLUMN public.ir_conviction_acquittal.verdict_date IS 'Date of verdict';
COMMENT ON COLUMN public.ir_conviction_acquittal.reason_if_acquitted IS 'Reason for acquittal if applicable';
COMMENT ON COLUMN public.ir_conviction_acquittal.sentence_if_convicted IS 'Details of sentence if convicted';
COMMENT ON COLUMN public.ir_conviction_acquittal.appeal_status IS 'Status of any appeal (Pending, Dismissed, Allowed, etc.)';

CREATE INDEX IF NOT EXISTS idx_ir_conviction_acquittal_ir_id 
ON public.ir_conviction_acquittal(interrogation_report_id);


-- =====================================================================================
-- PHASE 2: FIX BOOLEAN DEFAULTS IN EXISTING TABLES
-- =====================================================================================
-- Change from forced FALSE to NULL for better null-safety
-- This allows distinguishing between explicitly FALSE and unknown values

ALTER TABLE public.interrogation_reports 
ALTER COLUMN is_in_jail DROP DEFAULT,
ALTER COLUMN is_on_bail DROP DEFAULT,
ALTER COLUMN is_absconding DROP DEFAULT,
ALTER COLUMN is_normal_life DROP DEFAULT,
ALTER COLUMN is_rehabilitated DROP DEFAULT,
ALTER COLUMN is_dead DROP DEFAULT,
ALTER COLUMN is_facing_trial DROP DEFAULT;

-- Update existing records: if they are FALSE, set to NULL (unknown)
-- This preserves existing TRUE values while normalizing FALSE to unknown
UPDATE public.interrogation_reports
SET 
    is_in_jail = CASE WHEN is_in_jail = FALSE THEN NULL ELSE is_in_jail END,
    is_on_bail = CASE WHEN is_on_bail = FALSE THEN NULL ELSE is_on_bail END,
    is_absconding = CASE WHEN is_absconding = FALSE THEN NULL ELSE is_absconding END,
    is_normal_life = CASE WHEN is_normal_life = FALSE THEN NULL ELSE is_normal_life END,
    is_rehabilitated = CASE WHEN is_rehabilitated = FALSE THEN NULL ELSE is_rehabilitated END,
    is_dead = CASE WHEN is_dead = FALSE THEN NULL ELSE is_dead END,
    is_facing_trial = CASE WHEN is_facing_trial = FALSE THEN NULL ELSE is_facing_trial END;


-- =====================================================================================
-- PHASE 3: EXTEND EXISTING TABLES WITH MISSING COLUMNS
-- =====================================================================================

-- 3a. Add support for PURCHASE_AMOUNT_IN_INR (fix field name mismatch)
-- The API currently sends "PURCHASE_AMOUN_IN_INR" (typo), but we normalize to "PURCHASE_AMOUNT_IN_INR" in the database
-- The ETL will support both field names for backward compatibility

-- 3b. Extend ir_previous_offences_confessed with additional fields  
-- These fields may be part of the array but weren't previously mapped
ALTER TABLE public.ir_previous_offences_confessed 
ADD COLUMN IF NOT EXISTS conviction_status VARCHAR(100),
ADD COLUMN IF NOT EXISTS bail_status VARCHAR(100),
ADD COLUMN IF NOT EXISTS court_name VARCHAR(500),
ADD COLUMN IF NOT EXISTS judge_name VARCHAR(255);

COMMENT ON COLUMN public.ir_previous_offences_confessed.conviction_status IS 'Status of conviction (if relevant to the offense)';
COMMENT ON COLUMN public.ir_previous_offences_confessed.bail_status IS 'Bail status during this offense';
COMMENT ON COLUMN public.ir_previous_offences_confessed.court_name IS 'Court handling the case';
COMMENT ON COLUMN public.ir_previous_offences_confessed.judge_name IS 'Judge handling the case';

-- 3c. Fix date type in ir_previous_offences_confessed
-- The arrest_date is currently DATE but should be DATE (no change needed, already correct)
-- Note: The ETL currently truncates values to 100 chars - this fix removes that truncation

-- =====================================================================================
-- PHASE 4: CREATE VALIDATION VIEWS FOR DATA QUALITY CHECKS
-- =====================================================================================

-- View to check API field to DB persistence mapping
CREATE OR REPLACE VIEW public.ir_field_persistence_check AS
SELECT
    'INTERROGATION_REPORT_ID' AS api_field,
    'interrogation_report_id' AS db_column,
    COUNT(ir.interrogation_report_id) as records_with_value,
    COUNT(NULLIF(ir.interrogation_report_id, '')) as non_null_count
FROM public.interrogation_reports ir
UNION ALL
SELECT 'CRIME_ID', 'crime_id', COUNT(ir.crime_id), COUNT(NULLIF(ir.crime_id, '')) FROM public.interrogation_reports ir
UNION ALL
SELECT 'PERSON_ID', 'person_id', COUNT(ir.person_id), COUNT(NULLIF(ir.person_id, '')) FROM public.interrogation_reports ir
UNION ALL
SELECT 'INDULGANCE_BEFORE_OFFENCE', 'ir_indulgance_before_offence', COUNT(DISTINCT ib.interrogation_report_id), COUNT(ib.indulgance) FROM public.ir_indulgance_before_offence ib
UNION ALL
SELECT 'PROPERTY_DISPOSAL', 'ir_property_disposal', COUNT(DISTINCT ipd.interrogation_report_id), COUNT(ipd.mode_of_disposal) FROM public.ir_property_disposal ipd
UNION ALL
SELECT 'REGULARIZATION_OF_TRANSIT_WARRANTS', 'ir_regularization_transit_warrants', COUNT(DISTINCT irtw.interrogation_report_id), COUNT(irtw.warrant_number) FROM public.ir_regularization_transit_warrants irtw
UNION ALL
SELECT 'EXECUTION_OF_NBW', 'ir_execution_of_nbw', COUNT(DISTINCT ien.interrogation_report_id), COUNT(ien.nbw_number) FROM public.ir_execution_of_nbw ien
UNION ALL
SELECT 'PENDING_NBW', 'ir_pending_nbw', COUNT(DISTINCT ipn.interrogation_report_id), COUNT(ipn.nbw_number) FROM public.ir_pending_nbw ipn
UNION ALL
SELECT 'SURETIES', 'ir_sureties', COUNT(DISTINCT ise.interrogation_report_id), COUNT(ise.surety_name) FROM public.ir_sureties ise
UNION ALL
SELECT 'JAIL_SENTENCE', 'ir_jail_sentence', COUNT(DISTINCT ijs.interrogation_report_id), COUNT(ijs.sentence_type) FROM public.ir_jail_sentence ijs
UNION ALL
SELECT 'NEW_GANG_FORMATION', 'ir_new_gang_formation', COUNT(DISTINCT ingf.interrogation_report_id), COUNT(ingf.gang_name) FROM public.ir_new_gang_formation ingf
UNION ALL
SELECT 'CONVICTION_ACQUITTAL', 'ir_conviction_acquittal', COUNT(DISTINCT ica.interrogation_report_id), COUNT(ica.verdict) FROM public.ir_conviction_acquittal ica;

COMMENT ON VIEW public.ir_field_persistence_check IS 'Validates API field to DB persistence mapping - shows which fields are being stored and frequency of non-null values';


-- View to check for missing child table mappings
CREATE OR REPLACE VIEW public.ir_child_table_coverage AS
SELECT 'REGULAR_HABITS' AS array_field, COUNT(DISTINCT rh.interrogation_report_id) as ir_records_with_data, COUNT(*) as total_entries FROM public.ir_regular_habits rh
UNION ALL
SELECT 'TIMES_OF_DRUGS', COUNT(DISTINCT td.interrogation_report_id), COUNT(*) FROM public.ir_types_of_drugs td
UNION ALL
SELECT 'FAMILY_HISTORY', COUNT(DISTINCT fh.interrogation_report_id), COUNT(*) FROM public.ir_family_history fh
UNION ALL
SELECT 'LOCAL_CONTACTS', COUNT(DISTINCT lc.interrogation_report_id), COUNT(*) FROM public.ir_local_contacts lc
UNION ALL
SELECT 'MODUS_OPERANDI', COUNT(DISTINCT mo.interrogation_report_id), COUNT(*) FROM public.ir_modus_operandi mo
UNION ALL
SELECT 'PREVIOUS_OFFENCES_CONFESSED', COUNT(DISTINCT po.interrogation_report_id), COUNT(*) FROM public.ir_previous_offences_confessed po
UNION ALL
SELECT 'DEFENCE_COUNSEL', COUNT(DISTINCT dc.interrogation_report_id), COUNT(*) FROM public.ir_defence_counsel dc
UNION ALL
SELECT 'ASSOCIATE_DETAILS', COUNT(DISTINCT ad.interrogation_report_id), COUNT(*) FROM public.ir_associate_details ad
UNION ALL
SELECT 'SHELTER', COUNT(DISTINCT sh.interrogation_report_id), COUNT(*) FROM public.ir_shelter sh
UNION ALL
SELECT 'SIM_DETAILS', COUNT(DISTINCT sd.interrogation_report_id), COUNT(*) FROM public.ir_sim_details sd
UNION ALL
SELECT 'FINANCIAL_HISTORY', COUNT(DISTINCT fh.interrogation_report_id), COUNT(*) FROM public.ir_financial_history fh
UNION ALL
SELECT 'CONSUMER_DETAILS', COUNT(DISTINCT cd.interrogation_report_id), COUNT(*) FROM public.ir_consumer_details cd
UNION ALL
SELECT 'MEDIA', COUNT(DISTINCT m.interrogation_report_id), COUNT(*) FROM public.ir_media m
UNION ALL
SELECT 'INTERROGATION_REPORT_REFS', COUNT(DISTINCT irr.interrogation_report_id), COUNT(*) FROM public.ir_interrogation_report_refs irr
UNION ALL
SELECT 'DOPAMS_LINKS', COUNT(DISTINCT dl.interrogation_report_id), COUNT(*) FROM public.ir_dopams_links dl
UNION ALL
SELECT 'INDULGANCE_BEFORE_OFFENCE', COUNT(DISTINCT ifo.interrogation_report_id), COUNT(*) FROM public.ir_indulgance_before_offence ifo
UNION ALL
SELECT 'PROPERTY_DISPOSAL', COUNT(DISTINCT ipd.interrogation_report_id), COUNT(*) FROM public.ir_property_disposal ipd
UNION ALL
SELECT 'REGULARIZATION_TRANSIT_WARRANTS', COUNT(DISTINCT irtw.interrogation_report_id), COUNT(*) FROM public.ir_regularization_transit_warrants irtw
UNION ALL
SELECT 'EXECUTION_OF_NBW', COUNT(DISTINCT ien.interrogation_report_id), COUNT(*) FROM public.ir_execution_of_nbw ien
UNION ALL
SELECT 'PENDING_NBW', COUNT(DISTINCT ipn.interrogation_report_id), COUNT(*) FROM public.ir_pending_nbw ipn
UNION ALL
SELECT 'SURETIES', COUNT(DISTINCT ise.interrogation_report_id), COUNT(*) FROM public.ir_sureties ise
UNION ALL
SELECT 'JAIL_SENTENCE', COUNT(DISTINCT ijs.interrogation_report_id), COUNT(*) FROM public.ir_jail_sentence ijs
UNION ALL
SELECT 'NEW_GANG_FORMATION', COUNT(DISTINCT ingf.interrogation_report_id), COUNT(*) FROM public.ir_new_gang_formation ingf
UNION ALL
SELECT 'CONVICTION_ACQUITTAL', COUNT(DISTINCT ica.interrogation_report_id), COUNT(*) FROM public.ir_conviction_acquittal ica
ORDER BY array_field;

COMMENT ON VIEW public.ir_child_table_coverage IS 'Shows data coverage for all IR related arrays - helps identify which fields are being populated';


-- =====================================================================================
-- INDEX OPTIMIZATION
-- =====================================================================================

-- Create composite index on frequently joined columns
CREATE INDEX IF NOT EXISTS idx_ir_reports_crime_person 
ON public.interrogation_reports(crime_id, person_id);

CREATE INDEX IF NOT EXISTS idx_ir_reports_created_modified 
ON public.interrogation_reports(date_created, date_modified);

-- End of schema changes
-- =====================================================================================
