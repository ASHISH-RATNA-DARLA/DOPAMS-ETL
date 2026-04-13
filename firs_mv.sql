CREATE MATERIALIZED VIEW public.firs_mv AS
 SELECT c.crime_id AS id,
    h.dist_name AS unit,
    h.ps_name AS ps,
    (EXTRACT(year FROM c.fir_date))::integer AS year,
    c.fir_num AS "firNumber",
    c.fir_reg_num AS "firRegNum",
    c.acts_sections AS section,
    c.fir_type AS "firType",
    c.crime_type AS "crimeType",
    c.fir_date AS "crimeRegDate",
    c.major_head AS "majorHead",
    c.minor_head AS "minorHead",
    c.io_name AS "ioName",
    c.io_rank AS "ioRank",
    c.brief_facts AS "briefFacts",
    c.class_classification AS "caseClassification",
    c.case_status AS "caseStatus",
    ((c.class_classification)::text ~~* '%commercial%'::text) AS "isCommercial",
    c.fir_copy AS "firCopy",
    public.generate_file_url('crime'::public.source_type_enum, 'FIR_COPY'::public.source_field_enum, (c.fir_copy)::uuid) AS "firCopyUrl",
        CASE
            WHEN (c.fir_date IS NULL) THEN NULL::text
            WHEN ((c.class_classification)::text = 'Commercial'::text) THEN
            CASE
                WHEN (EXTRACT(day FROM (now() - (c.fir_date)::timestamp with time zone)) <= (180)::numeric) THEN 'Within Limit (180 Days)'::text
                ELSE 'Overdue (Beyond 180 Days)'::text
            END
            ELSE
            CASE
                WHEN (EXTRACT(day FROM (now() - (c.fir_date)::timestamp with time zone)) <= (60)::numeric) THEN 'Within Limit (60 Days)'::text
                ELSE 'Overdue (Beyond 60 Days)'::text
            END
        END AS "stipulatedPeriodForCS",
        CASE
            WHEN (c.fir_date IS NULL) THEN NULL::date
            WHEN ((c.class_classification)::text = 'Commercial'::text) THEN ((c.fir_date + '180 days'::interval))::date
            ELSE ((c.fir_date + '60 days'::interval))::date
        END AS chargesheet_due_date,
    ( SELECT count(*) AS count
           FROM public.brief_facts_ai_accused_flat bfa
          WHERE ((bfa.crime_id)::text = (c.crime_id)::text)) AS "noOfAccusedInvolved",
    ( SELECT jsonb_agg(jsonb_build_object('personCode', bfa.person_code, 'fullName', bfa.full_name, 'alias', bfa.alias_name, 'accusedType', bfa.accused_type, 'personId', bfa.person_id, 'status',
                CASE
                    WHEN ((bfa.status ~~* 'Arrest%'::text) AND (bfa.status !~~* 'Arrest Related%'::text)) THEN 'Arrested'::text
                    WHEN (bfa.status ~~* 'Surrendered%'::text) THEN 'Arrested'::text
                    WHEN (bfa.status ~~* 'Absconding'::text) THEN 'Absconding'::text
                    WHEN (bfa.status ~~* 'Arrest Related/41A CrPC Pending'::text) THEN 'Absconding'::text
                    WHEN (bfa.status ~~* '41A Cr.P.C%'::text) THEN 'Issued Notice'::text
                    WHEN (bfa.status ~~* 'High court directions%'::text) THEN 'Issued Notice'::text
                    ELSE 'Unknown'::text
                END) ORDER BY bfa.seq_num) AS jsonb_agg
           FROM public.brief_facts_ai_accused_flat bfa
          WHERE ((bfa.crime_id)::text = (c.crime_id)::text)) AS "accusedDetails",
    ( SELECT COALESCE(array_agg(DISTINCT upper(TRIM(BOTH FROM bfd.primary_drug_name))) FILTER (WHERE ((bfd.primary_drug_name IS NOT NULL) AND (bfd.primary_drug_name <> 'NO_DRUGS_DETECTED'::text))), ARRAY[]::text[]) AS "coalesce"
           FROM public.brief_facts_ai_drug_flat bfd
          WHERE ((bfd.crime_id)::text = (c.crime_id)::text)) AS "drugType",
    ( SELECT jsonb_agg(jsonb_build_object('name', bfd.primary_drug_name, 'quantity', bfd.quantity_str) ORDER BY bfd.primary_drug_name, bfd.drug_form) AS jsonb_agg
           FROM ( SELECT bfd2.primary_drug_name,
                    bfd2.drug_form,
                        CASE
                            WHEN (sum(bfd2.weight_kg) >= (1)::numeric) THEN concat(round(sum(bfd2.weight_kg), 3), ' Kg')
                            WHEN (sum(bfd2.weight_g) > (0)::numeric) THEN concat(round(sum(bfd2.weight_g), 2), ' g')
                            WHEN (sum(bfd2.volume_l) >= (1)::numeric) THEN concat(round(sum(bfd2.volume_l), 3), ' L')
                            WHEN (sum(bfd2.volume_ml) > (0)::numeric) THEN concat(round(sum(bfd2.volume_ml), 2), ' ml')
                            WHEN (sum(bfd2.count_total) > (0)::numeric) THEN concat(sum(bfd2.count_total), ' Units')
                            ELSE 'N/A'::text
                        END AS quantity_str
                   FROM public.brief_facts_ai_drug_flat bfd2
                  WHERE (((bfd2.crime_id)::text = (c.crime_id)::text) AND (bfd2.primary_drug_name IS NOT NULL) AND (bfd2.primary_drug_name <> 'NO_DRUGS_DETECTED'::text))
                  GROUP BY bfd2.primary_drug_name, bfd2.drug_form) bfd) AS "drugWithQuantity",
    ( SELECT jsonb_agg(DISTINCT jsonb_build_object('type', p2.category, 'value', p2.estimate_value)) AS jsonb_agg
           FROM public.properties p2
          WHERE ((p2.crime_id)::text = (c.crime_id)::text)) AS "propertyDetails",
    ( SELECT jsonb_agg(jsonb_build_object('id', mo.mo_seizure_id, 'seqNo', mo.seq_no, 'moId', mo.mo_id, 'type', mo.type, 'subType', mo.sub_type, 'description', mo.description, 'seizedFrom', mo.seized_from, 'seizedAt', mo.seized_at, 'seizedBy', mo.seized_by, 'strengthOfEvidence', mo.strength_of_evidence, 'posAddress1', mo.pos_address1, 'posAddress2', mo.pos_address2, 'posCity', mo.pos_city, 'posDistrict', mo.pos_district, 'posPincode', mo.pos_pincode, 'posLandmark', mo.pos_landmark, 'posDescription', mo.pos_description, 'posLatitude', mo.pos_latitude, 'posLongitude', mo.pos_longitude, 'moMediaUrl', mo.mo_media_url, 'moMediaName', mo.mo_media_name, 'moMediaFileId', mo.mo_media_file_id) ORDER BY mo.seq_no) AS jsonb_agg
           FROM public.mo_seizures mo
          WHERE ((mo.crime_id)::text = (c.crime_id)::text)) AS "moSeizuresDetails",
    (COALESCE(( SELECT count(*) AS count
           FROM public.disposal d
          WHERE (((d.crime_id)::text = (c.crime_id)::text) AND (d.disposal_type ~~* '%conviction%'::text))), (0)::bigint))::integer AS "convictionCount",
    (COALESCE(( SELECT count(*) AS count
           FROM public.disposal d
          WHERE (((d.crime_id)::text = (c.crime_id)::text) AND (d.disposal_type ~~* '%acquittal%'::text))), (0)::bigint))::integer AS "acquittalCount",
    (COALESCE(( SELECT count(*) AS count
           FROM public.disposal d
          WHERE ((d.crime_id)::text = (c.crime_id)::text)), (0)::bigint))::integer AS "totalDisposals",
    ( SELECT jsonb_agg(DISTINCT jsonb_build_object('id', d.id, 'disposalType', d.disposal_type, 'disposedAt', d.disposed_at, 'disposal', d.disposal, 'caseStatus', d.case_status, 'dateCreated', d.date_created, 'dateModified', d.date_modified)) AS jsonb_agg
           FROM public.disposal d
          WHERE ((d.crime_id)::text = (c.crime_id)::text)) AS "disposalDetails",
    ( SELECT jsonb_object_agg(d.disposal_type, d.cnt) AS jsonb_object_agg
           FROM ( SELECT disposal.disposal_type,
                    (count(*))::integer AS cnt
                   FROM public.disposal
                  WHERE ((disposal.crime_id)::text = (c.crime_id)::text)
                  GROUP BY disposal.disposal_type) d) AS "disposalCounts",
    ( SELECT jsonb_agg(jsonb_build_object('id', cs.id, 'chargesheetNo', cs.chargesheet_no, 'chargesheetNoIcjs', cs.chargesheet_no_icjs, 'chargesheetDate', cs.chargesheet_date, 'chargesheetType', cs.chargesheet_type, 'courtName', cs.court_name, 'isCcl', cs.is_ccl, 'isEsigned', cs.is_esigned, 'dateCreated', cs.date_created, 'dateModified', cs.date_modified, 'acts', ( SELECT COALESCE(jsonb_agg(jsonb_build_object('id', ca.id, 'actDescription', ca.act_description, 'section', ca.section, 'rwRequired', ca.rw_required, 'sectionDescription', ca.section_description, 'graveParticulars', ca.grave_particulars, 'createdAt', ca.created_at) ORDER BY ca.created_at), '[]'::jsonb) AS "coalesce"
                   FROM public.chargesheet_acts ca
                  WHERE (ca.chargesheet_id = cs.id)), 'accuseds', ( SELECT COALESCE(jsonb_agg(jsonb_build_object('id', csa.id, 'personId', csa.accused_person_id, 'value', p.full_name, 'chargeStatus', csa.charge_status, 'requestedForNbw', csa.requested_for_nbw, 'reasonForNoCharge', csa.reason_for_no_charge, 'isPersonMasterPresent', csa.is_person_master_present, 'createdAt', csa.created_at) ORDER BY csa.created_at), '[]'::jsonb) AS "coalesce"
                   FROM (public.chargesheet_accused csa
                     LEFT JOIN public.persons p ON (((p.person_id)::text = (csa.accused_person_id)::text)))
                  WHERE (csa.chargesheet_id = cs.id))) ORDER BY cs.chargesheet_date) AS jsonb_agg
           FROM public.chargesheets cs
          WHERE ((cs.crime_id)::text = (c.crime_id)::text)) AS chargesheets,
    ( SELECT jsonb_agg(jsonb_build_object('id', csu.id, 'updateChargeSheetId', csu.update_charge_sheet_id, 'chargeSheetNo', csu.charge_sheet_no, 'chargeSheetDate', csu.charge_sheet_date, 'chargeSheetStatus', csu.charge_sheet_status, 'takenOnFileDate', csu.taken_on_file_date, 'takenOnFileCaseType', csu.taken_on_file_case_type, 'takenOnFileCourtCaseNo', csu.taken_on_file_court_case_no, 'dateCreated', csu.date_created) ORDER BY csu.date_created DESC) AS jsonb_agg
           FROM public.charge_sheet_updates csu
          WHERE ((csu.crime_id)::text = (c.crime_id)::text)) AS "chargesheetUpdates",
    ( SELECT jsonb_agg(jsonb_build_object('casePropertyId', fcp.case_property_id, 'caseType', fcp.case_type, 'moId', fcp.mo_id, 'status', fcp.status, 'sendDate', fcp.send_date, 'fslDate', fcp.fsl_date, 'dateDisposal', fcp.date_disposal, 'releaseDate', fcp.release_date, 'returnDate', fcp.return_date, 'dateCustody', fcp.date_custody, 'dateSentToExpert', fcp.date_sent_to_expert, 'courtOrderDate', fcp.court_order_date, 'forwardingThrough', fcp.forwarding_through, 'courtName', fcp.court_name, 'fslCourtName', fcp.fsl_court_name, 'cprCourtName', fcp.cpr_court_name, 'courtOrderNumber', fcp.court_order_number, 'fslNo', fcp.fsl_no, 'fslRequestId', fcp.fsl_request_id, 'reportReceived', fcp.report_received, 'opinion', fcp.opinion, 'opinionFurnished', fcp.opinion_furnished, 'strengthOfEvidence', fcp.strength_of_evidence, 'expertType', fcp.expert_type, 'otherExpertType', fcp.other_expert_type, 'cprNo', fcp.cpr_no, 'directionByCourt', fcp.direction_by_court, 'detailsDisposal', fcp.details_disposal, 'placeDisposal', fcp.place_disposal, 'releaseOrderNo', fcp.release_order_no, 'placeCustody', fcp.place_custody, 'assignCustody', fcp.assign_custody, 'propertyReceivedBack', fcp.property_received_back, 'dateCreated', fcp.date_created, 'dateModified', fcp.date_modified) ORDER BY fcp.date_created) AS jsonb_agg
           FROM public.fsl_case_property fcp
          WHERE ((fcp.crime_id)::text = (c.crime_id)::text)) AS "casePropertyDetails",
    ( SELECT jsonb_agg(DISTINCT jsonb_build_object('id', f.id, 'filePath', f.file_path, 'fileUrl', f.file_url, 'type', f.source_field, 'name', f.notes, 'isDownloaded', f.is_downloaded)) AS jsonb_agg
           FROM public.files f
          WHERE (((f.parent_id)::text = (c.crime_id)::text) AND (f.source_type = 'crime'::public.source_type_enum))) AS documents,
    ( SELECT jsonb_agg(DISTINCT jsonb_build_object('id', f.id, 'filePath', f.file_path, 'fileUrl', f.file_url, 'type', f.source_field, 'name', f.notes, 'isDownloaded', f.is_downloaded)) AS jsonb_agg
           FROM public.files f
          WHERE (((f.source_type = 'property'::public.source_type_enum) AND ((f.parent_id)::text IN ( SELECT (properties.property_id)::text AS property_id
                   FROM public.properties
                  WHERE ((properties.crime_id)::text = (c.crime_id)::text)))) OR ((f.source_type = 'case_property'::public.source_type_enum) AND ((f.parent_id)::text IN ( SELECT (fsl_case_property.case_property_id)::text AS case_property_id
                   FROM public.fsl_case_property
                  WHERE ((fsl_case_property.crime_id)::text = (c.crime_id)::text)))))) AS "propertyDocuments",
    ( SELECT jsonb_agg(DISTINCT jsonb_build_object('id', sub.id, 'filePath', sub.file_path, 'fileUrl', sub.file_url, 'type', sub.type, 'name', sub.name, 'isDownloaded', sub.is_downloaded, 'chargesheetId', sub.chargesheet_id, 'chargesheetNo', sub.chargesheet_no)) AS jsonb_agg
           FROM ( SELECT (f.id)::text AS id,
                    f.file_path,
                    f.file_url,
                    f.notes AS name,
                    (f.source_field)::text AS type,
                    cs.id AS chargesheet_id,
                    cs.chargesheet_no,
                    f.is_downloaded
                   FROM (public.files f
                     JOIN public.chargesheets cs ON (((cs.id)::text = (f.parent_id)::text)))
                  WHERE (((cs.crime_id)::text = (c.crime_id)::text) AND (f.source_type = 'chargesheets'::public.source_type_enum))
                UNION ALL
                 SELECT (cf.id)::text AS id,
                    public.generate_file_path('chargesheets'::public.source_type_enum, 'uploadChargeSheet'::public.source_field_enum, (cf.file_id)::uuid) AS file_path,
                    public.generate_file_url('chargesheets'::public.source_type_enum, 'uploadChargeSheet'::public.source_field_enum, (cf.file_id)::uuid) AS file_url,
                    NULL::text AS name,
                    'CHARGESHEET_FILE'::text AS type,
                    cs.id AS chargesheet_id,
                    cs.chargesheet_no,
                    true AS is_downloaded
                   FROM (public.chargesheet_files cf
                     JOIN public.chargesheets cs ON ((cf.chargesheet_id = cs.id)))
                  WHERE ((cs.crime_id)::text = (c.crime_id)::text)) sub) AS "chargesheetDocuments",
    ( SELECT jsonb_agg(DISTINCT jsonb_build_object('id', sub.id, 'filePath', sub.file_path, 'fileUrl', sub.file_url, 'type', sub.type, 'name', sub.name, 'isDownloaded', sub.is_downloaded, 'moSeizureId', sub.mo_seizure_id, 'moId', sub.mo_id)) AS jsonb_agg
           FROM ( SELECT (f.id)::text AS id,
                    f.file_path,
                    f.file_url,
                    f.notes AS name,
                    f.source_field AS type,
                    mo.mo_seizure_id,
                    mo.mo_id,
                    f.is_downloaded
                   FROM (public.files f
                     JOIN public.mo_seizures mo ON (((mo.mo_seizure_id)::text = (f.parent_id)::text)))
                  WHERE (((mo.crime_id)::text = (c.crime_id)::text) AND (f.source_type = 'mo_seizures'::public.source_type_enum) AND (f.source_field = 'MO_MEDIA'::public.source_field_enum))
                UNION ALL
                 SELECT mo.mo_media_file_id AS id,
                    NULL::character varying AS file_path,
                    mo.mo_media_url AS file_url,
                    mo.mo_media_name AS name,
                    'MO_MEDIA'::public.source_field_enum AS type,
                    mo.mo_seizure_id,
                    mo.mo_id,
                    true AS is_downloaded
                   FROM public.mo_seizures mo
                  WHERE (((mo.crime_id)::text = (c.crime_id)::text) AND (mo.mo_media_url IS NOT NULL))) sub) AS "moMediaDocuments",
    ( SELECT jsonb_agg(DISTINCT jsonb_build_object('id', f.id, 'filePath', f.file_path, 'fileUrl', f.file_url, 'type', f.source_field, 'name', f.notes, 'isDownloaded', f.is_downloaded)) AS jsonb_agg
           FROM (public.files f
             JOIN public.interrogation_reports ir ON (((ir.interrogation_report_id)::text = (f.parent_id)::text)))
          WHERE (((ir.crime_id)::text = (c.crime_id)::text) AND (f.source_type = 'interrogation'::public.source_type_enum))) AS "irDocuments",
    ( SELECT jsonb_agg(((jsonb_build_object('id', ir.interrogation_report_id, 'personId', ir.person_id, 'physicalBeard', ir.physical_beard, 'physicalBuild', ir.physical_build, 'physicalBurnMarks', ir.physical_burn_marks, 'physicalColor', ir.physical_color, 'physicalDeformitiesOrPeculiarities', ir.physical_deformities_or_peculiarities, 'physicalDeformities', ir.physical_deformities, 'physicalEar', ir.physical_ear, 'physicalEyes', ir.physical_eyes, 'physicalFace', ir.physical_face, 'physicalHair', ir.physical_hair, 'physicalHeight', ir.physical_height, 'physicalIdentificationMarks', ir.physical_identification_marks, 'physicalLanguageOrDialect', ir.physical_language_or_dialect, 'physicalLeucoderma', ir.physical_leucoderma, 'physicalMole', ir.physical_mole, 'physicalMustache', ir.physical_mustache, 'physicalNose', ir.physical_nose, 'physicalScar', ir.physical_scar, 'physicalTattoo', ir.physical_tattoo, 'physicalTeeth', ir.physical_teeth) || jsonb_build_object('socioLivingStatus', ir.socio_living_status, 'socioMaritalStatus', ir.socio_marital_status, 'socioEducation', ir.socio_education, 'socioOccupation', ir.socio_occupation, 'socioIncomeGroup', ir.socio_income_group, 'offenceTime', ir.offence_time, 'otherOffenceTime', ir.other_offence_time, 'shareOfAmountSpent', ir.share_of_amount_spent, 'otherShareOfAmountSpent', ir.other_share_of_amount_spent, 'shareRemarks', ir.share_remarks, 'isInJail', ir.is_in_jail, 'fromWhereSentInJail', ir.from_where_sent_in_jail, 'inJailCrimeNum', ir.in_jail_crime_num, 'inJailDistUnit', ir.in_jail_dist_unit, 'isOnBail', ir.is_on_bail, 'fromWhereSentOnBail', ir.from_where_sent_on_bail, 'onBailCrimeNum', ir.on_bail_crime_num, 'dateOfBail', ir.date_of_bail, 'isAbsconding', ir.is_absconding, 'wantedInPoliceStation', ir.wanted_in_police_station, 'abscondingCrimeNum', ir.absconding_crime_num, 'isNormalLife', ir.is_normal_life, 'ekingLivelihoodByLaborWork', ir.eking_livelihood_by_labor_work, 'isRehabilitated', ir.is_rehabilitated, 'rehabilitationDetails', ir.rehabilitation_details, 'isDead', ir.is_dead, 'deathDetails', ir.death_details, 'isFacingTrial', ir.is_facing_trial, 'facingTrialPsName', ir.facing_trial_ps_name, 'facingTrialCrimeNum', ir.facing_trial_crime_num, 'otherRegularHabits', ir.other_regular_habits, 'otherIndulgenceBeforeOffence', ir.other_indulgence_before_offence, 'timeSinceModusOperandi', ir.time_since_modus_operandi, 'dateCreated', ir.date_created, 'dateModified', ir.date_modified, 'value', ( SELECT p.full_name
                   FROM public.persons p
                  WHERE ((p.person_id)::text = (ir.person_id)::text)
                 LIMIT 1))) || jsonb_build_object('associateDetails', ( SELECT COALESCE(jsonb_agg(jsonb_build_object('id', ad.id, 'personId', ad.person_id, 'gang', ad.gang, 'relation', ad.relation, 'value', ( SELECT p2.full_name
                           FROM public.persons p2
                          WHERE ((p2.person_id)::text = (ad.person_id)::text)
                         LIMIT 1))), '[]'::jsonb) AS "coalesce"
                   FROM public.ir_associate_details ad
                  WHERE ((ad.interrogation_report_id)::text = (ir.interrogation_report_id)::text)), 'consumerDetails', ( SELECT COALESCE(jsonb_agg(jsonb_build_object('id', cd.id, 'consumerPersonId', cd.consumer_person_id, 'placeOfConsumption', cd.place_of_consumption, 'otherSources', cd.other_sources, 'otherSourcesPhoneNo', cd.other_sources_phone_no, 'aadharCardNumber', cd.aadhar_card_number, 'aadharCardNumberPhoneNo', cd.aadhar_card_number_phone_no, 'value', ( SELECT p2.full_name
                           FROM public.persons p2
                          WHERE ((p2.person_id)::text = (cd.consumer_person_id)::text)
                         LIMIT 1))), '[]'::jsonb) AS "coalesce"
                   FROM public.ir_consumer_details cd
                  WHERE ((cd.interrogation_report_id)::text = (ir.interrogation_report_id)::text)), 'defenceCounsel', ( SELECT COALESCE(jsonb_agg(jsonb_build_object('id', dc.id, 'distDivision', dc.dist_division, 'psCode', dc.ps_code, 'crimeNum', dc.crime_num, 'lawSection', dc.law_section, 'scCcNum', dc.sc_cc_num, 'defenceCounselAddress', dc.defence_counsel_address, 'defenceCounselPhone', dc.defence_counsel_phone, 'assistance', dc.assistance, 'defenceCounselPersonId', dc.defence_counsel_person_id, 'value', ( SELECT p2.full_name
                           FROM public.persons p2
                          WHERE ((p2.person_id)::text = (dc.defence_counsel_person_id)::text)
                         LIMIT 1))), '[]'::jsonb) AS "coalesce"
                   FROM public.ir_defence_counsel dc
                  WHERE ((dc.interrogation_report_id)::text = (ir.interrogation_report_id)::text)), 'dopamsLinks', ( SELECT COALESCE(jsonb_agg(jsonb_build_object('id', dl.id, 'phoneNumber', dl.phone_number, 'dopamsData', dl.dopams_data)), '[]'::jsonb) AS "coalesce"
                   FROM public.ir_dopams_links dl
                  WHERE ((dl.interrogation_report_id)::text = (ir.interrogation_report_id)::text)), 'familyHistory', ( SELECT COALESCE(jsonb_agg(jsonb_build_object('id', fh.id, 'personId', fh.person_id, 'relation', fh.relation, 'familyMemberPeculiarity', fh.family_member_peculiarity, 'criminalBackground', fh.criminal_background, 'isAlive', fh.is_alive, 'familyStayTogether', fh.family_stay_together, 'value', ( SELECT p2.full_name
                           FROM public.persons p2
                          WHERE ((p2.person_id)::text = (fh.person_id)::text)
                         LIMIT 1))), '[]'::jsonb) AS "coalesce"
                   FROM public.ir_family_history fh
                  WHERE ((fh.interrogation_report_id)::text = (ir.interrogation_report_id)::text)), 'financialHistory', ( SELECT COALESCE(jsonb_agg(jsonb_build_object('id', fi.id, 'accountHolderPersonId', fi.account_holder_person_id, 'panNo', fi.pan_no, 'upiId', fi.upi_id, 'nameOfBank', fi.name_of_bank, 'accountNumber', fi.account_number, 'branchName', fi.branch_name, 'ifscCode', fi.ifsc_code, 'immovablePropertyAcquired', fi.immovable_property_acquired, 'movablePropertyAcquired', fi.movable_property_acquired, 'value', ( SELECT p2.full_name
                           FROM public.persons p2
                          WHERE ((p2.person_id)::text = (fi.account_holder_person_id)::text)
                         LIMIT 1))), '[]'::jsonb) AS "coalesce"
                   FROM public.ir_financial_history fi
                  WHERE ((fi.interrogation_report_id)::text = (ir.interrogation_report_id)::text)), 'localContacts', ( SELECT COALESCE(jsonb_agg(jsonb_build_object('id', lc.id, 'personId', lc.person_id, 'town', lc.town, 'address', lc.address, 'jurisdictionPs', lc.jurisdiction_ps, 'value', ( SELECT p2.full_name
                           FROM public.persons p2
                          WHERE ((p2.person_id)::text = (lc.person_id)::text)
                         LIMIT 1))), '[]'::jsonb) AS "coalesce"
                   FROM public.ir_local_contacts lc
                  WHERE ((lc.interrogation_report_id)::text = (ir.interrogation_report_id)::text)), 'modusOperandi', ( SELECT COALESCE(jsonb_agg(jsonb_build_object('id', mo2.id, 'crimeHead', mo2.crime_head, 'crimeSubHead', mo2.crime_sub_head, 'modusOperandi', mo2.modus_operandi)), '[]'::jsonb) AS "coalesce"
                   FROM public.ir_modus_operandi mo2
                  WHERE ((mo2.interrogation_report_id)::text = (ir.interrogation_report_id)::text)), 'previousOffencesConfessed', ( SELECT COALESCE(jsonb_agg(jsonb_build_object('id', po.id, 'arrestDate', po.arrest_date, 'arrestedBy', po.arrested_by, 'arrestPlace', po.arrest_place, 'crimeNum', po.crime_num, 'distUnitDivision', po.dist_unit_division, 'gangMember', po.gang_member, 'interrogatedBy', po.interrogated_by, 'lawSection', po.law_section, 'othersIdentify', po.others_identify, 'propertyRecovered', po.property_recovered, 'propertyStolen', po.property_stolen, 'psCode', po.ps_code, 'remarks', po.remarks) ORDER BY po.arrest_date DESC), '[]'::jsonb) AS "coalesce"
                   FROM public.ir_previous_offences_confessed po
                  WHERE ((po.interrogation_report_id)::text = (ir.interrogation_report_id)::text)), 'regularHabits', ( SELECT COALESCE(jsonb_agg(jsonb_build_object('id', rh.id, 'habit', rh.habit)), '[]'::jsonb) AS "coalesce"
                   FROM public.ir_regular_habits rh
                  WHERE ((rh.interrogation_report_id)::text = (ir.interrogation_report_id)::text)), 'shelter', ( SELECT COALESCE(jsonb_agg(jsonb_build_object('id', sh.id, 'preparationOfOffence', sh.preparation_of_offence, 'afterOffence', sh.after_offence, 'regularResidency', sh.regular_residency, 'remarks', sh.remarks, 'otherRegularResidency', sh.other_regular_residency)), '[]'::jsonb) AS "coalesce"
                   FROM public.ir_shelter sh
                  WHERE ((sh.interrogation_report_id)::text = (ir.interrogation_report_id)::text)), 'simDetails', ( SELECT COALESCE(jsonb_agg(jsonb_build_object('id', sd.id, 'phoneNumber', sd.phone_number, 'sdr', sd.sdr, 'imei', sd.imei, 'trueCallerName', sd.true_caller_name, 'personId', sd.person_id, 'value', ( SELECT p2.full_name
                           FROM public.persons p2
                          WHERE ((p2.person_id)::text = (sd.person_id)::text)
                         LIMIT 1))), '[]'::jsonb) AS "coalesce"
                   FROM public.ir_sim_details sd
                  WHERE ((sd.interrogation_report_id)::text = (ir.interrogation_report_id)::text)), 'typesOfDrugs', ( SELECT COALESCE(jsonb_agg(jsonb_build_object('id', td.id, 'typeOfDrug', td.type_of_drug, 'quantity', td.quantity, 'purchaseAmountInInr', td.purchase_amount_in_inr, 'modeOfPayment', td.mode_of_payment, 'modeOfTransport', td.mode_of_transport, 'supplierPersonId', td.supplier_person_id, 'receiversPersonId', td.receivers_person_id, 'supplierValue', ( SELECT p2.full_name
                           FROM public.persons p2
                          WHERE ((p2.person_id)::text = (td.supplier_person_id)::text)
                         LIMIT 1), 'receiverValue', ( SELECT p2.full_name
                           FROM public.persons p2
                          WHERE ((p2.person_id)::text = (td.receivers_person_id)::text)
                         LIMIT 1))), '[]'::jsonb) AS "coalesce"
                   FROM public.ir_types_of_drugs td
                  WHERE ((td.interrogation_report_id)::text = (ir.interrogation_report_id)::text)))) ORDER BY ir.date_created) AS jsonb_agg
           FROM public.interrogation_reports ir
          WHERE ((ir.crime_id)::text = (c.crime_id)::text)) AS "irDetails"
   FROM (public.crimes c
     JOIN public.hierarchy h ON (((h.ps_code)::text = (c.ps_code)::text)))
  WITH NO DATA;
