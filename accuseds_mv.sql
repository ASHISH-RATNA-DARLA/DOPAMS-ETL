CREATE MATERIALIZED VIEW public.accuseds_mv AS

-- Part 1: crimes already processed by bfai ETL
 SELECT bfa.accused_id AS id,
    h.dist_name AS unit,
    h.ps_name AS ps,
    (EXTRACT(year FROM c.fir_date))::integer AS year,
    c.crime_id AS "crimeId",
    bfa.person_id AS "personId",
    c.fir_num AS "firNumber",
    c.fir_reg_num AS "firRegNum",
    c.acts_sections AS section,
    c.fir_date AS "crimeRegDate",
    c.brief_facts AS "briefFacts",
    bfa.person_code AS "accusedCode",
    bfa.accused_type AS "accusedRole",
    a.seq_num AS "seqNum",
    a.is_ccl AS "isCCL",
    a.beard,
    a.build,
    a.color,
    a.ear,
    a.eyes,
    a.face,
    a.hair,
    a.height,
    a.leucoderma,
    a.mole,
    a.mustache,
    a.nose,
    a.teeth,
        CASE
            WHEN ((COALESCE(bfa.status, a.accused_status) ~~* 'Arrest%'::text) AND (COALESCE(bfa.status, a.accused_status) !~~* 'Arrest Related%'::text)) THEN 'Arrested'::text
            WHEN (COALESCE(bfa.status, a.accused_status) ~~* 'Surrendered%'::text) THEN 'Arrested'::text
            WHEN (COALESCE(bfa.status, a.accused_status) ~~* 'Absconding'::text) THEN 'Absconding'::text
            WHEN (COALESCE(bfa.status, a.accused_status) ~~* 'Arrest Related/41A CrPC Pending'::text) THEN 'Absconding'::text
            WHEN (COALESCE(bfa.status, a.accused_status) ~~* '41A Cr.P.C%'::text) THEN 'Issued Notice'::text
            WHEN (COALESCE(bfa.status, a.accused_status) ~~* 'High court directions%'::text) THEN 'Issued Notice'::text
            ELSE 'Unknown'::text
        END AS "accusedStatus",
    COALESCE(bfa.status, a.accused_status) AS "accusedStatusRaw",
    a.type AS "accusedType",
    ( SELECT count(*) AS count
           FROM public.brief_facts_ai_accused_flat bfa3
          WHERE ((bfa3.crime_id)::text = (c.crime_id)::text)
            AND (bfa3.accused_id IS NOT NULL OR bfa3.full_name IS NOT NULL)) AS "noOfAccusedInvolved",
    ( SELECT jsonb_agg(jsonb_build_object('name', COALESCE(p2.name, bfa2.full_name), 'surname', p2.surname, 'alias', COALESCE(p2.alias, bfa2.alias_name), 'fullName', COALESCE(p2.full_name, bfa2.full_name), 'status',
                CASE
                    WHEN ((COALESCE(bfa2.status, a2.accused_status) ~~* 'Arrest%'::text) AND (COALESCE(bfa2.status, a2.accused_status) !~~* 'Arrest Related%'::text)) THEN 'Arrested'::text
                    WHEN (COALESCE(bfa2.status, a2.accused_status) ~~* 'Surrendered%'::text) THEN 'Arrested'::text
                    WHEN (COALESCE(bfa2.status, a2.accused_status) ~~* 'Absconding'::text) THEN 'Absconding'::text
                    WHEN (COALESCE(bfa2.status, a2.accused_status) ~~* 'Arrest Related/41A CrPC Pending'::text) THEN 'Absconding'::text
                    WHEN (COALESCE(bfa2.status, a2.accused_status) ~~* '41A Cr.P.C%'::text) THEN 'Issued Notice'::text
                    WHEN (COALESCE(bfa2.status, a2.accused_status) ~~* 'High court directions%'::text) THEN 'Issued Notice'::text
                    ELSE 'Unknown'::text
                END, 'email', p2.email_id)) AS jsonb_agg
           FROM ((public.brief_facts_ai_accused_flat bfa2
             LEFT JOIN public.persons p2 ON (((bfa2.person_id)::text = (p2.person_id)::text)))
             LEFT JOIN public.accused a2 ON (((bfa2.accused_id)::text = (a2.accused_id)::text)))
          WHERE ((bfa2.crime_id)::text = (c.crime_id)::text)
            AND (bfa2.accused_id IS NOT NULL OR bfa2.full_name IS NOT NULL)) AS "accusedDetails",
    p.name,
    p.surname,
    p.alias,
    p.full_name AS "fullName",
    p.relative_name AS parentage,
    p.domicile_classification AS domicile,
    p.relation_type AS "relationType",
    p.gender,
    p.is_died AS "isDied",
    p.date_of_birth AS "dateOfBirth",
    p.age,
    p.occupation,
    p.education_qualification AS "educationQualification",
    p.caste,
    p.sub_caste AS "subCaste",
    p.religion,
    p.nationality,
    p.designation,
    p.place_of_work AS "placeOfWork",
    p.present_house_no AS "presentHouseNo",
    p.present_street_road_no AS "presentStreetRoadNo",
    p.present_ward_colony AS "presentWardColony",
    p.present_landmark_milestone AS "presentLandmarkMilestone",
    p.present_locality_village AS "presentLocalityVillage",
    p.present_area_mandal AS "presentAreaMandal",
    p.present_district AS "presentDistrict",
    p.present_state_ut AS "presentStateUt",
    p.present_country AS "presentCountry",
    p.present_residency_type AS "presentResidencyType",
    p.present_pin_code AS "presentPinCode",
    p.present_jurisdiction_ps AS "presentJurisdictionPs",
    p.permanent_house_no AS "permanentHouseNo",
    p.permanent_street_road_no AS "permanentStreetRoadNo",
    p.permanent_ward_colony AS "permanentWardColony",
    p.permanent_landmark_milestone AS "permanentLandmarkMilestone",
    p.permanent_locality_village AS "permanentLocalityVillage",
    p.permanent_area_mandal AS "permanentAreaMandal",
    p.permanent_district AS "permanentDistrict",
    p.permanent_state_ut AS "permanentStateUt",
    p.permanent_country AS "permanentCountry",
    p.permanent_residency_type AS "permanentResidencyType",
    p.permanent_pin_code AS "permanentPinCode",
    p.permanent_jurisdiction_ps AS "permanentJurisdictionPs",
    p.phone_number AS "phoneNumber",
    p.country_code AS "countryCode",
    p.email_id AS "emailId",
    concat_ws(', '::text, NULLIF((p.present_house_no)::text, ''::text), NULLIF((p.present_street_road_no)::text, ''::text), NULLIF((p.present_ward_colony)::text, ''::text), NULLIF((p.present_locality_village)::text, ''::text), NULLIF((p.present_district)::text, ''::text), NULLIF((p.present_state_ut)::text, ''::text), NULLIF((p.present_pin_code)::text, ''::text)) AS "presentAddress",
    concat_ws(', '::text, NULLIF((p.permanent_house_no)::text, ''::text), NULLIF((p.permanent_street_road_no)::text, ''::text), NULLIF((p.permanent_ward_colony)::text, ''::text), NULLIF((p.permanent_locality_village)::text, ''::text), NULLIF((p.permanent_district)::text, ''::text), NULLIF((p.permanent_state_ut)::text, ''::text), NULLIF((p.permanent_pin_code)::text, ''::text)) AS "permanentAddress",
    ( SELECT count(DISTINCT bfa_c.crime_id) AS count
           FROM public.brief_facts_ai_accused_flat bfa_c
          WHERE bfa_c.canonical_person_id IS NOT NULL
            AND bfa_c.canonical_person_id = bfa.canonical_person_id) AS "noOfCrimes",
    ( SELECT jsonb_agg(DISTINCT jsonb_build_object('crimeId', c2.crime_id, 'firNumber', c2.fir_num)) AS jsonb_agg
           FROM (public.brief_facts_ai_accused_flat bfa4
             JOIN public.crimes c2 ON (((bfa4.crime_id)::text = (c2.crime_id)::text)))
          WHERE bfa4.canonical_person_id IS NOT NULL
            AND bfa4.canonical_person_id = bfa.canonical_person_id
            AND (bfa4.crime_id)::text <> (c.crime_id)::text) AS "previouslyInvolvedCases",
    ( SELECT COALESCE(array_agg(DISTINCT upper(TRIM(BOTH FROM bfd.primary_drug_name))) FILTER (WHERE ((bfd.primary_drug_name IS NOT NULL) AND (bfd.primary_drug_name <> 'NO_DRUGS_DETECTED'::text))), ARRAY[]::text[]) AS "coalesce"
           FROM public.brief_facts_ai_drug_flat bfd
          WHERE ((bfd.crime_id)::text = (c.crime_id)::text)) AS "drugType",
    ( SELECT jsonb_agg(jsonb_build_object('name', bfd2.primary_drug_name, 'quantity',
                CASE
                    WHEN (bfd2.weight_kg >= (1)::numeric) THEN concat(round(bfd2.weight_kg, 3), ' Kg')
                    WHEN (bfd2.weight_g > (0)::numeric) THEN concat(round(bfd2.weight_g, 2), ' g')
                    WHEN (bfd2.volume_l >= (1)::numeric) THEN concat(round(bfd2.volume_l, 3), ' L')
                    WHEN (bfd2.volume_ml > (0)::numeric) THEN concat(round(bfd2.volume_ml, 2), ' ml')
                    WHEN (bfd2.count_total > (0)::numeric) THEN concat(bfd2.count_total, ' Units')
                    ELSE 'N/A'::text
                END, 'worth', COALESCE(bfd2.seizure_worth, (0)::numeric)) ORDER BY bfd2.created_at) AS jsonb_agg
           FROM public.brief_facts_ai_drug_flat bfd2
          WHERE ((bfd2.crime_id)::text = (c.crime_id)::text)) AS "drugWithQuantity",
    c.class_classification AS "caseClassification",
    c.case_status AS "caseStatus",
    ( SELECT jsonb_agg(DISTINCT jsonb_build_object('id', d.id, 'disposalType', d.disposal_type)) AS jsonb_agg
           FROM public.disposal d
          WHERE ((d.crime_id)::text = (c.crime_id)::text)) AS "disposalDetails"
   FROM ((((public.brief_facts_ai_accused_flat bfa
     JOIN public.crimes c ON (((bfa.crime_id)::text = (c.crime_id)::text)))
     JOIN (SELECT DISTINCT ON (ps_code) * FROM public.hierarchy ORDER BY ps_code, date_modified DESC NULLS LAST) h ON (((c.ps_code)::text = (h.ps_code)::text)))
     LEFT JOIN public.accused a ON (((bfa.accused_id)::text = (a.accused_id)::text)))
     LEFT JOIN public.persons p ON (((bfa.person_id)::text = (p.person_id)::text)))
  WHERE (bfa.accused_id IS NOT NULL OR bfa.full_name IS NOT NULL)

  WITH NO DATA;
