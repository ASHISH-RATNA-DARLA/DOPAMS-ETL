CREATE MATERIALIZED VIEW public.criminal_profiles_mv AS

-- Branch A/B: persons who exist in the persons table and have a bfai entry
 SELECT (p.person_id)::text AS id,
    p.alias,
    p.name,
    p.surname,
    p.full_name AS "fullName",
    p.relation_type AS "relationType",
    p.relative_name AS "relativeName",
    p.gender,
    p.is_died AS "isDied",
    p.date_of_birth AS "dateOfBirth",
    p.age,
    p.domicile_classification AS domicile,
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
    ( SELECT jsonb_agg(DISTINCT jsonb_build_object('id', f.id, 'identityType', f.identity_type, 'identityNumber', f.identity_number, 'filePath', f.file_path, 'fileUrl', f.file_url)) AS jsonb_agg
           FROM public.files f
          WHERE (((f.parent_id)::text = (p.person_id)::text) AND (f.source_type = 'person'::public.source_type_enum) AND (f.source_field = 'IDENTITY_DETAILS'::public.source_field_enum) AND (f.is_downloaded = true) AND (f.file_url IS NOT NULL))) AS "identityDocuments",
    ( SELECT jsonb_agg(DISTINCT jsonb_build_object('id', f.id, 'filePath', f.file_path, 'fileUrl', f.file_url)) AS jsonb_agg
           FROM public.files f
          WHERE (((f.parent_id)::text = (p.person_id)::text) AND (f.source_type = 'person'::public.source_type_enum) AND (f.source_field = 'MEDIA'::public.source_field_enum) AND (f.is_downloaded = true) AND (f.file_url IS NOT NULL))) AS documents,
    ( SELECT jsonb_agg(sub.crime_data) AS jsonb_agg
           FROM ( SELECT DISTINCT ON (c.crime_id) jsonb_build_object('id', c.crime_id, 'firNumber', c.fir_num, 'crimeRegDate', c.fir_date, 'accusedType', bfa.accused_type, 'accusedStatus',
                        CASE
                            WHEN ((bfa.status ~~* 'Arrest%'::text) AND (bfa.status !~~* 'Arrest Related%'::text)) THEN 'Arrested'::text
                            WHEN (bfa.status ~~* 'Surrendered%'::text) THEN 'Arrested'::text
                            WHEN (bfa.status ~~* 'Absconding'::text) THEN 'Absconding'::text
                            WHEN (bfa.status ~~* 'Arrest Related/41A CrPC Pending'::text) THEN 'Absconding'::text
                            WHEN (bfa.status ~~* '41A Cr.P.C%'::text) THEN 'Issued Notice'::text
                            WHEN (bfa.status ~~* 'High court directions%'::text) THEN 'Issued Notice'::text
                            ELSE 'Unknown'::text
                        END) AS crime_data
                   FROM (public.brief_facts_ai_accused_flat bfa
                     JOIN public.crimes c ON (((bfa.crime_id)::text = (c.crime_id)::text)))
                  WHERE bfa.canonical_person_id IS NOT NULL
                    AND bfa.canonical_person_id = bfa_canon.canonical_person_id
                  ORDER BY c.crime_id, bfa.date_created DESC NULLS LAST) sub) AS crimes,
    ( SELECT c.crime_id
           FROM (public.brief_facts_ai_accused_flat bfa
             JOIN public.crimes c ON (((bfa.crime_id)::text = (c.crime_id)::text)))
          WHERE bfa.canonical_person_id IS NOT NULL
            AND bfa.canonical_person_id = bfa_canon.canonical_person_id
          ORDER BY c.fir_date DESC
         LIMIT 1) AS "latestCrimeId",
    ( SELECT c.fir_num
           FROM (public.brief_facts_ai_accused_flat bfa
             JOIN public.crimes c ON (((bfa.crime_id)::text = (c.crime_id)::text)))
          WHERE bfa.canonical_person_id IS NOT NULL
            AND bfa.canonical_person_id = bfa_canon.canonical_person_id
          ORDER BY c.fir_date DESC
         LIMIT 1) AS "latestCrimeNo",
    ( SELECT count(DISTINCT bfa.crime_id) AS count
           FROM public.brief_facts_ai_accused_flat bfa
          WHERE bfa.canonical_person_id IS NOT NULL
            AND bfa.canonical_person_id = bfa_canon.canonical_person_id) AS "noOfCrimes",
    ( SELECT count(*) AS count
           FROM public.arrests arr
          WHERE (((arr.person_id)::text = (p.person_id)::text) AND (arr.is_arrested = true))) AS "arrestCount",
    ( SELECT max(c.fir_date) AS max
           FROM (public.brief_facts_ai_accused_flat bfa
             JOIN public.crimes c ON (((bfa.crime_id)::text = (c.crime_id)::text)))
          WHERE bfa.canonical_person_id IS NOT NULL
            AND bfa.canonical_person_id = bfa_canon.canonical_person_id
            AND (((bfa.status ~~* 'Arrest%'::text) AND (bfa.status !~~* 'Arrest Related%'::text)) OR (bfa.status ~~* 'Surrendered%'::text))) AS "lastArrestDate",
    ( SELECT jsonb_agg(DISTINCT jsonb_build_object('crimeId', bfa.crime_id, 'accusedId', bfa.accused_id, 'accusedRole', bfa.accused_type)) AS jsonb_agg
           FROM public.brief_facts_ai_accused_flat bfa
          WHERE bfa.canonical_person_id IS NOT NULL
            AND bfa.canonical_person_id = bfa_canon.canonical_person_id) AS "crimesInvolved",
    ( SELECT array_agg(DISTINCT bfa.accused_type) FILTER (WHERE (bfa.accused_type IS NOT NULL)) AS array_agg
           FROM public.brief_facts_ai_accused_flat bfa
          WHERE bfa.canonical_person_id IS NOT NULL
            AND bfa.canonical_person_id = bfa_canon.canonical_person_id) AS "accusedRoles",
    ( SELECT jsonb_agg(DISTINCT jsonb_build_object('id', c.crime_id, 'value', c.fir_num)) AS jsonb_agg
           FROM (public.brief_facts_ai_accused_flat bfa
             JOIN public.crimes c ON (((bfa.crime_id)::text = (c.crime_id)::text)))
          WHERE bfa.canonical_person_id IS NOT NULL
            AND bfa.canonical_person_id = bfa_canon.canonical_person_id) AS "previouslyInvolvedCases",
    ( SELECT COALESCE(array_agg(DISTINCT upper(TRIM(BOTH FROM bfd.primary_drug_name))) FILTER (WHERE ((bfd.primary_drug_name IS NOT NULL) AND (bfd.primary_drug_name <> 'NO_DRUGS_DETECTED'::text))), ARRAY[]::text[]) AS "coalesce"
           FROM (public.brief_facts_ai_accused_flat bfa_d
             JOIN public.brief_facts_ai_drug_flat bfd ON (((bfd.crime_id)::text = (bfa_d.crime_id)::text)))
          WHERE bfa_d.canonical_person_id IS NOT NULL
            AND bfa_d.canonical_person_id = bfa_canon.canonical_person_id) AS "associatedDrugs",
    ARRAY[]::text[] AS "DOPAMSLinks",
    NULL::text AS counselled,
    ARRAY[]::text[] AS "socialMedia",
    NULL::text AS "RTAData",
    NULL::text AS "bankAccountDetails",
    NULL::text AS "passportDetails_Foreigners",
    NULL::text AS "purposeOfVISA_Foreigners",
    NULL::text AS "validityOfVISA_Foreigners",
    NULL::text AS "localaddress_Foreigners",
    NULL::text AS "nativeAddress_Foreigners",
    NULL::text AS "statusOfTheAccused",
    NULL::text AS "historySheet",
    NULL::text AS "propertyForfeited",
    NULL::text AS "PITNDPSInitiated"
   FROM public.persons p
   LEFT JOIN LATERAL (
     SELECT canonical_person_id
     FROM public.brief_facts_ai
     WHERE (person_id)::text = (p.person_id)::text
       AND canonical_person_id IS NOT NULL
     LIMIT 1
   ) bfa_canon ON TRUE
  WHERE (EXISTS ( SELECT 1
           FROM public.brief_facts_ai_accused_flat bfa
          WHERE ((bfa.person_id)::text = (p.person_id)::text)))

UNION ALL

-- Branch C: AI-extracted persons not registered in persons table (person_id IS NULL)
 SELECT (bfa.accused_id)::text AS id,
    bfa.alias_name AS alias,
    bfa.full_name AS name,
    NULL::character varying AS surname,
    bfa.full_name AS "fullName",
    NULL::character varying AS "relationType",
    NULL::character varying AS "relativeName",
    NULL::character varying AS gender,
    NULL::boolean AS "isDied",
    NULL::date AS "dateOfBirth",
    NULL::integer AS age,
    NULL::character varying AS domicile,
    NULL::character varying AS occupation,
    NULL::character varying AS "educationQualification",
    NULL::character varying AS caste,
    NULL::character varying AS "subCaste",
    NULL::character varying AS religion,
    NULL::character varying AS nationality,
    NULL::character varying AS designation,
    NULL::character varying AS "placeOfWork",
    NULL::character varying AS "presentHouseNo",
    NULL::character varying AS "presentStreetRoadNo",
    NULL::character varying AS "presentWardColony",
    NULL::character varying AS "presentLandmarkMilestone",
    NULL::character varying AS "presentLocalityVillage",
    NULL::character varying AS "presentAreaMandal",
    NULL::character varying AS "presentDistrict",
    NULL::character varying AS "presentStateUt",
    NULL::character varying AS "presentCountry",
    NULL::character varying AS "presentResidencyType",
    NULL::character varying AS "presentPinCode",
    NULL::character varying AS "presentJurisdictionPs",
    NULL::character varying AS "permanentHouseNo",
    NULL::character varying AS "permanentStreetRoadNo",
    NULL::character varying AS "permanentWardColony",
    NULL::character varying AS "permanentLandmarkMilestone",
    NULL::character varying AS "permanentLocalityVillage",
    NULL::character varying AS "permanentAreaMandal",
    NULL::character varying AS "permanentDistrict",
    NULL::character varying AS "permanentStateUt",
    NULL::character varying AS "permanentCountry",
    NULL::character varying AS "permanentResidencyType",
    NULL::character varying AS "permanentPinCode",
    NULL::character varying AS "permanentJurisdictionPs",
    NULL::character varying AS "phoneNumber",
    NULL::character varying AS "countryCode",
    NULL::character varying AS "emailId",
    NULL::jsonb AS "identityDocuments",
    NULL::jsonb AS documents,
    ( SELECT jsonb_agg(sub.crime_data) AS jsonb_agg
           FROM ( SELECT DISTINCT ON (c.crime_id) jsonb_build_object('id', c.crime_id, 'firNumber', c.fir_num, 'crimeRegDate', c.fir_date, 'accusedType', bfa2.accused_type, 'accusedStatus',
                        CASE
                            WHEN ((bfa2.status ~~* 'Arrest%'::text) AND (bfa2.status !~~* 'Arrest Related%'::text)) THEN 'Arrested'::text
                            WHEN (bfa2.status ~~* 'Surrendered%'::text) THEN 'Arrested'::text
                            WHEN (bfa2.status ~~* 'Absconding'::text) THEN 'Absconding'::text
                            WHEN (bfa2.status ~~* 'Arrest Related/41A CrPC Pending'::text) THEN 'Absconding'::text
                            WHEN (bfa2.status ~~* '41A Cr.P.C%'::text) THEN 'Issued Notice'::text
                            WHEN (bfa2.status ~~* 'High court directions%'::text) THEN 'Issued Notice'::text
                            ELSE 'Unknown'::text
                        END) AS crime_data
                   FROM (public.brief_facts_ai_accused_flat bfa2
                     JOIN public.crimes c ON (((bfa2.crime_id)::text = (c.crime_id)::text)))
                  WHERE bfa2.canonical_person_id IS NOT NULL
                    AND bfa2.canonical_person_id = bfa.canonical_person_id
                  ORDER BY c.crime_id, bfa2.date_created DESC NULLS LAST) sub) AS crimes,
    ( SELECT c.crime_id
           FROM (public.brief_facts_ai_accused_flat bfa2
             JOIN public.crimes c ON (((bfa2.crime_id)::text = (c.crime_id)::text)))
          WHERE bfa2.canonical_person_id IS NOT NULL
            AND bfa2.canonical_person_id = bfa.canonical_person_id
          ORDER BY c.fir_date DESC
         LIMIT 1) AS "latestCrimeId",
    ( SELECT c.fir_num
           FROM (public.brief_facts_ai_accused_flat bfa2
             JOIN public.crimes c ON (((bfa2.crime_id)::text = (c.crime_id)::text)))
          WHERE bfa2.canonical_person_id IS NOT NULL
            AND bfa2.canonical_person_id = bfa.canonical_person_id
          ORDER BY c.fir_date DESC
         LIMIT 1) AS "latestCrimeNo",
    ( SELECT count(DISTINCT bfa2.crime_id) AS count
           FROM public.brief_facts_ai_accused_flat bfa2
          WHERE bfa2.canonical_person_id IS NOT NULL
            AND bfa2.canonical_person_id = bfa.canonical_person_id) AS "noOfCrimes",
    0::bigint AS "arrestCount",
    ( SELECT max(c.fir_date) AS max
           FROM (public.brief_facts_ai_accused_flat bfa2
             JOIN public.crimes c ON (((bfa2.crime_id)::text = (c.crime_id)::text)))
          WHERE bfa2.canonical_person_id IS NOT NULL
            AND bfa2.canonical_person_id = bfa.canonical_person_id
            AND (((bfa2.status ~~* 'Arrest%'::text) AND (bfa2.status !~~* 'Arrest Related%'::text)) OR (bfa2.status ~~* 'Surrendered%'::text))) AS "lastArrestDate",
    ( SELECT jsonb_agg(DISTINCT jsonb_build_object('crimeId', bfa2.crime_id, 'accusedId', bfa2.accused_id, 'accusedRole', bfa2.accused_type)) AS jsonb_agg
           FROM public.brief_facts_ai_accused_flat bfa2
          WHERE bfa2.canonical_person_id IS NOT NULL
            AND bfa2.canonical_person_id = bfa.canonical_person_id) AS "crimesInvolved",
    ( SELECT array_agg(DISTINCT bfa2.accused_type) FILTER (WHERE (bfa2.accused_type IS NOT NULL)) AS array_agg
           FROM public.brief_facts_ai_accused_flat bfa2
          WHERE bfa2.canonical_person_id IS NOT NULL
            AND bfa2.canonical_person_id = bfa.canonical_person_id) AS "accusedRoles",
    ( SELECT jsonb_agg(DISTINCT jsonb_build_object('id', c.crime_id, 'value', c.fir_num)) AS jsonb_agg
           FROM (public.brief_facts_ai_accused_flat bfa2
             JOIN public.crimes c ON (((bfa2.crime_id)::text = (c.crime_id)::text)))
          WHERE bfa2.canonical_person_id IS NOT NULL
            AND bfa2.canonical_person_id = bfa.canonical_person_id) AS "previouslyInvolvedCases",
    ( SELECT COALESCE(array_agg(DISTINCT upper(TRIM(BOTH FROM bfd.primary_drug_name))) FILTER (WHERE ((bfd.primary_drug_name IS NOT NULL) AND (bfd.primary_drug_name <> 'NO_DRUGS_DETECTED'::text))), ARRAY[]::text[]) AS "coalesce"
           FROM (public.brief_facts_ai_accused_flat bfa_d
             JOIN public.brief_facts_ai_drug_flat bfd ON (((bfd.crime_id)::text = (bfa_d.crime_id)::text)))
          WHERE bfa_d.canonical_person_id IS NOT NULL
            AND bfa_d.canonical_person_id = bfa.canonical_person_id) AS "associatedDrugs",
    ARRAY[]::text[] AS "DOPAMSLinks",
    NULL::text AS counselled,
    ARRAY[]::text[] AS "socialMedia",
    NULL::text AS "RTAData",
    NULL::text AS "bankAccountDetails",
    NULL::text AS "passportDetails_Foreigners",
    NULL::text AS "purposeOfVISA_Foreigners",
    NULL::text AS "validityOfVISA_Foreigners",
    NULL::text AS "localaddress_Foreigners",
    NULL::text AS "nativeAddress_Foreigners",
    NULL::text AS "statusOfTheAccused",
    NULL::text AS "historySheet",
    NULL::text AS "propertyForfeited",
    NULL::text AS "PITNDPSInitiated"
   FROM public.brief_facts_ai_accused_flat bfa
  WHERE (bfa.person_id IS NULL)

  WITH NO DATA;
