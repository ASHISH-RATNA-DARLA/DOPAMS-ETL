CREATE MATERIALIZED VIEW public.criminal_profiles_mv AS
 SELECT person_id AS id,
    alias,
    name,
    surname,
    full_name AS "fullName",
    relation_type AS "relationType",
    relative_name AS "relativeName",
    gender,
    is_died AS "isDied",
    date_of_birth AS "dateOfBirth",
    age,
    domicile_classification AS domicile,
    occupation,
    education_qualification AS "educationQualification",
    caste,
    sub_caste AS "subCaste",
    religion,
    nationality,
    designation,
    place_of_work AS "placeOfWork",
    present_house_no AS "presentHouseNo",
    present_street_road_no AS "presentStreetRoadNo",
    present_ward_colony AS "presentWardColony",
    present_landmark_milestone AS "presentLandmarkMilestone",
    present_locality_village AS "presentLocalityVillage",
    present_area_mandal AS "presentAreaMandal",
    present_district AS "presentDistrict",
    present_state_ut AS "presentStateUt",
    present_country AS "presentCountry",
    present_residency_type AS "presentResidencyType",
    present_pin_code AS "presentPinCode",
    present_jurisdiction_ps AS "presentJurisdictionPs",
    permanent_house_no AS "permanentHouseNo",
    permanent_street_road_no AS "permanentStreetRoadNo",
    permanent_ward_colony AS "permanentWardColony",
    permanent_landmark_milestone AS "permanentLandmarkMilestone",
    permanent_locality_village AS "permanentLocalityVillage",
    permanent_area_mandal AS "permanentAreaMandal",
    permanent_district AS "permanentDistrict",
    permanent_state_ut AS "permanentStateUt",
    permanent_country AS "permanentCountry",
    permanent_residency_type AS "permanentResidencyType",
    permanent_pin_code AS "permanentPinCode",
    permanent_jurisdiction_ps AS "permanentJurisdictionPs",
    phone_number AS "phoneNumber",
    country_code AS "countryCode",
    email_id AS "emailId",
    ( SELECT jsonb_agg(DISTINCT jsonb_build_object('id', f.id, 'identityType', f.identity_type, 'identityNumber', f.identity_number, 'filePath', f.file_path, 'fileUrl', f.file_url)) AS jsonb_agg
           FROM public.files f
          WHERE (((f.parent_id)::text = (p.person_id)::text) AND (f.source_type = 'person'::public.source_type_enum) AND (f.source_field = 'IDENTITY_DETAILS'::public.source_field_enum) AND (f.is_downloaded = true) AND (f.file_url IS NOT NULL))) AS "identityDocuments",
    ( SELECT jsonb_agg(DISTINCT jsonb_build_object('id', f.id, 'filePath', f.file_path, 'fileUrl', f.file_url)) AS jsonb_agg
           FROM public.files f
          WHERE (((f.parent_id)::text = (p.person_id)::text) AND (f.source_type = 'person'::public.source_type_enum) AND (f.source_field = 'MEDIA'::public.source_field_enum) AND (f.is_downloaded = true) AND (f.file_url IS NOT NULL))) AS documents,
    ( SELECT jsonb_agg(sub.crime_data) AS jsonb_agg
           FROM ( SELECT DISTINCT ON (c.crime_id) jsonb_build_object('id', c.crime_id, 'firNumber', c.fir_num, 'crimeRegDate', c.fir_date, 'accusedType', bfa.accused_type, 'accusedStatus',
                        CASE
                            WHEN ((COALESCE(bfa.status, a.accused_status) ~~* 'Arrest%'::text) AND (COALESCE(bfa.status, a.accused_status) !~~* 'Arrest Related%'::text)) THEN 'Arrested'::text
                            WHEN (COALESCE(bfa.status, a.accused_status) ~~* 'Surrendered%'::text) THEN 'Arrested'::text
                            WHEN (COALESCE(bfa.status, a.accused_status) ~~* 'Absconding'::text) THEN 'Absconding'::text
                            WHEN (COALESCE(bfa.status, a.accused_status) ~~* 'Arrest Related/41A CrPC Pending'::text) THEN 'Absconding'::text
                            WHEN (COALESCE(bfa.status, a.accused_status) ~~* '41A Cr.P.C%'::text) THEN 'Issued Notice'::text
                            WHEN (COALESCE(bfa.status, a.accused_status) ~~* 'High court directions%'::text) THEN 'Issued Notice'::text
                            ELSE 'Unknown'::text
                        END) AS crime_data
                   FROM ((public.accused a
                     JOIN public.crimes c ON (((a.crime_id)::text = (c.crime_id)::text)))
                     LEFT JOIN public.brief_facts_ai_accused_flat bfa ON (((bfa.accused_id)::text = (a.accused_id)::text)))
                  WHERE ((a.person_id)::text = (p.person_id)::text)
                  ORDER BY c.crime_id, bfa.date_created DESC NULLS LAST) sub) AS crimes,
    ( SELECT c.crime_id
           FROM (public.accused a
             JOIN public.crimes c ON (((a.crime_id)::text = (c.crime_id)::text)))
          WHERE ((a.person_id)::text = (p.person_id)::text)
          ORDER BY c.fir_date DESC
         LIMIT 1) AS "latestCrimeId",
    ( SELECT c.fir_num
           FROM (public.accused a
             JOIN public.crimes c ON (((a.crime_id)::text = (c.crime_id)::text)))
          WHERE ((a.person_id)::text = (p.person_id)::text)
          ORDER BY c.fir_date DESC
         LIMIT 1) AS "latestCrimeNo",
    ( SELECT count(DISTINCT bfa.crime_id) AS count
           FROM (public.accused a
             JOIN public.brief_facts_ai_accused_flat bfa ON (((bfa.accused_id)::text = (a.accused_id)::text)))
          WHERE ((a.person_id)::text = (p.person_id)::text)) AS "noOfCrimes",
    ( SELECT count(*) AS count
           FROM public.arrests arr
          WHERE (((arr.person_id)::text = (p.person_id)::text) AND (arr.is_arrested = true))) AS "arrestCount",
    ( SELECT max(c.fir_date) AS max
           FROM ((public.accused a
             JOIN public.crimes c ON (((a.crime_id)::text = (c.crime_id)::text)))
             LEFT JOIN public.brief_facts_ai_accused_flat bfa ON (((bfa.accused_id)::text = (a.accused_id)::text)))
          WHERE (((a.person_id)::text = (p.person_id)::text) AND (((COALESCE(bfa.status, a.accused_status) ~~* 'Arrest%'::text) AND (COALESCE(bfa.status, a.accused_status) !~~* 'Arrest Related%'::text)) OR (COALESCE(bfa.status, a.accused_status) ~~* 'Surrendered%'::text)))) AS "lastArrestDate",
    ( SELECT jsonb_agg(DISTINCT jsonb_build_object('crimeId', bfa.crime_id, 'accusedId', bfa.accused_id, 'accusedRole', bfa.accused_type)) AS jsonb_agg
           FROM (public.accused a
             JOIN public.brief_facts_ai_accused_flat bfa ON (((bfa.accused_id)::text = (a.accused_id)::text)))
          WHERE ((a.person_id)::text = (p.person_id)::text)) AS "crimesInvolved",
    ( SELECT array_agg(DISTINCT bfa.accused_type) FILTER (WHERE (bfa.accused_type IS NOT NULL)) AS array_agg
           FROM (public.accused a
             JOIN public.brief_facts_ai_accused_flat bfa ON (((bfa.accused_id)::text = (a.accused_id)::text)))
          WHERE ((a.person_id)::text = (p.person_id)::text)) AS "accusedRoles",
    ( SELECT jsonb_agg(DISTINCT jsonb_build_object('id', c.crime_id, 'value', c.fir_num)) AS jsonb_agg
           FROM (public.accused a
             JOIN public.crimes c ON (((a.crime_id)::text = (c.crime_id)::text)))
          WHERE ((a.person_id)::text = (p.person_id)::text)) AS "previouslyInvolvedCases",
    ( SELECT COALESCE(array_agg(DISTINCT upper(TRIM(BOTH FROM bfd.primary_drug_name))) FILTER (WHERE ((bfd.primary_drug_name IS NOT NULL) AND (bfd.primary_drug_name <> 'NO_DRUGS_DETECTED'::text))), ARRAY[]::text[]) AS "coalesce"
           FROM (public.accused a_drug
             JOIN public.brief_facts_ai_drug_flat bfd ON (((bfd.crime_id)::text = (a_drug.crime_id)::text)))
          WHERE ((a_drug.person_id)::text = (p.person_id)::text)) AS "associatedDrugs",
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
  WHERE (EXISTS ( SELECT 1
           FROM public.accused a
          WHERE ((a.person_id)::text = (p.person_id)::text)))
  WITH NO DATA;
