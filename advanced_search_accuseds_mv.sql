CREATE MATERIALIZED VIEW public.advanced_search_accuseds_mv AS
 SELECT a.accused_id AS id,
    a.accused_code AS "accusedCode",
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
    COALESCE(bfa.accused_type, a.type) AS "accusedRole",
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
    h.ps_code AS "psCode",
    c.crime_id AS "crimeId",
    c.fir_num AS "firNum",
    c.fir_reg_num AS "firRegNum",
    c.fir_type AS "firType",
    c.acts_sections AS sections,
    c.fir_date AS "firDate",
    c.case_status AS "caseStatus",
    c.class_classification AS "caseClass",
    c.major_head AS "majorHead",
    c.minor_head AS "minorHead",
    c.crime_type AS "crimeType",
    c.io_name AS "ioName",
    c.io_rank AS "ioRank",
    c.brief_facts AS "briefFacts",
    h.ps_name AS "psName",
    h.circle_code AS "circleCode",
    h.circle_name AS "circleName",
    h.sdpo_code AS "sdpoCode",
    h.sdpo_name AS "sdpoName",
    h.sub_zone_code AS "subZoneCode",
    h.sub_zone_name AS "subZoneName",
    h.dist_code AS "distCode",
    h.dist_name AS "distName",
    h.range_code AS "rangeCode",
    h.range_name AS "rangeName",
    h.zone_code AS "zoneCode",
    h.zone_name AS "zoneName",
    h.adg_code AS "adgCode",
    h.adg_name AS "adgName",
    p.person_id AS "personId",
    p.name,
    p.surname,
    p.alias,
    p.full_name AS "fullName",
    p.relation_type AS "relationType",
    p.relative_name AS "relativeName",
    p.gender,
    p.is_died AS "isDied",
    p.date_of_birth AS "dateOfBirth",
    p.age,
    p.occupation,
    p.education_qualification AS "educationQualification",
    p.caste,
    p.sub_caste AS "subCaste",
    p.religion,
    p.domicile_classification AS domicile,
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
    ( SELECT COALESCE(array_agg(DISTINCT upper(TRIM(BOTH FROM bfd.primary_drug_name))) FILTER (WHERE ((bfd.primary_drug_name IS NOT NULL) AND (bfd.primary_drug_name <> 'NO_DRUGS_DETECTED'::text))), ARRAY[]::text[]) AS "coalesce"
           FROM public.brief_facts_ai_drug_flat bfd
          WHERE ((bfd.crime_id)::text = (c.crime_id)::text)) AS "drugType",
    ( SELECT jsonb_agg(jsonb_build_object('name', bfd.primary_drug_name, 'quantity',
                CASE
                    WHEN (bfd.weight_kg >= (1)::numeric) THEN concat(round(bfd.weight_kg, 3), ' Kg')
                    WHEN (bfd.weight_g > (0)::numeric) THEN concat(round(bfd.weight_g, 2), ' g')
                    WHEN (bfd.volume_l >= (1)::numeric) THEN concat(round(bfd.volume_l, 3), ' L')
                    WHEN (bfd.volume_ml > (0)::numeric) THEN concat(round(bfd.volume_ml, 2), ' ml')
                    WHEN (bfd.count_total > (0)::numeric) THEN concat(bfd.count_total, ' Units')
                    ELSE 'N/A'::text
                END, 'worth', COALESCE(bfd.seizure_worth, (0)::numeric)) ORDER BY bfd.created_at) AS jsonb_agg
           FROM public.brief_facts_ai_drug_flat bfd
          WHERE ((bfd.crime_id)::text = (c.crime_id)::text)) AS "drugDetails",
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
        END AS chargesheet_due_date
   FROM ((((public.accused a
     JOIN public.crimes c ON (((a.crime_id)::text = (c.crime_id)::text)))
     JOIN public.hierarchy h ON (((c.ps_code)::text = (h.ps_code)::text)))
     LEFT JOIN public.persons p ON (((a.person_id)::text = (p.person_id)::text)))
     LEFT JOIN public.brief_facts_ai_accused_flat bfa ON (((a.accused_id)::text = (bfa.accused_id)::text)))
  WITH NO DATA;
