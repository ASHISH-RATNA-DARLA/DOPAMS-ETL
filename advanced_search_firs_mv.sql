CREATE MATERIALIZED VIEW public.advanced_search_firs_mv AS
 SELECT c.crime_id AS id,
    h.ps_code AS "psCode",
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
    ( SELECT count(*) AS count
           FROM public.accused a
          WHERE ((a.crime_id)::text = (c.crime_id)::text)) AS "noOfAccusedInvolved",
    ( SELECT jsonb_agg(jsonb_build_object('name', p2.name, 'surname', p2.surname, 'alias', p2.alias, 'fullName', p2.full_name, 'accusedRole', COALESCE(bfa2.accused_type, a2.type), 'status',
                CASE
                    WHEN ((COALESCE(bfa2.status, a2.accused_status) ~~* 'Arrest%'::text) AND (COALESCE(bfa2.status, a2.accused_status) !~~* 'Arrest Related%'::text)) THEN 'Arrested'::text
                    WHEN (COALESCE(bfa2.status, a2.accused_status) ~~* 'Surrendered%'::text) THEN 'Arrested'::text
                    WHEN (COALESCE(bfa2.status, a2.accused_status) ~~* 'Absconding'::text) THEN 'Absconding'::text
                    WHEN (COALESCE(bfa2.status, a2.accused_status) ~~* 'Arrest Related/41A CrPC Pending'::text) THEN 'Absconding'::text
                    WHEN (COALESCE(bfa2.status, a2.accused_status) ~~* '41A Cr.P.C%'::text) THEN 'Issued Notice'::text
                    WHEN (COALESCE(bfa2.status, a2.accused_status) ~~* 'High court directions%'::text) THEN 'Issued Notice'::text
                    ELSE 'Unknown'::text
                END)) AS jsonb_agg
           FROM ((public.accused a2
             LEFT JOIN public.persons p2 ON (((a2.person_id)::text = (p2.person_id)::text)))
             LEFT JOIN public.brief_facts_ai_accused_flat bfa2 ON (((a2.accused_id)::text = (bfa2.accused_id)::text)))
          WHERE ((a2.crime_id)::text = (c.crime_id)::text)) AS "accusedDetails",
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
   FROM (public.crimes c
     JOIN public.hierarchy h ON (((c.ps_code)::text = (h.ps_code)::text)))
  WITH NO DATA;
