-- =============================================================================
-- cctns-v2 PostgreSQL schema
-- =============================================================================
-- Purpose
--   Schema for a new "cctns-v2" database that stores CCTNS V2 data as
--   ingested by the current pure-CCTNS DOPAMS ETL pipeline
--   (etl_master/input.cctns-pure.txt, run via `master_etl.py --pure-cctns`).
--
-- Derivation
--   1. CCTNS V2 API response samples captured for all 29 tested endpoints
--      under .../cctnsv2/response/*.json (mapped by 00_summary.json).
--   2. The exact fields each current ETL module (etl-crimes, etl-accused,
--      etl-persons, etl-hierarchy, etl-properties, etl-disposal,
--      etl_arrests, etl_mo_seizures, etl_chargesheets,
--      etl_updated_chargesheet, etl_fsl_case_property, etl-ir, etl-files)
--      actually reads from the API and writes to Postgres.
--   3. Cross-checked column-for-column against the existing production
--      schema this same ETL codebase already targets (DB-schema.sql, a
--      pg_dump of the live "dev_dopamas"-owned database).
--
-- Scope
--   16 tables total. Only entities populated by the pure-CCTNS pipeline
--   are included:
--     hierarchy, crimes, persons (+ geo/address audit columns), accused,
--     arrests, disposal, properties, mo_seizures, chargesheets (+
--     actsAndSections[]/accusedParticulars[] as parallel arrays, see
--     below), charge_sheet_updates, fsl_case_property,
--     interrogation_reports (+ IR sub-entities as parallel arrays /
--     limited JSONB directly on the table, see below), plus
--     file_media_bookkeeping (ALL file/media references + download
--     tracking, for every entity above) and etl_bookkeeping (ALL ETL
--     checkpoint/watermark/retry/failure state), plus the two read-only
--     geo reference/lookup tables used by the non-LLM address resolver
--     (geo_countries, geo_reference).
--
--   Consolidation history (see cctns-v2_schema_mapping_report.md for the
--   full old-table -> new-table mapping, field-level evidence, and the
--   classification of every table):
--     1. etl_bookkeeping (kind: checkpoint | run_state | fk_retry |
--        failure) replaces etl_checkpoint, etl_run_state,
--        etl_fk_retry_queue, etl_address_failures, AND (this revision)
--        the two remaining per-entity retry tables properties_pending_fk
--        and ir_pending_fk (kind=fk_retry, module_name='properties' /
--        'interrogation_reports') -- all six stored the same underlying
--        "which ETL module, what state, when" row shape.
--     2. file_media_bookkeeping (source_type / source_field enums)
--        replaces files, property_media, mo_seizure_media,
--        chargesheet_files, chargesheet_media, fsl_case_property_media,
--        and ir_media -- all seven were "one file/media reference
--        belonging to one parent record" rows with only cosmetic
--        per-module differences.
--     3. chargesheet_acts / chargesheet_acts_sections / chargesheet_accused
--        and all 23 ir_* interrogation-report child tables were removed.
--        Business data that has a confirmed stable field schema from real
--        captured CCTNS responses is stored as parallel PostgreSQL arrays
--        directly on chargesheets / interrogation_reports (one array
--        position per source API array item); IR sub-entities with zero
--        real captured records anywhere in the response samples remain
--        JSONB with a documented reason (see the interrogation_reports
--        table definition). This is a per-field, evidence-based decision,
--        not "flatten everything" or "JSONB everything" -- see the
--        mapping report for the full audit of every field.
--
--   Explicitly EXCLUDED as out of pure-CCTNS scope (AI/LLM-derived or
--   legacy/unrelated, not populated by the pure-CCTNS pipeline):
--     brief_facts_ai, brief_facts_drug (LLM extraction output),
--     drug_categories, drug_ignore_list (drug_standardization tool),
--     person_deduplication_tracker (AI dedup tool),
--     etl_crime_processing_log (populated only by brief_facts_ai),
--     old_interragation_report (dead/legacy table), "user" (chatbot app
--     table) and all materialized views built on brief_facts_ai.
--
-- Design notes
--   * No hard FOREIGN KEY constraints are declared between crime_id-linked
--     tables. This matches the CURRENT production schema (confirmed: zero
--     FOREIGN KEY constraints exist there) and current ETL behavior: child
--     ETL modules validate parent existence with an application-level
--     SELECT before insert, and queue unresolved rows in etl_bookkeeping
--     (kind=fk_retry) for later retry, because CCTNS delivers entities out
--     of dependency order (e.g. a chargesheet can arrive before its
--     crime). Adding hard FKs here would change that behavior and is
--     intentionally avoided per the "no functionality change" requirement.
--     Logical FK relationships are documented as SQL comments on each
--     column instead.
--   * All CCTNS-origin IDs (crime_id, accused_id, person_id, mo_seizure_id,
--     property_id, interrogation_report_id, ...) are 24-character
--     MongoDB ObjectId-style strings, not UUIDs -> stored as
--     character varying, matching production and the raw API responses.
--   * source_system / source_endpoint / fetched_at / etl_run_id provenance
--     columns are added to the 13 entity tables populated directly from a
--     CCTNS endpoint fetch, matching
--     migrations/2026-09-23_add_cctns_provenance_columns.sql, plus
--     file_media_bookkeeping itself (every file/media reference already
--     carried these).
--   * Natural-key PRIMARY KEY constraints are declared for entities whose
--     "logical" PK is documented/used by the ETL as a unique upsert key
--     but is NOT currently enforced as a real PostgreSQL PRIMARY KEY in
--     production (hierarchy.ps_code, chargesheets.id,
--     fsl_case_property.case_property_id, mo_seizures.mo_seizure_id,
--     geo_reference.id). This adds integrity enforcement only; it does not
--     change ETL functionality because the ETL already treats these
--     columns as unique (upsert-by-natural-key), it just was not
--     previously enforced at the DB level.
--   * "" (empty string) vs NULL: several CCTNS endpoints use "" as their
--     null-sentinel for optional string fields instead of JSON null (see
--     mapping report). Columns are left nullable; the ETL is responsible
--     for any "" -> NULL normalization it already performs.
--
-- Do NOT create or modify any actual database with this file without
-- explicit instruction -- it is a design/validation artifact only.
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS pgcrypto;      -- gen_random_uuid()
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";   -- uuid_generate_v4()
CREATE EXTENSION IF NOT EXISTS pg_trgm;       -- trigram indexes used by the KB geo/address resolver
CREATE EXTENSION IF NOT EXISTS fuzzystrmatch; -- used alongside pg_trgm by the KB geo resolver
CREATE EXTENSION IF NOT EXISTS unaccent;      -- used by the KB geo resolver for accent-insensitive matching

-- =============================================================================
-- ENUM types
-- =============================================================================

-- file_media_bookkeeping: which entity + which API field a file/media
-- reference came from.
CREATE TYPE public.source_type_enum AS ENUM (
    'crime',
    'interrogation',
    'property',
    'person',
    'mo_seizures',
    'chargesheets',
    'case_property'
);

CREATE TYPE public.source_field_enum AS ENUM (
    'FIR_COPY',
    'MEDIA',
    'INTERROGATION_REPORT',
    'DOPAMS_DATA',
    'IDENTITY_DETAILS',
    'MO_MEDIA',
    'uploadChargeSheet'
);

-- etl_bookkeeping: which of the 4 consolidated bookkeeping concerns a row represents.
CREATE TYPE public.etl_bookkeeping_kind AS ENUM (
    'checkpoint',   -- was etl_checkpoint: resumable cursor per ETL module (1 row per module_name)
    'run_state',    -- was etl_run_state: incremental watermark per module (1 row per module_name)
    'fk_retry',     -- was etl_fk_retry_queue: parked records with an unresolved FK (many rows)
    'failure'       -- was etl_address_failures: per-record failure log (1 row per module_name+record_key)
);

-- =============================================================================
-- Trigger functions
-- =============================================================================

-- Builds the internal storage path for a file/media reference based on
-- which CCTNS entity + API field it came from (see source_type_enum /
-- source_field_enum above and file_media_bookkeeping.source_type /
-- file_media_bookkeeping.source_field).
CREATE FUNCTION public.generate_file_path(
    p_source_type public.source_type_enum,
    p_source_field public.source_field_enum,
    p_file_id uuid
) RETURNS character varying
    LANGUAGE plpgsql IMMUTABLE
    AS $$
DECLARE
    v_path VARCHAR(500);
BEGIN
    IF p_file_id IS NULL THEN
        RETURN NULL;
    END IF;

    IF p_source_type = 'crime' AND p_source_field = 'FIR_COPY' THEN
        v_path := '/crimes/' || p_file_id::TEXT;
    ELSIF p_source_type = 'crime' AND p_source_field = 'MEDIA' THEN
        v_path := '/crimes/' || p_file_id::TEXT;
    ELSIF p_source_type = 'person' AND p_source_field = 'MEDIA' THEN
        v_path := '/person/media/' || p_file_id::TEXT;
    ELSIF p_source_type = 'person' AND p_source_field = 'IDENTITY_DETAILS' THEN
        v_path := '/person/identitydetails/' || p_file_id::TEXT;
    ELSIF p_source_type = 'property' AND p_source_field = 'MEDIA' THEN
        v_path := '/property/' || p_file_id::TEXT;
    ELSIF p_source_type = 'interrogation' AND p_source_field = 'MEDIA' THEN
        v_path := '/interrogations/media/' || p_file_id::TEXT;
    ELSIF p_source_type = 'interrogation' AND p_source_field = 'INTERROGATION_REPORT' THEN
        v_path := '/interrogations/interrogationreport/' || p_file_id::TEXT;
    ELSIF p_source_type = 'interrogation' AND p_source_field = 'DOPAMS_DATA' THEN
        v_path := '/interrogations/dopamsdata/' || p_file_id::TEXT;
    ELSIF p_source_type = 'mo_seizures' AND p_source_field = 'MO_MEDIA' THEN
        v_path := '/mo_seizures/' || p_file_id::TEXT;
    ELSIF p_source_type = 'chargesheets' AND p_source_field = 'uploadChargeSheet' THEN
        v_path := '/chargesheets/' || p_file_id::TEXT;
    ELSIF p_source_type = 'case_property' AND p_source_field = 'MEDIA' THEN
        v_path := '/fsl_case_property/' || p_file_id::TEXT;
    ELSE
        v_path := NULL;
    END IF;

    RETURN v_path;
END;
$$;

-- Builds the externally-servable download URL for a file/media reference,
-- preserving the base URL configured for the files/media server.
CREATE FUNCTION public.generate_file_url(
    p_source_type public.source_type_enum,
    p_source_field public.source_field_enum,
    p_file_id uuid
) RETURNS character varying
    LANGUAGE plpgsql IMMUTABLE
    AS $$
DECLARE
    v_base_url VARCHAR(255) := current_setting('cctns.files_base_url', true);
    v_path VARCHAR(500);
BEGIN
    IF v_base_url IS NULL OR v_base_url = '' THEN
        v_base_url := 'http://localhost:8080/files';  -- overridden per-environment via FILES_BASE_URL
    END IF;
    v_path := generate_file_path(p_source_type, p_source_field, p_file_id);
    IF v_path IS NOT NULL THEN
        RETURN v_base_url || v_path;
    ELSE
        RETURN NULL;
    END IF;
END;
$$;

-- BEFORE INSERT/UPDATE trigger on file_media_bookkeeping: (re)derives
-- file_path/file_url whenever file_id is set, and preserves any
-- file-extension already present on file_url (extension-preservation
-- logic used by update_file_urls_with_extensions.py).
CREATE FUNCTION public.auto_generate_file_paths() RETURNS trigger
    LANGUAGE plpgsql
    AS $_$
DECLARE
    v_path VARCHAR(500);
    v_url VARCHAR(1000);
    v_extension VARCHAR(50);
BEGIN
    IF NEW.file_id IS NOT NULL THEN
        v_path := generate_file_path(NEW.source_type, NEW.source_field, NEW.file_id);
        v_url := generate_file_url(NEW.source_type, NEW.source_field, NEW.file_id);

        IF v_path IS NOT NULL THEN
            NEW.file_path := REPLACE(TRIM(v_path), ' ', '');
        ELSE
            NEW.file_path := NULL;
        END IF;

        IF v_url IS NOT NULL THEN
            v_url := REPLACE(TRIM(v_url), ' ', '');

            IF TG_OP = 'UPDATE' AND OLD.file_url IS NOT NULL THEN
                v_extension := (regexp_matches(OLD.file_url, '\.([a-zA-Z0-9\-_]+)(?:\?|#|$)', 'g'))[1];
                IF v_extension IS NOT NULL AND length(trim(v_extension)) > 0 THEN
                    NEW.file_url := v_url || '.' || lower(trim(v_extension));
                ELSE
                    NEW.file_url := v_url;
                END IF;
            ELSIF TG_OP = 'INSERT' THEN
                IF NEW.file_url IS NOT NULL AND NEW.file_url ~ '\.[a-zA-Z0-9\-_]+(?:\?|#|$)' THEN
                    v_extension := (regexp_matches(NEW.file_url, '\.([a-zA-Z0-9\-_]+)(?:\?|#|$)', 'g'))[1];
                    IF v_extension IS NOT NULL AND length(trim(v_extension)) > 0 THEN
                        NEW.file_url := v_url || '.' || lower(trim(v_extension));
                    ELSE
                        NEW.file_url := v_url;
                    END IF;
                ELSE
                    NEW.file_url := v_url;
                END IF;
            ELSE
                NEW.file_url := v_url;
            END IF;
        ELSE
            NEW.file_url := NULL;
        END IF;
    ELSE
        NEW.file_path := NULL;
        NEW.file_url := NULL;
    END IF;

    RETURN NEW;
END;
$_$;

-- BEFORE INSERT/UPDATE trigger on fsl_case_property: enforces that
-- MO_ID (mo_id), when present, actually exists in mo_seizures for the same
-- crime_id -- mirrors the real relationship between the CCTNS
-- /case-property endpoint's MO_ID field and the /mo-seizures MO_ID field.
CREATE FUNCTION public.enforce_case_property_mo_reference() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF NEW.mo_id IS NULL OR BTRIM(NEW.mo_id) = '' THEN
        RETURN NEW;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM public.mo_seizures ms
        WHERE ms.crime_id = NEW.crime_id
          AND ms.mo_id = NEW.mo_id
    ) THEN
        RAISE EXCEPTION 'Invalid MO reference: crime_id=% and mo_id=% not found in mo_seizures', NEW.crime_id, NEW.mo_id;
    END IF;

    RETURN NEW;
END;
$$;


-- =============================================================================
-- Reference / master data
-- =============================================================================

CREATE TABLE public.hierarchy (
    ps_code character varying(20) NOT NULL,
    ps_name character varying(255) NOT NULL,
    circle_code character varying(20),
    circle_name character varying(255),
    sdpo_code character varying(20),
    sdpo_name character varying(255),
    sub_zone_code character varying(20),
    sub_zone_name character varying(255),
    dist_code character varying(20),
    dist_name character varying(255),
    range_code character varying(20),
    range_name character varying(255),
    zone_code character varying(20),
    zone_name character varying(255),
    adg_code character varying(20),
    adg_name character varying(255),
    date_created timestamp without time zone,
    date_modified timestamp without time zone,
    source_system character varying(20) DEFAULT 'CCTNS_V2',
    source_endpoint text,
    fetched_at timestamptz,
    etl_run_id uuid,    
    CONSTRAINT hierarchy_pkey PRIMARY KEY (ps_code)
);
COMMENT ON TABLE public.hierarchy IS 'Police organizational hierarchy (ADG -> Zone -> Range -> District -> Sub-Zone -> SDPO -> Circle -> Police Station), one row per PS. Source: GET /master-data/hierarchy.';

CREATE TABLE public.geo_countries (
    country_name text,
    state_name text,
    timezone text
);
COMMENT ON TABLE public.geo_countries IS 'Static country/state/timezone reference used by the non-LLM (KB) address/geo resolver step of the pure-CCTNS pipeline. Not sourced from CCTNS; loaded/maintained separately.';

CREATE TABLE public.geo_reference (
    id SERIAL,
    state_code character varying(10),
    state_name character varying(255),
    district_code character varying(10),
    district_name character varying(255),
    sub_district_code character varying(20),
    sub_district_name character varying(255),
    village_code character varying(20),
    village_version character varying(10),
    village_name_english character varying(255),
    village_name_local character varying(255),
    village_category character varying(50),
    village_status character varying(50),
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT geo_reference_pkey PRIMARY KEY (id)
);
COMMENT ON TABLE public.geo_reference IS 'Static India state/district/sub-district/village reference used by the non-LLM (KB) address/geo resolver step. Not sourced from CCTNS; loaded/maintained separately.';


-- =============================================================================
-- Core: crimes, persons, accused, arrests, disposal
-- =============================================================================

CREATE TABLE public.crimes (
    crime_id character varying(50) NOT NULL,
    ps_code character varying(20) NOT NULL,
    fir_num character varying(50) NOT NULL,
    fir_reg_num character varying(50) NOT NULL,
    fir_type character varying(50),
    acts_sections text,
    fir_date timestamp without time zone,
    case_status character varying(100),
    major_head character varying(100),
    minor_head character varying(255),
    crime_type character varying(100),
    io_name character varying(255),
    io_rank character varying(100),
    brief_facts text,
    date_created timestamp without time zone,
    date_modified timestamp without time zone,
    class_classification character varying(50),
    fir_copy character varying(50),
    additional_json_data jsonb,
    source_system character varying(20) DEFAULT 'CCTNS_V2',
    source_endpoint text,
    fetched_at timestamptz,
    etl_run_id uuid,    
    CONSTRAINT crimes_pkey PRIMARY KEY (crime_id)
);
COMMENT ON TABLE public.crimes IS 'Crime/FIR records. Source: GET /crimes (bulk) and GET /crimes/{crimeId} (detail). additional_json_data holds the CCTNS response fields not individually mapped to a column (e.g. OCCURRENCE_DATE, PLACE_OF_OFFENCE, GD, COMPLAINANT_ID, IO_MOBILE).';

CREATE TABLE public.persons (
    person_id character varying(50) NOT NULL,
    name character varying(255),
    surname character varying(255),
    alias character varying(255),
    full_name character varying(500),
    relation_type character varying(50),
    relative_name character varying(255),
    gender character varying(20),
    is_died boolean DEFAULT false,
    date_of_birth date,
    age integer,
    occupation character varying(255),
    education_qualification character varying(255),
    caste character varying(100),
    sub_caste character varying(100),
    religion character varying(100),
    nationality character varying(100),
    designation character varying(255),
    place_of_work character varying(500),
    present_house_no character varying(255),
    present_street_road_no character varying(255),
    present_ward_colony character varying(255),
    present_landmark_milestone character varying(255),
    present_locality_village character varying(255),
    present_area_mandal character varying(255),
    present_district character varying(255),
    present_state_ut character varying(255),
    present_country character varying(255),
    present_residency_type character varying(100),
    present_pin_code character varying(20),
    present_jurisdiction_ps character varying(20),
    permanent_house_no character varying(255),
    permanent_street_road_no character varying(255),
    permanent_ward_colony character varying(255),
    permanent_landmark_milestone character varying(255),
    permanent_locality_village character varying(255),
    permanent_area_mandal character varying(255),
    permanent_district character varying(255),
    permanent_state_ut character varying(255),
    permanent_country character varying(255),
    permanent_residency_type character varying(100),
    permanent_pin_code character varying(20),
    permanent_jurisdiction_ps character varying(20),
    phone_number character varying(20),
    country_code character varying(10),
    email_id character varying(255),
    date_created timestamp without time zone,
    date_modified timestamp without time zone,
    domicile_classification character varying(50),
    raw_full_name character varying(500),
    gender_confidence numeric(4,3),
    gender_source character varying(20),
    phone_numbers character varying(255),
    geo_resolution_source text,
    geo_resolution_confidence real,
    source_system character varying(20) DEFAULT 'CCTNS_V2',
    source_endpoint text,
    fetched_at timestamptz,
    etl_run_id uuid,    
    CONSTRAINT persons_pkey PRIMARY KEY (person_id)
);
COMMENT ON TABLE public.persons IS 'Person master (accused/complainant/associate/etc.), 1:1 with CCTNS PERSON_ID. Source: GET /person-details/{personId}. geo_resolution_source/confidence and domicile_classification are written by the deterministic (LLM-off in pure-CCTNS mode) address/domicile steps that run after ingestion.';

CREATE TABLE public.accused (
    accused_id character varying(50) NOT NULL,
    crime_id character varying(50) NOT NULL,
    person_id character varying(50),
    accused_code character varying(20) NOT NULL,
    type character varying(50) DEFAULT 'Accused'::character varying,
    seq_num character varying(50),
    is_ccl boolean DEFAULT false,
    beard character varying(100),
    build character varying(100),
    color character varying(100),
    ear character varying(100),
    eyes character varying(100),
    face character varying(100),
    hair character varying(100),
    height character varying(100),
    leucoderma character varying(100),
    mole character varying(100),
    mustache character varying(100),
    nose character varying(100),
    teeth character varying(100),
    date_created timestamp without time zone,
    date_modified timestamp without time zone,
    accused_status text,
    source_system character varying(20) DEFAULT 'CCTNS_V2',
    source_endpoint text,
    fetched_at timestamptz,
    etl_run_id uuid,    
    CONSTRAINT accused_pkey PRIMARY KEY (accused_id)
);
COMMENT ON TABLE public.accused IS 'Links a person to a crime as an accused, with physical features. Source: GET /accused (bulk) and GET /accused/{crimeId} (detail).';

CREATE TABLE public.arrests (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    crime_id character varying(50) NOT NULL,
    person_id character varying(50),
    accused_seq_no text,
    accused_code text,
    accused_type text,
    is_arrested boolean,
    arrested_date timestamp with time zone,
    is_41a_crpc boolean,
    is_41a_explain_submitted boolean,
    date_of_issue_41a date,
    is_ccl boolean,
    is_apprehended boolean,
    is_absconding boolean,
    is_died boolean,
    date_created timestamp with time zone,
    date_modified timestamp with time zone,
    source_system character varying(20) DEFAULT 'CCTNS_V2',
    source_endpoint text,
    fetched_at timestamptz,
    etl_run_id uuid,    
    CONSTRAINT arrests_pkey PRIMARY KEY (id)
);
COMMENT ON TABLE public.arrests IS 'Arrest status per accused-per-crime. No ARREST_ID in the API; natural/application-level key is (crime_id, accused_seq_no). Source: GET /arrests (bulk) and GET /arrests/{crimeId} (detail).';

CREATE TABLE public.disposal (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    crime_id character varying(50) NOT NULL,
    disposal_type text,
    disposed_at timestamp with time zone,
    disposal text,
    case_status text,
    date_created timestamp with time zone,
    date_modified timestamp with time zone,
    source_system character varying(20) DEFAULT 'CCTNS_V2',
    source_endpoint text,
    fetched_at timestamptz,
    etl_run_id uuid,    
    CONSTRAINT disposal_pkey PRIMARY KEY (id),
    CONSTRAINT disposal_unique UNIQUE (crime_id, disposal_type, disposed_at)
);
COMMENT ON TABLE public.disposal IS 'Case disposal outcome, 1:1 with crime. Source: GET /crimes/disposal (bulk) and GET /crimes/disposal/{crimeId} (detail).';


-- =============================================================================
-- Properties (seized/recovered property)
-- =============================================================================

CREATE TABLE public.properties (
    property_id character varying(50) NOT NULL,
    crime_id character varying(50) NOT NULL,
    case_property_id character varying(50),
    property_status character varying(100),
    recovered_from character varying(255),
    place_of_recovery text,
    date_of_seizure timestamp with time zone,
    nature character varying(255),
    belongs character varying(100),
    estimate_value numeric(15,2),
    recovered_value numeric(15,2),
    particular_of_property text,
    category character varying(100),
    additional_details jsonb,
    date_created timestamp with time zone,
    date_modified timestamp with time zone,
    source_system character varying(20) DEFAULT 'CCTNS_V2',
    source_endpoint text,
    fetched_at timestamptz,
    etl_run_id uuid,    
    CONSTRAINT properties_pkey PRIMARY KEY (property_id)
);
COMMENT ON TABLE public.properties IS 'Recovered/seized property. additional_details shape varies by CATEGORY (Drugs/Narcotics, Electrical and Electronic Goods, Miscellaneous, ...). Source: GET /property-details (bulk) and GET /property-details/{crimeId} (detail).';




-- =============================================================================
-- MO Seizures
-- =============================================================================

CREATE TABLE public.mo_seizures (
    mo_seizure_id character varying(50) NOT NULL,
    crime_id character varying(50) NOT NULL,
    seq_no character varying(50),
    mo_id character varying(50),
    type character varying(100),
    sub_type text,
    description text,
    seized_from text,
    seized_at timestamp with time zone,
    seized_by text,
    strength_of_evidence text,
    pos_address1 text,
    pos_address2 text,
    pos_city text,
    pos_district text,
    pos_pincode text,
    pos_landmark text,
    pos_description text,
    pos_latitude double precision,
    pos_longitude double precision,
    mo_media_url text,
    mo_media_name text,
    mo_media_file_id text,
    date_created timestamp with time zone,
    date_modified timestamp with time zone,
    source_system character varying(20) DEFAULT 'CCTNS_V2',
    source_endpoint text,
    fetched_at timestamptz,
    etl_run_id uuid,    
    CONSTRAINT mo_seizures_pkey PRIMARY KEY (mo_seizure_id)
);
COMMENT ON TABLE public.mo_seizures IS 'Modus-operandi seizure records. Natural key is mo_seizure_id; (crime_id, mo_id) is the API-level ordinal key. Source: GET /mo-seizures (bulk) and GET /mo-seizures/{crimeId} (detail).';


-- =============================================================================
-- Chargesheets
-- =============================================================================

CREATE TABLE public.chargesheets (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    crime_id character varying(50) NOT NULL,
    chargesheet_no character varying(50),
    chargesheet_no_icjs character varying(50),
    chargesheet_date timestamp with time zone,
    chargesheet_type character varying(50),
    court_name text,
    is_ccl boolean DEFAULT false,
    is_esigned boolean DEFAULT false,
    date_created timestamp with time zone,
    date_modified timestamp with time zone,
    charge_sheet_id character varying(50),
    -- actsAndSections[] (confirmed stable 5-field schema, 287 real items
    -- across 21 captured chargesheets: sectionDescription missing in
    -- 111/287, graveParticulars missing in 43/287 -- both nullable).
    -- One CCTNS API array item = one position across all 5 parallel arrays.
    acts_descriptions text[],
    acts_sections text[],
    acts_section_descriptions text[],
    acts_grave_particulars text[],
    acts_rw_required boolean[],
    -- accusedParticulars[] (confirmed stable 4-field schema, 90 real items
    -- across 21 captured chargesheets).
    accused_person_ids text[],
    accused_charge_statuses text[],
    accused_reasons_for_no_charge text[],
    accused_requested_for_nbw boolean[],
    source_system character varying(20) DEFAULT 'CCTNS_V2',
    source_endpoint text,
    fetched_at timestamptz,
    etl_run_id uuid,
    CONSTRAINT chargesheets_pkey PRIMARY KEY (id),
    CONSTRAINT chargesheets_unique UNIQUE (charge_sheet_id)
);
COMMENT ON TABLE public.chargesheets IS 'Chargesheet records (camelCase API, unlike most other CCTNS endpoints). Source: GET /chargesheets (bulk) and GET /chargesheets/{crimeId} (detail). id is a synthetic surrogate key; charge_sheet_id carries the natural CCTNS chargeSheetId. actsAndSections[]/accusedParticulars[] are stored as parallel PostgreSQL arrays (one array position per source API array item) rather than a child table or JSONB, since both have confirmed stable field sets from real captured data -- replaces the former dedicated chargesheet_acts/chargesheet_acts_sections/chargesheet_accused tables.';





-- =============================================================================
-- Updated Chargesheets
-- =============================================================================

CREATE TABLE public.charge_sheet_updates (
    id SERIAL,
    update_charge_sheet_id character varying(50) NOT NULL,
    crime_id character varying(50) NOT NULL,
    charge_sheet_no character varying(100),
    charge_sheet_date timestamp with time zone,
    charge_sheet_status character varying(100),
    taken_on_file_date timestamp with time zone,
    taken_on_file_case_type character varying(50),
    taken_on_file_court_case_no character varying(100),
    date_created timestamp with time zone,
    date_modified timestamp with time zone,
    source_system character varying(20) DEFAULT 'CCTNS_V2',
    source_endpoint text,
    fetched_at timestamptz,
    etl_run_id uuid,    
    CONSTRAINT charge_sheet_updates_pkey PRIMARY KEY (id),
    CONSTRAINT charge_sheet_updates_unique UNIQUE (update_charge_sheet_id)
);
COMMENT ON TABLE public.charge_sheet_updates IS 'Chargesheet-status update records (takenOnFile.*). Source: GET /update-chargesheets (bulk) and GET /update-chargesheets/{crimeId} (detail).';


-- =============================================================================
-- FSL / Case Property
-- =============================================================================

CREATE TABLE public.fsl_case_property (
    case_property_id character varying(255) NOT NULL,
    case_type character varying(100),
    crime_id character varying(50) NOT NULL,
    mo_id character varying(255),
    status character varying(100),
    send_date timestamp with time zone,
    fsl_date timestamp with time zone,
    date_disposal timestamp with time zone,
    release_date timestamp with time zone,
    return_date timestamp with time zone,
    date_custody timestamp with time zone,
    date_sent_to_expert timestamp with time zone,
    court_order_date timestamp with time zone,
    date_created timestamp with time zone,
    date_modified timestamp with time zone,
    forwarding_through character varying(255),
    court_name character varying(500),
    fsl_court_name character varying(500),
    cpr_court_name character varying(500),
    court_order_number character varying(255),
    fsl_no character varying(255),
    fsl_request_id character varying(255),
    report_received boolean,
    opinion text,
    opinion_furnished character varying(255),
    strength_of_evidence character varying(255),
    expert_type character varying(255),
    other_expert_type character varying(255),
    cpr_no character varying(255),
    direction_by_court text,
    details_disposal text,
    place_disposal character varying(500),
    release_order_no character varying(255),
    place_custody character varying(500),
    assign_custody character varying(255),
    property_received_back boolean,
    source_system character varying(20) DEFAULT 'CCTNS_V2',
    source_endpoint text,
    fetched_at timestamptz,
    etl_run_id uuid,    
    CONSTRAINT fsl_case_property_pkey PRIMARY KEY (case_property_id)
);
COMMENT ON TABLE public.fsl_case_property IS 'Forensic/case-property register entries. CCTNS calls this endpoint "case-property" (CASE_PROPERTY_ID, SCREAMING_SNAKE_CASE); DOPAMS models it as fsl_case_property. Source: GET /case-property (bulk) and GET /case-property/{crimeId} (detail).';


-- =============================================================================
-- Interrogation Reports
-- =============================================================================

CREATE TABLE public.interrogation_reports (
    interrogation_report_id character varying(50) NOT NULL,
    crime_id character varying(50) NOT NULL,
    person_id character varying(50),
    physical_beard character varying(100),
    physical_build character varying(100),
    physical_burn_marks character varying(100),
    physical_color character varying(100),
    physical_deformities_or_peculiarities character varying(255),
    physical_deformities character varying(255),
    physical_ear character varying(100),
    physical_eyes character varying(100),
    physical_face character varying(100),
    physical_hair character varying(100),
    physical_height character varying(100),
    physical_identification_marks text,
    physical_language_or_dialect text[],
    physical_leucoderma character varying(100),
    physical_mole character varying(100),
    physical_mustache character varying(100),
    physical_nose character varying(100),
    physical_scar character varying(100),
    physical_tattoo character varying(100),
    physical_teeth character varying(100),
    socio_living_status character varying(100),
    socio_marital_status character varying(100),
    socio_education character varying(255),
    socio_occupation character varying(255),
    socio_income_group character varying(255),
    offence_time character varying(255),
    other_offence_time character varying(255),
    share_of_amount_spent character varying(255),
    other_share_of_amount_spent character varying(255),
    share_remarks text,
    is_in_jail boolean,
    from_where_sent_in_jail text,
    in_jail_crime_num character varying(255),
    in_jail_dist_unit character varying(255),
    is_on_bail boolean,
    from_where_sent_on_bail text,
    on_bail_crime_num character varying(255),
    date_of_bail date,
    is_absconding boolean,
    wanted_in_police_station character varying(255),
    absconding_crime_num character varying(255),
    is_normal_life boolean,
    eking_livelihood_by_labor_work text,
    is_rehabilitated boolean,
    rehabilitation_details text,
    is_dead boolean,
    death_details text,
    is_facing_trial boolean,
    facing_trial_ps_name character varying(255),
    facing_trial_crime_num character varying(255),
    other_regular_habits text,
    other_indulgence_before_offence text,
    -- INDULGANCE_BEFORE_OFFENCE is a mixed-type API field: an empty array
    -- `[]` in 187/191 sampled records, a plain free-text string in the
    -- other 4/191 (e.g. "Consuming ganja"). Never actually a multi-item
    -- array in practice -- stored as nullable text, not JSONB/array.
    indulgance_before_offence text,
    time_since_modus_operandi text,
    date_created timestamp without time zone,
    date_modified timestamp without time zone,
    source_system character varying(20) DEFAULT 'CCTNS_V2',
    source_endpoint text,
    fetched_at timestamptz,
    etl_run_id uuid,

    -- FAMILY_HISTORY[] (confirmed stable 6-field schema, 233 real items).
    family_history_person_ids text[],
    family_history_relations text[],
    family_history_peculiarities text[],
    family_history_criminal_background boolean[],
    family_history_is_alive boolean[],
    family_history_stay_together boolean[],

    -- ASSOCIATE_DETAILS[] (confirmed stable 3-field schema, 4 real items).
    associate_person_ids text[],
    associate_gangs text[],
    associate_relations text[],

    -- LOCAL_CONTACTS[] (confirmed stable 4-field schema, 3 real items).
    local_contact_person_ids text[],
    local_contact_towns text[],
    local_contact_addresses text[],
    local_contact_jurisdiction_ps text[],

    -- MODUS_OPERANDI[] (confirmed stable 3-field schema, 9 real items).
    mo_crime_heads text[],
    mo_crime_sub_heads text[],
    mo_descriptions text[],

    -- SHELTER[] (confirmed stable 5-field schema, 3 real items).
    shelter_preparation_of_offence text[],
    shelter_after_offence text[],
    shelter_regular_residency text[],
    shelter_remarks text[],
    shelter_other_regular_residency text[],

    -- DOPAMS_LINKS[] (confirmed stable schema, 71 real items). Each item is
    -- {PHONE_NUMBER, DOPAMS_DATA: [uuid, ...]}; DOPAMS_DATA is itself a
    -- ragged (variable-length per phone number) array of media reference
    -- UUIDs, which native Postgres arrays cannot represent as a rectangular
    -- 2-D array -- each element of dopams_link_data is instead the
    -- comma-joined UUID list for that phone number (UUIDs never contain
    -- commas, so this is lossless).
    dopams_link_phone_numbers text[],
    dopams_link_data text[],

    -- TYPES_OF_DRUGS[] (confirmed stable 7-field schema, 83 real items).
    drug_types text[],
    drug_quantities text[],
    drug_purchase_amounts_inr text[],
    drug_modes_of_payment text[],
    drug_modes_of_transport text[],
    drug_supplier_person_ids text[],
    drug_receiver_person_ids text[],

    -- CONSUMER_DETAILS[] (confirmed stable 6-field schema, 75 real items).
    consumer_person_ids text[],
    consumer_places_of_consumption text[],
    consumer_other_sources text[],
    consumer_other_sources_phone_nos text[],
    consumer_aadhar_numbers text[],
    consumer_aadhar_phone_nos text[],

    -- FINANCIAL_HISTORY[] (confirmed stable 9-field schema, 70 real items).
    financial_account_holder_person_ids text[],
    financial_pan_nos text[],
    financial_upi_ids text[],
    financial_bank_names text[],
    financial_account_numbers text[],
    financial_branch_names text[],
    financial_ifsc_codes text[],
    financial_immovable_property text[],
    financial_movable_property text[],

    -- SIM_DETAILS[] (confirmed stable 5-field schema, 90 real items).
    sim_phone_numbers text[],
    sim_sdrs text[],
    sim_imeis text[],
    sim_true_caller_names text[],
    sim_person_ids text[],

    -- REGULAR_HABITS[] is a flat array of enum-like strings (203 real
    -- items), not an array of objects -- a plain text[] is the exact
    -- representation, no flattening needed.
    regular_habits text[],

    -- The following 10 arrays were NEVER observed populated in any
    -- captured CCTNS V2 IR response (0/191 sampled records had data for
    -- any of them). Their structure cannot be verified against the actual
    -- API contract this schema is required to match. JSONB is retained
    -- deliberately here (Case 5): inventing ~10 columns each from the
    -- pre-existing production database's column list (a different, older
    -- system) risks fabricating a structure the current CCTNS V2 API may
    -- not actually send, which is a worse violation of "do not invent
    -- structure" than storing the raw object/array as-is. If/when real
    -- data is observed for any of these, re-run this audit against that
    -- data and flatten into columns/arrays per the same method used above.
    conviction_acquittal jsonb,
    defence_counsel jsonb,
    execution_of_nbw jsonb,
    jail_sentence jsonb,
    new_gang_formation jsonb,
    pending_nbw jsonb,
    previous_offences_confessed jsonb,
    property_disposal jsonb,
    regularization_transit_warrants jsonb,
    sureties jsonb,

    CONSTRAINT interrogation_reports_pkey PRIMARY KEY (interrogation_report_id)
);
COMMENT ON TABLE public.interrogation_reports IS 'Interrogation report main record, 1:1 with (crime_id, person_id) per interrogated subject. PHYSICAL_FEATURES/SOCIO_ECONOMIC_PROFILE/COMMISSION_OF_OFFENCE/SHARE_OF_AMOUNT_SPENT/PRESENT_WHEREABOUTS nested objects are flattened into columns here. Source: GET /interrogation-reports/v1/ (bulk) and GET /interrogation-reports/v1/{crimeId} (detail). MEDIA[] and INTERROGATION_REPORT[] (report/document file references) are stored in file_media_bookkeeping (source_type=''interrogation''), not here, since they are file/media references rather than business data. 11 array-of-object fields with confirmed stable schemas from real captured data are stored as parallel arrays (one array position per source API array item); 10 fields never observed populated in any capture remain JSONB with a documented reason (see comments above). Replaces the former 23 ir_* child tables.';














-- =============================================================================
-- File / Media bookkeeping (consolidated)
-- =============================================================================

CREATE TABLE public.file_media_bookkeeping (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    source_type public.source_type_enum NOT NULL,
    source_field public.source_field_enum NOT NULL,
    parent_id character varying(255) NOT NULL,
    file_id uuid,
    file_index integer,
    file_path character varying(500),
    file_url character varying(1000),
    media_url text,
    media_name text,
    media_payload jsonb,
    identity_type character varying(255),
    identity_number character varying(255),
    has_field boolean DEFAULT true,
    is_empty boolean DEFAULT false,
    notes text,
    is_downloaded boolean DEFAULT false,
    downloaded_at timestamp without time zone,
    download_error text,
    download_attempts integer DEFAULT 0,
    source_system character varying(20) DEFAULT 'CCTNS_V2',
    source_endpoint text,
    fetched_at timestamptz,
    etl_run_id uuid,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT file_media_bookkeeping_pkey PRIMARY KEY (id)
);
COMMENT ON TABLE public.file_media_bookkeeping IS 'Single consolidated table for every CCTNS file/media reference and its download status. Replaces the former files, property_media, mo_seizure_media, chargesheet_files, chargesheet_media, fsl_case_property_media, and ir_media tables (all 7 stored the same shape: a reference to one file/media item belonging to one parent CCTNS record, discovered from one API field, optionally with a richer payload and/or download tracking). source_type/source_field identify which CCTNS entity+field the reference came from (crime/FIR_COPY, crime/MEDIA, person/MEDIA, person/IDENTITY_DETAILS, property/MEDIA, interrogation/MEDIA, interrogation/INTERROGATION_REPORT, interrogation/DOPAMS_DATA, mo_seizures/MO_MEDIA, chargesheets/uploadChargeSheet, case_property/MEDIA); parent_id is the natural CCTNS id of the owning record (crime_id, person_id, property_id, mo_seizure_id, interrogation_report_id, chargesheets.charge_sheet_id, or fsl_case_property.case_property_id). file_path/file_url are always DERIVED (never written directly by ETL code) by the auto_generate_file_paths trigger. media_url/media_name/media_payload hold the RAW extra fields some source endpoints provide directly beyond a bare file_id (e.g. mo-seizures MO_MEDIA_URL/MO_MEDIA_NAME, chargesheet uploadChargeSheet payload, case-property MEDIA[] entries, property MEDIA[] entries) -- media_url is intentionally a separate column from the trigger-managed file_url so the ETL-provided source URL is never silently overwritten.';


-- =============================================================================
-- ETL bookkeeping (consolidated)
-- =============================================================================

CREATE TABLE public.etl_bookkeeping (
    id BIGSERIAL NOT NULL,
    kind public.etl_bookkeeping_kind NOT NULL,
    module_name text NOT NULL,
    record_key text,
    run_id text,
    checkpoint_value text,
    watermark timestamptz,
    record_json jsonb,
    missing_fk_column character varying(100),
    missing_fk_value text,
    reason text,
    attempt_count integer DEFAULT 0,
    last_attempted_at timestamptz,
    first_failed_at timestamptz DEFAULT CURRENT_TIMESTAMP,
    resolved boolean DEFAULT false,
    resolved_at timestamptz,
    created_at timestamptz DEFAULT now(),
    updated_at timestamptz DEFAULT now(),
    CONSTRAINT etl_bookkeeping_pkey PRIMARY KEY (id)
);
COMMENT ON TABLE public.etl_bookkeeping IS 'Single consolidated ETL bookkeeping table. Replaces etl_checkpoint (kind=checkpoint: crash-resume cursor for etl-address, one row per module_name; checkpoint_value=last_seen_id), etl_run_state (kind=run_state: incremental watermark used by etl-persons/etl-properties/etl-disposal/etl-accused/master_etl, one row per module_name; watermark=last_successful_end), etl_fk_retry_queue (kind=fk_retry: shared FK-retry queue used by disposal/arrests/chargesheets/update-chargesheets/fsl_case_property, many rows per module_name; record_key=record_id, record_json=parked raw record, missing_fk_column/missing_fk_value=the unresolved FK, attempt_count/last_attempted_at/first_failed_at/resolved/reason=retry bookkeeping), and etl_address_failures (kind=failure: per-record failure log for etl-address, one row per (module_name, record_key=person_id); reason/record_json=details, attempt_count=attempted, last_attempted_at=last_try). module_name doubles as etl_name (checkpoint) / module_name (run_state) / source_table (fk_retry) depending on kind -- all four were "which ETL wrote this row" identifiers in the original tables.';


-- =============================================================================
-- Triggers
-- =============================================================================

CREATE TRIGGER trigger_auto_generate_file_paths
    BEFORE INSERT OR UPDATE ON public.file_media_bookkeeping
    FOR EACH ROW
    EXECUTE FUNCTION public.auto_generate_file_paths();

CREATE TRIGGER trg_enforce_case_property_mo_reference
    BEFORE INSERT OR UPDATE OF crime_id, mo_id ON public.fsl_case_property
    FOR EACH ROW
    EXECUTE FUNCTION public.enforce_case_property_mo_reference();


-- =============================================================================
-- Indexes
-- =============================================================================
-- (Beyond the PRIMARY KEY / UNIQUE constraints already declared inline above.)

-- crimes: incremental-fetch / recency queries
CREATE INDEX idx_crimes_date_created ON public.crimes USING btree (date_created DESC NULLS LAST);
CREATE INDEX idx_crimes_date_modified_created ON public.crimes USING btree (date_modified DESC NULLS LAST, date_created DESC NULLS LAST);
CREATE INDEX idx_crimes_coalesce_date ON public.crimes USING btree (COALESCE(date_modified, date_created) DESC NULLS LAST);
CREATE INDEX idx_crimes_ps_code ON public.crimes USING btree (ps_code);

-- accused / arrests / disposal / properties: crime_id is the universal join key
CREATE INDEX idx_accused_crime_id ON public.accused USING btree (crime_id);
CREATE INDEX idx_accused_person_id ON public.accused USING btree (person_id);
CREATE INDEX idx_arrests_crime_id ON public.arrests USING btree (crime_id);
CREATE INDEX idx_arrests_person_id ON public.arrests USING btree (person_id);
CREATE INDEX idx_disposal_crime_id ON public.disposal USING btree (crime_id);
CREATE INDEX idx_properties_crime_id ON public.properties USING btree (crime_id);
CREATE INDEX idx_properties_case_property_id ON public.properties USING btree (case_property_id);

-- mo_seizures / chargesheets / charge_sheet_updates / fsl_case_property
CREATE INDEX idx_mo_seizures_crime_id ON public.mo_seizures USING btree (crime_id);
CREATE INDEX idx_chargesheets_crime_id ON public.chargesheets USING btree (crime_id);
CREATE INDEX idx_charge_sheet_updates_crime_id ON public.charge_sheet_updates USING btree (crime_id);
CREATE INDEX idx_fsl_crime_id ON public.fsl_case_property USING btree (crime_id);
CREATE INDEX idx_fsl_mo_id ON public.fsl_case_property USING btree (mo_id);
CREATE INDEX idx_fsl_status ON public.fsl_case_property USING btree (status);
CREATE INDEX idx_fsl_created ON public.fsl_case_property USING btree (date_created DESC NULLS LAST);

-- interrogation_reports
CREATE INDEX idx_ir_reports_crime_person ON public.interrogation_reports USING btree (crime_id, person_id);
CREATE INDEX idx_ir_reports_created_modified ON public.interrogation_reports USING btree (date_created, date_modified);

-- file_media_bookkeeping: matches files_loader.py's original dedup key
-- (FilesLoader._build_record_key) for singular fields (FIR_COPY,
-- uploadChargeSheet, ...) which never set file_index, PLUS one extra split
-- for the array-type media fields folded in from property_media/
-- mo_seizure_media/chargesheet_media/chargesheet_files/
-- fsl_case_property_media/ir_media, where a MEDIA[]/actsAndSections[]-style
-- array can contain more than one entry with a null/empty file_id per
-- parent -- those must still dedupe per array position (file_index), not
-- collapse to "one null-file_id row per parent" the way single-value
-- fields do.
--   file_id IS NOT NULL                       -> dedupe on (source_type, source_field, parent_id, file_id, file_index)
--   file_id IS NULL AND file_index IS NULL    -> dedupe on (source_type, source_field, parent_id)         [singular fields, e.g. FIR_COPY]
--   file_id IS NULL AND file_index IS NOT NULL -> dedupe on (source_type, source_field, parent_id, file_index) [array-position fields, e.g. MEDIA[]]
CREATE UNIQUE INDEX uq_file_media_bookkeeping_with_file_id ON public.file_media_bookkeeping (source_type, source_field, parent_id, file_id, file_index) WHERE (file_id IS NOT NULL);
CREATE UNIQUE INDEX uq_file_media_bookkeeping_no_file_id_singular ON public.file_media_bookkeeping (source_type, source_field, parent_id) WHERE (file_id IS NULL AND file_index IS NULL);
CREATE UNIQUE INDEX uq_file_media_bookkeeping_no_file_id_indexed ON public.file_media_bookkeeping (source_type, source_field, parent_id, file_index) WHERE (file_id IS NULL AND file_index IS NOT NULL);
CREATE INDEX idx_file_media_bookkeeping_source_type_created ON public.file_media_bookkeeping USING btree (source_type, created_at);
CREATE INDEX idx_file_media_bookkeeping_parent_id ON public.file_media_bookkeeping USING btree (parent_id);
CREATE INDEX idx_file_media_bookkeeping_file_id ON public.file_media_bookkeeping USING btree (file_id);
CREATE INDEX idx_file_media_bookkeeping_is_downloaded ON public.file_media_bookkeeping USING btree (is_downloaded) WHERE (is_downloaded = true);
CREATE INDEX idx_file_media_bookkeeping_downloaded_at ON public.file_media_bookkeeping USING btree (downloaded_at) WHERE (downloaded_at IS NOT NULL);
CREATE INDEX idx_file_media_bookkeeping_created_at ON public.file_media_bookkeeping USING btree (created_at);

-- etl_bookkeeping: singleton-per-module rows (checkpoint, run_state) and
-- singleton-per-record rows (failure), matching each old table's original
-- upsert key exactly (etl_checkpoint PK(etl_name), etl_run_state
-- PK(module_name), etl_address_failures PK(person_id) -- always scoped to
-- module_name='etl-address' since that was the only writer).
CREATE UNIQUE INDEX uq_etl_bookkeeping_singleton ON public.etl_bookkeeping (kind, module_name) WHERE (kind IN ('checkpoint', 'run_state'));
CREATE UNIQUE INDEX uq_etl_bookkeeping_failure ON public.etl_bookkeeping (kind, module_name, record_key) WHERE (kind = 'failure');
-- Without this, push_fk_failure's `ON CONFLICT DO NOTHING` has no matching
-- constraint to target, so every ETL run that revisits an unresolved record
-- re-queues it as a brand-new row instead of deduping (confirmed live: the
-- IR module's queue grew from 93 to 186 rows across two runs of the same
-- date window before this index was added).
CREATE UNIQUE INDEX uq_etl_bookkeeping_fk_retry ON public.etl_bookkeeping (kind, module_name, record_key) WHERE (kind = 'fk_retry');
CREATE INDEX idx_etl_bookkeeping_fk_retry_unresolved ON public.etl_bookkeeping USING btree (module_name) WHERE (kind = 'fk_retry' AND resolved = false);
CREATE INDEX idx_etl_bookkeeping_kind_module ON public.etl_bookkeeping USING btree (kind, module_name);
CREATE INDEX idx_etl_bookkeeping_failure_reason ON public.etl_bookkeeping USING btree (reason) WHERE (kind = 'failure');
CREATE INDEX idx_etl_bookkeeping_failure_last_attempted ON public.etl_bookkeeping USING btree (last_attempted_at) WHERE (kind = 'failure');

-- geo_countries / geo_reference: trigram indexes backing the KB address/geo resolver's fuzzy matching
CREATE INDEX idx_geo_countries_country ON public.geo_countries USING btree (country_name);
CREATE INDEX idx_geo_countries_country_trgm ON public.geo_countries USING gin (country_name public.gin_trgm_ops);
CREATE INDEX idx_geo_countries_state ON public.geo_countries USING btree (state_name);
CREATE INDEX idx_geo_countries_state_trgm ON public.geo_countries USING gin (state_name public.gin_trgm_ops);
CREATE INDEX idx_geo_reference_state_lower ON public.geo_reference USING btree (lower((state_name)::text));
CREATE INDEX idx_geo_reference_state_district_lower ON public.geo_reference USING btree (lower((state_name)::text), lower((district_name)::text));
CREATE INDEX idx_geo_reference_state_trgm ON public.geo_reference USING gin (lower((state_name)::text) public.gin_trgm_ops);
CREATE INDEX idx_geo_reference_district_trgm ON public.geo_reference USING gin (lower((district_name)::text) public.gin_trgm_ops);
CREATE INDEX idx_geo_reference_subdistrict_trgm ON public.geo_reference USING gin (lower((sub_district_name)::text) public.gin_trgm_ops) WHERE (sub_district_name IS NOT NULL);
CREATE INDEX idx_geo_reference_village_trgm ON public.geo_reference USING gin (lower((village_name_english)::text) public.gin_trgm_ops) WHERE (village_name_english IS NOT NULL);

-- persons: supports the address/geo-resolution step's "needs resolving" scan
CREATE INDEX idx_persons_geo_resolution_source ON public.persons (geo_resolution_source) WHERE (geo_resolution_source IS NOT NULL);
CREATE INDEX idx_persons_present_district ON public.persons USING btree (present_district);
CREATE INDEX idx_persons_permanent_district ON public.persons USING btree (permanent_district);
