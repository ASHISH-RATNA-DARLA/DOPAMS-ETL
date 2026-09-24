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
--   Only entities populated by the pure-CCTNS pipeline are included:
--     hierarchy, crimes, persons (+ geo/address audit columns), accused,
--     arrests, disposal, properties (+ pending-fk / additional-details
--     child tables), mo_seizures, chargesheets (+ acts / acts_sections /
--     accused child tables), charge_sheet_updates, fsl_case_property,
--     interrogation_reports (+ 22 child tables incl. ir_pending_fk),
--     file_media_bookkeeping (ALL file/media references + download
--     tracking, for every entity above -- consolidated, see below), and
--     etl_bookkeeping (ALL ETL checkpoint/watermark/retry/failure state --
--     consolidated, see below), plus the two read-only geo reference/
--     lookup tables used by the non-LLM address resolver (geo_countries,
--     geo_reference).
--
--   Consolidation (this revision): the prior version of this schema had
--   one bookkeeping table per concern. Both families are now single
--   tables with a discriminator column, since every "old" table in each
--   family stored the same underlying kind of row and existed only
--   because each ETL module was extended independently over time:
--     * etl_bookkeeping (kind: checkpoint | run_state | fk_retry | failure)
--       replaces etl_checkpoint, etl_run_state, etl_fk_retry_queue,
--       etl_address_failures.
--     * file_media_bookkeeping (source_type / source_field, reusing the
--       enums already used by the old `files` table) replaces files,
--       property_media, mo_seizure_media, chargesheet_files,
--       chargesheet_media, fsl_case_property_media, and ir_media -- all
--       seven were "one file/media reference belonging to one parent
--       record" rows with only cosmetic per-module differences (which
--       columns were populated, which natural key was used as parent_id).
--   See cctns-v2_schema_mapping_report.md for the full old-table ->
--   new-table mapping and the classification of every remaining table.
--
--   Explicitly EXCLUDED as out of pure-CCTNS scope (AI/LLM-derived or
--   legacy/unrelated, not populated by the pure-CCTNS pipeline):
--     brief_facts_ai, brief_facts_drug (LLM extraction output),
--     drug_categories, drug_ignore_list (drug_standardization tool),
--     person_deduplication_tracker (AI dedup tool),
--     etl_crime_processing_log (populated only by brief_facts_ai),
--     old_interragation_report (dead/legacy table), "user" (chatbot app
--     table) and all materialized views built on brief_facts_ai.
--   Also excluded from this revision: properties_pending_fk and
--   ir_pending_fk are NOT part of the etl_bookkeeping consolidation --
--   they are per-entity FK-retry tables distinct from the *shared*
--   etl_fk_retry_queue (used only by disposal/arrests/chargesheets/
--   update-chargesheets/fsl_case_property), were not named in the
--   consolidation request, and remain as-is.
--
-- Design notes
--   * No hard FOREIGN KEY constraints are declared between crime_id-linked
--     tables. This matches the CURRENT production schema (confirmed: zero
--     FOREIGN KEY constraints exist there) and current ETL behavior: child
--     ETL modules validate parent existence with an application-level
--     SELECT before insert, and queue unresolved rows in the *_pending_fk
--     tables / etl_bookkeeping (kind=fk_retry) for later retry, because
--     CCTNS delivers entities out of dependency order (e.g. a chargesheet
--     can arrive before its crime). Adding hard FKs here would change that
--     behavior and is intentionally avoided per the "no functionality
--     change" requirement. Logical FK relationships are documented as SQL
--     comments on each column instead.
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
    media jsonb DEFAULT '[]'::jsonb,
    date_created timestamp with time zone,
    date_modified timestamp with time zone,
    source_system character varying(20) DEFAULT 'CCTNS_V2',
    source_endpoint text,
    fetched_at timestamptz,
    etl_run_id uuid,
    CONSTRAINT properties_pkey PRIMARY KEY (property_id)
);
COMMENT ON TABLE public.properties IS 'Recovered/seized property. additional_details shape varies by CATEGORY (Drugs/Narcotics, Electrical and Electronic Goods, Miscellaneous, ...). Source: GET /property-details (bulk) and GET /property-details/{crimeId} (detail).';

CREATE TABLE public.properties_pending_fk (
    id SERIAL,
    property_id character varying(50) NOT NULL,
    crime_id character varying(50) NOT NULL,
    raw_data jsonb NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    retry_count integer DEFAULT 0,
    last_retry_at timestamp without time zone,
    resolved boolean DEFAULT false,
    resolved_at timestamp without time zone,
    CONSTRAINT properties_pending_fk_pkey PRIMARY KEY (id)
);
COMMENT ON TABLE public.properties_pending_fk IS 'Retry queue for property rows whose crime_id was not yet present in crimes at insert time (out-of-order CCTNS delivery).';

CREATE TABLE public.property_additional_details (
    property_id character varying(50) NOT NULL,
    additional_details jsonb NOT NULL,
    date_created timestamp with time zone,
    date_modified timestamp with time zone,
    CONSTRAINT property_additional_details_json_is_object CHECK ((jsonb_typeof(additional_details) = 'object'::text)),
    CONSTRAINT property_additional_details_pkey PRIMARY KEY (property_id),
    CONSTRAINT property_additional_details_check_1 CHECK (jsonb_typeof(additional_details) = 'object')
);
COMMENT ON TABLE public.property_additional_details IS 'Normalized snapshot of properties.additional_details (kept in sync by the ETL in overwrite mode); same data, queryable form.';


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
    source_system character varying(20) DEFAULT 'CCTNS_V2',
    source_endpoint text,
    fetched_at timestamptz,
    etl_run_id uuid,
    CONSTRAINT chargesheets_pkey PRIMARY KEY (id),
    CONSTRAINT chargesheets_unique UNIQUE (charge_sheet_id)
);
COMMENT ON TABLE public.chargesheets IS 'Chargesheet records (camelCase API, unlike most other CCTNS endpoints). Source: GET /chargesheets (bulk) and GET /chargesheets/{crimeId} (detail). id is a synthetic surrogate key; charge_sheet_id carries the natural CCTNS chargeSheetId.';

CREATE TABLE public.chargesheet_acts (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    chargesheet_id uuid NOT NULL,
    act_description text,
    section text,
    rw_required boolean DEFAULT false,
    section_description text,
    grave_particulars text,
    created_at timestamp with time zone,
    CONSTRAINT chargesheet_acts_pkey PRIMARY KEY (id)
);
COMMENT ON TABLE public.chargesheet_acts IS 'actsAndSections[] entries (references chargesheets.id).';

CREATE TABLE public.chargesheet_acts_sections (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    chargesheet_id character varying(50) NOT NULL,
    act_index integer DEFAULT 0 NOT NULL,
    section_index integer DEFAULT 0 NOT NULL,
    act_description text,
    section text,
    rw_required boolean DEFAULT false,
    section_description text,
    grave_particulars text,
    created_at timestamp with time zone,
    date_modified timestamp with time zone,
    CONSTRAINT chargesheet_acts_sections_pkey PRIMARY KEY (id)
);
COMMENT ON TABLE public.chargesheet_acts_sections IS 'actsAndSections[] entries, alternate normalized form keyed by the natural charge_sheet_id (references chargesheets.charge_sheet_id) with explicit act_index/section_index ordinals.';

CREATE TABLE public.chargesheet_accused (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    chargesheet_id uuid NOT NULL,
    accused_person_id character varying(50) NOT NULL,
    charge_status character varying(30),
    requested_for_nbw boolean DEFAULT false,
    reason_for_no_charge text,
    is_person_master_present boolean DEFAULT true,
    created_at timestamp with time zone,
    CONSTRAINT chargesheet_accused_pkey PRIMARY KEY (id)
);
COMMENT ON TABLE public.chargesheet_accused IS 'accusedParticulars[] entries (references chargesheets.id).';


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
    time_since_modus_operandi text,
    date_created timestamp without time zone,
    date_modified timestamp without time zone,
    source_system character varying(20) DEFAULT 'CCTNS_V2',
    source_endpoint text,
    fetched_at timestamptz,
    etl_run_id uuid,
    CONSTRAINT interrogation_reports_pkey PRIMARY KEY (interrogation_report_id)
);
COMMENT ON TABLE public.interrogation_reports IS 'Interrogation report main record, 1:1 with (crime_id, person_id) per interrogated subject. PHYSICAL_FEATURES/SOCIO_ECONOMIC_PROFILE/COMMISSION_OF_OFFENCE/SHARE_OF_AMOUNT_SPENT/PRESENT_WHEREABOUTS nested objects are flattened into columns here. Source: GET /interrogation-reports/v1/ (bulk) and GET /interrogation-reports/v1/{crimeId} (detail).';

CREATE TABLE public.ir_associate_details (
    id SERIAL,
    interrogation_report_id character varying(50) NOT NULL,
    person_id character varying(50),
    gang character varying(255),
    relation text,
    CONSTRAINT ir_associate_details_pkey PRIMARY KEY (id)
);
COMMENT ON TABLE public.ir_associate_details IS 'Interrogation report child table for ASSOCIATE_DETAILS[].';

CREATE TABLE public.ir_consumer_details (
    id SERIAL,
    interrogation_report_id character varying(50) NOT NULL,
    consumer_person_id character varying(50),
    place_of_consumption text,
    other_sources text,
    other_sources_phone_no character varying(20),
    aadhar_card_number character varying(20),
    aadhar_card_number_phone_no character varying(20),
    CONSTRAINT ir_consumer_details_pkey PRIMARY KEY (id)
);
COMMENT ON TABLE public.ir_consumer_details IS 'Interrogation report child table for CONSUMER_DETAILS[].';

CREATE TABLE public.ir_conviction_acquittal (
    id BIGSERIAL,
    interrogation_report_id character varying(50),
    crime_num text,
    jurisdiction_ps text,
    court_name text,
    judge_name text,
    law_section text,
    verdict text,
    verdict_date date,
    reason_if_acquitted text,
    conviction_remarks text,
    fine_amount_in_inr numeric,
    sentence_if_convicted text,
    appeal_status text,
    appeal_court text,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now(),
    CONSTRAINT ir_conviction_acquittal_pkey PRIMARY KEY (id)
);
COMMENT ON TABLE public.ir_conviction_acquittal IS 'Interrogation report child table for CONVICTION_ACQUITTAL[] (always empty in sampled data; shape unconfirmed beyond column list).';

CREATE TABLE public.ir_defence_counsel (
    id BIGSERIAL,
    interrogation_report_id character varying(50),
    dist_division text,
    ps_code text,
    crime_num text,
    law_section text,
    sc_cc_num text,
    defence_counsel_address text,
    defence_counsel_phone text,
    assistance text,
    defence_counsel_person_id character varying(50),
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now(),
    CONSTRAINT ir_defence_counsel_pkey PRIMARY KEY (id)
);
COMMENT ON TABLE public.ir_defence_counsel IS 'Interrogation report child table for DEFENCE_COUNSEL[] (always empty in sampled data; shape unconfirmed beyond column list).';

CREATE TABLE public.ir_dopams_links (
    id SERIAL,
    interrogation_report_id character varying(50) NOT NULL,
    phone_number character varying(20),
    dopams_data text[],
    CONSTRAINT ir_dopams_links_pkey PRIMARY KEY (id)
);
COMMENT ON TABLE public.ir_dopams_links IS 'Interrogation report child table for DOPAMS_LINKS[].';

CREATE TABLE public.ir_execution_of_nbw (
    id BIGSERIAL,
    interrogation_report_id character varying(50),
    nbw_number text,
    issued_date date,
    executed_date date,
    jurisdiction_ps text,
    crime_num text,
    executed_by text,
    place_of_execution text,
    remarks text,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now(),
    CONSTRAINT ir_execution_of_nbw_pkey PRIMARY KEY (id)
);
COMMENT ON TABLE public.ir_execution_of_nbw IS 'Interrogation report child table for EXECUTION_OF_NBW[] (always empty in sampled data; shape unconfirmed beyond column list).';

CREATE TABLE public.ir_family_history (
    id SERIAL,
    interrogation_report_id character varying(50) NOT NULL,
    person_id character varying(50),
    relation text,
    family_member_peculiarity text,
    criminal_background boolean DEFAULT false,
    is_alive boolean DEFAULT true,
    family_stay_together boolean DEFAULT true,
    CONSTRAINT ir_family_history_pkey PRIMARY KEY (id)
);
COMMENT ON TABLE public.ir_family_history IS 'Interrogation report child table for FAMILY_HISTORY[].';

CREATE TABLE public.ir_financial_history (
    id SERIAL,
    interrogation_report_id character varying(50) NOT NULL,
    account_holder_person_id character varying(50),
    pan_no character varying(50),
    upi_id character varying(255),
    name_of_bank character varying(255),
    account_number text,
    branch_name character varying(255),
    ifsc_code character varying(50),
    immovable_property_acquired text,
    movable_property_acquired text,
    CONSTRAINT ir_financial_history_pkey PRIMARY KEY (id)
);
COMMENT ON TABLE public.ir_financial_history IS 'Interrogation report child table for FINANCIAL_HISTORY[].';

CREATE TABLE public.ir_indulgance_before_offence (
    id SERIAL,
    interrogation_report_id character varying(50) NOT NULL,
    indulgance text,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ir_indulgance_before_offence_pkey PRIMARY KEY (id)
);
COMMENT ON TABLE public.ir_indulgance_before_offence IS 'Interrogation report child table for INDULGANCE_BEFORE_OFFENCE (mixed array/string type in the API; stored as free text, not a relational list, per the observed type instability).';

CREATE TABLE public.ir_interrogation_report_refs (
    id SERIAL,
    interrogation_report_id character varying(50) NOT NULL,
    report_ref_id text NOT NULL,
    CONSTRAINT ir_interrogation_report_refs_pkey PRIMARY KEY (id)
);
COMMENT ON TABLE public.ir_interrogation_report_refs IS 'Interrogation report child table for a report-reference id list associated with the interrogation report.';

CREATE TABLE public.ir_jail_sentence (
    id BIGSERIAL,
    interrogation_report_id character varying(50),
    crime_num text,
    jurisdiction_ps text,
    law_section text,
    sentence_type text,
    sentence_duration_in_months integer,
    sentence_start_date date,
    sentence_end_date date,
    sentence_amount_in_inr numeric,
    jail_name text,
    date_of_jail_entry date,
    date_of_jail_release date,
    remarks text,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now(),
    CONSTRAINT ir_jail_sentence_pkey PRIMARY KEY (id)
);
COMMENT ON TABLE public.ir_jail_sentence IS 'Interrogation report child table for JAIL_SENTENCE[] (always empty in sampled data; shape unconfirmed beyond column list).';

CREATE TABLE public.ir_local_contacts (
    id SERIAL,
    interrogation_report_id character varying(50) NOT NULL,
    person_id character varying(50),
    town character varying(255),
    address text,
    jurisdiction_ps text,
    CONSTRAINT ir_local_contacts_pkey PRIMARY KEY (id)
);
COMMENT ON TABLE public.ir_local_contacts IS 'Interrogation report child table for LOCAL_CONTACTS[].';

CREATE TABLE public.ir_modus_operandi (
    id SERIAL,
    interrogation_report_id character varying(50) NOT NULL,
    crime_head character varying(255),
    crime_sub_head character varying(255),
    modus_operandi text,
    CONSTRAINT ir_modus_operandi_pkey PRIMARY KEY (id)
);
COMMENT ON TABLE public.ir_modus_operandi IS 'Interrogation report child table for MODUS_OPERANDI[].';

CREATE TABLE public.ir_new_gang_formation (
    id BIGSERIAL,
    interrogation_report_id character varying(50),
    gang_name text,
    gang_formation_date date,
    number_of_members integer,
    leader_name text,
    leader_person_id character varying(50),
    gang_objective text,
    criminal_history text,
    jurisdiction_ps text,
    active text,
    remarks text,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now(),
    CONSTRAINT ir_new_gang_formation_pkey PRIMARY KEY (id)
);
COMMENT ON TABLE public.ir_new_gang_formation IS 'Interrogation report child table for NEW_GANG_FORMATION[] (always empty in sampled data; shape unconfirmed beyond column list).';

CREATE TABLE public.ir_pending_nbw (
    id BIGSERIAL,
    interrogation_report_id character varying(50),
    nbw_number text,
    issued_date date,
    jurisdiction_ps text,
    crime_num text,
    reason_for_pending text,
    expected_execution_date date,
    remarks text,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now(),
    CONSTRAINT ir_pending_nbw_pkey PRIMARY KEY (id)
);
COMMENT ON TABLE public.ir_pending_nbw IS 'Interrogation report child table for PENDING_NBW[] (always empty in sampled data; shape unconfirmed beyond column list).';

CREATE TABLE public.ir_previous_offences_confessed (
    id SERIAL,
    interrogation_report_id character varying(50) NOT NULL,
    arrest_date date,
    arrested_by character varying(255),
    arrest_place text,
    crime_num text,
    dist_unit_division character varying(255),
    gang_member character varying(255),
    interrogated_by character varying(255),
    law_section character varying(255),
    others_identify text,
    property_recovered text,
    property_stolen text,
    ps_code text,
    remarks text,
    conviction_status character varying(100),
    bail_status character varying(100),
    court_name character varying(500),
    judge_name character varying(255),
    CONSTRAINT ir_previous_offences_confessed_pkey PRIMARY KEY (id)
);
COMMENT ON TABLE public.ir_previous_offences_confessed IS 'Interrogation report child table for PREVIOUS_OFFENCES_CONFESSED[] (always empty in sampled data; shape unconfirmed beyond column list).';

CREATE TABLE public.ir_property_disposal (
    id BIGSERIAL,
    interrogation_report_id character varying(50),
    mode_of_disposal text,
    buyer_name text,
    sold_amount_in_inr numeric,
    location_of_disposal text,
    date_of_disposal date,
    remarks text,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now(),
    CONSTRAINT ir_property_disposal_pkey PRIMARY KEY (id)
);
COMMENT ON TABLE public.ir_property_disposal IS 'Interrogation report child table for PROPERTY_DISPOSAL[] (always empty in sampled data; shape unconfirmed beyond column list).';

CREATE TABLE public.ir_regular_habits (
    id SERIAL,
    interrogation_report_id character varying(50) NOT NULL,
    habit character varying(255) NOT NULL,
    CONSTRAINT ir_regular_habits_pkey PRIMARY KEY (id)
);
COMMENT ON TABLE public.ir_regular_habits IS 'Interrogation report child table for REGULAR_HABITS[] (flat string enum list).';

CREATE TABLE public.ir_regularization_transit_warrants (
    id BIGSERIAL,
    interrogation_report_id character varying(50),
    warrant_number text,
    warrant_type text,
    issued_date date,
    jurisdiction_ps text,
    crime_num text,
    status text,
    remarks text,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now(),
    CONSTRAINT ir_regularization_transit_warrants_pkey PRIMARY KEY (id)
);
COMMENT ON TABLE public.ir_regularization_transit_warrants IS 'Interrogation report child table for REGULARIZATION_OF_TRANSIT_WARRANTS[] (always empty in sampled data; shape unconfirmed beyond column list).';

CREATE TABLE public.ir_shelter (
    id SERIAL,
    interrogation_report_id character varying(50) NOT NULL,
    preparation_of_offence text,
    after_offence text,
    regular_residency character varying(255),
    remarks text,
    other_regular_residency text,
    CONSTRAINT ir_shelter_pkey PRIMARY KEY (id)
);
COMMENT ON TABLE public.ir_shelter IS 'Interrogation report child table for SHELTER[].';

CREATE TABLE public.ir_sim_details (
    id SERIAL,
    interrogation_report_id character varying(50) NOT NULL,
    phone_number character varying(20),
    sdr text,
    imei character varying(50),
    true_caller_name character varying(255),
    person_id character varying(50),
    CONSTRAINT ir_sim_details_pkey PRIMARY KEY (id)
);
COMMENT ON TABLE public.ir_sim_details IS 'Interrogation report child table for SIM_DETAILS[].';

CREATE TABLE public.ir_sureties (
    id BIGSERIAL,
    interrogation_report_id character varying(50),
    surety_person_id character varying(50),
    surety_name text,
    relation_to_accused text,
    occupation text,
    aadhar_number text,
    pan_number text,
    house_no text,
    street_road_no text,
    locality_village text,
    area_mandal text,
    district text,
    state_ut text,
    pin_code text,
    phone_number text,
    surety_amount_in_inr numeric,
    date_of_surety date,
    remarks text,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now(),
    CONSTRAINT ir_sureties_pkey PRIMARY KEY (id)
);
COMMENT ON TABLE public.ir_sureties IS 'Interrogation report child table for SURETIES[] (always empty in sampled data; shape unconfirmed beyond column list).';

CREATE TABLE public.ir_types_of_drugs (
    id SERIAL,
    interrogation_report_id character varying(50) NOT NULL,
    type_of_drug text,
    quantity character varying(255),
    purchase_amount_in_inr text,
    mode_of_payment text,
    mode_of_transport text,
    supplier_person_id character varying(50),
    receivers_person_id character varying(50),
    CONSTRAINT ir_types_of_drugs_pkey PRIMARY KEY (id)
);
COMMENT ON TABLE public.ir_types_of_drugs IS 'Interrogation report child table for TYPES_OF_DRUGS[].';

CREATE TABLE public.ir_pending_fk (
    id SERIAL,
    ir_id character varying(50) NOT NULL,
    crime_id character varying(50) NOT NULL,
    raw_data jsonb NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    retry_count integer DEFAULT 0,
    last_retry_at timestamp without time zone,
    resolved boolean DEFAULT false,
    resolved_at timestamp without time zone,
    CONSTRAINT ir_pending_fk_pkey PRIMARY KEY (id)
);
COMMENT ON TABLE public.ir_pending_fk IS 'Retry queue for interrogation-report rows whose crime_id was not yet present in crimes at insert time (out-of-order CCTNS delivery).';


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
CREATE INDEX idx_chargesheet_acts_chargesheet_id ON public.chargesheet_acts USING btree (chargesheet_id);
CREATE INDEX idx_chargesheet_acts_sections_chargesheet_id ON public.chargesheet_acts_sections USING btree (chargesheet_id);
CREATE INDEX idx_chargesheet_accused_chargesheet_id ON public.chargesheet_accused USING btree (chargesheet_id);
CREATE INDEX idx_chargesheet_accused_person_id ON public.chargesheet_accused USING btree (accused_person_id);
CREATE INDEX idx_charge_sheet_updates_crime_id ON public.charge_sheet_updates USING btree (crime_id);
CREATE INDEX idx_fsl_crime_id ON public.fsl_case_property USING btree (crime_id);
CREATE INDEX idx_fsl_mo_id ON public.fsl_case_property USING btree (mo_id);
CREATE INDEX idx_fsl_status ON public.fsl_case_property USING btree (status);
CREATE INDEX idx_fsl_created ON public.fsl_case_property USING btree (date_created DESC NULLS LAST);

-- properties child/pending-fk tables
CREATE UNIQUE INDEX idx_pending_fk_property_id ON public.properties_pending_fk USING btree (property_id) WHERE (NOT resolved);
CREATE INDEX idx_properties_pending_fk_crime_id ON public.properties_pending_fk USING btree (crime_id);

-- interrogation_reports + remaining child tables
CREATE INDEX idx_ir_reports_crime_person ON public.interrogation_reports USING btree (crime_id, person_id);
CREATE INDEX idx_ir_reports_created_modified ON public.interrogation_reports USING btree (date_created, date_modified);
CREATE INDEX idx_ir_associate_details_ir_id ON public.ir_associate_details USING btree (interrogation_report_id);
CREATE INDEX idx_ir_consumer_details_ir_id ON public.ir_consumer_details USING btree (interrogation_report_id);
CREATE INDEX idx_ir_conviction_acquittal_ir_id ON public.ir_conviction_acquittal USING btree (interrogation_report_id);
CREATE INDEX idx_ir_defence_counsel_ir_id ON public.ir_defence_counsel USING btree (interrogation_report_id);
CREATE INDEX idx_ir_defence_counsel_person_id ON public.ir_defence_counsel USING btree (defence_counsel_person_id);
CREATE INDEX idx_ir_dopams_links_ir_id ON public.ir_dopams_links USING btree (interrogation_report_id);
CREATE INDEX idx_ir_execution_of_nbw_ir_id ON public.ir_execution_of_nbw USING btree (interrogation_report_id);
CREATE INDEX idx_ir_family_history_ir_id ON public.ir_family_history USING btree (interrogation_report_id);
CREATE INDEX idx_ir_financial_history_ir_id ON public.ir_financial_history USING btree (interrogation_report_id);
CREATE INDEX idx_ir_indulgance_before_offence_ir_id ON public.ir_indulgance_before_offence USING btree (interrogation_report_id);
CREATE INDEX idx_ir_interrogation_report_refs_ir_id ON public.ir_interrogation_report_refs USING btree (interrogation_report_id);
CREATE INDEX idx_ir_jail_sentence_ir_id ON public.ir_jail_sentence USING btree (interrogation_report_id);
CREATE INDEX idx_ir_local_contacts_ir_id ON public.ir_local_contacts USING btree (interrogation_report_id);
CREATE INDEX idx_ir_modus_operandi_ir_id ON public.ir_modus_operandi USING btree (interrogation_report_id);
CREATE INDEX idx_ir_new_gang_formation_ir_id ON public.ir_new_gang_formation USING btree (interrogation_report_id);
CREATE INDEX idx_ir_new_gang_formation_leader_person_id ON public.ir_new_gang_formation USING btree (leader_person_id);
CREATE INDEX idx_ir_pending_nbw_ir_id ON public.ir_pending_nbw USING btree (interrogation_report_id);
CREATE INDEX idx_ir_previous_offences_confessed_ir_id ON public.ir_previous_offences_confessed USING btree (interrogation_report_id);
CREATE INDEX idx_ir_property_disposal_ir_id ON public.ir_property_disposal USING btree (interrogation_report_id);
CREATE INDEX idx_ir_regular_habits_ir_id ON public.ir_regular_habits USING btree (interrogation_report_id);
CREATE INDEX idx_ir_regularization_transit_warrants_ir_id ON public.ir_regularization_transit_warrants USING btree (interrogation_report_id);
CREATE INDEX idx_ir_shelter_ir_id ON public.ir_shelter USING btree (interrogation_report_id);
CREATE INDEX idx_ir_sim_details_ir_id ON public.ir_sim_details USING btree (interrogation_report_id);
CREATE INDEX idx_ir_sureties_ir_id ON public.ir_sureties USING btree (interrogation_report_id);
CREATE INDEX idx_ir_sureties_surety_person_id ON public.ir_sureties USING btree (surety_person_id);
CREATE INDEX idx_ir_types_of_drugs_ir_id ON public.ir_types_of_drugs USING btree (interrogation_report_id);
CREATE UNIQUE INDEX idx_pending_fk_ir_id ON public.ir_pending_fk USING btree (ir_id) WHERE (NOT resolved);
CREATE INDEX idx_ir_pending_fk_crime_id ON public.ir_pending_fk USING btree (crime_id);

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
