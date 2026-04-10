--
-- PostgreSQL database dump
--

\restrict 0TnedpPqFal0ilgtgpIawQbMMHTYAC4cwxEIZ175qcLpRjNDDbvxKk2ujEvLxJw

-- Dumped from database version 16.11 (Ubuntu 16.11-1.pgdg24.04+1)
-- Dumped by pg_dump version 17.6

-- Started on 2026-04-10 14:58:09

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- TOC entry 4 (class 3079 OID 20996530)
-- Name: pg_trgm; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;


--
-- TOC entry 4539 (class 0 OID 0)
-- Dependencies: 4
-- Name: EXTENSION pg_trgm; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION pg_trgm IS 'text similarity measurement and index searching based on trigrams';


--
-- TOC entry 3 (class 3079 OID 1413511)
-- Name: uuid-ossp; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public;


--
-- TOC entry 4540 (class 0 OID 0)
-- Dependencies: 3
-- Name: EXTENSION "uuid-ossp"; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION "uuid-ossp" IS 'generate universally unique identifiers (UUIDs)';


--
-- TOC entry 2 (class 3079 OID 1397237)
-- Name: vector; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public;


--
-- TOC entry 4541 (class 0 OID 0)
-- Dependencies: 2
-- Name: EXTENSION vector; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION vector IS 'vector data type and ivfflat and hnsw access methods';


--
-- TOC entry 1230 (class 1247 OID 1412918)
-- Name: source_field_enum; Type: TYPE; Schema: public; Owner: dev_dopamas
--

CREATE TYPE public.source_field_enum AS ENUM (
    'FIR_COPY',
    'MEDIA',
    'INTERROGATION_REPORT',
    'DOPAMS_DATA',
    'IDENTITY_DETAILS',
    'MO_MEDIA',
    'uploadChargeSheet'
);


ALTER TYPE public.source_field_enum OWNER TO dev_dopamas;

--
-- TOC entry 1227 (class 1247 OID 1412908)
-- Name: source_type_enum; Type: TYPE; Schema: public; Owner: dev_dopamas
--

CREATE TYPE public.source_type_enum AS ENUM (
    'crime',
    'interrogation',
    'property',
    'person',
    'mo_seizures',
    'chargesheets',
    'case_property'
);


ALTER TYPE public.source_type_enum OWNER TO dev_dopamas;

--
-- TOC entry 468 (class 1255 OID 1412952)
-- Name: auto_generate_file_paths(); Type: FUNCTION; Schema: public; Owner: dev_dopamas
--

CREATE FUNCTION public.auto_generate_file_paths() RETURNS trigger
    LANGUAGE plpgsql
    AS $_$
DECLARE
    v_path VARCHAR(500);
    v_url VARCHAR(1000);
    v_extension VARCHAR(50);
BEGIN
    -- Only generate paths if file_id is not NULL
    IF NEW.file_id IS NOT NULL THEN
        v_path := generate_file_path(NEW.source_type, NEW.source_field, NEW.file_id);
        v_url := generate_file_url(NEW.source_type, NEW.source_field, NEW.file_id);
        
        -- Ensure no spaces in path
        IF v_path IS NOT NULL THEN
            NEW.file_path := REPLACE(TRIM(v_path), ' ', '');
        ELSE
            NEW.file_path := NULL;
        END IF;
        
        -- Generate URL with extension preservation
        IF v_url IS NOT NULL THEN
            v_url := REPLACE(TRIM(v_url), ' ', '');
            
            -- ================================================================
            -- EXTENSION PRESERVATION LOGIC (UNIVERSAL - ALL FILE TYPES)
            -- ================================================================
            -- Works for both INSERT and UPDATE operations
            -- Preserves extensions for ANY file type (not hardcoded list)
            
            IF TG_OP = 'UPDATE' AND OLD.file_url IS NOT NULL THEN
                -- UPDATE: Try to extract extension from OLD URL
                -- Regex pattern: matches any extension (letters/numbers/hyphens)
                v_extension := (regexp_matches(OLD.file_url, '\.([a-zA-Z0-9\-_]+)(?:\?|#|$)', 'g'))[1];
                
                IF v_extension IS NOT NULL AND length(trim(v_extension)) > 0 THEN
                    -- Preserve existing extension
                    NEW.file_url := v_url || '.' || lower(trim(v_extension));
                ELSE
                    -- No extension found, use generated URL
                    NEW.file_url := v_url;
                END IF;
            
            ELSIF TG_OP = 'INSERT' THEN
                -- INSERT: Check if application provided file_url with extension
                IF NEW.file_url IS NOT NULL AND NEW.file_url ~ '\.[a-zA-Z0-9\-_]+(?:\?|#|$)' THEN
                    -- Extract extension from provided URL
                    v_extension := (regexp_matches(NEW.file_url, '\.([a-zA-Z0-9\-_]+)(?:\?|#|$)', 'g'))[1];
                    
                    IF v_extension IS NOT NULL AND length(trim(v_extension)) > 0 THEN
                        -- Use generated URL with provided extension
                        NEW.file_url := v_url || '.' || lower(trim(v_extension));
                    ELSE
                        NEW.file_url := v_url;
                    END IF;
                ELSE
                    -- No extension provided, use generated URL
                    NEW.file_url := v_url;
                END IF;
            
            ELSE
                -- UPDATE with NULL OLD.file_url
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


ALTER FUNCTION public.auto_generate_file_paths() OWNER TO dev_dopamas;

--
-- TOC entry 513 (class 1255 OID 38052671)
-- Name: enforce_case_property_mo_reference(); Type: FUNCTION; Schema: public; Owner: dev_dopamas
--

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


ALTER FUNCTION public.enforce_case_property_mo_reference() OWNER TO dev_dopamas;

--
-- TOC entry 466 (class 1255 OID 1412950)
-- Name: generate_file_path(public.source_type_enum, public.source_field_enum, uuid); Type: FUNCTION; Schema: public; Owner: dev_dopamas
--

CREATE FUNCTION public.generate_file_path(p_source_type public.source_type_enum, p_source_field public.source_field_enum, p_file_id uuid) RETURNS character varying
    LANGUAGE plpgsql IMMUTABLE
    AS $$
DECLARE
    v_path VARCHAR(500);
BEGIN
    IF p_file_id IS NULL THEN
        RETURN NULL;
    END IF;

    -- Original APIs
    IF p_source_type = 'crime' AND p_source_field = 'FIR_COPY' THEN
        v_path := '/crimes/' || p_file_id::TEXT;
    ELSIF p_source_type = 'crime' AND p_source_field = 'MEDIA' THEN
        -- Backward-compatibility for historical files rows tagged as crime/MEDIA.
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

    -- New APIs
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


ALTER FUNCTION public.generate_file_path(p_source_type public.source_type_enum, p_source_field public.source_field_enum, p_file_id uuid) OWNER TO dev_dopamas;

--
-- TOC entry 467 (class 1255 OID 1412951)
-- Name: generate_file_url(public.source_type_enum, public.source_field_enum, uuid); Type: FUNCTION; Schema: public; Owner: dev_dopamas
--

CREATE FUNCTION public.generate_file_url(p_source_type public.source_type_enum, p_source_field public.source_field_enum, p_file_id uuid) RETURNS character varying
    LANGUAGE plpgsql IMMUTABLE
    AS $$
DECLARE
    v_base_url VARCHAR(255) := 'http://192.168.103.106:8080/files';
    v_path VARCHAR(500);
BEGIN
    v_path := generate_file_path(p_source_type, p_source_field, p_file_id);
    
    IF v_path IS NOT NULL THEN
        RETURN v_base_url || v_path;
    ELSE
        RETURN NULL;
    END IF;
END;
$$;


ALTER FUNCTION public.generate_file_url(p_source_type public.source_type_enum, p_source_field public.source_field_enum, p_file_id uuid) OWNER TO dev_dopamas;

--
-- TOC entry 479 (class 1255 OID 1413845)
-- Name: get_accused_crime_history(character varying); Type: FUNCTION; Schema: public; Owner: dev_dopamas
--

CREATE FUNCTION public.get_accused_crime_history(target_accused_id character varying) RETURNS TABLE(person_fingerprint character varying, matching_strategy character varying, confidence_level text, canonical_person_id character varying, full_name character varying, parent_name character varying, age integer, total_crimes integer, total_duplicate_records integer, crime_details jsonb)
    LANGUAGE plpgsql
    AS $$
        BEGIN
            RETURN QUERY
            SELECT 
                pdt.person_fingerprint,
                pdt.matching_strategy,
                CASE 
                    WHEN pdt.matching_tier = 1 THEN 'Very High (★★★★★)'
                    WHEN pdt.matching_tier = 2 THEN 'High (★★★★☆)'
                    WHEN pdt.matching_tier = 3 THEN 'Good (★★★☆☆)'
                    WHEN pdt.matching_tier = 4 THEN 'Medium (★★☆☆☆)'
                    WHEN pdt.matching_tier = 5 THEN 'Basic (★☆☆☆☆)'
                END as confidence_level,
                pdt.canonical_person_id,
                pdt.full_name,
                pdt.relative_name as parent_name,
                pdt.age,
                pdt.crime_count as total_crimes,
                pdt.person_record_count as total_duplicate_records,
                pdt.crime_details
            FROM person_deduplication_tracker pdt
            WHERE target_accused_id = ANY(pdt.all_accused_ids);
        END;
        $$;


ALTER FUNCTION public.get_accused_crime_history(target_accused_id character varying) OWNER TO dev_dopamas;

--
-- TOC entry 4542 (class 0 OID 0)
-- Dependencies: 479
-- Name: FUNCTION get_accused_crime_history(target_accused_id character varying); Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON FUNCTION public.get_accused_crime_history(target_accused_id character varying) IS 'Get complete crime history for an accused by accused_id, includes all cases across duplicate records';


--
-- TOC entry 480 (class 1255 OID 1413846)
-- Name: get_person_crime_history(character varying); Type: FUNCTION; Schema: public; Owner: dev_dopamas
--

CREATE FUNCTION public.get_person_crime_history(target_person_id character varying) RETURNS TABLE(person_fingerprint character varying, matching_strategy character varying, confidence_level text, all_person_ids text[], all_accused_ids text[], total_crimes integer, crime_details jsonb)
    LANGUAGE plpgsql
    AS $$
        BEGIN
            RETURN QUERY
            SELECT 
                pdt.person_fingerprint,
                pdt.matching_strategy,
                CASE 
                    WHEN pdt.matching_tier = 1 THEN 'Very High'
                    WHEN pdt.matching_tier = 2 THEN 'High'
                    WHEN pdt.matching_tier = 3 THEN 'Good'
                    WHEN pdt.matching_tier = 4 THEN 'Medium'
                    WHEN pdt.matching_tier = 5 THEN 'Basic'
                END as confidence_level,
                pdt.all_person_ids,
                pdt.all_accused_ids,
                pdt.crime_count as total_crimes,
                pdt.crime_details
            FROM person_deduplication_tracker pdt
            WHERE target_person_id = ANY(pdt.all_person_ids);
        END;
        $$;


ALTER FUNCTION public.get_person_crime_history(target_person_id character varying) OWNER TO dev_dopamas;

--
-- TOC entry 4543 (class 0 OID 0)
-- Dependencies: 480
-- Name: FUNCTION get_person_crime_history(target_person_id character varying); Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON FUNCTION public.get_person_crime_history(target_person_id character varying) IS 'Get complete crime history for a person by person_id, shows all duplicate person records';


--
-- TOC entry 481 (class 1255 OID 1413847)
-- Name: search_person_by_name(character varying); Type: FUNCTION; Schema: public; Owner: dev_dopamas
--

CREATE FUNCTION public.search_person_by_name(search_name character varying) RETURNS TABLE(person_fingerprint character varying, matching_strategy character varying, full_name character varying, parent_name character varying, age integer, district character varying, phone character varying, total_crimes integer, total_duplicate_records integer)
    LANGUAGE plpgsql
    AS $$
        BEGIN
            RETURN QUERY
            SELECT 
                pdt.person_fingerprint,
                pdt.matching_strategy,
                pdt.full_name,
                pdt.relative_name as parent_name,
                pdt.age,
                pdt.present_district as district,
                pdt.phone_number as phone,
                pdt.crime_count as total_crimes,
                pdt.person_record_count as total_duplicate_records
            FROM person_deduplication_tracker pdt
            WHERE LOWER(pdt.full_name) LIKE LOWER('%' || search_name || '%')
            ORDER BY pdt.crime_count DESC;
        END;
        $$;


ALTER FUNCTION public.search_person_by_name(search_name character varying) OWNER TO dev_dopamas;

--
-- TOC entry 4544 (class 0 OID 0)
-- Dependencies: 481
-- Name: FUNCTION search_person_by_name(search_name character varying); Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON FUNCTION public.search_person_by_name(search_name character varying) IS 'Search for persons by name, returns deduplicated results with crime counts';


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 233 (class 1259 OID 1397598)
-- Name: accused; Type: TABLE; Schema: public; Owner: dev_dopamas
--

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
    accused_status text
);


ALTER TABLE public.accused OWNER TO dev_dopamas;

--
-- TOC entry 4545 (class 0 OID 0)
-- Dependencies: 233
-- Name: TABLE accused; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.accused IS 'Links persons to crimes as accused with physical features';


--
-- TOC entry 4546 (class 0 OID 0)
-- Dependencies: 233
-- Name: COLUMN accused.person_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.accused.person_id IS 'Can be NULL - stub persons are created by ETL when needed';


--
-- TOC entry 4547 (class 0 OID 0)
-- Dependencies: 233
-- Name: COLUMN accused.is_ccl; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.accused.is_ccl IS 'Is Child in Conflict with Law';


--
-- TOC entry 267 (class 1259 OID 1404620)
-- Name: brief_facts_accused; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.brief_facts_accused (
    bf_accused_id uuid NOT NULL,
    crime_id character varying(50) NOT NULL,
    accused_id character varying(50),
    person_id character varying(50),
    person_code character varying(50),
    seq_num character varying(50),
    full_name character varying(500),
    alias_name character varying(255),
    age integer,
    gender character varying(20),
    occupation character varying(255),
    address text,
    phone_numbers character varying(255),
    role_in_crime text,
    key_details text,
    accused_type character varying(40),
    status text,
    is_ccl boolean,
    source_person_fields jsonb,
    source_accused_fields jsonb,
    source_summary_fields jsonb,
    date_created timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    date_modified timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    existing_accused boolean DEFAULT false,
    CONSTRAINT brief_facts_accused_accused_type_check CHECK (((accused_type IS NULL) OR ((accused_type)::text = ANY (ARRAY['peddler'::text, 'consumer'::text, 'supplier'::text, 'harbourer'::text, 'organizer_kingpin'::text, 'processor'::text, 'financier'::text, 'manufacturer'::text, 'transporter'::text, 'producer'::text]))))
);


ALTER TABLE public.brief_facts_accused OWNER TO dev_dopamas;

--
-- TOC entry 298 (class 1259 OID 22014293)
-- Name: brief_facts_drug; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.brief_facts_drug (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    crime_id character varying(50) NOT NULL,
    raw_drug_name text NOT NULL,
    raw_quantity numeric(18,6),
    raw_unit text,
    primary_drug_name text NOT NULL,
    drug_form text,
    weight_g numeric(18,6),
    weight_kg numeric(18,6),
    volume_ml numeric(18,6),
    volume_l numeric(18,6),
    count_total numeric(18,6),
    confidence_score numeric(3,2),
    extraction_metadata jsonb,
    is_commercial boolean DEFAULT false,
    seizure_worth numeric DEFAULT 0.0,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    CONSTRAINT check_has_measurements CHECK (((weight_g IS NOT NULL) OR (weight_kg IS NOT NULL) OR (volume_ml IS NOT NULL) OR (volume_l IS NOT NULL) OR (count_total IS NOT NULL)))
);


ALTER TABLE public.brief_facts_drug OWNER TO dev_dopamas;

--
-- TOC entry 232 (class 1259 OID 1397584)
-- Name: crimes; Type: TABLE; Schema: public; Owner: dev_dopamas
--

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
    additional_json_data jsonb
);


ALTER TABLE public.crimes OWNER TO dev_dopamas;

--
-- TOC entry 4549 (class 0 OID 0)
-- Dependencies: 232
-- Name: TABLE crimes; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.crimes IS 'Crime/FIR records registered at police stations';


--
-- TOC entry 4550 (class 0 OID 0)
-- Dependencies: 232
-- Name: COLUMN crimes.brief_facts; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.crimes.brief_facts IS 'Detailed description of the crime incident';


--
-- TOC entry 275 (class 1259 OID 1420054)
-- Name: disposal; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.disposal (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    crime_id character varying(50) NOT NULL,
    disposal_type text,
    disposed_at timestamp with time zone,
    disposal text,
    case_status text,
    date_created timestamp with time zone,
    date_modified timestamp with time zone
);


ALTER TABLE public.disposal OWNER TO dev_dopamas;

--
-- TOC entry 230 (class 1259 OID 1397569)
-- Name: hierarchy; Type: TABLE; Schema: public; Owner: dev_dopamas
--

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
    date_modified timestamp without time zone
);


ALTER TABLE public.hierarchy OWNER TO dev_dopamas;

--
-- TOC entry 4551 (class 0 OID 0)
-- Dependencies: 230
-- Name: TABLE hierarchy; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.hierarchy IS 'Police organizational hierarchy from ADG to Police Station in single table';


--
-- TOC entry 231 (class 1259 OID 1397576)
-- Name: persons; Type: TABLE; Schema: public; Owner: dev_dopamas
--

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
    raw_full_name character varying(500)
);


ALTER TABLE public.persons OWNER TO dev_dopamas;

--
-- TOC entry 4552 (class 0 OID 0)
-- Dependencies: 231
-- Name: TABLE persons; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.persons IS 'Personal details of individuals (accused, victims, witnesses, etc.)';


--
-- TOC entry 307 (class 1259 OID 34762906)
-- Name: accuseds_mv; Type: MATERIALIZED VIEW; Schema: public; Owner: dev_dopamas
--

CREATE MATERIALIZED VIEW public.accuseds_mv AS
 SELECT a.accused_id AS id,
    h.dist_name AS unit,
    h.ps_name AS ps,
    (EXTRACT(year FROM c.fir_date))::integer AS year,
    c.crime_id AS "crimeId",
    p.person_id AS "personId",
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
           FROM public.accused a3
          WHERE ((a3.crime_id)::text = (c.crime_id)::text)) AS "noOfAccusedInvolved",
    ( SELECT jsonb_agg(jsonb_build_object('name', p2.name, 'surname', p2.surname, 'alias', p2.alias, 'fullName', p2.full_name, 'status',
                CASE
                    WHEN ((COALESCE(bfa2.status, a2.accused_status) ~~* 'Arrest%'::text) AND (COALESCE(bfa2.status, a2.accused_status) !~~* 'Arrest Related%'::text)) THEN 'Arrested'::text
                    WHEN (COALESCE(bfa2.status, a2.accused_status) ~~* 'Surrendered%'::text) THEN 'Arrested'::text
                    WHEN (COALESCE(bfa2.status, a2.accused_status) ~~* 'Absconding'::text) THEN 'Absconding'::text
                    WHEN (COALESCE(bfa2.status, a2.accused_status) ~~* 'Arrest Related/41A CrPC Pending'::text) THEN 'Absconding'::text
                    WHEN (COALESCE(bfa2.status, a2.accused_status) ~~* '41A Cr.P.C%'::text) THEN 'Issued Notice'::text
                    WHEN (COALESCE(bfa2.status, a2.accused_status) ~~* 'High court directions%'::text) THEN 'Issued Notice'::text
                    ELSE 'Unknown'::text
                END, 'email', p2.email_id)) AS jsonb_agg
           FROM ((public.accused a2
             LEFT JOIN public.persons p2 ON (((a2.person_id)::text = (p2.person_id)::text)))
             LEFT JOIN public.brief_facts_accused bfa2 ON (((a2.accused_id)::text = (bfa2.accused_id)::text)))
          WHERE ((a2.crime_id)::text = (c.crime_id)::text)) AS "accusedDetails",
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
           FROM public.brief_facts_accused bfa_c
          WHERE ((bfa_c.accused_id)::text = (a.accused_id)::text)) AS "noOfCrimes",
    ( SELECT jsonb_agg(DISTINCT jsonb_build_object('crimeId', c2.crime_id, 'firNumber', c2.fir_num)) AS jsonb_agg
           FROM (public.accused a4
             JOIN public.crimes c2 ON (((a4.crime_id)::text = (c2.crime_id)::text)))
          WHERE ((a4.person_id)::text = (p.person_id)::text)) AS "previouslyInvolvedCases",
    ( SELECT COALESCE(array_agg(DISTINCT upper(TRIM(BOTH FROM bfd.primary_drug_name))) FILTER (WHERE ((bfd.primary_drug_name IS NOT NULL) AND (bfd.primary_drug_name <> 'NO_DRUGS_DETECTED'::text))), ARRAY[]::text[]) AS "coalesce"
           FROM public.brief_facts_drug bfd
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
           FROM public.brief_facts_drug bfd2
          WHERE ((bfd2.crime_id)::text = (c.crime_id)::text)) AS "drugWithQuantity",
    c.class_classification AS "caseClassification",
    c.case_status AS "caseStatus",
    ( SELECT jsonb_agg(DISTINCT jsonb_build_object('id', d.id, 'disposalType', d.disposal_type)) AS jsonb_agg
           FROM public.disposal d
          WHERE ((d.crime_id)::text = (c.crime_id)::text)) AS "disposalDetails"
   FROM ((((public.brief_facts_accused bfa
     JOIN public.accused a ON (((bfa.accused_id)::text = (a.accused_id)::text)))
     JOIN public.crimes c ON (((a.crime_id)::text = (c.crime_id)::text)))
     JOIN public.hierarchy h ON (((c.ps_code)::text = (h.ps_code)::text)))
     LEFT JOIN public.persons p ON (((a.person_id)::text = (p.person_id)::text)))
  WITH NO DATA;


ALTER MATERIALIZED VIEW public.accuseds_mv OWNER TO dev_dopamas;

--
-- TOC entry 309 (class 1259 OID 35391579)
-- Name: advanced_search_accuseds_mv; Type: MATERIALIZED VIEW; Schema: public; Owner: dev_dopamas
--

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
           FROM public.brief_facts_drug bfd
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
           FROM public.brief_facts_drug bfd
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
     LEFT JOIN public.brief_facts_accused bfa ON (((a.accused_id)::text = (bfa.accused_id)::text)))
  WITH NO DATA;


ALTER MATERIALIZED VIEW public.advanced_search_accuseds_mv OWNER TO dev_dopamas;

--
-- TOC entry 299 (class 1259 OID 22071829)
-- Name: advanced_search_firs; Type: VIEW; Schema: public; Owner: dev_dopamas
--

CREATE VIEW public.advanced_search_firs AS
 SELECT NULLIF(TRIM(BOTH FROM c.crime_id), ''::text) AS id,
    c.fir_num AS "firNum",
    c.fir_date AS "firDate",
    c.ps_code AS "psCode",
    h.ps_name AS "psName",
    h.dist_name AS "districtName",
    COALESCE(drug_quantities.types, '[]'::jsonb) AS "drugDetails"
   FROM ((public.crimes c
     LEFT JOIN public.hierarchy h ON (((c.ps_code)::text = (h.ps_code)::text)))
     LEFT JOIN ( SELECT aggregated.crime_id,
            jsonb_agg(jsonb_build_object('name', aggregated.primary_drug_name, 'quantityKg', aggregated.total_kg, 'quantityMl', aggregated.total_ml, 'quantityCount', aggregated.total_count, 'worth', aggregated.total_worth)) AS types
           FROM ( SELECT bfd.crime_id,
                    bfd.primary_drug_name,
                    sum(COALESCE(bfd.weight_kg, (0)::numeric)) AS total_kg,
                    sum(COALESCE(bfd.volume_ml, (0)::numeric)) AS total_ml,
                    sum(COALESCE(bfd.count_total, (0)::numeric)) AS total_count,
                    sum(COALESCE(bfd.seizure_worth, (0)::numeric)) AS total_worth
                   FROM public.brief_facts_drug bfd
                  GROUP BY bfd.crime_id, bfd.primary_drug_name) aggregated
          GROUP BY aggregated.crime_id) drug_quantities ON (((drug_quantities.crime_id)::text = (c.crime_id)::text)));


ALTER VIEW public.advanced_search_firs OWNER TO dev_dopamas;

--
-- TOC entry 310 (class 1259 OID 35416225)
-- Name: advanced_search_firs_mv; Type: MATERIALIZED VIEW; Schema: public; Owner: dev_dopamas
--

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
             LEFT JOIN public.brief_facts_accused bfa2 ON (((a2.accused_id)::text = (bfa2.accused_id)::text)))
          WHERE ((a2.crime_id)::text = (c.crime_id)::text)) AS "accusedDetails",
    ( SELECT COALESCE(array_agg(DISTINCT upper(TRIM(BOTH FROM bfd.primary_drug_name))) FILTER (WHERE ((bfd.primary_drug_name IS NOT NULL) AND (bfd.primary_drug_name <> 'NO_DRUGS_DETECTED'::text))), ARRAY[]::text[]) AS "coalesce"
           FROM public.brief_facts_drug bfd
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
           FROM public.brief_facts_drug bfd
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


ALTER MATERIALIZED VIEW public.advanced_search_firs_mv OWNER TO dev_dopamas;

--
-- TOC entry 286 (class 1259 OID 2028663)
-- Name: agent_deduplication_tracker; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.agent_deduplication_tracker (
    id integer NOT NULL,
    matching_strategy character varying(100) NOT NULL,
    uses_fuzzy_matching boolean DEFAULT false,
    match_score numeric(3,2),
    canonical_person_id character varying(50) NOT NULL,
    full_name character varying(500),
    all_person_ids text[] NOT NULL,
    all_accused_ids text[],
    all_crime_ids text[] NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.agent_deduplication_tracker OWNER TO dev_dopamas;

--
-- TOC entry 287 (class 1259 OID 2028671)
-- Name: agent_deduplication_tracker_id_seq; Type: SEQUENCE; Schema: public; Owner: dev_dopamas
--

CREATE SEQUENCE public.agent_deduplication_tracker_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.agent_deduplication_tracker_id_seq OWNER TO dev_dopamas;

--
-- TOC entry 4553 (class 0 OID 0)
-- Dependencies: 287
-- Name: agent_deduplication_tracker_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.agent_deduplication_tracker_id_seq OWNED BY public.agent_deduplication_tracker.id;


--
-- TOC entry 276 (class 1259 OID 1420494)
-- Name: arrests; Type: TABLE; Schema: public; Owner: dev_dopamas
--

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
    date_modified timestamp with time zone
);


ALTER TABLE public.arrests OWNER TO dev_dopamas;

--
-- TOC entry 266 (class 1259 OID 1404603)
-- Name: brief_facts_crime_summaries; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.brief_facts_crime_summaries (
    crime_id character varying NOT NULL,
    summary_text text NOT NULL,
    summary_json jsonb,
    word_count integer,
    processing_time_seconds numeric,
    model_name character varying DEFAULT 'mistral'::character varying,
    date_created timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    date_modified timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.brief_facts_crime_summaries OWNER TO dev_dopamas;

--
-- TOC entry 336 (class 1259 OID 38052673)
-- Name: case_property_media; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.case_property_media (
    case_property_id character varying(255) NOT NULL,
    media_index integer NOT NULL,
    file_id character varying(255),
    media_payload jsonb,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.case_property_media OWNER TO dev_dopamas;

--
-- TOC entry 283 (class 1259 OID 1639314)
-- Name: charge_sheet_updates; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.charge_sheet_updates (
    id integer NOT NULL,
    update_charge_sheet_id character varying(50) NOT NULL,
    crime_id character varying(50) NOT NULL,
    charge_sheet_no character varying(100),
    charge_sheet_date timestamp with time zone,
    charge_sheet_status character varying(100),
    taken_on_file_date timestamp with time zone,
    taken_on_file_case_type character varying(50),
    taken_on_file_court_case_no character varying(100),
    date_created timestamp with time zone,
    date_modified timestamp with time zone
);


ALTER TABLE public.charge_sheet_updates OWNER TO dev_dopamas;

--
-- TOC entry 4554 (class 0 OID 0)
-- Dependencies: 283
-- Name: TABLE charge_sheet_updates; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.charge_sheet_updates IS 'Stores charge sheet update records from DOPAMS API. Each record represents a charge sheet update with its status and court filing information.';


--
-- TOC entry 4555 (class 0 OID 0)
-- Dependencies: 283
-- Name: COLUMN charge_sheet_updates.update_charge_sheet_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.charge_sheet_updates.update_charge_sheet_id IS 'Unique identifier from the API (MongoDB ObjectId format) - REQUIRED';


--
-- TOC entry 4556 (class 0 OID 0)
-- Dependencies: 283
-- Name: COLUMN charge_sheet_updates.crime_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.charge_sheet_updates.crime_id IS 'Reference to the crime record (MongoDB ObjectId format) - REQUIRED, Foreign Key to crimes(crime_id)';


--
-- TOC entry 4557 (class 0 OID 0)
-- Dependencies: 283
-- Name: COLUMN charge_sheet_updates.charge_sheet_no; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.charge_sheet_updates.charge_sheet_no IS 'Charge sheet number (e.g., "146/2024") - NULLABLE';


--
-- TOC entry 4558 (class 0 OID 0)
-- Dependencies: 283
-- Name: COLUMN charge_sheet_updates.charge_sheet_date; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.charge_sheet_updates.charge_sheet_date IS 'Date when the charge sheet was created - NULLABLE';


--
-- TOC entry 4559 (class 0 OID 0)
-- Dependencies: 283
-- Name: COLUMN charge_sheet_updates.charge_sheet_status; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.charge_sheet_updates.charge_sheet_status IS 'Current status of the charge sheet (e.g., "Taken on File", "Filed/Check And Put Up") - NULLABLE';


--
-- TOC entry 4560 (class 0 OID 0)
-- Dependencies: 283
-- Name: COLUMN charge_sheet_updates.taken_on_file_date; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.charge_sheet_updates.taken_on_file_date IS 'Date when the charge sheet was taken on file by the court - NULLABLE';


--
-- TOC entry 4561 (class 0 OID 0)
-- Dependencies: 283
-- Name: COLUMN charge_sheet_updates.taken_on_file_case_type; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.charge_sheet_updates.taken_on_file_case_type IS 'Type of case (e.g., "SC", "CC", "NDPS", "SC NDPS") - NULLABLE';


--
-- TOC entry 4562 (class 0 OID 0)
-- Dependencies: 283
-- Name: COLUMN charge_sheet_updates.taken_on_file_court_case_no; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.charge_sheet_updates.taken_on_file_court_case_no IS 'Court case number assigned when taken on file - NULLABLE';


--
-- TOC entry 4563 (class 0 OID 0)
-- Dependencies: 283
-- Name: COLUMN charge_sheet_updates.date_created; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.charge_sheet_updates.date_created IS 'Timestamp when the record was created in the API system (from API response) - NULLABLE';


--
-- TOC entry 4564 (class 0 OID 0)
-- Dependencies: 283
-- Name: COLUMN charge_sheet_updates.date_modified; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.charge_sheet_updates.date_modified IS 'Timestamp when the update record was last modified in the API system.';


--
-- TOC entry 282 (class 1259 OID 1639313)
-- Name: charge_sheet_updates_id_seq; Type: SEQUENCE; Schema: public; Owner: dev_dopamas
--

CREATE SEQUENCE public.charge_sheet_updates_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.charge_sheet_updates_id_seq OWNER TO dev_dopamas;

--
-- TOC entry 4565 (class 0 OID 0)
-- Dependencies: 282
-- Name: charge_sheet_updates_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.charge_sheet_updates_id_seq OWNED BY public.charge_sheet_updates.id;


--
-- TOC entry 280 (class 1259 OID 1422340)
-- Name: chargesheet_accused; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.chargesheet_accused (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    chargesheet_id uuid NOT NULL,
    accused_person_id character varying(50) NOT NULL,
    charge_status character varying(30),
    requested_for_nbw boolean DEFAULT false,
    reason_for_no_charge text,
    is_person_master_present boolean DEFAULT true,
    created_at timestamp with time zone
);


ALTER TABLE public.chargesheet_accused OWNER TO dev_dopamas;

--
-- TOC entry 281 (class 1259 OID 1422360)
-- Name: chargesheet_acts; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.chargesheet_acts (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    chargesheet_id uuid NOT NULL,
    act_description text,
    section text,
    rw_required boolean DEFAULT false,
    section_description text,
    grave_particulars text,
    created_at timestamp with time zone
);


ALTER TABLE public.chargesheet_acts OWNER TO dev_dopamas;

--
-- TOC entry 340 (class 1259 OID 38245020)
-- Name: chargesheet_acts_sections; Type: TABLE; Schema: public; Owner: dev_dopamas
--

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
    date_modified timestamp with time zone
);


ALTER TABLE public.chargesheet_acts_sections OWNER TO dev_dopamas;

--
-- TOC entry 4566 (class 0 OID 0)
-- Dependencies: 340
-- Name: TABLE chargesheet_acts_sections; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.chargesheet_acts_sections IS 'Normalized sections for chargesheets. One row per section entry extracted from actsAndSections[].';


--
-- TOC entry 4567 (class 0 OID 0)
-- Dependencies: 340
-- Name: COLUMN chargesheet_acts_sections.chargesheet_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.chargesheet_acts_sections.chargesheet_id IS 'API chargeSheetId used as the logical parent key.';


--
-- TOC entry 279 (class 1259 OID 1422324)
-- Name: chargesheet_files; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.chargesheet_files (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    chargesheet_id uuid NOT NULL,
    file_id character varying(100),
    created_at timestamp with time zone
);


ALTER TABLE public.chargesheet_files OWNER TO dev_dopamas;

--
-- TOC entry 339 (class 1259 OID 38245007)
-- Name: chargesheet_media; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.chargesheet_media (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    chargesheet_id character varying(50) NOT NULL,
    media_index integer DEFAULT 0 NOT NULL,
    file_id character varying(100),
    media_payload jsonb,
    created_at timestamp with time zone,
    date_modified timestamp with time zone
);


ALTER TABLE public.chargesheet_media OWNER TO dev_dopamas;

--
-- TOC entry 4568 (class 0 OID 0)
-- Dependencies: 339
-- Name: TABLE chargesheet_media; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.chargesheet_media IS 'Normalized media references for chargesheets. One row per uploadChargeSheet item.';


--
-- TOC entry 4569 (class 0 OID 0)
-- Dependencies: 339
-- Name: COLUMN chargesheet_media.chargesheet_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.chargesheet_media.chargesheet_id IS 'API chargeSheetId used as the logical parent key.';


--
-- TOC entry 4570 (class 0 OID 0)
-- Dependencies: 339
-- Name: COLUMN chargesheet_media.file_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.chargesheet_media.file_id IS 'uploadChargeSheet.fileId from the API payload.';


--
-- TOC entry 278 (class 1259 OID 1422309)
-- Name: chargesheets; Type: TABLE; Schema: public; Owner: dev_dopamas
--

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
    charge_sheet_id character varying(50)
);


ALTER TABLE public.chargesheets OWNER TO dev_dopamas;

--
-- TOC entry 4571 (class 0 OID 0)
-- Dependencies: 278
-- Name: COLUMN chargesheets.charge_sheet_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.chargesheets.charge_sheet_id IS 'API chargeSheetId. Natural key used by the chargesheets ETL for overwrite semantics.';


--
-- TOC entry 269 (class 1259 OID 1412929)
-- Name: files; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.files (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    source_type public.source_type_enum NOT NULL,
    source_field public.source_field_enum NOT NULL,
    parent_id character varying(255) NOT NULL,
    file_id uuid,
    has_field boolean DEFAULT true,
    is_empty boolean DEFAULT false,
    file_path character varying(500),
    file_url character varying(1000),
    file_index integer,
    identity_type character varying(255),
    identity_number character varying(255),
    notes text,
    downloaded_at timestamp without time zone,
    is_downloaded boolean DEFAULT false,
    download_error text,
    download_attempts integer DEFAULT 0,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.files OWNER TO dev_dopamas;

--
-- TOC entry 4572 (class 0 OID 0)
-- Dependencies: 269
-- Name: TABLE files; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.files IS 'Stores file references (UUIDs) from various sources (crimes, interrogations, properties, persons)';


--
-- TOC entry 4573 (class 0 OID 0)
-- Dependencies: 269
-- Name: COLUMN files.source_type; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.files.source_type IS 'Type of source: crime, interrogation, property, or person';


--
-- TOC entry 4574 (class 0 OID 0)
-- Dependencies: 269
-- Name: COLUMN files.source_field; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.files.source_field IS 'Field name from source: FIR_COPY, MEDIA, INTERROGATION_REPORT, DOPAMS_DATA, IDENTITY_DETAILS';


--
-- TOC entry 4575 (class 0 OID 0)
-- Dependencies: 269
-- Name: COLUMN files.parent_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.files.parent_id IS 'ID of the parent record (crime_id, interrogation_report_id, property_id, or person_id)';


--
-- TOC entry 4576 (class 0 OID 0)
-- Dependencies: 269
-- Name: COLUMN files.file_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.files.file_id IS 'The actual file UUID that can be used to fetch the file via API. NULL if field exists but has no file.';


--
-- TOC entry 4577 (class 0 OID 0)
-- Dependencies: 269
-- Name: COLUMN files.has_field; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.files.has_field IS 'TRUE if the field exists in API response, FALSE if field is missing';


--
-- TOC entry 4578 (class 0 OID 0)
-- Dependencies: 269
-- Name: COLUMN files.is_empty; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.files.is_empty IS 'TRUE if field exists but is null or empty array';


--
-- TOC entry 4579 (class 0 OID 0)
-- Dependencies: 269
-- Name: COLUMN files.file_path; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.files.file_path IS 'Relative file path on Tomcat server (auto-generated, NULL if file_id is NULL)';


--
-- TOC entry 4580 (class 0 OID 0)
-- Dependencies: 269
-- Name: COLUMN files.file_url; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.files.file_url IS 'Full file URL on Tomcat server (auto-generated, NULL if file_id is NULL)';


--
-- TOC entry 4581 (class 0 OID 0)
-- Dependencies: 269
-- Name: COLUMN files.file_index; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.files.file_index IS 'Index position in array (for MEDIA arrays with multiple files)';


--
-- TOC entry 4582 (class 0 OID 0)
-- Dependencies: 269
-- Name: COLUMN files.identity_type; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.files.identity_type IS 'For IDENTITY_DETAILS: type of identity document (Aadhar Card, Passport, etc.)';


--
-- TOC entry 4583 (class 0 OID 0)
-- Dependencies: 269
-- Name: COLUMN files.identity_number; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.files.identity_number IS 'For IDENTITY_DETAILS: identity document number';


--
-- TOC entry 4584 (class 0 OID 0)
-- Dependencies: 269
-- Name: COLUMN files.downloaded_at; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.files.downloaded_at IS 'Timestamp when file was successfully downloaded to media server';


--
-- TOC entry 4585 (class 0 OID 0)
-- Dependencies: 269
-- Name: COLUMN files.is_downloaded; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.files.is_downloaded IS 'Flag indicating if file has been successfully downloaded to media server';


--
-- TOC entry 4586 (class 0 OID 0)
-- Dependencies: 269
-- Name: COLUMN files.download_error; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.files.download_error IS 'Error message if file download failed';


--
-- TOC entry 4587 (class 0 OID 0)
-- Dependencies: 269
-- Name: COLUMN files.download_attempts; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.files.download_attempts IS 'Number of download attempts made';


--
-- TOC entry 4588 (class 0 OID 0)
-- Dependencies: 269
-- Name: COLUMN files.created_at; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.files.created_at IS 'Timestamp from API (DATE_CREATED or DATE_MODIFIED)';


--
-- TOC entry 308 (class 1259 OID 34844881)
-- Name: criminal_profiles_mv; Type: MATERIALIZED VIEW; Schema: public; Owner: dev_dopamas
--

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
                     LEFT JOIN public.brief_facts_accused bfa ON (((bfa.accused_id)::text = (a.accused_id)::text)))
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
             JOIN public.brief_facts_accused bfa ON (((bfa.accused_id)::text = (a.accused_id)::text)))
          WHERE ((a.person_id)::text = (p.person_id)::text)) AS "noOfCrimes",
    ( SELECT count(*) AS count
           FROM public.arrests arr
          WHERE (((arr.person_id)::text = (p.person_id)::text) AND (arr.is_arrested = true))) AS "arrestCount",
    ( SELECT max(c.fir_date) AS max
           FROM ((public.accused a
             JOIN public.crimes c ON (((a.crime_id)::text = (c.crime_id)::text)))
             LEFT JOIN public.brief_facts_accused bfa ON (((bfa.accused_id)::text = (a.accused_id)::text)))
          WHERE (((a.person_id)::text = (p.person_id)::text) AND (((COALESCE(bfa.status, a.accused_status) ~~* 'Arrest%'::text) AND (COALESCE(bfa.status, a.accused_status) !~~* 'Arrest Related%'::text)) OR (COALESCE(bfa.status, a.accused_status) ~~* 'Surrendered%'::text)))) AS "lastArrestDate",
    ( SELECT jsonb_agg(DISTINCT jsonb_build_object('crimeId', bfa.crime_id, 'accusedId', bfa.accused_id, 'accusedRole', bfa.accused_type)) AS jsonb_agg
           FROM (public.accused a
             JOIN public.brief_facts_accused bfa ON (((bfa.accused_id)::text = (a.accused_id)::text)))
          WHERE ((a.person_id)::text = (p.person_id)::text)) AS "crimesInvolved",
    ( SELECT array_agg(DISTINCT bfa.accused_type) FILTER (WHERE (bfa.accused_type IS NOT NULL)) AS array_agg
           FROM (public.accused a
             JOIN public.brief_facts_accused bfa ON (((bfa.accused_id)::text = (a.accused_id)::text)))
          WHERE ((a.person_id)::text = (p.person_id)::text)) AS "accusedRoles",
    ( SELECT jsonb_agg(DISTINCT jsonb_build_object('id', c.crime_id, 'value', c.fir_num)) AS jsonb_agg
           FROM (public.accused a
             JOIN public.crimes c ON (((a.crime_id)::text = (c.crime_id)::text)))
          WHERE ((a.person_id)::text = (p.person_id)::text)) AS "previouslyInvolvedCases",
    ( SELECT COALESCE(array_agg(DISTINCT upper(TRIM(BOTH FROM bfd.primary_drug_name))) FILTER (WHERE ((bfd.primary_drug_name IS NOT NULL) AND (bfd.primary_drug_name <> 'NO_DRUGS_DETECTED'::text))), ARRAY[]::text[]) AS "coalesce"
           FROM (public.accused a_drug
             JOIN public.brief_facts_drug bfd ON (((bfd.crime_id)::text = (a_drug.crime_id)::text)))
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


ALTER MATERIALIZED VIEW public.criminal_profiles_mv OWNER TO dev_dopamas;

--
-- TOC entry 288 (class 1259 OID 2028672)
-- Name: dedup_cluster_state; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.dedup_cluster_state (
    id bigint NOT NULL,
    cluster_id integer NOT NULL,
    person_index integer NOT NULL,
    person_id character varying(50) NOT NULL,
    is_representative boolean DEFAULT false NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


ALTER TABLE public.dedup_cluster_state OWNER TO dev_dopamas;

--
-- TOC entry 289 (class 1259 OID 2028677)
-- Name: dedup_cluster_state_id_seq; Type: SEQUENCE; Schema: public; Owner: dev_dopamas
--

CREATE SEQUENCE public.dedup_cluster_state_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.dedup_cluster_state_id_seq OWNER TO dev_dopamas;

--
-- TOC entry 4589 (class 0 OID 0)
-- Dependencies: 289
-- Name: dedup_cluster_state_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.dedup_cluster_state_id_seq OWNED BY public.dedup_cluster_state.id;


--
-- TOC entry 290 (class 1259 OID 2028678)
-- Name: dedup_comparison_progress; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.dedup_comparison_progress (
    id bigint NOT NULL,
    person_i_index integer NOT NULL,
    person_j_index integer NOT NULL,
    person_i_id character varying(50) NOT NULL,
    person_j_id character varying(50) NOT NULL,
    match_score_numeric double precision NOT NULL,
    is_match boolean DEFAULT false NOT NULL,
    matching_method character varying(100),
    completed_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


ALTER TABLE public.dedup_comparison_progress OWNER TO dev_dopamas;

--
-- TOC entry 291 (class 1259 OID 2028683)
-- Name: dedup_comparison_progress_id_seq; Type: SEQUENCE; Schema: public; Owner: dev_dopamas
--

CREATE SEQUENCE public.dedup_comparison_progress_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.dedup_comparison_progress_id_seq OWNER TO dev_dopamas;

--
-- TOC entry 4590 (class 0 OID 0)
-- Dependencies: 291
-- Name: dedup_comparison_progress_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.dedup_comparison_progress_id_seq OWNED BY public.dedup_comparison_progress.id;


--
-- TOC entry 292 (class 1259 OID 2028684)
-- Name: dedup_run_metadata; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.dedup_run_metadata (
    id bigint NOT NULL,
    run_id character varying(50) NOT NULL,
    total_persons integer NOT NULL,
    last_processed_index integer DEFAULT 0 NOT NULL,
    status character varying(20) DEFAULT 'running'::character varying NOT NULL,
    started_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    completed_at timestamp without time zone
);


ALTER TABLE public.dedup_run_metadata OWNER TO dev_dopamas;

--
-- TOC entry 293 (class 1259 OID 2028691)
-- Name: dedup_run_metadata_id_seq; Type: SEQUENCE; Schema: public; Owner: dev_dopamas
--

CREATE SEQUENCE public.dedup_run_metadata_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.dedup_run_metadata_id_seq OWNER TO dev_dopamas;

--
-- TOC entry 4591 (class 0 OID 0)
-- Dependencies: 293
-- Name: dedup_run_metadata_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.dedup_run_metadata_id_seq OWNED BY public.dedup_run_metadata.id;


--
-- TOC entry 295 (class 1259 OID 20996612)
-- Name: drug_categories; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.drug_categories (
    id integer NOT NULL,
    raw_name text NOT NULL,
    standard_name text NOT NULL,
    category_group text NOT NULL,
    is_verified boolean DEFAULT true,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.drug_categories OWNER TO dev_dopamas;

--
-- TOC entry 294 (class 1259 OID 20996611)
-- Name: drug_categories_id_seq; Type: SEQUENCE; Schema: public; Owner: dev_dopamas
--

CREATE SEQUENCE public.drug_categories_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.drug_categories_id_seq OWNER TO dev_dopamas;

--
-- TOC entry 4592 (class 0 OID 0)
-- Dependencies: 294
-- Name: drug_categories_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.drug_categories_id_seq OWNED BY public.drug_categories.id;


--
-- TOC entry 297 (class 1259 OID 20996626)
-- Name: drug_ignore_list; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.drug_ignore_list (
    id integer NOT NULL,
    term text NOT NULL,
    reason text,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.drug_ignore_list OWNER TO dev_dopamas;

--
-- TOC entry 296 (class 1259 OID 20996625)
-- Name: drug_ignore_list_id_seq; Type: SEQUENCE; Schema: public; Owner: dev_dopamas
--

CREATE SEQUENCE public.drug_ignore_list_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.drug_ignore_list_id_seq OWNER TO dev_dopamas;

--
-- TOC entry 4593 (class 0 OID 0)
-- Dependencies: 296
-- Name: drug_ignore_list_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.drug_ignore_list_id_seq OWNED BY public.drug_ignore_list.id;


--
-- TOC entry 270 (class 1259 OID 1412954)
-- Name: files_summary; Type: VIEW; Schema: public; Owner: dev_dopamas
--

CREATE VIEW public.files_summary AS
 SELECT source_type,
    source_field,
    count(DISTINCT parent_id) AS parent_records_count,
    count(*) AS total_files,
    count(DISTINCT file_id) AS unique_files
   FROM public.files
  GROUP BY source_type, source_field;


ALTER VIEW public.files_summary OWNER TO dev_dopamas;

--
-- TOC entry 284 (class 1259 OID 1639518)
-- Name: fsl_case_property; Type: TABLE; Schema: public; Owner: dev_dopamas
--

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
    property_received_back boolean
);


ALTER TABLE public.fsl_case_property OWNER TO dev_dopamas;

--
-- TOC entry 4594 (class 0 OID 0)
-- Dependencies: 284
-- Name: TABLE fsl_case_property; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.fsl_case_property IS 'Main table storing case property records from DOPAMS API';


--
-- TOC entry 4595 (class 0 OID 0)
-- Dependencies: 284
-- Name: COLUMN fsl_case_property.case_property_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.fsl_case_property.case_property_id IS 'Primary key from API (CASE_PROPERTY_ID) - MongoDB ObjectId (24 hex characters)';


--
-- TOC entry 4596 (class 0 OID 0)
-- Dependencies: 284
-- Name: COLUMN fsl_case_property.crime_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.fsl_case_property.crime_id IS 'Reference to crime/case (CRIME_ID) - Foreign key to crimes table';


--
-- TOC entry 4597 (class 0 OID 0)
-- Dependencies: 284
-- Name: COLUMN fsl_case_property.mo_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.fsl_case_property.mo_id IS 'Material Object ID (MO_ID)';


--
-- TOC entry 4598 (class 0 OID 0)
-- Dependencies: 284
-- Name: COLUMN fsl_case_property.status; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.fsl_case_property.status IS 'Current status (e.g., Send To FSL, Send To Court)';


--
-- TOC entry 4599 (class 0 OID 0)
-- Dependencies: 284
-- Name: COLUMN fsl_case_property.date_created; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.fsl_case_property.date_created IS 'Record creation timestamp from API (DATE_CREATED)';


--
-- TOC entry 4600 (class 0 OID 0)
-- Dependencies: 284
-- Name: COLUMN fsl_case_property.date_modified; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.fsl_case_property.date_modified IS 'Record modification timestamp from API (DATE_MODIFIED)';


--
-- TOC entry 4601 (class 0 OID 0)
-- Dependencies: 284
-- Name: COLUMN fsl_case_property.fsl_no; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.fsl_case_property.fsl_no IS 'FSL case number';


--
-- TOC entry 4602 (class 0 OID 0)
-- Dependencies: 284
-- Name: COLUMN fsl_case_property.report_received; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.fsl_case_property.report_received IS 'Whether FSL report has been received';


--
-- TOC entry 4603 (class 0 OID 0)
-- Dependencies: 284
-- Name: COLUMN fsl_case_property.property_received_back; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.fsl_case_property.property_received_back IS 'Whether property has been received back';


--
-- TOC entry 235 (class 1259 OID 1397634)
-- Name: interrogation_reports; Type: TABLE; Schema: public; Owner: dev_dopamas
--

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
    date_modified timestamp without time zone
);


ALTER TABLE public.interrogation_reports OWNER TO dev_dopamas;

--
-- TOC entry 4604 (class 0 OID 0)
-- Dependencies: 235
-- Name: TABLE interrogation_reports; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.interrogation_reports IS 'Main table storing Interrogation Report (IR) data. All common fields are stored as columns for easy querying.';


--
-- TOC entry 257 (class 1259 OID 1397797)
-- Name: ir_associate_details; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.ir_associate_details (
    id integer NOT NULL,
    interrogation_report_id character varying(50) NOT NULL,
    person_id character varying(50),
    gang character varying(255),
    relation text
);


ALTER TABLE public.ir_associate_details OWNER TO dev_dopamas;

--
-- TOC entry 4605 (class 0 OID 0)
-- Dependencies: 257
-- Name: TABLE ir_associate_details; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.ir_associate_details IS 'Associate information for each IR record. One record per associate.';


--
-- TOC entry 249 (class 1259 OID 1397741)
-- Name: ir_consumer_details; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.ir_consumer_details (
    id integer NOT NULL,
    interrogation_report_id character varying(50) NOT NULL,
    consumer_person_id character varying(50),
    place_of_consumption text,
    other_sources text,
    other_sources_phone_no character varying(20),
    aadhar_card_number character varying(20),
    aadhar_card_number_phone_no character varying(20)
);


ALTER TABLE public.ir_consumer_details OWNER TO dev_dopamas;

--
-- TOC entry 4606 (class 0 OID 0)
-- Dependencies: 249
-- Name: TABLE ir_consumer_details; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.ir_consumer_details IS 'Consumer information for each IR record. One record per consumer.';


--
-- TOC entry 255 (class 1259 OID 1397783)
-- Name: ir_defence_counsel; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.ir_defence_counsel (
    id integer NOT NULL,
    interrogation_report_id character varying(50) NOT NULL,
    dist_division character varying(255),
    ps_code text,
    crime_num text,
    law_section text,
    sc_cc_num text,
    defence_counsel_address text,
    defence_counsel_phone character varying(20),
    assistance text,
    defence_counsel_person_id character varying(50)
);


ALTER TABLE public.ir_defence_counsel OWNER TO dev_dopamas;

--
-- TOC entry 4607 (class 0 OID 0)
-- Dependencies: 255
-- Name: TABLE ir_defence_counsel; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.ir_defence_counsel IS 'Defence counsel information for each IR record. One record per counsel.';


--
-- TOC entry 265 (class 1259 OID 1397857)
-- Name: ir_dopams_links; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.ir_dopams_links (
    id integer NOT NULL,
    interrogation_report_id character varying(50) NOT NULL,
    phone_number character varying(20),
    dopams_data text[]
);


ALTER TABLE public.ir_dopams_links OWNER TO dev_dopamas;

--
-- TOC entry 4608 (class 0 OID 0)
-- Dependencies: 265
-- Name: TABLE ir_dopams_links; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.ir_dopams_links IS 'DOPAMS links for each IR record. One record per phone number with DOPAMS data.';


--
-- TOC entry 237 (class 1259 OID 1397654)
-- Name: ir_family_history; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.ir_family_history (
    id integer NOT NULL,
    interrogation_report_id character varying(50) NOT NULL,
    person_id character varying(50),
    relation text,
    family_member_peculiarity text,
    criminal_background boolean DEFAULT false,
    is_alive boolean DEFAULT true,
    family_stay_together boolean DEFAULT true
);


ALTER TABLE public.ir_family_history OWNER TO dev_dopamas;

--
-- TOC entry 4609 (class 0 OID 0)
-- Dependencies: 237
-- Name: TABLE ir_family_history; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.ir_family_history IS 'Family members associated with each IR record. One record per family member.';


--
-- TOC entry 247 (class 1259 OID 1397727)
-- Name: ir_financial_history; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.ir_financial_history (
    id integer NOT NULL,
    interrogation_report_id character varying(50) NOT NULL,
    account_holder_person_id character varying(50),
    pan_no character varying(50),
    upi_id character varying(255),
    name_of_bank character varying(255),
    account_number text,
    branch_name character varying(255),
    ifsc_code character varying(50),
    immovable_property_acquired text,
    movable_property_acquired text
);


ALTER TABLE public.ir_financial_history OWNER TO dev_dopamas;

--
-- TOC entry 4610 (class 0 OID 0)
-- Dependencies: 247
-- Name: TABLE ir_financial_history; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.ir_financial_history IS 'Financial information for each IR record. One record per financial account/history.';


--
-- TOC entry 239 (class 1259 OID 1397671)
-- Name: ir_local_contacts; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.ir_local_contacts (
    id integer NOT NULL,
    interrogation_report_id character varying(50) NOT NULL,
    person_id character varying(50),
    town character varying(255),
    address text,
    jurisdiction_ps text
);


ALTER TABLE public.ir_local_contacts OWNER TO dev_dopamas;

--
-- TOC entry 4611 (class 0 OID 0)
-- Dependencies: 239
-- Name: TABLE ir_local_contacts; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.ir_local_contacts IS 'Local contacts for each IR record. One record per contact.';


--
-- TOC entry 251 (class 1259 OID 1397755)
-- Name: ir_modus_operandi; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.ir_modus_operandi (
    id integer NOT NULL,
    interrogation_report_id character varying(50) NOT NULL,
    crime_head character varying(255),
    crime_sub_head character varying(255),
    modus_operandi text
);


ALTER TABLE public.ir_modus_operandi OWNER TO dev_dopamas;

--
-- TOC entry 4612 (class 0 OID 0)
-- Dependencies: 251
-- Name: TABLE ir_modus_operandi; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.ir_modus_operandi IS 'Modus operandi information for each IR record. One record per MO entry.';


--
-- TOC entry 253 (class 1259 OID 1397769)
-- Name: ir_previous_offences_confessed; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.ir_previous_offences_confessed (
    id integer NOT NULL,
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
    judge_name character varying(255)
);


ALTER TABLE public.ir_previous_offences_confessed OWNER TO dev_dopamas;

--
-- TOC entry 4613 (class 0 OID 0)
-- Dependencies: 253
-- Name: TABLE ir_previous_offences_confessed; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.ir_previous_offences_confessed IS 'Previous offences confessed for each IR record. One record per offence.';


--
-- TOC entry 4614 (class 0 OID 0)
-- Dependencies: 253
-- Name: COLUMN ir_previous_offences_confessed.conviction_status; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_previous_offences_confessed.conviction_status IS 'Status of conviction (if relevant to the offense)';


--
-- TOC entry 4615 (class 0 OID 0)
-- Dependencies: 253
-- Name: COLUMN ir_previous_offences_confessed.bail_status; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_previous_offences_confessed.bail_status IS 'Bail status during this offense';


--
-- TOC entry 4616 (class 0 OID 0)
-- Dependencies: 253
-- Name: COLUMN ir_previous_offences_confessed.court_name; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_previous_offences_confessed.court_name IS 'Court handling the case';


--
-- TOC entry 4617 (class 0 OID 0)
-- Dependencies: 253
-- Name: COLUMN ir_previous_offences_confessed.judge_name; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_previous_offences_confessed.judge_name IS 'Judge handling the case';


--
-- TOC entry 241 (class 1259 OID 1397685)
-- Name: ir_regular_habits; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.ir_regular_habits (
    id integer NOT NULL,
    interrogation_report_id character varying(50) NOT NULL,
    habit character varying(255) NOT NULL
);


ALTER TABLE public.ir_regular_habits OWNER TO dev_dopamas;

--
-- TOC entry 4618 (class 0 OID 0)
-- Dependencies: 241
-- Name: TABLE ir_regular_habits; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.ir_regular_habits IS 'Regular habits for each IR record. One record per habit (junction table for array of strings).';


--
-- TOC entry 259 (class 1259 OID 1397811)
-- Name: ir_shelter; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.ir_shelter (
    id integer NOT NULL,
    interrogation_report_id character varying(50) NOT NULL,
    preparation_of_offence text,
    after_offence text,
    regular_residency character varying(255),
    remarks text,
    other_regular_residency text
);


ALTER TABLE public.ir_shelter OWNER TO dev_dopamas;

--
-- TOC entry 4619 (class 0 OID 0)
-- Dependencies: 259
-- Name: TABLE ir_shelter; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.ir_shelter IS 'Shelter information for each IR record. One record per shelter entry.';


--
-- TOC entry 245 (class 1259 OID 1397713)
-- Name: ir_sim_details; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.ir_sim_details (
    id integer NOT NULL,
    interrogation_report_id character varying(50) NOT NULL,
    phone_number character varying(20),
    sdr text,
    imei character varying(50),
    true_caller_name character varying(255),
    person_id character varying(50)
);


ALTER TABLE public.ir_sim_details OWNER TO dev_dopamas;

--
-- TOC entry 4620 (class 0 OID 0)
-- Dependencies: 245
-- Name: TABLE ir_sim_details; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.ir_sim_details IS 'SIM card details for each IR record. One record per SIM card.';


--
-- TOC entry 243 (class 1259 OID 1397699)
-- Name: ir_types_of_drugs; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.ir_types_of_drugs (
    id integer NOT NULL,
    interrogation_report_id character varying(50) NOT NULL,
    type_of_drug character varying(255),
    quantity character varying(255),
    purchase_amount_in_inr text,
    mode_of_payment text,
    mode_of_transport text,
    supplier_person_id character varying(50),
    receivers_person_id character varying(50)
);


ALTER TABLE public.ir_types_of_drugs OWNER TO dev_dopamas;

--
-- TOC entry 4621 (class 0 OID 0)
-- Dependencies: 243
-- Name: TABLE ir_types_of_drugs; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.ir_types_of_drugs IS 'Drug information for each IR record. One record per drug type.';


--
-- TOC entry 277 (class 1259 OID 1420933)
-- Name: mo_seizures; Type: TABLE; Schema: public; Owner: dev_dopamas
--

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
    date_modified timestamp with time zone
);


ALTER TABLE public.mo_seizures OWNER TO dev_dopamas;

--
-- TOC entry 4622 (class 0 OID 0)
-- Dependencies: 277
-- Name: COLUMN mo_seizures.pos_latitude; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.mo_seizures.pos_latitude IS 'Latitude in decimal degrees';


--
-- TOC entry 4623 (class 0 OID 0)
-- Dependencies: 277
-- Name: COLUMN mo_seizures.pos_longitude; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.mo_seizures.pos_longitude IS 'Longitude in decimal degrees';


--
-- TOC entry 234 (class 1259 OID 1397619)
-- Name: properties; Type: TABLE; Schema: public; Owner: dev_dopamas
--

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
    date_modified timestamp with time zone
);


ALTER TABLE public.properties OWNER TO dev_dopamas;

--
-- TOC entry 4624 (class 0 OID 0)
-- Dependencies: 234
-- Name: TABLE properties; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.properties IS 'Seized and recovered property details linked to crimes';


--
-- TOC entry 4625 (class 0 OID 0)
-- Dependencies: 234
-- Name: COLUMN properties.case_property_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.properties.case_property_id IS 'Reference to related case property (may be null)';


--
-- TOC entry 4626 (class 0 OID 0)
-- Dependencies: 234
-- Name: COLUMN properties.additional_details; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.properties.additional_details IS 'JSONB field containing flexible additional data (drug details, vehicle info, etc.)';


--
-- TOC entry 4627 (class 0 OID 0)
-- Dependencies: 234
-- Name: COLUMN properties.media; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.properties.media IS 'JSONB array of media attachments';


--
-- TOC entry 338 (class 1259 OID 38213856)
-- Name: firs_mv; Type: MATERIALIZED VIEW; Schema: public; Owner: dev_dopamas
--

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
           FROM public.brief_facts_accused bfa
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
           FROM public.brief_facts_accused bfa
          WHERE ((bfa.crime_id)::text = (c.crime_id)::text)) AS "accusedDetails",
    ( SELECT COALESCE(array_agg(DISTINCT upper(TRIM(BOTH FROM bfd.primary_drug_name))) FILTER (WHERE ((bfd.primary_drug_name IS NOT NULL) AND (bfd.primary_drug_name <> 'NO_DRUGS_DETECTED'::text))), ARRAY[]::text[]) AS "coalesce"
           FROM public.brief_facts_drug bfd
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
                   FROM public.brief_facts_drug bfd2
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


ALTER MATERIALIZED VIEW public.firs_mv OWNER TO dev_dopamas;

--
-- TOC entry 285 (class 1259 OID 1639532)
-- Name: fsl_case_property_media; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.fsl_case_property_media (
    media_id uuid NOT NULL,
    case_property_id character varying(255) NOT NULL,
    file_id character varying(255)
);


ALTER TABLE public.fsl_case_property_media OWNER TO dev_dopamas;

--
-- TOC entry 4628 (class 0 OID 0)
-- Dependencies: 285
-- Name: TABLE fsl_case_property_media; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.fsl_case_property_media IS 'Media files associated with case properties';


--
-- TOC entry 302 (class 1259 OID 24850397)
-- Name: geo_countries; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.geo_countries (
    country_name text,
    state_name text,
    timezone text
);


ALTER TABLE public.geo_countries OWNER TO dev_dopamas;

--
-- TOC entry 301 (class 1259 OID 23469812)
-- Name: geo_reference; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.geo_reference (
    id integer NOT NULL,
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
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.geo_reference OWNER TO dev_dopamas;

--
-- TOC entry 300 (class 1259 OID 23469811)
-- Name: geo_reference_id_seq; Type: SEQUENCE; Schema: public; Owner: dev_dopamas
--

CREATE SEQUENCE public.geo_reference_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.geo_reference_id_seq OWNER TO dev_dopamas;

--
-- TOC entry 4629 (class 0 OID 0)
-- Dependencies: 300
-- Name: geo_reference_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.geo_reference_id_seq OWNED BY public.geo_reference.id;


--
-- TOC entry 256 (class 1259 OID 1397796)
-- Name: ir_associate_details_id_seq; Type: SEQUENCE; Schema: public; Owner: dev_dopamas
--

CREATE SEQUENCE public.ir_associate_details_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.ir_associate_details_id_seq OWNER TO dev_dopamas;

--
-- TOC entry 4630 (class 0 OID 0)
-- Dependencies: 256
-- Name: ir_associate_details_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.ir_associate_details_id_seq OWNED BY public.ir_associate_details.id;


--
-- TOC entry 328 (class 1259 OID 37992937)
-- Name: ir_conviction_acquittal; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.ir_conviction_acquittal (
    id integer NOT NULL,
    interrogation_report_id character varying(50) NOT NULL,
    crime_num character varying(255),
    jurisdiction_ps character varying(255),
    court_name character varying(500),
    judge_name character varying(255),
    law_section character varying(255),
    verdict character varying(100),
    verdict_date date,
    reason_if_acquitted text,
    conviction_remarks text,
    fine_amount_in_inr text,
    sentence_if_convicted text,
    appeal_status character varying(100),
    appeal_court character varying(500),
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.ir_conviction_acquittal OWNER TO dev_dopamas;

--
-- TOC entry 4631 (class 0 OID 0)
-- Dependencies: 328
-- Name: TABLE ir_conviction_acquittal; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.ir_conviction_acquittal IS 'Conviction/acquittal details for each IR record. One record per case verdict entry.';


--
-- TOC entry 4632 (class 0 OID 0)
-- Dependencies: 328
-- Name: COLUMN ir_conviction_acquittal.interrogation_report_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_conviction_acquittal.interrogation_report_id IS 'Foreign key to interrogation_reports table';


--
-- TOC entry 4633 (class 0 OID 0)
-- Dependencies: 328
-- Name: COLUMN ir_conviction_acquittal.crime_num; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_conviction_acquittal.crime_num IS 'Associated crime number';


--
-- TOC entry 4634 (class 0 OID 0)
-- Dependencies: 328
-- Name: COLUMN ir_conviction_acquittal.court_name; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_conviction_acquittal.court_name IS 'Court name where verdict was delivered';


--
-- TOC entry 4635 (class 0 OID 0)
-- Dependencies: 328
-- Name: COLUMN ir_conviction_acquittal.verdict; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_conviction_acquittal.verdict IS 'Verdict (Convicted, Acquitted, Discharged, etc.)';


--
-- TOC entry 4636 (class 0 OID 0)
-- Dependencies: 328
-- Name: COLUMN ir_conviction_acquittal.verdict_date; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_conviction_acquittal.verdict_date IS 'Date of verdict';


--
-- TOC entry 4637 (class 0 OID 0)
-- Dependencies: 328
-- Name: COLUMN ir_conviction_acquittal.reason_if_acquitted; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_conviction_acquittal.reason_if_acquitted IS 'Reason for acquittal if applicable';


--
-- TOC entry 4638 (class 0 OID 0)
-- Dependencies: 328
-- Name: COLUMN ir_conviction_acquittal.sentence_if_convicted; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_conviction_acquittal.sentence_if_convicted IS 'Details of sentence if convicted';


--
-- TOC entry 4639 (class 0 OID 0)
-- Dependencies: 328
-- Name: COLUMN ir_conviction_acquittal.appeal_status; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_conviction_acquittal.appeal_status IS 'Status of any appeal (Pending, Dismissed, Allowed, etc.)';


--
-- TOC entry 318 (class 1259 OID 37992857)
-- Name: ir_execution_of_nbw; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.ir_execution_of_nbw (
    id integer NOT NULL,
    interrogation_report_id character varying(50) NOT NULL,
    nbw_number character varying(255),
    issued_date date,
    executed_date date,
    jurisdiction_ps character varying(255),
    crime_num character varying(255),
    executed_by character varying(255),
    place_of_execution text,
    remarks text,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.ir_execution_of_nbw OWNER TO dev_dopamas;

--
-- TOC entry 4640 (class 0 OID 0)
-- Dependencies: 318
-- Name: TABLE ir_execution_of_nbw; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.ir_execution_of_nbw IS 'Execution of NBW (Non-Bailable Warrant) for each IR record. One record per NBW execution entry.';


--
-- TOC entry 4641 (class 0 OID 0)
-- Dependencies: 318
-- Name: COLUMN ir_execution_of_nbw.interrogation_report_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_execution_of_nbw.interrogation_report_id IS 'Foreign key to interrogation_reports table';


--
-- TOC entry 4642 (class 0 OID 0)
-- Dependencies: 318
-- Name: COLUMN ir_execution_of_nbw.nbw_number; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_execution_of_nbw.nbw_number IS 'NBW number/reference';


--
-- TOC entry 4643 (class 0 OID 0)
-- Dependencies: 318
-- Name: COLUMN ir_execution_of_nbw.issued_date; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_execution_of_nbw.issued_date IS 'Date NBW was issued';


--
-- TOC entry 4644 (class 0 OID 0)
-- Dependencies: 318
-- Name: COLUMN ir_execution_of_nbw.executed_date; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_execution_of_nbw.executed_date IS 'Date NBW was executed';


--
-- TOC entry 4645 (class 0 OID 0)
-- Dependencies: 318
-- Name: COLUMN ir_execution_of_nbw.jurisdiction_ps; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_execution_of_nbw.jurisdiction_ps IS 'Police station where executed';


--
-- TOC entry 4646 (class 0 OID 0)
-- Dependencies: 318
-- Name: COLUMN ir_execution_of_nbw.crime_num; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_execution_of_nbw.crime_num IS 'Associated crime number';


--
-- TOC entry 4647 (class 0 OID 0)
-- Dependencies: 318
-- Name: COLUMN ir_execution_of_nbw.executed_by; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_execution_of_nbw.executed_by IS 'Name of officer who executed';


--
-- TOC entry 4648 (class 0 OID 0)
-- Dependencies: 318
-- Name: COLUMN ir_execution_of_nbw.place_of_execution; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_execution_of_nbw.place_of_execution IS 'Location of execution';


--
-- TOC entry 312 (class 1259 OID 37992809)
-- Name: ir_indulgance_before_offence; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.ir_indulgance_before_offence (
    id integer NOT NULL,
    interrogation_report_id character varying(50) NOT NULL,
    indulgance text,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.ir_indulgance_before_offence OWNER TO dev_dopamas;

--
-- TOC entry 4649 (class 0 OID 0)
-- Dependencies: 312
-- Name: TABLE ir_indulgance_before_offence; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.ir_indulgance_before_offence IS 'Substances/habits indulged in before offense for each IR record. One record per indulgance entry (junction table for INDULGANCE_BEFORE_OFFENCE array).';


--
-- TOC entry 4650 (class 0 OID 0)
-- Dependencies: 312
-- Name: COLUMN ir_indulgance_before_offence.interrogation_report_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_indulgance_before_offence.interrogation_report_id IS 'Foreign key to interrogation_reports table';


--
-- TOC entry 4651 (class 0 OID 0)
-- Dependencies: 312
-- Name: COLUMN ir_indulgance_before_offence.indulgance; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_indulgance_before_offence.indulgance IS 'Type of indulgance (e.g., alcohol, drugs, etc.)';


--
-- TOC entry 263 (class 1259 OID 1397841)
-- Name: ir_interrogation_report_refs; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.ir_interrogation_report_refs (
    id integer NOT NULL,
    interrogation_report_id character varying(50) NOT NULL,
    report_ref_id text NOT NULL
);


ALTER TABLE public.ir_interrogation_report_refs OWNER TO dev_dopamas;

--
-- TOC entry 4652 (class 0 OID 0)
-- Dependencies: 263
-- Name: TABLE ir_interrogation_report_refs; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.ir_interrogation_report_refs IS 'Interrogation report references (UUIDs) for each IR record. One record per reference.';


--
-- TOC entry 324 (class 1259 OID 37992905)
-- Name: ir_jail_sentence; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.ir_jail_sentence (
    id integer NOT NULL,
    interrogation_report_id character varying(50) NOT NULL,
    crime_num character varying(255),
    jurisdiction_ps character varying(255),
    law_section character varying(255),
    sentence_type character varying(100),
    sentence_duration_in_months integer,
    sentence_start_date date,
    sentence_end_date date,
    sentence_amount_in_inr text,
    jail_name character varying(255),
    date_of_jail_entry date,
    date_of_jail_release date,
    remarks text,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.ir_jail_sentence OWNER TO dev_dopamas;

--
-- TOC entry 4653 (class 0 OID 0)
-- Dependencies: 324
-- Name: TABLE ir_jail_sentence; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.ir_jail_sentence IS 'Jail sentence details for each IR record. One record per sentence entry.';


--
-- TOC entry 4654 (class 0 OID 0)
-- Dependencies: 324
-- Name: COLUMN ir_jail_sentence.interrogation_report_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_jail_sentence.interrogation_report_id IS 'Foreign key to interrogation_reports table';


--
-- TOC entry 4655 (class 0 OID 0)
-- Dependencies: 324
-- Name: COLUMN ir_jail_sentence.crime_num; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_jail_sentence.crime_num IS 'Associated crime number';


--
-- TOC entry 4656 (class 0 OID 0)
-- Dependencies: 324
-- Name: COLUMN ir_jail_sentence.sentence_type; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_jail_sentence.sentence_type IS 'Type of sentence (RI, SI, etc.)';


--
-- TOC entry 4657 (class 0 OID 0)
-- Dependencies: 324
-- Name: COLUMN ir_jail_sentence.sentence_duration_in_months; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_jail_sentence.sentence_duration_in_months IS 'Duration in months';


--
-- TOC entry 4658 (class 0 OID 0)
-- Dependencies: 324
-- Name: COLUMN ir_jail_sentence.sentence_start_date; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_jail_sentence.sentence_start_date IS 'When sentence started';


--
-- TOC entry 4659 (class 0 OID 0)
-- Dependencies: 324
-- Name: COLUMN ir_jail_sentence.sentence_end_date; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_jail_sentence.sentence_end_date IS 'When sentence ended';


--
-- TOC entry 4660 (class 0 OID 0)
-- Dependencies: 324
-- Name: COLUMN ir_jail_sentence.sentence_amount_in_inr; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_jail_sentence.sentence_amount_in_inr IS 'Fine amount in INR if applicable';


--
-- TOC entry 4661 (class 0 OID 0)
-- Dependencies: 324
-- Name: COLUMN ir_jail_sentence.jail_name; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_jail_sentence.jail_name IS 'Name of jail where served';


--
-- TOC entry 4662 (class 0 OID 0)
-- Dependencies: 324
-- Name: COLUMN ir_jail_sentence.date_of_jail_entry; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_jail_sentence.date_of_jail_entry IS 'When admitted to jail';


--
-- TOC entry 4663 (class 0 OID 0)
-- Dependencies: 324
-- Name: COLUMN ir_jail_sentence.date_of_jail_release; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_jail_sentence.date_of_jail_release IS 'When released from jail';


--
-- TOC entry 261 (class 1259 OID 1397825)
-- Name: ir_media; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.ir_media (
    id integer NOT NULL,
    interrogation_report_id character varying(50) NOT NULL,
    media_id text NOT NULL
);


ALTER TABLE public.ir_media OWNER TO dev_dopamas;

--
-- TOC entry 4664 (class 0 OID 0)
-- Dependencies: 261
-- Name: TABLE ir_media; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.ir_media IS 'Media references (UUIDs) for each IR record. One record per media reference.';


--
-- TOC entry 326 (class 1259 OID 37992921)
-- Name: ir_new_gang_formation; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.ir_new_gang_formation (
    id integer NOT NULL,
    interrogation_report_id character varying(50) NOT NULL,
    gang_name character varying(255),
    gang_formation_date date,
    number_of_members integer,
    leader_name character varying(255),
    leader_person_id character varying(50),
    gang_objective text,
    criminal_history text,
    jurisdiction_ps character varying(255),
    active boolean,
    remarks text,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.ir_new_gang_formation OWNER TO dev_dopamas;

--
-- TOC entry 4665 (class 0 OID 0)
-- Dependencies: 326
-- Name: TABLE ir_new_gang_formation; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.ir_new_gang_formation IS 'New gang formation details for each IR record. One record per gang entry.';


--
-- TOC entry 4666 (class 0 OID 0)
-- Dependencies: 326
-- Name: COLUMN ir_new_gang_formation.interrogation_report_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_new_gang_formation.interrogation_report_id IS 'Foreign key to interrogation_reports table';


--
-- TOC entry 4667 (class 0 OID 0)
-- Dependencies: 326
-- Name: COLUMN ir_new_gang_formation.gang_name; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_new_gang_formation.gang_name IS 'Name of the gang';


--
-- TOC entry 4668 (class 0 OID 0)
-- Dependencies: 326
-- Name: COLUMN ir_new_gang_formation.gang_formation_date; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_new_gang_formation.gang_formation_date IS 'When gang was formed';


--
-- TOC entry 4669 (class 0 OID 0)
-- Dependencies: 326
-- Name: COLUMN ir_new_gang_formation.number_of_members; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_new_gang_formation.number_of_members IS 'Number of members';


--
-- TOC entry 4670 (class 0 OID 0)
-- Dependencies: 326
-- Name: COLUMN ir_new_gang_formation.leader_name; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_new_gang_formation.leader_name IS 'Name of gang leader';


--
-- TOC entry 4671 (class 0 OID 0)
-- Dependencies: 326
-- Name: COLUMN ir_new_gang_formation.leader_person_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_new_gang_formation.leader_person_id IS 'Reference to person_id if leader is in DOPAMS';


--
-- TOC entry 4672 (class 0 OID 0)
-- Dependencies: 326
-- Name: COLUMN ir_new_gang_formation.gang_objective; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_new_gang_formation.gang_objective IS 'Stated objective of gang';


--
-- TOC entry 4673 (class 0 OID 0)
-- Dependencies: 326
-- Name: COLUMN ir_new_gang_formation.criminal_history; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_new_gang_formation.criminal_history IS 'Known criminal activities';


--
-- TOC entry 4674 (class 0 OID 0)
-- Dependencies: 326
-- Name: COLUMN ir_new_gang_formation.active; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_new_gang_formation.active IS 'Whether gang is still active';


--
-- TOC entry 320 (class 1259 OID 37992873)
-- Name: ir_pending_nbw; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.ir_pending_nbw (
    id integer NOT NULL,
    interrogation_report_id character varying(50) NOT NULL,
    nbw_number character varying(255),
    issued_date date,
    jurisdiction_ps character varying(255),
    crime_num character varying(255),
    reason_for_pending text,
    expected_execution_date date,
    remarks text,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.ir_pending_nbw OWNER TO dev_dopamas;

--
-- TOC entry 4675 (class 0 OID 0)
-- Dependencies: 320
-- Name: TABLE ir_pending_nbw; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.ir_pending_nbw IS 'Pending NBW (Non-Bailable Warrant) for each IR record. One record per pending NBW entry.';


--
-- TOC entry 4676 (class 0 OID 0)
-- Dependencies: 320
-- Name: COLUMN ir_pending_nbw.interrogation_report_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_pending_nbw.interrogation_report_id IS 'Foreign key to interrogation_reports table';


--
-- TOC entry 4677 (class 0 OID 0)
-- Dependencies: 320
-- Name: COLUMN ir_pending_nbw.nbw_number; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_pending_nbw.nbw_number IS 'NBW number/reference';


--
-- TOC entry 4678 (class 0 OID 0)
-- Dependencies: 320
-- Name: COLUMN ir_pending_nbw.issued_date; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_pending_nbw.issued_date IS 'Date NBW was issued';


--
-- TOC entry 4679 (class 0 OID 0)
-- Dependencies: 320
-- Name: COLUMN ir_pending_nbw.jurisdiction_ps; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_pending_nbw.jurisdiction_ps IS 'Police station where issued';


--
-- TOC entry 4680 (class 0 OID 0)
-- Dependencies: 320
-- Name: COLUMN ir_pending_nbw.crime_num; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_pending_nbw.crime_num IS 'Associated crime number';


--
-- TOC entry 4681 (class 0 OID 0)
-- Dependencies: 320
-- Name: COLUMN ir_pending_nbw.reason_for_pending; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_pending_nbw.reason_for_pending IS 'Reason why NBW is still pending';


--
-- TOC entry 4682 (class 0 OID 0)
-- Dependencies: 320
-- Name: COLUMN ir_pending_nbw.expected_execution_date; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_pending_nbw.expected_execution_date IS 'Expected date of execution';


--
-- TOC entry 314 (class 1259 OID 37992825)
-- Name: ir_property_disposal; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.ir_property_disposal (
    id integer NOT NULL,
    interrogation_report_id character varying(50) NOT NULL,
    mode_of_disposal character varying(255),
    buyer_name character varying(255),
    sold_amount_in_inr text,
    location_of_disposal text,
    date_of_disposal date,
    remarks text,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.ir_property_disposal OWNER TO dev_dopamas;

--
-- TOC entry 4683 (class 0 OID 0)
-- Dependencies: 314
-- Name: TABLE ir_property_disposal; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.ir_property_disposal IS 'Property disposal details for each IR record. One record per disposal entry.';


--
-- TOC entry 4684 (class 0 OID 0)
-- Dependencies: 314
-- Name: COLUMN ir_property_disposal.interrogation_report_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_property_disposal.interrogation_report_id IS 'Foreign key to interrogation_reports table';


--
-- TOC entry 4685 (class 0 OID 0)
-- Dependencies: 314
-- Name: COLUMN ir_property_disposal.mode_of_disposal; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_property_disposal.mode_of_disposal IS 'How property was disposed (sold, donated, etc.)';


--
-- TOC entry 4686 (class 0 OID 0)
-- Dependencies: 314
-- Name: COLUMN ir_property_disposal.buyer_name; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_property_disposal.buyer_name IS 'Name of buyer or recipient';


--
-- TOC entry 4687 (class 0 OID 0)
-- Dependencies: 314
-- Name: COLUMN ir_property_disposal.sold_amount_in_inr; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_property_disposal.sold_amount_in_inr IS 'Amount in INR if sold';


--
-- TOC entry 4688 (class 0 OID 0)
-- Dependencies: 314
-- Name: COLUMN ir_property_disposal.location_of_disposal; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_property_disposal.location_of_disposal IS 'Location where property was disposed';


--
-- TOC entry 4689 (class 0 OID 0)
-- Dependencies: 314
-- Name: COLUMN ir_property_disposal.date_of_disposal; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_property_disposal.date_of_disposal IS 'Date of disposal';


--
-- TOC entry 316 (class 1259 OID 37992841)
-- Name: ir_regularization_transit_warrants; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.ir_regularization_transit_warrants (
    id integer NOT NULL,
    interrogation_report_id character varying(50) NOT NULL,
    warrant_number character varying(255),
    warrant_type character varying(100),
    issued_date date,
    jurisdiction_ps character varying(255),
    crime_num character varying(255),
    status character varying(100),
    remarks text,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.ir_regularization_transit_warrants OWNER TO dev_dopamas;

--
-- TOC entry 4690 (class 0 OID 0)
-- Dependencies: 316
-- Name: TABLE ir_regularization_transit_warrants; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.ir_regularization_transit_warrants IS 'Regularization of transit warrants for each IR record. One record per warrant entry.';


--
-- TOC entry 4691 (class 0 OID 0)
-- Dependencies: 316
-- Name: COLUMN ir_regularization_transit_warrants.interrogation_report_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_regularization_transit_warrants.interrogation_report_id IS 'Foreign key to interrogation_reports table';


--
-- TOC entry 4692 (class 0 OID 0)
-- Dependencies: 316
-- Name: COLUMN ir_regularization_transit_warrants.warrant_number; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_regularization_transit_warrants.warrant_number IS 'Warrant number/reference';


--
-- TOC entry 4693 (class 0 OID 0)
-- Dependencies: 316
-- Name: COLUMN ir_regularization_transit_warrants.warrant_type; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_regularization_transit_warrants.warrant_type IS 'Type of warrant (NBW, transit, etc.)';


--
-- TOC entry 4694 (class 0 OID 0)
-- Dependencies: 316
-- Name: COLUMN ir_regularization_transit_warrants.issued_date; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_regularization_transit_warrants.issued_date IS 'Date warrant was issued';


--
-- TOC entry 4695 (class 0 OID 0)
-- Dependencies: 316
-- Name: COLUMN ir_regularization_transit_warrants.jurisdiction_ps; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_regularization_transit_warrants.jurisdiction_ps IS 'Police station/jurisdiction';


--
-- TOC entry 4696 (class 0 OID 0)
-- Dependencies: 316
-- Name: COLUMN ir_regularization_transit_warrants.crime_num; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_regularization_transit_warrants.crime_num IS 'Associated crime number';


--
-- TOC entry 4697 (class 0 OID 0)
-- Dependencies: 316
-- Name: COLUMN ir_regularization_transit_warrants.status; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_regularization_transit_warrants.status IS 'Current status (pending, executed, withdrawn, etc.)';


--
-- TOC entry 322 (class 1259 OID 37992889)
-- Name: ir_sureties; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.ir_sureties (
    id integer NOT NULL,
    interrogation_report_id character varying(50) NOT NULL,
    surety_person_id character varying(50),
    surety_name character varying(255),
    relation_to_accused character varying(100),
    occupation character varying(255),
    aadhar_number character varying(50),
    pan_number character varying(50),
    house_no character varying(100),
    street_road_no character varying(255),
    locality_village text,
    area_mandal character varying(255),
    district character varying(255),
    state_ut character varying(255),
    pin_code character varying(10),
    phone_number character varying(20),
    surety_amount_in_inr text,
    date_of_surety date,
    remarks text,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.ir_sureties OWNER TO dev_dopamas;

--
-- TOC entry 4698 (class 0 OID 0)
-- Dependencies: 322
-- Name: TABLE ir_sureties; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.ir_sureties IS 'Surety information for bail for each IR record. One record per surety entry.';


--
-- TOC entry 4699 (class 0 OID 0)
-- Dependencies: 322
-- Name: COLUMN ir_sureties.interrogation_report_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_sureties.interrogation_report_id IS 'Foreign key to interrogation_reports table';


--
-- TOC entry 4700 (class 0 OID 0)
-- Dependencies: 322
-- Name: COLUMN ir_sureties.surety_person_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_sureties.surety_person_id IS 'Reference to person_id if surety is in DOPAMS';


--
-- TOC entry 4701 (class 0 OID 0)
-- Dependencies: 322
-- Name: COLUMN ir_sureties.surety_name; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_sureties.surety_name IS 'Name of surety';


--
-- TOC entry 4702 (class 0 OID 0)
-- Dependencies: 322
-- Name: COLUMN ir_sureties.relation_to_accused; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_sureties.relation_to_accused IS 'Relationship to accused (friend, family, etc.)';


--
-- TOC entry 4703 (class 0 OID 0)
-- Dependencies: 322
-- Name: COLUMN ir_sureties.occupation; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_sureties.occupation IS 'Occupation of surety';


--
-- TOC entry 4704 (class 0 OID 0)
-- Dependencies: 322
-- Name: COLUMN ir_sureties.surety_amount_in_inr; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_sureties.surety_amount_in_inr IS 'Amount of surety in INR';


--
-- TOC entry 4705 (class 0 OID 0)
-- Dependencies: 322
-- Name: COLUMN ir_sureties.date_of_surety; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_sureties.date_of_surety IS 'Date surety was provided';


--
-- TOC entry 330 (class 1259 OID 37992957)
-- Name: ir_child_table_coverage; Type: VIEW; Schema: public; Owner: dev_dopamas
--

CREATE VIEW public.ir_child_table_coverage AS
 SELECT 'REGULAR_HABITS'::text AS array_field,
    count(DISTINCT rh.interrogation_report_id) AS ir_records_with_data,
    count(*) AS total_entries
   FROM public.ir_regular_habits rh
UNION ALL
 SELECT 'TIMES_OF_DRUGS'::text AS array_field,
    count(DISTINCT td.interrogation_report_id) AS ir_records_with_data,
    count(*) AS total_entries
   FROM public.ir_types_of_drugs td
UNION ALL
 SELECT 'FAMILY_HISTORY'::text AS array_field,
    count(DISTINCT fh.interrogation_report_id) AS ir_records_with_data,
    count(*) AS total_entries
   FROM public.ir_family_history fh
UNION ALL
 SELECT 'LOCAL_CONTACTS'::text AS array_field,
    count(DISTINCT lc.interrogation_report_id) AS ir_records_with_data,
    count(*) AS total_entries
   FROM public.ir_local_contacts lc
UNION ALL
 SELECT 'MODUS_OPERANDI'::text AS array_field,
    count(DISTINCT mo.interrogation_report_id) AS ir_records_with_data,
    count(*) AS total_entries
   FROM public.ir_modus_operandi mo
UNION ALL
 SELECT 'PREVIOUS_OFFENCES_CONFESSED'::text AS array_field,
    count(DISTINCT po.interrogation_report_id) AS ir_records_with_data,
    count(*) AS total_entries
   FROM public.ir_previous_offences_confessed po
UNION ALL
 SELECT 'DEFENCE_COUNSEL'::text AS array_field,
    count(DISTINCT dc.interrogation_report_id) AS ir_records_with_data,
    count(*) AS total_entries
   FROM public.ir_defence_counsel dc
UNION ALL
 SELECT 'ASSOCIATE_DETAILS'::text AS array_field,
    count(DISTINCT ad.interrogation_report_id) AS ir_records_with_data,
    count(*) AS total_entries
   FROM public.ir_associate_details ad
UNION ALL
 SELECT 'SHELTER'::text AS array_field,
    count(DISTINCT sh.interrogation_report_id) AS ir_records_with_data,
    count(*) AS total_entries
   FROM public.ir_shelter sh
UNION ALL
 SELECT 'SIM_DETAILS'::text AS array_field,
    count(DISTINCT sd.interrogation_report_id) AS ir_records_with_data,
    count(*) AS total_entries
   FROM public.ir_sim_details sd
UNION ALL
 SELECT 'FINANCIAL_HISTORY'::text AS array_field,
    count(DISTINCT fh.interrogation_report_id) AS ir_records_with_data,
    count(*) AS total_entries
   FROM public.ir_financial_history fh
UNION ALL
 SELECT 'CONSUMER_DETAILS'::text AS array_field,
    count(DISTINCT cd.interrogation_report_id) AS ir_records_with_data,
    count(*) AS total_entries
   FROM public.ir_consumer_details cd
UNION ALL
 SELECT 'MEDIA'::text AS array_field,
    count(DISTINCT m.interrogation_report_id) AS ir_records_with_data,
    count(*) AS total_entries
   FROM public.ir_media m
UNION ALL
 SELECT 'INTERROGATION_REPORT_REFS'::text AS array_field,
    count(DISTINCT irr.interrogation_report_id) AS ir_records_with_data,
    count(*) AS total_entries
   FROM public.ir_interrogation_report_refs irr
UNION ALL
 SELECT 'DOPAMS_LINKS'::text AS array_field,
    count(DISTINCT dl.interrogation_report_id) AS ir_records_with_data,
    count(*) AS total_entries
   FROM public.ir_dopams_links dl
UNION ALL
 SELECT 'INDULGANCE_BEFORE_OFFENCE'::text AS array_field,
    count(DISTINCT ifo.interrogation_report_id) AS ir_records_with_data,
    count(*) AS total_entries
   FROM public.ir_indulgance_before_offence ifo
UNION ALL
 SELECT 'PROPERTY_DISPOSAL'::text AS array_field,
    count(DISTINCT ipd.interrogation_report_id) AS ir_records_with_data,
    count(*) AS total_entries
   FROM public.ir_property_disposal ipd
UNION ALL
 SELECT 'REGULARIZATION_TRANSIT_WARRANTS'::text AS array_field,
    count(DISTINCT irtw.interrogation_report_id) AS ir_records_with_data,
    count(*) AS total_entries
   FROM public.ir_regularization_transit_warrants irtw
UNION ALL
 SELECT 'EXECUTION_OF_NBW'::text AS array_field,
    count(DISTINCT ien.interrogation_report_id) AS ir_records_with_data,
    count(*) AS total_entries
   FROM public.ir_execution_of_nbw ien
UNION ALL
 SELECT 'PENDING_NBW'::text AS array_field,
    count(DISTINCT ipn.interrogation_report_id) AS ir_records_with_data,
    count(*) AS total_entries
   FROM public.ir_pending_nbw ipn
UNION ALL
 SELECT 'SURETIES'::text AS array_field,
    count(DISTINCT ise.interrogation_report_id) AS ir_records_with_data,
    count(*) AS total_entries
   FROM public.ir_sureties ise
UNION ALL
 SELECT 'JAIL_SENTENCE'::text AS array_field,
    count(DISTINCT ijs.interrogation_report_id) AS ir_records_with_data,
    count(*) AS total_entries
   FROM public.ir_jail_sentence ijs
UNION ALL
 SELECT 'NEW_GANG_FORMATION'::text AS array_field,
    count(DISTINCT ingf.interrogation_report_id) AS ir_records_with_data,
    count(*) AS total_entries
   FROM public.ir_new_gang_formation ingf
UNION ALL
 SELECT 'CONVICTION_ACQUITTAL'::text AS array_field,
    count(DISTINCT ica.interrogation_report_id) AS ir_records_with_data,
    count(*) AS total_entries
   FROM public.ir_conviction_acquittal ica
  ORDER BY 1;


ALTER VIEW public.ir_child_table_coverage OWNER TO dev_dopamas;

--
-- TOC entry 4706 (class 0 OID 0)
-- Dependencies: 330
-- Name: VIEW ir_child_table_coverage; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON VIEW public.ir_child_table_coverage IS 'Shows data coverage for all IR related arrays - helps identify which fields are being populated';


--
-- TOC entry 248 (class 1259 OID 1397740)
-- Name: ir_consumer_details_id_seq; Type: SEQUENCE; Schema: public; Owner: dev_dopamas
--

CREATE SEQUENCE public.ir_consumer_details_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.ir_consumer_details_id_seq OWNER TO dev_dopamas;

--
-- TOC entry 4707 (class 0 OID 0)
-- Dependencies: 248
-- Name: ir_consumer_details_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.ir_consumer_details_id_seq OWNED BY public.ir_consumer_details.id;


--
-- TOC entry 327 (class 1259 OID 37992936)
-- Name: ir_conviction_acquittal_id_seq; Type: SEQUENCE; Schema: public; Owner: dev_dopamas
--

CREATE SEQUENCE public.ir_conviction_acquittal_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.ir_conviction_acquittal_id_seq OWNER TO dev_dopamas;

--
-- TOC entry 4708 (class 0 OID 0)
-- Dependencies: 327
-- Name: ir_conviction_acquittal_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.ir_conviction_acquittal_id_seq OWNED BY public.ir_conviction_acquittal.id;


--
-- TOC entry 254 (class 1259 OID 1397782)
-- Name: ir_defence_counsel_id_seq; Type: SEQUENCE; Schema: public; Owner: dev_dopamas
--

CREATE SEQUENCE public.ir_defence_counsel_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.ir_defence_counsel_id_seq OWNER TO dev_dopamas;

--
-- TOC entry 4709 (class 0 OID 0)
-- Dependencies: 254
-- Name: ir_defence_counsel_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.ir_defence_counsel_id_seq OWNED BY public.ir_defence_counsel.id;


--
-- TOC entry 264 (class 1259 OID 1397856)
-- Name: ir_dopams_links_id_seq; Type: SEQUENCE; Schema: public; Owner: dev_dopamas
--

CREATE SEQUENCE public.ir_dopams_links_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.ir_dopams_links_id_seq OWNER TO dev_dopamas;

--
-- TOC entry 4710 (class 0 OID 0)
-- Dependencies: 264
-- Name: ir_dopams_links_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.ir_dopams_links_id_seq OWNED BY public.ir_dopams_links.id;


--
-- TOC entry 317 (class 1259 OID 37992856)
-- Name: ir_execution_of_nbw_id_seq; Type: SEQUENCE; Schema: public; Owner: dev_dopamas
--

CREATE SEQUENCE public.ir_execution_of_nbw_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.ir_execution_of_nbw_id_seq OWNER TO dev_dopamas;

--
-- TOC entry 4711 (class 0 OID 0)
-- Dependencies: 317
-- Name: ir_execution_of_nbw_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.ir_execution_of_nbw_id_seq OWNED BY public.ir_execution_of_nbw.id;


--
-- TOC entry 236 (class 1259 OID 1397653)
-- Name: ir_family_history_id_seq; Type: SEQUENCE; Schema: public; Owner: dev_dopamas
--

CREATE SEQUENCE public.ir_family_history_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.ir_family_history_id_seq OWNER TO dev_dopamas;

--
-- TOC entry 4712 (class 0 OID 0)
-- Dependencies: 236
-- Name: ir_family_history_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.ir_family_history_id_seq OWNED BY public.ir_family_history.id;


--
-- TOC entry 329 (class 1259 OID 37992952)
-- Name: ir_field_persistence_check; Type: VIEW; Schema: public; Owner: dev_dopamas
--

CREATE VIEW public.ir_field_persistence_check AS
 SELECT 'INTERROGATION_REPORT_ID'::text AS api_field,
    'interrogation_report_id'::text AS db_column,
    count(ir.interrogation_report_id) AS records_with_value,
    count(NULLIF((ir.interrogation_report_id)::text, ''::text)) AS non_null_count
   FROM public.interrogation_reports ir
UNION ALL
 SELECT 'CRIME_ID'::text AS api_field,
    'crime_id'::text AS db_column,
    count(ir.crime_id) AS records_with_value,
    count(NULLIF((ir.crime_id)::text, ''::text)) AS non_null_count
   FROM public.interrogation_reports ir
UNION ALL
 SELECT 'PERSON_ID'::text AS api_field,
    'person_id'::text AS db_column,
    count(ir.person_id) AS records_with_value,
    count(NULLIF((ir.person_id)::text, ''::text)) AS non_null_count
   FROM public.interrogation_reports ir
UNION ALL
 SELECT 'INDULGANCE_BEFORE_OFFENCE'::text AS api_field,
    'ir_indulgance_before_offence'::text AS db_column,
    count(DISTINCT ib.interrogation_report_id) AS records_with_value,
    count(ib.indulgance) AS non_null_count
   FROM public.ir_indulgance_before_offence ib
UNION ALL
 SELECT 'PROPERTY_DISPOSAL'::text AS api_field,
    'ir_property_disposal'::text AS db_column,
    count(DISTINCT ipd.interrogation_report_id) AS records_with_value,
    count(ipd.mode_of_disposal) AS non_null_count
   FROM public.ir_property_disposal ipd
UNION ALL
 SELECT 'REGULARIZATION_OF_TRANSIT_WARRANTS'::text AS api_field,
    'ir_regularization_transit_warrants'::text AS db_column,
    count(DISTINCT irtw.interrogation_report_id) AS records_with_value,
    count(irtw.warrant_number) AS non_null_count
   FROM public.ir_regularization_transit_warrants irtw
UNION ALL
 SELECT 'EXECUTION_OF_NBW'::text AS api_field,
    'ir_execution_of_nbw'::text AS db_column,
    count(DISTINCT ien.interrogation_report_id) AS records_with_value,
    count(ien.nbw_number) AS non_null_count
   FROM public.ir_execution_of_nbw ien
UNION ALL
 SELECT 'PENDING_NBW'::text AS api_field,
    'ir_pending_nbw'::text AS db_column,
    count(DISTINCT ipn.interrogation_report_id) AS records_with_value,
    count(ipn.nbw_number) AS non_null_count
   FROM public.ir_pending_nbw ipn
UNION ALL
 SELECT 'SURETIES'::text AS api_field,
    'ir_sureties'::text AS db_column,
    count(DISTINCT ise.interrogation_report_id) AS records_with_value,
    count(ise.surety_name) AS non_null_count
   FROM public.ir_sureties ise
UNION ALL
 SELECT 'JAIL_SENTENCE'::text AS api_field,
    'ir_jail_sentence'::text AS db_column,
    count(DISTINCT ijs.interrogation_report_id) AS records_with_value,
    count(ijs.sentence_type) AS non_null_count
   FROM public.ir_jail_sentence ijs
UNION ALL
 SELECT 'NEW_GANG_FORMATION'::text AS api_field,
    'ir_new_gang_formation'::text AS db_column,
    count(DISTINCT ingf.interrogation_report_id) AS records_with_value,
    count(ingf.gang_name) AS non_null_count
   FROM public.ir_new_gang_formation ingf
UNION ALL
 SELECT 'CONVICTION_ACQUITTAL'::text AS api_field,
    'ir_conviction_acquittal'::text AS db_column,
    count(DISTINCT ica.interrogation_report_id) AS records_with_value,
    count(ica.verdict) AS non_null_count
   FROM public.ir_conviction_acquittal ica;


ALTER VIEW public.ir_field_persistence_check OWNER TO dev_dopamas;

--
-- TOC entry 4713 (class 0 OID 0)
-- Dependencies: 329
-- Name: VIEW ir_field_persistence_check; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON VIEW public.ir_field_persistence_check IS 'Validates API field to DB persistence mapping - shows which fields are being stored and frequency of non-null values';


--
-- TOC entry 246 (class 1259 OID 1397726)
-- Name: ir_financial_history_id_seq; Type: SEQUENCE; Schema: public; Owner: dev_dopamas
--

CREATE SEQUENCE public.ir_financial_history_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.ir_financial_history_id_seq OWNER TO dev_dopamas;

--
-- TOC entry 4714 (class 0 OID 0)
-- Dependencies: 246
-- Name: ir_financial_history_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.ir_financial_history_id_seq OWNED BY public.ir_financial_history.id;


--
-- TOC entry 311 (class 1259 OID 37992808)
-- Name: ir_indulgance_before_offence_id_seq; Type: SEQUENCE; Schema: public; Owner: dev_dopamas
--

CREATE SEQUENCE public.ir_indulgance_before_offence_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.ir_indulgance_before_offence_id_seq OWNER TO dev_dopamas;

--
-- TOC entry 4715 (class 0 OID 0)
-- Dependencies: 311
-- Name: ir_indulgance_before_offence_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.ir_indulgance_before_offence_id_seq OWNED BY public.ir_indulgance_before_offence.id;


--
-- TOC entry 262 (class 1259 OID 1397840)
-- Name: ir_interrogation_report_refs_id_seq; Type: SEQUENCE; Schema: public; Owner: dev_dopamas
--

CREATE SEQUENCE public.ir_interrogation_report_refs_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.ir_interrogation_report_refs_id_seq OWNER TO dev_dopamas;

--
-- TOC entry 4716 (class 0 OID 0)
-- Dependencies: 262
-- Name: ir_interrogation_report_refs_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.ir_interrogation_report_refs_id_seq OWNED BY public.ir_interrogation_report_refs.id;


--
-- TOC entry 323 (class 1259 OID 37992904)
-- Name: ir_jail_sentence_id_seq; Type: SEQUENCE; Schema: public; Owner: dev_dopamas
--

CREATE SEQUENCE public.ir_jail_sentence_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.ir_jail_sentence_id_seq OWNER TO dev_dopamas;

--
-- TOC entry 4717 (class 0 OID 0)
-- Dependencies: 323
-- Name: ir_jail_sentence_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.ir_jail_sentence_id_seq OWNED BY public.ir_jail_sentence.id;


--
-- TOC entry 238 (class 1259 OID 1397670)
-- Name: ir_local_contacts_id_seq; Type: SEQUENCE; Schema: public; Owner: dev_dopamas
--

CREATE SEQUENCE public.ir_local_contacts_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.ir_local_contacts_id_seq OWNER TO dev_dopamas;

--
-- TOC entry 4718 (class 0 OID 0)
-- Dependencies: 238
-- Name: ir_local_contacts_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.ir_local_contacts_id_seq OWNED BY public.ir_local_contacts.id;


--
-- TOC entry 260 (class 1259 OID 1397824)
-- Name: ir_media_id_seq; Type: SEQUENCE; Schema: public; Owner: dev_dopamas
--

CREATE SEQUENCE public.ir_media_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.ir_media_id_seq OWNER TO dev_dopamas;

--
-- TOC entry 4719 (class 0 OID 0)
-- Dependencies: 260
-- Name: ir_media_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.ir_media_id_seq OWNED BY public.ir_media.id;


--
-- TOC entry 250 (class 1259 OID 1397754)
-- Name: ir_modus_operandi_id_seq; Type: SEQUENCE; Schema: public; Owner: dev_dopamas
--

CREATE SEQUENCE public.ir_modus_operandi_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.ir_modus_operandi_id_seq OWNER TO dev_dopamas;

--
-- TOC entry 4720 (class 0 OID 0)
-- Dependencies: 250
-- Name: ir_modus_operandi_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.ir_modus_operandi_id_seq OWNED BY public.ir_modus_operandi.id;


--
-- TOC entry 325 (class 1259 OID 37992920)
-- Name: ir_new_gang_formation_id_seq; Type: SEQUENCE; Schema: public; Owner: dev_dopamas
--

CREATE SEQUENCE public.ir_new_gang_formation_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.ir_new_gang_formation_id_seq OWNER TO dev_dopamas;

--
-- TOC entry 4721 (class 0 OID 0)
-- Dependencies: 325
-- Name: ir_new_gang_formation_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.ir_new_gang_formation_id_seq OWNED BY public.ir_new_gang_formation.id;


--
-- TOC entry 306 (class 1259 OID 25200401)
-- Name: ir_pending_fk; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.ir_pending_fk (
    id integer NOT NULL,
    ir_id character varying(50) NOT NULL,
    crime_id character varying(50) NOT NULL,
    raw_data jsonb NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    retry_count integer DEFAULT 0,
    last_retry_at timestamp without time zone,
    resolved boolean DEFAULT false,
    resolved_at timestamp without time zone
);


ALTER TABLE public.ir_pending_fk OWNER TO dev_dopamas;

--
-- TOC entry 305 (class 1259 OID 25200400)
-- Name: ir_pending_fk_id_seq; Type: SEQUENCE; Schema: public; Owner: dev_dopamas
--

CREATE SEQUENCE public.ir_pending_fk_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.ir_pending_fk_id_seq OWNER TO dev_dopamas;

--
-- TOC entry 4722 (class 0 OID 0)
-- Dependencies: 305
-- Name: ir_pending_fk_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.ir_pending_fk_id_seq OWNED BY public.ir_pending_fk.id;


--
-- TOC entry 319 (class 1259 OID 37992872)
-- Name: ir_pending_nbw_id_seq; Type: SEQUENCE; Schema: public; Owner: dev_dopamas
--

CREATE SEQUENCE public.ir_pending_nbw_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.ir_pending_nbw_id_seq OWNER TO dev_dopamas;

--
-- TOC entry 4723 (class 0 OID 0)
-- Dependencies: 319
-- Name: ir_pending_nbw_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.ir_pending_nbw_id_seq OWNED BY public.ir_pending_nbw.id;


--
-- TOC entry 252 (class 1259 OID 1397768)
-- Name: ir_previous_offences_confessed_id_seq; Type: SEQUENCE; Schema: public; Owner: dev_dopamas
--

CREATE SEQUENCE public.ir_previous_offences_confessed_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.ir_previous_offences_confessed_id_seq OWNER TO dev_dopamas;

--
-- TOC entry 4724 (class 0 OID 0)
-- Dependencies: 252
-- Name: ir_previous_offences_confessed_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.ir_previous_offences_confessed_id_seq OWNED BY public.ir_previous_offences_confessed.id;


--
-- TOC entry 313 (class 1259 OID 37992824)
-- Name: ir_property_disposal_id_seq; Type: SEQUENCE; Schema: public; Owner: dev_dopamas
--

CREATE SEQUENCE public.ir_property_disposal_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.ir_property_disposal_id_seq OWNER TO dev_dopamas;

--
-- TOC entry 4725 (class 0 OID 0)
-- Dependencies: 313
-- Name: ir_property_disposal_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.ir_property_disposal_id_seq OWNED BY public.ir_property_disposal.id;


--
-- TOC entry 240 (class 1259 OID 1397684)
-- Name: ir_regular_habits_id_seq; Type: SEQUENCE; Schema: public; Owner: dev_dopamas
--

CREATE SEQUENCE public.ir_regular_habits_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.ir_regular_habits_id_seq OWNER TO dev_dopamas;

--
-- TOC entry 4726 (class 0 OID 0)
-- Dependencies: 240
-- Name: ir_regular_habits_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.ir_regular_habits_id_seq OWNED BY public.ir_regular_habits.id;


--
-- TOC entry 315 (class 1259 OID 37992840)
-- Name: ir_regularization_transit_warrants_id_seq; Type: SEQUENCE; Schema: public; Owner: dev_dopamas
--

CREATE SEQUENCE public.ir_regularization_transit_warrants_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.ir_regularization_transit_warrants_id_seq OWNER TO dev_dopamas;

--
-- TOC entry 4727 (class 0 OID 0)
-- Dependencies: 315
-- Name: ir_regularization_transit_warrants_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.ir_regularization_transit_warrants_id_seq OWNED BY public.ir_regularization_transit_warrants.id;


--
-- TOC entry 258 (class 1259 OID 1397810)
-- Name: ir_shelter_id_seq; Type: SEQUENCE; Schema: public; Owner: dev_dopamas
--

CREATE SEQUENCE public.ir_shelter_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.ir_shelter_id_seq OWNER TO dev_dopamas;

--
-- TOC entry 4728 (class 0 OID 0)
-- Dependencies: 258
-- Name: ir_shelter_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.ir_shelter_id_seq OWNED BY public.ir_shelter.id;


--
-- TOC entry 244 (class 1259 OID 1397712)
-- Name: ir_sim_details_id_seq; Type: SEQUENCE; Schema: public; Owner: dev_dopamas
--

CREATE SEQUENCE public.ir_sim_details_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.ir_sim_details_id_seq OWNER TO dev_dopamas;

--
-- TOC entry 4729 (class 0 OID 0)
-- Dependencies: 244
-- Name: ir_sim_details_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.ir_sim_details_id_seq OWNED BY public.ir_sim_details.id;


--
-- TOC entry 321 (class 1259 OID 37992888)
-- Name: ir_sureties_id_seq; Type: SEQUENCE; Schema: public; Owner: dev_dopamas
--

CREATE SEQUENCE public.ir_sureties_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.ir_sureties_id_seq OWNER TO dev_dopamas;

--
-- TOC entry 4730 (class 0 OID 0)
-- Dependencies: 321
-- Name: ir_sureties_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.ir_sureties_id_seq OWNED BY public.ir_sureties.id;


--
-- TOC entry 242 (class 1259 OID 1397698)
-- Name: ir_types_of_drugs_id_seq; Type: SEQUENCE; Schema: public; Owner: dev_dopamas
--

CREATE SEQUENCE public.ir_types_of_drugs_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.ir_types_of_drugs_id_seq OWNER TO dev_dopamas;

--
-- TOC entry 4731 (class 0 OID 0)
-- Dependencies: 242
-- Name: ir_types_of_drugs_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.ir_types_of_drugs_id_seq OWNED BY public.ir_types_of_drugs.id;


--
-- TOC entry 332 (class 1259 OID 37993005)
-- Name: mo_seizure_media; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.mo_seizure_media (
    id bigint NOT NULL,
    mo_seizure_id character varying(50) NOT NULL,
    media_index integer DEFAULT 0 NOT NULL,
    media_file_id text,
    media_url text,
    media_name text,
    date_created timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    date_modified timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


ALTER TABLE public.mo_seizure_media OWNER TO dev_dopamas;

--
-- TOC entry 4732 (class 0 OID 0)
-- Dependencies: 332
-- Name: TABLE mo_seizure_media; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.mo_seizure_media IS 'Normalized media references for mo_seizures. One row per media item.';


--
-- TOC entry 4733 (class 0 OID 0)
-- Dependencies: 332
-- Name: COLUMN mo_seizure_media.mo_seizure_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.mo_seizure_media.mo_seizure_id IS 'Foreign key to mo_seizures.mo_seizure_id';


--
-- TOC entry 4734 (class 0 OID 0)
-- Dependencies: 332
-- Name: COLUMN mo_seizure_media.media_index; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.mo_seizure_media.media_index IS 'Zero-based ordering of media items in the source payload';


--
-- TOC entry 4735 (class 0 OID 0)
-- Dependencies: 332
-- Name: COLUMN mo_seizure_media.media_file_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.mo_seizure_media.media_file_id IS 'Media file identifier from API';


--
-- TOC entry 4736 (class 0 OID 0)
-- Dependencies: 332
-- Name: COLUMN mo_seizure_media.media_url; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.mo_seizure_media.media_url IS 'Media URL from API';


--
-- TOC entry 4737 (class 0 OID 0)
-- Dependencies: 332
-- Name: COLUMN mo_seizure_media.media_name; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.mo_seizure_media.media_name IS 'Media file name from API';


--
-- TOC entry 4738 (class 0 OID 0)
-- Dependencies: 332
-- Name: COLUMN mo_seizure_media.date_created; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.mo_seizure_media.date_created IS 'Record creation timestamp (source timestamp or load timestamp)';


--
-- TOC entry 4739 (class 0 OID 0)
-- Dependencies: 332
-- Name: COLUMN mo_seizure_media.date_modified; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.mo_seizure_media.date_modified IS 'Record modification timestamp (source timestamp or load timestamp)';


--
-- TOC entry 331 (class 1259 OID 37993004)
-- Name: mo_seizure_media_id_seq; Type: SEQUENCE; Schema: public; Owner: dev_dopamas
--

CREATE SEQUENCE public.mo_seizure_media_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.mo_seizure_media_id_seq OWNER TO dev_dopamas;

--
-- TOC entry 4740 (class 0 OID 0)
-- Dependencies: 331
-- Name: mo_seizure_media_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.mo_seizure_media_id_seq OWNED BY public.mo_seizure_media.id;


--
-- TOC entry 271 (class 1259 OID 1413497)
-- Name: old_interragation_report; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.old_interragation_report (
    interrogation_report_id uuid DEFAULT gen_random_uuid() NOT NULL,
    crime_id character varying(255) NOT NULL,
    int_aunt_address text,
    int_aunt_mobile_no character varying(20),
    int_aunt_name character varying(255),
    int_aunt_occupation character varying(255),
    int_relation_type_aunt character varying(100),
    int_brother_address text,
    int_brother_mobile_no character varying(20),
    int_brother_name character varying(255),
    int_brother_occupation character varying(255),
    int_relation_type_brother character varying(100),
    int_daughter_address text,
    int_daughter_mobile_no character varying(20),
    int_daughter_name character varying(255),
    int_daughter_occupation character varying(255),
    int_relation_type_daughter character varying(100),
    int_father_address text,
    int_father_mobile_no character varying(20),
    int_father_name character varying(255),
    int_father_occupation character varying(255),
    int_fil_address text,
    int_fil_mobile_no character varying(20),
    int_fil_name character varying(255),
    int_fil_occupation character varying(255),
    int_relation_type_fil character varying(100),
    int_friend_address text,
    int_friend_mobile_no character varying(20),
    int_friend_name character varying(255),
    int_friend_occupation character varying(255),
    int_relation_type_friend character varying(100),
    int_mil_address text,
    int_mil_mobile_no character varying(20),
    int_mil_name character varying(255),
    int_mil_occupation character varying(255),
    int_relation_type_mil character varying(100),
    int_mother_address text,
    int_mother_mobile_no character varying(20),
    int_mother_name character varying(255),
    int_mother_occupation character varying(255),
    int_relation_type_mother character varying(100),
    int_sister_address text,
    int_sister_mobile_no character varying(20),
    int_sister_name character varying(255),
    int_sister_occupation character varying(255),
    int_relation_type_sister character varying(100),
    int_son_address text,
    int_son_mobile_no character varying(20),
    int_son_name character varying(255),
    int_son_occupation character varying(255),
    int_relation_type_son character varying(100),
    int_uncle_address text,
    int_uncle_mobile_no character varying(20),
    int_uncle_name character varying(255),
    int_uncle_occupation character varying(255),
    int_relation_type_uncle character varying(100),
    int_wife_address text,
    int_wife_mobile_no character varying(20),
    int_wife_name character varying(255),
    int_wife_occupation character varying(255),
    int_relation_type_wife character varying(100)
);


ALTER TABLE public.old_interragation_report OWNER TO dev_dopamas;

--
-- TOC entry 4741 (class 0 OID 0)
-- Dependencies: 271
-- Name: TABLE old_interragation_report; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.old_interragation_report IS 'Interrogation report with family and social relations information';


--
-- TOC entry 4742 (class 0 OID 0)
-- Dependencies: 271
-- Name: COLUMN old_interragation_report.crime_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.old_interragation_report.crime_id IS 'Reference to the crime record';


--
-- TOC entry 273 (class 1259 OID 1413815)
-- Name: person_deduplication_tracker; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.person_deduplication_tracker (
    id integer NOT NULL,
    person_fingerprint character varying(32) NOT NULL,
    matching_tier smallint NOT NULL,
    matching_strategy character varying(100) NOT NULL,
    uses_fuzzy_matching boolean DEFAULT false,
    fuzzy_match_score numeric(3,2),
    name_variations text[],
    canonical_person_id character varying(50) NOT NULL,
    full_name character varying(500),
    relative_name character varying(255),
    age integer,
    gender character varying(20),
    phone_number character varying(20),
    present_district character varying(255),
    present_locality_village character varying(255),
    all_person_ids text[] NOT NULL,
    person_record_count integer DEFAULT 1 NOT NULL,
    all_accused_ids text[] NOT NULL,
    all_crime_ids text[] NOT NULL,
    crime_count integer DEFAULT 0 NOT NULL,
    crime_details jsonb,
    confidence_score numeric(3,2),
    data_quality_flags jsonb,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT person_deduplication_tracker_confidence_score_check CHECK (((confidence_score >= (0)::numeric) AND (confidence_score <= (1)::numeric))),
    CONSTRAINT person_deduplication_tracker_matching_tier_check CHECK (((matching_tier >= 1) AND (matching_tier <= 5)))
);


ALTER TABLE public.person_deduplication_tracker OWNER TO dev_dopamas;

--
-- TOC entry 4743 (class 0 OID 0)
-- Dependencies: 273
-- Name: TABLE person_deduplication_tracker; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.person_deduplication_tracker IS 'Tracks unique persons across multiple crimes using hierarchical fingerprinting strategies';


--
-- TOC entry 4744 (class 0 OID 0)
-- Dependencies: 273
-- Name: COLUMN person_deduplication_tracker.person_fingerprint; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.person_deduplication_tracker.person_fingerprint IS 'MD5 hash combining person identifying fields based on matching strategy';


--
-- TOC entry 4745 (class 0 OID 0)
-- Dependencies: 273
-- Name: COLUMN person_deduplication_tracker.matching_tier; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.person_deduplication_tracker.matching_tier IS '1=Best (Name+Parent+Locality+Age+Phone), 5=Basic (Name+District+Age)';


--
-- TOC entry 274 (class 1259 OID 1413840)
-- Name: person_deduplication_summary; Type: VIEW; Schema: public; Owner: dev_dopamas
--

CREATE VIEW public.person_deduplication_summary AS
 SELECT person_fingerprint,
    matching_tier,
    matching_strategy,
    canonical_person_id,
    full_name,
    relative_name,
    age,
    phone_number,
    present_district,
    person_record_count,
    crime_count,
        CASE
            WHEN (matching_tier = 1) THEN 'Very High'::text
            WHEN (matching_tier = 2) THEN 'High'::text
            WHEN (matching_tier = 3) THEN 'Good'::text
            WHEN (matching_tier = 4) THEN 'Medium'::text
            WHEN (matching_tier = 5) THEN 'Basic'::text
            ELSE NULL::text
        END AS confidence_level,
    confidence_score,
        CASE
            WHEN (crime_count > 5) THEN 'Repeat Offender'::text
            WHEN (crime_count > 2) THEN 'Multiple Cases'::text
            WHEN (crime_count = 1) THEN 'Single Case'::text
            ELSE 'No Cases'::text
        END AS offender_category,
    created_at,
    updated_at
   FROM public.person_deduplication_tracker
  ORDER BY crime_count DESC, matching_tier;


ALTER VIEW public.person_deduplication_summary OWNER TO dev_dopamas;

--
-- TOC entry 272 (class 1259 OID 1413814)
-- Name: person_deduplication_tracker_id_seq; Type: SEQUENCE; Schema: public; Owner: dev_dopamas
--

CREATE SEQUENCE public.person_deduplication_tracker_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.person_deduplication_tracker_id_seq OWNER TO dev_dopamas;

--
-- TOC entry 4746 (class 0 OID 0)
-- Dependencies: 272
-- Name: person_deduplication_tracker_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.person_deduplication_tracker_id_seq OWNED BY public.person_deduplication_tracker.id;


--
-- TOC entry 304 (class 1259 OID 25200388)
-- Name: properties_pending_fk; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.properties_pending_fk (
    id integer NOT NULL,
    property_id character varying(50) NOT NULL,
    crime_id character varying(50) NOT NULL,
    raw_data jsonb NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    retry_count integer DEFAULT 0,
    last_retry_at timestamp without time zone,
    resolved boolean DEFAULT false,
    resolved_at timestamp without time zone
);


ALTER TABLE public.properties_pending_fk OWNER TO dev_dopamas;

--
-- TOC entry 303 (class 1259 OID 25200387)
-- Name: properties_pending_fk_id_seq; Type: SEQUENCE; Schema: public; Owner: dev_dopamas
--

CREATE SEQUENCE public.properties_pending_fk_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.properties_pending_fk_id_seq OWNER TO dev_dopamas;

--
-- TOC entry 4747 (class 0 OID 0)
-- Dependencies: 303
-- Name: properties_pending_fk_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.properties_pending_fk_id_seq OWNED BY public.properties_pending_fk.id;


--
-- TOC entry 333 (class 1259 OID 38052550)
-- Name: property_additional_details; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.property_additional_details (
    property_id character varying(50) NOT NULL,
    additional_details jsonb NOT NULL,
    date_created timestamp with time zone,
    date_modified timestamp with time zone,
    CONSTRAINT property_additional_details_json_is_object CHECK ((jsonb_typeof(additional_details) = 'object'::text))
);


ALTER TABLE public.property_additional_details OWNER TO dev_dopamas;

--
-- TOC entry 335 (class 1259 OID 38052566)
-- Name: property_media; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.property_media (
    id bigint NOT NULL,
    property_id character varying(50) NOT NULL,
    media_index integer NOT NULL,
    media_file_id text,
    media_url text,
    media_payload jsonb,
    date_created timestamp with time zone,
    date_modified timestamp with time zone
);


ALTER TABLE public.property_media OWNER TO dev_dopamas;

--
-- TOC entry 334 (class 1259 OID 38052565)
-- Name: property_media_id_seq; Type: SEQUENCE; Schema: public; Owner: dev_dopamas
--

CREATE SEQUENCE public.property_media_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.property_media_id_seq OWNER TO dev_dopamas;

--
-- TOC entry 4748 (class 0 OID 0)
-- Dependencies: 334
-- Name: property_media_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.property_media_id_seq OWNED BY public.property_media.id;


--
-- TOC entry 337 (class 1259 OID 38079274)
-- Name: update_chargesheets; Type: VIEW; Schema: public; Owner: dev_dopamas
--

CREATE VIEW public.update_chargesheets AS
 SELECT id,
    update_charge_sheet_id,
    crime_id,
    charge_sheet_no,
    charge_sheet_date,
    charge_sheet_status,
    taken_on_file_date,
    taken_on_file_case_type,
    taken_on_file_court_case_no,
    date_created,
    date_modified
   FROM public.charge_sheet_updates;


ALTER VIEW public.update_chargesheets OWNER TO dev_dopamas;

--
-- TOC entry 4749 (class 0 OID 0)
-- Dependencies: 337
-- Name: VIEW update_chargesheets; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON VIEW public.update_chargesheets IS 'API-facing alias for charge_sheet_updates. Read-only compatibility layer.';


--
-- TOC entry 268 (class 1259 OID 1404739)
-- Name: user; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public."user" (
    id uuid NOT NULL,
    email character varying(255) NOT NULL,
    password character varying(255) NOT NULL,
    role integer DEFAULT 0 NOT NULL,
    status integer DEFAULT 1 NOT NULL,
    "createdAt" timestamp(6) with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    "updatedAt" timestamp(6) with time zone NOT NULL,
    background_color character varying(50)
);


ALTER TABLE public."user" OWNER TO dev_dopamas;

--
-- TOC entry 3961 (class 2604 OID 2028692)
-- Name: agent_deduplication_tracker id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.agent_deduplication_tracker ALTER COLUMN id SET DEFAULT nextval('public.agent_deduplication_tracker_id_seq'::regclass);


--
-- TOC entry 3960 (class 2604 OID 1639317)
-- Name: charge_sheet_updates id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.charge_sheet_updates ALTER COLUMN id SET DEFAULT nextval('public.charge_sheet_updates_id_seq'::regclass);


--
-- TOC entry 3965 (class 2604 OID 2028693)
-- Name: dedup_cluster_state id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.dedup_cluster_state ALTER COLUMN id SET DEFAULT nextval('public.dedup_cluster_state_id_seq'::regclass);


--
-- TOC entry 3968 (class 2604 OID 2028694)
-- Name: dedup_comparison_progress id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.dedup_comparison_progress ALTER COLUMN id SET DEFAULT nextval('public.dedup_comparison_progress_id_seq'::regclass);


--
-- TOC entry 3971 (class 2604 OID 2028695)
-- Name: dedup_run_metadata id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.dedup_run_metadata ALTER COLUMN id SET DEFAULT nextval('public.dedup_run_metadata_id_seq'::regclass);


--
-- TOC entry 3976 (class 2604 OID 20996615)
-- Name: drug_categories id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.drug_categories ALTER COLUMN id SET DEFAULT nextval('public.drug_categories_id_seq'::regclass);


--
-- TOC entry 3979 (class 2604 OID 20996629)
-- Name: drug_ignore_list id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.drug_ignore_list ALTER COLUMN id SET DEFAULT nextval('public.drug_ignore_list_id_seq'::regclass);


--
-- TOC entry 3986 (class 2604 OID 23469815)
-- Name: geo_reference id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.geo_reference ALTER COLUMN id SET DEFAULT nextval('public.geo_reference_id_seq'::regclass);


--
-- TOC entry 3922 (class 2604 OID 1397800)
-- Name: ir_associate_details id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_associate_details ALTER COLUMN id SET DEFAULT nextval('public.ir_associate_details_id_seq'::regclass);


--
-- TOC entry 3918 (class 2604 OID 1397744)
-- Name: ir_consumer_details id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_consumer_details ALTER COLUMN id SET DEFAULT nextval('public.ir_consumer_details_id_seq'::regclass);


--
-- TOC entry 4012 (class 2604 OID 37992940)
-- Name: ir_conviction_acquittal id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_conviction_acquittal ALTER COLUMN id SET DEFAULT nextval('public.ir_conviction_acquittal_id_seq'::regclass);


--
-- TOC entry 3921 (class 2604 OID 1397786)
-- Name: ir_defence_counsel id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_defence_counsel ALTER COLUMN id SET DEFAULT nextval('public.ir_defence_counsel_id_seq'::regclass);


--
-- TOC entry 3926 (class 2604 OID 1397860)
-- Name: ir_dopams_links id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_dopams_links ALTER COLUMN id SET DEFAULT nextval('public.ir_dopams_links_id_seq'::regclass);


--
-- TOC entry 4002 (class 2604 OID 37992860)
-- Name: ir_execution_of_nbw id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_execution_of_nbw ALTER COLUMN id SET DEFAULT nextval('public.ir_execution_of_nbw_id_seq'::regclass);


--
-- TOC entry 3909 (class 2604 OID 1397657)
-- Name: ir_family_history id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_family_history ALTER COLUMN id SET DEFAULT nextval('public.ir_family_history_id_seq'::regclass);


--
-- TOC entry 3917 (class 2604 OID 1397730)
-- Name: ir_financial_history id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_financial_history ALTER COLUMN id SET DEFAULT nextval('public.ir_financial_history_id_seq'::regclass);


--
-- TOC entry 3996 (class 2604 OID 37992812)
-- Name: ir_indulgance_before_offence id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_indulgance_before_offence ALTER COLUMN id SET DEFAULT nextval('public.ir_indulgance_before_offence_id_seq'::regclass);


--
-- TOC entry 3925 (class 2604 OID 1397844)
-- Name: ir_interrogation_report_refs id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_interrogation_report_refs ALTER COLUMN id SET DEFAULT nextval('public.ir_interrogation_report_refs_id_seq'::regclass);


--
-- TOC entry 4008 (class 2604 OID 37992908)
-- Name: ir_jail_sentence id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_jail_sentence ALTER COLUMN id SET DEFAULT nextval('public.ir_jail_sentence_id_seq'::regclass);


--
-- TOC entry 3913 (class 2604 OID 1397674)
-- Name: ir_local_contacts id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_local_contacts ALTER COLUMN id SET DEFAULT nextval('public.ir_local_contacts_id_seq'::regclass);


--
-- TOC entry 3924 (class 2604 OID 1397828)
-- Name: ir_media id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_media ALTER COLUMN id SET DEFAULT nextval('public.ir_media_id_seq'::regclass);


--
-- TOC entry 3919 (class 2604 OID 1397758)
-- Name: ir_modus_operandi id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_modus_operandi ALTER COLUMN id SET DEFAULT nextval('public.ir_modus_operandi_id_seq'::regclass);


--
-- TOC entry 4010 (class 2604 OID 37992924)
-- Name: ir_new_gang_formation id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_new_gang_formation ALTER COLUMN id SET DEFAULT nextval('public.ir_new_gang_formation_id_seq'::regclass);


--
-- TOC entry 3992 (class 2604 OID 25200404)
-- Name: ir_pending_fk id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_pending_fk ALTER COLUMN id SET DEFAULT nextval('public.ir_pending_fk_id_seq'::regclass);


--
-- TOC entry 4004 (class 2604 OID 37992876)
-- Name: ir_pending_nbw id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_pending_nbw ALTER COLUMN id SET DEFAULT nextval('public.ir_pending_nbw_id_seq'::regclass);


--
-- TOC entry 3920 (class 2604 OID 1397772)
-- Name: ir_previous_offences_confessed id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_previous_offences_confessed ALTER COLUMN id SET DEFAULT nextval('public.ir_previous_offences_confessed_id_seq'::regclass);


--
-- TOC entry 3998 (class 2604 OID 37992828)
-- Name: ir_property_disposal id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_property_disposal ALTER COLUMN id SET DEFAULT nextval('public.ir_property_disposal_id_seq'::regclass);


--
-- TOC entry 3914 (class 2604 OID 1397688)
-- Name: ir_regular_habits id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_regular_habits ALTER COLUMN id SET DEFAULT nextval('public.ir_regular_habits_id_seq'::regclass);


--
-- TOC entry 4000 (class 2604 OID 37992844)
-- Name: ir_regularization_transit_warrants id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_regularization_transit_warrants ALTER COLUMN id SET DEFAULT nextval('public.ir_regularization_transit_warrants_id_seq'::regclass);


--
-- TOC entry 3923 (class 2604 OID 1397814)
-- Name: ir_shelter id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_shelter ALTER COLUMN id SET DEFAULT nextval('public.ir_shelter_id_seq'::regclass);


--
-- TOC entry 3916 (class 2604 OID 1397716)
-- Name: ir_sim_details id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_sim_details ALTER COLUMN id SET DEFAULT nextval('public.ir_sim_details_id_seq'::regclass);


--
-- TOC entry 4006 (class 2604 OID 37992892)
-- Name: ir_sureties id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_sureties ALTER COLUMN id SET DEFAULT nextval('public.ir_sureties_id_seq'::regclass);


--
-- TOC entry 3915 (class 2604 OID 1397702)
-- Name: ir_types_of_drugs id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_types_of_drugs ALTER COLUMN id SET DEFAULT nextval('public.ir_types_of_drugs_id_seq'::regclass);


--
-- TOC entry 4014 (class 2604 OID 37993008)
-- Name: mo_seizure_media id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.mo_seizure_media ALTER COLUMN id SET DEFAULT nextval('public.mo_seizure_media_id_seq'::regclass);


--
-- TOC entry 3943 (class 2604 OID 1413818)
-- Name: person_deduplication_tracker id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.person_deduplication_tracker ALTER COLUMN id SET DEFAULT nextval('public.person_deduplication_tracker_id_seq'::regclass);


--
-- TOC entry 3988 (class 2604 OID 25200391)
-- Name: properties_pending_fk id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.properties_pending_fk ALTER COLUMN id SET DEFAULT nextval('public.properties_pending_fk_id_seq'::regclass);


--
-- TOC entry 4018 (class 2604 OID 38052569)
-- Name: property_media id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.property_media ALTER COLUMN id SET DEFAULT nextval('public.property_media_id_seq'::regclass);


--
-- TOC entry 4056 (class 2606 OID 1397606)
-- Name: accused accused_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.accused
    ADD CONSTRAINT accused_pkey PRIMARY KEY (accused_id);


--
-- TOC entry 4058 (class 2606 OID 1397608)
-- Name: accused accused_seq_num_key; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.accused
    ADD CONSTRAINT accused_seq_num_key UNIQUE (seq_num);


--
-- TOC entry 4208 (class 2606 OID 2028697)
-- Name: agent_deduplication_tracker agent_deduplication_tracker_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.agent_deduplication_tracker
    ADD CONSTRAINT agent_deduplication_tracker_pkey PRIMARY KEY (id);


--
-- TOC entry 4168 (class 2606 OID 1420503)
-- Name: arrests arrests_crime_id_accused_seq_no_key; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.arrests
    ADD CONSTRAINT arrests_crime_id_accused_seq_no_key UNIQUE (crime_id, accused_seq_no);


--
-- TOC entry 4170 (class 2606 OID 1420501)
-- Name: arrests arrests_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.arrests
    ADD CONSTRAINT arrests_pkey PRIMARY KEY (id);


--
-- TOC entry 4132 (class 2606 OID 1404629)
-- Name: brief_facts_accused brief_facts_accused_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.brief_facts_accused
    ADD CONSTRAINT brief_facts_accused_pkey PRIMARY KEY (bf_accused_id);


--
-- TOC entry 4130 (class 2606 OID 1404612)
-- Name: brief_facts_crime_summaries brief_facts_crime_summaries_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.brief_facts_crime_summaries
    ADD CONSTRAINT brief_facts_crime_summaries_pkey PRIMARY KEY (crime_id);


--
-- TOC entry 4236 (class 2606 OID 22014304)
-- Name: brief_facts_drug brief_facts_drug_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.brief_facts_drug
    ADD CONSTRAINT brief_facts_drug_pkey PRIMARY KEY (id);


--
-- TOC entry 4309 (class 2606 OID 38052681)
-- Name: case_property_media case_property_media_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.case_property_media
    ADD CONSTRAINT case_property_media_pkey PRIMARY KEY (case_property_id, media_index);


--
-- TOC entry 4193 (class 2606 OID 1639319)
-- Name: charge_sheet_updates charge_sheet_updates_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.charge_sheet_updates
    ADD CONSTRAINT charge_sheet_updates_pkey PRIMARY KEY (id);


--
-- TOC entry 4195 (class 2606 OID 1639321)
-- Name: charge_sheet_updates charge_sheet_updates_update_charge_sheet_id_key; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.charge_sheet_updates
    ADD CONSTRAINT charge_sheet_updates_update_charge_sheet_id_key UNIQUE (update_charge_sheet_id);


--
-- TOC entry 4187 (class 2606 OID 1422349)
-- Name: chargesheet_accused chargesheet_accused_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.chargesheet_accused
    ADD CONSTRAINT chargesheet_accused_pkey PRIMARY KEY (id);


--
-- TOC entry 4191 (class 2606 OID 1422368)
-- Name: chargesheet_acts chargesheet_acts_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.chargesheet_acts
    ADD CONSTRAINT chargesheet_acts_pkey PRIMARY KEY (id);


--
-- TOC entry 4321 (class 2606 OID 38245030)
-- Name: chargesheet_acts_sections chargesheet_acts_sections_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.chargesheet_acts_sections
    ADD CONSTRAINT chargesheet_acts_sections_pkey PRIMARY KEY (id);


--
-- TOC entry 4323 (class 2606 OID 38245032)
-- Name: chargesheet_acts_sections chargesheet_acts_sections_unique_entry; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.chargesheet_acts_sections
    ADD CONSTRAINT chargesheet_acts_sections_unique_entry UNIQUE (chargesheet_id, act_index, section_index);


--
-- TOC entry 4185 (class 2606 OID 1422329)
-- Name: chargesheet_files chargesheet_files_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.chargesheet_files
    ADD CONSTRAINT chargesheet_files_pkey PRIMARY KEY (id);


--
-- TOC entry 4315 (class 2606 OID 38245015)
-- Name: chargesheet_media chargesheet_media_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.chargesheet_media
    ADD CONSTRAINT chargesheet_media_pkey PRIMARY KEY (id);


--
-- TOC entry 4317 (class 2606 OID 38245017)
-- Name: chargesheet_media chargesheet_media_unique_entry; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.chargesheet_media
    ADD CONSTRAINT chargesheet_media_unique_entry UNIQUE (chargesheet_id, media_index);


--
-- TOC entry 4179 (class 2606 OID 1422318)
-- Name: chargesheets chargesheets_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.chargesheets
    ADD CONSTRAINT chargesheets_pkey PRIMARY KEY (id);


--
-- TOC entry 4046 (class 2606 OID 1397592)
-- Name: crimes crimes_fir_reg_num_key; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.crimes
    ADD CONSTRAINT crimes_fir_reg_num_key UNIQUE (fir_reg_num);


--
-- TOC entry 4048 (class 2606 OID 1397590)
-- Name: crimes crimes_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.crimes
    ADD CONSTRAINT crimes_pkey PRIMARY KEY (crime_id);


--
-- TOC entry 4212 (class 2606 OID 2028699)
-- Name: dedup_cluster_state dedup_cluster_state_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.dedup_cluster_state
    ADD CONSTRAINT dedup_cluster_state_pkey PRIMARY KEY (id);


--
-- TOC entry 4216 (class 2606 OID 2028701)
-- Name: dedup_comparison_progress dedup_comparison_progress_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.dedup_comparison_progress
    ADD CONSTRAINT dedup_comparison_progress_pkey PRIMARY KEY (id);


--
-- TOC entry 4219 (class 2606 OID 2028710)
-- Name: dedup_run_metadata dedup_run_metadata_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.dedup_run_metadata
    ADD CONSTRAINT dedup_run_metadata_pkey PRIMARY KEY (id);


--
-- TOC entry 4221 (class 2606 OID 2028712)
-- Name: dedup_run_metadata dedup_run_metadata_run_id_key; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.dedup_run_metadata
    ADD CONSTRAINT dedup_run_metadata_run_id_key UNIQUE (run_id);


--
-- TOC entry 4162 (class 2606 OID 38052644)
-- Name: disposal disposal_crime_id_disposal_type_disposed_at_key; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.disposal
    ADD CONSTRAINT disposal_crime_id_disposal_type_disposed_at_key UNIQUE NULLS NOT DISTINCT (crime_id, disposal_type, disposed_at);


--
-- TOC entry 4164 (class 2606 OID 1420061)
-- Name: disposal disposal_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.disposal
    ADD CONSTRAINT disposal_pkey PRIMARY KEY (id);


--
-- TOC entry 4223 (class 2606 OID 20996621)
-- Name: drug_categories drug_categories_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.drug_categories
    ADD CONSTRAINT drug_categories_pkey PRIMARY KEY (id);


--
-- TOC entry 4225 (class 2606 OID 20996623)
-- Name: drug_categories drug_categories_raw_name_key; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.drug_categories
    ADD CONSTRAINT drug_categories_raw_name_key UNIQUE (raw_name);


--
-- TOC entry 4230 (class 2606 OID 20996634)
-- Name: drug_ignore_list drug_ignore_list_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.drug_ignore_list
    ADD CONSTRAINT drug_ignore_list_pkey PRIMARY KEY (id);


--
-- TOC entry 4232 (class 2606 OID 20996636)
-- Name: drug_ignore_list drug_ignore_list_term_key; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.drug_ignore_list
    ADD CONSTRAINT drug_ignore_list_term_key UNIQUE (term);


--
-- TOC entry 4144 (class 2606 OID 1412938)
-- Name: files files_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.files
    ADD CONSTRAINT files_pkey PRIMARY KEY (id);


--
-- TOC entry 4030 (class 2606 OID 38052670)
-- Name: fsl_case_property fsl_case_property_crime_id_not_null_chk; Type: CHECK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE public.fsl_case_property
    ADD CONSTRAINT fsl_case_property_crime_id_not_null_chk CHECK ((crime_id IS NOT NULL)) NOT VALID;


--
-- TOC entry 4205 (class 2606 OID 1639538)
-- Name: fsl_case_property_media fsl_case_property_media_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.fsl_case_property_media
    ADD CONSTRAINT fsl_case_property_media_pkey PRIMARY KEY (media_id);


--
-- TOC entry 4200 (class 2606 OID 1639526)
-- Name: fsl_case_property fsl_case_property_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.fsl_case_property
    ADD CONSTRAINT fsl_case_property_pkey PRIMARY KEY (case_property_id);


--
-- TOC entry 4245 (class 2606 OID 23469820)
-- Name: geo_reference geo_reference_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.geo_reference
    ADD CONSTRAINT geo_reference_pkey PRIMARY KEY (id);


--
-- TOC entry 4034 (class 2606 OID 1397575)
-- Name: hierarchy hierarchy_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.hierarchy
    ADD CONSTRAINT hierarchy_pkey PRIMARY KEY (ps_code);


--
-- TOC entry 4077 (class 2606 OID 1397647)
-- Name: interrogation_reports interrogation_reports_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.interrogation_reports
    ADD CONSTRAINT interrogation_reports_pkey PRIMARY KEY (interrogation_report_id);


--
-- TOC entry 4113 (class 2606 OID 1397804)
-- Name: ir_associate_details ir_associate_details_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_associate_details
    ADD CONSTRAINT ir_associate_details_pkey PRIMARY KEY (id);


--
-- TOC entry 4101 (class 2606 OID 1397748)
-- Name: ir_consumer_details ir_consumer_details_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_consumer_details
    ADD CONSTRAINT ir_consumer_details_pkey PRIMARY KEY (id);


--
-- TOC entry 4290 (class 2606 OID 37992945)
-- Name: ir_conviction_acquittal ir_conviction_acquittal_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_conviction_acquittal
    ADD CONSTRAINT ir_conviction_acquittal_pkey PRIMARY KEY (id);


--
-- TOC entry 4110 (class 2606 OID 1397790)
-- Name: ir_defence_counsel ir_defence_counsel_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_defence_counsel
    ADD CONSTRAINT ir_defence_counsel_pkey PRIMARY KEY (id);


--
-- TOC entry 4128 (class 2606 OID 1397864)
-- Name: ir_dopams_links ir_dopams_links_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_dopams_links
    ADD CONSTRAINT ir_dopams_links_pkey PRIMARY KEY (id);


--
-- TOC entry 4275 (class 2606 OID 37992865)
-- Name: ir_execution_of_nbw ir_execution_of_nbw_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_execution_of_nbw
    ADD CONSTRAINT ir_execution_of_nbw_pkey PRIMARY KEY (id);


--
-- TOC entry 4081 (class 2606 OID 1397664)
-- Name: ir_family_history ir_family_history_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_family_history
    ADD CONSTRAINT ir_family_history_pkey PRIMARY KEY (id);


--
-- TOC entry 4098 (class 2606 OID 1397734)
-- Name: ir_financial_history ir_financial_history_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_financial_history
    ADD CONSTRAINT ir_financial_history_pkey PRIMARY KEY (id);


--
-- TOC entry 4266 (class 2606 OID 37992817)
-- Name: ir_indulgance_before_offence ir_indulgance_before_offence_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_indulgance_before_offence
    ADD CONSTRAINT ir_indulgance_before_offence_pkey PRIMARY KEY (id);


--
-- TOC entry 4123 (class 2606 OID 1397848)
-- Name: ir_interrogation_report_refs ir_interrogation_report_refs_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_interrogation_report_refs
    ADD CONSTRAINT ir_interrogation_report_refs_pkey PRIMARY KEY (id);


--
-- TOC entry 4125 (class 2606 OID 1397935)
-- Name: ir_interrogation_report_refs ir_interrogation_report_refs_unique; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_interrogation_report_refs
    ADD CONSTRAINT ir_interrogation_report_refs_unique UNIQUE (interrogation_report_id, report_ref_id);


--
-- TOC entry 4284 (class 2606 OID 37992913)
-- Name: ir_jail_sentence ir_jail_sentence_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_jail_sentence
    ADD CONSTRAINT ir_jail_sentence_pkey PRIMARY KEY (id);


--
-- TOC entry 4084 (class 2606 OID 1397678)
-- Name: ir_local_contacts ir_local_contacts_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_local_contacts
    ADD CONSTRAINT ir_local_contacts_pkey PRIMARY KEY (id);


--
-- TOC entry 4119 (class 2606 OID 1397832)
-- Name: ir_media ir_media_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_media
    ADD CONSTRAINT ir_media_pkey PRIMARY KEY (id);


--
-- TOC entry 4121 (class 2606 OID 1397933)
-- Name: ir_media ir_media_unique; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_media
    ADD CONSTRAINT ir_media_unique UNIQUE (interrogation_report_id, media_id);


--
-- TOC entry 4104 (class 2606 OID 1397762)
-- Name: ir_modus_operandi ir_modus_operandi_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_modus_operandi
    ADD CONSTRAINT ir_modus_operandi_pkey PRIMARY KEY (id);


--
-- TOC entry 4287 (class 2606 OID 37992929)
-- Name: ir_new_gang_formation ir_new_gang_formation_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_new_gang_formation
    ADD CONSTRAINT ir_new_gang_formation_pkey PRIMARY KEY (id);


--
-- TOC entry 4257 (class 2606 OID 25200411)
-- Name: ir_pending_fk ir_pending_fk_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_pending_fk
    ADD CONSTRAINT ir_pending_fk_pkey PRIMARY KEY (id);


--
-- TOC entry 4278 (class 2606 OID 37992881)
-- Name: ir_pending_nbw ir_pending_nbw_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_pending_nbw
    ADD CONSTRAINT ir_pending_nbw_pkey PRIMARY KEY (id);


--
-- TOC entry 4107 (class 2606 OID 1397776)
-- Name: ir_previous_offences_confessed ir_previous_offences_confessed_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_previous_offences_confessed
    ADD CONSTRAINT ir_previous_offences_confessed_pkey PRIMARY KEY (id);


--
-- TOC entry 4269 (class 2606 OID 37992833)
-- Name: ir_property_disposal ir_property_disposal_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_property_disposal
    ADD CONSTRAINT ir_property_disposal_pkey PRIMARY KEY (id);


--
-- TOC entry 4087 (class 2606 OID 1397690)
-- Name: ir_regular_habits ir_regular_habits_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_regular_habits
    ADD CONSTRAINT ir_regular_habits_pkey PRIMARY KEY (id);


--
-- TOC entry 4089 (class 2606 OID 1397692)
-- Name: ir_regular_habits ir_regular_habits_unique; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_regular_habits
    ADD CONSTRAINT ir_regular_habits_unique UNIQUE (interrogation_report_id, habit);


--
-- TOC entry 4272 (class 2606 OID 37992849)
-- Name: ir_regularization_transit_warrants ir_regularization_transit_warrants_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_regularization_transit_warrants
    ADD CONSTRAINT ir_regularization_transit_warrants_pkey PRIMARY KEY (id);


--
-- TOC entry 4116 (class 2606 OID 1397818)
-- Name: ir_shelter ir_shelter_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_shelter
    ADD CONSTRAINT ir_shelter_pkey PRIMARY KEY (id);


--
-- TOC entry 4095 (class 2606 OID 1397720)
-- Name: ir_sim_details ir_sim_details_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_sim_details
    ADD CONSTRAINT ir_sim_details_pkey PRIMARY KEY (id);


--
-- TOC entry 4281 (class 2606 OID 37992897)
-- Name: ir_sureties ir_sureties_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_sureties
    ADD CONSTRAINT ir_sureties_pkey PRIMARY KEY (id);


--
-- TOC entry 4092 (class 2606 OID 1397706)
-- Name: ir_types_of_drugs ir_types_of_drugs_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_types_of_drugs
    ADD CONSTRAINT ir_types_of_drugs_pkey PRIMARY KEY (id);


--
-- TOC entry 4294 (class 2606 OID 37993015)
-- Name: mo_seizure_media mo_seizure_media_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.mo_seizure_media
    ADD CONSTRAINT mo_seizure_media_pkey PRIMARY KEY (id);


--
-- TOC entry 4296 (class 2606 OID 37993017)
-- Name: mo_seizure_media mo_seizure_media_unique_entry; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.mo_seizure_media
    ADD CONSTRAINT mo_seizure_media_unique_entry UNIQUE (mo_seizure_id, media_index);


--
-- TOC entry 4177 (class 2606 OID 1420939)
-- Name: mo_seizures mo_seizures_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.mo_seizures
    ADD CONSTRAINT mo_seizures_pkey PRIMARY KEY (mo_seizure_id);


--
-- TOC entry 4156 (class 2606 OID 1413504)
-- Name: old_interragation_report old_interragation_report_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.old_interragation_report
    ADD CONSTRAINT old_interragation_report_pkey PRIMARY KEY (interrogation_report_id);


--
-- TOC entry 4158 (class 2606 OID 1413831)
-- Name: person_deduplication_tracker person_deduplication_tracker_person_fingerprint_key; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.person_deduplication_tracker
    ADD CONSTRAINT person_deduplication_tracker_person_fingerprint_key UNIQUE (person_fingerprint);


--
-- TOC entry 4160 (class 2606 OID 1413829)
-- Name: person_deduplication_tracker person_deduplication_tracker_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.person_deduplication_tracker
    ADD CONSTRAINT person_deduplication_tracker_pkey PRIMARY KEY (id);


--
-- TOC entry 4042 (class 2606 OID 1397583)
-- Name: persons persons_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.persons
    ADD CONSTRAINT persons_pkey PRIMARY KEY (person_id);


--
-- TOC entry 4254 (class 2606 OID 25200398)
-- Name: properties_pending_fk properties_pending_fk_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.properties_pending_fk
    ADD CONSTRAINT properties_pending_fk_pkey PRIMARY KEY (id);


--
-- TOC entry 4070 (class 2606 OID 1397628)
-- Name: properties properties_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.properties
    ADD CONSTRAINT properties_pkey PRIMARY KEY (property_id);


--
-- TOC entry 4300 (class 2606 OID 38052557)
-- Name: property_additional_details property_additional_details_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.property_additional_details
    ADD CONSTRAINT property_additional_details_pkey PRIMARY KEY (property_id);


--
-- TOC entry 4305 (class 2606 OID 38052573)
-- Name: property_media property_media_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.property_media
    ADD CONSTRAINT property_media_pkey PRIMARY KEY (id);


--
-- TOC entry 4307 (class 2606 OID 38052575)
-- Name: property_media property_media_unique_entry; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.property_media
    ADD CONSTRAINT property_media_unique_entry UNIQUE (property_id, media_index);


--
-- TOC entry 4214 (class 2606 OID 2028714)
-- Name: dedup_cluster_state uix_cluster_person; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.dedup_cluster_state
    ADD CONSTRAINT uix_cluster_person UNIQUE (cluster_id, person_index);


--
-- TOC entry 4154 (class 2606 OID 1412940)
-- Name: files unique_file_per_source; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.files
    ADD CONSTRAINT unique_file_per_source UNIQUE (source_type, source_field, parent_id, file_id, file_index);


--
-- TOC entry 4139 (class 2606 OID 1404631)
-- Name: brief_facts_accused uq_bf_accused_id_accused_id; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.brief_facts_accused
    ADD CONSTRAINT uq_bf_accused_id_accused_id UNIQUE (bf_accused_id, accused_id);


--
-- TOC entry 4142 (class 2606 OID 1404748)
-- Name: user user_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public."user"
    ADD CONSTRAINT user_pkey PRIMARY KEY (id);


--
-- TOC entry 4312 (class 1259 OID 38245004)
-- Name: firs_search_idx; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX firs_search_idx ON public.firs_mv USING gin (((((setweight(to_tsvector('english'::regconfig, (COALESCE("firNumber", ''::character varying))::text), 'A'::"char") || setweight(to_tsvector('english'::regconfig, (COALESCE(ps, ''::character varying))::text), 'B'::"char")) || setweight(to_tsvector('english'::regconfig, (COALESCE("majorHead", ''::character varying))::text), 'C'::"char")) || setweight(to_tsvector('english'::regconfig, (COALESCE("ioName", ''::character varying))::text), 'D'::"char"))));


--
-- TOC entry 4239 (class 1259 OID 26454181)
-- Name: geo_ref_district_trgm; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX geo_ref_district_trgm ON public.geo_reference USING gin (district_name public.gin_trgm_ops);


--
-- TOC entry 4240 (class 1259 OID 26454182)
-- Name: geo_ref_mandal_trgm; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX geo_ref_mandal_trgm ON public.geo_reference USING gin (sub_district_name public.gin_trgm_ops);


--
-- TOC entry 4241 (class 1259 OID 26454180)
-- Name: geo_ref_state_trgm; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX geo_ref_state_trgm ON public.geo_reference USING gin (state_name public.gin_trgm_ops);


--
-- TOC entry 4242 (class 1259 OID 26454112)
-- Name: geo_ref_trgm_idx; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX geo_ref_trgm_idx ON public.geo_reference USING gin (district_name public.gin_trgm_ops, sub_district_name public.gin_trgm_ops);


--
-- TOC entry 4243 (class 1259 OID 26454183)
-- Name: geo_reference_mandal_trgm; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX geo_reference_mandal_trgm ON public.geo_reference USING gin (sub_district_name public.gin_trgm_ops);


--
-- TOC entry 4059 (class 1259 OID 1397888)
-- Name: idx_accused_code; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_accused_code ON public.accused USING btree (accused_code);


--
-- TOC entry 4060 (class 1259 OID 1397886)
-- Name: idx_accused_crime; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_accused_crime ON public.accused USING btree (crime_id);


--
-- TOC entry 4061 (class 1259 OID 23355611)
-- Name: idx_accused_crime_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_accused_crime_id ON public.accused USING btree (crime_id);


--
-- TOC entry 4062 (class 1259 OID 23355600)
-- Name: idx_accused_crime_person; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_accused_crime_person ON public.accused USING btree (crime_id, person_id);


--
-- TOC entry 4063 (class 1259 OID 1397887)
-- Name: idx_accused_person; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_accused_person ON public.accused USING btree (person_id);


--
-- TOC entry 4064 (class 1259 OID 26424259)
-- Name: idx_accused_status; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_accused_status ON public.accused USING btree (accused_status);


--
-- TOC entry 4258 (class 1259 OID 34762915)
-- Name: idx_accuseds_mv_accused_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_accuseds_mv_accused_id ON public.accuseds_mv USING btree (id);


--
-- TOC entry 4259 (class 1259 OID 34762914)
-- Name: idx_accuseds_mv_crime_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_accuseds_mv_crime_id ON public.accuseds_mv USING btree ("crimeId");


--
-- TOC entry 4260 (class 1259 OID 34762913)
-- Name: idx_accuseds_mv_person_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_accuseds_mv_person_id ON public.accuseds_mv USING btree ("personId");


--
-- TOC entry 4209 (class 1259 OID 23355609)
-- Name: idx_adt_all_person_ids_gin; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_adt_all_person_ids_gin ON public.agent_deduplication_tracker USING gin (all_person_ids);


--
-- TOC entry 4210 (class 1259 OID 23355610)
-- Name: idx_adt_canonical_person_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_adt_canonical_person_id ON public.agent_deduplication_tracker USING btree (canonical_person_id);


--
-- TOC entry 4171 (class 1259 OID 1420514)
-- Name: idx_arrests_crime; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_arrests_crime ON public.arrests USING btree (crime_id);


--
-- TOC entry 4172 (class 1259 OID 1420515)
-- Name: idx_arrests_person; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_arrests_person ON public.arrests USING btree (person_id);


--
-- TOC entry 4133 (class 1259 OID 26473114)
-- Name: idx_bf_accused_accused_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_bf_accused_accused_id ON public.brief_facts_accused USING btree (accused_id);


--
-- TOC entry 4134 (class 1259 OID 26473112)
-- Name: idx_bf_accused_crime_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_bf_accused_crime_id ON public.brief_facts_accused USING btree (crime_id);


--
-- TOC entry 4135 (class 1259 OID 26473113)
-- Name: idx_bf_accused_person_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_bf_accused_person_id ON public.brief_facts_accused USING btree (person_id);


--
-- TOC entry 4237 (class 1259 OID 22014315)
-- Name: idx_bfd_crime_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_bfd_crime_id ON public.brief_facts_drug USING btree (crime_id);


--
-- TOC entry 4238 (class 1259 OID 22014317)
-- Name: idx_bfd_primary_drug; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_bfd_primary_drug ON public.brief_facts_drug USING btree (primary_drug_name);


--
-- TOC entry 4136 (class 1259 OID 1404633)
-- Name: idx_brief_facts_accused_crime_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_brief_facts_accused_crime_id ON public.brief_facts_accused USING btree (crime_id);


--
-- TOC entry 4137 (class 1259 OID 1404634)
-- Name: idx_brief_facts_accused_crime_person; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_brief_facts_accused_crime_person ON public.brief_facts_accused USING btree (crime_id, person_id);


--
-- TOC entry 4310 (class 1259 OID 38052689)
-- Name: idx_case_property_media_case_property_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_case_property_media_case_property_id ON public.case_property_media USING btree (case_property_id);


--
-- TOC entry 4311 (class 1259 OID 38052690)
-- Name: idx_case_property_media_file_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_case_property_media_file_id ON public.case_property_media USING btree (file_id);


--
-- TOC entry 4196 (class 1259 OID 38079273)
-- Name: idx_charge_sheet_updates_date_modified; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_charge_sheet_updates_date_modified ON public.charge_sheet_updates USING btree (date_modified);


--
-- TOC entry 4197 (class 1259 OID 1639331)
-- Name: idx_charge_sheet_updates_status; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_charge_sheet_updates_status ON public.charge_sheet_updates USING btree (charge_sheet_status);


--
-- TOC entry 4198 (class 1259 OID 1639328)
-- Name: idx_charge_sheet_updates_update_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_charge_sheet_updates_update_id ON public.charge_sheet_updates USING btree (update_charge_sheet_id);


--
-- TOC entry 4188 (class 1259 OID 23355604)
-- Name: idx_chargesheet_accused_cs_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_chargesheet_accused_cs_id ON public.chargesheet_accused USING btree (chargesheet_id);


--
-- TOC entry 4189 (class 1259 OID 23355605)
-- Name: idx_chargesheet_accused_cs_person; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_chargesheet_accused_cs_person ON public.chargesheet_accused USING btree (chargesheet_id, accused_person_id);


--
-- TOC entry 4324 (class 1259 OID 38245033)
-- Name: idx_chargesheet_acts_sections_chargesheet_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_chargesheet_acts_sections_chargesheet_id ON public.chargesheet_acts_sections USING btree (chargesheet_id);


--
-- TOC entry 4325 (class 1259 OID 38245034)
-- Name: idx_chargesheet_acts_sections_section; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_chargesheet_acts_sections_section ON public.chargesheet_acts_sections USING btree (section);


--
-- TOC entry 4318 (class 1259 OID 38245018)
-- Name: idx_chargesheet_media_chargesheet_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_chargesheet_media_chargesheet_id ON public.chargesheet_media USING btree (chargesheet_id);


--
-- TOC entry 4319 (class 1259 OID 38245019)
-- Name: idx_chargesheet_media_file_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_chargesheet_media_file_id ON public.chargesheet_media USING btree (file_id);


--
-- TOC entry 4180 (class 1259 OID 38213775)
-- Name: idx_chargesheets_charge_sheet_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE UNIQUE INDEX idx_chargesheets_charge_sheet_id ON public.chargesheets USING btree (charge_sheet_id) WHERE (charge_sheet_id IS NOT NULL);


--
-- TOC entry 4181 (class 1259 OID 23355602)
-- Name: idx_chargesheets_crime_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_chargesheets_crime_id ON public.chargesheets USING btree (crime_id);


--
-- TOC entry 4182 (class 1259 OID 38213785)
-- Name: idx_chargesheets_crime_no_date; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_chargesheets_crime_no_date ON public.chargesheets USING btree (crime_id, chargesheet_no, chargesheet_date);


--
-- TOC entry 4183 (class 1259 OID 38213815)
-- Name: idx_chargesheets_date_modified; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_chargesheets_date_modified ON public.chargesheets USING btree (date_modified);


--
-- TOC entry 4049 (class 1259 OID 1397882)
-- Name: idx_crimes_case_status; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_crimes_case_status ON public.crimes USING btree (case_status);


--
-- TOC entry 4050 (class 1259 OID 23355601)
-- Name: idx_crimes_dates; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_crimes_dates ON public.crimes USING btree (date_created DESC, date_modified DESC);


--
-- TOC entry 4051 (class 1259 OID 1397880)
-- Name: idx_crimes_fir_date; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_crimes_fir_date ON public.crimes USING btree (fir_date);


--
-- TOC entry 4052 (class 1259 OID 1397883)
-- Name: idx_crimes_fir_num; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_crimes_fir_num ON public.crimes USING btree (fir_num);


--
-- TOC entry 4053 (class 1259 OID 1397879)
-- Name: idx_crimes_ps_code; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_crimes_ps_code ON public.crimes USING btree (ps_code);


--
-- TOC entry 4261 (class 1259 OID 34844899)
-- Name: idx_criminal_profiles_mv_fullname; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_criminal_profiles_mv_fullname ON public.criminal_profiles_mv USING btree ("fullName");


--
-- TOC entry 4262 (class 1259 OID 34844898)
-- Name: idx_criminal_profiles_mv_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE UNIQUE INDEX idx_criminal_profiles_mv_id ON public.criminal_profiles_mv USING btree (id);


--
-- TOC entry 4263 (class 1259 OID 34844900)
-- Name: idx_criminal_profiles_mv_noofcrimes; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_criminal_profiles_mv_noofcrimes ON public.criminal_profiles_mv USING btree ("noOfCrimes");


--
-- TOC entry 4165 (class 1259 OID 1420069)
-- Name: idx_disposal_crime; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_disposal_crime ON public.disposal USING btree (crime_id);


--
-- TOC entry 4166 (class 1259 OID 38052645)
-- Name: idx_disposal_date_modified; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_disposal_date_modified ON public.disposal USING btree (date_modified);


--
-- TOC entry 4226 (class 1259 OID 23355606)
-- Name: idx_drug_categories_raw_name; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_drug_categories_raw_name ON public.drug_categories USING btree (raw_name);


--
-- TOC entry 4227 (class 1259 OID 23355607)
-- Name: idx_drug_categories_standard_name; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_drug_categories_standard_name ON public.drug_categories USING btree (standard_name);


--
-- TOC entry 4233 (class 1259 OID 23355608)
-- Name: idx_drug_ignore_list_term; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_drug_ignore_list_term ON public.drug_ignore_list USING btree (term);


--
-- TOC entry 4145 (class 1259 OID 10628347)
-- Name: idx_files_created_at; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_files_created_at ON public.files USING btree (created_at);


--
-- TOC entry 4146 (class 1259 OID 25648888)
-- Name: idx_files_downloaded_at; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_files_downloaded_at ON public.files USING btree (downloaded_at) WHERE (downloaded_at IS NOT NULL);


--
-- TOC entry 4147 (class 1259 OID 33365243)
-- Name: idx_files_file_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_files_file_id ON public.files USING btree (file_id) WHERE (file_id IS NOT NULL);


--
-- TOC entry 4148 (class 1259 OID 25648887)
-- Name: idx_files_is_downloaded; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_files_is_downloaded ON public.files USING btree (is_downloaded) WHERE (is_downloaded = true);


--
-- TOC entry 4149 (class 1259 OID 1412943)
-- Name: idx_files_parent_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_files_parent_id ON public.files USING btree (parent_id);


--
-- TOC entry 4150 (class 1259 OID 1412945)
-- Name: idx_files_source_parent; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_files_source_parent ON public.files USING btree (source_type, parent_id);


--
-- TOC entry 4151 (class 1259 OID 25648889)
-- Name: idx_files_source_type_created; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_files_source_type_created ON public.files USING btree (source_type, created_at);


--
-- TOC entry 4152 (class 1259 OID 33365242)
-- Name: idx_files_unique_null_file_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE UNIQUE INDEX idx_files_unique_null_file_id ON public.files USING btree (source_type, source_field, parent_id) WHERE (file_id IS NULL);


--
-- TOC entry 4313 (class 1259 OID 38245006)
-- Name: idx_firs_mv_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE UNIQUE INDEX idx_firs_mv_id ON public.firs_mv USING btree (id);


--
-- TOC entry 4201 (class 1259 OID 1639545)
-- Name: idx_fsl_case_property_crime_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_fsl_case_property_crime_id ON public.fsl_case_property USING btree (crime_id);


--
-- TOC entry 4202 (class 1259 OID 38052688)
-- Name: idx_fsl_case_property_date_modified; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_fsl_case_property_date_modified ON public.fsl_case_property USING btree (date_modified);


--
-- TOC entry 4206 (class 1259 OID 1639552)
-- Name: idx_fsl_case_property_media_case_property_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_fsl_case_property_media_case_property_id ON public.fsl_case_property_media USING btree (case_property_id);


--
-- TOC entry 4203 (class 1259 OID 38052687)
-- Name: idx_fsl_case_property_mo_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_fsl_case_property_mo_id ON public.fsl_case_property USING btree (mo_id);


--
-- TOC entry 4250 (class 1259 OID 24850411)
-- Name: idx_geo_countries_country_trgm; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_geo_countries_country_trgm ON public.geo_countries USING gin (country_name public.gin_trgm_ops);


--
-- TOC entry 4251 (class 1259 OID 24850412)
-- Name: idx_geo_countries_state_trgm; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_geo_countries_state_trgm ON public.geo_countries USING gin (state_name public.gin_trgm_ops);


--
-- TOC entry 4234 (class 1259 OID 20996637)
-- Name: idx_ignore_term; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ignore_term ON public.drug_ignore_list USING btree (term);


--
-- TOC entry 4111 (class 1259 OID 1397926)
-- Name: idx_ir_associate_details_ir; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_associate_details_ir ON public.ir_associate_details USING btree (interrogation_report_id);


--
-- TOC entry 4099 (class 1259 OID 1397922)
-- Name: idx_ir_consumer_details_ir; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_consumer_details_ir ON public.ir_consumer_details USING btree (interrogation_report_id);


--
-- TOC entry 4288 (class 1259 OID 37992951)
-- Name: idx_ir_conviction_acquittal_ir_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_conviction_acquittal_ir_id ON public.ir_conviction_acquittal USING btree (interrogation_report_id);


--
-- TOC entry 4071 (class 1259 OID 1397898)
-- Name: idx_ir_crime_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_crime_id ON public.interrogation_reports USING btree (crime_id);


--
-- TOC entry 4108 (class 1259 OID 1397925)
-- Name: idx_ir_defence_counsel_ir; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_defence_counsel_ir ON public.ir_defence_counsel USING btree (interrogation_report_id);


--
-- TOC entry 4126 (class 1259 OID 1397930)
-- Name: idx_ir_dopams_links_ir; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_dopams_links_ir ON public.ir_dopams_links USING btree (interrogation_report_id);


--
-- TOC entry 4273 (class 1259 OID 37992871)
-- Name: idx_ir_execution_of_nbw_ir_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_execution_of_nbw_ir_id ON public.ir_execution_of_nbw USING btree (interrogation_report_id);


--
-- TOC entry 4078 (class 1259 OID 1397912)
-- Name: idx_ir_family_history_ir; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_family_history_ir ON public.ir_family_history USING btree (interrogation_report_id);


--
-- TOC entry 4079 (class 1259 OID 1397913)
-- Name: idx_ir_family_history_person; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_family_history_person ON public.ir_family_history USING btree (person_id);


--
-- TOC entry 4096 (class 1259 OID 1397921)
-- Name: idx_ir_financial_history_ir; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_financial_history_ir ON public.ir_financial_history USING btree (interrogation_report_id);


--
-- TOC entry 4264 (class 1259 OID 37992823)
-- Name: idx_ir_indulgance_before_offence_ir_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_indulgance_before_offence_ir_id ON public.ir_indulgance_before_offence USING btree (interrogation_report_id);


--
-- TOC entry 4072 (class 1259 OID 1397908)
-- Name: idx_ir_is_in_jail; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_is_in_jail ON public.interrogation_reports USING btree (is_in_jail);


--
-- TOC entry 4282 (class 1259 OID 37992919)
-- Name: idx_ir_jail_sentence_ir_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_jail_sentence_ir_id ON public.ir_jail_sentence USING btree (interrogation_report_id);


--
-- TOC entry 4082 (class 1259 OID 1397914)
-- Name: idx_ir_local_contacts_ir; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_local_contacts_ir ON public.ir_local_contacts USING btree (interrogation_report_id);


--
-- TOC entry 4117 (class 1259 OID 1397928)
-- Name: idx_ir_media_ir; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_media_ir ON public.ir_media USING btree (interrogation_report_id);


--
-- TOC entry 4102 (class 1259 OID 1397923)
-- Name: idx_ir_modus_operandi_ir; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_modus_operandi_ir ON public.ir_modus_operandi USING btree (interrogation_report_id);


--
-- TOC entry 4285 (class 1259 OID 37992935)
-- Name: idx_ir_new_gang_formation_ir_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_new_gang_formation_ir_id ON public.ir_new_gang_formation USING btree (interrogation_report_id);


--
-- TOC entry 4276 (class 1259 OID 37992887)
-- Name: idx_ir_pending_nbw_ir_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_pending_nbw_ir_id ON public.ir_pending_nbw USING btree (interrogation_report_id);


--
-- TOC entry 4073 (class 1259 OID 1397899)
-- Name: idx_ir_person_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_person_id ON public.interrogation_reports USING btree (person_id);


--
-- TOC entry 4105 (class 1259 OID 1397924)
-- Name: idx_ir_previous_offences_ir; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_previous_offences_ir ON public.ir_previous_offences_confessed USING btree (interrogation_report_id);


--
-- TOC entry 4267 (class 1259 OID 37992839)
-- Name: idx_ir_property_disposal_ir_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_property_disposal_ir_id ON public.ir_property_disposal USING btree (interrogation_report_id);


--
-- TOC entry 4085 (class 1259 OID 1397915)
-- Name: idx_ir_regular_habits_ir; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_regular_habits_ir ON public.ir_regular_habits USING btree (interrogation_report_id);


--
-- TOC entry 4270 (class 1259 OID 37992855)
-- Name: idx_ir_regularization_transit_warrants_ir_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_regularization_transit_warrants_ir_id ON public.ir_regularization_transit_warrants USING btree (interrogation_report_id);


--
-- TOC entry 4074 (class 1259 OID 37992963)
-- Name: idx_ir_reports_created_modified; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_reports_created_modified ON public.interrogation_reports USING btree (date_created, date_modified);


--
-- TOC entry 4075 (class 1259 OID 37992962)
-- Name: idx_ir_reports_crime_person; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_reports_crime_person ON public.interrogation_reports USING btree (crime_id, person_id);


--
-- TOC entry 4114 (class 1259 OID 1397927)
-- Name: idx_ir_shelter_ir; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_shelter_ir ON public.ir_shelter USING btree (interrogation_report_id);


--
-- TOC entry 4093 (class 1259 OID 1397919)
-- Name: idx_ir_sim_details_ir; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_sim_details_ir ON public.ir_sim_details USING btree (interrogation_report_id);


--
-- TOC entry 4279 (class 1259 OID 37992903)
-- Name: idx_ir_sureties_ir_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_sureties_ir_id ON public.ir_sureties USING btree (interrogation_report_id);


--
-- TOC entry 4090 (class 1259 OID 1397917)
-- Name: idx_ir_types_of_drugs_ir; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_types_of_drugs_ir ON public.ir_types_of_drugs USING btree (interrogation_report_id);


--
-- TOC entry 4291 (class 1259 OID 37993024)
-- Name: idx_mo_seizure_media_media_file_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_mo_seizure_media_media_file_id ON public.mo_seizure_media USING btree (media_file_id);


--
-- TOC entry 4292 (class 1259 OID 37993023)
-- Name: idx_mo_seizure_media_mo_seizure_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_mo_seizure_media_mo_seizure_id ON public.mo_seizure_media USING btree (mo_seizure_id);


--
-- TOC entry 4173 (class 1259 OID 1420945)
-- Name: idx_mo_seizures_crime; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_mo_seizures_crime ON public.mo_seizures USING btree (crime_id);


--
-- TOC entry 4174 (class 1259 OID 37993025)
-- Name: idx_mo_seizures_date_modified; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_mo_seizures_date_modified ON public.mo_seizures USING btree (date_modified);


--
-- TOC entry 4175 (class 1259 OID 1420946)
-- Name: idx_mo_seizures_seized_at; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_mo_seizures_seized_at ON public.mo_seizures USING btree (seized_at);


--
-- TOC entry 4255 (class 1259 OID 25200412)
-- Name: idx_pending_fk_ir_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE UNIQUE INDEX idx_pending_fk_ir_id ON public.ir_pending_fk USING btree (ir_id) WHERE (NOT resolved);


--
-- TOC entry 4252 (class 1259 OID 25200399)
-- Name: idx_pending_fk_property_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE UNIQUE INDEX idx_pending_fk_property_id ON public.properties_pending_fk USING btree (property_id) WHERE (NOT resolved);


--
-- TOC entry 4035 (class 1259 OID 24497541)
-- Name: idx_persons_email; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_persons_email ON public.persons USING btree (email_id);


--
-- TOC entry 4036 (class 1259 OID 1397875)
-- Name: idx_persons_full_name; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_persons_full_name ON public.persons USING btree (full_name);


--
-- TOC entry 4037 (class 1259 OID 1397874)
-- Name: idx_persons_name; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_persons_name ON public.persons USING btree (name);


--
-- TOC entry 4038 (class 1259 OID 26454157)
-- Name: idx_persons_person_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_persons_person_id ON public.persons USING btree (person_id);


--
-- TOC entry 4039 (class 1259 OID 1397876)
-- Name: idx_persons_phone; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_persons_phone ON public.persons USING btree (phone_number);


--
-- TOC entry 4040 (class 1259 OID 1397877)
-- Name: idx_persons_present_district; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_persons_present_district ON public.persons USING btree (present_district);


--
-- TOC entry 4065 (class 1259 OID 24497543)
-- Name: idx_properties_additional_details_gin; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_properties_additional_details_gin ON public.properties USING gin (additional_details);


--
-- TOC entry 4066 (class 1259 OID 1397889)
-- Name: idx_properties_crime_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_properties_crime_id ON public.properties USING btree (crime_id);


--
-- TOC entry 4067 (class 1259 OID 38052585)
-- Name: idx_properties_date_modified; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_properties_date_modified ON public.properties USING btree (date_modified);


--
-- TOC entry 4068 (class 1259 OID 38052584)
-- Name: idx_properties_date_seizure; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_properties_date_seizure ON public.properties USING btree (date_of_seizure);


--
-- TOC entry 4297 (class 1259 OID 38052563)
-- Name: idx_property_additional_details_date_modified; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_property_additional_details_date_modified ON public.property_additional_details USING btree (date_modified);


--
-- TOC entry 4298 (class 1259 OID 38052564)
-- Name: idx_property_additional_details_gin; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_property_additional_details_gin ON public.property_additional_details USING gin (additional_details);


--
-- TOC entry 4301 (class 1259 OID 38052583)
-- Name: idx_property_media_date_modified; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_property_media_date_modified ON public.property_media USING btree (date_modified);


--
-- TOC entry 4302 (class 1259 OID 38052582)
-- Name: idx_property_media_file_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_property_media_file_id ON public.property_media USING btree (media_file_id);


--
-- TOC entry 4303 (class 1259 OID 38052581)
-- Name: idx_property_media_property_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_property_media_property_id ON public.property_media USING btree (property_id);


--
-- TOC entry 4217 (class 1259 OID 2028722)
-- Name: ix_dedup_comparison_progress_person_i_index; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX ix_dedup_comparison_progress_person_i_index ON public.dedup_comparison_progress USING btree (person_i_index);


--
-- TOC entry 4054 (class 1259 OID 24497542)
-- Name: trgm_idx_crimes_acts_sections; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX trgm_idx_crimes_acts_sections ON public.crimes USING gin (acts_sections public.gin_trgm_ops);


--
-- TOC entry 4228 (class 1259 OID 20996624)
-- Name: trgm_idx_drug_raw_name; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX trgm_idx_drug_raw_name ON public.drug_categories USING gin (raw_name public.gin_trgm_ops);


--
-- TOC entry 4246 (class 1259 OID 23469823)
-- Name: trgm_idx_geo_district; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX trgm_idx_geo_district ON public.geo_reference USING gin (district_name public.gin_trgm_ops);


--
-- TOC entry 4247 (class 1259 OID 23469824)
-- Name: trgm_idx_geo_state; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX trgm_idx_geo_state ON public.geo_reference USING gin (state_name public.gin_trgm_ops);


--
-- TOC entry 4248 (class 1259 OID 23469822)
-- Name: trgm_idx_geo_sub_district; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX trgm_idx_geo_sub_district ON public.geo_reference USING gin (sub_district_name public.gin_trgm_ops);


--
-- TOC entry 4249 (class 1259 OID 23469821)
-- Name: trgm_idx_geo_village; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX trgm_idx_geo_village ON public.geo_reference USING gin (village_name_english public.gin_trgm_ops);


--
-- TOC entry 4043 (class 1259 OID 24497540)
-- Name: trgm_idx_persons_alias; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX trgm_idx_persons_alias ON public.persons USING gin (alias public.gin_trgm_ops);


--
-- TOC entry 4044 (class 1259 OID 24497539)
-- Name: trgm_idx_persons_full_name; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX trgm_idx_persons_full_name ON public.persons USING gin (full_name public.gin_trgm_ops);


--
-- TOC entry 4140 (class 1259 OID 1404749)
-- Name: user_email_key; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE UNIQUE INDEX user_email_key ON public."user" USING btree (email);


--
-- TOC entry 4378 (class 2620 OID 38052672)
-- Name: fsl_case_property trg_enforce_case_property_mo_reference; Type: TRIGGER; Schema: public; Owner: dev_dopamas
--

CREATE TRIGGER trg_enforce_case_property_mo_reference BEFORE INSERT OR UPDATE OF crime_id, mo_id ON public.fsl_case_property FOR EACH ROW EXECUTE FUNCTION public.enforce_case_property_mo_reference();


--
-- TOC entry 4377 (class 2620 OID 29321374)
-- Name: files trigger_auto_generate_file_paths; Type: TRIGGER; Schema: public; Owner: dev_dopamas
--

CREATE TRIGGER trigger_auto_generate_file_paths BEFORE INSERT OR UPDATE ON public.files FOR EACH ROW EXECUTE FUNCTION public.auto_generate_file_paths();


--
-- TOC entry 4327 (class 2606 OID 1397609)
-- Name: accused accused_crime_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.accused
    ADD CONSTRAINT accused_crime_id_fkey FOREIGN KEY (crime_id) REFERENCES public.crimes(crime_id) ON DELETE CASCADE;


--
-- TOC entry 4328 (class 2606 OID 1397614)
-- Name: accused accused_person_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.accused
    ADD CONSTRAINT accused_person_id_fkey FOREIGN KEY (person_id) REFERENCES public.persons(person_id) ON DELETE RESTRICT;


--
-- TOC entry 4350 (class 2606 OID 1420504)
-- Name: arrests arrests_crime_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.arrests
    ADD CONSTRAINT arrests_crime_id_fkey FOREIGN KEY (crime_id) REFERENCES public.crimes(crime_id);


--
-- TOC entry 4351 (class 2606 OID 1420509)
-- Name: arrests arrests_person_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.arrests
    ADD CONSTRAINT arrests_person_id_fkey FOREIGN KEY (person_id) REFERENCES public.persons(person_id);


--
-- TOC entry 4363 (class 2606 OID 22014305)
-- Name: brief_facts_drug brief_facts_drug_crime_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.brief_facts_drug
    ADD CONSTRAINT brief_facts_drug_crime_id_fkey FOREIGN KEY (crime_id) REFERENCES public.crimes(crime_id) ON DELETE CASCADE;


--
-- TOC entry 4376 (class 2606 OID 38052682)
-- Name: case_property_media case_property_media_case_property_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.case_property_media
    ADD CONSTRAINT case_property_media_case_property_id_fkey FOREIGN KEY (case_property_id) REFERENCES public.fsl_case_property(case_property_id) ON DELETE CASCADE;


--
-- TOC entry 4360 (class 2606 OID 1639322)
-- Name: charge_sheet_updates charge_sheet_updates_crime_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.charge_sheet_updates
    ADD CONSTRAINT charge_sheet_updates_crime_id_fkey FOREIGN KEY (crime_id) REFERENCES public.crimes(crime_id);


--
-- TOC entry 4356 (class 2606 OID 1422350)
-- Name: chargesheet_accused chargesheet_accused_chargesheet_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.chargesheet_accused
    ADD CONSTRAINT chargesheet_accused_chargesheet_id_fkey FOREIGN KEY (chargesheet_id) REFERENCES public.chargesheets(id);


--
-- TOC entry 4358 (class 2606 OID 1422369)
-- Name: chargesheet_acts chargesheet_acts_chargesheet_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.chargesheet_acts
    ADD CONSTRAINT chargesheet_acts_chargesheet_id_fkey FOREIGN KEY (chargesheet_id) REFERENCES public.chargesheets(id);


--
-- TOC entry 4354 (class 2606 OID 1422330)
-- Name: chargesheet_files chargesheet_files_chargesheet_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.chargesheet_files
    ADD CONSTRAINT chargesheet_files_chargesheet_id_fkey FOREIGN KEY (chargesheet_id) REFERENCES public.chargesheets(id);


--
-- TOC entry 4353 (class 2606 OID 1422319)
-- Name: chargesheets chargesheets_crime_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.chargesheets
    ADD CONSTRAINT chargesheets_crime_id_fkey FOREIGN KEY (crime_id) REFERENCES public.crimes(crime_id);


--
-- TOC entry 4326 (class 2606 OID 1397593)
-- Name: crimes crimes_ps_code_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.crimes
    ADD CONSTRAINT crimes_ps_code_fkey FOREIGN KEY (ps_code) REFERENCES public.hierarchy(ps_code) ON DELETE RESTRICT;


--
-- TOC entry 4349 (class 2606 OID 1420064)
-- Name: disposal disposal_crime_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.disposal
    ADD CONSTRAINT disposal_crime_id_fkey FOREIGN KEY (crime_id) REFERENCES public.crimes(crime_id);


--
-- TOC entry 4347 (class 2606 OID 1404637)
-- Name: brief_facts_accused fk_bf_accused_crime; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.brief_facts_accused
    ADD CONSTRAINT fk_bf_accused_crime FOREIGN KEY (crime_id) REFERENCES public.crimes(crime_id) ON DELETE CASCADE;


--
-- TOC entry 4357 (class 2606 OID 1422355)
-- Name: chargesheet_accused fk_chargesheet_accused_chargesheet; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.chargesheet_accused
    ADD CONSTRAINT fk_chargesheet_accused_chargesheet FOREIGN KEY (chargesheet_id) REFERENCES public.chargesheets(id) ON DELETE CASCADE;


--
-- TOC entry 4359 (class 2606 OID 1422374)
-- Name: chargesheet_acts fk_chargesheet_acts_chargesheet; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.chargesheet_acts
    ADD CONSTRAINT fk_chargesheet_acts_chargesheet FOREIGN KEY (chargesheet_id) REFERENCES public.chargesheets(id) ON DELETE CASCADE;


--
-- TOC entry 4355 (class 2606 OID 1422335)
-- Name: chargesheet_files fk_chargesheet_files_chargesheet; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.chargesheet_files
    ADD CONSTRAINT fk_chargesheet_files_chargesheet FOREIGN KEY (chargesheet_id) REFERENCES public.chargesheets(id) ON DELETE CASCADE;


--
-- TOC entry 4348 (class 2606 OID 1413505)
-- Name: old_interragation_report fk_crime; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.old_interragation_report
    ADD CONSTRAINT fk_crime FOREIGN KEY (crime_id) REFERENCES public.crimes(crime_id) ON DELETE CASCADE;


--
-- TOC entry 4346 (class 2606 OID 1404615)
-- Name: brief_facts_crime_summaries fk_summaries_crime; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.brief_facts_crime_summaries
    ADD CONSTRAINT fk_summaries_crime FOREIGN KEY (crime_id) REFERENCES public.crimes(crime_id) ON DELETE CASCADE;


--
-- TOC entry 4361 (class 2606 OID 1639527)
-- Name: fsl_case_property fsl_case_property_crime_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.fsl_case_property
    ADD CONSTRAINT fsl_case_property_crime_id_fkey FOREIGN KEY (crime_id) REFERENCES public.crimes(crime_id);


--
-- TOC entry 4362 (class 2606 OID 1639539)
-- Name: fsl_case_property_media fsl_case_property_media_case_property_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.fsl_case_property_media
    ADD CONSTRAINT fsl_case_property_media_case_property_id_fkey FOREIGN KEY (case_property_id) REFERENCES public.fsl_case_property(case_property_id) ON DELETE CASCADE;


--
-- TOC entry 4330 (class 2606 OID 1397648)
-- Name: interrogation_reports interrogation_reports_crime_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.interrogation_reports
    ADD CONSTRAINT interrogation_reports_crime_id_fkey FOREIGN KEY (crime_id) REFERENCES public.crimes(crime_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4341 (class 2606 OID 1397805)
-- Name: ir_associate_details ir_associate_details_ir_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_associate_details
    ADD CONSTRAINT ir_associate_details_ir_fkey FOREIGN KEY (interrogation_report_id) REFERENCES public.interrogation_reports(interrogation_report_id) ON DELETE CASCADE;


--
-- TOC entry 4337 (class 2606 OID 1397749)
-- Name: ir_consumer_details ir_consumer_details_ir_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_consumer_details
    ADD CONSTRAINT ir_consumer_details_ir_fkey FOREIGN KEY (interrogation_report_id) REFERENCES public.interrogation_reports(interrogation_report_id) ON DELETE CASCADE;


--
-- TOC entry 4372 (class 2606 OID 37992946)
-- Name: ir_conviction_acquittal ir_conviction_acquittal_interrogation_report_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_conviction_acquittal
    ADD CONSTRAINT ir_conviction_acquittal_interrogation_report_id_fkey FOREIGN KEY (interrogation_report_id) REFERENCES public.interrogation_reports(interrogation_report_id) ON DELETE CASCADE;


--
-- TOC entry 4340 (class 2606 OID 1397791)
-- Name: ir_defence_counsel ir_defence_counsel_ir_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_defence_counsel
    ADD CONSTRAINT ir_defence_counsel_ir_fkey FOREIGN KEY (interrogation_report_id) REFERENCES public.interrogation_reports(interrogation_report_id) ON DELETE CASCADE;


--
-- TOC entry 4345 (class 2606 OID 1397865)
-- Name: ir_dopams_links ir_dopams_links_ir_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_dopams_links
    ADD CONSTRAINT ir_dopams_links_ir_fkey FOREIGN KEY (interrogation_report_id) REFERENCES public.interrogation_reports(interrogation_report_id) ON DELETE CASCADE;


--
-- TOC entry 4367 (class 2606 OID 37992866)
-- Name: ir_execution_of_nbw ir_execution_of_nbw_interrogation_report_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_execution_of_nbw
    ADD CONSTRAINT ir_execution_of_nbw_interrogation_report_id_fkey FOREIGN KEY (interrogation_report_id) REFERENCES public.interrogation_reports(interrogation_report_id) ON DELETE CASCADE;


--
-- TOC entry 4331 (class 2606 OID 1397665)
-- Name: ir_family_history ir_family_history_ir_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_family_history
    ADD CONSTRAINT ir_family_history_ir_fkey FOREIGN KEY (interrogation_report_id) REFERENCES public.interrogation_reports(interrogation_report_id) ON DELETE CASCADE;


--
-- TOC entry 4336 (class 2606 OID 1397735)
-- Name: ir_financial_history ir_financial_history_ir_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_financial_history
    ADD CONSTRAINT ir_financial_history_ir_fkey FOREIGN KEY (interrogation_report_id) REFERENCES public.interrogation_reports(interrogation_report_id) ON DELETE CASCADE;


--
-- TOC entry 4364 (class 2606 OID 37992818)
-- Name: ir_indulgance_before_offence ir_indulgance_before_offence_interrogation_report_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_indulgance_before_offence
    ADD CONSTRAINT ir_indulgance_before_offence_interrogation_report_id_fkey FOREIGN KEY (interrogation_report_id) REFERENCES public.interrogation_reports(interrogation_report_id) ON DELETE CASCADE;


--
-- TOC entry 4344 (class 2606 OID 1397851)
-- Name: ir_interrogation_report_refs ir_interrogation_report_refs_ir_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_interrogation_report_refs
    ADD CONSTRAINT ir_interrogation_report_refs_ir_fkey FOREIGN KEY (interrogation_report_id) REFERENCES public.interrogation_reports(interrogation_report_id) ON DELETE CASCADE;


--
-- TOC entry 4370 (class 2606 OID 37992914)
-- Name: ir_jail_sentence ir_jail_sentence_interrogation_report_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_jail_sentence
    ADD CONSTRAINT ir_jail_sentence_interrogation_report_id_fkey FOREIGN KEY (interrogation_report_id) REFERENCES public.interrogation_reports(interrogation_report_id) ON DELETE CASCADE;


--
-- TOC entry 4332 (class 2606 OID 1397679)
-- Name: ir_local_contacts ir_local_contacts_ir_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_local_contacts
    ADD CONSTRAINT ir_local_contacts_ir_fkey FOREIGN KEY (interrogation_report_id) REFERENCES public.interrogation_reports(interrogation_report_id) ON DELETE CASCADE;


--
-- TOC entry 4343 (class 2606 OID 1397835)
-- Name: ir_media ir_media_ir_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_media
    ADD CONSTRAINT ir_media_ir_fkey FOREIGN KEY (interrogation_report_id) REFERENCES public.interrogation_reports(interrogation_report_id) ON DELETE CASCADE;


--
-- TOC entry 4338 (class 2606 OID 1397763)
-- Name: ir_modus_operandi ir_modus_operandi_ir_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_modus_operandi
    ADD CONSTRAINT ir_modus_operandi_ir_fkey FOREIGN KEY (interrogation_report_id) REFERENCES public.interrogation_reports(interrogation_report_id) ON DELETE CASCADE;


--
-- TOC entry 4371 (class 2606 OID 37992930)
-- Name: ir_new_gang_formation ir_new_gang_formation_interrogation_report_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_new_gang_formation
    ADD CONSTRAINT ir_new_gang_formation_interrogation_report_id_fkey FOREIGN KEY (interrogation_report_id) REFERENCES public.interrogation_reports(interrogation_report_id) ON DELETE CASCADE;


--
-- TOC entry 4368 (class 2606 OID 37992882)
-- Name: ir_pending_nbw ir_pending_nbw_interrogation_report_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_pending_nbw
    ADD CONSTRAINT ir_pending_nbw_interrogation_report_id_fkey FOREIGN KEY (interrogation_report_id) REFERENCES public.interrogation_reports(interrogation_report_id) ON DELETE CASCADE;


--
-- TOC entry 4339 (class 2606 OID 1397777)
-- Name: ir_previous_offences_confessed ir_previous_offences_ir_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_previous_offences_confessed
    ADD CONSTRAINT ir_previous_offences_ir_fkey FOREIGN KEY (interrogation_report_id) REFERENCES public.interrogation_reports(interrogation_report_id) ON DELETE CASCADE;


--
-- TOC entry 4365 (class 2606 OID 37992834)
-- Name: ir_property_disposal ir_property_disposal_interrogation_report_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_property_disposal
    ADD CONSTRAINT ir_property_disposal_interrogation_report_id_fkey FOREIGN KEY (interrogation_report_id) REFERENCES public.interrogation_reports(interrogation_report_id) ON DELETE CASCADE;


--
-- TOC entry 4333 (class 2606 OID 1397693)
-- Name: ir_regular_habits ir_regular_habits_ir_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_regular_habits
    ADD CONSTRAINT ir_regular_habits_ir_fkey FOREIGN KEY (interrogation_report_id) REFERENCES public.interrogation_reports(interrogation_report_id) ON DELETE CASCADE;


--
-- TOC entry 4366 (class 2606 OID 37992850)
-- Name: ir_regularization_transit_warrants ir_regularization_transit_warrants_interrogation_report_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_regularization_transit_warrants
    ADD CONSTRAINT ir_regularization_transit_warrants_interrogation_report_id_fkey FOREIGN KEY (interrogation_report_id) REFERENCES public.interrogation_reports(interrogation_report_id) ON DELETE CASCADE;


--
-- TOC entry 4342 (class 2606 OID 1397819)
-- Name: ir_shelter ir_shelter_ir_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_shelter
    ADD CONSTRAINT ir_shelter_ir_fkey FOREIGN KEY (interrogation_report_id) REFERENCES public.interrogation_reports(interrogation_report_id) ON DELETE CASCADE;


--
-- TOC entry 4335 (class 2606 OID 1397721)
-- Name: ir_sim_details ir_sim_details_ir_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_sim_details
    ADD CONSTRAINT ir_sim_details_ir_fkey FOREIGN KEY (interrogation_report_id) REFERENCES public.interrogation_reports(interrogation_report_id) ON DELETE CASCADE;


--
-- TOC entry 4369 (class 2606 OID 37992898)
-- Name: ir_sureties ir_sureties_interrogation_report_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_sureties
    ADD CONSTRAINT ir_sureties_interrogation_report_id_fkey FOREIGN KEY (interrogation_report_id) REFERENCES public.interrogation_reports(interrogation_report_id) ON DELETE CASCADE;


--
-- TOC entry 4334 (class 2606 OID 1397707)
-- Name: ir_types_of_drugs ir_types_of_drugs_ir_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_types_of_drugs
    ADD CONSTRAINT ir_types_of_drugs_ir_fkey FOREIGN KEY (interrogation_report_id) REFERENCES public.interrogation_reports(interrogation_report_id) ON DELETE CASCADE;


--
-- TOC entry 4373 (class 2606 OID 37993018)
-- Name: mo_seizure_media mo_seizure_media_mo_seizure_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.mo_seizure_media
    ADD CONSTRAINT mo_seizure_media_mo_seizure_id_fkey FOREIGN KEY (mo_seizure_id) REFERENCES public.mo_seizures(mo_seizure_id) ON DELETE CASCADE;


--
-- TOC entry 4352 (class 2606 OID 1420940)
-- Name: mo_seizures mo_seizures_crime_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.mo_seizures
    ADD CONSTRAINT mo_seizures_crime_id_fkey FOREIGN KEY (crime_id) REFERENCES public.crimes(crime_id);


--
-- TOC entry 4329 (class 2606 OID 1397629)
-- Name: properties properties_crime_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.properties
    ADD CONSTRAINT properties_crime_id_fkey FOREIGN KEY (crime_id) REFERENCES public.crimes(crime_id) ON DELETE CASCADE;


--
-- TOC entry 4374 (class 2606 OID 38052558)
-- Name: property_additional_details property_additional_details_property_fk; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.property_additional_details
    ADD CONSTRAINT property_additional_details_property_fk FOREIGN KEY (property_id) REFERENCES public.properties(property_id) ON DELETE CASCADE;


--
-- TOC entry 4375 (class 2606 OID 38052576)
-- Name: property_media property_media_property_fk; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.property_media
    ADD CONSTRAINT property_media_property_fk FOREIGN KEY (property_id) REFERENCES public.properties(property_id) ON DELETE CASCADE;


--
-- TOC entry 4538 (class 0 OID 0)
-- Dependencies: 8
-- Name: SCHEMA public; Type: ACL; Schema: -; Owner: pg_database_owner
--

GRANT ALL ON SCHEMA public TO dev_dopamas;


--
-- TOC entry 4548 (class 0 OID 0)
-- Dependencies: 267
-- Name: TABLE brief_facts_accused; Type: ACL; Schema: public; Owner: dev_dopamas
--

GRANT SELECT ON TABLE public.brief_facts_accused TO readonly_userdev;
GRANT SELECT ON TABLE public.brief_facts_accused TO dopamas_chat_ur;


--
-- TOC entry 2670 (class 826 OID 1397567)
-- Name: DEFAULT PRIVILEGES FOR SEQUENCES; Type: DEFAULT ACL; Schema: public; Owner: dopamasprd_ur
--

ALTER DEFAULT PRIVILEGES FOR ROLE dopamasprd_ur IN SCHEMA public GRANT ALL ON SEQUENCES TO dev_dopamas;


--
-- TOC entry 2669 (class 826 OID 1397566)
-- Name: DEFAULT PRIVILEGES FOR TABLES; Type: DEFAULT ACL; Schema: public; Owner: dopamasprd_ur
--

ALTER DEFAULT PRIVILEGES FOR ROLE dopamasprd_ur IN SCHEMA public GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO dev_dopamas;


-- Completed on 2026-04-10 14:58:11

--
-- PostgreSQL database dump complete
--

\unrestrict 0TnedpPqFal0ilgtgpIawQbMMHTYAC4cwxEIZ175qcLpRjNDDbvxKk2ujEvLxJw

