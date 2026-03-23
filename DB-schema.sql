--
-- PostgreSQL database dump
--

\restrict shGvuVMkHJiy4Un0LrsqGanbwhYgfMiYEacuiSphbthihbzVho3Ab9DISZUwdFE

-- Dumped from database version 16.11 (Ubuntu 16.11-1.pgdg24.04+1)
-- Dumped by pg_dump version 17.6

-- Started on 2026-03-23 10:50:39

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
-- TOC entry 4406 (class 0 OID 0)
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
-- TOC entry 4407 (class 0 OID 0)
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
-- TOC entry 4408 (class 0 OID 0)
-- Dependencies: 2
-- Name: EXTENSION vector; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION vector IS 'vector data type and ivfflat and hnsw access methods';


--
-- TOC entry 1186 (class 1247 OID 1412918)
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
-- TOC entry 1183 (class 1247 OID 1412908)
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
-- TOC entry 439 (class 1255 OID 1412952)
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
-- TOC entry 437 (class 1255 OID 1412950)
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
    
    -- NEW APIs
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
-- TOC entry 438 (class 1255 OID 1412951)
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
-- TOC entry 450 (class 1255 OID 1413845)
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
-- TOC entry 4409 (class 0 OID 0)
-- Dependencies: 450
-- Name: FUNCTION get_accused_crime_history(target_accused_id character varying); Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON FUNCTION public.get_accused_crime_history(target_accused_id character varying) IS 'Get complete crime history for an accused by accused_id, includes all cases across duplicate records';


--
-- TOC entry 451 (class 1255 OID 1413846)
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
-- TOC entry 4410 (class 0 OID 0)
-- Dependencies: 451
-- Name: FUNCTION get_person_crime_history(target_person_id character varying); Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON FUNCTION public.get_person_crime_history(target_person_id character varying) IS 'Get complete crime history for a person by person_id, shows all duplicate person records';


--
-- TOC entry 452 (class 1255 OID 1413847)
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
-- TOC entry 4411 (class 0 OID 0)
-- Dependencies: 452
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
-- TOC entry 4412 (class 0 OID 0)
-- Dependencies: 233
-- Name: TABLE accused; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.accused IS 'Links persons to crimes as accused with physical features';


--
-- TOC entry 4413 (class 0 OID 0)
-- Dependencies: 233
-- Name: COLUMN accused.person_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.accused.person_id IS 'Can be NULL - stub persons are created by ETL when needed';


--
-- TOC entry 4414 (class 0 OID 0)
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
-- TOC entry 4416 (class 0 OID 0)
-- Dependencies: 232
-- Name: TABLE crimes; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.crimes IS 'Crime/FIR records registered at police stations';


--
-- TOC entry 4417 (class 0 OID 0)
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
    disposal_type text NOT NULL,
    disposed_at timestamp with time zone,
    disposal text,
    case_status text,
    date_created timestamp with time zone NOT NULL,
    date_modified timestamp with time zone NOT NULL
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
-- TOC entry 4418 (class 0 OID 0)
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
-- TOC entry 4419 (class 0 OID 0)
-- Dependencies: 231
-- Name: TABLE persons; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.persons IS 'Personal details of individuals (accused, victims, witnesses, etc.)';


--
-- TOC entry 310 (class 1259 OID 30835373)
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
    ( SELECT count(*) AS count
           FROM public.accused a3
          WHERE ((a3.person_id)::text = (p.person_id)::text)) AS "noOfCrimes",
    ( SELECT jsonb_agg(DISTINCT jsonb_build_object('crimeId', c2.crime_id, 'firNumber', c2.fir_num)) AS jsonb_agg
           FROM (public.accused a4
             JOIN public.crimes c2 ON (((a4.crime_id)::text = (c2.crime_id)::text)))
          WHERE ((a4.person_id)::text = (p.person_id)::text)) AS "previouslyInvolvedCases",
    ( SELECT COALESCE(array_agg(DISTINCT upper(TRIM(BOTH FROM bfd.primary_drug_name))) FILTER (WHERE ((bfd.primary_drug_name IS NOT NULL) AND (bfd.primary_drug_name <> 'NO_DRUGS_DETECTED'::text))), ARRAY[]::text[]) AS "coalesce"
           FROM public.brief_facts_drug bfd
          WHERE ((bfd.crime_id)::text = (c.crime_id)::text)) AS "drugType",
    ( SELECT jsonb_agg(jsonb_build_object('name', bfd2.primary_drug_name, 'quantityKg', COALESCE(bfd2.weight_kg, (0)::numeric), 'quantityMl', COALESCE(bfd2.volume_ml, (0)::numeric), 'quantityCount', COALESCE(bfd2.count_total, (0)::numeric), 'worth', COALESCE(bfd2.seizure_worth, (0)::numeric)) ORDER BY bfd2.created_at) AS jsonb_agg
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
-- TOC entry 307 (class 1259 OID 30029975)
-- Name: advanced_search_accuseds_mv; Type: MATERIALIZED VIEW; Schema: public; Owner: dev_dopamas
--

CREATE MATERIALIZED VIEW public.advanced_search_accuseds_mv AS
 SELECT a.accused_id AS id,
    a.accused_code AS "accusedCode",
    a.type,
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
    a.type AS "accusedType",
    COALESCE(bfa.status, a.accused_status, 'Unknown'::text) AS "accusedStatus",
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
    ( SELECT jsonb_agg(jsonb_build_object('name', bfd.primary_drug_name, 'quantityKg', COALESCE(bfd.weight_kg, (0)::numeric), 'quantityMl', COALESCE(bfd.volume_ml, (0)::numeric), 'quantityCount', COALESCE(bfd.count_total, (0)::numeric), 'worth', COALESCE(bfd.seizure_worth, (0)::numeric)) ORDER BY bfd.created_at) AS jsonb_agg
           FROM public.brief_facts_drug bfd
          WHERE ((bfd.crime_id)::text = (c.crime_id)::text)) AS "drugDetails",
        CASE
            WHEN (c.fir_date IS NULL) THEN NULL::text
            WHEN (EXTRACT(day FROM (now() - (c.fir_date)::timestamp with time zone)) <= (30)::numeric) THEN '0 - 30 Days'::text
            WHEN (EXTRACT(day FROM (now() - (c.fir_date)::timestamp with time zone)) <= (60)::numeric) THEN '31 - 60 Days'::text
            WHEN (EXTRACT(day FROM (now() - (c.fir_date)::timestamp with time zone)) <= (90)::numeric) THEN '61 - 90 Days'::text
            ELSE 'More than 90 Days'::text
        END AS "stipulatedPeriodForCS"
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
-- TOC entry 308 (class 1259 OID 30054433)
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
    ( SELECT jsonb_agg(jsonb_build_object('name', bfd.primary_drug_name, 'quantityKg', COALESCE(bfd.weight_kg, (0)::numeric), 'quantityMl', COALESCE(bfd.volume_ml, (0)::numeric), 'quantityCount', COALESCE(bfd.count_total, (0)::numeric), 'worth', COALESCE(bfd.seizure_worth, (0)::numeric)) ORDER BY bfd.created_at) AS jsonb_agg
           FROM public.brief_facts_drug bfd
          WHERE ((bfd.crime_id)::text = (c.crime_id)::text)) AS "drugDetails",
        CASE
            WHEN (c.fir_date IS NULL) THEN NULL::text
            WHEN (EXTRACT(day FROM (now() - (c.fir_date)::timestamp with time zone)) <= (30)::numeric) THEN '0 - 30 Days'::text
            WHEN (EXTRACT(day FROM (now() - (c.fir_date)::timestamp with time zone)) <= (60)::numeric) THEN '31 - 60 Days'::text
            WHEN (EXTRACT(day FROM (now() - (c.fir_date)::timestamp with time zone)) <= (90)::numeric) THEN '61 - 90 Days'::text
            ELSE 'More than 90 Days'::text
        END AS "stipulatedPeriodForCS"
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
-- TOC entry 4420 (class 0 OID 0)
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
    date_created timestamp with time zone
);


ALTER TABLE public.charge_sheet_updates OWNER TO dev_dopamas;

--
-- TOC entry 4421 (class 0 OID 0)
-- Dependencies: 283
-- Name: TABLE charge_sheet_updates; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.charge_sheet_updates IS 'Stores charge sheet update records from DOPAMS API. Each record represents a charge sheet update with its status and court filing information.';


--
-- TOC entry 4422 (class 0 OID 0)
-- Dependencies: 283
-- Name: COLUMN charge_sheet_updates.update_charge_sheet_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.charge_sheet_updates.update_charge_sheet_id IS 'Unique identifier from the API (MongoDB ObjectId format) - REQUIRED';


--
-- TOC entry 4423 (class 0 OID 0)
-- Dependencies: 283
-- Name: COLUMN charge_sheet_updates.crime_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.charge_sheet_updates.crime_id IS 'Reference to the crime record (MongoDB ObjectId format) - REQUIRED, Foreign Key to crimes(crime_id)';


--
-- TOC entry 4424 (class 0 OID 0)
-- Dependencies: 283
-- Name: COLUMN charge_sheet_updates.charge_sheet_no; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.charge_sheet_updates.charge_sheet_no IS 'Charge sheet number (e.g., "146/2024") - NULLABLE';


--
-- TOC entry 4425 (class 0 OID 0)
-- Dependencies: 283
-- Name: COLUMN charge_sheet_updates.charge_sheet_date; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.charge_sheet_updates.charge_sheet_date IS 'Date when the charge sheet was created - NULLABLE';


--
-- TOC entry 4426 (class 0 OID 0)
-- Dependencies: 283
-- Name: COLUMN charge_sheet_updates.charge_sheet_status; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.charge_sheet_updates.charge_sheet_status IS 'Current status of the charge sheet (e.g., "Taken on File", "Filed/Check And Put Up") - NULLABLE';


--
-- TOC entry 4427 (class 0 OID 0)
-- Dependencies: 283
-- Name: COLUMN charge_sheet_updates.taken_on_file_date; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.charge_sheet_updates.taken_on_file_date IS 'Date when the charge sheet was taken on file by the court - NULLABLE';


--
-- TOC entry 4428 (class 0 OID 0)
-- Dependencies: 283
-- Name: COLUMN charge_sheet_updates.taken_on_file_case_type; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.charge_sheet_updates.taken_on_file_case_type IS 'Type of case (e.g., "SC", "CC", "NDPS", "SC NDPS") - NULLABLE';


--
-- TOC entry 4429 (class 0 OID 0)
-- Dependencies: 283
-- Name: COLUMN charge_sheet_updates.taken_on_file_court_case_no; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.charge_sheet_updates.taken_on_file_court_case_no IS 'Court case number assigned when taken on file - NULLABLE';


--
-- TOC entry 4430 (class 0 OID 0)
-- Dependencies: 283
-- Name: COLUMN charge_sheet_updates.date_created; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.charge_sheet_updates.date_created IS 'Timestamp when the record was created in the API system (from API response) - NULLABLE';


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
-- TOC entry 4431 (class 0 OID 0)
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
    created_at timestamp without time zone
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
    section character varying(50),
    rw_required boolean DEFAULT false,
    section_description text,
    grave_particulars text,
    created_at timestamp without time zone
);


ALTER TABLE public.chargesheet_acts OWNER TO dev_dopamas;

--
-- TOC entry 279 (class 1259 OID 1422324)
-- Name: chargesheet_files; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.chargesheet_files (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    chargesheet_id uuid NOT NULL,
    file_id character varying(100),
    created_at timestamp without time zone
);


ALTER TABLE public.chargesheet_files OWNER TO dev_dopamas;

--
-- TOC entry 278 (class 1259 OID 1422309)
-- Name: chargesheets; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.chargesheets (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    crime_id character varying(50) NOT NULL,
    chargesheet_no character varying(50),
    chargesheet_no_icjs character varying(50),
    chargesheet_date timestamp without time zone,
    chargesheet_type character varying(50),
    court_name text,
    is_ccl boolean DEFAULT false,
    is_esigned boolean DEFAULT false,
    date_created timestamp without time zone,
    date_modified timestamp without time zone
);


ALTER TABLE public.chargesheets OWNER TO dev_dopamas;

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
-- TOC entry 4432 (class 0 OID 0)
-- Dependencies: 269
-- Name: TABLE files; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.files IS 'Stores file references (UUIDs) from various sources (crimes, interrogations, properties, persons)';


--
-- TOC entry 4433 (class 0 OID 0)
-- Dependencies: 269
-- Name: COLUMN files.source_type; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.files.source_type IS 'Type of source: crime, interrogation, property, or person';


--
-- TOC entry 4434 (class 0 OID 0)
-- Dependencies: 269
-- Name: COLUMN files.source_field; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.files.source_field IS 'Field name from source: FIR_COPY, MEDIA, INTERROGATION_REPORT, DOPAMS_DATA, IDENTITY_DETAILS';


--
-- TOC entry 4435 (class 0 OID 0)
-- Dependencies: 269
-- Name: COLUMN files.parent_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.files.parent_id IS 'ID of the parent record (crime_id, interrogation_report_id, property_id, or person_id)';


--
-- TOC entry 4436 (class 0 OID 0)
-- Dependencies: 269
-- Name: COLUMN files.file_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.files.file_id IS 'The actual file UUID that can be used to fetch the file via API. NULL if field exists but has no file.';


--
-- TOC entry 4437 (class 0 OID 0)
-- Dependencies: 269
-- Name: COLUMN files.has_field; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.files.has_field IS 'TRUE if the field exists in API response, FALSE if field is missing';


--
-- TOC entry 4438 (class 0 OID 0)
-- Dependencies: 269
-- Name: COLUMN files.is_empty; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.files.is_empty IS 'TRUE if field exists but is null or empty array';


--
-- TOC entry 4439 (class 0 OID 0)
-- Dependencies: 269
-- Name: COLUMN files.file_path; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.files.file_path IS 'Relative file path on Tomcat server (auto-generated, NULL if file_id is NULL)';


--
-- TOC entry 4440 (class 0 OID 0)
-- Dependencies: 269
-- Name: COLUMN files.file_url; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.files.file_url IS 'Full file URL on Tomcat server (auto-generated, NULL if file_id is NULL)';


--
-- TOC entry 4441 (class 0 OID 0)
-- Dependencies: 269
-- Name: COLUMN files.file_index; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.files.file_index IS 'Index position in array (for MEDIA arrays with multiple files)';


--
-- TOC entry 4442 (class 0 OID 0)
-- Dependencies: 269
-- Name: COLUMN files.identity_type; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.files.identity_type IS 'For IDENTITY_DETAILS: type of identity document (Aadhar Card, Passport, etc.)';


--
-- TOC entry 4443 (class 0 OID 0)
-- Dependencies: 269
-- Name: COLUMN files.identity_number; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.files.identity_number IS 'For IDENTITY_DETAILS: identity document number';


--
-- TOC entry 4444 (class 0 OID 0)
-- Dependencies: 269
-- Name: COLUMN files.downloaded_at; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.files.downloaded_at IS 'Timestamp when file was successfully downloaded to media server';


--
-- TOC entry 4445 (class 0 OID 0)
-- Dependencies: 269
-- Name: COLUMN files.is_downloaded; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.files.is_downloaded IS 'Flag indicating if file has been successfully downloaded to media server';


--
-- TOC entry 4446 (class 0 OID 0)
-- Dependencies: 269
-- Name: COLUMN files.download_error; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.files.download_error IS 'Error message if file download failed';


--
-- TOC entry 4447 (class 0 OID 0)
-- Dependencies: 269
-- Name: COLUMN files.download_attempts; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.files.download_attempts IS 'Number of download attempts made';


--
-- TOC entry 4448 (class 0 OID 0)
-- Dependencies: 269
-- Name: COLUMN files.created_at; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.files.created_at IS 'Timestamp from API (DATE_CREATED or DATE_MODIFIED)';


--
-- TOC entry 309 (class 1259 OID 30060315)
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
    ( SELECT jsonb_agg(DISTINCT jsonb_build_object('id', f.id, 'identityType', f.identity_type, 'identityNumber', f.identity_number, 'filePath', f.file_path)) AS jsonb_agg
           FROM public.files f
          WHERE (((f.parent_id)::text = (p.person_id)::text) AND (f.source_type = 'person'::public.source_type_enum) AND (f.source_field = 'IDENTITY_DETAILS'::public.source_field_enum))) AS "identityDocuments",
    ( SELECT jsonb_agg(DISTINCT jsonb_build_object('id', f.id, 'filePath', f.file_path)) AS jsonb_agg
           FROM public.files f
          WHERE (((f.parent_id)::text = (p.person_id)::text) AND (f.source_type = 'person'::public.source_type_enum) AND (f.source_field = 'MEDIA'::public.source_field_enum))) AS documents,
    ( SELECT jsonb_agg(DISTINCT jsonb_build_object('id', c.crime_id, 'firNumber', c.fir_num, 'crimeRegDate', c.fir_date)) AS jsonb_agg
           FROM (public.accused a
             JOIN public.crimes c ON (((a.crime_id)::text = (c.crime_id)::text)))
          WHERE ((a.person_id)::text = (p.person_id)::text)) AS crimes,
    ( SELECT max((c.crime_id)::text) AS max
           FROM (public.accused a
             JOIN public.crimes c ON (((a.crime_id)::text = (c.crime_id)::text)))
          WHERE ((a.person_id)::text = (p.person_id)::text)) AS "latestCrimeId",
    ( SELECT max((c.fir_num)::text) AS max
           FROM (public.accused a
             JOIN public.crimes c ON (((a.crime_id)::text = (c.crime_id)::text)))
          WHERE ((a.person_id)::text = (p.person_id)::text)) AS "latestCrimeNo",
    ( SELECT count(DISTINCT a.crime_id) AS count
           FROM public.accused a
          WHERE ((a.person_id)::text = (p.person_id)::text)) AS "noOfCrimes",
    ( SELECT count(*) AS count
           FROM public.arrests arr
          WHERE (((arr.person_id)::text = (p.person_id)::text) AND (arr.is_arrested = true))) AS "arrestCount",
    ( SELECT jsonb_agg(DISTINCT jsonb_build_object('crimeId', c.crime_id, 'firNumber', c.fir_num)) AS jsonb_agg
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
    NULL::text AS "bankAcountDetails",
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
-- TOC entry 4449 (class 0 OID 0)
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
-- TOC entry 4450 (class 0 OID 0)
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
-- TOC entry 4451 (class 0 OID 0)
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
-- TOC entry 4452 (class 0 OID 0)
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
-- TOC entry 4453 (class 0 OID 0)
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
    report_received boolean DEFAULT false,
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
    property_received_back boolean DEFAULT false
);


ALTER TABLE public.fsl_case_property OWNER TO dev_dopamas;

--
-- TOC entry 4454 (class 0 OID 0)
-- Dependencies: 284
-- Name: TABLE fsl_case_property; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.fsl_case_property IS 'Main table storing case property records from DOPAMS API';


--
-- TOC entry 4455 (class 0 OID 0)
-- Dependencies: 284
-- Name: COLUMN fsl_case_property.case_property_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.fsl_case_property.case_property_id IS 'Primary key from API (CASE_PROPERTY_ID) - MongoDB ObjectId (24 hex characters)';


--
-- TOC entry 4456 (class 0 OID 0)
-- Dependencies: 284
-- Name: COLUMN fsl_case_property.crime_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.fsl_case_property.crime_id IS 'Reference to crime/case (CRIME_ID) - Foreign key to crimes table';


--
-- TOC entry 4457 (class 0 OID 0)
-- Dependencies: 284
-- Name: COLUMN fsl_case_property.mo_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.fsl_case_property.mo_id IS 'Material Object ID (MO_ID)';


--
-- TOC entry 4458 (class 0 OID 0)
-- Dependencies: 284
-- Name: COLUMN fsl_case_property.status; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.fsl_case_property.status IS 'Current status (e.g., Send To FSL, Send To Court)';


--
-- TOC entry 4459 (class 0 OID 0)
-- Dependencies: 284
-- Name: COLUMN fsl_case_property.date_created; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.fsl_case_property.date_created IS 'Record creation timestamp from API (DATE_CREATED)';


--
-- TOC entry 4460 (class 0 OID 0)
-- Dependencies: 284
-- Name: COLUMN fsl_case_property.date_modified; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.fsl_case_property.date_modified IS 'Record modification timestamp from API (DATE_MODIFIED)';


--
-- TOC entry 4461 (class 0 OID 0)
-- Dependencies: 284
-- Name: COLUMN fsl_case_property.fsl_no; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.fsl_case_property.fsl_no IS 'FSL case number';


--
-- TOC entry 4462 (class 0 OID 0)
-- Dependencies: 284
-- Name: COLUMN fsl_case_property.report_received; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.fsl_case_property.report_received IS 'Whether FSL report has been received';


--
-- TOC entry 4463 (class 0 OID 0)
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
    is_in_jail boolean DEFAULT false,
    from_where_sent_in_jail text,
    in_jail_crime_num character varying(255),
    in_jail_dist_unit character varying(255),
    is_on_bail boolean DEFAULT false,
    from_where_sent_on_bail text,
    on_bail_crime_num character varying(255),
    date_of_bail date,
    is_absconding boolean DEFAULT false,
    wanted_in_police_station character varying(255),
    absconding_crime_num character varying(255),
    is_normal_life boolean DEFAULT false,
    eking_livelihood_by_labor_work text,
    is_rehabilitated boolean DEFAULT false,
    rehabilitation_details text,
    is_dead boolean DEFAULT false,
    death_details text,
    is_facing_trial boolean DEFAULT false,
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
-- TOC entry 4464 (class 0 OID 0)
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
-- TOC entry 4465 (class 0 OID 0)
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
-- TOC entry 4466 (class 0 OID 0)
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
-- TOC entry 4467 (class 0 OID 0)
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
-- TOC entry 4468 (class 0 OID 0)
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
-- TOC entry 4469 (class 0 OID 0)
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
-- TOC entry 4470 (class 0 OID 0)
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
-- TOC entry 4471 (class 0 OID 0)
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
-- TOC entry 4472 (class 0 OID 0)
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
    remarks text
);


ALTER TABLE public.ir_previous_offences_confessed OWNER TO dev_dopamas;

--
-- TOC entry 4473 (class 0 OID 0)
-- Dependencies: 253
-- Name: TABLE ir_previous_offences_confessed; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.ir_previous_offences_confessed IS 'Previous offences confessed for each IR record. One record per offence.';


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
-- TOC entry 4474 (class 0 OID 0)
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
-- TOC entry 4475 (class 0 OID 0)
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
-- TOC entry 4476 (class 0 OID 0)
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
-- TOC entry 4477 (class 0 OID 0)
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
    pos_latitude text,
    pos_longitude text,
    mo_media_url text,
    mo_media_name text,
    mo_media_file_id text,
    date_created timestamp with time zone,
    date_modified timestamp with time zone
);


ALTER TABLE public.mo_seizures OWNER TO dev_dopamas;

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
    date_of_seizure timestamp without time zone,
    nature character varying(255),
    belongs character varying(100),
    estimate_value numeric(15,2) DEFAULT 0,
    recovered_value numeric(15,2) DEFAULT 0,
    particular_of_property text,
    category character varying(100),
    additional_details jsonb,
    media jsonb DEFAULT '[]'::jsonb,
    date_created timestamp without time zone,
    date_modified timestamp without time zone
);


ALTER TABLE public.properties OWNER TO dev_dopamas;

--
-- TOC entry 4478 (class 0 OID 0)
-- Dependencies: 234
-- Name: TABLE properties; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.properties IS 'Seized and recovered property details linked to crimes';


--
-- TOC entry 4479 (class 0 OID 0)
-- Dependencies: 234
-- Name: COLUMN properties.case_property_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.properties.case_property_id IS 'Reference to related case property (may be null)';


--
-- TOC entry 4480 (class 0 OID 0)
-- Dependencies: 234
-- Name: COLUMN properties.additional_details; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.properties.additional_details IS 'JSONB field containing flexible additional data (drug details, vehicle info, etc.)';


--
-- TOC entry 4481 (class 0 OID 0)
-- Dependencies: 234
-- Name: COLUMN properties.media; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.properties.media IS 'JSONB array of media attachments';


--
-- TOC entry 311 (class 1259 OID 32629186)
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
            WHEN (EXTRACT(day FROM (now() - (c.fir_date)::timestamp with time zone)) <= (30)::numeric) THEN '0 - 30 Days'::text
            WHEN (EXTRACT(day FROM (now() - (c.fir_date)::timestamp with time zone)) <= (60)::numeric) THEN '31 - 60 Days'::text
            WHEN (EXTRACT(day FROM (now() - (c.fir_date)::timestamp with time zone)) <= (90)::numeric) THEN '61 - 90 Days'::text
            ELSE 'More than 90 Days'::text
        END AS "stipulatedPeriodForCS",
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
                            WHEN (sum(bfd2.weight_kg) > (0)::numeric) THEN concat(sum(bfd2.weight_kg), ' Kg')
                            WHEN (sum(bfd2.volume_l) > (0)::numeric) THEN concat(sum(bfd2.volume_l), ' L')
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
    ( SELECT jsonb_agg(DISTINCT jsonb_build_object('id', f.id, 'filePath', f.file_path, 'fileUrl', f.file_url, 'type', f.source_field, 'name', f.notes)) AS jsonb_agg
           FROM public.files f
          WHERE (((f.parent_id)::text = (c.crime_id)::text) AND (f.source_type = 'crime'::public.source_type_enum) AND (f.source_field = 'FIR_COPY'::public.source_field_enum))) AS documents,
    ( SELECT jsonb_agg(DISTINCT jsonb_build_object('id', f.id, 'filePath', f.file_path, 'fileUrl', f.file_url, 'type', f.source_field, 'name', f.notes)) AS jsonb_agg
           FROM (public.files f
             JOIN public.properties prop ON (((prop.property_id)::text = (f.parent_id)::text)))
          WHERE (((prop.crime_id)::text = (c.crime_id)::text) AND (f.source_type = 'property'::public.source_type_enum) AND (f.source_field = 'MEDIA'::public.source_field_enum))) AS "propertyDocuments",
    ( SELECT jsonb_agg(DISTINCT jsonb_build_object('id', f.id, 'filePath', f.file_path, 'fileUrl', f.file_url, 'type', f.source_field, 'name', f.notes)) AS jsonb_agg
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
-- TOC entry 4482 (class 0 OID 0)
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
-- TOC entry 4483 (class 0 OID 0)
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
-- TOC entry 4484 (class 0 OID 0)
-- Dependencies: 256
-- Name: ir_associate_details_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.ir_associate_details_id_seq OWNED BY public.ir_associate_details.id;


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
-- TOC entry 4485 (class 0 OID 0)
-- Dependencies: 248
-- Name: ir_consumer_details_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.ir_consumer_details_id_seq OWNED BY public.ir_consumer_details.id;


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
-- TOC entry 4486 (class 0 OID 0)
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
-- TOC entry 4487 (class 0 OID 0)
-- Dependencies: 264
-- Name: ir_dopams_links_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.ir_dopams_links_id_seq OWNED BY public.ir_dopams_links.id;


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
-- TOC entry 4488 (class 0 OID 0)
-- Dependencies: 236
-- Name: ir_family_history_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.ir_family_history_id_seq OWNED BY public.ir_family_history.id;


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
-- TOC entry 4489 (class 0 OID 0)
-- Dependencies: 246
-- Name: ir_financial_history_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.ir_financial_history_id_seq OWNED BY public.ir_financial_history.id;


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
-- TOC entry 4490 (class 0 OID 0)
-- Dependencies: 263
-- Name: TABLE ir_interrogation_report_refs; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.ir_interrogation_report_refs IS 'Interrogation report references (UUIDs) for each IR record. One record per reference.';


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
-- TOC entry 4491 (class 0 OID 0)
-- Dependencies: 262
-- Name: ir_interrogation_report_refs_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.ir_interrogation_report_refs_id_seq OWNED BY public.ir_interrogation_report_refs.id;


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
-- TOC entry 4492 (class 0 OID 0)
-- Dependencies: 238
-- Name: ir_local_contacts_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.ir_local_contacts_id_seq OWNED BY public.ir_local_contacts.id;


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
-- TOC entry 4493 (class 0 OID 0)
-- Dependencies: 261
-- Name: TABLE ir_media; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.ir_media IS 'Media references (UUIDs) for each IR record. One record per media reference.';


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
-- TOC entry 4494 (class 0 OID 0)
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
-- TOC entry 4495 (class 0 OID 0)
-- Dependencies: 250
-- Name: ir_modus_operandi_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.ir_modus_operandi_id_seq OWNED BY public.ir_modus_operandi.id;


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
-- TOC entry 4496 (class 0 OID 0)
-- Dependencies: 305
-- Name: ir_pending_fk_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.ir_pending_fk_id_seq OWNED BY public.ir_pending_fk.id;


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
-- TOC entry 4497 (class 0 OID 0)
-- Dependencies: 252
-- Name: ir_previous_offences_confessed_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.ir_previous_offences_confessed_id_seq OWNED BY public.ir_previous_offences_confessed.id;


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
-- TOC entry 4498 (class 0 OID 0)
-- Dependencies: 240
-- Name: ir_regular_habits_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.ir_regular_habits_id_seq OWNED BY public.ir_regular_habits.id;


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
-- TOC entry 4499 (class 0 OID 0)
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
-- TOC entry 4500 (class 0 OID 0)
-- Dependencies: 244
-- Name: ir_sim_details_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.ir_sim_details_id_seq OWNED BY public.ir_sim_details.id;


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
-- TOC entry 4501 (class 0 OID 0)
-- Dependencies: 242
-- Name: ir_types_of_drugs_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.ir_types_of_drugs_id_seq OWNED BY public.ir_types_of_drugs.id;


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
-- TOC entry 4502 (class 0 OID 0)
-- Dependencies: 271
-- Name: TABLE old_interragation_report; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.old_interragation_report IS 'Interrogation report with family and social relations information';


--
-- TOC entry 4503 (class 0 OID 0)
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
-- TOC entry 4504 (class 0 OID 0)
-- Dependencies: 273
-- Name: TABLE person_deduplication_tracker; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.person_deduplication_tracker IS 'Tracks unique persons across multiple crimes using hierarchical fingerprinting strategies';


--
-- TOC entry 4505 (class 0 OID 0)
-- Dependencies: 273
-- Name: COLUMN person_deduplication_tracker.person_fingerprint; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.person_deduplication_tracker.person_fingerprint IS 'MD5 hash combining person identifying fields based on matching strategy';


--
-- TOC entry 4506 (class 0 OID 0)
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
-- TOC entry 4507 (class 0 OID 0)
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
-- TOC entry 4508 (class 0 OID 0)
-- Dependencies: 303
-- Name: properties_pending_fk_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.properties_pending_fk_id_seq OWNED BY public.properties_pending_fk.id;


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
-- TOC entry 3888 (class 2604 OID 2028692)
-- Name: agent_deduplication_tracker id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.agent_deduplication_tracker ALTER COLUMN id SET DEFAULT nextval('public.agent_deduplication_tracker_id_seq'::regclass);


--
-- TOC entry 3885 (class 2604 OID 1639317)
-- Name: charge_sheet_updates id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.charge_sheet_updates ALTER COLUMN id SET DEFAULT nextval('public.charge_sheet_updates_id_seq'::regclass);


--
-- TOC entry 3892 (class 2604 OID 2028693)
-- Name: dedup_cluster_state id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.dedup_cluster_state ALTER COLUMN id SET DEFAULT nextval('public.dedup_cluster_state_id_seq'::regclass);


--
-- TOC entry 3895 (class 2604 OID 2028694)
-- Name: dedup_comparison_progress id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.dedup_comparison_progress ALTER COLUMN id SET DEFAULT nextval('public.dedup_comparison_progress_id_seq'::regclass);


--
-- TOC entry 3898 (class 2604 OID 2028695)
-- Name: dedup_run_metadata id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.dedup_run_metadata ALTER COLUMN id SET DEFAULT nextval('public.dedup_run_metadata_id_seq'::regclass);


--
-- TOC entry 3903 (class 2604 OID 20996615)
-- Name: drug_categories id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.drug_categories ALTER COLUMN id SET DEFAULT nextval('public.drug_categories_id_seq'::regclass);


--
-- TOC entry 3906 (class 2604 OID 20996629)
-- Name: drug_ignore_list id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.drug_ignore_list ALTER COLUMN id SET DEFAULT nextval('public.drug_ignore_list_id_seq'::regclass);


--
-- TOC entry 3913 (class 2604 OID 23469815)
-- Name: geo_reference id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.geo_reference ALTER COLUMN id SET DEFAULT nextval('public.geo_reference_id_seq'::regclass);


--
-- TOC entry 3847 (class 2604 OID 1397800)
-- Name: ir_associate_details id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_associate_details ALTER COLUMN id SET DEFAULT nextval('public.ir_associate_details_id_seq'::regclass);


--
-- TOC entry 3843 (class 2604 OID 1397744)
-- Name: ir_consumer_details id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_consumer_details ALTER COLUMN id SET DEFAULT nextval('public.ir_consumer_details_id_seq'::regclass);


--
-- TOC entry 3846 (class 2604 OID 1397786)
-- Name: ir_defence_counsel id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_defence_counsel ALTER COLUMN id SET DEFAULT nextval('public.ir_defence_counsel_id_seq'::regclass);


--
-- TOC entry 3851 (class 2604 OID 1397860)
-- Name: ir_dopams_links id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_dopams_links ALTER COLUMN id SET DEFAULT nextval('public.ir_dopams_links_id_seq'::regclass);


--
-- TOC entry 3834 (class 2604 OID 1397657)
-- Name: ir_family_history id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_family_history ALTER COLUMN id SET DEFAULT nextval('public.ir_family_history_id_seq'::regclass);


--
-- TOC entry 3842 (class 2604 OID 1397730)
-- Name: ir_financial_history id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_financial_history ALTER COLUMN id SET DEFAULT nextval('public.ir_financial_history_id_seq'::regclass);


--
-- TOC entry 3850 (class 2604 OID 1397844)
-- Name: ir_interrogation_report_refs id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_interrogation_report_refs ALTER COLUMN id SET DEFAULT nextval('public.ir_interrogation_report_refs_id_seq'::regclass);


--
-- TOC entry 3838 (class 2604 OID 1397674)
-- Name: ir_local_contacts id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_local_contacts ALTER COLUMN id SET DEFAULT nextval('public.ir_local_contacts_id_seq'::regclass);


--
-- TOC entry 3849 (class 2604 OID 1397828)
-- Name: ir_media id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_media ALTER COLUMN id SET DEFAULT nextval('public.ir_media_id_seq'::regclass);


--
-- TOC entry 3844 (class 2604 OID 1397758)
-- Name: ir_modus_operandi id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_modus_operandi ALTER COLUMN id SET DEFAULT nextval('public.ir_modus_operandi_id_seq'::regclass);


--
-- TOC entry 3919 (class 2604 OID 25200404)
-- Name: ir_pending_fk id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_pending_fk ALTER COLUMN id SET DEFAULT nextval('public.ir_pending_fk_id_seq'::regclass);


--
-- TOC entry 3845 (class 2604 OID 1397772)
-- Name: ir_previous_offences_confessed id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_previous_offences_confessed ALTER COLUMN id SET DEFAULT nextval('public.ir_previous_offences_confessed_id_seq'::regclass);


--
-- TOC entry 3839 (class 2604 OID 1397688)
-- Name: ir_regular_habits id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_regular_habits ALTER COLUMN id SET DEFAULT nextval('public.ir_regular_habits_id_seq'::regclass);


--
-- TOC entry 3848 (class 2604 OID 1397814)
-- Name: ir_shelter id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_shelter ALTER COLUMN id SET DEFAULT nextval('public.ir_shelter_id_seq'::regclass);


--
-- TOC entry 3841 (class 2604 OID 1397716)
-- Name: ir_sim_details id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_sim_details ALTER COLUMN id SET DEFAULT nextval('public.ir_sim_details_id_seq'::regclass);


--
-- TOC entry 3840 (class 2604 OID 1397702)
-- Name: ir_types_of_drugs id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_types_of_drugs ALTER COLUMN id SET DEFAULT nextval('public.ir_types_of_drugs_id_seq'::regclass);


--
-- TOC entry 3868 (class 2604 OID 1413818)
-- Name: person_deduplication_tracker id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.person_deduplication_tracker ALTER COLUMN id SET DEFAULT nextval('public.person_deduplication_tracker_id_seq'::regclass);


--
-- TOC entry 3915 (class 2604 OID 25200391)
-- Name: properties_pending_fk id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.properties_pending_fk ALTER COLUMN id SET DEFAULT nextval('public.properties_pending_fk_id_seq'::regclass);


--
-- TOC entry 3950 (class 2606 OID 1397606)
-- Name: accused accused_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.accused
    ADD CONSTRAINT accused_pkey PRIMARY KEY (accused_id);


--
-- TOC entry 3952 (class 2606 OID 1397608)
-- Name: accused accused_seq_num_key; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.accused
    ADD CONSTRAINT accused_seq_num_key UNIQUE (seq_num);


--
-- TOC entry 4090 (class 2606 OID 2028697)
-- Name: agent_deduplication_tracker agent_deduplication_tracker_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.agent_deduplication_tracker
    ADD CONSTRAINT agent_deduplication_tracker_pkey PRIMARY KEY (id);


--
-- TOC entry 4056 (class 2606 OID 1420503)
-- Name: arrests arrests_crime_id_accused_seq_no_key; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.arrests
    ADD CONSTRAINT arrests_crime_id_accused_seq_no_key UNIQUE (crime_id, accused_seq_no);


--
-- TOC entry 4058 (class 2606 OID 1420501)
-- Name: arrests arrests_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.arrests
    ADD CONSTRAINT arrests_pkey PRIMARY KEY (id);


--
-- TOC entry 4023 (class 2606 OID 1404629)
-- Name: brief_facts_accused brief_facts_accused_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.brief_facts_accused
    ADD CONSTRAINT brief_facts_accused_pkey PRIMARY KEY (bf_accused_id);


--
-- TOC entry 4021 (class 2606 OID 1404612)
-- Name: brief_facts_crime_summaries brief_facts_crime_summaries_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.brief_facts_crime_summaries
    ADD CONSTRAINT brief_facts_crime_summaries_pkey PRIMARY KEY (crime_id);


--
-- TOC entry 4118 (class 2606 OID 22014304)
-- Name: brief_facts_drug brief_facts_drug_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.brief_facts_drug
    ADD CONSTRAINT brief_facts_drug_pkey PRIMARY KEY (id);


--
-- TOC entry 4078 (class 2606 OID 1639319)
-- Name: charge_sheet_updates charge_sheet_updates_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.charge_sheet_updates
    ADD CONSTRAINT charge_sheet_updates_pkey PRIMARY KEY (id);


--
-- TOC entry 4080 (class 2606 OID 1639321)
-- Name: charge_sheet_updates charge_sheet_updates_update_charge_sheet_id_key; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.charge_sheet_updates
    ADD CONSTRAINT charge_sheet_updates_update_charge_sheet_id_key UNIQUE (update_charge_sheet_id);


--
-- TOC entry 4072 (class 2606 OID 1422349)
-- Name: chargesheet_accused chargesheet_accused_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.chargesheet_accused
    ADD CONSTRAINT chargesheet_accused_pkey PRIMARY KEY (id);


--
-- TOC entry 4076 (class 2606 OID 1422368)
-- Name: chargesheet_acts chargesheet_acts_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.chargesheet_acts
    ADD CONSTRAINT chargesheet_acts_pkey PRIMARY KEY (id);


--
-- TOC entry 4070 (class 2606 OID 1422329)
-- Name: chargesheet_files chargesheet_files_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.chargesheet_files
    ADD CONSTRAINT chargesheet_files_pkey PRIMARY KEY (id);


--
-- TOC entry 4066 (class 2606 OID 1422318)
-- Name: chargesheets chargesheets_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.chargesheets
    ADD CONSTRAINT chargesheets_pkey PRIMARY KEY (id);


--
-- TOC entry 3940 (class 2606 OID 1397592)
-- Name: crimes crimes_fir_reg_num_key; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.crimes
    ADD CONSTRAINT crimes_fir_reg_num_key UNIQUE (fir_reg_num);


--
-- TOC entry 3942 (class 2606 OID 1397590)
-- Name: crimes crimes_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.crimes
    ADD CONSTRAINT crimes_pkey PRIMARY KEY (crime_id);


--
-- TOC entry 4094 (class 2606 OID 2028699)
-- Name: dedup_cluster_state dedup_cluster_state_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.dedup_cluster_state
    ADD CONSTRAINT dedup_cluster_state_pkey PRIMARY KEY (id);


--
-- TOC entry 4098 (class 2606 OID 2028701)
-- Name: dedup_comparison_progress dedup_comparison_progress_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.dedup_comparison_progress
    ADD CONSTRAINT dedup_comparison_progress_pkey PRIMARY KEY (id);


--
-- TOC entry 4101 (class 2606 OID 2028710)
-- Name: dedup_run_metadata dedup_run_metadata_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.dedup_run_metadata
    ADD CONSTRAINT dedup_run_metadata_pkey PRIMARY KEY (id);


--
-- TOC entry 4103 (class 2606 OID 2028712)
-- Name: dedup_run_metadata dedup_run_metadata_run_id_key; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.dedup_run_metadata
    ADD CONSTRAINT dedup_run_metadata_run_id_key UNIQUE (run_id);


--
-- TOC entry 4051 (class 2606 OID 1420063)
-- Name: disposal disposal_crime_id_disposal_type_disposed_at_key; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.disposal
    ADD CONSTRAINT disposal_crime_id_disposal_type_disposed_at_key UNIQUE (crime_id, disposal_type, disposed_at);


--
-- TOC entry 4053 (class 2606 OID 1420061)
-- Name: disposal disposal_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.disposal
    ADD CONSTRAINT disposal_pkey PRIMARY KEY (id);


--
-- TOC entry 4105 (class 2606 OID 20996621)
-- Name: drug_categories drug_categories_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.drug_categories
    ADD CONSTRAINT drug_categories_pkey PRIMARY KEY (id);


--
-- TOC entry 4107 (class 2606 OID 20996623)
-- Name: drug_categories drug_categories_raw_name_key; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.drug_categories
    ADD CONSTRAINT drug_categories_raw_name_key UNIQUE (raw_name);


--
-- TOC entry 4112 (class 2606 OID 20996634)
-- Name: drug_ignore_list drug_ignore_list_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.drug_ignore_list
    ADD CONSTRAINT drug_ignore_list_pkey PRIMARY KEY (id);


--
-- TOC entry 4114 (class 2606 OID 20996636)
-- Name: drug_ignore_list drug_ignore_list_term_key; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.drug_ignore_list
    ADD CONSTRAINT drug_ignore_list_term_key UNIQUE (term);


--
-- TOC entry 4035 (class 2606 OID 1412938)
-- Name: files files_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.files
    ADD CONSTRAINT files_pkey PRIMARY KEY (id);


--
-- TOC entry 4087 (class 2606 OID 1639538)
-- Name: fsl_case_property_media fsl_case_property_media_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.fsl_case_property_media
    ADD CONSTRAINT fsl_case_property_media_pkey PRIMARY KEY (media_id);


--
-- TOC entry 4084 (class 2606 OID 1639526)
-- Name: fsl_case_property fsl_case_property_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.fsl_case_property
    ADD CONSTRAINT fsl_case_property_pkey PRIMARY KEY (case_property_id);


--
-- TOC entry 4127 (class 2606 OID 23469820)
-- Name: geo_reference geo_reference_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.geo_reference
    ADD CONSTRAINT geo_reference_pkey PRIMARY KEY (id);


--
-- TOC entry 3928 (class 2606 OID 1397575)
-- Name: hierarchy hierarchy_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.hierarchy
    ADD CONSTRAINT hierarchy_pkey PRIMARY KEY (ps_code);


--
-- TOC entry 3968 (class 2606 OID 1397647)
-- Name: interrogation_reports interrogation_reports_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.interrogation_reports
    ADD CONSTRAINT interrogation_reports_pkey PRIMARY KEY (interrogation_report_id);


--
-- TOC entry 4004 (class 2606 OID 1397804)
-- Name: ir_associate_details ir_associate_details_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_associate_details
    ADD CONSTRAINT ir_associate_details_pkey PRIMARY KEY (id);


--
-- TOC entry 3992 (class 2606 OID 1397748)
-- Name: ir_consumer_details ir_consumer_details_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_consumer_details
    ADD CONSTRAINT ir_consumer_details_pkey PRIMARY KEY (id);


--
-- TOC entry 4001 (class 2606 OID 1397790)
-- Name: ir_defence_counsel ir_defence_counsel_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_defence_counsel
    ADD CONSTRAINT ir_defence_counsel_pkey PRIMARY KEY (id);


--
-- TOC entry 4019 (class 2606 OID 1397864)
-- Name: ir_dopams_links ir_dopams_links_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_dopams_links
    ADD CONSTRAINT ir_dopams_links_pkey PRIMARY KEY (id);


--
-- TOC entry 3972 (class 2606 OID 1397664)
-- Name: ir_family_history ir_family_history_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_family_history
    ADD CONSTRAINT ir_family_history_pkey PRIMARY KEY (id);


--
-- TOC entry 3989 (class 2606 OID 1397734)
-- Name: ir_financial_history ir_financial_history_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_financial_history
    ADD CONSTRAINT ir_financial_history_pkey PRIMARY KEY (id);


--
-- TOC entry 4014 (class 2606 OID 1397848)
-- Name: ir_interrogation_report_refs ir_interrogation_report_refs_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_interrogation_report_refs
    ADD CONSTRAINT ir_interrogation_report_refs_pkey PRIMARY KEY (id);


--
-- TOC entry 4016 (class 2606 OID 1397935)
-- Name: ir_interrogation_report_refs ir_interrogation_report_refs_unique; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_interrogation_report_refs
    ADD CONSTRAINT ir_interrogation_report_refs_unique UNIQUE (interrogation_report_id, report_ref_id);


--
-- TOC entry 3975 (class 2606 OID 1397678)
-- Name: ir_local_contacts ir_local_contacts_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_local_contacts
    ADD CONSTRAINT ir_local_contacts_pkey PRIMARY KEY (id);


--
-- TOC entry 4010 (class 2606 OID 1397832)
-- Name: ir_media ir_media_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_media
    ADD CONSTRAINT ir_media_pkey PRIMARY KEY (id);


--
-- TOC entry 4012 (class 2606 OID 1397933)
-- Name: ir_media ir_media_unique; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_media
    ADD CONSTRAINT ir_media_unique UNIQUE (interrogation_report_id, media_id);


--
-- TOC entry 3995 (class 2606 OID 1397762)
-- Name: ir_modus_operandi ir_modus_operandi_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_modus_operandi
    ADD CONSTRAINT ir_modus_operandi_pkey PRIMARY KEY (id);


--
-- TOC entry 4139 (class 2606 OID 25200411)
-- Name: ir_pending_fk ir_pending_fk_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_pending_fk
    ADD CONSTRAINT ir_pending_fk_pkey PRIMARY KEY (id);


--
-- TOC entry 3998 (class 2606 OID 1397776)
-- Name: ir_previous_offences_confessed ir_previous_offences_confessed_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_previous_offences_confessed
    ADD CONSTRAINT ir_previous_offences_confessed_pkey PRIMARY KEY (id);


--
-- TOC entry 3978 (class 2606 OID 1397690)
-- Name: ir_regular_habits ir_regular_habits_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_regular_habits
    ADD CONSTRAINT ir_regular_habits_pkey PRIMARY KEY (id);


--
-- TOC entry 3980 (class 2606 OID 1397692)
-- Name: ir_regular_habits ir_regular_habits_unique; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_regular_habits
    ADD CONSTRAINT ir_regular_habits_unique UNIQUE (interrogation_report_id, habit);


--
-- TOC entry 4007 (class 2606 OID 1397818)
-- Name: ir_shelter ir_shelter_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_shelter
    ADD CONSTRAINT ir_shelter_pkey PRIMARY KEY (id);


--
-- TOC entry 3986 (class 2606 OID 1397720)
-- Name: ir_sim_details ir_sim_details_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_sim_details
    ADD CONSTRAINT ir_sim_details_pkey PRIMARY KEY (id);


--
-- TOC entry 3983 (class 2606 OID 1397706)
-- Name: ir_types_of_drugs ir_types_of_drugs_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_types_of_drugs
    ADD CONSTRAINT ir_types_of_drugs_pkey PRIMARY KEY (id);


--
-- TOC entry 4064 (class 2606 OID 1420939)
-- Name: mo_seizures mo_seizures_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.mo_seizures
    ADD CONSTRAINT mo_seizures_pkey PRIMARY KEY (mo_seizure_id);


--
-- TOC entry 4045 (class 2606 OID 1413504)
-- Name: old_interragation_report old_interragation_report_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.old_interragation_report
    ADD CONSTRAINT old_interragation_report_pkey PRIMARY KEY (interrogation_report_id);


--
-- TOC entry 4047 (class 2606 OID 1413831)
-- Name: person_deduplication_tracker person_deduplication_tracker_person_fingerprint_key; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.person_deduplication_tracker
    ADD CONSTRAINT person_deduplication_tracker_person_fingerprint_key UNIQUE (person_fingerprint);


--
-- TOC entry 4049 (class 2606 OID 1413829)
-- Name: person_deduplication_tracker person_deduplication_tracker_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.person_deduplication_tracker
    ADD CONSTRAINT person_deduplication_tracker_pkey PRIMARY KEY (id);


--
-- TOC entry 3936 (class 2606 OID 1397583)
-- Name: persons persons_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.persons
    ADD CONSTRAINT persons_pkey PRIMARY KEY (person_id);


--
-- TOC entry 4136 (class 2606 OID 25200398)
-- Name: properties_pending_fk properties_pending_fk_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.properties_pending_fk
    ADD CONSTRAINT properties_pending_fk_pkey PRIMARY KEY (id);


--
-- TOC entry 3963 (class 2606 OID 1397628)
-- Name: properties properties_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.properties
    ADD CONSTRAINT properties_pkey PRIMARY KEY (property_id);


--
-- TOC entry 4096 (class 2606 OID 2028714)
-- Name: dedup_cluster_state uix_cluster_person; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.dedup_cluster_state
    ADD CONSTRAINT uix_cluster_person UNIQUE (cluster_id, person_index);


--
-- TOC entry 4043 (class 2606 OID 1412940)
-- Name: files unique_file_per_source; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.files
    ADD CONSTRAINT unique_file_per_source UNIQUE (source_type, source_field, parent_id, file_id, file_index);


--
-- TOC entry 4030 (class 2606 OID 1404631)
-- Name: brief_facts_accused uq_bf_accused_id_accused_id; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.brief_facts_accused
    ADD CONSTRAINT uq_bf_accused_id_accused_id UNIQUE (bf_accused_id, accused_id);


--
-- TOC entry 4033 (class 2606 OID 1404748)
-- Name: user user_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public."user"
    ADD CONSTRAINT user_pkey PRIMARY KEY (id);


--
-- TOC entry 4121 (class 1259 OID 26454181)
-- Name: geo_ref_district_trgm; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX geo_ref_district_trgm ON public.geo_reference USING gin (district_name public.gin_trgm_ops);


--
-- TOC entry 4122 (class 1259 OID 26454182)
-- Name: geo_ref_mandal_trgm; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX geo_ref_mandal_trgm ON public.geo_reference USING gin (sub_district_name public.gin_trgm_ops);


--
-- TOC entry 4123 (class 1259 OID 26454180)
-- Name: geo_ref_state_trgm; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX geo_ref_state_trgm ON public.geo_reference USING gin (state_name public.gin_trgm_ops);


--
-- TOC entry 4124 (class 1259 OID 26454112)
-- Name: geo_ref_trgm_idx; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX geo_ref_trgm_idx ON public.geo_reference USING gin (district_name public.gin_trgm_ops, sub_district_name public.gin_trgm_ops);


--
-- TOC entry 4125 (class 1259 OID 26454183)
-- Name: geo_reference_mandal_trgm; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX geo_reference_mandal_trgm ON public.geo_reference USING gin (sub_district_name public.gin_trgm_ops);


--
-- TOC entry 3953 (class 1259 OID 1397888)
-- Name: idx_accused_code; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_accused_code ON public.accused USING btree (accused_code);


--
-- TOC entry 3954 (class 1259 OID 1397886)
-- Name: idx_accused_crime; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_accused_crime ON public.accused USING btree (crime_id);


--
-- TOC entry 3955 (class 1259 OID 23355611)
-- Name: idx_accused_crime_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_accused_crime_id ON public.accused USING btree (crime_id);


--
-- TOC entry 3956 (class 1259 OID 23355600)
-- Name: idx_accused_crime_person; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_accused_crime_person ON public.accused USING btree (crime_id, person_id);


--
-- TOC entry 3957 (class 1259 OID 1397887)
-- Name: idx_accused_person; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_accused_person ON public.accused USING btree (person_id);


--
-- TOC entry 3958 (class 1259 OID 26424259)
-- Name: idx_accused_status; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_accused_status ON public.accused USING btree (accused_status);


--
-- TOC entry 4168 (class 1259 OID 30861072)
-- Name: idx_accuseds_mv_accusedrole; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_accuseds_mv_accusedrole ON public.accuseds_mv USING btree ("accusedRole");


--
-- TOC entry 4169 (class 1259 OID 30861069)
-- Name: idx_accuseds_mv_accusedstatus; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_accuseds_mv_accusedstatus ON public.accuseds_mv USING btree ("accusedStatus");


--
-- TOC entry 4170 (class 1259 OID 30861070)
-- Name: idx_accuseds_mv_accusedstatusraw; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_accuseds_mv_accusedstatusraw ON public.accuseds_mv USING btree ("accusedStatusRaw");


--
-- TOC entry 4171 (class 1259 OID 30861071)
-- Name: idx_accuseds_mv_accusedtype; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_accuseds_mv_accusedtype ON public.accuseds_mv USING btree ("accusedType");


--
-- TOC entry 4172 (class 1259 OID 30861082)
-- Name: idx_accuseds_mv_age; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_accuseds_mv_age ON public.accuseds_mv USING btree (age);


--
-- TOC entry 4173 (class 1259 OID 30861080)
-- Name: idx_accuseds_mv_caseclassification; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_accuseds_mv_caseclassification ON public.accuseds_mv USING btree ("caseClassification");


--
-- TOC entry 4174 (class 1259 OID 30861064)
-- Name: idx_accuseds_mv_crimeid; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_accuseds_mv_crimeid ON public.accuseds_mv USING btree ("crimeId");


--
-- TOC entry 4175 (class 1259 OID 30861068)
-- Name: idx_accuseds_mv_crimeregdate; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_accuseds_mv_crimeregdate ON public.accuseds_mv USING btree ("crimeRegDate");


--
-- TOC entry 4176 (class 1259 OID 30861074)
-- Name: idx_accuseds_mv_date_status; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_accuseds_mv_date_status ON public.accuseds_mv USING btree ("crimeRegDate", "accusedStatus");


--
-- TOC entry 4177 (class 1259 OID 30861076)
-- Name: idx_accuseds_mv_date_unit_domicile; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_accuseds_mv_date_unit_domicile ON public.accuseds_mv USING btree ("crimeRegDate", unit, domicile);


--
-- TOC entry 4178 (class 1259 OID 30861075)
-- Name: idx_accuseds_mv_date_unit_status; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_accuseds_mv_date_unit_status ON public.accuseds_mv USING btree ("crimeRegDate", unit, "accusedStatus");


--
-- TOC entry 4179 (class 1259 OID 30861073)
-- Name: idx_accuseds_mv_domicile; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_accuseds_mv_domicile ON public.accuseds_mv USING btree (domicile);


--
-- TOC entry 4180 (class 1259 OID 30861084)
-- Name: idx_accuseds_mv_drugtype_gin; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_accuseds_mv_drugtype_gin ON public.accuseds_mv USING gin ("drugType");


--
-- TOC entry 4181 (class 1259 OID 30861083)
-- Name: idx_accuseds_mv_firnumber; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_accuseds_mv_firnumber ON public.accuseds_mv USING btree ("firNumber");


--
-- TOC entry 4182 (class 1259 OID 30861081)
-- Name: idx_accuseds_mv_fullname; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_accuseds_mv_fullname ON public.accuseds_mv USING btree ("fullName");


--
-- TOC entry 4183 (class 1259 OID 30861077)
-- Name: idx_accuseds_mv_gender; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_accuseds_mv_gender ON public.accuseds_mv USING btree (gender);


--
-- TOC entry 4184 (class 1259 OID 30861063)
-- Name: idx_accuseds_mv_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE UNIQUE INDEX idx_accuseds_mv_id ON public.accuseds_mv USING btree (id);


--
-- TOC entry 4185 (class 1259 OID 30861078)
-- Name: idx_accuseds_mv_nationality; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_accuseds_mv_nationality ON public.accuseds_mv USING btree (nationality);


--
-- TOC entry 4186 (class 1259 OID 30861079)
-- Name: idx_accuseds_mv_permanentstateut; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_accuseds_mv_permanentstateut ON public.accuseds_mv USING btree ("permanentStateUt");


--
-- TOC entry 4187 (class 1259 OID 30861067)
-- Name: idx_accuseds_mv_ps; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_accuseds_mv_ps ON public.accuseds_mv USING btree (ps);


--
-- TOC entry 4188 (class 1259 OID 30861066)
-- Name: idx_accuseds_mv_unit; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_accuseds_mv_unit ON public.accuseds_mv USING btree (unit);


--
-- TOC entry 4189 (class 1259 OID 30861065)
-- Name: idx_accuseds_mv_year; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_accuseds_mv_year ON public.accuseds_mv USING btree (year);


--
-- TOC entry 4091 (class 1259 OID 23355609)
-- Name: idx_adt_all_person_ids_gin; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_adt_all_person_ids_gin ON public.agent_deduplication_tracker USING gin (all_person_ids);


--
-- TOC entry 4092 (class 1259 OID 23355610)
-- Name: idx_adt_canonical_person_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_adt_canonical_person_id ON public.agent_deduplication_tracker USING btree (canonical_person_id);


--
-- TOC entry 4140 (class 1259 OID 30054431)
-- Name: idx_adv_accuseds_mv_accusedstatus; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_adv_accuseds_mv_accusedstatus ON public.advanced_search_accuseds_mv USING btree ("accusedStatus");


--
-- TOC entry 4141 (class 1259 OID 30054430)
-- Name: idx_adv_accuseds_mv_caseclass; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_adv_accuseds_mv_caseclass ON public.advanced_search_accuseds_mv USING btree ("caseClass");


--
-- TOC entry 4142 (class 1259 OID 30054429)
-- Name: idx_adv_accuseds_mv_casestatus; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_adv_accuseds_mv_casestatus ON public.advanced_search_accuseds_mv USING btree ("caseStatus");


--
-- TOC entry 4143 (class 1259 OID 30054428)
-- Name: idx_adv_accuseds_mv_distname; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_adv_accuseds_mv_distname ON public.advanced_search_accuseds_mv USING btree ("distName");


--
-- TOC entry 4144 (class 1259 OID 30054432)
-- Name: idx_adv_accuseds_mv_firdate_psname; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_adv_accuseds_mv_firdate_psname ON public.advanced_search_accuseds_mv USING btree ("firDate", "psName");


--
-- TOC entry 4145 (class 1259 OID 30054425)
-- Name: idx_adv_accuseds_mv_fullname; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_adv_accuseds_mv_fullname ON public.advanced_search_accuseds_mv USING btree ("fullName");


--
-- TOC entry 4146 (class 1259 OID 30054426)
-- Name: idx_adv_accuseds_mv_name; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_adv_accuseds_mv_name ON public.advanced_search_accuseds_mv USING btree (name);


--
-- TOC entry 4147 (class 1259 OID 30054427)
-- Name: idx_adv_accuseds_mv_psname; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_adv_accuseds_mv_psname ON public.advanced_search_accuseds_mv USING btree ("psName");


--
-- TOC entry 4151 (class 1259 OID 30060311)
-- Name: idx_adv_firs_mv_caseclass; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_adv_firs_mv_caseclass ON public.advanced_search_firs_mv USING btree ("caseClass");


--
-- TOC entry 4152 (class 1259 OID 30060310)
-- Name: idx_adv_firs_mv_casestatus; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_adv_firs_mv_casestatus ON public.advanced_search_firs_mv USING btree ("caseStatus");


--
-- TOC entry 4153 (class 1259 OID 30060312)
-- Name: idx_adv_firs_mv_crimetype; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_adv_firs_mv_crimetype ON public.advanced_search_firs_mv USING btree ("crimeType");


--
-- TOC entry 4154 (class 1259 OID 30060309)
-- Name: idx_adv_firs_mv_distname; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_adv_firs_mv_distname ON public.advanced_search_firs_mv USING btree ("distName");


--
-- TOC entry 4155 (class 1259 OID 30060314)
-- Name: idx_adv_firs_mv_firdate_distname; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_adv_firs_mv_firdate_distname ON public.advanced_search_firs_mv USING btree ("firDate", "distName");


--
-- TOC entry 4156 (class 1259 OID 30060313)
-- Name: idx_adv_firs_mv_firdate_psname; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_adv_firs_mv_firdate_psname ON public.advanced_search_firs_mv USING btree ("firDate", "psName");


--
-- TOC entry 4157 (class 1259 OID 30060308)
-- Name: idx_adv_firs_mv_psname; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_adv_firs_mv_psname ON public.advanced_search_firs_mv USING btree ("psName");


--
-- TOC entry 4148 (class 1259 OID 30054424)
-- Name: idx_advanced_search_accuseds_mv_firdate; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_advanced_search_accuseds_mv_firdate ON public.advanced_search_accuseds_mv USING btree ("firDate");


--
-- TOC entry 4149 (class 1259 OID 30054423)
-- Name: idx_advanced_search_accuseds_mv_firnum; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_advanced_search_accuseds_mv_firnum ON public.advanced_search_accuseds_mv USING btree ("firNum");


--
-- TOC entry 4150 (class 1259 OID 30054422)
-- Name: idx_advanced_search_accuseds_mv_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE UNIQUE INDEX idx_advanced_search_accuseds_mv_id ON public.advanced_search_accuseds_mv USING btree (id);


--
-- TOC entry 4158 (class 1259 OID 30060307)
-- Name: idx_advanced_search_firs_mv_firdate; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_advanced_search_firs_mv_firdate ON public.advanced_search_firs_mv USING btree ("firDate");


--
-- TOC entry 4159 (class 1259 OID 30060306)
-- Name: idx_advanced_search_firs_mv_firnum; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_advanced_search_firs_mv_firnum ON public.advanced_search_firs_mv USING btree ("firNum");


--
-- TOC entry 4160 (class 1259 OID 30060305)
-- Name: idx_advanced_search_firs_mv_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE UNIQUE INDEX idx_advanced_search_firs_mv_id ON public.advanced_search_firs_mv USING btree (id);


--
-- TOC entry 4059 (class 1259 OID 1420514)
-- Name: idx_arrests_crime; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_arrests_crime ON public.arrests USING btree (crime_id);


--
-- TOC entry 4060 (class 1259 OID 1420515)
-- Name: idx_arrests_person; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_arrests_person ON public.arrests USING btree (person_id);


--
-- TOC entry 4024 (class 1259 OID 26473114)
-- Name: idx_bf_accused_accused_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_bf_accused_accused_id ON public.brief_facts_accused USING btree (accused_id);


--
-- TOC entry 4025 (class 1259 OID 26473112)
-- Name: idx_bf_accused_crime_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_bf_accused_crime_id ON public.brief_facts_accused USING btree (crime_id);


--
-- TOC entry 4026 (class 1259 OID 26473113)
-- Name: idx_bf_accused_person_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_bf_accused_person_id ON public.brief_facts_accused USING btree (person_id);


--
-- TOC entry 4119 (class 1259 OID 22014315)
-- Name: idx_bfd_crime_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_bfd_crime_id ON public.brief_facts_drug USING btree (crime_id);


--
-- TOC entry 4120 (class 1259 OID 22014317)
-- Name: idx_bfd_primary_drug; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_bfd_primary_drug ON public.brief_facts_drug USING btree (primary_drug_name);


--
-- TOC entry 4027 (class 1259 OID 1404633)
-- Name: idx_brief_facts_accused_crime_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_brief_facts_accused_crime_id ON public.brief_facts_accused USING btree (crime_id);


--
-- TOC entry 4028 (class 1259 OID 1404634)
-- Name: idx_brief_facts_accused_crime_person; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_brief_facts_accused_crime_person ON public.brief_facts_accused USING btree (crime_id, person_id);


--
-- TOC entry 4081 (class 1259 OID 1639331)
-- Name: idx_charge_sheet_updates_status; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_charge_sheet_updates_status ON public.charge_sheet_updates USING btree (charge_sheet_status);


--
-- TOC entry 4082 (class 1259 OID 1639328)
-- Name: idx_charge_sheet_updates_update_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_charge_sheet_updates_update_id ON public.charge_sheet_updates USING btree (update_charge_sheet_id);


--
-- TOC entry 4073 (class 1259 OID 23355604)
-- Name: idx_chargesheet_accused_cs_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_chargesheet_accused_cs_id ON public.chargesheet_accused USING btree (chargesheet_id);


--
-- TOC entry 4074 (class 1259 OID 23355605)
-- Name: idx_chargesheet_accused_cs_person; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_chargesheet_accused_cs_person ON public.chargesheet_accused USING btree (chargesheet_id, accused_person_id);


--
-- TOC entry 4067 (class 1259 OID 23355602)
-- Name: idx_chargesheets_crime_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_chargesheets_crime_id ON public.chargesheets USING btree (crime_id);


--
-- TOC entry 4068 (class 1259 OID 23355603)
-- Name: idx_chargesheets_crime_no_date; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_chargesheets_crime_no_date ON public.chargesheets USING btree (crime_id, chargesheet_no, chargesheet_date);


--
-- TOC entry 3943 (class 1259 OID 1397882)
-- Name: idx_crimes_case_status; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_crimes_case_status ON public.crimes USING btree (case_status);


--
-- TOC entry 3944 (class 1259 OID 23355601)
-- Name: idx_crimes_dates; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_crimes_dates ON public.crimes USING btree (date_created DESC, date_modified DESC);


--
-- TOC entry 3945 (class 1259 OID 1397880)
-- Name: idx_crimes_fir_date; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_crimes_fir_date ON public.crimes USING btree (fir_date);


--
-- TOC entry 3946 (class 1259 OID 1397883)
-- Name: idx_crimes_fir_num; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_crimes_fir_num ON public.crimes USING btree (fir_num);


--
-- TOC entry 3947 (class 1259 OID 1397879)
-- Name: idx_crimes_ps_code; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_crimes_ps_code ON public.crimes USING btree (ps_code);


--
-- TOC entry 4161 (class 1259 OID 30092757)
-- Name: idx_criminal_profiles_mv_age; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_criminal_profiles_mv_age ON public.criminal_profiles_mv USING btree (age);


--
-- TOC entry 4162 (class 1259 OID 30092756)
-- Name: idx_criminal_profiles_mv_domicile; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_criminal_profiles_mv_domicile ON public.criminal_profiles_mv USING btree (domicile);


--
-- TOC entry 4163 (class 1259 OID 30092754)
-- Name: idx_criminal_profiles_mv_fullname; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_criminal_profiles_mv_fullname ON public.criminal_profiles_mv USING btree ("fullName");


--
-- TOC entry 4164 (class 1259 OID 30092758)
-- Name: idx_criminal_profiles_mv_gender; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_criminal_profiles_mv_gender ON public.criminal_profiles_mv USING btree (gender);


--
-- TOC entry 4165 (class 1259 OID 30092753)
-- Name: idx_criminal_profiles_mv_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE UNIQUE INDEX idx_criminal_profiles_mv_id ON public.criminal_profiles_mv USING btree (id);


--
-- TOC entry 4166 (class 1259 OID 30092759)
-- Name: idx_criminal_profiles_mv_nationality; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_criminal_profiles_mv_nationality ON public.criminal_profiles_mv USING btree (nationality);


--
-- TOC entry 4167 (class 1259 OID 30092755)
-- Name: idx_criminal_profiles_mv_noofcrimes; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_criminal_profiles_mv_noofcrimes ON public.criminal_profiles_mv USING btree ("noOfCrimes");


--
-- TOC entry 4054 (class 1259 OID 1420069)
-- Name: idx_disposal_crime; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_disposal_crime ON public.disposal USING btree (crime_id);


--
-- TOC entry 4108 (class 1259 OID 23355606)
-- Name: idx_drug_categories_raw_name; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_drug_categories_raw_name ON public.drug_categories USING btree (raw_name);


--
-- TOC entry 4109 (class 1259 OID 23355607)
-- Name: idx_drug_categories_standard_name; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_drug_categories_standard_name ON public.drug_categories USING btree (standard_name);


--
-- TOC entry 4115 (class 1259 OID 23355608)
-- Name: idx_drug_ignore_list_term; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_drug_ignore_list_term ON public.drug_ignore_list USING btree (term);


--
-- TOC entry 4036 (class 1259 OID 10628347)
-- Name: idx_files_created_at; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_files_created_at ON public.files USING btree (created_at);


--
-- TOC entry 4037 (class 1259 OID 25648888)
-- Name: idx_files_downloaded_at; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_files_downloaded_at ON public.files USING btree (downloaded_at) WHERE (downloaded_at IS NOT NULL);


--
-- TOC entry 4038 (class 1259 OID 25648887)
-- Name: idx_files_is_downloaded; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_files_is_downloaded ON public.files USING btree (is_downloaded) WHERE (is_downloaded = true);


--
-- TOC entry 4039 (class 1259 OID 1412943)
-- Name: idx_files_parent_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_files_parent_id ON public.files USING btree (parent_id);


--
-- TOC entry 4040 (class 1259 OID 1412945)
-- Name: idx_files_source_parent; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_files_source_parent ON public.files USING btree (source_type, parent_id);


--
-- TOC entry 4041 (class 1259 OID 25648889)
-- Name: idx_files_source_type_created; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_files_source_type_created ON public.files USING btree (source_type, created_at);


--
-- TOC entry 4190 (class 1259 OID 32629204)
-- Name: idx_firs_mv_acquittalcount; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_firs_mv_acquittalcount ON public.firs_mv USING btree ("acquittalCount");


--
-- TOC entry 4191 (class 1259 OID 32629200)
-- Name: idx_firs_mv_caseclassification; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_firs_mv_caseclassification ON public.firs_mv USING btree ("caseClassification");


--
-- TOC entry 4192 (class 1259 OID 32629199)
-- Name: idx_firs_mv_casestatus; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_firs_mv_casestatus ON public.firs_mv USING btree ("caseStatus");


--
-- TOC entry 4193 (class 1259 OID 32629203)
-- Name: idx_firs_mv_convictioncount; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_firs_mv_convictioncount ON public.firs_mv USING btree ("convictionCount");


--
-- TOC entry 4194 (class 1259 OID 32629198)
-- Name: idx_firs_mv_crimeregdate; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_firs_mv_crimeregdate ON public.firs_mv USING btree ("crimeRegDate");


--
-- TOC entry 4195 (class 1259 OID 32629201)
-- Name: idx_firs_mv_crimetype; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_firs_mv_crimetype ON public.firs_mv USING btree ("crimeType");


--
-- TOC entry 4196 (class 1259 OID 32629212)
-- Name: idx_firs_mv_date_commercial; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_firs_mv_date_commercial ON public.firs_mv USING btree ("crimeRegDate", "isCommercial");


--
-- TOC entry 4197 (class 1259 OID 32629208)
-- Name: idx_firs_mv_date_status; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_firs_mv_date_status ON public.firs_mv USING btree ("crimeRegDate", "caseStatus");


--
-- TOC entry 4198 (class 1259 OID 32629209)
-- Name: idx_firs_mv_date_unit; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_firs_mv_date_unit ON public.firs_mv USING btree ("crimeRegDate", unit);


--
-- TOC entry 4199 (class 1259 OID 32629210)
-- Name: idx_firs_mv_date_unit_acquittal; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_firs_mv_date_unit_acquittal ON public.firs_mv USING btree ("crimeRegDate", unit, "acquittalCount");


--
-- TOC entry 4200 (class 1259 OID 32629211)
-- Name: idx_firs_mv_date_unit_conviction; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_firs_mv_date_unit_conviction ON public.firs_mv USING btree ("crimeRegDate", unit, "convictionCount");


--
-- TOC entry 4201 (class 1259 OID 32629207)
-- Name: idx_firs_mv_drugtype_gin; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_firs_mv_drugtype_gin ON public.firs_mv USING gin ("drugType");


--
-- TOC entry 4202 (class 1259 OID 32629197)
-- Name: idx_firs_mv_firnumber; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_firs_mv_firnumber ON public.firs_mv USING btree ("firNumber");


--
-- TOC entry 4203 (class 1259 OID 32629193)
-- Name: idx_firs_mv_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE UNIQUE INDEX idx_firs_mv_id ON public.firs_mv USING btree (id);


--
-- TOC entry 4204 (class 1259 OID 32629202)
-- Name: idx_firs_mv_iscommercial; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_firs_mv_iscommercial ON public.firs_mv USING btree ("isCommercial");


--
-- TOC entry 4205 (class 1259 OID 32629196)
-- Name: idx_firs_mv_ps; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_firs_mv_ps ON public.firs_mv USING btree (ps);


--
-- TOC entry 4206 (class 1259 OID 32629206)
-- Name: idx_firs_mv_stipulated; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_firs_mv_stipulated ON public.firs_mv USING btree ("stipulatedPeriodForCS");


--
-- TOC entry 4207 (class 1259 OID 32629205)
-- Name: idx_firs_mv_totaldisposals; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_firs_mv_totaldisposals ON public.firs_mv USING btree ("totalDisposals");


--
-- TOC entry 4208 (class 1259 OID 32629195)
-- Name: idx_firs_mv_unit; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_firs_mv_unit ON public.firs_mv USING btree (unit);


--
-- TOC entry 4209 (class 1259 OID 32629194)
-- Name: idx_firs_mv_year; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_firs_mv_year ON public.firs_mv USING btree (year);


--
-- TOC entry 4085 (class 1259 OID 1639545)
-- Name: idx_fsl_case_property_crime_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_fsl_case_property_crime_id ON public.fsl_case_property USING btree (crime_id);


--
-- TOC entry 4088 (class 1259 OID 1639552)
-- Name: idx_fsl_case_property_media_case_property_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_fsl_case_property_media_case_property_id ON public.fsl_case_property_media USING btree (case_property_id);


--
-- TOC entry 4132 (class 1259 OID 24850411)
-- Name: idx_geo_countries_country_trgm; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_geo_countries_country_trgm ON public.geo_countries USING gin (country_name public.gin_trgm_ops);


--
-- TOC entry 4133 (class 1259 OID 24850412)
-- Name: idx_geo_countries_state_trgm; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_geo_countries_state_trgm ON public.geo_countries USING gin (state_name public.gin_trgm_ops);


--
-- TOC entry 4116 (class 1259 OID 20996637)
-- Name: idx_ignore_term; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ignore_term ON public.drug_ignore_list USING btree (term);


--
-- TOC entry 4002 (class 1259 OID 1397926)
-- Name: idx_ir_associate_details_ir; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_associate_details_ir ON public.ir_associate_details USING btree (interrogation_report_id);


--
-- TOC entry 3990 (class 1259 OID 1397922)
-- Name: idx_ir_consumer_details_ir; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_consumer_details_ir ON public.ir_consumer_details USING btree (interrogation_report_id);


--
-- TOC entry 3964 (class 1259 OID 1397898)
-- Name: idx_ir_crime_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_crime_id ON public.interrogation_reports USING btree (crime_id);


--
-- TOC entry 3999 (class 1259 OID 1397925)
-- Name: idx_ir_defence_counsel_ir; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_defence_counsel_ir ON public.ir_defence_counsel USING btree (interrogation_report_id);


--
-- TOC entry 4017 (class 1259 OID 1397930)
-- Name: idx_ir_dopams_links_ir; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_dopams_links_ir ON public.ir_dopams_links USING btree (interrogation_report_id);


--
-- TOC entry 3969 (class 1259 OID 1397912)
-- Name: idx_ir_family_history_ir; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_family_history_ir ON public.ir_family_history USING btree (interrogation_report_id);


--
-- TOC entry 3970 (class 1259 OID 1397913)
-- Name: idx_ir_family_history_person; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_family_history_person ON public.ir_family_history USING btree (person_id);


--
-- TOC entry 3987 (class 1259 OID 1397921)
-- Name: idx_ir_financial_history_ir; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_financial_history_ir ON public.ir_financial_history USING btree (interrogation_report_id);


--
-- TOC entry 3965 (class 1259 OID 1397908)
-- Name: idx_ir_is_in_jail; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_is_in_jail ON public.interrogation_reports USING btree (is_in_jail);


--
-- TOC entry 3973 (class 1259 OID 1397914)
-- Name: idx_ir_local_contacts_ir; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_local_contacts_ir ON public.ir_local_contacts USING btree (interrogation_report_id);


--
-- TOC entry 4008 (class 1259 OID 1397928)
-- Name: idx_ir_media_ir; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_media_ir ON public.ir_media USING btree (interrogation_report_id);


--
-- TOC entry 3993 (class 1259 OID 1397923)
-- Name: idx_ir_modus_operandi_ir; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_modus_operandi_ir ON public.ir_modus_operandi USING btree (interrogation_report_id);


--
-- TOC entry 3966 (class 1259 OID 1397899)
-- Name: idx_ir_person_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_person_id ON public.interrogation_reports USING btree (person_id);


--
-- TOC entry 3996 (class 1259 OID 1397924)
-- Name: idx_ir_previous_offences_ir; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_previous_offences_ir ON public.ir_previous_offences_confessed USING btree (interrogation_report_id);


--
-- TOC entry 3976 (class 1259 OID 1397915)
-- Name: idx_ir_regular_habits_ir; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_regular_habits_ir ON public.ir_regular_habits USING btree (interrogation_report_id);


--
-- TOC entry 4005 (class 1259 OID 1397927)
-- Name: idx_ir_shelter_ir; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_shelter_ir ON public.ir_shelter USING btree (interrogation_report_id);


--
-- TOC entry 3984 (class 1259 OID 1397919)
-- Name: idx_ir_sim_details_ir; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_sim_details_ir ON public.ir_sim_details USING btree (interrogation_report_id);


--
-- TOC entry 3981 (class 1259 OID 1397917)
-- Name: idx_ir_types_of_drugs_ir; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_types_of_drugs_ir ON public.ir_types_of_drugs USING btree (interrogation_report_id);


--
-- TOC entry 4061 (class 1259 OID 1420945)
-- Name: idx_mo_seizures_crime; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_mo_seizures_crime ON public.mo_seizures USING btree (crime_id);


--
-- TOC entry 4062 (class 1259 OID 1420946)
-- Name: idx_mo_seizures_seized_at; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_mo_seizures_seized_at ON public.mo_seizures USING btree (seized_at);


--
-- TOC entry 4137 (class 1259 OID 25200412)
-- Name: idx_pending_fk_ir_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE UNIQUE INDEX idx_pending_fk_ir_id ON public.ir_pending_fk USING btree (ir_id) WHERE (NOT resolved);


--
-- TOC entry 4134 (class 1259 OID 25200399)
-- Name: idx_pending_fk_property_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE UNIQUE INDEX idx_pending_fk_property_id ON public.properties_pending_fk USING btree (property_id) WHERE (NOT resolved);


--
-- TOC entry 3929 (class 1259 OID 24497541)
-- Name: idx_persons_email; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_persons_email ON public.persons USING btree (email_id);


--
-- TOC entry 3930 (class 1259 OID 1397875)
-- Name: idx_persons_full_name; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_persons_full_name ON public.persons USING btree (full_name);


--
-- TOC entry 3931 (class 1259 OID 1397874)
-- Name: idx_persons_name; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_persons_name ON public.persons USING btree (name);


--
-- TOC entry 3932 (class 1259 OID 26454157)
-- Name: idx_persons_person_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_persons_person_id ON public.persons USING btree (person_id);


--
-- TOC entry 3933 (class 1259 OID 1397876)
-- Name: idx_persons_phone; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_persons_phone ON public.persons USING btree (phone_number);


--
-- TOC entry 3934 (class 1259 OID 1397877)
-- Name: idx_persons_present_district; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_persons_present_district ON public.persons USING btree (present_district);


--
-- TOC entry 3959 (class 1259 OID 24497543)
-- Name: idx_properties_additional_details_gin; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_properties_additional_details_gin ON public.properties USING gin (additional_details);


--
-- TOC entry 3960 (class 1259 OID 1397889)
-- Name: idx_properties_crime_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_properties_crime_id ON public.properties USING btree (crime_id);


--
-- TOC entry 3961 (class 1259 OID 1397894)
-- Name: idx_properties_date_seizure; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_properties_date_seizure ON public.properties USING btree (date_of_seizure);


--
-- TOC entry 4099 (class 1259 OID 2028722)
-- Name: ix_dedup_comparison_progress_person_i_index; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX ix_dedup_comparison_progress_person_i_index ON public.dedup_comparison_progress USING btree (person_i_index);


--
-- TOC entry 3948 (class 1259 OID 24497542)
-- Name: trgm_idx_crimes_acts_sections; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX trgm_idx_crimes_acts_sections ON public.crimes USING gin (acts_sections public.gin_trgm_ops);


--
-- TOC entry 4110 (class 1259 OID 20996624)
-- Name: trgm_idx_drug_raw_name; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX trgm_idx_drug_raw_name ON public.drug_categories USING gin (raw_name public.gin_trgm_ops);


--
-- TOC entry 4128 (class 1259 OID 23469823)
-- Name: trgm_idx_geo_district; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX trgm_idx_geo_district ON public.geo_reference USING gin (district_name public.gin_trgm_ops);


--
-- TOC entry 4129 (class 1259 OID 23469824)
-- Name: trgm_idx_geo_state; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX trgm_idx_geo_state ON public.geo_reference USING gin (state_name public.gin_trgm_ops);


--
-- TOC entry 4130 (class 1259 OID 23469822)
-- Name: trgm_idx_geo_sub_district; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX trgm_idx_geo_sub_district ON public.geo_reference USING gin (sub_district_name public.gin_trgm_ops);


--
-- TOC entry 4131 (class 1259 OID 23469821)
-- Name: trgm_idx_geo_village; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX trgm_idx_geo_village ON public.geo_reference USING gin (village_name_english public.gin_trgm_ops);


--
-- TOC entry 3937 (class 1259 OID 24497540)
-- Name: trgm_idx_persons_alias; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX trgm_idx_persons_alias ON public.persons USING gin (alias public.gin_trgm_ops);


--
-- TOC entry 3938 (class 1259 OID 24497539)
-- Name: trgm_idx_persons_full_name; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX trgm_idx_persons_full_name ON public.persons USING gin (full_name public.gin_trgm_ops);


--
-- TOC entry 4031 (class 1259 OID 1404749)
-- Name: user_email_key; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE UNIQUE INDEX user_email_key ON public."user" USING btree (email);


--
-- TOC entry 4248 (class 2620 OID 29321374)
-- Name: files trigger_auto_generate_file_paths; Type: TRIGGER; Schema: public; Owner: dev_dopamas
--

CREATE TRIGGER trigger_auto_generate_file_paths BEFORE INSERT OR UPDATE ON public.files FOR EACH ROW EXECUTE FUNCTION public.auto_generate_file_paths();


--
-- TOC entry 4211 (class 2606 OID 1397609)
-- Name: accused accused_crime_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.accused
    ADD CONSTRAINT accused_crime_id_fkey FOREIGN KEY (crime_id) REFERENCES public.crimes(crime_id) ON DELETE CASCADE;


--
-- TOC entry 4212 (class 2606 OID 1397614)
-- Name: accused accused_person_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.accused
    ADD CONSTRAINT accused_person_id_fkey FOREIGN KEY (person_id) REFERENCES public.persons(person_id) ON DELETE RESTRICT;


--
-- TOC entry 4234 (class 2606 OID 1420504)
-- Name: arrests arrests_crime_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.arrests
    ADD CONSTRAINT arrests_crime_id_fkey FOREIGN KEY (crime_id) REFERENCES public.crimes(crime_id);


--
-- TOC entry 4235 (class 2606 OID 1420509)
-- Name: arrests arrests_person_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.arrests
    ADD CONSTRAINT arrests_person_id_fkey FOREIGN KEY (person_id) REFERENCES public.persons(person_id);


--
-- TOC entry 4247 (class 2606 OID 22014305)
-- Name: brief_facts_drug brief_facts_drug_crime_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.brief_facts_drug
    ADD CONSTRAINT brief_facts_drug_crime_id_fkey FOREIGN KEY (crime_id) REFERENCES public.crimes(crime_id) ON DELETE CASCADE;


--
-- TOC entry 4244 (class 2606 OID 1639322)
-- Name: charge_sheet_updates charge_sheet_updates_crime_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.charge_sheet_updates
    ADD CONSTRAINT charge_sheet_updates_crime_id_fkey FOREIGN KEY (crime_id) REFERENCES public.crimes(crime_id);


--
-- TOC entry 4240 (class 2606 OID 1422350)
-- Name: chargesheet_accused chargesheet_accused_chargesheet_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.chargesheet_accused
    ADD CONSTRAINT chargesheet_accused_chargesheet_id_fkey FOREIGN KEY (chargesheet_id) REFERENCES public.chargesheets(id);


--
-- TOC entry 4242 (class 2606 OID 1422369)
-- Name: chargesheet_acts chargesheet_acts_chargesheet_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.chargesheet_acts
    ADD CONSTRAINT chargesheet_acts_chargesheet_id_fkey FOREIGN KEY (chargesheet_id) REFERENCES public.chargesheets(id);


--
-- TOC entry 4238 (class 2606 OID 1422330)
-- Name: chargesheet_files chargesheet_files_chargesheet_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.chargesheet_files
    ADD CONSTRAINT chargesheet_files_chargesheet_id_fkey FOREIGN KEY (chargesheet_id) REFERENCES public.chargesheets(id);


--
-- TOC entry 4237 (class 2606 OID 1422319)
-- Name: chargesheets chargesheets_crime_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.chargesheets
    ADD CONSTRAINT chargesheets_crime_id_fkey FOREIGN KEY (crime_id) REFERENCES public.crimes(crime_id);


--
-- TOC entry 4210 (class 2606 OID 1397593)
-- Name: crimes crimes_ps_code_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.crimes
    ADD CONSTRAINT crimes_ps_code_fkey FOREIGN KEY (ps_code) REFERENCES public.hierarchy(ps_code) ON DELETE RESTRICT;


--
-- TOC entry 4233 (class 2606 OID 1420064)
-- Name: disposal disposal_crime_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.disposal
    ADD CONSTRAINT disposal_crime_id_fkey FOREIGN KEY (crime_id) REFERENCES public.crimes(crime_id);


--
-- TOC entry 4231 (class 2606 OID 1404637)
-- Name: brief_facts_accused fk_bf_accused_crime; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.brief_facts_accused
    ADD CONSTRAINT fk_bf_accused_crime FOREIGN KEY (crime_id) REFERENCES public.crimes(crime_id) ON DELETE CASCADE;


--
-- TOC entry 4241 (class 2606 OID 1422355)
-- Name: chargesheet_accused fk_chargesheet_accused_chargesheet; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.chargesheet_accused
    ADD CONSTRAINT fk_chargesheet_accused_chargesheet FOREIGN KEY (chargesheet_id) REFERENCES public.chargesheets(id) ON DELETE CASCADE;


--
-- TOC entry 4243 (class 2606 OID 1422374)
-- Name: chargesheet_acts fk_chargesheet_acts_chargesheet; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.chargesheet_acts
    ADD CONSTRAINT fk_chargesheet_acts_chargesheet FOREIGN KEY (chargesheet_id) REFERENCES public.chargesheets(id) ON DELETE CASCADE;


--
-- TOC entry 4239 (class 2606 OID 1422335)
-- Name: chargesheet_files fk_chargesheet_files_chargesheet; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.chargesheet_files
    ADD CONSTRAINT fk_chargesheet_files_chargesheet FOREIGN KEY (chargesheet_id) REFERENCES public.chargesheets(id) ON DELETE CASCADE;


--
-- TOC entry 4232 (class 2606 OID 1413505)
-- Name: old_interragation_report fk_crime; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.old_interragation_report
    ADD CONSTRAINT fk_crime FOREIGN KEY (crime_id) REFERENCES public.crimes(crime_id) ON DELETE CASCADE;


--
-- TOC entry 4230 (class 2606 OID 1404615)
-- Name: brief_facts_crime_summaries fk_summaries_crime; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.brief_facts_crime_summaries
    ADD CONSTRAINT fk_summaries_crime FOREIGN KEY (crime_id) REFERENCES public.crimes(crime_id) ON DELETE CASCADE;


--
-- TOC entry 4245 (class 2606 OID 1639527)
-- Name: fsl_case_property fsl_case_property_crime_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.fsl_case_property
    ADD CONSTRAINT fsl_case_property_crime_id_fkey FOREIGN KEY (crime_id) REFERENCES public.crimes(crime_id);


--
-- TOC entry 4246 (class 2606 OID 1639539)
-- Name: fsl_case_property_media fsl_case_property_media_case_property_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.fsl_case_property_media
    ADD CONSTRAINT fsl_case_property_media_case_property_id_fkey FOREIGN KEY (case_property_id) REFERENCES public.fsl_case_property(case_property_id) ON DELETE CASCADE;


--
-- TOC entry 4214 (class 2606 OID 1397648)
-- Name: interrogation_reports interrogation_reports_crime_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.interrogation_reports
    ADD CONSTRAINT interrogation_reports_crime_id_fkey FOREIGN KEY (crime_id) REFERENCES public.crimes(crime_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4225 (class 2606 OID 1397805)
-- Name: ir_associate_details ir_associate_details_ir_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_associate_details
    ADD CONSTRAINT ir_associate_details_ir_fkey FOREIGN KEY (interrogation_report_id) REFERENCES public.interrogation_reports(interrogation_report_id) ON DELETE CASCADE;


--
-- TOC entry 4221 (class 2606 OID 1397749)
-- Name: ir_consumer_details ir_consumer_details_ir_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_consumer_details
    ADD CONSTRAINT ir_consumer_details_ir_fkey FOREIGN KEY (interrogation_report_id) REFERENCES public.interrogation_reports(interrogation_report_id) ON DELETE CASCADE;


--
-- TOC entry 4224 (class 2606 OID 1397791)
-- Name: ir_defence_counsel ir_defence_counsel_ir_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_defence_counsel
    ADD CONSTRAINT ir_defence_counsel_ir_fkey FOREIGN KEY (interrogation_report_id) REFERENCES public.interrogation_reports(interrogation_report_id) ON DELETE CASCADE;


--
-- TOC entry 4229 (class 2606 OID 1397865)
-- Name: ir_dopams_links ir_dopams_links_ir_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_dopams_links
    ADD CONSTRAINT ir_dopams_links_ir_fkey FOREIGN KEY (interrogation_report_id) REFERENCES public.interrogation_reports(interrogation_report_id) ON DELETE CASCADE;


--
-- TOC entry 4215 (class 2606 OID 1397665)
-- Name: ir_family_history ir_family_history_ir_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_family_history
    ADD CONSTRAINT ir_family_history_ir_fkey FOREIGN KEY (interrogation_report_id) REFERENCES public.interrogation_reports(interrogation_report_id) ON DELETE CASCADE;


--
-- TOC entry 4220 (class 2606 OID 1397735)
-- Name: ir_financial_history ir_financial_history_ir_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_financial_history
    ADD CONSTRAINT ir_financial_history_ir_fkey FOREIGN KEY (interrogation_report_id) REFERENCES public.interrogation_reports(interrogation_report_id) ON DELETE CASCADE;


--
-- TOC entry 4228 (class 2606 OID 1397851)
-- Name: ir_interrogation_report_refs ir_interrogation_report_refs_ir_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_interrogation_report_refs
    ADD CONSTRAINT ir_interrogation_report_refs_ir_fkey FOREIGN KEY (interrogation_report_id) REFERENCES public.interrogation_reports(interrogation_report_id) ON DELETE CASCADE;


--
-- TOC entry 4216 (class 2606 OID 1397679)
-- Name: ir_local_contacts ir_local_contacts_ir_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_local_contacts
    ADD CONSTRAINT ir_local_contacts_ir_fkey FOREIGN KEY (interrogation_report_id) REFERENCES public.interrogation_reports(interrogation_report_id) ON DELETE CASCADE;


--
-- TOC entry 4227 (class 2606 OID 1397835)
-- Name: ir_media ir_media_ir_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_media
    ADD CONSTRAINT ir_media_ir_fkey FOREIGN KEY (interrogation_report_id) REFERENCES public.interrogation_reports(interrogation_report_id) ON DELETE CASCADE;


--
-- TOC entry 4222 (class 2606 OID 1397763)
-- Name: ir_modus_operandi ir_modus_operandi_ir_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_modus_operandi
    ADD CONSTRAINT ir_modus_operandi_ir_fkey FOREIGN KEY (interrogation_report_id) REFERENCES public.interrogation_reports(interrogation_report_id) ON DELETE CASCADE;


--
-- TOC entry 4223 (class 2606 OID 1397777)
-- Name: ir_previous_offences_confessed ir_previous_offences_ir_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_previous_offences_confessed
    ADD CONSTRAINT ir_previous_offences_ir_fkey FOREIGN KEY (interrogation_report_id) REFERENCES public.interrogation_reports(interrogation_report_id) ON DELETE CASCADE;


--
-- TOC entry 4217 (class 2606 OID 1397693)
-- Name: ir_regular_habits ir_regular_habits_ir_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_regular_habits
    ADD CONSTRAINT ir_regular_habits_ir_fkey FOREIGN KEY (interrogation_report_id) REFERENCES public.interrogation_reports(interrogation_report_id) ON DELETE CASCADE;


--
-- TOC entry 4226 (class 2606 OID 1397819)
-- Name: ir_shelter ir_shelter_ir_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_shelter
    ADD CONSTRAINT ir_shelter_ir_fkey FOREIGN KEY (interrogation_report_id) REFERENCES public.interrogation_reports(interrogation_report_id) ON DELETE CASCADE;


--
-- TOC entry 4219 (class 2606 OID 1397721)
-- Name: ir_sim_details ir_sim_details_ir_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_sim_details
    ADD CONSTRAINT ir_sim_details_ir_fkey FOREIGN KEY (interrogation_report_id) REFERENCES public.interrogation_reports(interrogation_report_id) ON DELETE CASCADE;


--
-- TOC entry 4218 (class 2606 OID 1397707)
-- Name: ir_types_of_drugs ir_types_of_drugs_ir_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_types_of_drugs
    ADD CONSTRAINT ir_types_of_drugs_ir_fkey FOREIGN KEY (interrogation_report_id) REFERENCES public.interrogation_reports(interrogation_report_id) ON DELETE CASCADE;


--
-- TOC entry 4236 (class 2606 OID 1420940)
-- Name: mo_seizures mo_seizures_crime_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.mo_seizures
    ADD CONSTRAINT mo_seizures_crime_id_fkey FOREIGN KEY (crime_id) REFERENCES public.crimes(crime_id);


--
-- TOC entry 4213 (class 2606 OID 1397629)
-- Name: properties properties_crime_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.properties
    ADD CONSTRAINT properties_crime_id_fkey FOREIGN KEY (crime_id) REFERENCES public.crimes(crime_id) ON DELETE CASCADE;


--
-- TOC entry 4405 (class 0 OID 0)
-- Dependencies: 8
-- Name: SCHEMA public; Type: ACL; Schema: -; Owner: pg_database_owner
--

GRANT ALL ON SCHEMA public TO dev_dopamas;


--
-- TOC entry 4415 (class 0 OID 0)
-- Dependencies: 267
-- Name: TABLE brief_facts_accused; Type: ACL; Schema: public; Owner: dev_dopamas
--

GRANT SELECT ON TABLE public.brief_facts_accused TO readonly_userdev;
GRANT SELECT ON TABLE public.brief_facts_accused TO dopamas_chat_ur;


--
-- TOC entry 2586 (class 826 OID 1397567)
-- Name: DEFAULT PRIVILEGES FOR SEQUENCES; Type: DEFAULT ACL; Schema: public; Owner: dopamasprd_ur
--

ALTER DEFAULT PRIVILEGES FOR ROLE dopamasprd_ur IN SCHEMA public GRANT ALL ON SEQUENCES TO dev_dopamas;


--
-- TOC entry 2585 (class 826 OID 1397566)
-- Name: DEFAULT PRIVILEGES FOR TABLES; Type: DEFAULT ACL; Schema: public; Owner: dopamasprd_ur
--

ALTER DEFAULT PRIVILEGES FOR ROLE dopamasprd_ur IN SCHEMA public GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLES TO dev_dopamas;


-- Completed on 2026-03-23 10:50:46

--
-- PostgreSQL database dump complete
--

\unrestrict shGvuVMkHJiy4Un0LrsqGanbwhYgfMiYEacuiSphbthihbzVho3Ab9DISZUwdFE

