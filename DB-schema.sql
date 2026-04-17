--
-- PostgreSQL database dump
--


-- Dumped from database version 16.11 (Ubuntu 16.11-1.pgdg24.04+1)
-- Dumped by pg_dump version 17.6

-- Started on 2026-04-16 17:16:37

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
-- TOC entry 23 (class 2615 OID 39131797)
-- Name: public; Type: SCHEMA; Schema: -; Owner: dev_dopamas
--

-- *not* creating schema, since initdb creates it


ALTER SCHEMA public OWNER TO dev_dopamas;

--
-- TOC entry 4312 (class 0 OID 0)
-- Dependencies: 23
-- Name: SCHEMA public; Type: COMMENT; Schema: -; Owner: dev_dopamas
--

COMMENT ON SCHEMA public IS '';


--
-- TOC entry 7 (class 3079 OID 39133043)
-- Name: dblink; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS dblink WITH SCHEMA public;


--
-- TOC entry 4314 (class 0 OID 0)
-- Dependencies: 7
-- Name: EXTENSION dblink; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION dblink IS 'connect to other PostgreSQL databases from within a database';


--
-- TOC entry 2 (class 3079 OID 39131798)
-- Name: fuzzystrmatch; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS fuzzystrmatch WITH SCHEMA public;


--
-- TOC entry 4315 (class 0 OID 0)
-- Dependencies: 2
-- Name: EXTENSION fuzzystrmatch; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION fuzzystrmatch IS 'determine similarities and distance between strings';


--
-- TOC entry 3 (class 3079 OID 39131810)
-- Name: pg_trgm; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;


--
-- TOC entry 4316 (class 0 OID 0)
-- Dependencies: 3
-- Name: EXTENSION pg_trgm; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION pg_trgm IS 'text similarity measurement and index searching based on trigrams';


--
-- TOC entry 4 (class 3079 OID 39131891)
-- Name: pgcrypto; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;


--
-- TOC entry 4317 (class 0 OID 0)
-- Dependencies: 4
-- Name: EXTENSION pgcrypto; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION pgcrypto IS 'cryptographic functions';


--
-- TOC entry 5 (class 3079 OID 39131928)
-- Name: uuid-ossp; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public;


--
-- TOC entry 4318 (class 0 OID 0)
-- Dependencies: 5
-- Name: EXTENSION "uuid-ossp"; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION "uuid-ossp" IS 'generate universally unique identifiers (UUIDs)';


--
-- TOC entry 6 (class 3079 OID 39131939)
-- Name: vector; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public;


--
-- TOC entry 4319 (class 0 OID 0)
-- Dependencies: 6
-- Name: EXTENSION vector; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION vector IS 'vector data type and ivfflat and hnsw access methods';


--
-- TOC entry 1349 (class 1247 OID 39132268)
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
-- TOC entry 1352 (class 1247 OID 39132284)
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
-- TOC entry 538 (class 1255 OID 39132299)
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
-- TOC entry 539 (class 1255 OID 39132300)
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
-- TOC entry 540 (class 1255 OID 39132301)
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
-- TOC entry 541 (class 1255 OID 39132302)
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
-- TOC entry 542 (class 1255 OID 39132303)
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
-- TOC entry 4320 (class 0 OID 0)
-- Dependencies: 542
-- Name: FUNCTION get_accused_crime_history(target_accused_id character varying); Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON FUNCTION public.get_accused_crime_history(target_accused_id character varying) IS 'Get complete crime history for an accused by accused_id, includes all cases across duplicate records';


--
-- TOC entry 543 (class 1255 OID 39132304)
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
-- TOC entry 4321 (class 0 OID 0)
-- Dependencies: 543
-- Name: FUNCTION get_person_crime_history(target_person_id character varying); Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON FUNCTION public.get_person_crime_history(target_person_id character varying) IS 'Get complete crime history for a person by person_id, shows all duplicate person records';


--
-- TOC entry 544 (class 1255 OID 39132305)
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
-- TOC entry 4322 (class 0 OID 0)
-- Dependencies: 544
-- Name: FUNCTION search_person_by_name(search_name character varying); Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON FUNCTION public.search_person_by_name(search_name character varying) IS 'Search for persons by name, returns deduplicated results with crime counts';


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 233 (class 1259 OID 39132306)
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
-- TOC entry 4323 (class 0 OID 0)
-- Dependencies: 233
-- Name: TABLE accused; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.accused IS 'Links persons to crimes as accused with physical features';


--
-- TOC entry 4324 (class 0 OID 0)
-- Dependencies: 233
-- Name: COLUMN accused.person_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.accused.person_id IS 'Can be NULL - stub persons are created by ETL when needed';


--
-- TOC entry 4325 (class 0 OID 0)
-- Dependencies: 233
-- Name: COLUMN accused.is_ccl; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.accused.is_ccl IS 'Is Child in Conflict with Law';


--
-- TOC entry 234 (class 1259 OID 39132313)
-- Name: brief_facts_ai; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.brief_facts_ai (
    bf_accused_id uuid DEFAULT gen_random_uuid() NOT NULL,
    crime_id character varying(50) NOT NULL,
    accused_id character varying(50),
    person_id character varying(50),
    canonical_person_id character varying(50),
    person_code character varying(50),
    seq_num character varying(50),
    existing_accused boolean DEFAULT false NOT NULL,
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
    drugs jsonb,
    dedup_match_tier smallint,
    dedup_confidence numeric(3,2),
    dedup_review_flag boolean DEFAULT false,
    source_person_fields jsonb,
    source_accused_fields jsonb,
    source_summary_fields jsonb,
    etl_run_id uuid NOT NULL,
    date_created timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    date_modified timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT brief_facts_ai_accused_type_check CHECK (((accused_type IS NULL) OR ((accused_type)::text = ANY (ARRAY[('peddler'::character varying)::text, ('consumer'::character varying)::text, ('supplier'::character varying)::text, ('harbourer'::character varying)::text, ('organizer_kingpin'::character varying)::text, ('processor'::character varying)::text, ('financier'::character varying)::text, ('manufacturer'::character varying)::text, ('transporter'::character varying)::text, ('producer'::character varying)::text])))),
    CONSTRAINT brief_facts_ai_dedup_tier_check CHECK (((dedup_match_tier IS NULL) OR (dedup_match_tier = ANY (ARRAY[1, 2, 3]))))
);


ALTER TABLE public.brief_facts_ai OWNER TO dev_dopamas;

--
-- TOC entry 235 (class 1259 OID 39132325)
-- Name: brief_facts_ai_accused_flat; Type: VIEW; Schema: public; Owner: dev_dopamas
--

CREATE VIEW public.brief_facts_ai_accused_flat AS
 SELECT bf_accused_id,
    crime_id,
    accused_id,
    person_id,
    person_code,
    seq_num,
    full_name,
    alias_name,
    age,
    gender,
    occupation,
    address,
    phone_numbers,
    role_in_crime,
    key_details,
    accused_type,
    status,
    is_ccl,
    source_person_fields,
    source_accused_fields,
    source_summary_fields,
    date_created,
    date_modified,
    existing_accused
   FROM public.brief_facts_ai bfa;


ALTER VIEW public.brief_facts_ai_accused_flat OWNER TO dev_dopamas;

--
-- TOC entry 310 (class 1259 OID 39606096)
-- Name: brief_facts_ai_drug_flat; Type: VIEW; Schema: public; Owner: dev_dopamas
--

CREATE VIEW public.brief_facts_ai_drug_flat AS
 SELECT public.uuid_generate_v5('00000000-0000-0000-0000-000000000000'::uuid, (((bfa.bf_accused_id)::text || ':'::text) || (x.ord)::text)) AS id,
    bfa.crime_id,
    bfa.bf_accused_id,
    (x.d ->> 'raw_drug_name'::text) AS raw_drug_name,
    (x.d ->> 'primary_drug_name'::text) AS primary_drug_name,
    (x.d ->> 'drug_form'::text) AS drug_form,
    (x.d ->> 'drug_category'::text) AS drug_category,
    (x.d ->> 'supplier_name'::text) AS supplier_name,
    (x.d ->> 'source_location'::text) AS source_location,
    (x.d ->> 'destination'::text) AS destination,
    (NULLIF((x.d ->> 'raw_quantity'::text), ''::text))::numeric(18,6) AS raw_quantity,
    (x.d ->> 'raw_unit'::text) AS raw_unit,
    (NULLIF((x.d ->> 'weight_g'::text), ''::text))::numeric(18,6) AS weight_g,
    (NULLIF((x.d ->> 'weight_kg'::text), ''::text))::numeric(18,6) AS weight_kg,
    (NULLIF((x.d ->> 'volume_ml'::text), ''::text))::numeric(18,6) AS volume_ml,
    (NULLIF((x.d ->> 'volume_l'::text), ''::text))::numeric(18,6) AS volume_l,
    (NULLIF((x.d ->> 'count_total'::text), ''::text))::numeric(18,6) AS count_total,
    (NULLIF((x.d ->> 'confidence_score'::text), ''::text))::numeric(3,2) AS confidence_score,
    COALESCE(((x.d ->> 'is_commercial'::text))::boolean, false) AS is_commercial,
    (NULLIF((x.d ->> 'seizure_worth'::text), ''::text))::numeric AS seizure_worth,
    (NULLIF((x.d ->> 'purchase_price_per_unit'::text), ''::text))::numeric AS purchase_price_per_unit,
    (x.d ->> 'drug_attribution_source'::text) AS drug_attribution_source,
    COALESCE((x.d -> 'extraction_metadata'::text), '{}'::jsonb) AS extraction_metadata,
    (bfa.date_created)::timestamp with time zone AS created_at,
    (bfa.date_modified)::timestamp with time zone AS updated_at
   FROM (public.brief_facts_ai bfa
     CROSS JOIN LATERAL jsonb_array_elements(COALESCE(bfa.drugs, '[]'::jsonb)) WITH ORDINALITY x(d, ord));


ALTER VIEW public.brief_facts_ai_drug_flat OWNER TO dev_dopamas;

--
-- TOC entry 236 (class 1259 OID 39132335)
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
-- TOC entry 4326 (class 0 OID 0)
-- Dependencies: 236
-- Name: TABLE crimes; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.crimes IS 'Crime/FIR records registered at police stations';


--
-- TOC entry 4327 (class 0 OID 0)
-- Dependencies: 236
-- Name: COLUMN crimes.brief_facts; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.crimes.brief_facts IS 'Detailed description of the crime incident';


--
-- TOC entry 237 (class 1259 OID 39132340)
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
-- TOC entry 238 (class 1259 OID 39132346)
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
-- TOC entry 4328 (class 0 OID 0)
-- Dependencies: 238
-- Name: TABLE hierarchy; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.hierarchy IS 'Police organizational hierarchy from ADG to Police Station in single table';


--
-- TOC entry 239 (class 1259 OID 39132351)
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
    raw_full_name character varying(500),
    gender_confidence numeric(4,3),
    gender_source character varying(20),
    phone_numbers character varying(255)
);


ALTER TABLE public.persons OWNER TO dev_dopamas;

--
-- TOC entry 4329 (class 0 OID 0)
-- Dependencies: 239
-- Name: TABLE persons; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.persons IS 'Personal details of individuals (accused, victims, witnesses, etc.)';


--
-- TOC entry 311 (class 1259 OID 39606101)
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
             LEFT JOIN public.brief_facts_ai_accused_flat bfa2 ON (((a2.accused_id)::text = (bfa2.accused_id)::text)))
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
           FROM public.brief_facts_ai_accused_flat bfa_c
          WHERE ((bfa_c.accused_id)::text = (a.accused_id)::text)) AS "noOfCrimes",
    ( SELECT jsonb_agg(DISTINCT jsonb_build_object('crimeId', c2.crime_id, 'firNumber', c2.fir_num)) AS jsonb_agg
           FROM (public.accused a4
             JOIN public.crimes c2 ON (((a4.crime_id)::text = (c2.crime_id)::text)))
          WHERE ((a4.person_id)::text = (p.person_id)::text)) AS "previouslyInvolvedCases",
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
     JOIN public.accused a ON (((bfa.accused_id)::text = (a.accused_id)::text)))
     JOIN public.crimes c ON (((a.crime_id)::text = (c.crime_id)::text)))
     JOIN public.hierarchy h ON (((c.ps_code)::text = (h.ps_code)::text)))
     LEFT JOIN public.persons p ON (((a.person_id)::text = (p.person_id)::text)))
  WITH NO DATA;


ALTER MATERIALIZED VIEW public.accuseds_mv OWNER TO dev_dopamas;

--
-- TOC entry 312 (class 1259 OID 39606108)
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


ALTER MATERIALIZED VIEW public.advanced_search_accuseds_mv OWNER TO dev_dopamas;

--
-- TOC entry 240 (class 1259 OID 39132371)
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
-- TOC entry 241 (class 1259 OID 39132382)
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
-- TOC entry 313 (class 1259 OID 39606115)
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


ALTER MATERIALIZED VIEW public.advanced_search_firs_mv OWNER TO dev_dopamas;

--
-- TOC entry 242 (class 1259 OID 39132394)
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
-- TOC entry 243 (class 1259 OID 39132402)
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
-- TOC entry 4330 (class 0 OID 0)
-- Dependencies: 243
-- Name: agent_deduplication_tracker_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.agent_deduplication_tracker_id_seq OWNED BY public.agent_deduplication_tracker.id;


--
-- TOC entry 244 (class 1259 OID 39132403)
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
-- TOC entry 245 (class 1259 OID 39132409)
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
-- TOC entry 246 (class 1259 OID 39132418)
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
-- TOC entry 247 (class 1259 OID 39132426)
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
-- TOC entry 248 (class 1259 OID 39132433)
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
-- TOC entry 4331 (class 0 OID 0)
-- Dependencies: 248
-- Name: TABLE charge_sheet_updates; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.charge_sheet_updates IS 'Stores charge sheet update records from DOPAMS API. Each record represents a charge sheet update with its status and court filing information.';


--
-- TOC entry 4332 (class 0 OID 0)
-- Dependencies: 248
-- Name: COLUMN charge_sheet_updates.update_charge_sheet_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.charge_sheet_updates.update_charge_sheet_id IS 'Unique identifier from the API (MongoDB ObjectId format) - REQUIRED';


--
-- TOC entry 4333 (class 0 OID 0)
-- Dependencies: 248
-- Name: COLUMN charge_sheet_updates.crime_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.charge_sheet_updates.crime_id IS 'Reference to the crime record (MongoDB ObjectId format) - REQUIRED, Foreign Key to crimes(crime_id)';


--
-- TOC entry 4334 (class 0 OID 0)
-- Dependencies: 248
-- Name: COLUMN charge_sheet_updates.charge_sheet_no; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.charge_sheet_updates.charge_sheet_no IS 'Charge sheet number (e.g., "146/2024") - NULLABLE';


--
-- TOC entry 4335 (class 0 OID 0)
-- Dependencies: 248
-- Name: COLUMN charge_sheet_updates.charge_sheet_date; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.charge_sheet_updates.charge_sheet_date IS 'Date when the charge sheet was created - NULLABLE';


--
-- TOC entry 4336 (class 0 OID 0)
-- Dependencies: 248
-- Name: COLUMN charge_sheet_updates.charge_sheet_status; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.charge_sheet_updates.charge_sheet_status IS 'Current status of the charge sheet (e.g., "Taken on File", "Filed/Check And Put Up") - NULLABLE';


--
-- TOC entry 4337 (class 0 OID 0)
-- Dependencies: 248
-- Name: COLUMN charge_sheet_updates.taken_on_file_date; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.charge_sheet_updates.taken_on_file_date IS 'Date when the charge sheet was taken on file by the court - NULLABLE';


--
-- TOC entry 4338 (class 0 OID 0)
-- Dependencies: 248
-- Name: COLUMN charge_sheet_updates.taken_on_file_case_type; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.charge_sheet_updates.taken_on_file_case_type IS 'Type of case (e.g., "SC", "CC", "NDPS", "SC NDPS") - NULLABLE';


--
-- TOC entry 4339 (class 0 OID 0)
-- Dependencies: 248
-- Name: COLUMN charge_sheet_updates.taken_on_file_court_case_no; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.charge_sheet_updates.taken_on_file_court_case_no IS 'Court case number assigned when taken on file - NULLABLE';


--
-- TOC entry 4340 (class 0 OID 0)
-- Dependencies: 248
-- Name: COLUMN charge_sheet_updates.date_created; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.charge_sheet_updates.date_created IS 'Timestamp when the record was created in the API system (from API response) - NULLABLE';


--
-- TOC entry 4341 (class 0 OID 0)
-- Dependencies: 248
-- Name: COLUMN charge_sheet_updates.date_modified; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.charge_sheet_updates.date_modified IS 'Timestamp when the update record was last modified in the API system.';


--
-- TOC entry 249 (class 1259 OID 39132436)
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
-- TOC entry 4342 (class 0 OID 0)
-- Dependencies: 249
-- Name: charge_sheet_updates_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.charge_sheet_updates_id_seq OWNED BY public.charge_sheet_updates.id;


--
-- TOC entry 250 (class 1259 OID 39132437)
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
-- TOC entry 251 (class 1259 OID 39132445)
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
-- TOC entry 252 (class 1259 OID 39132452)
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
-- TOC entry 4343 (class 0 OID 0)
-- Dependencies: 252
-- Name: TABLE chargesheet_acts_sections; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.chargesheet_acts_sections IS 'Normalized sections for chargesheets. One row per section entry extracted from actsAndSections[].';


--
-- TOC entry 4344 (class 0 OID 0)
-- Dependencies: 252
-- Name: COLUMN chargesheet_acts_sections.chargesheet_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.chargesheet_acts_sections.chargesheet_id IS 'API chargeSheetId used as the logical parent key.';


--
-- TOC entry 253 (class 1259 OID 39132461)
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
-- TOC entry 254 (class 1259 OID 39132465)
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
-- TOC entry 4345 (class 0 OID 0)
-- Dependencies: 254
-- Name: TABLE chargesheet_media; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.chargesheet_media IS 'Normalized media references for chargesheets. One row per uploadChargeSheet item.';


--
-- TOC entry 4346 (class 0 OID 0)
-- Dependencies: 254
-- Name: COLUMN chargesheet_media.chargesheet_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.chargesheet_media.chargesheet_id IS 'API chargeSheetId used as the logical parent key.';


--
-- TOC entry 4347 (class 0 OID 0)
-- Dependencies: 254
-- Name: COLUMN chargesheet_media.file_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.chargesheet_media.file_id IS 'uploadChargeSheet.fileId from the API payload.';


--
-- TOC entry 255 (class 1259 OID 39132472)
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
-- TOC entry 4348 (class 0 OID 0)
-- Dependencies: 255
-- Name: COLUMN chargesheets.charge_sheet_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.chargesheets.charge_sheet_id IS 'API chargeSheetId. Natural key used by the chargesheets ETL for overwrite semantics.';


--
-- TOC entry 256 (class 1259 OID 39132480)
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
-- TOC entry 4349 (class 0 OID 0)
-- Dependencies: 256
-- Name: TABLE files; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.files IS 'Stores file references (UUIDs) from various sources (crimes, interrogations, properties, persons)';


--
-- TOC entry 4350 (class 0 OID 0)
-- Dependencies: 256
-- Name: COLUMN files.source_type; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.files.source_type IS 'Type of source: crime, interrogation, property, or person';


--
-- TOC entry 4351 (class 0 OID 0)
-- Dependencies: 256
-- Name: COLUMN files.source_field; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.files.source_field IS 'Field name from source: FIR_COPY, MEDIA, INTERROGATION_REPORT, DOPAMS_DATA, IDENTITY_DETAILS';


--
-- TOC entry 4352 (class 0 OID 0)
-- Dependencies: 256
-- Name: COLUMN files.parent_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.files.parent_id IS 'ID of the parent record (crime_id, interrogation_report_id, property_id, or person_id)';


--
-- TOC entry 4353 (class 0 OID 0)
-- Dependencies: 256
-- Name: COLUMN files.file_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.files.file_id IS 'The actual file UUID that can be used to fetch the file via API. NULL if field exists but has no file.';


--
-- TOC entry 4354 (class 0 OID 0)
-- Dependencies: 256
-- Name: COLUMN files.has_field; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.files.has_field IS 'TRUE if the field exists in API response, FALSE if field is missing';


--
-- TOC entry 4355 (class 0 OID 0)
-- Dependencies: 256
-- Name: COLUMN files.is_empty; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.files.is_empty IS 'TRUE if field exists but is null or empty array';


--
-- TOC entry 4356 (class 0 OID 0)
-- Dependencies: 256
-- Name: COLUMN files.file_path; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.files.file_path IS 'Relative file path on Tomcat server (auto-generated, NULL if file_id is NULL)';


--
-- TOC entry 4357 (class 0 OID 0)
-- Dependencies: 256
-- Name: COLUMN files.file_url; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.files.file_url IS 'Full file URL on Tomcat server (auto-generated, NULL if file_id is NULL)';


--
-- TOC entry 4358 (class 0 OID 0)
-- Dependencies: 256
-- Name: COLUMN files.file_index; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.files.file_index IS 'Index position in array (for MEDIA arrays with multiple files)';


--
-- TOC entry 4359 (class 0 OID 0)
-- Dependencies: 256
-- Name: COLUMN files.identity_type; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.files.identity_type IS 'For IDENTITY_DETAILS: type of identity document (Aadhar Card, Passport, etc.)';


--
-- TOC entry 4360 (class 0 OID 0)
-- Dependencies: 256
-- Name: COLUMN files.identity_number; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.files.identity_number IS 'For IDENTITY_DETAILS: identity document number';


--
-- TOC entry 4361 (class 0 OID 0)
-- Dependencies: 256
-- Name: COLUMN files.downloaded_at; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.files.downloaded_at IS 'Timestamp when file was successfully downloaded to media server';


--
-- TOC entry 4362 (class 0 OID 0)
-- Dependencies: 256
-- Name: COLUMN files.is_downloaded; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.files.is_downloaded IS 'Flag indicating if file has been successfully downloaded to media server';


--
-- TOC entry 4363 (class 0 OID 0)
-- Dependencies: 256
-- Name: COLUMN files.download_error; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.files.download_error IS 'Error message if file download failed';


--
-- TOC entry 4364 (class 0 OID 0)
-- Dependencies: 256
-- Name: COLUMN files.download_attempts; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.files.download_attempts IS 'Number of download attempts made';


--
-- TOC entry 4365 (class 0 OID 0)
-- Dependencies: 256
-- Name: COLUMN files.created_at; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.files.created_at IS 'Timestamp from API (DATE_CREATED or DATE_MODIFIED)';


--
-- TOC entry 314 (class 1259 OID 39606122)
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
                     LEFT JOIN public.brief_facts_ai bfa ON (((bfa.accused_id)::text = (a.accused_id)::text)))
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
             JOIN public.brief_facts_ai bfa ON (((bfa.accused_id)::text = (a.accused_id)::text)))
          WHERE ((a.person_id)::text = (p.person_id)::text)) AS "noOfCrimes",
    ( SELECT count(*) AS count
           FROM public.arrests arr
          WHERE (((arr.person_id)::text = (p.person_id)::text) AND (arr.is_arrested = true))) AS "arrestCount",
    ( SELECT max(c.fir_date) AS max
           FROM ((public.accused a
             JOIN public.crimes c ON (((a.crime_id)::text = (c.crime_id)::text)))
             LEFT JOIN public.brief_facts_ai bfa ON (((bfa.accused_id)::text = (a.accused_id)::text)))
          WHERE (((a.person_id)::text = (p.person_id)::text) AND (((COALESCE(bfa.status, a.accused_status) ~~* 'Arrest%'::text) AND (COALESCE(bfa.status, a.accused_status) !~~* 'Arrest Related%'::text)) OR (COALESCE(bfa.status, a.accused_status) ~~* 'Surrendered%'::text)))) AS "lastArrestDate",
    ( SELECT jsonb_agg(DISTINCT jsonb_build_object('crimeId', bfa.crime_id, 'accusedId', bfa.accused_id, 'accusedRole', bfa.accused_type)) AS jsonb_agg
           FROM (public.accused a
             JOIN public.brief_facts_ai bfa ON (((bfa.accused_id)::text = (a.accused_id)::text)))
          WHERE ((a.person_id)::text = (p.person_id)::text)) AS "crimesInvolved",
    ( SELECT array_agg(DISTINCT bfa.accused_type) FILTER (WHERE (bfa.accused_type IS NOT NULL)) AS array_agg
           FROM (public.accused a
             JOIN public.brief_facts_ai bfa ON (((bfa.accused_id)::text = (a.accused_id)::text)))
          WHERE ((a.person_id)::text = (p.person_id)::text)) AS "accusedRoles",
    ( SELECT jsonb_agg(DISTINCT jsonb_build_object('id', c.crime_id, 'value', c.fir_num)) AS jsonb_agg
           FROM (public.accused a
             JOIN public.crimes c ON (((a.crime_id)::text = (c.crime_id)::text)))
          WHERE ((a.person_id)::text = (p.person_id)::text)) AS "previouslyInvolvedCases",
    ( SELECT COALESCE(array_agg(DISTINCT upper(TRIM(BOTH FROM (drug.value ->> 'primary_drug_name'::text)))) FILTER (WHERE (((drug.value ->> 'primary_drug_name'::text) IS NOT NULL) AND ((drug.value ->> 'primary_drug_name'::text) <> 'NO_DRUGS_DETECTED'::text))), ARRAY[]::text[]) AS "coalesce"
           FROM (public.accused a_drug
             JOIN public.brief_facts_ai bfa ON (((bfa.crime_id)::text = (a_drug.crime_id)::text))),
            LATERAL jsonb_array_elements(bfa.drugs) drug(value)
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
-- TOC entry 257 (class 1259 OID 39132498)
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
-- TOC entry 258 (class 1259 OID 39132503)
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
-- TOC entry 4366 (class 0 OID 0)
-- Dependencies: 258
-- Name: dedup_cluster_state_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.dedup_cluster_state_id_seq OWNED BY public.dedup_cluster_state.id;


--
-- TOC entry 259 (class 1259 OID 39132504)
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
-- TOC entry 260 (class 1259 OID 39132509)
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
-- TOC entry 4367 (class 0 OID 0)
-- Dependencies: 260
-- Name: dedup_comparison_progress_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.dedup_comparison_progress_id_seq OWNED BY public.dedup_comparison_progress.id;


--
-- TOC entry 261 (class 1259 OID 39132510)
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
-- TOC entry 262 (class 1259 OID 39132517)
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
-- TOC entry 4368 (class 0 OID 0)
-- Dependencies: 262
-- Name: dedup_run_metadata_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.dedup_run_metadata_id_seq OWNED BY public.dedup_run_metadata.id;


--
-- TOC entry 263 (class 1259 OID 39132518)
-- Name: drug_categories; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.drug_categories (
    id integer NOT NULL,
    raw_name text NOT NULL,
    standard_name text NOT NULL,
    category_group text NOT NULL,
    is_verified boolean DEFAULT true,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    raw_name_clean text
);


ALTER TABLE public.drug_categories OWNER TO dev_dopamas;

--
-- TOC entry 264 (class 1259 OID 39132525)
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
-- TOC entry 4369 (class 0 OID 0)
-- Dependencies: 264
-- Name: drug_categories_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.drug_categories_id_seq OWNED BY public.drug_categories.id;


--
-- TOC entry 265 (class 1259 OID 39132526)
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
-- TOC entry 266 (class 1259 OID 39132532)
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
-- TOC entry 4370 (class 0 OID 0)
-- Dependencies: 266
-- Name: drug_ignore_list_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.drug_ignore_list_id_seq OWNED BY public.drug_ignore_list.id;


--
-- TOC entry 267 (class 1259 OID 39132533)
-- Name: etl_crime_processing_log; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.etl_crime_processing_log (
    run_id uuid DEFAULT gen_random_uuid() NOT NULL,
    crime_id character varying(50) NOT NULL,
    status character varying(20) DEFAULT 'in_progress'::character varying NOT NULL,
    accused_count_written integer,
    started_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    completed_at timestamp without time zone,
    error_detail text,
    CONSTRAINT etl_crime_processing_log_status_check CHECK (((status)::text = ANY ((ARRAY['in_progress'::character varying, 'complete'::character varying, 'failed'::character varying, 'stale'::character varying])::text[])))
);


ALTER TABLE public.etl_crime_processing_log OWNER TO dev_dopamas;

--
-- TOC entry 316 (class 1259 OID 39606141)
-- Name: etl_fk_retry_queue; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.etl_fk_retry_queue (
    queue_id bigint NOT NULL,
    source_table character varying(100) NOT NULL,
    record_id text NOT NULL,
    record_json jsonb NOT NULL,
    missing_fk_column character varying(100) NOT NULL,
    missing_fk_value text NOT NULL,
    attempt_count integer DEFAULT 0 NOT NULL,
    last_attempted_at timestamp with time zone,
    first_failed_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    resolved boolean DEFAULT false NOT NULL,
    error_detail text
);


ALTER TABLE public.etl_fk_retry_queue OWNER TO dev_dopamas;

--
-- TOC entry 315 (class 1259 OID 39606140)
-- Name: etl_fk_retry_queue_queue_id_seq; Type: SEQUENCE; Schema: public; Owner: dev_dopamas
--

CREATE SEQUENCE public.etl_fk_retry_queue_queue_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.etl_fk_retry_queue_queue_id_seq OWNER TO dev_dopamas;

--
-- TOC entry 4371 (class 0 OID 0)
-- Dependencies: 315
-- Name: etl_fk_retry_queue_queue_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.etl_fk_retry_queue_queue_id_seq OWNED BY public.etl_fk_retry_queue.queue_id;


--
-- TOC entry 317 (class 1259 OID 39606153)
-- Name: etl_run_state; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.etl_run_state (
    module_name character varying(100) NOT NULL,
    last_successful_end character varying(30) NOT NULL,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.etl_run_state OWNER TO dev_dopamas;

--
-- TOC entry 268 (class 1259 OID 39132542)
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
-- TOC entry 269 (class 1259 OID 39132546)
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
-- TOC entry 4372 (class 0 OID 0)
-- Dependencies: 269
-- Name: TABLE fsl_case_property; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.fsl_case_property IS 'Main table storing case property records from DOPAMS API';


--
-- TOC entry 4373 (class 0 OID 0)
-- Dependencies: 269
-- Name: COLUMN fsl_case_property.case_property_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.fsl_case_property.case_property_id IS 'Primary key from API (CASE_PROPERTY_ID) - MongoDB ObjectId (24 hex characters)';


--
-- TOC entry 4374 (class 0 OID 0)
-- Dependencies: 269
-- Name: COLUMN fsl_case_property.crime_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.fsl_case_property.crime_id IS 'Reference to crime/case (CRIME_ID) - Foreign key to crimes table';


--
-- TOC entry 4375 (class 0 OID 0)
-- Dependencies: 269
-- Name: COLUMN fsl_case_property.mo_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.fsl_case_property.mo_id IS 'Material Object ID (MO_ID)';


--
-- TOC entry 4376 (class 0 OID 0)
-- Dependencies: 269
-- Name: COLUMN fsl_case_property.status; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.fsl_case_property.status IS 'Current status (e.g., Send To FSL, Send To Court)';


--
-- TOC entry 4377 (class 0 OID 0)
-- Dependencies: 269
-- Name: COLUMN fsl_case_property.date_created; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.fsl_case_property.date_created IS 'Record creation timestamp from API (DATE_CREATED)';


--
-- TOC entry 4378 (class 0 OID 0)
-- Dependencies: 269
-- Name: COLUMN fsl_case_property.date_modified; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.fsl_case_property.date_modified IS 'Record modification timestamp from API (DATE_MODIFIED)';


--
-- TOC entry 4379 (class 0 OID 0)
-- Dependencies: 269
-- Name: COLUMN fsl_case_property.fsl_no; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.fsl_case_property.fsl_no IS 'FSL case number';


--
-- TOC entry 4380 (class 0 OID 0)
-- Dependencies: 269
-- Name: COLUMN fsl_case_property.report_received; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.fsl_case_property.report_received IS 'Whether FSL report has been received';


--
-- TOC entry 4381 (class 0 OID 0)
-- Dependencies: 269
-- Name: COLUMN fsl_case_property.property_received_back; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.fsl_case_property.property_received_back IS 'Whether property has been received back';


--
-- TOC entry 272 (class 1259 OID 39132829)
-- Name: fsl_case_property_media; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.fsl_case_property_media (
    media_id uuid NOT NULL,
    case_property_id character varying(255) NOT NULL,
    file_id character varying(255)
);


ALTER TABLE public.fsl_case_property_media OWNER TO dev_dopamas;

--
-- TOC entry 273 (class 1259 OID 39132834)
-- Name: geo_countries; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.geo_countries (
    country_name text,
    state_name text,
    timezone text
);


ALTER TABLE public.geo_countries OWNER TO dev_dopamas;

--
-- TOC entry 274 (class 1259 OID 39132839)
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
-- TOC entry 309 (class 1259 OID 39133096)
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
-- TOC entry 4382 (class 0 OID 0)
-- Dependencies: 309
-- Name: geo_reference_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.geo_reference_id_seq OWNED BY public.geo_reference.id;


--
-- TOC entry 270 (class 1259 OID 39132551)
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
-- TOC entry 4383 (class 0 OID 0)
-- Dependencies: 270
-- Name: TABLE interrogation_reports; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.interrogation_reports IS 'Main table storing Interrogation Report (IR) data. All common fields are stored as columns for easy querying.';


--
-- TOC entry 271 (class 1259 OID 39132556)
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
-- TOC entry 4384 (class 0 OID 0)
-- Dependencies: 271
-- Name: TABLE ir_associate_details; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.ir_associate_details IS 'Associate information for each IR record. One record per associate.';


--
-- TOC entry 275 (class 1259 OID 39132845)
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
-- TOC entry 276 (class 1259 OID 39132850)
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
-- TOC entry 4385 (class 0 OID 0)
-- Dependencies: 276
-- Name: TABLE ir_conviction_acquittal; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.ir_conviction_acquittal IS 'Conviction/acquittal details for each IR record. One record per case verdict entry.';


--
-- TOC entry 4386 (class 0 OID 0)
-- Dependencies: 276
-- Name: COLUMN ir_conviction_acquittal.interrogation_report_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_conviction_acquittal.interrogation_report_id IS 'Foreign key to interrogation_reports table';


--
-- TOC entry 4387 (class 0 OID 0)
-- Dependencies: 276
-- Name: COLUMN ir_conviction_acquittal.crime_num; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_conviction_acquittal.crime_num IS 'Associated crime number';


--
-- TOC entry 4388 (class 0 OID 0)
-- Dependencies: 276
-- Name: COLUMN ir_conviction_acquittal.court_name; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_conviction_acquittal.court_name IS 'Court name where verdict was delivered';


--
-- TOC entry 4389 (class 0 OID 0)
-- Dependencies: 276
-- Name: COLUMN ir_conviction_acquittal.verdict; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_conviction_acquittal.verdict IS 'Verdict (Convicted, Acquitted, Discharged, etc.)';


--
-- TOC entry 4390 (class 0 OID 0)
-- Dependencies: 276
-- Name: COLUMN ir_conviction_acquittal.verdict_date; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_conviction_acquittal.verdict_date IS 'Date of verdict';


--
-- TOC entry 4391 (class 0 OID 0)
-- Dependencies: 276
-- Name: COLUMN ir_conviction_acquittal.reason_if_acquitted; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_conviction_acquittal.reason_if_acquitted IS 'Reason for acquittal if applicable';


--
-- TOC entry 4392 (class 0 OID 0)
-- Dependencies: 276
-- Name: COLUMN ir_conviction_acquittal.sentence_if_convicted; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_conviction_acquittal.sentence_if_convicted IS 'Details of sentence if convicted';


--
-- TOC entry 4393 (class 0 OID 0)
-- Dependencies: 276
-- Name: COLUMN ir_conviction_acquittal.appeal_status; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_conviction_acquittal.appeal_status IS 'Status of any appeal (Pending, Dismissed, Allowed, etc.)';


--
-- TOC entry 277 (class 1259 OID 39132856)
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
-- TOC entry 278 (class 1259 OID 39132861)
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
-- TOC entry 279 (class 1259 OID 39132866)
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
-- TOC entry 4394 (class 0 OID 0)
-- Dependencies: 279
-- Name: TABLE ir_execution_of_nbw; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.ir_execution_of_nbw IS 'Execution of NBW (Non-Bailable Warrant) for each IR record. One record per NBW execution entry.';


--
-- TOC entry 4395 (class 0 OID 0)
-- Dependencies: 279
-- Name: COLUMN ir_execution_of_nbw.interrogation_report_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_execution_of_nbw.interrogation_report_id IS 'Foreign key to interrogation_reports table';


--
-- TOC entry 4396 (class 0 OID 0)
-- Dependencies: 279
-- Name: COLUMN ir_execution_of_nbw.nbw_number; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_execution_of_nbw.nbw_number IS 'NBW number/reference';


--
-- TOC entry 4397 (class 0 OID 0)
-- Dependencies: 279
-- Name: COLUMN ir_execution_of_nbw.issued_date; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_execution_of_nbw.issued_date IS 'Date NBW was issued';


--
-- TOC entry 4398 (class 0 OID 0)
-- Dependencies: 279
-- Name: COLUMN ir_execution_of_nbw.executed_date; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_execution_of_nbw.executed_date IS 'Date NBW was executed';


--
-- TOC entry 4399 (class 0 OID 0)
-- Dependencies: 279
-- Name: COLUMN ir_execution_of_nbw.jurisdiction_ps; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_execution_of_nbw.jurisdiction_ps IS 'Police station where executed';


--
-- TOC entry 4400 (class 0 OID 0)
-- Dependencies: 279
-- Name: COLUMN ir_execution_of_nbw.crime_num; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_execution_of_nbw.crime_num IS 'Associated crime number';


--
-- TOC entry 4401 (class 0 OID 0)
-- Dependencies: 279
-- Name: COLUMN ir_execution_of_nbw.executed_by; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_execution_of_nbw.executed_by IS 'Name of officer who executed';


--
-- TOC entry 4402 (class 0 OID 0)
-- Dependencies: 279
-- Name: COLUMN ir_execution_of_nbw.place_of_execution; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_execution_of_nbw.place_of_execution IS 'Location of execution';


--
-- TOC entry 280 (class 1259 OID 39132872)
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
-- TOC entry 281 (class 1259 OID 39132880)
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
-- TOC entry 282 (class 1259 OID 39132885)
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
-- TOC entry 4403 (class 0 OID 0)
-- Dependencies: 282
-- Name: TABLE ir_indulgance_before_offence; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.ir_indulgance_before_offence IS 'Substances/habits indulged in before offense for each IR record. One record per indulgance entry (junction table for INDULGANCE_BEFORE_OFFENCE array).';


--
-- TOC entry 4404 (class 0 OID 0)
-- Dependencies: 282
-- Name: COLUMN ir_indulgance_before_offence.interrogation_report_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_indulgance_before_offence.interrogation_report_id IS 'Foreign key to interrogation_reports table';


--
-- TOC entry 4405 (class 0 OID 0)
-- Dependencies: 282
-- Name: COLUMN ir_indulgance_before_offence.indulgance; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_indulgance_before_offence.indulgance IS 'Type of indulgance (e.g., alcohol, drugs, etc.)';


--
-- TOC entry 283 (class 1259 OID 39132891)
-- Name: ir_interrogation_report_refs; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.ir_interrogation_report_refs (
    id integer NOT NULL,
    interrogation_report_id character varying(50) NOT NULL,
    report_ref_id text NOT NULL
);


ALTER TABLE public.ir_interrogation_report_refs OWNER TO dev_dopamas;

--
-- TOC entry 284 (class 1259 OID 39132896)
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
-- TOC entry 4406 (class 0 OID 0)
-- Dependencies: 284
-- Name: TABLE ir_jail_sentence; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.ir_jail_sentence IS 'Jail sentence details for each IR record. One record per sentence entry.';


--
-- TOC entry 4407 (class 0 OID 0)
-- Dependencies: 284
-- Name: COLUMN ir_jail_sentence.interrogation_report_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_jail_sentence.interrogation_report_id IS 'Foreign key to interrogation_reports table';


--
-- TOC entry 4408 (class 0 OID 0)
-- Dependencies: 284
-- Name: COLUMN ir_jail_sentence.crime_num; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_jail_sentence.crime_num IS 'Associated crime number';


--
-- TOC entry 4409 (class 0 OID 0)
-- Dependencies: 284
-- Name: COLUMN ir_jail_sentence.sentence_type; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_jail_sentence.sentence_type IS 'Type of sentence (RI, SI, etc.)';


--
-- TOC entry 4410 (class 0 OID 0)
-- Dependencies: 284
-- Name: COLUMN ir_jail_sentence.sentence_duration_in_months; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_jail_sentence.sentence_duration_in_months IS 'Duration in months';


--
-- TOC entry 4411 (class 0 OID 0)
-- Dependencies: 284
-- Name: COLUMN ir_jail_sentence.sentence_start_date; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_jail_sentence.sentence_start_date IS 'When sentence started';


--
-- TOC entry 4412 (class 0 OID 0)
-- Dependencies: 284
-- Name: COLUMN ir_jail_sentence.sentence_end_date; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_jail_sentence.sentence_end_date IS 'When sentence ended';


--
-- TOC entry 4413 (class 0 OID 0)
-- Dependencies: 284
-- Name: COLUMN ir_jail_sentence.sentence_amount_in_inr; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_jail_sentence.sentence_amount_in_inr IS 'Fine amount in INR if applicable';


--
-- TOC entry 4414 (class 0 OID 0)
-- Dependencies: 284
-- Name: COLUMN ir_jail_sentence.jail_name; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_jail_sentence.jail_name IS 'Name of jail where served';


--
-- TOC entry 4415 (class 0 OID 0)
-- Dependencies: 284
-- Name: COLUMN ir_jail_sentence.date_of_jail_entry; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_jail_sentence.date_of_jail_entry IS 'When admitted to jail';


--
-- TOC entry 4416 (class 0 OID 0)
-- Dependencies: 284
-- Name: COLUMN ir_jail_sentence.date_of_jail_release; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_jail_sentence.date_of_jail_release IS 'When released from jail';


--
-- TOC entry 285 (class 1259 OID 39132902)
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
-- TOC entry 286 (class 1259 OID 39132907)
-- Name: ir_media; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.ir_media (
    id integer NOT NULL,
    interrogation_report_id character varying(50) NOT NULL,
    media_id text NOT NULL
);


ALTER TABLE public.ir_media OWNER TO dev_dopamas;

--
-- TOC entry 287 (class 1259 OID 39132912)
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
-- TOC entry 288 (class 1259 OID 39132917)
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
-- TOC entry 4417 (class 0 OID 0)
-- Dependencies: 288
-- Name: TABLE ir_new_gang_formation; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.ir_new_gang_formation IS 'New gang formation details for each IR record. One record per gang entry.';


--
-- TOC entry 4418 (class 0 OID 0)
-- Dependencies: 288
-- Name: COLUMN ir_new_gang_formation.interrogation_report_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_new_gang_formation.interrogation_report_id IS 'Foreign key to interrogation_reports table';


--
-- TOC entry 4419 (class 0 OID 0)
-- Dependencies: 288
-- Name: COLUMN ir_new_gang_formation.gang_name; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_new_gang_formation.gang_name IS 'Name of the gang';


--
-- TOC entry 4420 (class 0 OID 0)
-- Dependencies: 288
-- Name: COLUMN ir_new_gang_formation.gang_formation_date; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_new_gang_formation.gang_formation_date IS 'When gang was formed';


--
-- TOC entry 4421 (class 0 OID 0)
-- Dependencies: 288
-- Name: COLUMN ir_new_gang_formation.number_of_members; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_new_gang_formation.number_of_members IS 'Number of members';


--
-- TOC entry 4422 (class 0 OID 0)
-- Dependencies: 288
-- Name: COLUMN ir_new_gang_formation.leader_name; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_new_gang_formation.leader_name IS 'Name of gang leader';


--
-- TOC entry 4423 (class 0 OID 0)
-- Dependencies: 288
-- Name: COLUMN ir_new_gang_formation.leader_person_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_new_gang_formation.leader_person_id IS 'Reference to person_id if leader is in DOPAMS';


--
-- TOC entry 4424 (class 0 OID 0)
-- Dependencies: 288
-- Name: COLUMN ir_new_gang_formation.gang_objective; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_new_gang_formation.gang_objective IS 'Stated objective of gang';


--
-- TOC entry 4425 (class 0 OID 0)
-- Dependencies: 288
-- Name: COLUMN ir_new_gang_formation.criminal_history; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_new_gang_formation.criminal_history IS 'Known criminal activities';


--
-- TOC entry 4426 (class 0 OID 0)
-- Dependencies: 288
-- Name: COLUMN ir_new_gang_formation.active; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_new_gang_formation.active IS 'Whether gang is still active';


--
-- TOC entry 290 (class 1259 OID 39132931)
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
-- TOC entry 4427 (class 0 OID 0)
-- Dependencies: 290
-- Name: TABLE ir_pending_nbw; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.ir_pending_nbw IS 'Pending NBW (Non-Bailable Warrant) for each IR record. One record per pending NBW entry.';


--
-- TOC entry 4428 (class 0 OID 0)
-- Dependencies: 290
-- Name: COLUMN ir_pending_nbw.interrogation_report_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_pending_nbw.interrogation_report_id IS 'Foreign key to interrogation_reports table';


--
-- TOC entry 4429 (class 0 OID 0)
-- Dependencies: 290
-- Name: COLUMN ir_pending_nbw.nbw_number; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_pending_nbw.nbw_number IS 'NBW number/reference';


--
-- TOC entry 4430 (class 0 OID 0)
-- Dependencies: 290
-- Name: COLUMN ir_pending_nbw.issued_date; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_pending_nbw.issued_date IS 'Date NBW was issued';


--
-- TOC entry 4431 (class 0 OID 0)
-- Dependencies: 290
-- Name: COLUMN ir_pending_nbw.jurisdiction_ps; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_pending_nbw.jurisdiction_ps IS 'Police station where issued';


--
-- TOC entry 4432 (class 0 OID 0)
-- Dependencies: 290
-- Name: COLUMN ir_pending_nbw.crime_num; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_pending_nbw.crime_num IS 'Associated crime number';


--
-- TOC entry 4433 (class 0 OID 0)
-- Dependencies: 290
-- Name: COLUMN ir_pending_nbw.reason_for_pending; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_pending_nbw.reason_for_pending IS 'Reason why NBW is still pending';


--
-- TOC entry 4434 (class 0 OID 0)
-- Dependencies: 290
-- Name: COLUMN ir_pending_nbw.expected_execution_date; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_pending_nbw.expected_execution_date IS 'Expected date of execution';


--
-- TOC entry 291 (class 1259 OID 39132937)
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
-- TOC entry 4435 (class 0 OID 0)
-- Dependencies: 291
-- Name: COLUMN ir_previous_offences_confessed.conviction_status; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_previous_offences_confessed.conviction_status IS 'Status of conviction (if relevant to the offense)';


--
-- TOC entry 4436 (class 0 OID 0)
-- Dependencies: 291
-- Name: COLUMN ir_previous_offences_confessed.bail_status; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_previous_offences_confessed.bail_status IS 'Bail status during this offense';


--
-- TOC entry 4437 (class 0 OID 0)
-- Dependencies: 291
-- Name: COLUMN ir_previous_offences_confessed.court_name; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_previous_offences_confessed.court_name IS 'Court handling the case';


--
-- TOC entry 4438 (class 0 OID 0)
-- Dependencies: 291
-- Name: COLUMN ir_previous_offences_confessed.judge_name; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_previous_offences_confessed.judge_name IS 'Judge handling the case';


--
-- TOC entry 292 (class 1259 OID 39132942)
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
-- TOC entry 4439 (class 0 OID 0)
-- Dependencies: 292
-- Name: TABLE ir_property_disposal; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.ir_property_disposal IS 'Property disposal details for each IR record. One record per disposal entry.';


--
-- TOC entry 4440 (class 0 OID 0)
-- Dependencies: 292
-- Name: COLUMN ir_property_disposal.interrogation_report_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_property_disposal.interrogation_report_id IS 'Foreign key to interrogation_reports table';


--
-- TOC entry 4441 (class 0 OID 0)
-- Dependencies: 292
-- Name: COLUMN ir_property_disposal.mode_of_disposal; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_property_disposal.mode_of_disposal IS 'How property was disposed (sold, donated, etc.)';


--
-- TOC entry 4442 (class 0 OID 0)
-- Dependencies: 292
-- Name: COLUMN ir_property_disposal.buyer_name; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_property_disposal.buyer_name IS 'Name of buyer or recipient';


--
-- TOC entry 4443 (class 0 OID 0)
-- Dependencies: 292
-- Name: COLUMN ir_property_disposal.sold_amount_in_inr; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_property_disposal.sold_amount_in_inr IS 'Amount in INR if sold';


--
-- TOC entry 4444 (class 0 OID 0)
-- Dependencies: 292
-- Name: COLUMN ir_property_disposal.location_of_disposal; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_property_disposal.location_of_disposal IS 'Location where property was disposed';


--
-- TOC entry 4445 (class 0 OID 0)
-- Dependencies: 292
-- Name: COLUMN ir_property_disposal.date_of_disposal; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_property_disposal.date_of_disposal IS 'Date of disposal';


--
-- TOC entry 293 (class 1259 OID 39132948)
-- Name: ir_regular_habits; Type: TABLE; Schema: public; Owner: dev_dopamas
--

CREATE TABLE public.ir_regular_habits (
    id integer NOT NULL,
    interrogation_report_id character varying(50) NOT NULL,
    habit character varying(255) NOT NULL
);


ALTER TABLE public.ir_regular_habits OWNER TO dev_dopamas;

--
-- TOC entry 294 (class 1259 OID 39132951)
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
-- TOC entry 4446 (class 0 OID 0)
-- Dependencies: 294
-- Name: TABLE ir_regularization_transit_warrants; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.ir_regularization_transit_warrants IS 'Regularization of transit warrants for each IR record. One record per warrant entry.';


--
-- TOC entry 4447 (class 0 OID 0)
-- Dependencies: 294
-- Name: COLUMN ir_regularization_transit_warrants.interrogation_report_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_regularization_transit_warrants.interrogation_report_id IS 'Foreign key to interrogation_reports table';


--
-- TOC entry 4448 (class 0 OID 0)
-- Dependencies: 294
-- Name: COLUMN ir_regularization_transit_warrants.warrant_number; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_regularization_transit_warrants.warrant_number IS 'Warrant number/reference';


--
-- TOC entry 4449 (class 0 OID 0)
-- Dependencies: 294
-- Name: COLUMN ir_regularization_transit_warrants.warrant_type; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_regularization_transit_warrants.warrant_type IS 'Type of warrant (NBW, transit, etc.)';


--
-- TOC entry 4450 (class 0 OID 0)
-- Dependencies: 294
-- Name: COLUMN ir_regularization_transit_warrants.issued_date; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_regularization_transit_warrants.issued_date IS 'Date warrant was issued';


--
-- TOC entry 4451 (class 0 OID 0)
-- Dependencies: 294
-- Name: COLUMN ir_regularization_transit_warrants.jurisdiction_ps; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_regularization_transit_warrants.jurisdiction_ps IS 'Police station/jurisdiction';


--
-- TOC entry 4452 (class 0 OID 0)
-- Dependencies: 294
-- Name: COLUMN ir_regularization_transit_warrants.crime_num; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_regularization_transit_warrants.crime_num IS 'Associated crime number';


--
-- TOC entry 4453 (class 0 OID 0)
-- Dependencies: 294
-- Name: COLUMN ir_regularization_transit_warrants.status; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_regularization_transit_warrants.status IS 'Current status (pending, executed, withdrawn, etc.)';


--
-- TOC entry 295 (class 1259 OID 39132957)
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
-- TOC entry 296 (class 1259 OID 39132962)
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
-- TOC entry 297 (class 1259 OID 39132967)
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
-- TOC entry 4454 (class 0 OID 0)
-- Dependencies: 297
-- Name: TABLE ir_sureties; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON TABLE public.ir_sureties IS 'Surety information for bail for each IR record. One record per surety entry.';


--
-- TOC entry 4455 (class 0 OID 0)
-- Dependencies: 297
-- Name: COLUMN ir_sureties.interrogation_report_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_sureties.interrogation_report_id IS 'Foreign key to interrogation_reports table';


--
-- TOC entry 4456 (class 0 OID 0)
-- Dependencies: 297
-- Name: COLUMN ir_sureties.surety_person_id; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_sureties.surety_person_id IS 'Reference to person_id if surety is in DOPAMS';


--
-- TOC entry 4457 (class 0 OID 0)
-- Dependencies: 297
-- Name: COLUMN ir_sureties.surety_name; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_sureties.surety_name IS 'Name of surety';


--
-- TOC entry 4458 (class 0 OID 0)
-- Dependencies: 297
-- Name: COLUMN ir_sureties.relation_to_accused; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_sureties.relation_to_accused IS 'Relationship to accused (friend, family, etc.)';


--
-- TOC entry 4459 (class 0 OID 0)
-- Dependencies: 297
-- Name: COLUMN ir_sureties.occupation; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_sureties.occupation IS 'Occupation of surety';


--
-- TOC entry 4460 (class 0 OID 0)
-- Dependencies: 297
-- Name: COLUMN ir_sureties.surety_amount_in_inr; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_sureties.surety_amount_in_inr IS 'Amount of surety in INR';


--
-- TOC entry 4461 (class 0 OID 0)
-- Dependencies: 297
-- Name: COLUMN ir_sureties.date_of_surety; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON COLUMN public.ir_sureties.date_of_surety IS 'Date surety was provided';


--
-- TOC entry 298 (class 1259 OID 39132973)
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
-- TOC entry 319 (class 1259 OID 39606178)
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
-- TOC entry 4462 (class 0 OID 0)
-- Dependencies: 319
-- Name: VIEW ir_child_table_coverage; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON VIEW public.ir_child_table_coverage IS 'Shows data coverage for all IR related arrays - helps identify which fields are being populated';


--
-- TOC entry 318 (class 1259 OID 39606173)
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
-- TOC entry 4463 (class 0 OID 0)
-- Dependencies: 318
-- Name: VIEW ir_field_persistence_check; Type: COMMENT; Schema: public; Owner: dev_dopamas
--

COMMENT ON VIEW public.ir_field_persistence_check IS 'Validates API field to DB persistence mapping - shows which fields are being stored and frequency of non-null values';


--
-- TOC entry 323 (class 1259 OID 39851498)
-- Name: ir_indulgance_before_offence_id_seq; Type: SEQUENCE; Schema: public; Owner: dev_dopamas
--

CREATE SEQUENCE public.ir_indulgance_before_offence_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.ir_indulgance_before_offence_id_seq OWNER TO dev_dopamas;

--
-- TOC entry 4464 (class 0 OID 0)
-- Dependencies: 323
-- Name: ir_indulgance_before_offence_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.ir_indulgance_before_offence_id_seq OWNED BY public.ir_indulgance_before_offence.id;


--
-- TOC entry 322 (class 1259 OID 39851496)
-- Name: ir_interrogation_report_refs_id_seq; Type: SEQUENCE; Schema: public; Owner: dev_dopamas
--

CREATE SEQUENCE public.ir_interrogation_report_refs_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.ir_interrogation_report_refs_id_seq OWNER TO dev_dopamas;

--
-- TOC entry 4465 (class 0 OID 0)
-- Dependencies: 322
-- Name: ir_interrogation_report_refs_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.ir_interrogation_report_refs_id_seq OWNED BY public.ir_interrogation_report_refs.id;


--
-- TOC entry 321 (class 1259 OID 39851494)
-- Name: ir_media_id_seq; Type: SEQUENCE; Schema: public; Owner: dev_dopamas
--

CREATE SEQUENCE public.ir_media_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.ir_media_id_seq OWNER TO dev_dopamas;

--
-- TOC entry 4466 (class 0 OID 0)
-- Dependencies: 321
-- Name: ir_media_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.ir_media_id_seq OWNED BY public.ir_media.id;


--
-- TOC entry 289 (class 1259 OID 39132923)
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
-- TOC entry 324 (class 1259 OID 39851500)
-- Name: ir_pending_fk_id_seq; Type: SEQUENCE; Schema: public; Owner: dev_dopamas
--

CREATE SEQUENCE public.ir_pending_fk_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.ir_pending_fk_id_seq OWNER TO dev_dopamas;

--
-- TOC entry 4467 (class 0 OID 0)
-- Dependencies: 324
-- Name: ir_pending_fk_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.ir_pending_fk_id_seq OWNED BY public.ir_pending_fk.id;


--
-- TOC entry 320 (class 1259 OID 39851492)
-- Name: ir_regular_habits_id_seq; Type: SEQUENCE; Schema: public; Owner: dev_dopamas
--

CREATE SEQUENCE public.ir_regular_habits_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.ir_regular_habits_id_seq OWNER TO dev_dopamas;

--
-- TOC entry 4468 (class 0 OID 0)
-- Dependencies: 320
-- Name: ir_regular_habits_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dev_dopamas
--

ALTER SEQUENCE public.ir_regular_habits_id_seq OWNED BY public.ir_regular_habits.id;


--
-- TOC entry 299 (class 1259 OID 39132978)
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
-- TOC entry 300 (class 1259 OID 39132986)
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
-- TOC entry 301 (class 1259 OID 39132991)
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
-- TOC entry 302 (class 1259 OID 39132997)
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
-- TOC entry 303 (class 1259 OID 39133009)
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
-- TOC entry 304 (class 1259 OID 39133015)
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
-- TOC entry 305 (class 1259 OID 39133023)
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
-- TOC entry 306 (class 1259 OID 39133029)
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
-- TOC entry 307 (class 1259 OID 39133034)
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
-- TOC entry 4086 (class 2604 OID 39606144)
-- Name: etl_fk_retry_queue queue_id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.etl_fk_retry_queue ALTER COLUMN queue_id SET DEFAULT nextval('public.etl_fk_retry_queue_queue_id_seq'::regclass);


--
-- TOC entry 4055 (class 2604 OID 39851505)
-- Name: ir_indulgance_before_offence id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_indulgance_before_offence ALTER COLUMN id SET DEFAULT nextval('public.ir_indulgance_before_offence_id_seq'::regclass);


--
-- TOC entry 4057 (class 2604 OID 39851504)
-- Name: ir_interrogation_report_refs id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_interrogation_report_refs ALTER COLUMN id SET DEFAULT nextval('public.ir_interrogation_report_refs_id_seq'::regclass);


--
-- TOC entry 4059 (class 2604 OID 39851503)
-- Name: ir_media id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_media ALTER COLUMN id SET DEFAULT nextval('public.ir_media_id_seq'::regclass);


--
-- TOC entry 4061 (class 2604 OID 39851506)
-- Name: ir_pending_fk id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_pending_fk ALTER COLUMN id SET DEFAULT nextval('public.ir_pending_fk_id_seq'::regclass);


--
-- TOC entry 4067 (class 2604 OID 39851502)
-- Name: ir_regular_habits id; Type: DEFAULT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.ir_regular_habits ALTER COLUMN id SET DEFAULT nextval('public.ir_regular_habits_id_seq'::regclass);


--
-- TOC entry 4102 (class 2606 OID 39606133)
-- Name: brief_facts_ai brief_facts_ai_crime_accused_key; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.brief_facts_ai
    ADD CONSTRAINT brief_facts_ai_crime_accused_key UNIQUE (crime_id, accused_id);


--
-- TOC entry 4104 (class 2606 OID 39606131)
-- Name: brief_facts_ai brief_facts_ai_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.brief_facts_ai
    ADD CONSTRAINT brief_facts_ai_pkey PRIMARY KEY (bf_accused_id);


--
-- TOC entry 4150 (class 2606 OID 39606151)
-- Name: etl_fk_retry_queue etl_fk_retry_queue_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.etl_fk_retry_queue
    ADD CONSTRAINT etl_fk_retry_queue_pkey PRIMARY KEY (queue_id);


--
-- TOC entry 4153 (class 2606 OID 39606158)
-- Name: etl_run_state etl_run_state_pkey; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.etl_run_state
    ADD CONSTRAINT etl_run_state_pkey PRIMARY KEY (module_name);


--
-- TOC entry 4100 (class 2606 OID 39850228)
-- Name: accused pk_accused_id; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.accused
    ADD CONSTRAINT pk_accused_id PRIMARY KEY (accused_id);


--
-- TOC entry 4115 (class 2606 OID 39843162)
-- Name: crimes pk_crimes_id; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.crimes
    ADD CONSTRAINT pk_crimes_id PRIMARY KEY (crime_id);


--
-- TOC entry 4124 (class 2606 OID 39850234)
-- Name: interrogation_reports pk_ir_id; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.interrogation_reports
    ADD CONSTRAINT pk_ir_id PRIMARY KEY (interrogation_report_id);


--
-- TOC entry 4119 (class 2606 OID 39850230)
-- Name: persons pk_persons_id; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.persons
    ADD CONSTRAINT pk_persons_id PRIMARY KEY (person_id);


--
-- TOC entry 4147 (class 2606 OID 39850232)
-- Name: properties pk_properties_id; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.properties
    ADD CONSTRAINT pk_properties_id PRIMARY KEY (property_id);


--
-- TOC entry 4117 (class 2606 OID 39850236)
-- Name: disposal uk_disposal_composite; Type: CONSTRAINT; Schema: public; Owner: dev_dopamas
--

ALTER TABLE ONLY public.disposal
    ADD CONSTRAINT uk_disposal_composite UNIQUE (crime_id, disposal_type, disposed_at);


--
-- TOC entry 4151 (class 1259 OID 39606152)
-- Name: etl_fk_retry_queue_source_unresolved; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX etl_fk_retry_queue_source_unresolved ON public.etl_fk_retry_queue USING btree (source_table) WHERE (resolved = false);


--
-- TOC entry 4127 (class 1259 OID 39133097)
-- Name: geo_ref_district_trgm; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX geo_ref_district_trgm ON public.geo_reference USING gin (district_name public.gin_trgm_ops);


--
-- TOC entry 4128 (class 1259 OID 39133098)
-- Name: geo_ref_mandal_trgm; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX geo_ref_mandal_trgm ON public.geo_reference USING gin (sub_district_name public.gin_trgm_ops);


--
-- TOC entry 4129 (class 1259 OID 39133099)
-- Name: geo_ref_state_trgm; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX geo_ref_state_trgm ON public.geo_reference USING gin (state_name public.gin_trgm_ops);


--
-- TOC entry 4130 (class 1259 OID 39133100)
-- Name: geo_ref_trgm_idx; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX geo_ref_trgm_idx ON public.geo_reference USING gin (district_name public.gin_trgm_ops, sub_district_name public.gin_trgm_ops);


--
-- TOC entry 4131 (class 1259 OID 39133101)
-- Name: geo_reference_mandal_trgm; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX geo_reference_mandal_trgm ON public.geo_reference USING gin (sub_district_name public.gin_trgm_ops);


--
-- TOC entry 4105 (class 1259 OID 39606135)
-- Name: idx_bfai_accused_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_bfai_accused_id ON public.brief_facts_ai USING btree (accused_id);


--
-- TOC entry 4106 (class 1259 OID 39606137)
-- Name: idx_bfai_canonical_person_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_bfai_canonical_person_id ON public.brief_facts_ai USING btree (canonical_person_id);


--
-- TOC entry 4107 (class 1259 OID 39606134)
-- Name: idx_bfai_crime_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_bfai_crime_id ON public.brief_facts_ai USING btree (crime_id);


--
-- TOC entry 4108 (class 1259 OID 39606139)
-- Name: idx_bfai_drugs_gin; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_bfai_drugs_gin ON public.brief_facts_ai USING gin (drugs);


--
-- TOC entry 4109 (class 1259 OID 39606136)
-- Name: idx_bfai_person_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_bfai_person_id ON public.brief_facts_ai USING btree (person_id);


--
-- TOC entry 4110 (class 1259 OID 39606138)
-- Name: idx_bfai_soundex_name; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_bfai_soundex_name ON public.brief_facts_ai USING btree (public.soundex((full_name)::text));


--
-- TOC entry 4111 (class 1259 OID 39852063)
-- Name: idx_crimes_coalesce_date; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_crimes_coalesce_date ON public.crimes USING btree (COALESCE(date_modified, date_created) DESC NULLS LAST);


--
-- TOC entry 4112 (class 1259 OID 39852061)
-- Name: idx_crimes_date_created; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_crimes_date_created ON public.crimes USING btree (date_created DESC NULLS LAST);


--
-- TOC entry 4113 (class 1259 OID 39852059)
-- Name: idx_crimes_date_modified_created; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_crimes_date_modified_created ON public.crimes USING btree (date_modified DESC NULLS LAST, date_created DESC NULLS LAST);


--
-- TOC entry 4120 (class 1259 OID 39852062)
-- Name: idx_etl_log_crime_status_completed; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_etl_log_crime_status_completed ON public.etl_crime_processing_log USING btree (crime_id, status, completed_at DESC NULLS LAST);


--
-- TOC entry 4125 (class 1259 OID 39133089)
-- Name: idx_geo_countries_country_trgm; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_geo_countries_country_trgm ON public.geo_countries USING gin (country_name public.gin_trgm_ops);


--
-- TOC entry 4126 (class 1259 OID 39133090)
-- Name: idx_geo_countries_state_trgm; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_geo_countries_state_trgm ON public.geo_countries USING gin (state_name public.gin_trgm_ops);


--
-- TOC entry 4136 (class 1259 OID 39606172)
-- Name: idx_ir_conviction_acquittal_ir_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_conviction_acquittal_ir_id ON public.ir_conviction_acquittal USING btree (interrogation_report_id);


--
-- TOC entry 4137 (class 1259 OID 39606167)
-- Name: idx_ir_execution_of_nbw_ir_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_execution_of_nbw_ir_id ON public.ir_execution_of_nbw USING btree (interrogation_report_id);


--
-- TOC entry 4138 (class 1259 OID 39606164)
-- Name: idx_ir_indulgance_before_offence_ir_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_indulgance_before_offence_ir_id ON public.ir_indulgance_before_offence USING btree (interrogation_report_id);


--
-- TOC entry 4139 (class 1259 OID 39606170)
-- Name: idx_ir_jail_sentence_ir_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_jail_sentence_ir_id ON public.ir_jail_sentence USING btree (interrogation_report_id);


--
-- TOC entry 4140 (class 1259 OID 39606171)
-- Name: idx_ir_new_gang_formation_ir_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_new_gang_formation_ir_id ON public.ir_new_gang_formation USING btree (interrogation_report_id);


--
-- TOC entry 4142 (class 1259 OID 39606168)
-- Name: idx_ir_pending_nbw_ir_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_pending_nbw_ir_id ON public.ir_pending_nbw USING btree (interrogation_report_id);


--
-- TOC entry 4143 (class 1259 OID 39606165)
-- Name: idx_ir_property_disposal_ir_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_property_disposal_ir_id ON public.ir_property_disposal USING btree (interrogation_report_id);


--
-- TOC entry 4144 (class 1259 OID 39606166)
-- Name: idx_ir_regularization_transit_warrants_ir_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_regularization_transit_warrants_ir_id ON public.ir_regularization_transit_warrants USING btree (interrogation_report_id);


--
-- TOC entry 4121 (class 1259 OID 39606184)
-- Name: idx_ir_reports_created_modified; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_reports_created_modified ON public.interrogation_reports USING btree (date_created, date_modified);


--
-- TOC entry 4122 (class 1259 OID 39606183)
-- Name: idx_ir_reports_crime_person; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_reports_crime_person ON public.interrogation_reports USING btree (crime_id, person_id);


--
-- TOC entry 4145 (class 1259 OID 39606169)
-- Name: idx_ir_sureties_ir_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX idx_ir_sureties_ir_id ON public.ir_sureties USING btree (interrogation_report_id);


--
-- TOC entry 4141 (class 1259 OID 39606163)
-- Name: idx_pending_fk_ir_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE UNIQUE INDEX idx_pending_fk_ir_id ON public.ir_pending_fk USING btree (ir_id) WHERE (NOT resolved);


--
-- TOC entry 4148 (class 1259 OID 39606162)
-- Name: idx_pending_fk_property_id; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE UNIQUE INDEX idx_pending_fk_property_id ON public.properties_pending_fk USING btree (property_id) WHERE (NOT resolved);


--
-- TOC entry 4132 (class 1259 OID 39133102)
-- Name: trgm_idx_geo_district; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX trgm_idx_geo_district ON public.geo_reference USING gin (district_name public.gin_trgm_ops);


--
-- TOC entry 4133 (class 1259 OID 39133103)
-- Name: trgm_idx_geo_state; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX trgm_idx_geo_state ON public.geo_reference USING gin (state_name public.gin_trgm_ops);


--
-- TOC entry 4134 (class 1259 OID 39133104)
-- Name: trgm_idx_geo_sub_district; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX trgm_idx_geo_sub_district ON public.geo_reference USING gin (sub_district_name public.gin_trgm_ops);


--
-- TOC entry 4135 (class 1259 OID 39133105)
-- Name: trgm_idx_geo_village; Type: INDEX; Schema: public; Owner: dev_dopamas
--

CREATE INDEX trgm_idx_geo_village ON public.geo_reference USING gin (village_name_english public.gin_trgm_ops);


--
-- TOC entry 4313 (class 0 OID 0)
-- Dependencies: 23
-- Name: SCHEMA public; Type: ACL; Schema: -; Owner: dev_dopamas
--

REVOKE USAGE ON SCHEMA public FROM PUBLIC;
GRANT ALL ON SCHEMA public TO PUBLIC;


-- Completed on 2026-04-16 17:16:38

--
-- PostgreSQL database dump complete
--


