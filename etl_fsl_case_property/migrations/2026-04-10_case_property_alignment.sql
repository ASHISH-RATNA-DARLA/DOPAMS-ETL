-- MODULE: FSL Case Property
-- Migration: API/ETL/Schema alignment + normalized media snapshot table
-- Safety: Idempotent, production-safe, no destructive drops

BEGIN;

-- 1) Parent table hardening: enforce required keys and expected data types.
ALTER TABLE public.fsl_case_property
    ADD COLUMN IF NOT EXISTS case_property_id character varying(255),
    ADD COLUMN IF NOT EXISTS case_type character varying(100),
    ADD COLUMN IF NOT EXISTS crime_id character varying(50),
    ADD COLUMN IF NOT EXISTS mo_id character varying(255),
    ADD COLUMN IF NOT EXISTS status character varying(100),
    ADD COLUMN IF NOT EXISTS send_date timestamptz,
    ADD COLUMN IF NOT EXISTS forwarding_through character varying(255),
    ADD COLUMN IF NOT EXISTS court_name character varying(500),
    ADD COLUMN IF NOT EXISTS fsl_court_name character varying(500),
    ADD COLUMN IF NOT EXISTS fsl_request_id character varying(255),
    ADD COLUMN IF NOT EXISTS cpr_no character varying(255),
    ADD COLUMN IF NOT EXISTS direction_by_court text,
    ADD COLUMN IF NOT EXISTS details_disposal text,
    ADD COLUMN IF NOT EXISTS cpr_court_name character varying(500),
    ADD COLUMN IF NOT EXISTS place_disposal character varying(500),
    ADD COLUMN IF NOT EXISTS date_disposal timestamptz,
    ADD COLUMN IF NOT EXISTS release_order_no character varying(255),
    ADD COLUMN IF NOT EXISTS release_date timestamptz,
    ADD COLUMN IF NOT EXISTS return_date timestamptz,
    ADD COLUMN IF NOT EXISTS place_custody character varying(500),
    ADD COLUMN IF NOT EXISTS assign_custody character varying(255),
    ADD COLUMN IF NOT EXISTS date_custody timestamptz,
    ADD COLUMN IF NOT EXISTS fsl_no character varying(255),
    ADD COLUMN IF NOT EXISTS fsl_date timestamptz,
    ADD COLUMN IF NOT EXISTS report_received boolean,
    ADD COLUMN IF NOT EXISTS opinion text,
    ADD COLUMN IF NOT EXISTS opinion_furnished character varying(255),
    ADD COLUMN IF NOT EXISTS strength_of_evidence character varying(255),
    ADD COLUMN IF NOT EXISTS property_received_back boolean,
    ADD COLUMN IF NOT EXISTS expert_type character varying(255),
    ADD COLUMN IF NOT EXISTS other_expert_type character varying(255),
    ADD COLUMN IF NOT EXISTS date_sent_to_expert timestamptz,
    ADD COLUMN IF NOT EXISTS court_order_number character varying(255),
    ADD COLUMN IF NOT EXISTS court_order_date timestamptz,
    ADD COLUMN IF NOT EXISTS date_created timestamptz,
    ADD COLUMN IF NOT EXISTS date_modified timestamptz;

ALTER TABLE public.fsl_case_property
    ALTER COLUMN case_property_id SET NOT NULL,
    ALTER COLUMN report_received DROP DEFAULT,
    ALTER COLUMN property_received_back DROP DEFAULT;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'fsl_case_property_crime_id_not_null_chk'
          AND conrelid = 'public.fsl_case_property'::regclass
    ) THEN
        ALTER TABLE public.fsl_case_property
            ADD CONSTRAINT fsl_case_property_crime_id_not_null_chk
            CHECK (crime_id IS NOT NULL) NOT VALID;
    END IF;
END;
$$;

-- Type normalization is performed conditionally to avoid touching columns that are
-- already in the target type (important when dependent views/materialized views exist).
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'fsl_case_property'
          AND column_name = 'send_date' AND data_type <> 'timestamp with time zone'
    ) THEN
        EXECUTE 'ALTER TABLE public.fsl_case_property ALTER COLUMN send_date TYPE timestamptz USING NULLIF(BTRIM(send_date::text), '''')::timestamptz';
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'fsl_case_property'
          AND column_name = 'fsl_date' AND data_type <> 'timestamp with time zone'
    ) THEN
        EXECUTE 'ALTER TABLE public.fsl_case_property ALTER COLUMN fsl_date TYPE timestamptz USING NULLIF(BTRIM(fsl_date::text), '''')::timestamptz';
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'fsl_case_property'
          AND column_name = 'date_disposal' AND data_type <> 'timestamp with time zone'
    ) THEN
        EXECUTE 'ALTER TABLE public.fsl_case_property ALTER COLUMN date_disposal TYPE timestamptz USING NULLIF(BTRIM(date_disposal::text), '''')::timestamptz';
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'fsl_case_property'
          AND column_name = 'release_date' AND data_type <> 'timestamp with time zone'
    ) THEN
        EXECUTE 'ALTER TABLE public.fsl_case_property ALTER COLUMN release_date TYPE timestamptz USING NULLIF(BTRIM(release_date::text), '''')::timestamptz';
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'fsl_case_property'
          AND column_name = 'return_date' AND data_type <> 'timestamp with time zone'
    ) THEN
        EXECUTE 'ALTER TABLE public.fsl_case_property ALTER COLUMN return_date TYPE timestamptz USING NULLIF(BTRIM(return_date::text), '''')::timestamptz';
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'fsl_case_property'
          AND column_name = 'date_custody' AND data_type <> 'timestamp with time zone'
    ) THEN
        EXECUTE 'ALTER TABLE public.fsl_case_property ALTER COLUMN date_custody TYPE timestamptz USING NULLIF(BTRIM(date_custody::text), '''')::timestamptz';
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'fsl_case_property'
          AND column_name = 'date_sent_to_expert' AND data_type <> 'timestamp with time zone'
    ) THEN
        EXECUTE 'ALTER TABLE public.fsl_case_property ALTER COLUMN date_sent_to_expert TYPE timestamptz USING NULLIF(BTRIM(date_sent_to_expert::text), '''')::timestamptz';
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'fsl_case_property'
          AND column_name = 'court_order_date' AND data_type <> 'timestamp with time zone'
    ) THEN
        EXECUTE 'ALTER TABLE public.fsl_case_property ALTER COLUMN court_order_date TYPE timestamptz USING NULLIF(BTRIM(court_order_date::text), '''')::timestamptz';
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'fsl_case_property'
          AND column_name = 'date_created' AND data_type <> 'timestamp with time zone'
    ) THEN
        EXECUTE 'ALTER TABLE public.fsl_case_property ALTER COLUMN date_created TYPE timestamptz USING NULLIF(BTRIM(date_created::text), '''')::timestamptz';
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'fsl_case_property'
          AND column_name = 'date_modified' AND data_type <> 'timestamp with time zone'
    ) THEN
        EXECUTE 'ALTER TABLE public.fsl_case_property ALTER COLUMN date_modified TYPE timestamptz USING NULLIF(BTRIM(date_modified::text), '''')::timestamptz';
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'fsl_case_property'
          AND column_name = 'report_received' AND data_type <> 'boolean'
    ) THEN
        EXECUTE 'ALTER TABLE public.fsl_case_property ALTER COLUMN report_received TYPE boolean USING (CASE WHEN report_received IS NULL THEN NULL WHEN report_received::text ILIKE ''true'' OR report_received::text = ''1'' THEN true WHEN report_received::text ILIKE ''false'' OR report_received::text = ''0'' THEN false ELSE NULL END)';
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'fsl_case_property'
          AND column_name = 'property_received_back' AND data_type <> 'boolean'
    ) THEN
        EXECUTE 'ALTER TABLE public.fsl_case_property ALTER COLUMN property_received_back TYPE boolean USING (CASE WHEN property_received_back IS NULL THEN NULL WHEN property_received_back::text ILIKE ''true'' OR property_received_back::text = ''1'' THEN true WHEN property_received_back::text ILIKE ''false'' OR property_received_back::text = ''0'' THEN false ELSE NULL END)';
    END IF;
END;
$$;

-- 2) Normalize blank strings to NULL for optional text fields.
UPDATE public.fsl_case_property
SET
    case_type = NULLIF(BTRIM(case_type), ''),
    mo_id = NULLIF(BTRIM(mo_id), ''),
    status = NULLIF(BTRIM(status), ''),
    forwarding_through = NULLIF(BTRIM(forwarding_through), ''),
    court_name = NULLIF(BTRIM(court_name), ''),
    fsl_court_name = NULLIF(BTRIM(fsl_court_name), ''),
    fsl_request_id = NULLIF(BTRIM(fsl_request_id), ''),
    cpr_no = NULLIF(BTRIM(cpr_no), ''),
    direction_by_court = NULLIF(BTRIM(direction_by_court), ''),
    details_disposal = NULLIF(BTRIM(details_disposal), ''),
    cpr_court_name = NULLIF(BTRIM(cpr_court_name), ''),
    place_disposal = NULLIF(BTRIM(place_disposal), ''),
    release_order_no = NULLIF(BTRIM(release_order_no), ''),
    place_custody = NULLIF(BTRIM(place_custody), ''),
    assign_custody = NULLIF(BTRIM(assign_custody), ''),
    fsl_no = NULLIF(BTRIM(fsl_no), ''),
    opinion = NULLIF(BTRIM(opinion), ''),
    opinion_furnished = NULLIF(BTRIM(opinion_furnished), ''),
    strength_of_evidence = NULLIF(BTRIM(strength_of_evidence), ''),
    expert_type = NULLIF(BTRIM(expert_type), ''),
    other_expert_type = NULLIF(BTRIM(other_expert_type), ''),
    court_order_number = NULLIF(BTRIM(court_order_number), '');

-- 3) Key and relationship constraints.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'fsl_case_property_pkey'
          AND conrelid = 'public.fsl_case_property'::regclass
    ) THEN
        ALTER TABLE ONLY public.fsl_case_property
            ADD CONSTRAINT fsl_case_property_pkey PRIMARY KEY (case_property_id);
    END IF;
END;
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'fsl_case_property_crime_id_fkey'
          AND conrelid = 'public.fsl_case_property'::regclass
    ) THEN
        ALTER TABLE ONLY public.fsl_case_property
            ADD CONSTRAINT fsl_case_property_crime_id_fkey
            FOREIGN KEY (crime_id) REFERENCES public.crimes(crime_id);
    END IF;
END;
$$;

-- Logical MO relationship enforcement: MO_ID must exist in mo_seizures for same CRIME_ID.
CREATE OR REPLACE FUNCTION public.enforce_case_property_mo_reference()
RETURNS trigger
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

DROP TRIGGER IF EXISTS trg_enforce_case_property_mo_reference ON public.fsl_case_property;
CREATE TRIGGER trg_enforce_case_property_mo_reference
BEFORE INSERT OR UPDATE OF crime_id, mo_id
ON public.fsl_case_property
FOR EACH ROW
EXECUTE FUNCTION public.enforce_case_property_mo_reference();

-- 4) Normalized media snapshot table (API MEDIA array/object values).
CREATE TABLE IF NOT EXISTS public.case_property_media (
    case_property_id character varying(255) NOT NULL,
    media_index integer NOT NULL,
    file_id character varying(255),
    media_payload jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT case_property_media_pkey PRIMARY KEY (case_property_id, media_index),
    CONSTRAINT case_property_media_case_property_id_fkey
        FOREIGN KEY (case_property_id)
        REFERENCES public.fsl_case_property(case_property_id)
        ON DELETE CASCADE
);

-- Backfill from legacy table when available.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'fsl_case_property_media'
    ) THEN
        INSERT INTO public.case_property_media (case_property_id, media_index, file_id, media_payload)
        SELECT
            legacy.case_property_id,
            legacy.media_index,
            legacy.file_id,
            legacy.media_payload
        FROM (
            SELECT
                m.case_property_id,
                ROW_NUMBER() OVER (PARTITION BY m.case_property_id ORDER BY m.media_id) - 1 AS media_index,
                NULLIF(BTRIM(m.file_id), '') AS file_id,
                jsonb_build_object('FILE_ID', NULLIF(BTRIM(m.file_id), '')) AS media_payload
            FROM public.fsl_case_property_media m
        ) AS legacy
        ON CONFLICT (case_property_id, media_index) DO UPDATE
        SET
            file_id = EXCLUDED.file_id,
            media_payload = EXCLUDED.media_payload,
            updated_at = now();
    END IF;
END;
$$;

-- 5) Performance indexes for incremental ETL and joins.
CREATE INDEX IF NOT EXISTS idx_fsl_case_property_crime_id ON public.fsl_case_property (crime_id);
CREATE INDEX IF NOT EXISTS idx_fsl_case_property_mo_id ON public.fsl_case_property (mo_id);
CREATE INDEX IF NOT EXISTS idx_fsl_case_property_date_modified ON public.fsl_case_property (date_modified);
CREATE INDEX IF NOT EXISTS idx_case_property_media_case_property_id ON public.case_property_media (case_property_id);
CREATE INDEX IF NOT EXISTS idx_case_property_media_file_id ON public.case_property_media (file_id);

COMMIT;
