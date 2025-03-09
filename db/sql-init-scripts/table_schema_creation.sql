-- Table: public.user_credentials

-- DROP TABLE IF EXISTS public.user_credentials;

CREATE TABLE IF NOT EXISTS public.user_credentials
(
    user_id SERIAL,
    username character varying(25) NOT NULL COLLATE pg_catalog."default" NOT NULL,
    password character varying(2000) COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT user_credentials_pkey PRIMARY KEY (user_id)
);

-- Table: public.s3

-- DROP TABLE IF EXISTS public.s3;

CREATE TABLE IF NOT EXISTS public.s3
(
    user_id integer NOT NULL,
    bucket_id SERIAL PRIMARY KEY,
    name character varying(63) NOT NULL COLLATE pg_catalog."default",
    arn character varying(200) COLLATE pg_catalog."default",
    bucket_type character varying(3) COLLATE pg_catalog."default",
    bucket_versioning boolean,
    acl_enabled boolean,
    block_public_access boolean,
    bucket_key boolean,
    object_lock boolean,
    encrypt_method character varying(8) COLLATE pg_catalog."default",
    bucket_policy jsonb,
    tags text[] COLLATE pg_catalog."default",
    object_replication boolean,
    replication_bucket_id integer,
    CONSTRAINT fk_replication FOREIGN KEY (replication_bucket_id) REFERENCES public.s3(bucket_id),
    CONSTRAINT fk_user FOREIGN KEY (user_id)
        REFERENCES public.user_credentials (user_id) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
);

-- Table: public.s3_bucket

-- DROP TABLE IF EXISTS public.s3_bucket;

CREATE TABLE IF NOT EXISTS public.s3_bucket
(
    user_id integer NOT NULL,
    bucket_id integer NOT NULL,
    object_id SERIAL PRIMARY KEY,
    sub_version_id smallint,
    version_id character varying(32) COLLATE pg_catalog."default",
    uri character varying(200) COLLATE pg_catalog."default",
    arn character varying(200) COLLATE pg_catalog."default",
    etag character varying(32) COLLATE pg_catalog."default",
    object_url character varying(200) COLLATE pg_catalog."default",
    owner character varying(25) COLLATE pg_catalog."default",
    creation_date timestamp without time zone,
    last_modified timestamp without time zone,
    size character varying(16) COLLATE pg_catalog."default",
    type character varying(8) COLLATE pg_catalog."default",
    storage_class character varying(40) COLLATE pg_catalog."default",
    tags text[] COLLATE pg_catalog."default",
    CONSTRAINT fk_bid FOREIGN KEY (bucket_id)
        REFERENCES public.s3 (bucket_id) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    CONSTRAINT fk_uid FOREIGN KEY (user_id)
        REFERENCES public.user_credentials (user_id) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS public.lifecycle_rule (
    user_id integer NOT NULL,
    bucket_id integer NOT NULL,
    lifecycle_id SERIAL PRIMARY KEY,
    rule_enabled boolean,
    CONSTRAINT fk_bid FOREIGN KEY (bucket_id)
        REFERENCES public.s3 (bucket_id) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    CONSTRAINT fk_uid FOREIGN KEY (user_id)
        REFERENCES public.user_credentials (user_id) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
);

ALTER TABLE s3 
ADD COLUMN lifecycle_id integer,
ADD CONSTRAINT fk_lfid FOREIGN KEY (lifecycle_id) REFERENCES lifecycle_rule(lifecycle_id);

CREATE TABLE IF NOT EXISTS public.lifecycle_transition (
    lifecycle_id integer NOT NULL,
    transition_id SERIAL PRIMARY KEY,
    transition_to character varying(40) COLLATE pg_catalog."default",
    days_till_transition smallint,
    CONSTRAINT fk_parent FOREIGN KEY (lifecycle_id)
        REFERENCES public.lifecycle_rule (lifecycle_id) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
);