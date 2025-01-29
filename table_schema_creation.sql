-- Table: public.user_credentials

-- DROP TABLE IF EXISTS public.user_credentials;

CREATE TABLE IF NOT EXISTS public.user_credentials
(
    user_id integer NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
    username character varying(25) COLLATE pg_catalog."default" NOT NULL,
    password character varying(2000) COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT user_credentials_pkey PRIMARY KEY (user_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.user_credentials
    OWNER to postgres;


-- Table: public.s3

-- DROP TABLE IF EXISTS public.s3;

CREATE TABLE IF NOT EXISTS public.s3
(
    user_id integer NOT NULL,
    bucket_id integer NOT NULL DEFAULT nextval('id_seq'::regclass),
    name character varying(63) COLLATE pg_catalog."default",
    bucket_type character varying(3) COLLATE pg_catalog."default",
    bucket_versioning boolean,
    acl_enabled boolean,
    block_public_access boolean,
    bucket_key boolean,
    object_lock boolean,
    encrypt_method character varying(8) COLLATE pg_catalog."default",
    bucket_policy jsonb,
    tags text[] COLLATE pg_catalog."default",
    CONSTRAINT s3_pkey PRIMARY KEY (user_id, bucket_id),
    CONSTRAINT fk_user FOREIGN KEY (user_id)
        REFERENCES public.user_credentials (user_id) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.s3
    OWNER to postgres;

-- Table: public.s3_bucket

-- DROP TABLE IF EXISTS public.s3_bucket;

CREATE TABLE IF NOT EXISTS public.s3_bucket
(
    user_id integer NOT NULL,
    bucket_id integer NOT NULL,
    object_id integer NOT NULL DEFAULT nextval('s3_bucket_object_id_seq'::regclass),
    version smallint,
    uri character varying(200) COLLATE pg_catalog."default",
    arn character varying(200) COLLATE pg_catalog."default",
    etag character varying(32) COLLATE pg_catalog."default",
    object_url character varying(200) COLLATE pg_catalog."default",
    owner character varying(25) COLLATE pg_catalog."default",
    last_modified timestamp without time zone,
    size character varying(16) COLLATE pg_catalog."default",
    type character varying(8) COLLATE pg_catalog."default",
    storage_class character varying(40) COLLATE pg_catalog."default",
    tags text[] COLLATE pg_catalog."default",
    CONSTRAINT s3_bucket_pkey PRIMARY KEY (user_id, bucket_id, object_id),
    CONSTRAINT fk_bid FOREIGN KEY (user_id, bucket_id)
        REFERENCES public.s3 (user_id, bucket_id) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    CONSTRAINT fk_uid FOREIGN KEY (user_id)
        REFERENCES public.user_credentials (user_id) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.s3_bucket
    OWNER to postgres;