-- Table: public.user_credentials

-- DROP TABLE IF EXISTS public.user_credentials;

CREATE TABLE IF NOT EXISTS public.user_credentials
(
    user_id SERIAL,
    username character varying(25) NOT NULL COLLATE pg_catalog."default" NOT NULL,
    password character varying(2000) COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT user_credentials_pkey PRIMARY KEY (user_id)
);

-- Table: public.cortex

-- DROP TABLE IF EXISTS public.cortex;

CREATE TABLE IF NOT EXISTS public.cortex
(
    user_id integer NOT NULL,
    node_id SERIAL PRIMARY KEY,
    name character varying(63) NOT NULL COLLATE pg_catalog."default",
    nrn character varying(200) COLLATE pg_catalog."default",
    node_type character varying(3) COLLATE pg_catalog."default",
    node_versioning boolean,
    acl_enabled boolean,
    block_public_access boolean,
    node_key boolean,
    file_lock boolean,
    encrypt_method character varying(16) COLLATE pg_catalog."default",
    node_policy jsonb,
    tags text[] COLLATE pg_catalog."default",
    file_replication boolean,
    replication_node_id integer,
    CONSTRAINT fk_replication FOREIGN KEY (replication_node_id) REFERENCES public.cortex(node_id),
    CONSTRAINT fk_user FOREIGN KEY (user_id)
        REFERENCES public.user_credentials (user_id) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
);

-- Table: public.cortex_node

-- DROP TABLE IF EXISTS public.cortex_node;

CREATE TABLE IF NOT EXISTS public.cortex_node
(
    user_id integer NOT NULL,
    node_id integer NOT NULL,
    file_id SERIAL PRIMARY KEY,
    sub_version_id smallint,
    version_id character varying(32) COLLATE pg_catalog."default",
    uri character varying(200) COLLATE pg_catalog."default",
    nrn character varying(200) COLLATE pg_catalog."default",
    etag character varying(32) COLLATE pg_catalog."default",
    file_url character varying(200) COLLATE pg_catalog."default",
    owner character varying(25) COLLATE pg_catalog."default",
    creation_date timestamp without time zone,
    last_modified timestamp without time zone,
    size character varying(16) COLLATE pg_catalog."default",
    type character varying(8) COLLATE pg_catalog."default",
    storage_class character varying(40) COLLATE pg_catalog."default",
    tags text[] COLLATE pg_catalog."default",
    CONSTRAINT fk_bid FOREIGN KEY (node_id)
        REFERENCES public.cortex (node_id) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    CONSTRAINT fk_uid FOREIGN KEY (user_id)
        REFERENCES public.user_credentials (user_id) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS public.lifecycle_rule (
    user_id integer NOT NULL,
    node_id integer NOT NULL,
    lifecycle_id SERIAL PRIMARY KEY,
    rule_enabled boolean,
    CONSTRAINT fk_bid FOREIGN KEY (node_id)
        REFERENCES public.cortex (node_id) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    CONSTRAINT fk_uid FOREIGN KEY (user_id)
        REFERENCES public.user_credentials (user_id) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
);

ALTER TABLE cortex 
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