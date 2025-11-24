CREATE TABLE IF NOT EXISTS public.user_credentials
(
    user_id SERIAL,
    username character varying(25) NOT NULL COLLATE pg_catalog."default" NOT NULL,
    password character varying(2000) COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT user_credentials_pkey PRIMARY KEY (user_id)
);

CREATE TABLE IF NOT EXISTS public.cortex
(
    user_id integer NOT NULL,
    node_id SERIAL PRIMARY KEY,
    name character varying(63) NOT NULL COLLATE pg_catalog."default",
    nrn character varying(200) COLLATE pg_catalog."default",
    tags text[] COLLATE pg_catalog."default",
    CONSTRAINT fk_user FOREIGN KEY (user_id)
        REFERENCES public.user_credentials (user_id) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS public.cortex_node
(
    user_id integer NOT NULL,
    node_id integer NOT NULL,
    file_id SERIAL PRIMARY KEY,
    uri character varying(200) COLLATE pg_catalog."default",
    nrn character varying(200) COLLATE pg_catalog."default",
    hdfs_path character varying(200) COLLATE pg_catalog."default",
    owner character varying(25) COLLATE pg_catalog."default",
    creation_date timestamp without time zone,
    last_modified timestamp without time zone,
    size character varying(16) COLLATE pg_catalog."default",
    file_extension character varying(8) COLLATE pg_catalog."default",
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

CREATE TABLE IF NOT EXISTS public.synapse_qf (
    user_id integer NOT NULL,
    synapse_qf_id SERIAL PRIMARY KEY,
    name character varying(50) COLLATE pg_catalog."default",
    in_srv_type character varying(20) COLLATE pg_catalog."default",
    in_srv_id smallint,
    in_trig_type character varying(8) COLLATE pg_catalog."default",
    in_trig_object character varying(16) COLLATE pg_catalog."default",
    out_srv_type character varying(20) COLLATE pg_catalog."default",
    out_srv_id smallint,
    out_trig_type character varying(8) COLLATE pg_catalog."default",
    out_trig_object character varying(16) COLLATE pg_catalog."default",
    src_file character varying(100) COLLATE pg_catalog."default",
    timeout numeric(4,0),
    enabled boolean,
    CONSTRAINT synapse_timeout_check CHECK (timeout <= 1500::numeric),
    CONSTRAINT fk_uid FOREIGN KEY (user_id)
        REFERENCES public.user_credentials (user_id) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS public.logging (
    log_id SERIAL PRIMARY KEY,
    created_timestamp timestamp,
    description varchar(100),
    user_id integer NOT NULL,
    service varchar(30),
    action varchar(8),
    host varchar(30),
    host_details jsonb,
    object varchar(30),
    object_details jsonb,
    synapse_processed boolean,
    CONSTRAINT fk_uid FOREIGN KEY (user_id)
        REFERENCES public.user_credentials (user_id) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
)
