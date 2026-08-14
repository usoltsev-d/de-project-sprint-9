CREATE TABLE IF NOT EXISTS dds.h_user
(
    h_user_pk UUID NOT NULL,
    user_id   VARCHAR NOT NULL,
    load_dt   TIMESTAMP NOT NULL,
    load_src  VARCHAR NOT NULL,
    CONSTRAINT h_user_pkey
        PRIMARY KEY (h_user_pk),
    CONSTRAINT h_user_user_id_key
        UNIQUE (user_id)
);