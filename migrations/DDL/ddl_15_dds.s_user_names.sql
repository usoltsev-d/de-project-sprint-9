CREATE TABLE IF NOT EXISTS dds.s_user_names
(
    h_user_pk              UUID NOT NULL,
    username               VARCHAR NOT NULL,
    userlogin              VARCHAR NOT NULL,
    load_dt                TIMESTAMP NOT NULL,
    load_src               VARCHAR NOT NULL,
    hk_user_names_hashdiff UUID NOT NULL,
    CONSTRAINT s_user_names_pkey
        PRIMARY KEY (h_user_pk, load_dt),
    CONSTRAINT s_user_names_h_user_fk
        FOREIGN KEY (h_user_pk)
        REFERENCES dds.h_user (h_user_pk)
);