CREATE TABLE IF NOT EXISTS dds.s_restaurant_names
(
    h_restaurant_pk              UUID NOT NULL,
    name                         VARCHAR NOT NULL,
    load_dt                      TIMESTAMP NOT NULL,
    load_src                     VARCHAR NOT NULL,
    hk_restaurant_names_hashdiff UUID NOT NULL,
    CONSTRAINT s_restaurant_names_pkey
        PRIMARY KEY (h_restaurant_pk, load_dt),
    CONSTRAINT s_restaurant_names_h_restaurant_fk
        FOREIGN KEY (h_restaurant_pk)
        REFERENCES dds.h_restaurant (h_restaurant_pk)
);