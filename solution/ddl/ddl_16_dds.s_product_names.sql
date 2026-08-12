CREATE TABLE IF NOT EXISTS dds.s_product_names
(
    h_product_pk              UUID NOT NULL,
    name                      VARCHAR NOT NULL,
    load_dt                   TIMESTAMP NOT NULL,
    load_src                  VARCHAR NOT NULL,
    hk_product_names_hashdiff UUID NOT NULL,
    CONSTRAINT s_product_names_pkey
        PRIMARY KEY (h_product_pk, load_dt),
    CONSTRAINT s_product_names_h_product_fk
        FOREIGN KEY (h_product_pk)
        REFERENCES dds.h_product (h_product_pk)
);