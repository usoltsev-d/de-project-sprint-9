CREATE TABLE IF NOT EXISTS dds.h_product
(
    h_product_pk UUID NOT NULL,
    product_id   VARCHAR NOT NULL,
    load_dt      TIMESTAMP NOT NULL,
    load_src     VARCHAR NOT NULL,
    CONSTRAINT h_product_pkey
        PRIMARY KEY (h_product_pk),
    CONSTRAINT h_product_product_id_key
        UNIQUE (product_id)
);