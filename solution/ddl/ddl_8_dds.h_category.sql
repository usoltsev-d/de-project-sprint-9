CREATE TABLE IF NOT EXISTS dds.h_category
(
    h_category_pk UUID NOT NULL,
    category_name VARCHAR NOT NULL,
    load_dt       TIMESTAMP NOT NULL,
    load_src      VARCHAR NOT NULL,
    CONSTRAINT h_category_pkey
        PRIMARY KEY (h_category_pk),
    CONSTRAINT h_category_category_name_key
        UNIQUE (category_name)
);