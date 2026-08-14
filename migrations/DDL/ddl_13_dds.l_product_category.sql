CREATE TABLE IF NOT EXISTS dds.l_product_category
(
    hk_product_category_pk UUID NOT NULL,
    h_product_pk            UUID NOT NULL,
    h_category_pk           UUID NOT NULL,
    load_dt                 TIMESTAMP NOT NULL,
    load_src                VARCHAR NOT NULL,
    CONSTRAINT l_product_category_pkey
        PRIMARY KEY (hk_product_category_pk),
    CONSTRAINT l_product_category_h_product_fk
        FOREIGN KEY (h_product_pk)
        REFERENCES dds.h_product (h_product_pk),
    CONSTRAINT l_product_category_h_category_fk
        FOREIGN KEY (h_category_pk)
        REFERENCES dds.h_category (h_category_pk)
);