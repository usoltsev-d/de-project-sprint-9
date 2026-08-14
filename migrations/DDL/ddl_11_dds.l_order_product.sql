CREATE TABLE IF NOT EXISTS dds.l_order_product
(
    hk_order_product_pk UUID NOT NULL,
    h_order_pk          UUID NOT NULL,
    h_product_pk        UUID NOT NULL,
    load_dt             TIMESTAMP NOT NULL,
    load_src            VARCHAR NOT NULL,
    CONSTRAINT l_order_product_pkey
        PRIMARY KEY (hk_order_product_pk),
    CONSTRAINT l_order_product_h_order_fk
        FOREIGN KEY (h_order_pk)
        REFERENCES dds.h_order (h_order_pk),
    CONSTRAINT l_order_product_h_product_fk
        FOREIGN KEY (h_product_pk)
        REFERENCES dds.h_product (h_product_pk)
);