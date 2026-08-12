CREATE TABLE IF NOT EXISTS dds.l_product_restaurant
(
    hk_product_restaurant_pk UUID NOT NULL,
    h_product_pk              UUID NOT NULL,
    h_restaurant_pk           UUID NOT NULL,
    load_dt                   TIMESTAMP NOT NULL,
    load_src                  VARCHAR NOT NULL,
    CONSTRAINT l_product_restaurant_pkey
        PRIMARY KEY (hk_product_restaurant_pk),
    CONSTRAINT l_product_restaurant_h_product_fk
        FOREIGN KEY (h_product_pk)
        REFERENCES dds.h_product (h_product_pk),
    CONSTRAINT l_product_restaurant_h_restaurant_fk
        FOREIGN KEY (h_restaurant_pk)
        REFERENCES dds.h_restaurant (h_restaurant_pk)
);