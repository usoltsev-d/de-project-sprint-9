CREATE TABLE IF NOT EXISTS dds.h_restaurant
(
    h_restaurant_pk UUID NOT NULL,
    restaurant_id   VARCHAR NOT NULL,
    load_dt         TIMESTAMP NOT NULL,
    load_src        VARCHAR NOT NULL,
    CONSTRAINT h_restaurant_pkey
        PRIMARY KEY (h_restaurant_pk),
    CONSTRAINT h_restaurant_restaurant_id_key
        UNIQUE (restaurant_id)
);