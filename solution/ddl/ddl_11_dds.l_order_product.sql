CREATE TABLE IF NOT EXISTS dds.h_order
(
    h_order_pk UUID NOT NULL,
    order_id   INT NOT NULL,
    order_dt   TIMESTAMP NOT NULL,
    load_dt    TIMESTAMP NOT NULL,
    load_src   VARCHAR NOT NULL,
    CONSTRAINT h_order_pkey
        PRIMARY KEY (h_order_pk),
    CONSTRAINT h_order_order_id_key
        UNIQUE (order_id)
);