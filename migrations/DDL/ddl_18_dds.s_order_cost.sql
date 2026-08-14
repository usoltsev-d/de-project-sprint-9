CREATE TABLE IF NOT EXISTS dds.s_order_cost
(
    h_order_pk             UUID NOT NULL,
    cost                   DECIMAL(19, 5) NOT NULL,
    payment                DECIMAL(19, 5) NOT NULL,
    load_dt                TIMESTAMP NOT NULL,
    load_src               VARCHAR NOT NULL,
    hk_order_cost_hashdiff UUID NOT NULL,
    CONSTRAINT s_order_cost_pkey
        PRIMARY KEY (h_order_pk, load_dt),
    CONSTRAINT s_order_cost_h_order_fk
        FOREIGN KEY (h_order_pk)
        REFERENCES dds.h_order (h_order_pk),
    CONSTRAINT s_order_cost_cost_check
        CHECK (cost >= 0),
    CONSTRAINT s_order_cost_payment_check
        CHECK (payment >= 0)
);