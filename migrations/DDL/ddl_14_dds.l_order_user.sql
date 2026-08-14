CREATE TABLE IF NOT EXISTS dds.l_order_user
(
    hk_order_user_pk UUID NOT NULL,
    h_order_pk       UUID NOT NULL,
    h_user_pk        UUID NOT NULL,
    load_dt          TIMESTAMP NOT NULL,
    load_src         VARCHAR NOT NULL,
    CONSTRAINT l_order_user_pkey
        PRIMARY KEY (hk_order_user_pk),
    CONSTRAINT l_order_user_h_order_fk
        FOREIGN KEY (h_order_pk)
        REFERENCES dds.h_order (h_order_pk),
    CONSTRAINT l_order_user_h_user_fk
        FOREIGN KEY (h_user_pk)
        REFERENCES dds.h_user (h_user_pk)
);