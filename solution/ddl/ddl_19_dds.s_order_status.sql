CREATE TABLE IF NOT EXISTS dds.s_order_status
(
    h_order_pk               UUID NOT NULL,
    status                   VARCHAR NOT NULL,
    load_dt                  TIMESTAMP NOT NULL,
    load_src                 VARCHAR NOT NULL,
    hk_order_status_hashdiff UUID NOT NULL,
    CONSTRAINT s_order_status_pkey
        PRIMARY KEY (h_order_pk, load_dt),
    CONSTRAINT s_order_status_h_order_fk
        FOREIGN KEY (h_order_pk)
        REFERENCES dds.h_order (h_order_pk)
);