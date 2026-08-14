CREATE TABLE IF NOT EXISTS cdm.user_product_counters
(
    id           INT GENERATED ALWAYS AS IDENTITY,
    user_id      UUID NOT NULL,
    product_id   UUID NOT NULL,
    product_name VARCHAR NOT NULL,
    order_cnt    INT NOT NULL,
    CONSTRAINT user_product_counters_pkey
        PRIMARY KEY (id),
    CONSTRAINT user_product_counters_order_cnt_check
        CHECK (order_cnt >= 0),
    CONSTRAINT user_product_counters_user_id_product_id_key
        UNIQUE (user_id, product_id)
);