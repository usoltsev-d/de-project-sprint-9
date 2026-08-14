CREATE TABLE IF NOT EXISTS cdm.user_category_counters
(
    id            INT GENERATED ALWAYS AS IDENTITY,
    user_id       UUID NOT NULL,
    category_id   UUID NOT NULL,
    category_name VARCHAR NOT NULL,
    order_cnt     INT NOT NULL,
    CONSTRAINT user_category_counters_pkey
        PRIMARY KEY (id),
    CONSTRAINT user_category_counters_order_cnt_check
        CHECK (order_cnt >= 0),
    CONSTRAINT user_category_counters_user_id_category_id_key
        UNIQUE (user_id, category_id)
);