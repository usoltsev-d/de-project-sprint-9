CREATE TABLE IF NOT EXISTS stg.order_events
(
    id          INT GENERATED ALWAYS AS IDENTITY,
    object_id   INT NOT NULL,
    object_type VARCHAR NOT NULL,
    sent_dttm   TIMESTAMP NOT NULL,
    payload     JSON NOT NULL,
    CONSTRAINT order_events_pkey
        PRIMARY KEY (id),
    CONSTRAINT order_events_object_id_key
        UNIQUE (object_id)
);