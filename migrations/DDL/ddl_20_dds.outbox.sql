DO $$
BEGIN
    CREATE TABLE IF NOT EXISTS dds.outbox
    (
        id          BIGINT GENERATED ALWAYS AS IDENTITY,
        order_id    UUID      NOT NULL,
        payload     JSONB     NOT NULL,
        created_at  TIMESTAMP NOT NULL,
        sent_at     TIMESTAMP NULL,
        CONSTRAINT outbox_pkey
            PRIMARY KEY (id),
        CONSTRAINT outbox_order_id_key
            UNIQUE (order_id)
    );

    CREATE INDEX IF NOT EXISTS ix_outbox_unsent
        ON dds.outbox (created_at)
        WHERE sent_at IS NULL;
END
$$;