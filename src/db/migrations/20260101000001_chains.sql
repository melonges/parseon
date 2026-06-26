CREATE TABLE IF NOT EXISTS chains (
    id              SERIAL PRIMARY KEY,
    name            TEXT NOT NULL,
    chain_id        BIGINT NOT NULL UNIQUE,
    rpc_url         TEXT NOT NULL,
    start_block     BIGINT NOT NULL DEFAULT 0,
    poll_interval_ms INT  NOT NULL DEFAULT 2000,
    batch_size      INT  NOT NULL DEFAULT 10,
    enabled         BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
