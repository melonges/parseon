CREATE TABLE IF NOT EXISTS monitors (
    id              SERIAL PRIMARY KEY,
    chain_id        BIGINT NOT NULL REFERENCES chains(id) ON DELETE CASCADE,
    address         TEXT NOT NULL,
    name            TEXT NOT NULL,
    signature       TEXT NOT NULL,
    selector        TEXT NOT NULL,
    input_types     TEXT NOT NULL,
    param_schema    JSONB NOT NULL,
    start_block     BIGINT NOT NULL,
    end_block       BIGINT NULL,
    cursor          BIGINT NULL,
    completed       BOOLEAN NOT NULL DEFAULT FALSE,
    enabled         BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (chain_id, address, selector)
);

CREATE INDEX IF NOT EXISTS monitors_chain_id_idx ON monitors (chain_id);
