CREATE TABLE IF NOT EXISTS transactions (
    tx_hash         TEXT NOT NULL PRIMARY KEY,
    chain_id        BIGINT NOT NULL,
    monitor_id      BIGINT NOT NULL REFERENCES monitors(id) ON DELETE CASCADE,
    block_number    BIGINT NOT NULL,
    block_hash      TEXT NOT NULL,
    from_addr       TEXT NOT NULL,
    to_addr         TEXT NOT NULL,
    value           NUMERIC NOT NULL,
    gas_used        NUMERIC NOT NULL,
    gas_price       NUMERIC NOT NULL,
    status          SMALLINT NOT NULL,
    input_raw       BYTEA NOT NULL,
    selector        TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS transactions_chain_block_idx ON transactions (chain_id, block_number);
CREATE INDEX IF NOT EXISTS transactions_monitor_idx ON transactions (monitor_id);
