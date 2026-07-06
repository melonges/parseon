-- v0.5 intentionally resets all v0.4 monitors and dynamic result tables.
DO $$
DECLARE t RECORD;
BEGIN
  FOR t IN SELECT tablename FROM pg_tables
    WHERE schemaname = current_schema()
      AND tablename ~ '^monitor_[0-9]+_results$'
  LOOP EXECUTE format('DROP TABLE IF EXISTS %I CASCADE', t.tablename); END LOOP;
END $$;

TRUNCATE TABLE monitors RESTART IDENTITY CASCADE;

CREATE TABLE chains (
    chain_id        BIGINT PRIMARY KEY CHECK (chain_id >= 0),
    rpc_url         TEXT NOT NULL,
    enabled         BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE monitors
    DROP CONSTRAINT IF EXISTS monitors_kind_address_signature_hash_key;
ALTER TABLE monitors
    ADD COLUMN chain_id BIGINT NOT NULL REFERENCES chains(chain_id) ON DELETE CASCADE;
ALTER TABLE monitors
    ADD CONSTRAINT monitors_chain_kind_address_signature_hash_key
    UNIQUE (chain_id, kind, address, signature_hash);

CREATE INDEX monitors_chain_id_idx ON monitors (chain_id);
