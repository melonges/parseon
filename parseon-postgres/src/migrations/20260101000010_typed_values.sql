-- Binary EVM identities and unsigned-domain constraints. Existing monitors and
-- dynamic results are intentionally reset because result table schemas vary by
-- monitor and are rebuilt by the indexer.
DO $$
DECLARE t RECORD;
BEGIN
  FOR t IN SELECT tablename FROM pg_tables
    WHERE schemaname = current_schema()
      AND tablename ~ '^monitor_[0-9]+_results$'
  LOOP EXECUTE format('DROP TABLE IF EXISTS %I CASCADE', t.tablename); END LOOP;
END $$;

TRUNCATE TABLE monitors RESTART IDENTITY CASCADE;

ALTER TABLE monitors
  DROP CONSTRAINT IF EXISTS monitors_chain_kind_address_signature_hash_key;

ALTER TABLE monitors
  ALTER COLUMN address TYPE BYTEA USING decode(substring(address FROM 3), 'hex'),
  ALTER COLUMN signature_hash TYPE BYTEA USING decode(substring(signature_hash FROM 3), 'hex');

ALTER TABLE monitors
  ADD CONSTRAINT monitors_id_positive CHECK (id > 0),
  ADD CONSTRAINT monitors_address_length CHECK (octet_length(address) = 20),
  ADD CONSTRAINT monitors_signature_hash_length CHECK (
    (kind = 'call' AND octet_length(signature_hash) = 4)
    OR (kind = 'event' AND octet_length(signature_hash) = 32)
  ),
  ADD CONSTRAINT monitors_start_block_nonnegative CHECK (start_block >= 0),
  ADD CONSTRAINT monitors_end_block_valid CHECK (end_block IS NULL OR end_block >= start_block),
  ADD CONSTRAINT monitors_cursor_nonnegative CHECK (cursor IS NULL OR cursor >= 0),
  ADD CONSTRAINT monitors_chain_kind_address_signature_hash_key
    UNIQUE (chain_id, kind, address, signature_hash);
