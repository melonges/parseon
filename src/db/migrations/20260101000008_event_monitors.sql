-- v0.4 intentionally resets v0.3 monitor state and result tables. Recreated
-- per-monitor tables store only minimal result identity and decoded ABI values.
DO $$
DECLARE t RECORD;
BEGIN
  FOR t IN SELECT tablename FROM pg_tables
    WHERE schemaname = current_schema()
      AND (tablename ~ '^[0-9a-f]{40}_[0-9a-f]{8}$' OR tablename ~ '^monitor_[0-9]+_results$')
  LOOP EXECUTE format('DROP TABLE IF EXISTS %I CASCADE', t.tablename); END LOOP;
END $$;

TRUNCATE TABLE monitors RESTART IDENTITY CASCADE;
ALTER TABLE monitors DROP CONSTRAINT IF EXISTS monitors_address_selector_key;
ALTER TABLE monitors RENAME COLUMN selector TO signature_hash;
ALTER TABLE monitors ADD COLUMN kind TEXT NOT NULL DEFAULT 'call'
  CHECK (kind IN ('call', 'event'));
ALTER TABLE monitors ADD CONSTRAINT monitors_kind_address_signature_hash_key
  UNIQUE (kind, address, signature_hash);
