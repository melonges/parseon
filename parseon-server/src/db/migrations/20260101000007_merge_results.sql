-- Merge transaction metadata into per-monitor result tables and remove the
-- separate transactions table.
--
-- Each monitor's result table (<address>_<selector>) now stores the full
-- transaction row (tx_hash, block, from/to, value, gas, status, input_raw)
-- alongside the decoded ABI parameter columns. The table is created by
-- dyn_table::create_result_table when a monitor is added.
--
-- Existing result tables used a params-only schema with no link back to the
-- transactions table, so they cannot be migrated in place. Drop them and
-- reset monitors; users re-create monitors to rebuild result tables.
DO $$
DECLARE t RECORD;
BEGIN
  FOR t IN
    SELECT tablename FROM pg_tables
    WHERE schemaname = current_schema()
      AND tablename ~ '^[0-9a-f]{40}_[0-9a-f]{8}$'
  LOOP
    EXECUTE format('DROP TABLE IF EXISTS %I CASCADE', t.tablename);
  END LOOP;
END $$;

DROP TABLE IF EXISTS transactions CASCADE;

TRUNCATE TABLE monitors RESTART IDENTITY CASCADE;
