-- The UNIQUE (chain_id, block_hash) constraint in migration 14 already owns
-- the required fork-safe lookup index. Remove the duplicate named index without
-- editing the applied canonical-ledger migration.
DROP INDEX IF EXISTS canonical_blocks_chain_hash_idx;
