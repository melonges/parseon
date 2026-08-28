-- v1 canonical ledger. Existing v0.8 result rows do not carry block hashes and
-- cannot be mapped safely to a fork, so fail closed instead of guessing.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM monitors)
     OR EXISTS (
       SELECT 1 FROM pg_tables
       WHERE schemaname = current_schema()
         AND tablename ~ '^monitor_[0-9]+_results$'
     ) THEN
    RAISE EXCEPTION
      'v1 canonical ledger requires an empty legacy monitor/result state; backup and reset/reindex before upgrade';
  END IF;
END $$;

CREATE TABLE canonical_blocks (
    chain_id        BIGINT NOT NULL REFERENCES chains(chain_id) ON DELETE CASCADE,
    block_number    BIGINT NOT NULL CHECK (block_number >= 0),
    block_hash      BYTEA NOT NULL CHECK (octet_length(block_hash) = 32),
    parent_hash     BYTEA NOT NULL CHECK (octet_length(parent_hash) = 32),
    block_timestamp BIGINT NOT NULL CHECK (block_timestamp >= 0),
    finality        TEXT NOT NULL CHECK (finality IN ('provisional', 'finalized')),
    PRIMARY KEY (chain_id, block_number),
    UNIQUE (chain_id, block_hash)
);

CREATE INDEX canonical_blocks_chain_hash_idx
    ON canonical_blocks (chain_id, block_hash);
CREATE INDEX canonical_blocks_finality_idx
    ON canonical_blocks (chain_id, finality, block_number);
