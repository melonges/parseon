ALTER TABLE monitors
  DROP CONSTRAINT monitors_chain_kind_address_signature_hash_key;

CREATE INDEX monitors_target_idx
  ON monitors (chain_id, kind, address, signature_hash);
