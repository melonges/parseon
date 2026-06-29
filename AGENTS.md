# AGENTS.md

## Build & test

- `cargo test` — unit tests only (in `src/abi/parse.rs` and `src/abi/decode.rs`); fast, no services needed.
- `cargo build --release` — release binary at `target/release/parseon`.
- No lint/format config exists (no clippy/rustfmt config, no CI). Verify with `cargo test`.

## Running the indexer

1. `docker compose up -d` — starts `postgres:16` and the Rust indexer.
2. `cp .env.example .env` — `.env` is gitignored; loaded via `dotenvy` + clap env vars.
3. Set `RPC_URL` and `CHAIN_ID` in `.env` (the defaults target Base mainnet).
4. `./target/release/parseon` — runs HTTP API + single-chain indexing coordinator.

Default `HTTP_LISTEN=0.0.0.0:8080`. Override if port is taken (e.g. `HTTP_LISTEN=0.0.0.0:8081`).

The indexer validates the RPC endpoint's chain ID at startup and only indexes
blocks returned by the RPC `finalized` tag. Base's public endpoint is
rate-limited; replace `RPC_URL` for sustained workloads.

### Per-chain deployment

Each Parseon instance indexes a single chain. For multi-chain:
```bash
# Instance 1: Base
CHAIN_ID=8453 ./target/release/parseon

# Instance 2: Ethereum
CHAIN_ID=1 ./target/release/parseon

# Instance 3: Arbitrum (different terminal)
CHAIN_ID=42161 ./target/release/parseon
```

Each instance needs its own direct `RPC_URL`. Instances may share PostgreSQL,
but monitor ownership must not overlap because every instance polls the same
`monitors` table.

## Critical gotchas

### sqlx migrations are embedded at compile time

`src/db/pool.rs` uses `sqlx::migrate!("./src/db/migrations")` which embeds migration SQL into the binary at build time. **Editing a migration file has no effect without `cargo build`.** The running binary will not see the change.

### Modifying an applied migration breaks startup

sqlx stores checksums in `_sqlx_migrations`. If you edit an already-applied migration, the app refuses to start: `migration was previously applied but has been modified`.

To reset the schema during development:
```sql
DROP TABLE IF EXISTS transactions, monitors CASCADE;
DELETE FROM _sqlx_migrations;
```
Then rebuild and restart.

### sqlx + Postgres does not support `u64`

Postgres has no unsigned BIGINT. `chain_id` is `i64` in Rust (maps to `BIGINT`) with a `CHECK (chain_id >= 0)` constraint. Do not change Rust fields to `u64` — `sqlx::Decode`/`sqlx::Type` are not implemented for `u64` with Postgres, and the build will fail.

### axum 0.8 route syntax

Path captures use `{param}`, not `:param` (the latter panics at startup with "Path segments must not start with `:`").

## Architecture

```
main.rs        config load → DB connect (+migrate) → RPC chain validation → coordinator + HTTP API
indexer/       coordinator: DB monitor load → finalized head → ordered block processing
               decode_persist: atomic tx + params + cursor persistence per block
watcher/       runtime Monitor conversion and covers()/next_block() cursor logic
abi/           runtime function-signature parsing + calldata decoding (no codegen, no sol! macro)
rpc/           alloy HTTP provider, block fetch with receipt filtering, LRU block cache
db/            monitor_repo, tx_repo, dyn_table (per-monitor result tables)
api/           axum REST: /monitors/{id}, /healthz
```

## Key design decisions

- **Single-chain per instance**: Each Parseon instance indexes one chain. `CHAIN_ID` env var selects the chain.
- **Direct RPC endpoint**: `RPC_URL` must serve the configured `CHAIN_ID` and support the `finalized` block tag.
- **Database-backed monitor state**: The coordinator reloads monitors each poll; no in-memory registry can retain stale cursors.
- **`poll_interval_ms` is a global config param** (env `POLL_INTERVAL_MS`). `batch_size` is global (env `DEFAULT_BATCH_SIZE`).
- **Per-monitor dynamic tables**: each monitor gets an `<address_without_0x>_<selector_without_0x>` table containing only decoded ABI parameter columns. Created/dropped dynamically in `db/dyn_table.rs` using `AssertSqlSafe`.
- **Monitors use a surrogate `BIGSERIAL id`** for REST endpoints (`/monitors/{id}`); result tables are keyed by address and selector.
- **Atomic block persistence**: transaction metadata, decoded parameter rows, and all covering monitor cursors commit in one PostgreSQL transaction.
