# AGENTS.md

## Build & test

- `cargo test` — unit tests only (in `src/abi/parse.rs` and `src/abi/decode.rs`); fast, no services needed.
- `cargo build --release` — release binary at `target/release/evm-indexer`.
- No lint/format config exists (no clippy/rustfmt config, no CI). Verify with `cargo test`.

## Running the indexer

1. `docker compose up -d` — starts `postgres:16`, `erpc` (port 4000), and `anvil` (port 8545, 1s block time).
2. `cp .env.example .env` — `.env` is gitignored; loaded via `dotenvy` + clap env vars.
3. Set `CHAIN_ID` in `.env` (e.g. `CHAIN_ID=1` for Ethereum, `CHAIN_ID=42161` for Arbitrum).
4. `./target/release/evm-indexer` — runs HTTP API + single-chain indexing coordinator.

Default `HTTP_LISTEN=0.0.0.0:8080`. Override if port is taken (e.g. `HTTP_LISTEN=0.0.0.0:8081`).

### Per-chain deployment

Each evm-indexer instance indexes a single chain. For multi-chain:
```bash
# Instance 1: Ethereum
CHAIN_ID=1 ./target/release/evm-indexer

# Instance 2: Arbitrum (different terminal)
CHAIN_ID=42161 ./target/release/evm-indexer
```

All instances share the same erpc and postgres.

### erpc Configuration

erpc is configured via `erpc.yaml` in the project root. Key settings:
- **Providers**: Add API keys to `.env` (e.g. `ALCHEMY_API_KEY`) and reference them in `erpc.yaml`
- **Networks**: Add/remove chain IDs in the `projects[].networks[]` section of `erpc.yaml`
- **Caching**: Memory-based caching with finality-aware TTLs (finalized = permanent, realtime = 2s)
- **Metrics**: Prometheus metrics available at `:4001/metrics`

### Chain URL Format

With erpc, chains are accessed via URLs like:
- `http://erpc:4000/main/evm/1` (Ethereum mainnet)
- `http://erpc:4000/main/evm/42161` (Arbitrum)

The indexer's `ERPC_URL` should be set to `http://erpc:4000/main/evm` (base path), and the coordinator appends `/{chain_id}` automatically.

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

### anvil docker command must be a single string

The foundry image entrypoint is `/bin/sh -c`. Passing flags as separate array elements drops them (anvil binds `127.0.0.1` only). The command must be a single-element array: `["anvil --host 0.0.0.0 --port 8545 --block-time 1"]`.

## Architecture

```
main.rs        config load → DB connect (+migrate) → registry reload → coordinator + HTTP API
indexer/       coordinator: poll loop (fetch blocks → match monitors → decode calldata → persist)
               decode_persist: ABI decode + insert tx + insert params into dynamic table
watcher/       registry: in-memory monitors keyed by blockchain chain_id, reloaded on API mutations
                model: Monitor struct with covers()/next_block() cursor logic
abi/           runtime function-signature parsing + calldata decoding (no codegen, no sol! macro)
rpc/           alloy HTTP provider, block fetch with receipt filtering, LRU block cache
db/            monitor_repo, tx_repo, dyn_table (per-monitor params tables)
api/           axum REST: /monitors/{id}, /healthz, /metrics
erpc/          fault-tolerant RPC proxy with caching, failover, retries (separate container)
```

## Key design decisions

- **Single-chain per instance**: Each evm-indexer instance indexes one chain. `CHAIN_ID` env var selects the chain.
- **Single erpc endpoint**: All chains route through erpc at `{ERPC_URL}/{chain_id}`.
- **The registry and coordinator key everything by blockchain `chain_id`**, not a database row id.
- **`poll_interval_ms` is a global config param** (env `POLL_INTERVAL_MS`). `batch_size` is global (env `DEFAULT_BATCH_SIZE`).
- **Per-monitor dynamic tables**: each monitor gets a `params_{monitor_id}` table with columns derived from the function signature's param types. Created/dropped dynamically in `db/dyn_table.rs` using `AssertSqlSafe`.
- **Monitors use a surrogate `BIGSERIAL id`** for REST endpoints (`/monitors/{id}`) and dynamic table naming.
- **erpc provides**: fault-tolerant RPC with failover, re-org-aware permanent caching, rate limiting, circuit breakers, hedged requests.
