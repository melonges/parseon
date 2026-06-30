# AGENTS.md

## Build & test

- `cargo test` — unit and HTTP router tests; fast, no services needed.
- `cargo build --release` — release binary at `target/release/parseon`.
- No lint/format config exists (no clippy/rustfmt config, no CI). Verify with `cargo test`.
- Don't run build commands by yourself, ask me and I'll run and give you output

## Git commits

- Use Conventional Commits: `<type>[optional scope][!]: <description>`.
- Keep the description imperative, lowercase, and concise; use `!` only for intentional breaking changes.
- Prefer one coherent change per commit. Common types are `feat`, `fix`, `refactor`, `docs`, `test`, `build`, and `chore`.

## Development stage

- Parseon is in an early stage of development. Breaking changes are allowed when they improve the design.
- Do not preserve legacy APIs, compatibility layers, deprecated paths, or transitional code unless explicitly requested.
- Update all affected code, tests, docs, examples, and migrations together so the repository represents only the current design.

## Running Parseon

1. `docker compose up -d` — starts PostgreSQL 16 on `localhost:5432`.
2. `cp .env.example .env` — `.env` is gitignored; loaded via `dotenvy` + clap env vars.
3. Set `RPC_URL` and `CHAIN_ID` in `.env` (the defaults target Base mainnet).
4. Run the Parseon app on the host. Its default `DATABASE_URL` connects to the
   Compose PostgreSQL instance.

PostgreSQL data is retained in the `pgdata` named volume. Check database logs
with `docker compose logs -f postgres`. The Dockerfile remains available for
building a standalone production image with `docker build -t parseon .`.

Default `HTTP_LISTEN=0.0.0.0:8080`. Override if port is taken (e.g. `HTTP_LISTEN=0.0.0.0:8081`).

Swagger UI is served at `/swagger-ui/`; the generated OpenAPI document is at
`/api-docs/openapi.json`.

The indexer validates the RPC endpoint's chain ID at startup and only indexes
blocks returned by the RPC `finalized` tag. Base's public endpoint is
rate-limited; replace `RPC_URL` for sustained workloads.

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

## Terminology

Follow `terminology.md` for new code, API names, docs, and roadmap updates.

Preferred project vocabulary:

- Use **Monitor**, not **Watcher**, for the user-defined indexing rule.
- Use **Target** for the chain/address/selector/signature matched by a monitor.
- Use **Filter** for optional post-decode conditions.
- Use **Cursor** for per-monitor indexing progress.
- Use **BlockSource**, not generic **Provider**, for core chain-data abstractions.
- Use **Storage** for primary persisted state and queryable decoded results.
- Use **Cache** for temporary block/receipt caching.
- Use **Worker** for a runtime indexing task, usually one per chain.
- Use **DecodedCall** in core, **ResultRecord** in storage, and **MonitorResult** in API responses.
- Use **Adapter**, not **Plugin**, until there is a real need for runtime-loaded extensions.
- Use **Sink** for optional output destinations such as Kafka, webhooks, files, or ClickHouse.

Some implementation files may still use library-specific provider terminology internally. New core abstractions should use the terminology above.

## Architecture

```
main.rs        config load → DB connect (+migrate) → source chain validation → worker + HTTP API
core/          storage-neutral Chain, Target, Cursor, DecodedCall, and source data models
monitor/       runtime Monitor model and matching/range helpers
filter/        Filter model; v0.2 supports the behavior-preserving All variant
cache/         BlockCache trait and chain-aware in-memory LRU implementation
storage/       Storage trait and atomic BlockCommit contract
worker/        monitor reload → finalized head → scheduled ordered block processing
indexer/       transaction matching and calldata decoding into DecodedCall values
scheduler/     pure block-range planning and deduplication
abi/           runtime function-signature parsing + calldata decoding (no codegen, no sol! macro)
rpc/           BlockSource trait and Alloy JSON-RPC implementation
db/            PostgresStorage, monitor_repo, dyn_table (per-monitor result tables)
api/           axum REST + OpenAPI/Swagger UI: /monitors/{id}, /healthz, /swagger-ui/
```

Future architecture should move toward:

```
parseon-core   monitor targets, filters, decoded calls, scheduler, workers, reorg/finality logic
block-source   JSON-RPC / eRPC / Etherscan source adapters
storage        PostgreSQL first, MongoDB later
cache          memory first, Redis later
server         HTTP/OpenAPI now, GraphQL and management frontend later
sinks          optional webhooks, Kafka, files, ClickHouse
```

## Key design decisions

- **Single-chain per instance**: Each Parseon instance indexes one chain. `CHAIN_ID` env var selects the chain.
- **Direct RPC endpoint**: `RPC_URL` must serve the configured `CHAIN_ID` and support the `finalized` block tag.
- **Database-backed monitor state**: The worker reloads monitors each poll; no in-memory registry can retain stale cursors.
- **`poll_interval_ms` is a global config param** (env `POLL_INTERVAL_MS`). `batch_size` is global (env `DEFAULT_BATCH_SIZE`).
- **Per-monitor dynamic tables**: each monitor gets an `<address_without_0x>_<selector_without_0x>` table containing only decoded ABI parameter columns. Created/dropped dynamically in `db/dyn_table.rs` using `AssertSqlSafe`.
- **Monitors use a surrogate `BIGSERIAL id`** for REST endpoints (`/monitors/{id}`); result tables are keyed by address and selector.
- **Atomic block persistence**: transaction metadata, decoded parameter rows, and all covering monitor cursors commit in one PostgreSQL transaction.
