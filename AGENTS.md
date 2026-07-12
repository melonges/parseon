# AGENTS.md

## Build & test

- `rtk cargo fmt --all -- --check` — verify the repository's stable Rustfmt policy.
- `rtk cargo clippy -q --workspace --all-targets --all-features --message-format=short -- -D warnings` — enforce workspace Rust and Clippy lints.
- `rtk cargo test -q --workspace --all-features --message-format=short` — unit and HTTP router tests; fast, no services needed.
- `rtk cargo build -q --release --message-format=short` — release binary at `target/release/parseon`.
- Agents may run all verification commands. Prefer Cargo's `-q` flag to suppress successful compilation progress while preserving diagnostics and test failures.
- No CI exists; run format, lint, and test checks before handing off code changes.

## Git commits

- Use Conventional Commits: `<type>[optional scope][!]: <description>`.
- Keep the description imperative, lowercase, and concise; use `!` only for intentional breaking changes.
- Prefer one coherent change per commit. Common types are `feat`, `fix`, `refactor`, `docs`, `test`, `build`, and `chore`.

## Changelog

- Maintain `CHANGELOG.md` for every notable user-, operator-, API-, architecture-, performance-, or contributor-facing change.
- Add entries under `Unreleased` in the same change; do not create a version or release date unless explicitly requested.
- Use applicable `Added`, `Changed`, `Fixed`, and `Breaking` headings and describe outcomes rather than implementation details.
- Keep each entry concise and independently understandable. Omit formatting-only changes and incidental maintenance with no meaningful impact.

## Releases

- Support major, minor, and patch releases according to Semantic Versioning: breaking changes require a major bump, backward-compatible features a minor bump, and backward-compatible fixes a patch bump.
- Before `1.0.0`, use a minor bump for breaking changes and a patch bump for backward-compatible changes, consistent with Parseon's existing `0.x` release history.
- When explicitly asked to prepare a release, update the workspace version and `Cargo.lock`, move `Unreleased` entries under `## <version> - <YYYY-MM-DD>`, and leave a fresh empty `Unreleased` section.
- Use `chore(release): prepare v<version>` for the release commit. Do not publish, tag, push, or create a GitHub release unless explicitly requested.

## Development stage

- Parseon is in an early stage of development. Breaking changes are allowed when they improve the design.
- Do not preserve legacy APIs, compatibility layers, deprecated paths, or transitional code unless explicitly requested.
- Update all affected code, tests, docs, examples, and migrations together so the repository represents only the current design.

## Running Parseon

1. `docker compose up -d` — starts PostgreSQL 16 on `localhost:5432`.
2. `cp .env.example .env` — `.env` is gitignored; loaded via `dotenvy` + clap env vars.
3. Run the Parseon app on the host. Its default `DATABASE_URL` connects to the
   Compose PostgreSQL instance.
4. Register RPC endpoints with `POST /chains`. Parseon discovers and stores each endpoint's chain ID.

PostgreSQL data is retained in the `pgdata` named volume. Check database logs
with `docker compose logs -f postgres`. The Dockerfile remains available for
building a standalone production image with `docker build -t parseon .`.

Default `HTTP_LISTEN=0.0.0.0:8080`. Override if port is taken (e.g. `HTTP_LISTEN=0.0.0.0:8081`).

Swagger UI is served at `/swagger-ui/`; the generated OpenAPI document is at
`/api-docs/openapi.json`.
Prometheus-compatible metrics are served at `/metrics`.

The chain API validates each RPC endpoint's chain ID and `finalized` tag before
registration. The supervisor runs one finalized-only worker per enabled chain.
Base's public endpoint is rate-limited; register a private endpoint for sustained workloads.

### sqlx migrations are embedded at compile time

`parseon-postgres/src/pool.rs` uses `sqlx::migrate!("./src/migrations")` which embeds migration SQL into the binary at build time. **Editing a migration file has no effect without `cargo build`.** The running binary will not see the change.

### Modifying an applied migration breaks startup

sqlx stores checksums in `_sqlx_migrations`. If you edit an already-applied migration, the app refuses to start: `migration was previously applied but has been modified`.

To reset the schema during development:
```sql
DROP TABLE IF EXISTS transactions, monitors, chains CASCADE;
DELETE FROM _sqlx_migrations;
```
Then rebuild and restart.

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
- Use **DecodedCall** and **DecodedEvent** in core, **ResultRecord** in storage, and **MonitorResult** in API responses.
- Use **Adapter**, not **Plugin**, until there is a real need for runtime-loaded extensions.
- Use **Sink** for optional output destinations such as Kafka, webhooks, files, or ClickHouse.

Some implementation files may still use library-specific provider terminology internally. New core abstractions should use the terminology above.

## Architecture

```text
parseon-server
├── parseon-core
├── parseon-rpc ──────────> parseon-core
├── parseon-postgres ─────> parseon-core
└── parseon-memory-cache ─> parseon-core
```

- `parseon-core`: domain models, ABI decoding, commands, views, application services, workers, supervisor, and ports.
- `parseon-rpc`: Alloy JSON-RPC `BlockSource` adapter, receipt batching, and log fetching.
- `parseon-postgres`: SQLx repositories, dynamic result tables, migrations, and atomic block commits.
- `parseon-memory-cache`: chain-aware LRU `BlockCache` and per-worker factory.
- `parseon-server`: grouped CLI/env configuration, Axum/OpenAPI, Prometheus telemetry, and dependency wiring.

Core must not depend on the server or any adapter crate. HTTP handlers call core application services and serialize core-derived views; adapters implement core ports.

## Key design decisions

- **Database-backed chain registry**: Each enabled chain has one isolated worker, source, cache, cancellation token, and status record.
- **Direct RPC endpoints**: Registered endpoints determine their EIP-155 chain IDs and must support the `finalized` block tag.
- **Write-only RPC URLs**: Provider endpoints are persisted for workers but never returned or logged.
- **Database-backed monitor state**: The worker reloads monitors each poll; no in-memory registry can retain stale cursors.
- **`poll_interval_ms` is a global config param** (env `POLL_INTERVAL_MS`). `batch_size` is global (env `DEFAULT_BATCH_SIZE`).
- **Bounded indexing**: `BLOCK_CONCURRENCY` and `RPC_REQUEST_CONCURRENCY` apply per chain; `DB_WRITE_CONCURRENCY` limits atomic commits across the process; `RPC_BATCH_SIZE` controls targeted receipt batches.
- **Chain-scoped monitors**: Each monitor belongs to one immutable registered chain; identical targets may exist on different chains.
- **Per-monitor dynamic tables**: each monitor gets a `monitor_<id>_results` table containing minimal result identity and decoded ABI parameter columns. PostgreSQL column names and types are derived inside `parseon-postgres/src/dyn_table.rs`; they are not part of the core ABI model.
- **Monitors use a surrogate `BIGSERIAL id`** for REST endpoints (`/monitors/{id}`) and result-table names.
- **Atomic block persistence**: decoded call/event rows and all covering monitor cursors commit in one PostgreSQL transaction.

## Roadmap

See `roadmap.md`.
