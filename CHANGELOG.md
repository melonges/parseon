# Changelog

All notable changes to Parseon are documented in this file.

## 0.2.0 - 2026-06-30

### Added

- Storage-neutral domain models for chains, monitor targets, cursors, decoded calls, and decoded Solidity values.
- `Storage`, `BlockSource`, and chain-aware `BlockCache` adapter traits.
- Pure block scheduling and a service-free worker test seam.
- PostgreSQL, JSON-RPC, and in-memory implementations of the v0.2 adapter contracts.

### Changed

- Replaced the legacy watcher/coordinator structure with monitor, scheduler, indexer, and worker modules.
- Moved SQL value conversion and atomic block persistence behind `PostgresStorage`.
- Separated API response types from PostgreSQL records while preserving the existing HTTP and OpenAPI surface.

### Compatibility

- Existing environment variables, database migrations, monitor result tables, REST routes, and finalized-only indexing behavior remain unchanged.
