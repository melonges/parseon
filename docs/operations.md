# Production operations

This runbook applies to Parseon v1.0. Parseon is a single active process per storage
schema: deploy one replica with a `Recreate` strategy. Run it behind an ingress that
terminates TLS and restricts the service network.

## Security boundary

- Set a long random `API_TOKEN`. The bearer token is required for `/chains`,
  `/monitors`, `/filters/preview`, result queries and `/status`.
- `/healthz`, `/readyz` and `/metrics` are unauthenticated probe endpoints; keep them
  on an internal network or ingress allowlist. Swagger/OpenAPI metadata is public to
  make the browser UI usable, so expose those paths only through an operator ingress.
- Keep `CORS_ORIGINS` empty unless a specific browser origin is required. Never use `*`
  for an authenticated browser deployment.
- `ALLOW_PRIVATE_RPC_NETWORKS=false` in production. RPC URLs are validated before
  probing; private/link-local/metadata destinations are rejected. Network egress
  policy remains the final SSRF control.
- Use an ingress/network policy for TLS, client IP limits and rate limiting. Parseon
  does not terminate TLS or provide user roles/RBAC.
- RPC URLs are write-only. Do not put provider credentials in Git, logs, URLs copied
  into tickets, or generated artifacts. The historical credential previously present
  in `erpc.yaml` must be revoked independently of this change.

## Deployment

### Docker Compose

Copy the production environment into a secret-managed shell environment and set at
least `PARSEON_IMAGE` to an immutable image reference, `STORAGE_URL`,
`POSTGRES_PASSWORD`, and `API_TOKEN`:

```bash
export PARSEON_IMAGE=registry.example/parseon@sha256:<digest>
export STORAGE_URL='postgres://parseon:<password>@postgres:5432/parseon'
export POSTGRES_PASSWORD='<different-long-random-password>'
export API_TOKEN='<long-random-token>'
docker compose -f compose.production.yml config
docker compose -f compose.production.yml up -d
# The Compose file intentionally publishes no host port; probe from the service network.
docker compose -f compose.production.yml exec -T parseon wget -qO- http://127.0.0.1:8080/healthz
docker compose -f compose.production.yml exec -T parseon wget -qO- http://127.0.0.1:8080/readyz
# Use the configured ingress URL for external probes.
```

The production file has no host database ports. It uses a private database network and
an egress network so Parseon can reach configured RPC endpoints. Do not expose the
container port directly to the Internet.

### Kubernetes / Helm

Create the external Secret before installing; it must contain `storage-url` and
`api-token` (or override those key names in values):

```bash
kubectl create secret generic parseon-secrets \
  --from-literal=storage-url="$STORAGE_URL" \
  --from-literal=api-token="$API_TOKEN"
helm upgrade --install parseon deploy/helm/parseon \
  --set image.repository=registry.example/parseon \
  --set image.tag=1.0.0 \
  --set image.digest=sha256:<digest>
```

The chart intentionally runs one replica with `Recreate`; scaling it without a
leader-election/lease design can duplicate workers and writes. Configure an ingress
for TLS and API access, and keep the Secret in an external secret manager in real
clusters.

## Health and readiness

- `/healthz` only proves that the process and HTTP listener are alive.
- `/readyz` checks storage connectivity and requires every enabled chain worker to be
  running. A disabled-only registry is ready; starting, degraded, blocked, stale, or
  unexpectedly exited worker tasks return `503`.
- A blocked worker means reorg recovery crossed a promoted finalized boundary or the
  retained ancestor was unavailable. Do not restart-loop blindly; follow the recovery
  procedure below.
- Alert on `parseon_worker_lag_blocks`, worker blocked/degraded status, increasing RPC
  errors, storage commit errors and a stale `last_successful_poll_at`.

## Finality and reorg recovery

Blocks are indexed provisionally through the latest head. Promotion uses:

```text
promotion_height = min(source_finalized_head,
                       latest_head - CONFIRMATION_DEPTH)
```

The defaults are `CONFIRMATION_DEPTH=64` and `ROLLBACK_RETENTION=256`. Results API
queries default to finalized rows; request `finality=provisional` or `finality=all`
only when an operator needs unstable data.

A fork within retention is rolled back atomically: orphaned blocks/results are removed
and all affected monitor cursors (including completed monitors) are rewound. If the
fork reaches promoted finalized data, or no common ancestor is retained, the worker
blocks without deleting data. Resolve the source/finality incident, then take a
backup and reindex the affected chain/monitors according to the release procedure.

Webhook delivery is finalized-only and best-effort. Storage commits are authoritative;
a webhook failure does not rewind indexing and delivery is not exactly-once.

## PostgreSQL migrations and legacy data

Migrations are embedded at build time and run at startup. Never edit an applied SQL
migration and never downgrade a running schema. The v1 migration refuses a populated
pre-ledger monitor/result state because old rows have no block hashes. Upgrade such a
release as follows:

1. Stop Parseon and take a verified backup.
2. Restore the backup into a staging database and test it.
3. For a database still using the pre-v1 schema, export anything needed, then create
   an empty Parseon database (or drop only the Parseon schema after confirming the
   backup). Do not delete `_sqlx_migrations` in a production database as a shortcut.
4. Start the v1 image so migrations create the ledger, then recreate monitors from
   their source definitions and reindex from their chosen start blocks.
5. Verify chain status, canonical/finalized heads, monitor cursors and result counts.

There is no safe automatic hash backfill for old rows. MongoDB follows the same rule:
its schema marker is checked at startup and documents without v1 block identity require
an explicit backup/reset/reindex. There is no cross-storage importer or dual-write
migration.

## Backups

Target service objectives for the default self-hosted deployment are **RPO 15 minutes**
and **RTO 60 minutes**. Operators may tighten them, but must record the changed policy.
Backups must be encrypted, copied off-host, access-controlled and periodically tested.

### PostgreSQL

Quiesce writes by stopping Parseon before a logical backup (or use an established WAL
archiving policy for continuous recovery):

```bash
BACKUP_DIR=${BACKUP_DIR:-./backups}
mkdir -p "$BACKUP_DIR"
# Use libpq service/password files; URLs and passwords stay out of process arguments.
export PGSERVICE=parseon
export PGSERVICEFILE=/run/secrets/pg_service.conf
export PGPASSFILE=/run/secrets/pgpass
export AGE_RECIPIENT='age1...'
./scripts/backup_postgres.sh
```

The script uses `pg_dump --format=custom` and never stores credentials in the
repository. Restore only into an isolated target first:

```bash
export RESTORE_CONFIRM=YES
export PGSERVICE=parseon_restore
export PGSERVICEFILE=/run/secrets/pg_restore_service.conf
export PGPASSFILE=/run/secrets/pgpass_restore
export AGE_IDENTITY=/run/secrets/age_identity
./scripts/restore_postgres.sh backups/parseon-<timestamp>.dump.age
```

After restore, start the matching Parseon image, wait for `/readyz`, compare monitor
and result counts, and verify a known result plus canonical/finalized status. A restore
is not accepted until the checksum and application-level checks pass.

### MongoDB

Quiesce Parseon, then use a replica-set aware dump with oplog capture:

```bash
mongodump --uri="$MONGODB_URI" --db="${STORAGE_DATABASE:-parseon}" \
  --out="$BACKUP_DIR/mongodb-$(date -u +%Y%m%dT%H%M%SZ)" --oplog
```

Restore to an isolated replica set with `mongorestore --drop --oplogReplay`, verify the
`schema_metadata` version, chain/monitor counts, canonical block continuity and a
known result, then point a matching Parseon image at the restored database. Never use a
standalone MongoDB deployment: transactions require a replica set or sharded cluster.

## Incident checklist

1. Check `/readyz`, `/status` (with bearer token), metrics and worker logs without
   printing RPC URLs or credentials.
2. If a worker is degraded, fix provider connectivity/finality support first.
3. If blocked, preserve the database, capture a backup and identify the common ancestor
   or finality violation before any reset.
4. If reset/reindex is required, stop Parseon, verify backup, perform it in staging
   first, then recreate monitors and compare result counts.
5. Record the source endpoint/provider incident, affected chain/range, restore point,
   replay start, and final verification in the incident log.

## Release checklist

- Build with `--locked`; run all four storage/webhook feature combinations and Compose
  smoke checks.
- Scan tracked files and generated artifacts for credentials; revoke any exposed key.
- Verify the image digest, non-root runtime, liveness/readiness probes and resource
  limits.
- Run a restore drill at least once before first production use and after migration
  changes.
- Keep one release image and its migration notes available for rollback. Schema
  downgrade is unsupported; rollback means restore the previous backup or reindex.
