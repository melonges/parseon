#!/usr/bin/env bash
set -euo pipefail
umask 077

: "${PGSERVICE:?set PGSERVICE to a PostgreSQL service name}"
: "${PGSERVICEFILE:?set PGSERVICEFILE to a libpq service file}"
: "${PGPASSFILE:?set PGPASSFILE to a protected libpq password file}"
: "${AGE_RECIPIENT:?set AGE_RECIPIENT to the age public key used for backups}"
BACKUP_DIR=${BACKUP_DIR:-./backups}
command -v age >/dev/null || { echo 'age is required for encrypted backups' >&2; exit 127; }
command -v sha256sum >/dev/null || { echo 'sha256sum is required for backup checksums' >&2; exit 127; }
mkdir -p "$BACKUP_DIR"
output="$BACKUP_DIR/parseon-$(date -u +%Y%m%dT%H%M%SZ).dump.age"
partial="$output.tmp.$$"
trap 'rm -f "$partial"' EXIT
pg_dump --format=custom --no-owner --dbname="$PGSERVICE" \
  | age -r "$AGE_RECIPIENT" -o "$partial"
mv "$partial" "$output"
sha256sum "$output" > "$output.sha256"
trap - EXIT
printf '%s\n' "$output"
