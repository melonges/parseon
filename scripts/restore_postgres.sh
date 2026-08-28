#!/usr/bin/env bash
set -euo pipefail
umask 077

: "${PGSERVICE:?set PGSERVICE to the isolated PostgreSQL restore service}"
: "${PGSERVICEFILE:?set PGSERVICEFILE to a libpq service file}"
: "${PGPASSFILE:?set PGPASSFILE to a protected libpq password file}"
: "${AGE_IDENTITY:?set AGE_IDENTITY to the age private key used for backups}"
: "${RESTORE_CONFIRM:?set RESTORE_CONFIRM=YES to allow a destructive restore}"
[ "$RESTORE_CONFIRM" = YES ] || { echo 'RESTORE_CONFIRM must be YES' >&2; exit 2; }
[ "$#" -eq 1 ] || { echo "usage: $0 backup.dump.age" >&2; exit 2; }
[ -f "$1" ] || { echo "backup not found: $1" >&2; exit 2; }
[ -f "$1.sha256" ] || { echo "backup checksum not found: $1.sha256" >&2; exit 2; }
command -v age >/dev/null || { echo 'age is required for encrypted restores' >&2; exit 127; }
command -v sha256sum >/dev/null || { echo 'sha256sum is required for backup checksums' >&2; exit 127; }
sha256sum -c "$1.sha256"
age -d -i "$AGE_IDENTITY" "$1" \
  | pg_restore --clean --if-exists --exit-on-error --no-owner --dbname="$PGSERVICE" -
