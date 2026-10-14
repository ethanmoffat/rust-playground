#!/usr/bin/env bash
set -Eeuo pipefail

sqlcmd=/opt/mssql-tools18/bin/sqlcmd
host="${SQLSERVER_HOST:-sqlserver}"
port="${SQLSERVER_PORT:-1433}"
secret_file="${ARROWCOL_MIGRATOR_PASSWORD_FILE:-/run/secrets/arrowcol_migrator_password}"

if [[ ! -r "$secret_file" ]]; then
  echo "Arrowcol migrator password secret is not readable: $secret_file" >&2
  exit 64
fi

export SQLCMDPASSWORD="$(<"$secret_file")"
if [[ -z "$SQLCMDPASSWORD" || "$SQLCMDPASSWORD" == *$'\n'* || "$SQLCMDPASSWORD" == *$'\r'* ]]; then
  echo "Arrowcol migrator password secret must be non-empty and single-line" >&2
  exit 64
fi

for attempt in $(seq 1 60); do
  if "$sqlcmd" \
    -S "tcp:${host},${port}" \
    -U arrowcol_migrator \
    -C \
    -b \
    -l 3 \
    -d arrowcol \
    -Q "SET NOCOUNT ON; SELECT 1;" \
    -o /dev/null; then
    break
  fi

  if [[ "$attempt" -eq 60 ]]; then
    echo "Arrowcol database did not become ready within 120 seconds" >&2
    exit 1
  fi
  sleep 2
done

migration_count=0
while IFS= read -r migration; do
  migration_count=$((migration_count + 1))
  checksum="$(sha256sum "$migration" | cut -d ' ' -f 1)"
  migration_name="$(basename "$migration")"
  version="${migration_name#V}"
  version="${version%%__*}"
  existing_checksum="$(
    "$sqlcmd" \
      -S "tcp:${host},${port}" \
      -U arrowcol_migrator \
      -C \
      -b \
      -l 30 \
      -h -1 \
      -W \
      -d arrowcol \
      -Q "SET NOCOUNT ON;
          IF OBJECT_ID(N'arrowcol.schema_migrations', N'U') IS NULL
              SELECT N'';
          ELSE
              SELECT COALESCE((
                  SELECT checksum_sha256
                  FROM arrowcol.schema_migrations
                  WHERE version = N'${version}'
              ), N'');" \
      | tr -d '\r[:space:]'
  )"

  if [[ -n "$existing_checksum" ]]; then
    if [[ "$existing_checksum" != "$checksum" ]]; then
      echo "Migration ${version} checksum does not match the recorded version" >&2
      exit 1
    fi
    echo "Migration ${version} already applied"
    continue
  fi

  echo "Applying ${migration_name} (sha256:${checksum})"

  "$sqlcmd" \
    -S "tcp:${host},${port}" \
    -U arrowcol_migrator \
    -C \
    -b \
    -r 1 \
    -l 30 \
    -I \
    -d arrowcol \
    -v "MIGRATION_CHECKSUM=${checksum}" \
    -i "$migration"
done < <(find /opt/arrowcol/migrations -maxdepth 1 -type f -name 'V*.sql' -print | sort)

if [[ "$migration_count" -eq 0 ]]; then
  echo "No application migrations were found" >&2
  exit 1
fi

"$sqlcmd" \
  -S "tcp:${host},${port}" \
  -U arrowcol_migrator \
  -C \
  -b \
  -l 30 \
  -d arrowcol \
  -Q "SET NOCOUNT ON; SELECT version, description, checksum_sha256, applied_at_utc FROM arrowcol.schema_migrations ORDER BY version;"
