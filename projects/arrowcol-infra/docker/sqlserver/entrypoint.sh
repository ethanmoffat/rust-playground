#!/usr/bin/env bash
set -Eeuo pipefail

secret_file="${MSSQL_SA_PASSWORD_FILE:-/run/secrets/sqlserver_sa_password}"

if [[ ! -r "$secret_file" ]]; then
  echo "SQL Server password secret is not readable: $secret_file" >&2
  exit 64
fi

password="$(<"$secret_file")"
if [[ -z "$password" || "$password" == *$'\n'* || "$password" == *$'\r'* ]]; then
  echo "SQL Server password secret must be non-empty and single-line" >&2
  exit 64
fi

# The upstream image only accepts MSSQL_SA_PASSWORD. Keep the secret in a
# Docker secret until the final process exec, then preserve its launch script.
export MSSQL_SA_PASSWORD="$password"
unset password

if [[ "$(id -u)" -eq 0 ]]; then
  exec setpriv \
    --reuid=10001 \
    --regid=10001 \
    --clear-groups \
    /opt/mssql/bin/launch_sqlservr.sh "$@"
fi

exec /opt/mssql/bin/launch_sqlservr.sh "$@"
