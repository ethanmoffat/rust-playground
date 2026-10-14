#!/usr/bin/env bash
set -Eeuo pipefail

secret_file="${SQLCMDPASSWORD_FILE:-${MSSQL_SA_PASSWORD_FILE:-/run/secrets/sqlserver_sa_password}}"
if [[ ! -r "$secret_file" ]]; then
  echo "sqlcmd password secret is not readable: $secret_file" >&2
  exit 64
fi

export SQLCMDPASSWORD="$(<"$secret_file")"
exec /opt/mssql-tools18/bin/sqlcmd -C "$@"
