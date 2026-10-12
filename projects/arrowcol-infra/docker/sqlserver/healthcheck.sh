#!/usr/bin/env bash
set -Eeuo pipefail

secret_file="${MSSQL_SA_PASSWORD_FILE:-/run/secrets/sqlserver_sa_password}"
[[ -r "$secret_file" ]] || exit 1

export SQLCMDPASSWORD="$(<"$secret_file")"
exec timeout 6 /opt/mssql-tools18/bin/sqlcmd \
  -S tcp:127.0.0.1,1433 \
  -U sa \
  -C \
  -b \
  -l 3 \
  -Q "SET NOCOUNT ON; SELECT 1;" \
  -o /dev/null
