#!/usr/bin/env bash
set -Eeuo pipefail

WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
INFRA_ROOT="$WORKSPACE_ROOT/projects/arrowcol-infra"
DIAGNOSTIC_ROOT="$WORKSPACE_ROOT/projects/arrowcol-dataflow-rust/.diagnostics"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
DIAGNOSTICS="$DIAGNOSTIC_ROOT/$RUN_ID"
mkdir -p "$DIAGNOSTICS"
workspace_container=""
infra_network=""

# shellcheck source=../../arrowcol-infra/scripts/lib.sh
source "$INFRA_ROOT/scripts/lib.sh"

wait_for_port() {
  local host="$1"
  local port="$2"
  local label="$3"
  local attempts=60
  while (( attempts > 0 )); do
    if (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null; then
      exec 3>&-
      exec 3<&-
      return 0
    fi
    attempts=$((attempts - 1))
    sleep 1
  done
  echo "Timed out waiting for $label at $host:$port" >&2
  return 1
}

cleanup() {
  local status="$1"
  compose ps --all >"$DIAGNOSTICS/compose-ps.txt" 2>&1 || true
  compose logs --no-color --timestamps --tail 1000 >"$DIAGNOSTICS/compose.log" 2>&1 || true
  if [[ -n "$workspace_container" && -n "$infra_network" ]]; then
    docker network disconnect "$infra_network" "$workspace_container" >/dev/null 2>&1 || true
  fi
  "$INFRA_ROOT/scripts/stack.sh" down >"$DIAGNOSTICS/down.log" 2>&1 || true
  printf 'exit_status=%s\n' "$status" >"$DIAGNOSTICS/result.txt"
  printf 'diagnostics=%s\n' "$DIAGNOSTICS"
}

on_exit() {
  local status=$?
  trap - EXIT
  cleanup "$status"
  exit "$status"
}
trap on_exit EXIT

if [[ ! -f "$INFRA_ROOT/.env" || ! -s "$INFRA_ROOT/secrets/arrowcol-app-password.txt" ]]; then
  "$INFRA_ROOT/scripts/bootstrap-secrets.sh" >"$DIAGNOSTICS/bootstrap.log" 2>&1
fi

if ! "$INFRA_ROOT/scripts/stack.sh" up >"$DIAGNOSTICS/up.log" 2>&1; then
  sleep 10
  "$INFRA_ROOT/scripts/stack.sh" up >"$DIAGNOSTICS/up-retry.log" 2>&1
fi

sql_host=127.0.0.1
sql_port="${SQLSERVER_PORT:-14333}"
hive_host=127.0.0.1
hive_port="${HIVE_METASTORE_PORT:-9083}"
workspace_container="$(hostname)"
if docker inspect "$workspace_container" >/dev/null 2>&1; then
  sql_container="$(compose ps -q sqlserver)"
  infra_network="$(
    docker inspect \
      --format '{{range $name, $_ := .NetworkSettings.Networks}}{{$name}}{{"\n"}}{{end}}' \
      "$sql_container" \
      | head -n 1
  )"
  docker network connect "$infra_network" "$workspace_container" >/dev/null 2>&1 || true
  sql_host=sqlserver
  sql_port=1433
  hive_host=hive-metastore
  hive_port=9083
fi

wait_for_port "$sql_host" "$sql_port" "SQL Server"
wait_for_port "$hive_host" "$hive_port" "Hive Metastore"

(
  cd "$WORKSPACE_ROOT"
  ARROWCOL_RUN_LIVE_TESTS=1 \
  ARROWCOL_SQL_HOST="$sql_host" \
  ARROWCOL_SQL_PORT="$sql_port" \
  ARROWCOL_HIVE_HOST="$hive_host" \
  ARROWCOL_HIVE_PORT="$hive_port" \
  ARROWCOL_SQL_PASSWORD_FILE="$INFRA_ROOT/secrets/arrowcol-app-password.txt" \
  RUST_BACKTRACE=1 \
    cargo test --locked --package arrowcol-dataflow-components \
      live_sql_hive_delta -- --ignored --nocapture --test-threads=1
) 2>&1 | tee "$DIAGNOSTICS/live-e2e.log"
