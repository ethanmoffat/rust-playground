#!/usr/bin/env bash
set -Eeuo pipefail

WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
INFRA_ROOT="$WORKSPACE_ROOT/projects/arrowcol-infra"

"$INFRA_ROOT/scripts/bootstrap-secrets.sh"

set -a
# shellcheck disable=SC1091
source "$INFRA_ROOT/.env"
set +a

resolve_infra_path() {
  local path="$1"
  if [[ "$path" = /* ]]; then
    printf '%s' "$path"
  else
    printf '%s/%s' "$INFRA_ROOT" "${path#./}"
  fi
}

export ARROWCOL_CATALOG_LIVE_TEST=1
export ARROWCOL_SQLSERVER_HOST=127.0.0.1
export ARROWCOL_SQLSERVER_PORT="${SQLSERVER_PORT:-14333}"
export ARROWCOL_SQLSERVER_PASSWORD_FILE="$(
  resolve_infra_path "${ARROWCOL_APP_PASSWORD_FILE:-./secrets/arrowcol-app-password.txt}"
)"
export ARROWCOL_HIVE_METASTORE_HOST=127.0.0.1
export ARROWCOL_HIVE_METASTORE_PORT="${HIVE_METASTORE_PORT:-9083}"

runner_binary=""
cleanup() {
  if [[ -n "$runner_binary" ]]; then
    rm -f "$runner_binary"
  fi
  "$INFRA_ROOT/scripts/stack.sh" down
}
trap cleanup EXIT

stack_ready=false
for attempt in 1 2 3; do
  if "$INFRA_ROOT/scripts/stack.sh" up; then
    stack_ready=true
    break
  fi
  if [[ "$attempt" -lt 3 ]]; then
    echo "Infrastructure startup attempt $attempt failed; waiting for durable SQL databases to recover" >&2
    sleep 5
  fi
done

if [[ "$stack_ready" != true ]]; then
  docker compose \
    --project-directory "$INFRA_ROOT" \
    --env-file "$INFRA_ROOT/.env" \
    -f "$INFRA_ROOT/compose.yaml" \
    logs --no-color --tail 200 || true
  exit 1
fi

if timeout 3 bash -c "</dev/tcp/127.0.0.1/$ARROWCOL_SQLSERVER_PORT" 2>/dev/null \
  && timeout 3 bash -c "</dev/tcp/127.0.0.1/$ARROWCOL_HIVE_METASTORE_PORT" 2>/dev/null; then
  cargo test --locked --package arrowcol-catalog --test live_infra -- --nocapture
  exit
fi

echo "Host-loopback ports are outside this container; running the test on the Compose network"
runner_image="${ARROWCOL_LIVE_RUNNER_IMAGE:-}"
if [[ -z "$runner_image" ]]; then
  runner_image="$(docker inspect --format '{{.Config.Image}}' "$(hostname)")"
fi

test_binary="$(
  cargo test \
    --locked \
    --package arrowcol-catalog \
    --test live_infra \
    --no-run \
    --message-format=json \
    2>/dev/null \
  | node -e '
      let input = "";
      process.stdin.on("data", chunk => input += chunk);
      process.stdin.on("end", () => {
        for (const line of input.trim().split(/\n/)) {
          try {
            const message = JSON.parse(line);
            if (
              message.reason === "compiler-artifact"
              && message.target?.name === "live_infra"
              && message.executable
            ) {
              process.stdout.write(message.executable);
            }
          } catch {}
        }
      });
    '
)"
if [[ -z "$test_binary" || ! -x "$test_binary" ]]; then
  echo "Could not locate the compiled live integration test executable" >&2
  exit 1
fi

runner_binary="$WORKSPACE_ROOT/crates/arrowcol-catalog/.live-infra-test"
cp "$test_binary" "$runner_binary"
chmod 0555 "$runner_binary"

docker run --rm \
  --network "${COMPOSE_PROJECT_NAME:-arrowcol-infra}_default" \
  --user "$(id -u):$(id -g)" \
  -v "$WORKSPACE_ROOT:$WORKSPACE_ROOT" \
  -w "$WORKSPACE_ROOT" \
  -e ARROWCOL_CATALOG_LIVE_TEST=1 \
  -e ARROWCOL_SQLSERVER_HOST=sqlserver \
  -e ARROWCOL_SQLSERVER_PORT=1433 \
  -e "ARROWCOL_SQLSERVER_PASSWORD_FILE=$WORKSPACE_ROOT/projects/arrowcol-infra/secrets/arrowcol-app-password.txt" \
  -e ARROWCOL_HIVE_METASTORE_HOST=hive-metastore \
  -e ARROWCOL_HIVE_METASTORE_PORT=9083 \
  --entrypoint "$runner_binary" \
  "$runner_image" \
  --nocapture
