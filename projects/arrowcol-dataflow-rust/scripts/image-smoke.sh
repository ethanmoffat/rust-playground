#!/usr/bin/env bash
set -euo pipefail

image="ghcr.io/mdrakiburrahman/rust-playground/arrowcol-dataflow-rust:dev"
container="arrowcol-dataflow-rust-image-smoke-$$"

cleanup() {
  docker rm --force "$container" >/dev/null 2>&1 || true
}
trap cleanup EXIT

for config in config.yaml config.smoke.yaml config.quiver.yaml config.smoke-quiver.yaml; do
  docker run --rm --platform linux/amd64 "$image" \
    --validate-and-exit \
    --config "/etc/arrowcol-dataflow-rust/$config"
done

docker run --rm --platform linux/amd64 --entrypoint /bin/sh "$image" -eu -c '
  test "$(id -u)" -eq 10001
  for config in config.yaml config.smoke.yaml config.quiver.yaml config.smoke-quiver.yaml; do
    test ! -w "/etc/arrowcol-dataflow-rust/$config"
  done
  test -w /onelake
  test -w /var/lib/arrowcol/quiver
'

# Starts the memory-backed topology from one embedded config, waits for Docker
# health, and prints its capability document.
run_healthy() {
  local config="$1"
  local admin_port="$2"
  docker run --detach --platform linux/amd64 --name "$container" \
    --health-cmd="curl --fail --silent --show-error --max-time 2 http://127.0.0.1:$admin_port/readyz" \
    --health-interval=2s \
    --health-timeout=3s \
    --health-start-period=5s \
    --health-retries=30 \
    "$image" \
    --num-cores 1 \
    --config "/etc/arrowcol-dataflow-rust/$config" >/dev/null

  for _ in $(seq 1 90); do
    status="$(docker inspect --format '{{.State.Health.Status}}' "$container" 2>/dev/null || true)"
    case "$status" in
      healthy)
        break
        ;;
      unhealthy)
        docker logs "$container" >&2
        exit 1
        ;;
    esac
    if [[ "$(docker inspect --format '{{.State.Running}}' "$container" 2>/dev/null || true)" != true ]]; then
      docker logs "$container" >&2
      exit 1
    fi
    sleep 1
  done

  if [[ "$(docker inspect --format '{{.State.Health.Status}}' "$container")" != healthy ]]; then
    docker logs "$container" >&2
    exit 1
  fi
  docker exec "$container" curl --fail --silent --show-error --max-time 2 \
    "http://127.0.0.1:$admin_port/healthz" >/dev/null
  docker exec "$container" curl --fail --silent --show-error --max-time 2 \
    "http://127.0.0.1:$admin_port/v1/capabilities"
}

stop_container() {
  docker stop --time 30 "$container" >/dev/null
  docker rm "$container" >/dev/null
}

capabilities="$(run_healthy config.smoke.yaml 32311)"
grep -F '"delivery_guarantee":"best_effort"' <<<"$capabilities" >/dev/null
grep -F '"receipt_polling":false' <<<"$capabilities" >/dev/null
grep -F '"at_least_once":false' <<<"$capabilities" >/dev/null
grep -F '"native_quiver":{"supported":true,"enabled":false}' <<<"$capabilities" >/dev/null
stop_container

# The durable topology uses the image's default Quiver path, proving UID 10001 can create its WAL.
capabilities="$(run_healthy config.smoke-quiver.yaml 32411)"
grep -F '"delivery_guarantee":"durable_admission"' <<<"$capabilities" >/dev/null
grep -F '"receipt_polling":false' <<<"$capabilities" >/dev/null
grep -F '"at_least_once":true' <<<"$capabilities" >/dev/null
grep -F '"native_quiver":{"supported":true,"enabled":true}' <<<"$capabilities" >/dev/null
docker exec "$container" test -d /var/lib/arrowcol/quiver/core_0/wal
stop_container
trap - EXIT
