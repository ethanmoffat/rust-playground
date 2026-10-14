#!/usr/bin/env bash
set -euo pipefail

image="ghcr.io/mdrakiburrahman/rust-playground/arrowcol-dataflow-rust:dev"
container="arrowcol-dataflow-rust-image-smoke-$$"

cleanup() {
  docker rm --force "$container" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker run --rm --platform linux/amd64 "$image" \
  --validate-and-exit \
  --config /etc/arrowcol-dataflow-rust/config.yaml
docker run --rm --platform linux/amd64 "$image" \
  --validate-and-exit \
  --config /etc/arrowcol-dataflow-rust/config.smoke.yaml

docker run --rm --platform linux/amd64 --entrypoint /bin/sh "$image" -eu -c '
  test "$(id -u)" -eq 10001
  test ! -w /etc/arrowcol-dataflow-rust/config.yaml
  test ! -w /etc/arrowcol-dataflow-rust/config.smoke.yaml
  test -w /onelake
'

docker run --detach --platform linux/amd64 --name "$container" \
  --health-cmd='curl --fail --silent --show-error --max-time 2 http://127.0.0.1:32311/readyz' \
  --health-interval=2s \
  --health-timeout=3s \
  --health-start-period=5s \
  --health-retries=30 \
  "$image" \
  --num-cores 1 \
  --config /etc/arrowcol-dataflow-rust/config.smoke.yaml >/dev/null

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

capabilities="$(docker exec "$container" curl --fail --silent --show-error --max-time 2 \
  http://127.0.0.1:32311/v1/capabilities)"
grep -F '"delivery_guarantee":"best_effort"' <<<"$capabilities" >/dev/null
grep -F '"receipt_polling":false' <<<"$capabilities" >/dev/null
grep -F '"at_least_once":false' <<<"$capabilities" >/dev/null

docker exec "$container" curl --fail --silent --show-error --max-time 2 \
  http://127.0.0.1:32311/healthz >/dev/null
docker stop --time 30 "$container" >/dev/null
docker rm "$container" >/dev/null
trap - EXIT
