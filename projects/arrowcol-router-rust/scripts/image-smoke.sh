#!/usr/bin/env bash
set -euo pipefail

image="ghcr.io/mdrakiburrahman/rust-playground/arrowcol-router-rust:dev"
container="arrowcol-router-rust-image-smoke-$$"

cleanup() {
  docker rm --force "$container" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker run --rm --platform linux/amd64 "$image" \
  --validate-and-exit \
  --config /etc/arrowcol-router/config.yaml
docker run --rm --platform linux/amd64 "$image" \
  --validate-and-exit \
  --config /etc/arrowcol-router/config.smoke.yaml

docker run --rm --platform linux/amd64 --entrypoint /bin/sh "$image" -eu -c '
  test "$(id -u)" -eq 10001
  test ! -w /etc/arrowcol-router/config.yaml
  test ! -w /etc/arrowcol-router/config.smoke.yaml
  test -w /var/run/arrowcol-router
'

docker run --detach --platform linux/amd64 --name "$container" \
  --health-cmd='curl --fail --silent --show-error --max-time 2 http://127.0.0.1:50050/healthz' \
  --health-interval=2s \
  --health-timeout=3s \
  --health-start-period=5s \
  --health-retries=30 \
  --entrypoint /bin/sh \
  "$image" -eu -c '
    umask 077
    head -c 48 /dev/urandom | base64 > /var/run/arrowcol-router/sql-password
    head -c 48 /dev/urandom | base64 > /var/run/arrowcol-router/edge-hmac
    head -c 48 /dev/urandom | base64 > /var/run/arrowcol-router/internal-hmac
    exec /usr/local/bin/arrowcol-router-rust --config /etc/arrowcol-router/config.smoke.yaml
  ' >/dev/null

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
  http://127.0.0.1:50050/healthz | grep -F '"status":"healthy"' >/dev/null
docker exec "$container" curl --fail --silent --show-error --max-time 2 \
  --http2-prior-knowledge http://127.0.0.1:50050/v1/capabilities \
  | grep -F '"transparent_flight_data":true' >/dev/null
docker exec "$container" /bin/sh -eu -c '
  code="$(curl --silent --output /var/run/arrowcol-router/ready.json \
    --write-out "%{http_code}" --max-time 2 --http2-prior-knowledge \
    http://127.0.0.1:50050/readyz)"
  test "$code" = 503
  grep -F "\"status\":\"not_ready\"" /var/run/arrowcol-router/ready.json >/dev/null
'

docker stop --time 20 "$container" >/dev/null
docker rm "$container" >/dev/null
trap - EXIT
