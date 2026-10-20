#!/usr/bin/env bash
# Smoke-tests the locally loaded kusto-explorer image without a Kusto cluster.
set -euo pipefail

image="ghcr.io/mdrakiburrahman/rust-playground/kusto-explorer:dev"
container="kusto-explorer-image-smoke-$$"

cleanup() {
  docker rm --force "$container" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker run --rm --platform linux/amd64 "$image" --validate-and-exit
docker run --rm --platform linux/amd64 \
  --env 'KUSTO_EXPLORER_CONNECTIONS=Primary=http://kusto:8080;Secondary=https://example.invalid' \
  --env KUSTO_EXPLORER_DEFAULT_DATABASE=loadtest \
  "$image" --validate-and-exit

docker run --rm --platform linux/amd64 --entrypoint /bin/sh "$image" -eu -c '
  test "$(id -u)" -eq 10001
  test "$(id -g)" -eq 10001
  test -f /usr/share/kusto-explorer/www/index.html
  test -f /usr/share/kusto-explorer/www/THIRD_PARTY_NOTICES.txt
  test ! -w /usr/share/kusto-explorer/www
  test ! -w /usr/share/kusto-explorer/www/index.html
  test ! -w /usr/local/bin/kusto-explorer
  ls /usr/share/kusto-explorer/www/assets/*.wasm >/dev/null
  ls /usr/share/kusto-explorer/www/assets/*.wasm.br >/dev/null
'

docker run --detach --platform linux/amd64 --name "$container" \
  --health-interval=2s \
  --health-timeout=3s \
  --health-start-period=5s \
  --health-retries=30 \
  "$image" >/dev/null

for _ in $(seq 1 60); do
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

docker exec "$container" /bin/sh -eu -c '
  base=http://127.0.0.1:8080
  curl --fail --silent --show-error --max-time 2 "$base/healthz" | grep -F "\"status\":\"healthy\"" >/dev/null
  curl --fail --silent --show-error --max-time 2 "$base/" | grep -F "<div id=\"app\"" >/dev/null
  curl --fail --silent --show-error --max-time 2 "$base/api/config" | grep -F "\"clusterUri\":\"http://kusto:8080\"" >/dev/null
  curl --fail --silent --show-error --max-time 2 --head --header "Accept-Encoding: br" "$base/assets/main.js" \
    | tr -d "\r" > /tmp/main-js-headers
  grep -qix "content-encoding: br" /tmp/main-js-headers
  grep -qi "^content-security-policy: default-src .self.;" /tmp/main-js-headers
  grep -qix "x-content-type-options: nosniff" /tmp/main-js-headers
  code="$(curl --silent --output /dev/null --write-out "%{http_code}" --max-time 2 "$base/api/unknown")"
  test "$code" = 404
  code="$(curl --silent --output /tmp/ready.json --write-out "%{http_code}" --max-time 10 "$base/readyz")"
  test "$code" = 503
  grep -F "\"ready\":false" /tmp/ready.json >/dev/null
'

docker stop --time 20 "$container" >/dev/null
test "$(docker inspect --format '{{.State.ExitCode}}' "$container")" -eq 0
docker rm "$container" >/dev/null
trap - EXIT
