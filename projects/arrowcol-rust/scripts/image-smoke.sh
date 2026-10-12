#!/usr/bin/env bash
set -euo pipefail

image="ghcr.io/mdrakiburrahman/rust-playground/arrowcol-rust:dev"

docker run --rm --platform linux/amd64 "$image" \
  --validate-and-exit \
  --config /etc/arrowcol-rust/config.yaml

docker run --rm --platform linux/amd64 --entrypoint /bin/sh "$image" -eu -c '
  test "$(id -u)" -eq 10001
  test ! -w /etc/arrowcol-rust/config.yaml
  test -w /var/lib/arrowcol/delta
'

healthcheck="$(docker image inspect --format '{{json .Config.Healthcheck.Test}}' "$image")"
grep -F '/readyz' <<<"$healthcheck" >/dev/null
grep -F '8081' <<<"$healthcheck" >/dev/null
