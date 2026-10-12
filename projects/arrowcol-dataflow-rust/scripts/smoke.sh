#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
work="$root/projects/arrowcol-dataflow-rust/.temp/smoke"
binary="$root/target/debug/arrowcol-dataflow-rust"
rm -rf "$work"
mkdir -p "$work"

"$binary" --num-cores 1 \
  --config "$root/projects/arrowcol-dataflow-rust/config.smoke.yaml" \
  >"$work/collector.log" 2>&1 &
pid=$!

cleanup() {
  if kill -0 "$pid" 2>/dev/null; then
    kill -TERM "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  fi
  rm -rf "$work"
}
trap cleanup EXIT

ready=false
for _ in $(seq 1 60); do
  if curl --fail --silent http://127.0.0.1:32311/readyz >"$work/ready.json"; then
    ready=true
    break
  fi
  if ! kill -0 "$pid" 2>/dev/null; then
    cat "$work/collector.log" >&2
    exit 1
  fi
  sleep 0.25
done
if [[ "$ready" != true ]]; then
  cat "$work/collector.log" >&2
  exit 1
fi

curl --fail --silent http://127.0.0.1:32311/healthz >"$work/health.json"
curl --fail --silent http://127.0.0.1:32311/v1/capabilities >"$work/capabilities.json"
curl --fail --silent http://127.0.0.1:32311/v1/pressure >"$work/pressure.json"
curl --fail --silent http://127.0.0.1:32311/metrics >"$work/metrics.txt"

grep -F '"delivery_guarantee":"best_effort"' "$work/capabilities.json" >/dev/null
grep -F '"receipt_polling":false' "$work/capabilities.json" >/dev/null
grep -F '"at_least_once":false' "$work/capabilities.json" >/dev/null
grep -F 'arrowcol_delivery_guarantee_info' "$work/metrics.txt" >/dev/null

kill -TERM "$pid"
for _ in $(seq 1 80); do
  if ! kill -0 "$pid" 2>/dev/null; then
    wait "$pid"
    pid=0
    rm -rf "$work"
    trap - EXIT
    exit 0
  fi
  sleep 0.25
done

cat "$work/collector.log" >&2
exit 1
