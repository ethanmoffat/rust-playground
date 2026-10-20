#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
project="$root/projects/arrowcol-dataflow-rust"
work="$project/.temp/config-check"
target_dir="${CARGO_TARGET_DIR:-$root/target}"
[[ "$target_dir" = /* ]] || target_dir="$root/$target_dir"
binary="$target_dir/debug/arrowcol-dataflow-rust"
rm -rf "$work"
mkdir -p "$work"
trap 'rm -rf "$work"' EXIT

for config in config.yaml config.smoke.yaml config.quiver.yaml config.smoke-quiver.yaml; do
  "$binary" --validate-and-exit --config "$project/$config"
done

# The durable-admission topology must keep receiver:arrow_flight -> processor:durable_buffer
# -> exporter:arrow_deltalake and must enable native Quiver on the receiver.
grep -F 'native_quiver: true' "$project/config.quiver.yaml" >/dev/null
grep -F 'type: processor:durable_buffer' "$project/config.quiver.yaml" >/dev/null
grep -F 'wal_flush_interval: 0s' "$project/config.quiver.yaml" >/dev/null

# A STANDARD-only processor (processor:batch accepts only OTAP/OTLP pdata) must be
# rejected by DFE wiring contracts when it receives Arrowcol pluggable Arrow pdata.
if "$binary" --validate-and-exit \
  --config "$project/config.incompatible-otap.yaml" \
  >"$work/incompatible-otap.log" 2>&1; then
  echo "OTAP-only batch processor wiring unexpectedly passed" >&2
  exit 1
fi
grep -F "IncompatiblePdataWiring" "$work/incompatible-otap.log" >/dev/null
grep -F 'node: "otap_batch"' "$work/incompatible-otap.log" >/dev/null
grep -F "incoming: PdataRepresentationSet(PLUGGABLE_ARROW)" "$work/incompatible-otap.log" >/dev/null
grep -F "accepted: PdataRepresentationSet(STANDARD)" "$work/incompatible-otap.log" >/dev/null
