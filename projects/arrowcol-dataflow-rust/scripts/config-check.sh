#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
work="$root/projects/arrowcol-dataflow-rust/.temp/config-check"
binary="$root/target/debug/arrowcol-dataflow-rust"
rm -rf "$work"
mkdir -p "$work"
trap 'rm -rf "$work"' EXIT

"$binary" --validate-and-exit \
  --config "$root/projects/arrowcol-dataflow-rust/config.yaml"
"$binary" --validate-and-exit \
  --config "$root/projects/arrowcol-dataflow-rust/config.smoke.yaml"

if "$binary" --validate-and-exit \
  --config "$root/projects/arrowcol-dataflow-rust/config.native-quiver-unsupported.yaml" \
  >"$work/native-quiver.log" 2>&1; then
  echo "native Quiver config unexpectedly passed" >&2
  exit 1
fi
grep -F \
  'Quiver capability `persist` is unavailable for pdata representation `arrowcol.flight@1.0`' \
  "$work/native-quiver.log" >/dev/null

if "$binary" --validate-and-exit \
  --config "$root/projects/arrowcol-dataflow-rust/config.incompatible-otap.yaml" \
  >"$work/incompatible-otap.log" 2>&1; then
  echo "OTAP-only durable-buffer wiring unexpectedly passed" >&2
  exit 1
fi
grep -F "IncompatiblePdataWiring" "$work/incompatible-otap.log" >/dev/null
grep -F "PLUGGABLE_ARROW" "$work/incompatible-otap.log" >/dev/null
grep -F "durable" "$work/incompatible-otap.log" >/dev/null
