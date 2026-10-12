#!/usr/bin/env bash
set -euo pipefail

workspace="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
target_root="${CARGO_TARGET_DIR:-$workspace/target}"
if [[ "${target_root}" != /* ]]; then
  target_root="$workspace/$target_root"
fi
static_root="$target_root/loadtest-static"

cd "$workspace"
cargo build --locked --package delta-inspect
CARGO_TARGET_DIR="$static_root" \
RUSTFLAGS="-C target-feature=+crt-static" \
  cargo build --locked \
    --target x86_64-unknown-linux-gnu \
    --package arrowcol-loadgen \
    --package arrowcol-sdk-gen

install -d "$target_root/debug"
for binary in arrowcol-loadgen arrowcol-sdk-gen; do
  install -m 0755 \
    "$static_root/x86_64-unknown-linux-gnu/debug/$binary" \
    "$target_root/debug/$binary"
  strip "$target_root/debug/$binary"
  if readelf --program-headers "$target_root/debug/$binary" | grep -Fq 'Requesting program interpreter'; then
    echo "$binary must be statically linked for the Debian validator image" >&2
    exit 1
  fi
done

test -x "$target_root/debug/delta-inspect"
test -x "$target_root/debug/arrowcol-loadgen"
test -x "$target_root/debug/arrowcol-sdk-gen"
