#!/usr/bin/env bash
# Builds the browser UI (WebAssembly plus web assets) into the static directory served by kusto-explorer.
#
# Usage: projects/kusto-explorer/scripts/build-dist.sh [output directory, default projects/kusto-explorer/dist]
set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
workspace_dir="$(cd "$project_dir/../.." && pwd)"
out_dir="$(realpath -m "${1:-$project_dir/dist}")"
target_dir="$(realpath -m "${CARGO_TARGET_DIR:-$workspace_dir/target}")"

# The CLI must match the wasm-bindgen library version locked in Cargo.lock exactly.
version="$(sed -n '/^name = "wasm-bindgen"$/{n;s/^version = "\(.*\)"$/\1/p;q;}' "$workspace_dir/Cargo.lock")"
if [[ -z "$version" ]]; then
  echo "error: wasm-bindgen is missing from Cargo.lock" >&2
  exit 1
fi
tool_root="$target_dir/tools/wasm-bindgen-$version"
if [[ ! -x "$tool_root/bin/wasm-bindgen" ]]; then
  cargo install --locked --root "$tool_root" wasm-bindgen-cli --version "$version"
fi

cd "$workspace_dir"
if ! rustup target list --installed | grep -qx wasm32-unknown-unknown; then
  rustup target add wasm32-unknown-unknown
fi
cargo build --locked --release --package kusto-explorer-ui --target wasm32-unknown-unknown

pkg_dir="$target_dir/kusto-explorer/pkg"
rm -rf "$pkg_dir"
"$tool_root/bin/wasm-bindgen" --target web --no-typescript --out-dir "$pkg_dir" \
  "$target_dir/wasm32-unknown-unknown/release/kusto_explorer_ui.wasm"

bash "$project_dir/scripts/web-deps.sh"
cd "$project_dir"
node web/build.mjs --pkg "$pkg_dir" --out "$out_dir"
