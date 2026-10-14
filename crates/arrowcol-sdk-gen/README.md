# arrowcol-sdk-gen

Deterministically generates typed Rust and TypeScript SDK artifacts from one
immutable Arrowcol catalog schema bundle.

## Artifact input

The generator reads three files from an artifact directory:

```text
manifest.json   canonical normalized CatalogSchemaVersion manifest
schema.arrow    Arrow IPC schema message
descriptor.pb   protobuf FileDescriptorSet
```

Every file is regenerated in memory from the manifest and compared byte for
byte before code generation. A stale, edited, lossy, or inconsistent bundle is
rejected.

## Safe output workflow

All write commands require an explicit workspace-relative path below a
gitignored `.temp` or `target` directory. Existing output is replaced as one
staged directory, so stale files disappear. Absolute paths, traversal, symlinked
parents, generated-name collisions, and unsupported cross-language types fail
closed.

```bash
# Example-only artifact fixture from public SQL DDL.
cargo run --locked -p arrowcol-sdk-gen -- \
  fixture \
  --ddl crates/arrowcol-sdk-gen/fixtures/telemetry-events.sql \
  --out crates/arrowcol-sdk-gen/.temp/catalog

# Generate both language packages from the same normalized artifacts.
cargo run --locked -p arrowcol-sdk-gen -- \
  generate \
  --artifacts crates/arrowcol-sdk-gen/.temp/catalog \
  --out crates/arrowcol-sdk-gen/.temp/generated

# Verify exact regeneration without changing files.
cargo run --locked -p arrowcol-sdk-gen -- \
  check \
  --artifacts crates/arrowcol-sdk-gen/.temp/catalog \
  --out crates/arrowcol-sdk-gen/.temp/generated

# Verify a candidate artifact set against a previous immutable version.
cargo run --locked -p arrowcol-sdk-gen -- \
  compatibility \
  --previous path/to/version-1-artifacts \
  --current path/to/version-2-artifacts

# Create a deterministic distributable archive.
cargo run --locked -p arrowcol-sdk-gen -- \
  package-artifact \
  --input crates/arrowcol-sdk-gen/.temp/generated \
  --out crates/arrowcol-sdk-gen/.temp/arrowcol-sdk.tar.gz
```

The generated `sdk-manifest.json` contains table identity, version,
fingerprint, descriptor hashes, Flight routing details, package roots, and
content hashes for every generated file. External load generators can consume
that manifest without inspecting generator source.

The checked-in Nx workflow uses this exact gitignored layout:

```text
crates/arrowcol-sdk-gen/.temp/catalog/
  manifest.json
  schema.arrow
  descriptor.pb

crates/arrowcol-sdk-gen/.temp/generated/
  sdk-manifest.json
  descriptors/
  rust/
  typescript/
```

`rust/` is the generated Cargo package and `typescript/` is the generated npm
package. Generation atomically replaces the output directory; `check` verifies
exact regeneration; `package-artifact` writes a deterministic archive beneath
the same `.temp` root. These artifacts are intentionally not committed.
Archive a packaged historical SDK outside disposable workspace output when it
must remain available to clients.

After nullable tail-only catalog evolution, a historical SDK remains valid
because the server accepts its exact registered schema version and null-fills
only the new fields. The `compatibility` command verifies both that physical
layout rule and protobuf wire/JSON stability before a new SDK is distributed.

## Generated APIs

Both languages expose:

- typed row and nested-value builders;
- exact Arrow schema and record-batch construction;
- table identity, schema version, and fingerprint constants;
- bounded validated routing metadata;
- Arrow Flight `DoPut` descriptor/request helpers;
- receipt-ID parsing and canonical receipt-status URL helpers.

Unsupported dictionary and ListView representations are rejected because the
shared Rust/Apache Arrow JavaScript construction path cannot currently preserve
them losslessly. Supported scalar, timestamp/timezone, binary/view, decimal,
list, large-list, fixed-size-list, struct, and map mappings retain their exact
catalog physical layout.

## Nx workflows

```bash
npx --no-install nx run arrowcol-sdk-gen:generate
npx --no-install nx run arrowcol-sdk-gen:check
npx --no-install nx run arrowcol-sdk-gen:generated-test
npx --no-install nx run arrowcol-sdk-gen:package-artifact
npx --no-install nx run arrowcol-sdk-gen:verify
```
