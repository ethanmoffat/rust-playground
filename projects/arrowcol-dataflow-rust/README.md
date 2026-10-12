# Arrowcol Dataflow Rust collector

This binary links the DFE-native Arrowcol components and calls the fork's
standard startup entry point. The declarative pipeline in `config.yaml` is:

```text
Arrow Flight -> bounded DFE channel -> generic Delta exporter
```

The process is pinned to one DFE core in both YAML policy and the documented
command. Delta table ownership is process-shared, but a single pipeline core
also keeps Flight registration and admin lifecycle unambiguous.

## Build and validate

```bash
npx --no-install nx run arrowcol-dataflow-rust:build
npx --no-install nx run arrowcol-dataflow-rust:config-check
npx --no-install nx run arrowcol-dataflow-rust:smoke
npx --no-install nx run arrowcol-dataflow-rust:image-smoke
npx --no-install nx run arrowcol-dataflow-rust:verify
```

`config-check` validates the Docker-ready SQL/Hive configuration, proves that
`native_quiver: true` reports the exact missing persistence capability, and
proves that an OTAP-only durable-buffer node is rejected by DFE wiring
contracts. `smoke` starts the memory-backed topology, verifies every admin
endpoint and capability claim, sends `SIGTERM`, and requires bounded graceful
shutdown.

## Local infrastructure

Start the repository SQL Server and Hive Metastore:

```bash
npx --no-install nx run arrowcol-infra:bootstrap
npx --no-install nx run arrowcol-infra:up
```

Then run the collector with host-facing settings:

```bash
ARROWCOL_SQL_HOST=127.0.0.1 \
ARROWCOL_SQL_PORT=14333 \
ARROWCOL_SQL_PASSWORD_FILE=projects/arrowcol-infra/secrets/arrowcol-app-password.txt \
ARROWCOL_HIVE_HOST=127.0.0.1 \
ARROWCOL_HIVE_PORT=9083 \
ARROWCOL_DELTA_ROOT="$PWD/onelake/arrowcol-dataflow" \
npx --no-install nx run arrowcol-dataflow-rust:run
```

The live SQL/Hive/Delta qualification is:

```bash
npx --no-install nx run arrowcol-dataflow-rust:live-e2e
```

The script owns one bounded Compose window, connects the workspace container
to the infrastructure network when necessary, and always disconnects and
stops the stack. Logs and Compose evidence are retained under ignored
`.diagnostics/` run directories.

## Endpoints

| Endpoint | Purpose |
| --- | --- |
| `0.0.0.0:32010` | Arrow Flight `DoPut` and strict Flight SQL DDL |
| `/healthz` | Process health |
| `/readyz` | Registration/catalog/admission readiness |
| `/v1/capabilities` | Honest delivery and feature metadata |
| `/v1/pressure` | Rich router pressure plus occupancy provenance |
| `/metrics` | Prometheus text metrics |

Admin HTTP listens on port `32011` by default.

## Delivery guarantee

The DFE pool returns only `best_effort_admission`. A successful `PutResult`
confirms bounded DFE admission, not Delta publication. This collector has no
SQL delivery receipts, no receipt polling endpoint, and no at-least-once
claim. Delta failures after admission are visible through DFE Nacks, pressure,
and metrics.

## Delta and schema behavior

`arrowcol-catalog` performs exact schema lookup before the first batch. Current
schemas pass through unchanged; historical nullable-append schemas reuse
source arrays and add only null arrays. The pdata envelope preserves logical
table, source/target catalog schema versions, lease lineage, owner, and fencing
epoch. The exporter writes the original arbitrary Arrow batch through
`arrow-deltalake`, which performs only its generic lossless Delta physical
mapping.

One bounded `BatchedTable` owner exists per logical table in the process.
Every commit rechecks the authoritative routing lease through the dedicated
fence worker. Shutdown stops Flight admission, drains active streams, forces
pending Delta batches, publishes final pressure, and then relinquishes
registration.

## Native durable buffer

The DFE Quiver adapter deliberately returns typed unsupported for
`arrowcol.flight@1.0`; it cannot yet persist and reconstruct representation
metadata and arbitrary tables. The collector therefore rejects native Quiver
configuration rather than converting to OTLP/OTAP or claiming durability.

## Container

`Dockerfile` is ready for local/CI builds and expects:

- `/run/secrets/arrowcol-app-password`;
- SQL Server DNS `sqlserver:1433`;
- Hive Metastore DNS `hive-metastore:9083`; and
- writable `/onelake`.

The pinned `linux/amd64` runtime runs as UID/GID `10001`, keeps both embedded
configs read-only, and exposes production readiness on port `32011`.
`image-smoke` validates the production and memory-backed YAML/capability graphs,
starts the memory topology without credentials, and checks readiness plus the
honest capability document. `publish` is clean-worktree guarded and writes its
exact tag manifest under `target/images/`.
