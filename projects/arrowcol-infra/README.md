# Arrowcol local SQL Server + Hive Metastore

This project provides a self-contained local control-plane persistence slice:

- one SQL Server 2022 Linux instance;
- separate `arrowcol` and `hive_metastore` databases;
- Apache Hive Metastore 4.0.1 on Thrift port `9083`;
- the unmodified Hive 4.0.0 SQL Server vendor schema, installed and validated
  by Hive `schematool`; and
- versioned Arrowcol application migrations and integration verification.

The services communicate through Compose DNS (`sqlserver`) and keep SQL data
and the local Hive warehouse in project-scoped named volumes. Host ports bind
to loopback only.

## Bootstrap and run

Docker Engine with Compose v2, OpenSSL, Bash, and approximately 4 GiB of
available memory are required. SQL Server's published Linux image is amd64
only; an arm64 host needs amd64 emulation.

```bash
npx --no-install nx run arrowcol-infra:bootstrap
npx --no-install nx run arrowcol-infra:config-check
npx --no-install nx run arrowcol-infra:up
npx --no-install nx run arrowcol-infra:integration-verify
```

`bootstrap` copies `.env.example` to the ignored `.env` file and creates four
independent, mode-`0600` local secret files beneath the ignored `secrets/`
directory. It is idempotent and never prints password values. Existing secrets
are preserved; use `./projects/arrowcol-infra/scripts/bootstrap-secrets.sh
--force` only after `arrowcol-infra:reset` when intentionally rotating all
local credentials. The script refuses to rotate the `sa` secret while the
persistent SQL volume exists because SQL Server retains the password stored in
that volume. Do not reuse these generated development credentials in another
environment. `.dockerignore` also excludes `.env` and the complete secrets
directory from every image build context.

The long-running readiness gates are deterministic:

- SQL Server executes `SELECT 1` through `sqlcmd`;
- the Metastore opens its Thrift listener only after vendor schema
  initialization and verification finish; and
- Compose waits for both service health checks.

Initialization jobs are noninteractive and fail on the first SQL or schema
error. Shutdown is bounded by `STACK_STOP_SECONDS` (30 seconds by default).

## Operations

```bash
npx --no-install nx run arrowcol-infra:logs
npx --no-install nx run arrowcol-infra:down
npx --no-install nx run arrowcol-infra:reset
npx --no-install nx run arrowcol-infra:clean
```

- `down` removes only this project's containers and network; named data volumes
  remain.
- `reset` also removes only this project's named SQL/warehouse volumes.
- `clean` additionally removes only the images built by this Compose project.
  It does not run a global Docker prune and does not delete secret files.

`SQLSERVER_PORT` defaults to `14333`; `HIVE_METASTORE_PORT` defaults to `9083`.
Both can be changed in `.env`.

## Database ownership and secrets

The bootstrap creates:

| Database | Principal | Local role |
| --- | --- | --- |
| `arrowcol` | `arrowcol_app` | `db_datareader`, `db_datawriter` |
| `arrowcol` | `arrowcol_migrator` | `db_owner` for local migrations |
| `hive_metastore` | `hive_metastore` | `db_owner` for vendor schema management |

The upstream SQL Server image requires `MSSQL_SA_PASSWORD`. Its minimal wrapper
reads the Docker secret immediately before `exec` and exports the value only to
the SQL Server process. The Hive wrapper does **not** put its password in
`SERVICE_OPTS`: the upstream Hive entrypoint enables shell tracing. Instead it
creates a mode-`0600` runtime `hive-site.xml` from the Docker secret.

Compose's local secret implementation is file-backed rather than a Swarm secret
store. Host file permissions are therefore part of the local security boundary.
Production deployments should use their platform secret manager, TLS with a
trusted SQL Server certificate, and separately scoped runtime/schema-owner
principals.

## Hive vendor schema

`hive-schema` runs:

```text
schematool -dbType mssql -initOrUpgradeSchema
schematool -dbType mssql -validate
schematool -dbType mssql -info
```

Hive 4.0.1 intentionally reports vendor schema version `4.0.0`. The schema
comes from `/opt/hive/scripts/metastore/upgrade/mssql` in the official Apache
image. No Hive vendor table or migration is copied, redesigned, or modified by
this project. `hive-metastore` starts with schema auto-creation disabled and
schema verification enabled.

The warehouse volume is durable for local catalog locations, but this slice is
not an object store or a HiveServer2 deployment.

## Arrowcol application schema

`scripts/container/run-migrations.sh` applies sorted `V*.sql` files under an
exclusive SQL application lock. Each migration is transactional, records its
SHA-256 checksum in `arrowcol.schema_migrations`, skips an identical recorded
version, and fails if an applied file changes.

The current migrations define:

- collector instances with active-instance and expiry lookups;
- schema-change sagas, normalized catalog/table identity, immutable version
  history, ordered stable field identity/position/number, descriptor artifact
  chunks, and current schema heads;
- latest and historical pressure snapshots;
- persistent fencing counters and lease history;
- production-router collector registrations, rich latest/history pressure
  snapshots, and one CAS/fencing lease per implementation-pool/table key;
- SQL Server-generated delivery receipts with optimistic accepted → buffered →
  flushing → published/failure transitions, owner/table-lease fencing,
  heartbeat/expiry reconciliation, and covering hot-path indexes;
- append-only ordered receipt events plus durable Delta version/file/row/byte
  observations. Duplicate metadata is intentionally stored as a new delivery;
  there is no receipt idempotency or deduplication key; and
- per-owner durable receipt recovery windows that keep a live or recently
  failed durable-buffer owner's receipts out of expiry reconciliation, plus
  append-only `adopted` receipt events recording a same-key successor's
  ownership rebinding without changing receipt state or revision.

All application timestamps are `datetime2(7)` columns explicitly named `_utc`.
SQL stores only control metadata, byte counts, and fixed-size SHA-256 hashes.
Arrow batch/payload bytes are never stored in SQL; `destination_uri` points at
the external delivery destination.

## Verification

`integration-verify` checks:

1. Compose/static configuration and image/checksum pins.
2. SQL Server and Metastore health.
3. Separate online databases and expected local principals.
4. All application tables, trusted constraints, filtered/covering hot-path
   indexes (including the durable-owner-aware expiry scan), durable recovery
   window and adoption-event columns, migration checksums, UTC timestamp
   policy, and absence of payload byte-storage types.
5. Hive core vendor tables and schema version `4.0.0`.
6. `schematool -validate`/`-info`.
7. A second database bootstrap, application migration, and Hive schema
   initialization pass, followed by the same checks.

On failure, the stack is intentionally left running so
`arrowcol-infra:logs` can capture evidence.

## Reproducibility pins and limitations

- SQL Server:
  `mcr.microsoft.com/mssql/server:2022-CU27-ubuntu-22.04@sha256:4402d880dd4c34bfa7d8705e56a86cd6c88da80a1f6bbbe741f999e76264a090`
  (exact amd64 manifest).
- Hive:
  `apache/hive:4.0.1@sha256:5194161ef50b80875f937dff04936df047cbe894e9727ab8f0606349345b1bd3`
  (multi-platform index).
- Microsoft JDBC:
  `mssql-jdbc-13.6.0.jre8.jar`, verified during BuildKit `ADD` with
  SHA-256
  `79da517f86a982f0c371f804ba06305708c43923786386c2582c81cb3e4ae648`.

The official Hive 4.0.1 image is based on Java 8, which is why the matching
JDBC artifact is selected. Remote-URL checksum verification requires the
BuildKit Dockerfile frontend used by current Compose v2. Registry retention,
host architecture/emulation, and Docker's platform selection remain external
tooling constraints even with digest pins.
