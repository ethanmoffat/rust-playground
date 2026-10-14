# arrowcol-rust

Production direct Arrow collector: Arrow Flight `DoPut` and Flight SQL DDL in,
bounded zero-copy Arrow buffers in memory, fenced append-only Delta tables out.

## Data and control path

```text
router / direct authenticated client
        |
        v
arrowcol-flight
  metadata + auth + incremental IPC decode
        |
        v
arrowcol-catalog
  exact registered schema / nullable-append alignment
        |
        v
one SQL-generated receipt per RecordBatch
        |
        v
arrowcol-buffer
  reserve global + per-table bytes/rows/batches
  retain Arrow arrays by reference
  group by table + schema version + publication fence
        |
        v
arrow-deltalake
  stream one Parquet file, fence immediately before Delta commit
        |
        v
arrowcol-receipts
  atomically publish every grouped receipt after durable Delta visibility
```

Every decoded non-empty `RecordBatch` receives a fresh server-generated receipt,
including byte-identical resends. Each receipt is emitted in its own Flight
`PutResult`; there is no client idempotency key or deduplication path. A
`PutResult` means SQL has recorded the receipt and the process has retained the
batch in its bounded in-memory buffer. It is not a durable payload
acknowledgement and is not purge permission. Source data may be purged only
when `GET /v1/receipts/{id}` returns `"state": "published"` and
`"purge_allowed": true`, after the fenced Delta commit is durable.

Unknown, malicious, or breaking Arrow schemas are rejected before receipt
creation. Existing Arrow arrays are not concatenated or deep-copied. Historical
registered schemas use the catalog's explicit source/null-fill alignment plan.

## Configuration

Both checked-in configurations are strict YAML (`deny_unknown_fields`):

- `config.yaml`: local host ports for `arrowcol-infra` and the production
  router's internal HMAC secret.
- `config.docker.yaml`: Compose/Kubernetes-style service names, `/run/secrets`
  paths, durable `/var/lib/arrowcol/delta`, and all-interface binds. Replace
  `flight.advertised_endpoint` with the collector's stable container/pod IP;
  the current routing contract intentionally stores a `SocketAddr`, not DNS.

The document covers:

- Flight/admin binds and advertised endpoint;
- SQL Server and Hive Metastore endpoints and timeouts;
- Delta root and bounded commit batching;
- direct implementation pool and process identity/generation;
- router HMAC trust and authoritative routing-lease headers;
- receipt-owner, receipt, and table-lease heartbeat/expiry timings;
- global/per-table byte, row, and batch capacities;
- Arrow-memory, row, batch, age, and flush-concurrency caps;
- adaptive compressed Parquet sizing (128 MiB by default);
- Flight message/channel/deadline limits;
- TLS identity and optional client CA;
- router-HMAC, static-bearer, or explicit development allow-all auth;
- graceful stream/publication drain ceilings; and
- optional native Quiver settings.

Direct reuses separate bounded SQL Server connection pools for receipts,
routing, and catalog operations. `sql_server.pool` defaults to
`receipts_max_size: 16`, `routing_max_size: 4`, `catalog_max_size: 4`, and
`acquire_timeout_ms: 5000`; existing configurations may omit the section.
Checkout, connection/login, and command deadlines remain distinct. Saturation
surfaces a database error rather than opening connections beyond the limit.
Cancelled or uncertain operations discard their connection, and completed
transactions are checked and reset before reuse. Durable receipt and fencing
semantics are unchanged; router and Dataflow stores do not opt into these pools.
The `/metrics` endpoint exposes `arrowcol_direct_sql_pool_*` counters and pool
occupancy with fixed `receipts`, `routing`, and `catalog` role labels.

Native Quiver is rejected during config validation with the typed
`native_quiver_generic_arrow` capability error. The checked-in Quiver boundary
supports fixed OTAP slots, not arbitrary Arrow table/schema/receipt identities,
so this collector does not invent a lossy adapter.

Validate without opening sockets or dependencies:

```bash
cargo run --locked --package arrowcol-rust -- \
  --validate-and-exit --config projects/arrowcol-rust/config.yaml

npx --no-install nx run arrowcol-rust:config-check
```

## Flight and HTTP API

### Arrow Flight

- `DoPut` with all six `arrowcol-protocol` ingest metadata fields:
  incremental direct ingest with one receipt result per `RecordBatch`.
- Flight SQL `CommandStatementUpdate` without ingest metadata:
  strict `CREATE TABLE` and nullable tail-only `ALTER TABLE ... ADD COLUMNS`
  through `arrowcol-catalog`.
- Other Flight RPCs remain explicitly unimplemented.

When `auth.mode: router_hmac`, the collector verifies:

- `x-arrowcol-internal-auth-context`
- `x-arrowcol-internal-auth-expires`
- `x-arrowcol-internal-auth-signature`
- `x-arrowcol-routing-lease-id`
- `x-arrowcol-routing-fencing-epoch`
- `x-arrowcol-routing-lease-version`
- `x-arrowcol-routing-lease-expires`

The HMAC assertion, operation scope, current SQL routing view, holder instance,
lease lineage/version/fence, and expiry must all agree before body admission.

### Admin HTTP

| Path | Purpose |
| --- | --- |
| `/healthz` | Process health; fatal heartbeat/ownership failures fail closed |
| `/readyz` | SQL/catalog/Hive, registration, lease loop, writable Delta root, admission |
| `/v1/capabilities` | Versioned direct-collector capability document |
| `/v1/pressure` | Versioned bounded pressure document |
| `/v1/receipts/{uuid}` | Receipt status and authoritative `purge_allowed` |
| `/v1/receipts?ids=a,b` | Bounded receipt status batch |
| `/metrics` | Prometheus text exposition |

The admin bind is intended for a protected internal network. The external
router already provides authenticated receipt status APIs.

## Startup, fencing, and shutdown

Startup completes before listeners serve:

1. connect to SQL Server and reconcile incomplete catalog sagas through Hive;
2. probe SQL catalog state and Hive Metastore;
3. verify create/sync/remove access on the Delta root;
4. reconcile expired receipts in bounded batches;
5. register the receipt owner and routing collector;
6. publish an initial rich pressure snapshot; and
7. start owner heartbeat, routing heartbeat/pressure, table-lease renewal, and
   dependency-readiness loops.

Expired `accepted`/`buffered` receipts become retryable. Expired `flushing`
receipts become `publication_unknown` and require explicit Delta/operator
reconciliation; they are never blindly replayed.

SQL stores receipt/control metadata, hashes, counts, errors, and Delta
publication observations only. Arrow IPC and Arrow value buffers are never
stored in SQL. A crash therefore loses the in-memory batch; after lease expiry
the retryable receipt result tells the client to resend, creating a new
delivery and a new receipt. A resend can coexist with an earlier copy that was
already published or later reconciled, so downstream consumers must tolerate
duplicates.

One SQL receipt/publication lease is held per table. Successful renewals update
the synchronous Delta fence. Routing ownership loss, heartbeat failure, renewal
failure, or expiry invalidates that fence immediately and stops admission.
After a crashed process lease expires, a replacement may claim the same
`identity.instance_key` only with a strictly greater `identity.generation`.
Registration atomically marks the expired predecessor failed and releases its
remaining SQL table leases; the predecessor's next heartbeat is fenced.

Graceful shutdown:

1. mark routing state draining and stop new Flight admission;
2. allow active streams to finish, then force-cancel at the configured ceiling;
3. persist receipt-owner drain state;
4. bypass Delta collection windows and flush every buffer;
5. await Delta visibility and grouped SQL receipt completion;
6. release table leases and finish the receipt owner; and
7. mark routing state stopped.

Incomplete Delta/receipt work is logged with table, receipt IDs, reason, and any
known durable publication. The process exits nonzero when drain is incomplete.

## SQL/Hive bootstrap and migrations

This binary does not own schema migration privileges. Bootstrap the existing
application database and official Hive Metastore schema first:

```bash
npx --no-install nx run arrowcol-infra:bootstrap
npx --no-install nx run arrowcol-infra:up
npx --no-install nx run arrowcol-infra:integration-verify
```

`projects/arrowcol-infra/migrations/` contains the catalog saga, receipt,
publication-fence, collector registration, pressure, and routing lease tables.
Use a separately scoped migrator principal in production. The collector uses
only the runtime application principal and never modifies Hive vendor tables.

## Validation

```bash
cargo fmt --package arrowcol-rust -- --check
cargo clippy --locked --package arrowcol-rust --all-targets -- -D warnings
cargo test --locked --package arrowcol-rust
RUSTDOCFLAGS='-D warnings' cargo doc --locked --package arrowcol-rust --no-deps
npx --no-install nx run arrowcol-rust:verify
```

Unit/in-process integration coverage includes strict config, typed Quiver
rejection, router HMAC tamper rejection, pre-receipt schema rejection,
per-batch duplicate receipts, bounded backpressure, grouped receipt publication,
stale-fence rejection, graceful buffer drain, and reading committed Delta
Parquet content.

Live SQL Server + official Hive Metastore + Delta coverage:

```bash
npx --no-install nx run arrowcol-rust:live-e2e
```

The harness bootstraps/starts `arrowcol-infra`, runs generated-schema and
malicious-schema Flight paths, duplicate/grouped publication, expired uncertain
receipt reconciliation, component restart/resend duplication, Delta reader
content, and an intentional SQL outage probe. It restarts SQL for recovery,
always stops the Compose services, and retains logs/results under
`projects/arrowcol-rust/.diagnostics/<run-id>/`.

## Container

The multi-stage `Dockerfile` builds from the pinned root toolchain, lockfile,
workspace, and `otel-arrow` submodule. The pinned `linux/amd64` runtime runs as
UID/GID `10001`, keeps `config.docker.yaml` read-only, owns writable
`/var/lib/arrowcol/delta`, includes CA certificates, and probes `/readyz`.

The dependency-free image smoke validates the embedded strict YAML and runtime
filesystem contract. Full readiness still requires the documented SQL Server,
Hive Metastore, secret files, and Delta storage:

```bash
npx --no-install nx run arrowcol-rust:image-smoke
npx --no-install nx run arrowcol-rust:publish
```

Publication is clean-worktree guarded and records the exact published
references under `target/images/`.
