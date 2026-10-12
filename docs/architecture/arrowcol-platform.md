# Arrow collector platform architecture

This document defines the public contracts of the Arrow collector platform.
The root [quick start](../../README.md#arrow-collector-platform) is the shortest
path to a finite or continuous local run.

## System boundary

```text
                         control plane
       +------------------------------------------------+
       | SQL Server                                    |
       | - catalog sagas and immutable schema versions |
       | - collector registration and pressure         |
       | - routing and publication leases              |
       | - direct delivery receipts and events         |
       |                                                |
       | Apache Hive Metastore                         |
       | - catalog/database/table definitions          |
       +------------------------+-----------------------+
                                |
                    immutable schema artifacts
                                |
                 Rust and TypeScript SDK generation
                                |
                                v
client --HTTP/2 Flight--> router --stream--> selected collector
                                |                 |
                       direct pool          dataflow pool
                       durable receipt      best-effort admission
                                |                 |
                         direct Delta        dataflow Delta
```

SQL is a control plane. It stores identities, versions, bounded counts, hashes,
leases, receipt states, events, and Delta publication observations. It never
stores Arrow IPC messages, record batches, or Arrow value buffers.

## Router protocol and trust boundary

### Ingest metadata

Every ingest request uses `DoPut` and carries these initial gRPC metadata
fields:

| Metadata | Contract |
| --- | --- |
| `x-arrowcol-protocol-version` | Canonical decimal version; currently `1`. |
| `x-arrowcol-implementation-pool` | `direct` or `dataflow`. |
| `x-arrowcol-catalog` | Validated catalog component. |
| `x-arrowcol-schema` | Validated schema component. |
| `x-arrowcol-table` | Validated table component. |
| `x-arrowcol-schema-fingerprint` | `sha256:` plus 64 lowercase hexadecimal digits. |

Flight SQL control requests instead include
`x-arrowcol-control-operation: catalog_read | catalog_write | new_table` and an
implementation pool. A `new_table` operation does not require an existing
table lease.

The client fingerprint is an untrusted routing claim. The router validates its
shape but cannot prove the Flight body schema. The selected collector
incrementally decodes the schema message, computes the canonical fingerprint,
and revalidates the exact schema against the catalog before receipt creation or
DFE admission.

### Streaming, TLS, and authentication

The Pingora router does not decode or buffer the Flight body, does not retry
`DoPut`, and does not switch backends during a stream. It forwards HTTP/2
metadata, data frames, trailers, flow control, and cancellation to the selected
collector. Bounded decoding and batch channels exist in the collector, not in
the router.

The checked-in deployment boundary is:

1. a trusted edge terminates external TLS;
2. the edge signs time-bounded request metadata with its HMAC key;
3. the router verifies timestamp, nonce, signature, method, path, content type,
   routing metadata, and control metadata;
4. the router removes external proof and signs a bounded internal auth context
   with a different key; and
5. the collector verifies the internal context and authoritative routing lease.

The current pluggable router authenticator is `allow_all` after the trusted-edge
proof succeeds. Replacing it with a real identity provider and replacing the
signed internal hop with mTLS are explicit extension points. The repository
does not claim end-to-end mTLS or a production identity provider by default.

### Ownership, pressure, and cached leases

Routing ownership is unique for each
`(implementation_pool, catalog, schema, table)` key. A hot table therefore has
one owner in the direct pool and, independently, one owner in the dataflow
pool.

Collectors publish bounded rich pressure snapshots containing:

- CPU basis points and RSS/limit;
- active streams and cumulative received bytes, rows, and batches;
- buffered bytes, rows, and batches;
- available permits, pending commits, and oldest buffered age;
- Delta, DFE, and Quiver readiness/pressure state; and
- table-level stream, buffer, age, and commit observations.

An unexpired cached active lease keeps ingest routing off the SQL hot path. A
missing or expired lease uses serializable SQL compare-and-set. If SQL refresh
fails, an existing unexpired cached lease remains usable; the router does not
invent new ownership from stale cache data and fails closed after expiry.

Pressure or staleness reassignment is two phase:

1. compare-and-set the lease to `draining`, so new streams receive a retryable
   error;
2. leave existing streams on their selected backend;
3. wait for a fresh post-drain table snapshot with zero active streams,
   buffered bytes/rows/batches, and pending commits; and
4. transfer with a new lease ID and greater fencing epoch/version.

Collectors enforce the authoritative lease again immediately before each
Delta log commit. A stale owner can hold Arrow memory or finish Parquet work,
but it cannot publish a fenced Delta version.

## Catalog and Flight SQL contract

### Supported DDL

Flight SQL uses `CommandStatementUpdate` on `DoPut`, without ingest metadata.
The maintained-parser subset is exactly:

```sql
CREATE TABLE catalog.schema.table (
    field TYPE [NULL | NOT NULL],
    ...
) [PARTITIONED BY (...)]

ALTER TABLE catalog.schema.table
ADD COLUMNS (
    new_field TYPE NULL,
    ...
)
```

Transactions, body record batches, drops, renames, replacement, CTAS,
defaults, constraints, storage clauses, type/nullability changes, field
reordering, `FIRST`/`AFTER`, partition changes, and every other statement are
rejected.

Each successful change is a durable SQL/Hive saga:

1. reserve the operation and pending immutable version in SQL;
2. create or compare-and-set the Hive table through the official Metastore
   API;
3. persist deterministic `manifest.json`, `schema.arrow`, and `descriptor.pb`
   bytes in checked SQL chunks; and
4. atomically activate the new catalog head.

Existing fields, positions, protobuf numbers, types, nullability, and
partition roles never change. Evolution is limited to nullable, top-level,
tail-appended fields. Field positions and protobuf numbers are never reused.

### Exact schema admission and historical SDKs

Admission requires both:

- the claimed fingerprint to equal the fingerprint computed from the received
  Arrow schema; and
- that exact fingerprint and physical schema to identify one registered
  immutable version.

An arbitrary prefix, a structurally similar unregistered schema, a changed
field, a reordered field, or a mismatched fingerprint is rejected before a
direct receipt or DFE admission is created.

A generated SDK for historical version `N` remains valid after version `N+1`
only when the newer head is a nullable tail-append-only superset. The collector
reuses the historical arrays and appends all-null arrays for the newer fields.
Clients are never silently coerced across breaking physical layouts.

`arrowcol-sdk-gen compatibility` checks the same catalog rule plus protobuf
wire/JSON compatibility. It rejects removals, reorders, renumbering, name/type
changes, required additions, partition changes, and changed contents for one
immutable version.

### Generated artifact layout

The standalone repository workflow writes:

```text
crates/arrowcol-sdk-gen/.temp/catalog/
  manifest.json
  schema.arrow
  descriptor.pb

crates/arrowcol-sdk-gen/.temp/generated/
  sdk-manifest.json
  descriptors/
    manifest.json
    schema.arrow
    descriptor.pb
  rust/
    Cargo.toml
    src/lib.rs
    tests/generated.rs
  typescript/
    package.json
    src/index.ts
    test/generated.test.ts
```

The production load generator exports and generates one directory per table:

```text
projects/loadtest/.temp/loadgen/<run-id>/artifacts/<table>/
projects/loadtest/.temp/loadgen/<run-id>/generated/<table>/
```

Both roots are gitignored. Generation replaces a staged output directory so
stale files disappear; `check` regenerates in memory and compares exact bytes.
`package-artifact` creates a deterministic archive, also under a gitignored
output root. Consumers that need long-lived historical SDKs must retain the
packaged artifact outside the disposable workspace output.

## Direct delivery and purge contract

### Receipt timeline

A non-empty direct `RecordBatch` follows this sequence:

1. the collector validates metadata, internal identity, routing lease, and the
   exact received Arrow schema;
2. SQL Server creates a fresh receipt in `accepted`;
3. bounded global and per-table capacity is reserved;
4. SQL transitions the receipt to `buffered` and the process retains the Arrow
   arrays in memory;
5. Flight emits a `PutResult` containing that receipt ID;
6. a grouped flush transitions receipts to `publishing`;
7. Parquet is closed and synced, then the publication fence is checked;
8. the Delta log commit becomes durable and visible; and
9. SQL atomically records the Delta version/file/row/byte observation and
   transitions every grouped receipt to `published`.

The Flight `PutResult` in step 5 is only an acknowledgement that the receipt
and bounded in-memory admission exist. It is not a durable payload
acknowledgement and is never purge permission.

The authoritative client rule is:

```text
GET /v1/receipts/{receipt-id}
    purge_allowed == true  <=>  state == "published"
```

Only the HTTP `published` result after the durable Delta commit permits source
purge. `accepted`, `buffered`, `publishing`, retryable failure, permanent
failure, and `publication_unknown` do not.

### Duplicates, crash, and lease expiry

- Every physical delivery receives a new server-generated receipt. A
  byte-identical resend is a valid new delivery, not an idempotent retry.
- The platform provides no payload-deduplication key and makes no exactly-once
  claim. A client and downstream consumer must tolerate duplicate rows.
- If an owner crashes and its `accepted` or `buffered` receipt lease expires,
  reconciliation records a retryable failure. The client may resend, creating
  a new receipt.
- If a `publishing` receipt expires, reconciliation records
  `publication_unknown`. The file/log commit may have crossed the durable
  boundary, so the platform does not blindly replay it. Explicit Delta/operator
  reconciliation must resolve the existing receipt first.
- After owner/lease expiry, a replacement may register the same stable
  instance key only with a greater generation. The expired predecessor is
  fenced from heartbeat and publication.

SQL contains only receipt/control metadata, fixed-size hashes, counts, errors,
and publication observations. Losing a collector process loses its in-memory
Arrow arrays; there are no Arrow bytes in SQL from which to reconstruct them.

## Direct and DFE capability matrix

| Capability | Direct pool | DFE pool |
| --- | --- | --- |
| Arbitrary Arrow `RecordBatch` ingest | Supported | Supported through the representation-local `arrowcol.flight@1.0` envelope |
| Exact catalog schema validation | Required before receipt | Required before DFE admission |
| Flight SQL DDL | Supported | Supported |
| Flight completion | One server-generated receipt per batch | `best_effort_admission` only |
| SQL delivery receipt | Yes | No |
| HTTP receipt polling | Yes | No |
| Purge signal | Only HTTP `published` with `purge_allowed: true` | None |
| At-least-once path | Client polling plus resend of retryable failures; duplicates are possible | Not claimed |
| Downstream Ack/Nack | Reflected by receipt/publication state | Reflected later in pressure and metrics; may occur after Flight completion |
| Routing and commit fencing | Yes | Yes |
| Native Quiver for arbitrary Arrow | Unsupported and rejected by config | Unsupported and rejected by DFE representation validation |

The DFE collector does not relabel best-effort channel admission as durability.
Its capability endpoint and
`arrowcol_delivery_guarantee_info{delivery_guarantee="best_effort",...}` metric
state that receipt polling and at-least-once delivery are false.

Quiver support for fixed OTAP bundle slots does not provide the arbitrary
table/schema/fingerprint/receipt representation needed here. Both collector
implementations reject that mode instead of converting arbitrary Arrow into
OTAP slots or claiming a persistence guarantee they do not have.

## Buffer, file, and Delta controls

The direct collector exposes strict YAML controls in
[`projects/arrowcol-rust/config.yaml`](../../projects/arrowcol-rust/config.yaml):

| Control | Checked-in local value | Purpose |
| --- | ---: | --- |
| `buffer.global_capacity.bytes` | 2 GiB | Process-wide retained and in-flight Arrow bytes |
| `buffer.per_table_capacity.bytes` | 512 MiB | Per-table retained and in-flight Arrow bytes |
| `buffer.max_arrow_memory_per_flush` | 512 MiB | Logical Arrow input rotation bound |
| `buffer.max_rows_per_flush` | 1,000,000 | Row rotation bound |
| `buffer.max_batches_per_flush` | 512 | Batch rotation bound |
| `buffer.max_age_ms` | 30,000 | Oldest-batch flush trigger |
| `buffer.max_concurrent_flushes` | 4 | Independent flush concurrency |
| `buffer.adaptive_file.target_parquet_bytes` | 128 MiB | Compressed Parquet target |
| `buffer.adaptive_file.min_input_bytes` | 16 MiB | Lower logical-input estimate clamp |
| `buffer.adaptive_file.max_input_bytes` | 1 GiB | Upper logical-input estimate clamp |
| `delta.commit_batch_window_ms` | 10,000 | Delta file collection window |
| `delta.max_files_per_commit` | 64 | Maximum file actions per commit |
| `delta.max_pending_files` | 256 | Queued and committing file capacity |

The loadtest direct template keeps the 128 MiB compressed target but lowers
the cold-start minimum input clamp to 1 MiB and the age/commit windows for
finite tests. All limits are configuration, not unbounded defaults.

The DFE collector separately bounds its Flight decoder, DFE pdata channel,
reported buffered capacity, authority timeout, and Delta pending-file queue in
[`projects/arrowcol-dataflow-rust/config.yaml`](../../projects/arrowcol-dataflow-rust/config.yaml).
DFE admission remains best effort even when those queues are healthy.

Local Delta roots are deliberately separate:

| Context | Direct root | DFE root |
| --- | --- | --- |
| Direct host config | `projects/arrowcol-rust/.data/delta` | Not applicable |
| Direct container config | `/var/lib/arrowcol/delta` | Not applicable |
| DFE host/container override | Not applicable | `ARROWCOL_DELTA_ROOT`, default `/onelake` |
| Benchmark host | `onelake/benchmark/<run-id>/direct` | `onelake/benchmark/<run-id>/dataflow` |
| Benchmark container | `/delta/direct` | `/delta/dataflow` |

## Metrics and dashboards

The platform exports low-cardinality process metrics and SQL-derived durable
state. Representative groups are:

- router decisions, failures, active streams, cached leases, lease events, and
  maximum collector pressure;
- direct active streams, decoder bytes, received batches/rows/bytes, control
  failures, reconciled receipts, and incomplete shutdown groups;
- DFE channel occupancy, downstream Ack/Nack, pending commits, Delta
  files/rows/bytes/failures, and delivery-guarantee truth;
- SQL receipt state/count/rows/Arrow bytes, published totals, failures, and
  creation-to-publication and flushing-to-publication latency; and
- SQL collector/table pressure, snapshot age, routing owner, lease age, and
  time to expiry.

Receipt IDs and lease UUIDs are not metric labels.

The loadtest observability overlay provisions two independent Grafana folders:

- **Arrow Flight**: router, direct receipts, direct-versus-DFE throughput and
  pressure, Delta files, schema state, resources, logs, and traces.
- **OTEL Collector Baseline**: only the optional OTEL collector's DFE engine,
  resources, logs, and traces.

The Arrow dashboard is the Grafana home dashboard. The OTEL baseline is never
mixed into the arbitrary-Arrow receipt/delivery panels.

## Intentional limitations

- The benchmark uses one host and shared bind-mounted Delta directories. It
  validates routing, process failure, local filesystem fencing, and independent
  snapshot reads; it is not an object-store or distributed-filesystem commit
  benchmark.
- One owner per pool/table means a single hot table cannot be written
  concurrently by multiple collectors in the same pool. More collectors scale
  across tables, not within one table.
- DFE completion is best-effort admission, with no durable receipt, polling, or
  at-least-once claim.
- Native Quiver persistence for arbitrary Arrow is unsupported in both pools.
- Delta output is append-only. There is no upsert, overwrite, compaction,
  cross-table transaction, or exactly-once guarantee.
