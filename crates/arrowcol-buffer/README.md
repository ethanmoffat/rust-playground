# arrowcol-buffer

Bounded in-memory buffering and durable publication coordination for direct
Arrow collectors.

## Design

- Queues are isolated by table, registered schema version/fingerprint, and
  table lease. `RecordBatch` values keep their existing reference-counted Arrow
  arrays; batches are never concatenated or deep-copied.
- Admission reserves global and per-table byte, row, and batch capacity before
  the manager retains a delivery. Capacity remains reserved through durable
  publication and receipt completion.
- A queue flushes on the first adaptive compressed-size target, Arrow-memory
  limit, row limit, batch limit, maximum age, schema boundary, lease boundary,
  explicit drain, or shutdown.
- `PublicationSink` / `PublicationSession` form the storage-neutral streaming
  worker interface. `DeltaPublicationSink` adapts the shared
  `arrow-deltalake::commit::BatchedTable`, preserving typed stale-fence and
  uncertain-commit failures.
- Every grouped receipt moves to flushing before sink I/O and becomes
  published only after the sink reports a durable commit. A SQL failure after
  durable storage poisons the table and appears in `ShutdownReport` with the
  publication observation.
- `BufferManager::pressure` reports exact capacity held by queued and in-flight
  batches. Flush concurrency is bounded independently.
- `shutdown` closes admission, waits for active admissions, drains all queues,
  releases Arrow references, and reports incomplete work.

## Direct worker integration

Create one validated receipt per Flight delivery, then submit:

```text
Admission {
  receipt: ReceiptVersion,
  delivery: ValidatedDelivery,
  lease: TableLease,
  batches: Vec<RecordBatch>,
}
```

Use `admit_with_timeout` when the transport has a request deadline. Call
`drain` for an explicit flush while admission remains open. During process
shutdown, first put the SQL owner into draining state, call `shutdown`, resolve
or report any `IncompleteWork`, and then finish the owner drain.

## Quiver boundary

`QuiverPersistence::open` returns
`BufferErrorKind::UnsupportedCapability`. The checked-in Quiver format is tied
to fixed OTAP bundle slots and cannot directly persist arbitrary Arrowcol
table/schema identities, schema fingerprints, receipt IDs, and generic record
batches. A future adapter must add those native persistence semantics; this
crate does not invent a lossy mode or map arbitrary tables into OTAP slots.

## Validation

```bash
npx --no-install nx run arrowcol-buffer:verify
```

Tests cover zero-copy array identity, capacity/backpressure, every flush
trigger, adaptive convergence, schema isolation, grouped receipt publication,
receipt and sink failures, stale fencing, cancellation, explicit drain, and
shutdown reporting.
