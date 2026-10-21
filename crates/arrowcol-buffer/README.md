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

## Quiver durable stage

`QuiverStage` puts a Quiver write-ahead log and segment store in front of the
buffer for direct collectors that must not lose acknowledged payloads:

- `persist` writes one accepted delivery (its record batch plus a versioned
  manifest with the receipt, owner, table lease, and schema identity) to Quiver
  as bundle metadata. With a zero WAL flush interval it returns only after the
  WAL fsync, so an acknowledgement implies durable local storage.
- A consumer reads finalized bundles back from disk, admits them through
  `BufferManager::admit_tracked`, and acknowledges a bundle only after its
  receipt is published (or Delta committed). Terminal receipt failures drop the
  bundle; unknown outcomes defer it for a retry with backoff.
- After a crash, replayed bundles are reconciled against SQL: published
  receipts are acknowledged without republishing, failed ones are dropped, and
  active receipts of a predecessor (or of an older lease) are adopted onto the
  current owner and lease through `ReceiptStore::adopt_receipts` before they are
  published. Receipts adopted while `flushing` are published again, so a commit
  that landed just before the crash can appear twice (at least once).
- `check_admissible` rejects deliveries that could never fit the buffer before
  they are persisted. `drain` waits for a stable empty backlog; `snapshot`
  reports disk usage, backlog, and counters for pressure and metrics.

`admit_tracked` also lets any durable stage admit recovered deliveries whose
receipts are already `buffered` or `flushing` (`TrackedState`) and returns a
`DeliveryCompletion` that resolves to a `DeliveryOutcome`.

## Validation

```bash
npx --no-install nx run arrowcol-buffer:verify
```

Tests cover zero-copy array identity, capacity/backpressure, every flush
trigger, adaptive convergence, schema isolation, grouped receipt publication,
receipt and sink failures, stale fencing, cancellation, explicit drain,
shutdown reporting, publishing persisted batches from disk, crash replay with
receipt adoption, and acknowledging already-published replays without
republishing.
