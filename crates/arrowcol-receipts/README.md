# arrowcol-receipts

Reusable SQL Server durability for the direct Arrow collector. The crate uses
the shared `arrowcol-protocol` receipt, lease, error, identity, and publication
types and accepts receipt metadata only through
`arrowcol_catalog::ValidatedIncomingSchema`. The one exception is crash replay:
`ValidatedDelivery::recovered` rebuilds the same delivery from the persisted
getter values (`table_id`, `table`, `schema_version`, `schema_fingerprint`,
`rows`, `batches`, `arrow_memory_bytes`) of a batch that was validated before
it reached the collector's local durable buffer, applying the same zero-batch
and measurement-range rules as `ValidatedDelivery::new`.

## Worker API

`SqlServerReceiptStore` implements `ReceiptStore`:

1. `register_owner` and `heartbeat_owner` establish a process lifetime.
2. `acquire_table_lease` / `renew_table_lease` advance and maintain the
   per-table fencing epoch.
3. `create_flight_batch` transactionally creates fresh SQL Server-generated
   receipt IDs after schema validation. Identical metadata always creates
   independent receipts.
4. `mark_buffered` and `mark_flushing` use optimistic receipt revisions.
5. `publish_commit` atomically completes every receipt represented by one
   durable Delta commit.
6. `fail_batch` records a structured retryable or permanent protocol error.
7. `receipt` / `receipts` provide bounded status polling.
8. `reconcile_expired` turns expired accepted/buffered work into a retryable
   failure, but records expired flushing work as
   `publication_outcome_unknown`; it never invites blind Delta replay. It
   skips receipts protected by an owner's durable recovery window.
9. `start_owner_drain` / `finish_owner_drain` stop admission, account for
   outstanding work, release leases, and stop the owner.
10. `adopt_receipts` lets a restarted durable-buffer successor take over a
    crashed predecessor's active receipts, or rebind its own receipts to a
    newer table lease, without changing their state or revision.

SQL state `flushing` is exposed as the shared protocol's
`ReceiptState::Publishing`. Only `published` makes
`ReceiptRecord::can_client_purge` true.

`LeasePublicationFence` is the synchronous adapter used by
`arrow-deltalake` at the final commit boundary. Update it only after a
successful SQL lease renewal and invalidate it immediately after lease loss or
heartbeat failure.

## Durable recovery and adoption

An owner that persists accepted batches and their receipt identities on local
disk registers with `OwnerRegistration::durable_recovery_window_ms` set to
`Some(window)` (at most `MAX_DURABLE_RECOVERY_WINDOW_MS`, one day; SQL column
`arrowcol.collector_instances.durable_recovery_window_ms`). Both the selection
and the optimistic update in `reconcile_expired` then skip that owner's
accepted, buffered, and flushing receipts while its owner lease is unexpired
and until `window` milliseconds after the lease expires, even when receipt
deadlines have passed. Durable backlog can therefore wait locally beyond its
receipt deadline, and another collector's startup reconciliation cannot fail
receipts whose owner is alive or died within the window. Owners registered
with `None` keep receipt-deadline reconciliation, and receipts that nobody
adopts before the window elapses are reconciled exactly as before.

After a crash, the restarted process registers the same instance key with a
higher generation (which marks the expired predecessor `failed` and releases
its table leases), acquires a fresh table lease, and calls `adopt_receipts`
with a `ReceiptAdoption`:

- `successor`, its current table `lease`, the new receipt `lease_deadline`,
  and `adopted_at`; and
- one `AdoptionCandidate` per persisted receipt with the expected owner,
  state, and revision (`AdoptionCandidate::from_record` captures a current
  record).

One serializable transaction, bounded like every other mutation batch, adopts
a candidate only when:

- the receipt still has the expected owner, state, and revision and is
  accepted, buffered, or flushing;
- the successor is `active` with an unexpired owner lease, and the lease is
  the successor's unreleased, unexpired lease for the receipt's table;
- the new deadline is no later than either lease; and
- the expected owner is the successor itself (rebinding to a newer table
  lease) or a `failed` or stopped predecessor with the same instance key and a
  lower generation.

Adoption sets the owner, lease, fencing epoch, deadline, heartbeat, and update
time, keeps state and revision, and appends an `adopted` receipt event that
names the previous owner. `ReceiptAdoptionReport` returns the adopted records
and one `RejectedAdoption` per other candidate with its current record, or
`None` when the receipt does not exist. The caller can then drop work that was
published or reconciled meanwhile, retry with a refreshed expectation, or
continue adopted work through `mark_buffered`, `mark_flushing`, and
`publish_commit`. Adopting flushing work does not resolve its publication
uncertainty: determine whether the predecessor's Delta commit landed before
publishing it again.

`ReceiptAdoption::validate` rejects, before any SQL work, a lease that the
successor does not hold (`Fenced`), a deadline that does not follow
`adopted_at` or outlives the table lease, a duplicated candidate, and a
candidate that does not expect accepted/0, buffered/1, or publishing/2
(`InvalidConfiguration`). An inactive successor or lease fails the whole call
as `Fenced`.

## Persistence invariants

- SQL Server generates receipt IDs with `NEWSEQUENTIALID()`.
- Receipt events are append-only. Transition events are uniquely ordered by
  receipt revision; `adopted` events repeat the unchanged revision and record
  the previous owner.
- Every transition checks the expected owner, state, revision, table lease, and
  fencing epoch in one transaction.
- Published receipts store Delta version plus file, row, and byte
  observations.
- SQL stores counts, fingerprints, status, errors, and publication metadata;
  it never stores Arrow payload bytes.
- There is no idempotency key, payload hash lookup, or deduplication path.

## Validation

```bash
npx --no-install nx run arrowcol-receipts:verify
npx --no-install nx run arrowcol-receipts:live-integration
```

The live target starts `arrowcol-infra`, runs the ignored SQL tests against the
application principal, verifies transition and retry races, duplicate receipt
creation, expiry reconciliation, durable recovery windows, successor adoption
and its rejections, hot index hints, append-only events, and graceful drain,
then stops services without deleting named volumes.
