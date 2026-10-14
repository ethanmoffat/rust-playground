# arrowcol-receipts

Reusable SQL Server durability for the direct Arrow collector. The crate uses
the shared `arrowcol-protocol` receipt, lease, error, identity, and publication
types and accepts receipt metadata only through
`arrowcol_catalog::ValidatedIncomingSchema`.

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
   `publication_outcome_unknown`; it never invites blind Delta replay.
9. `start_owner_drain` / `finish_owner_drain` stop admission, account for
   outstanding work, release leases, and stop the owner.

SQL state `flushing` is exposed as the shared protocol's
`ReceiptState::Publishing`. Only `published` makes
`ReceiptRecord::can_client_purge` true.

`LeasePublicationFence` is the synchronous adapter used by
`arrow-deltalake` at the final commit boundary. Update it only after a
successful SQL lease renewal and invalidate it immediately after lease loss or
heartbeat failure.

## Persistence invariants

- SQL Server generates receipt IDs with `NEWSEQUENTIALID()`.
- Receipt events are append-only and uniquely ordered by receipt revision.
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
creation, expiry reconciliation, hot index hints, append-only events, and
graceful drain, then stops services without deleting named volumes.
