# Catalog and schema admission

[SDK pipeline diagram](diagrams/sdk-generation.html) | [Architecture index](README.md)

## Supported DDL

Flight SQL uses `CommandStatementUpdate` on `DoPut`, without ingest metadata.
The maintained parser accepts exactly these shapes:

```sql
CREATE TABLE telemetry.prod.events (
    id BIGINT NOT NULL,
    body VARCHAR NULL
) PARTITIONED BY (id)

ALTER TABLE telemetry.prod.events
ADD COLUMNS (source VARCHAR NULL)
```

Transactions, body record batches, drops, renames, replacement, CTAS, defaults,
constraints, storage clauses, type/nullability changes, reordering,
`FIRST`/`AFTER`, partition changes, and other statements are rejected.
The [catalog library](../../../crates/arrowcol-catalog/README.md) lists supported
SQL/Arrow types and closed extensions; unsupported/lossy mappings fail closed.

## Durable SQL/Hive saga

```text
Reserve operation + pending immutable version in SQL
  -> create/CAS Hive table through official Metastore API
  -> persist checked manifest / Arrow schema / protobuf descriptor chunks
  -> atomically activate the SQL catalog head
```

Startup reconciles incomplete operations. Canonical DDL hashes and deterministic
identities make saga retries idempotent; failed steps retain attempts and errors.
This catalog idempotency is **not** ingest deduplication.

Existing fields, positions, protobuf numbers, types, nullability, and partition
roles are immutable. Evolution permits only nullable, top-level, tail-appended
fields; positions and protobuf numbers are never reused.

## Schema admission

| Incoming schema | Result |
| --- | --- |
| Claim equals computed fingerprint; exact layout identifies a registered version | Admit after other identity/lease checks |
| Historical registered version; current head is nullable tail-append-only superset | Reuse old arrays; append all-null arrays for new fields |
| Unregistered prefix, reordered/changed field, mismatched claim, similar but unregistered layout | Reject before a direct receipt or DFE admission |

Historical SDKs remain valid only under that append-only rule. Clients are not
silently coerced across breaking layouts. SDK compatibility additionally checks
protobuf wire/JSON stability; see [generation](sdk-generation.md#evolution-and-limits).

Sources: [DDL parser](../../../crates/arrowcol-catalog/src/ddl.rs),
[saga](../../../crates/arrowcol-catalog/src/saga.rs),
[admission validation](../../../crates/arrowcol-catalog/src/validation.rs).
