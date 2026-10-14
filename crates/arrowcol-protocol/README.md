# arrowcol-protocol

Versioned, reusable contracts shared by Arrow collector routers, direct
collectors, and dataflow collectors. The crate is deliberately independent of
SQL, Hive, Pingora, Delta implementations, and the OTAP dataflow engine.

## Contract surface

- Portable validated `catalog.schema.table` identity and `direct` / `dataflow`
  implementation pools.
- Required bounded ASCII gRPC metadata:

  | Key | Value |
  | --- | --- |
  | `x-arrowcol-protocol-version` | canonical decimal protocol version (`1`) |
  | `x-arrowcol-implementation-pool` | `direct` or `dataflow` |
  | `x-arrowcol-catalog` | validated catalog name |
  | `x-arrowcol-schema` | validated schema name |
  | `x-arrowcol-table` | validated table name |
  | `x-arrowcol-schema-fingerprint` | `sha256:` plus 64 lowercase hex digits |

  Version/pool values are capped at 16 bytes, identity components at 128 bytes,
  and fingerprints at 71 bytes. Parsing rejects missing, malformed, oversized,
  control-bearing, non-ASCII, and unsupported-version values before routing.
- Canonical SHA-256 fingerprints for the Arrow 58.3 compatibility line. The
  canonical encoding includes field order/name/nullability, sorted field and
  schema metadata, every nested type, time units/time zones, decimal
  precision/scale, dictionary ordering and key/value types, list/map children,
  union IDs/mode, and run-end structure. Deprecated IPC dictionary IDs are
  intentionally excluded because they are transport-local rather than schema
  identity.
- Stable public error categories/codes and explicit retryability.
- Server-generated receipt IDs, receipt transitions, status payloads, and
  fenced Delta publication metadata.
- Capability, authenticated-principal, pressure, instance-lifecycle, and table
  lease/fencing documents with bounded measurements.

## Trust boundaries

`ClaimedSchemaFingerprint` is intentionally a distinct type. Metadata is only a
client claim: collectors must compute the received Arrow schema fingerprint and
compare it with authoritative catalog state. A matching metadata value is not
schema proof or authorization.

Every admitted delivery gets a fresh server-generated `ReceiptId`. Duplicate
payloads are valid new deliveries. There is no client idempotency key,
deduplication key, or exactly-once promise in this crate.

A `PublicationOutcomeUnknown` error is non-retryable until its existing receipt
is reconciled. Retrying other retryable failures creates a new delivery and a
new receipt.

## Receipt state machine

```text
accepted -> buffered -> publishing -> published
    |           |            |  \
    +-----------+----------> failed
                             |
                             +-> publication_unknown -> published | failed
```

Direct collectors may transition `accepted -> publishing` without buffering.
Terminal states have no outgoing transition.

## Wire compatibility

JSON uses an integer `protocol_version` and snake-case enum values. Committed V1
fixtures under `tests/fixtures/` are compatibility snapshots. Optional receipt
and error fields are omitted when absent. Deserialization revalidates bounds and
cross-field invariants rather than trusting JSON.

```bash
npx --no-install nx run arrowcol-protocol:verify
```
