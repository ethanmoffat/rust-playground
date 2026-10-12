# arrowcol-catalog

Production catalog and schema-control-plane library for Arrow collectors. It is
transport-embeddable: collector binaries can share the same
`FlightSqlDdlHandler` without this crate owning a server socket.

## Contracts

- Exactly one maintained-parser DDL statement:
  - `CREATE TABLE catalog.schema.table (...) [PARTITIONED BY (...)]`
  - `ALTER TABLE catalog.schema.table ADD COLUMNS (...)`
- Existing fields are immutable. New fields are nullable, tail-appended, and
  receive never-reused positions and protobuf field numbers.
- Drops, renames, type/nullability changes, `FIRST`/`AFTER`, partition changes,
  replacement, CTAS, defaults, constraints, storage clauses, and every other
  statement are rejected with `CatalogErrorCode` plus an
  `arrowcol_protocol::ErrorCode`.
- Incoming Arrow schemas are accepted only when their computed fingerprint and
  claim match one exact registered version. An older registered version receives
  an explicit source-copy/null-fill plan only when the current head is a
  nullable append-only superset.

The exact historical-version rule is what keeps generated clients compatible:
an SDK generated for version `N` can continue sending its exact schema after
nullable tail-appended version `N+1`; the collector adds only all-null arrays
for the new fields. An unregistered prefix, reordered field, changed physical
type, nullability change, or fingerprint mismatch is rejected before collector
admission.

The SQL type surface follows DataFusion 54's Arrow mappings for booleans,
signed/unsigned integers, real/double, strings, `BYTEA`, date, time,
`TIMESTAMP(0|3|6|9)`, explicit `DECIMAL(p,s)`, `ARRAY`, and `STRUCT`. `MAP`
uses the parser's `MAP(key, value)` spelling. Closed Arrow extensions required
by collector schemas include `FIXED_SIZE_BINARY(n)`, `LARGE_STRING`,
`STRING_VIEW`, `LARGE_BINARY`, `BINARY_VIEW`, `DATE64`, and explicit
time/duration unit names. Unsupported or lossy mappings fail closed.

## Durable saga

`CatalogControlPlane::start` reconciles incomplete operations before serving:

1. reserve the app change and pending immutable version under a SQL application
   lock;
2. create/validate the Hive catalog and database, then create or compare-and-set
   alter the real table through Hive 4.0.1 Thrift;
3. persist deterministic Arrow IPC, protobuf descriptor-set, and JSON manifest
   artifacts in checked 8,000-byte chunks;
4. atomically activate the new application schema head.

Retries use a SHA-256 hash of canonical DDL and deterministic table, field, and
saga UUIDs. Every step is idempotent. Failures remain in
`schema_change_sagas` with their step, attempt count, error code, and message.

The Hive adapter is generated from a reviewed narrow subset of Apache Hive
4.0.1's official Thrift IDL. It never mutates Hive vendor tables.

## Embedding

```rust
use std::sync::Arc;
use arrowcol_catalog::{
    CatalogControlPlane, DdlParser, FlightSqlDdlHandler, FlightSqlDdlRequest,
    HiveMetastore, HiveMetastoreConfig, SqlServerCatalogStore, SqlServerConfig,
};

# async fn example() -> anyhow::Result<()> {
let sql = SqlServerCatalogStore::new(
    SqlServerConfig::new("sql.example", 1433, "arrowcol", "arrowcol_app", "secret")
);
let hive = HiveMetastore::new(HiveMetastoreConfig::new("hive.example", 9083));
let (catalog, startup) = CatalogControlPlane::start(
    DdlParser::default(),
    Arc::new(sql),
    Arc::new(hive),
).await?;
assert!(startup.failures.is_empty());

let completion = catalog.execute_ddl(FlightSqlDdlRequest::new(
    "CREATE TABLE telemetry.prod.events (id BIGINT NOT NULL, body VARCHAR)"
)).await?;
assert_eq!(completion.schema_version().get(), 1);
# Ok(())
# }
```

Use a secret file or platform secret manager in production rather than the
literal password shown in the compact API example. See `config.example.toml`.

## Validation

```bash
npx --no-install nx run arrowcol-catalog:verify
npx --no-install nx run arrowcol-catalog:live-integration
```

The live target starts `arrowcol-infra`, exercises SQL Server and the official
Hive Metastore API, and always runs `down` afterward. Named durable volumes are
preserved.
