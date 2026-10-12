# Official Hive Metastore Thrift surface

`hive_metastore_minimal.thrift` is a narrow, wire-compatible subset of Apache
Hive's authoritative `hive_metastore.thrift` at tag `rel/release-4.0.1`.

Only catalog/database creation and table create/get/alter RPCs required by the
Arrowcol schema saga are included. RPC names and field IDs are unchanged.
Apache Thrift's unknown-field behavior makes the subset interoperable while
avoiding an unreviewable generated client for thousands of unrelated methods.

Regenerate the checked-in Rust binding with Apache Thrift 0.19:

```bash
thrift --gen rs \
  -out crates/arrowcol-catalog/src/hive/generated \
  crates/arrowcol-catalog/src/hive/official/hive_metastore_minimal.thrift
```
