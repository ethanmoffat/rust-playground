# arrow-deltalake

Streaming Arrow `RecordBatch` -> Parquet / Delta. Local Linux, append-only.
No OTEL or Flight dependency. Writes use pinned Delta Kernel 0.29.0;
delta-rs is only the independent loadtest reader.

## Data path

```text
                         Arrow RecordBatch
                                |
                  +-------------+--------------+
                  |                            |
             PARQUET ONLY                    DELTA
                  |                            |
         +--------v----------+        +--------v----------+
         | ParquetFileWriter |        | normalize + align |
         | input schema kept |        | canonical schema  |
         +--------+----------+        +--------+----------+
                  |                            |
             finish().await           DeltaFileWriter
                  |                            |
          closed + synced                 stage().await
                  |                            |
             .parquet file             private .parquet
                                               |
                                      submit -> Kernel commit
                                               |
                                          Delta snapshot
```

Parquet-only: `default-features = false`. Batches stream into files, not a
whole-table buffer.

```rust
use arrow_deltalake::delta::DeltaTableWriter;

async fn append(batch: &arrow::array::RecordBatch) -> anyhow::Result<()> {
    let mut writer = DeltaTableWriter::open("/tmp/my-table")?;
    writer.write(batch).await?;
    writer.close().await?;
    Ok(())
}
```

Standalone `flush` / `close` commits immediately; no collection window.

## Cross-core batching

```text
  Core A: write -> stage F1 --+
  Core B: write -> stage F2 --+   concurrent Parquet finalization
  Core C: write -> stage F3 --+
                             |
                             v
           +-----------------------------------------+
           | BatchedTable: one owner per table path  |
           | 256 queued + committing files maximum   |
           | full queue -> async backpressure        |
           +--------------------+--------------------+
                                |
                  64 eligible files OR 10 seconds
                  from the oldest file OR forced flush
                                |
                                v
           +-----------------------------------------+
           | Per-table worker: serial Kernel commits |
           | CREATE / ALTER / INSERT metadata + log  |
           | one INSERT version -> many file adds    |
           +--------------------+--------------------+
                                |
                 sync log + directories -> receipts
                                |
               F1, F2, F3 share the committed version

  Table X: [queue X] -> [worker X] -> _delta_log X
  Table Y: [queue Y] -> [worker Y] -> _delta_log Y
             independent workers, no global commit lock
```

`new_file(...).await` keeps synchronous Kernel work off collector cores.
Shared handles require identical commit options. One collector process owns
the filesystem lock; the last clients close and submitted work drains before
ownership is released.

## Visibility and completion

```text
  write -> stage -> submit -> commit + sync -> receipt.wait()
           private  queued     Delta-visible     SUCCESS

  stage != commit       queue admission != durable delivery
  drop before submit -> no commit
  drop after submit  -> may still commit; cancellation is not rollback
  commit error       -> fail waiters + poison coordinator; no blind retry

  OTEL adapter's dependency gate (not a cross-table transaction):

  child:  stage -> submit -> commit -> receipt
                                         |
  parent: stage ------------ wait --------+-> submit -> commit

  flush_pending()  -> publish partial admitted batches now
  start_draining() -> bypass delay for every remaining dependency wave
```

Data files are synced before publication. Conflicts and uncertain outcomes
are explicit errors; orphan files remain for inspection. The 10-second
collection window is not a visibility bound: dependency waves add latency.

## Arrow representation

```text
  Arrow input              Delta representation
  -----------------------  -------------------------------------------
  UInt8 / UInt16 / UInt32   Int16 / Int32 / Int64
  UInt64                   Decimal128(20,0)
  fixed-width binary       binary, every byte preserved
  dictionary               decoded values; key width is irrelevant
  timestamp                UTC microseconds + <field>__unix_nanos
  duration                 Int64 count + original-unit metadata
  struct / list / map      normalized children; null masks preserved

  timestamp -1 ns -> timestamp -1 us + companion -1 ns
                       floor rounding       exact value

  SCHEMA
    new nullable top-level column -> ALTER -> append
    absent nullable column        -> null-filled
    nested/type/logical changes   -> ERROR
    missing required/new required -> ERROR
```

Timestamp scaling overflow and reserved companion names fail. Bare timestamp
list/map elements need a named struct field. Container logical types live on
the enclosing field because Delta has no element/key/value field metadata.
Schema evolution is separate: a failed append may leave a new nullable column.

## Local table layout

```text
  /tmp/my-table/
  +-- .arrow-lake-writer.lock        exclusive writer ownership
  +-- part-<uuid>.parquet            staged or committed data
  +-- _delta_log/
      +-- 00000000000000000000.json  table metadata
      +-- 00000000000000000001.json  add(F1), add(F2), ...
      +-- ...                       snapshot-selected files only
```

The legacy lock filename and `arrow_lake.original_type` metadata key stay
stable across the crate rename. Corrupt logs/raw Parquet directories are not
adopted as missing tables. `open_with_store` supports local-URL fault injection,
not cloud commit semantics.

No upsert/overwrite, compaction, catalog/cloud authentication, cross-table
atomicity, or exactly-once guarantee.

```bash
npx --no-install nx run arrow-deltalake:verify
```
