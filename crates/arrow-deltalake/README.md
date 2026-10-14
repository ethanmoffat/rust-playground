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

## Completion accounting and adaptive rotation

Every successful batch write is checked into per-file counters before it is
reported as accepted. `CompletedFile::observation()` returns a path-independent
`{ rows, input_bytes, parquet_bytes }` value; `StagedAppend::observation()` makes
the same counters available before Delta publication. Input bytes describe the
caller's logical Arrow slices (including nested validity and values), not
allocator capacity or Delta's normalized physical columns. Shared buffers count
once per logical reference, so the value remains useful as the denominator for
Parquet compression and file-size control.

`AdaptiveFileSizeEstimator` learns only from completed files. It owns no
`RecordBatch` and returns a bounded logical-input threshold for the caller's
next file:

```rust
use arrow_deltalake::sizing::{
    AdaptiveFileSizeConfig, AdaptiveFileSizeEstimator,
};

let mut sizing = AdaptiveFileSizeEstimator::new(
    AdaptiveFileSizeConfig::default(), // 128 MiB compressed target
)?;
let completed = writer.finish().await?;
let next_input_target = sizing.observe(completed.observation())?;
```

The configuration includes minimum/maximum input clamps, a finite cold-start
Parquet/input ratio, conservative EWMA weight, and ratio bounds. Zero counters,
NaN/infinite settings, inverted bounds, and unsafe divisions are rejected.
Callers can use `record_batch_logical_bytes`, or the streaming writers'
`input_bytes_written()`, to compare the current file with the returned target.

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

## Publication fencing

`PublicationFence` is a synchronous, storage-agnostic hook for caller-owned
lease or generation state. Open with `DeltaTable::open_with_fence` or
`BatchedTable::open_with_fence`; after the append transaction and file actions
are prepared, the hook runs under the local commit lock immediately before the
versioned Delta log commit. The crate has no SQL/client dependency and performs
no fence retry.

A rejected batched transaction is never made visible. Every receipt in that
transaction receives `CommitReceiptError::PublicationFenced` through
`wait_typed()`; the coordinator is then poisoned just like other uncertain or
commit failures. Existing `wait()` remains available and wraps the typed error
in `anyhow::Error`.

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
