# Buffers, files, and Delta

[Architecture index](README.md) | [Delivery boundary](delivery-guarantees.md)

## Direct controls

Checked-in host values in [direct config](../../../projects/arrowcol-rust/config.yaml):

| Control | Value | Bound |
| --- | ---: | --- |
| `buffer.global_capacity.bytes` | 2 GiB | Retained and in-flight process Arrow bytes |
| `buffer.per_table_capacity.bytes` | 512 MiB | Retained and in-flight table Arrow bytes |
| `buffer.max_arrow_memory_per_flush` | 512 MiB | Logical Arrow rotation |
| `buffer.max_rows_per_flush` | 1,000,000 | Row rotation |
| `buffer.max_batches_per_flush` | 512 | Batch rotation |
| `buffer.max_age_ms` | 30,000 | Oldest-batch flush trigger |
| `buffer.max_concurrent_flushes` | 4 | Independent flushes |
| `buffer.adaptive_file.target_parquet_bytes` | 128 MiB | Compressed Parquet target |
| `buffer.adaptive_file.min_input_bytes` | 16 MiB | Lower input estimate clamp |
| `buffer.adaptive_file.max_input_bytes` | 1 GiB | Upper input estimate clamp |
| `delta.commit_batch_window_ms` | 10,000 | File collection window |
| `delta.max_files_per_commit` | 64 | File actions per commit |
| `delta.max_pending_files` | 256 | Queued and committing files |

The loadtest direct template keeps the 128 MiB compressed target, lowers the
cold-start minimum input clamp to 1 MiB, and shortens age/commit windows for
finite tests. These are configuration bounds, not unbounded defaults.

[DFE config](../../../projects/arrowcol-dataflow-rust/config.yaml) separately
bounds Flight decoding, pdata channels, buffered capacity, authority timeout,
and pending Delta files. Healthy queues do not turn admission into durability.

## Separate roots

| Context | Direct | DFE |
| --- | --- | --- |
| Host config | `projects/arrowcol-rust/.data/delta` | `ARROWCOL_DELTA_ROOT`, default `/onelake` |
| Direct container config | `/var/lib/arrowcol/delta` | Not applicable |
| Benchmark host | `onelake/benchmark/<run-id>/direct` | `onelake/benchmark/<run-id>/dataflow` |
| Benchmark container | `/delta/direct` | `/delta/dataflow` |

## Persistence limits

SQL stores receipt/control metadata, fixed-size hashes, bounded counts, errors,
and publication observations, **not pending ingest payloads**. By default,
collector loss loses in-memory Arrow arrays.

Native Quiver is optional local-disk payload persistence. It is enabled with
`buffer.native_quiver` for Direct, and with `native_quiver: true` plus
`processor:durable_buffer` for DFE. Each Quiver bundle carries bounded metadata,
so it preserves the table, schema, fingerprint, and lease identity of
arbitrary Arrow. Pending batches then survive a process crash, and the restarted
collector replays them. See
[delivery guarantees](delivery-guarantees.md#native-quiver-disk-persistence).

| Quiver directory | Direct | DFE |
| --- | --- | --- |
| Config key | `buffer.native_quiver.directory` | `processor:durable_buffer` `path` (per-core `core_N/`) |
| Benchmark host | `onelake/benchmark/latest/direct-state-1/quiver` | `onelake/benchmark/latest/dataflow-state-1/quiver` |
| Benchmark container | `/state/quiver` | `/quiver` |

A Quiver directory must outlive the process and belong to one
`instance_key`. Disk usage is capped by the configured capacity, and a full
budget backpressures ingest.

Delta is append-only: no upsert, overwrite, compaction, cross-table transaction,
or exactly-once guarantee. The single-host benchmark uses shared bind mounts;
it checks routing, process failures, local filesystem fencing, and independent
snapshot reads, not object-store/distributed-filesystem commit guarantees.

Sources: [buffer manager](../../../crates/arrowcol-buffer/src/manager.rs),
[direct sink](../../../projects/arrowcol-rust/src/delta_sink.rs),
[DFE exporter](../../../crates/arrowcol-dataflow-components/src/exporter.rs).
