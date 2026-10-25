# Delta commit benchmark report

Status: complete for the design questions in scope. The findings log at the end records every result in
the order it was found, including bench defects and their fixes.

## Design summary

| Question | Answer | Evidence |
|---|---|---|
| One commit loop per table (option C) | Supported. One loop sustains about 12.5 commits/s on flat and 10 on HNS, and group commit absorbs 160 times the hot-table file rate. A second writer adds contention with no throughput gain. | Exp 1, exp 5 |
| Guarantees G1 to G4 | Hold in every cell on both accounts, including faults, zombies and identity changes, with `txn` idempotency, resolve-before-recommit, the fence and adoption. | Exp 1, 2, 3, 5, 7 |
| Ownership moves | The fence is required. It is a Delta commit and needs no separate service. Ack/nack covers graceful moves only. | Exp 7, "Ownership moves" |
| Many tables in one process | Works only with a process-wide cap on concurrent Delta operations. Without it, delta-rs deadlocks the tokio blocking pool at the first checkpoint round (F14). A cap of 32 to 128 removes the deadlock with no commit p50 cost. | Exp 2, F14 |
| Checkpoint timing | Stagger checkpoints across tables. Aligned checkpoints push visibility p99 to about 23 s at 1000 tables; staggered, 0.6 s on HNS. | Exp 2 F14 follow-up |
| Ingest throughput per VM | Encode CPU is the limit. About 330 to 360 MB/s of raw input per physical core, 210 to 260 MB/s per vCPU past the physical core count. The 6 cores per GB/s planning number holds. NIC and commit loop are far from their limits. | Exp 3 |
| Flat vs HNS | Same guarantees and ingest throughput. Flat commits about 35 to 45% faster at p50. | Exp 1, 2, 3 |
| Per-table WAL | Costs disk syncs on every commit and must be readable by a new owner. It needs a low-latency device and shared placement or another recovery path. | F13, risks |
| Table growth | Writer memory and commit latency grow with active file count. Compaction must bound it. | F12 |

## Setup

| Item | Value |
|---|---|
| Region | westus2. The plan was westus3. The subscription policy "Deny global resources in unsupported locations" blocks storage accounts there. |
| Storage, HNS | `emdcbhnsf35219`: StorageV2, Standard_LRS, Hot, hierarchical namespace on |
| Storage, flat | `emdcbflatf35219`: StorageV2, Standard_LRS, Hot, hierarchical namespace off |
| Storage security | Shared key off. Anonymous access off. Firewall default Deny with a VNet service endpoint rule. Network security perimeter in Enforced mode with one inbound rule for the subscription. |
| Auth | User-assigned managed identity with Storage Blob Data Contributor |
| VMs | Flexible VMSS, 3 x Standard_D16s_v5, plus 1 x Standard_D32ds_v5 and 1 x Standard_D64ds_v5 for exp 3 (`infra.sh addvmss`). Ubuntu 22.04 gen2, Trusted Launch, accelerated networking, no public IP. Access through Azure Bastion. Egress through a NAT gateway. |
| Crates | deltalake-core 1.1.1, object_store 0.13.2, arrow and parquet 59.3.0, buoyant_kernel 0.28.1, delta_kernel 0.29.0, tokio 1.53.2 |
| Toolchain | rustc 1.96.1, release build |
| Table properties | delta-rs defaults. No properties set unless a run lists `--prop`. |
| Checkpoints | Separate from the commit, every 100 versions, unless a run says otherwise |
| Commit runtime | Multi-thread tokio runtime with `--commit-threads` workers (default 1) |
| Client | object_store default `RetryConfig`, 30 s request timeout |
| Infra script | `scripts/infra.sh up` (policy compliant). `scripts/infra.sh down` deletes everything. |

## How delta-rs writes the log

- Log commits use `put_opts(PutMode::Create)`. On Azure this sends `If-None-Match: *`. A 409 maps to
  `VersionAlreadyExists`. This holds for both account types; both use the Blob endpoint.
- On `VersionAlreadyExists` the library reloads, runs the conflict checker, and retries at the next
  version, up to 15 times by default.
- The conflict checker raises `ConcurrentTransaction` when a winning commit carries a `txn` with the same
  appId as the losing commit. Blind appends with different appIds never conflict.
- object_store retries a 5xx even on a create-only PUT. If the first PUT landed, the retry receives a 409
  and delta-rs recommits the same actions at the next version. Without a `txn` this duplicates data. With
  a `txn` the conflict checker stops it.

## The commit loop under test

The loop follows "The working commit design to test" with these changes. Each change came from a failed
guarantee or a measured cost.

| Change | Reason |
|---|---|
| Resolve before recommit | After any commit error the loop refreshes the snapshot and checks whether the batch landed. It recommits only on a definite "no". If the check itself fails, the manifest stays pending and the loop stops. A blind recommit after a failed check produced duplicates. |
| Fence on ownership change | The new owner commits a `txn`-only entry for the old appId with version `2^62 + last`. The old owner's next commit fails with `ConcurrentTransaction`. A fence already inside its snapshot is caught by the pre-commit check. |
| Adopt the old WAL | The new owner fences, then recommits the old owner's pending manifests above the fenced version under its own appId. |
| Skip the pre-commit check when verified | The check runs only when the snapshot contains commits this loop has not screened. See finding F5. |
| Multi-thread commit runtime | See finding F6. |
| Fresh snapshot load after each checkpoint | See finding F9. `--reload-after-checkpoint false` reproduces the old behavior. |

## Ownership moves: graceful and failure

There are two kinds of ownership move. Each needs a different mechanism.

**Graceful move.** The old owner X is alive and cooperative, for example during a rebalance or a rolling
deploy. A DFE ack/nack handshake is enough:

1. Routing marks the table as moving. New files for the table go to the new owner Y.
2. X stops taking files, commits its pending batches, and acks "drained".
3. Y waits for the ack, then starts committing. X's WAL holds nothing, so Y has nothing to adopt.

The fence still runs here. It costs one commit and makes the move safe even if step 2 is wrong.

**Failure move.** X is paused (GC, VM freeze), partitioned from routing, or crashed. Ack/nack can't help:

| Problem | Why ack/nack fails | What the fence does |
|---|---|---|
| X never sees the nack | X is partitioned from DFE and still reaches storage | Storage rejects X's next commit |
| X has a commit PUT in flight | A nack can't recall a request already sent | The PUT either lands before the fence, and Y's adopt check finds it, or it loses the version race |
| X resumes after a pause | X thinks it still owns the table | X's next commit fails with ConcurrentTransaction |
| Y must wait for X | A timeout guesses that X is dead | Y needs no wait. The fence makes X dead for this table. |

The fence turns "the old owner must be dead" from a timeout guess into a storage-enforced fact. Any
commit X makes after the fence version fails.

**What causes harm.** Two writers with different appIds and disjoint files are safe. They only cause
library retries (exp 5). Duplicates need Y to adopt X's pending files while X still commits them. The
fence prevents this. Losses need nobody to adopt X's pending files. Adoption prevents this, but only if
Y can read X's WAL (see WAL locality under risks).

**Core moves inside one pod** never create a second writer. All files for a table fan in to its one
commit loop, regardless of which core encoded them.

## Hypotheses

| Hypothesis | Verdict | Evidence |
|---|---|---|
| No fencing service | Refuted as stated. A fence is required. It needs no separate service: the fence is a Delta commit. | Exp 7 zombie-fence and identity-change, local and on both Azure accounts. |
| Concurrency safety | Holds for blind appends with distinct appIds. Contention costs latency and fairness. | Exp 5 (2 and 5 writers, both accounts). Exp 7 concurrent-optimize. |
| One process commits for many tables | Holds with a process-wide cap on concurrent Delta operations and staggered checkpoints. Fails without the cap (F14). | Exp 2 and the F14 follow-up. |
| Encode throughput meets 6 cores per GB/s | Holds on synthetic data. | Exp 3 on 16, 32 and 64 vCPU. |

## Experiment 7: failure injection (local, file://)

Rate 10 files/s, 12 s per scenario. Raw results: `results/exp7-local/<scenario>/`.
Repro: `scripts/faults.sh file:///tmp/f/t /tmp/f/w 10 12`.

| Scenario | G1 | G2 | G3 | G4 | Notes |
|---|---|---|---|---|---|
| crash-after-log-txn | pass | pass | pass | pass | Replay found the batch landed |
| crash-after-log-scan-log | pass | pass | pass | pass | |
| ambiguous-timeout-txn | pass | pass | pass | pass | 17 landed after error |
| ambiguous-conflict-txn | pass | pass | pass | pass | 23 landed after error |
| ambiguous-timeout-scan-log | pass | pass | pass | pass | |
| ambiguous-conflict-scan-log | pass | **fail** | pass | pass | 30 duplicates with default library retries. 0 with `--library-max-retries 0`. |
| errors-503-log | pass | pass | pass | pass | 277 injected 503s |
| throttle-200ms-log | pass | pass | pass | pass | |
| identity-change-no-adopt-crash-before | **fail** | pass | pass | pass | 1 batch lost. Expected: nobody replays the old WAL. |
| identity-change-adopt-crash-before | pass | pass | pass | pass | |
| identity-change-no-adopt-crash-after | pass | pass | pass | pass | |
| identity-change-adopt-crash-after | pass | pass | pass | pass | |
| crash-before-log-txn | pass | pass | pass | pass | |
| zombie-fence | pass | pass | pass | pass | Zombie fenced by ConcurrentTransaction. No zombie commits after the fence. 8 orphan files (uploaded, never committed). |
| crash-mid-upload | pass | pass | pass | pass | |
| concurrent-optimize | pass | pass | pass | pass | 1 library retry |

## Experiment 7 on Azure: failure injection

The same suite ran against both accounts from VM3. Raw results: `results/faults-azure/`.
Repro: `scripts/faults.sh abfss://bench@<acct>.dfs.core.windows.net/faults/t ~/results/faults/<acct> 10 12`.

The verdicts match the local run on both accounts. Every scenario passes G1 to G4 except the two
expected failures:

| Scenario | Flat | HNS |
|---|---|---|
| ambiguous-conflict-scan-log | G2 fails, 28 duplicates | G2 fails, 27 duplicates |
| identity-change-no-adopt-crash-before | G1 fails, 2 lost | G1 fails, 1 lost |
| zombie-fence | pass. Fence at version 9. 0 zombie commits after it. | pass. Fence at version 9. 0 zombie commits after it. |
| errors-503-log | pass. 255 injected 503s. | pass. 191 injected 503s. |

On real storage, the fenced zombie's next commit failed with `ConcurrentTransaction` and its loop
stopped. The checker found no inst-a commit above the fence version.

## Experiment 5: multiple writers on one table

Reduced to 2 and 5 writers, for evidence only. Each writer is a separate process with its own appId and
disjoint files. Rate is per writer, 4 encoders each, group pacing, 180 s. Raw results:
`results/exp5/<account>/<cell>/`. Repro: `scripts/exp5.sh <account>`.

| Account | Cell | Offered files/s | G1 to G4 | Commits | p50 / p99 commit ms | Library retries per commit | Per-writer commits/s | Worst visibility ms |
|---|---|---|---|---|---|---|---|---|
| Flat | 2 x 1.2 | 2.4 | pass | 433 | 61 / 167 | 0.12 | 1.22, 1.22 | 1161 |
| Flat | 2 x 12 | 24 | pass | 2420 | 42 / 1012 | 0.37 | 2.36, 11.08 | 11316 |
| Flat | 5 x 1.2 | 6 | pass | 1081 | 117 / 417 | 0.37 | 1.20 to 1.21 | 762 |
| Flat | 5 x 12 | 60 | pass | 3005 | 66 / 6011 | 1.27 | 1.88 to 5.30 | 16560 |
| HNS | 2 x 1.2 | 2.4 | pass | 426 | 80 / 265 | 0.00 | 1.19, 1.20 | 1260 |
| HNS | 2 x 12 | 24 | pass | 1610 | 81 / 1434 | 0.39 | 6.47, 2.47 | 22823 |
| HNS | 5 x 1.2 | 6 | pass | 1054 | 194 / 685 | 0.64 | 1.07 to 1.21 | 1325 |
| HNS | 5 x 12 | 60 | pass | 1995 | 75 / 9960 | 0.75 | 0.98 to 3.66 | 65257 |

- Correctness holds. Concurrent blind appends with distinct appIds never lost or duplicated a file.
- At low rates every writer keeps up, but p50 commit latency doubles at 5 writers.
- At high rates the writers starve each other. One writer can win most races while another falls
  behind. p99 commit latency reaches 6 to 16 s and visibility reaches 11 to 65 s.
- Total commits/s at 5 writers (17 flat, 11 HNS) is no better than one loop (exp 1). Adding writers
  adds contention and no throughput.

This supports option C. A table should have one writer.

## Experiment 1: commit loop characterization

One table, one commit loop, D16s_v5 in westus2. Each cell runs 180 s (the 1-file ceiling cell runs 60 s
and then drains its backlog). Log cleanup off, fresh snapshot load after each checkpoint, checkpoint every
100 versions. WAL on the VM's Premium SSD OS disk. Raw results: `results/exp1-v3/<account>/<cell>/`.
Repro: `scripts/exp1.sh <account>`, then `scripts/summarize.py results/exp1-v3/<account> --csv`.
Earlier runs measured bench or library artifacts (F8 to F11) and are superseded.

Every cell passes G1 to G4 on both accounts. No library retries in any cell.

**Pacing.** Rate is files/s offered. Visibility is the time from upload done to commit visible.

| Pacing | Rate | Flat commits/s | Flat commit p50 / p99 ms | Flat visibility p50 / p99 ms | HNS commits/s | HNS commit p50 / p99 ms | HNS visibility p50 / p99 ms |
|---|---|---|---|---|---|---|---|
| group | 1.2 | 1.18 | 45 / 90 | 58 / 108 | 1.17 | 66 / 108 | 81 / 196 |
| group | 3 | 2.94 | 47 / 94 | 61 / 132 | 2.98 | 67 / 113 | 75 / 120 |
| group | 6 | 5.97 | 47 / 98 | 69 / 177 | 5.83 | 68 / 130 | 87 / 333 |
| group | 12 | 10.94 | 47 / 105 | 93 / 276 | 7.53 | 71 / 203 | 120 / 1175 |
| 1 s | 1.2 to 12 | 0.78 to 0.99 | 41 to 45 / 85 to 89 | 603 to 992 / about 1040 | 0.49 to 0.99 | 67 to 74 / 117 to 194 | 790 to 1040 / 904 to 1144 |
| 5 s | 1.2 to 12 | 0.19 | 34 to 41 / 80 to 87 | about 5000 / about 5060 | 0.19 | 62 to 70 / 98 to 162 | 4633 to 5001 / 4677 to 5111 |

- Group commit gives the lowest visibility at every rate. It commits each file as soon as the loop is
  free and batches whatever arrived during the last commit.
- A timer adds its period to visibility and saves nothing the design needs. Commit cost does not
  depend on files per commit.
- At the hot-table rate (1.2 files/s) group commit makes about one commit per file. That is 1.2
  commits/s, or about 10% of the loop's ceiling.

**Ceiling.** 16 encoders, group pacing unless noted.

| Cell | Flat commits/s | Flat files/commit | Flat visibility p50 / p99 ms | HNS commits/s | HNS files/commit | HNS visibility p50 / p99 ms |
|---|---|---|---|---|---|---|
| r25 | 12.42 | 2.0 | 100 / 243 | 10.49 | 2.4 | 126 / 368 |
| r50 | 12.76 | 3.9 | 115 / 405 | 9.86 | 5.1 | 150 / 927 |
| r100 | 12.90 | 7.8 | 122 / 332 | 10.28 | 9.6 | 159 / 708 |
| r200 | 12.06 | 16.6 | 135 / 308 | 9.21 | 21.3 | 174 / 1136 |
| r100, 1 file per commit | 12.69 | 1.0 | 200077 / 408320 | 7.75 | 1.0 | 275817 / 708661 |

- One loop commits about 12.5 times a second on flat and about 10 on HNS. The cap is commit latency:
  2 log GETs, 2 log LISTs and 1 conditional PUT per commit.
- Group commit absorbs any file rate by batching. At 200 files/s, visibility p99 stays under 0.4 s
  on flat and 1.2 s on HNS. That is about 160 times the hot-table assumption.
- With one file per commit, the loop falls behind above about 12 files/s and builds a backlog of minutes.
- Checkpoints take 200 to 460 ms (p50) and run every 100 versions.
- A fresh reader loads the r200 table (36k files, v2172) in 270 to 600 ms with 132 to 156 log GETs.

## Experiment 2: many tables in one process

Each table gets one encoder at 0.2 files/s and its own commit loop. All loops share one commit runtime
with 1 worker thread. Checkpoint every 50 versions. The WAL is on tmpfs (`/dev/shm`) so WAL fsyncs (F13)
stay out of the measurement. Flat on VM4 (D32ds_v5), HNS on VM3 (D16s_v5).

**Baseline, 300 s per cell.** Summaries: `results/exp2-shm/<account>.csv`.

| Tables | Account | Commits/s | Commit p50 / p99 ms | Visibility p50 / p99 ms | Checkpoint p50 ms |
|---|---|---|---|---|---|
| 1 | Flat | 0.2 | 36 / 79 | 36 / 79 | 326 |
| 1 | HNS | 0.2 | 67 / 130 | 67 / 130 | 1776 (one sample) |
| 100 | Flat | 19.7 | 35 / 87 | 35 / 89 | 170 |
| 100 | HNS | 19.7 | 64 / 130 | 64 / 130 | 199 |
| 1000 | Both | Deadlocked at version 49 on every table (F14) | Flat 45 / 546, HNS 84 / 926 before the deadlock | | |

- Up to 100 tables, per-table commit latency matches a single table. Tables do not slow each other down.
- At 1000 tables the process deadlocked at the first checkpoint round. See F14 for the mechanism.

### F14 follow-up: caps and checkpoint placement

Each cell runs 600 s, so every table passes two checkpoint versions (50 and 100). A watchdog ends a
cell after 300 s without a new commit and saves a gdb dump. Variants:

| Variant | Checkpoint runs on | Commit cap | Checkpoint cap | Starts |
|---|---|---|---|---|
| base | async worker (`create_checkpoint`) | none | none | aligned |
| blk | `spawn_blocking` | none | none | aligned |
| cap32 / cap64 / cap128 | async worker | 32 / 64 / 128 | 8 | aligned |
| cap64blk | `spawn_blocking` | 64 | 8 | aligned |
| cap64blkj | `spawn_blocking` | 64 | 8 | spread over one checkpoint period (250 s) |

The commit cap covers table open and each commit. The checkpoint cap covers each checkpoint and the
snapshot reload after it.

**1000 tables (200 files/s offered).** Every completed cell committed all 120000 files and passes G1 to G4.
Summary: `results/exp2-f14/summary.csv`. Script: `results/exp2-f14/f14sum.py`.

| Variant | Account | Commits/s | Commit p50 / p99 ms | Visibility p50 / p99 ms | Checkpoint p50 / p99 ms | Checkpoint permit wait p99 ms | Commit permit wait p99 ms | Max commit-runtime threads |
|---|---|---|---|---|---|---|---|---|
| base | HNS | deadlock at v49 | | | | | | 514 |
| base | Flat | deadlock at v49 | | | | | | 514 |
| blk | HNS | 158 | 83 / 19793 | 83 / 58237 | 27240 / 46562 | | | 514 |
| blk | Flat | 158 | 47 / 14436 | 48 / 60771 | 29683 / 50209 | | | 514 |
| cap32 | HNS | 187 | 78 / 257 | 80 / 22547 | 313 / 974 | 44847 | 4145 | 137 |
| cap32 | Flat | 187 | 49 / 263 | 50 / 22845 | 309 / 1019 | 44403 | 4623 | 298 |
| cap64 | HNS | 187 | 80 / 421 | 81 / 23313 | 300 / 1534 | 47638 | 972 | 156 |
| cap64 | Flat | 187 | 46 / 443 | 47 / 22619 | 298 / 1628 | 47597 | 1809 | 149 |
| cap128 | HNS | 186 | 83 / 669 | 85 / 23495 | 310 / 2195 | 48940 | 140 | 259 |
| cap128 | Flat | 186 | 51 / 806 | 52 / 25356 | 285 / 2849 | 50064 | 974 | 225 |
| cap64blk | HNS | 184 | 92 / 449 | 95 / 27662 | 375 / 1587 | 58442 | 1336 | 195 |
| cap64blk | Flat | 174 | 61 / 561 | 62 / 41342 | 380 / 2080 | 65654 | 5002 | 169 |
| cap64blkj | HNS | 142 | 82 / 251 | 83 / 555 | 325 / 994 | 0 | 207 | 130 |
| cap64blkj | Flat | 141 | 98 / 508 | 100 / 9369 | 768 / 1912 | 9091 | 5003 | 127 |

Commits/s in the staggered cells is lower because the same files spread over a longer run.
Thread counts include idle pool threads, which tokio keeps for 10 s.

- **The deadlock reproduces on both accounts** in under 5 minutes, at version 49, with 514 threads
  (the 513-thread pool limit plus the runtime's driver thread).
- **A cap alone prevents it.** Every cap size completed, held the thread count far below the limit,
  and kept commit p50 at the uncapped level (46 to 51 ms flat, 78 to 83 ms HNS). Smaller caps lower
  commit p99 and raise permit wait.
- **Moving the checkpoint to `spawn_blocking` alone avoids the deadlock but stalls.** All 1000
  checkpoints run at once, fill the pool, and take 27 to 30 s each. Commits nearly stop for about 60 s
  per checkpoint round. The pool was full for that whole time and the process recovered, so a second
  deadlock path through DNS lookups did not occur here.
- **With a cap, `spawn_blocking` checkpoints cost a little.** Checkpoint p50 rose from about 300 to
  375 ms and commit p50 by 12 to 15 ms. The cap already removes the deadlock, so this change is
  defense in depth only.
- **Aligned checkpoints set visibility p99.** With the checkpoint cap at 8, each round of 1000
  checkpoints takes about 40 s to drain. A table waiting for a checkpoint permit commits nothing, so
  visibility p99 is about 23 s in every capped cell. Staggered starts cut it to 0.6 s on HNS and 9 s
  on flat. The flat staggered cell had slower checkpoints (p50 768 ms) for reasons not investigated.

**5000 tables (1000 files/s offered).** This load exceeds what one process with 1 commit worker
delivers. Every variant settled at 254 to 338 commits/s, about 3 files per commit, with visibility p50 of
22 to 30 s. Tables reached only version 39 to 44 in 600 s, so aligned cells wrote no checkpoints. That
is why the base cell did not deadlock even though its pool was full (514 threads): the deadlock needs a
checkpoint blocking an async worker. Uncapped cells held about 5000 commits in flight with commit p50 of
15 to 17 s. Capped cells held commit p50 at 97 to 376 ms and moved the wait into the permit queue
(p50 13 to 16 s). Throughput was the same either way, which matches Little's law: cap / commit time is
about 330 commits/s in every capped cell. The ceiling's cause is not verified; the single commit worker
is the likely limit. Sizing tables per process is deferred until benchmarking.

## Experiment 3: ingest throughput per VM

Continuous synthetic Arrow batches are encoded into Parquet files of about 1 GiB with 4 row groups of
about 256 MiB (compression ratio 6.6, plain strings), uploaded with multipart PUTs, and committed by one
table loop. Each point runs 180 s. Summaries: `results/exp3/<vm size>-<account>.csv`. Raw GB/s is
uncompressed input per second at steady state. NIC GB/s is corrected for a double count (see F15).

| VM | Encoders | Flat raw GB/s | HNS raw GB/s | MB/s per encoder core | NIC GB/s | Commits/s | Commit p50 ms flat / HNS |
|---|---|---|---|---|---|---|---|
| D16s_v5 (16 vCPU, 8 cores) | 4 / 8 / 12 / 14 | 1.45 / 2.82 / 3.19 / 3.40 | 1.45 / 2.81 / 3.19 / 3.42 | 363 / 355 / 280 / 257 | up to 0.5 | 0.2 to 0.5 | 40 to 61 / 69 to 107 |
| D32ds_v5 (32 vCPU, 16 cores) | 8 / 16 / 24 / 30 | 2.64 / 4.97 / 5.54 / 6.05 | 2.59 / 4.99 / 5.61 / 6.16 | 337 / 317 / 245 / 212 | up to 0.94 | 0.35 to 0.81 | 44 to 64 / 73 to 92 |
| D64ds_v5 (64 vCPU, 32 cores) | 16 / 32 / 48 / 60 | 5.63 / 10.35 / 12.10 / 13.05 | 5.70 / 10.36 / 11.97 / 12.91 | 361 / 339 / 267 / 228 | up to 1.96 | 0.68 to 1.55 | 53 to 75 / 79 to 99 |

Every point passes G1 to G4.

- **Encode CPU is the limit.** Encoder threads are busy 91 to 95% of the time. Upload waits take 5 to 9%.
- **Per-core rate drops past the physical core count.** Up to one encoder per physical core each
  encoder does 315 to 365 MB/s. With hyperthreads in use it falls to 210 to 280 MB/s.
- **Scaling is linear in cores.** 64 vCPU delivers about 4 times the 16 vCPU rate.
- **NIC is far from its limit.** 1.96 GB/s at 64 vCPU against about 3.75 GB/s for the size. It matches
  the Parquet output rate.
- **The commit loop is far from its limit.** At most 1.55 commits/s against a ceiling of 10 to 12.5.
- **Memory** is about 470 MB per encoder (28.7 GB at 60).
- **Planning number.** 6 cores per GB/s at 60% utilization means about 280 MB/s per fully busy core.
  Measured: 330 to 360 per physical core, 210 to 260 per vCPU. The number holds with modest margin.
- **Account type does not matter** for ingest throughput.

Caveats: synthetic data with plain strings. Production decode (Flight or DFE) also costs CPU and is not
included. Aggregate ingress limits of the storage account were not tested.

## Experiments not run

| Experiment | Reason |
|---|---|
| 4 (zombie writer) | Covered by the fence tests in exp 7 on both accounts. |
| 6 | Out of scope for the design questions. |
| 8 (appId expiry) | Implementation detail. Listed under open questions. |

## Recommendation

1. **Adopt option C:** one commit loop per table, group commit, `txn` idempotency with
   resolve-before-recommit, the fence on every ownership change, and adoption of the old owner's WAL.
2. **Cap concurrent Delta operations per process.** Size the commit cap with Little's law (commits/s
   times commit time, with headroom). 64 covers about 1000 commits/s at 50 ms. Use a separate small
   checkpoint cap. Keep the sum far below the tokio blocking pool limit (512 by default).
3. **Stagger checkpoints across tables**, for example by offsetting each table's checkpoint version by a
   hash of its id. Aligned checkpoints are the main source of visibility p99 at scale.
4. **Report the async-worker checkpoint to delta-rs.** `create_checkpoint` blocks an async worker. With
   the cap in place this is not required for correctness.
5. **Keep expired-log cleanup out of the commit path** (F8). Run it with table maintenance.
6. **Bound the active file count with compaction** (F12).
7. **Place the WAL on a low-latency device, sync off the async workers, and make it readable by a new
   owner** (F13, WAL locality).
8. **Plan ingest at about 300 MB/s of raw input per physical core** plus decode cost. The commit loop and
   NIC are not the limits.
9. **Choose the account type on product needs.** Both meet the guarantees. Flat commits faster.

## Repro

All scripts run on a bench VM with `~/dcb` and `~/scripts` from `scripts/infra.sh push`.

| What | Command |
|---|---|
| Infra up / down | `scripts/infra.sh up`, `scripts/infra.sh addvmss <name> <sku>`, `scripts/infra.sh down` |
| Exp 1 | `scripts/exp1.sh <account>`, then `scripts/summarize.py results/exp1-v3/<account> --csv` |
| Exp 2 baseline | `TAG=shm WAL_ROOT=/dev/shm/wal DUR=300 scripts/exp2.sh <account> 1 100 1000` |
| Exp 2 F14 follow-up | `scripts/exp2-f14.sh <account> 1000 5000`, then `python3 results/exp2-f14/f14sum.py <results dir>` |
| Exp 3 | `scripts/exp3.sh <account> <encoder counts...>` |
| Exp 5 | `scripts/exp5.sh <account>` |
| Exp 7 | `scripts/faults.sh <table-base-url> <work-dir> [rate] [duration]`, with an `abfss://` base (Azure) or a `file://` base (local) |

Raw run logs (`run.jsonl`) are not committed. Committed results are per-cell summaries, guarantee checks,
progress logs and thread dumps.

## Risks and open questions

- Scan-log idempotency duplicates under the library's default retries. It is safe only with library
  retries off, and it cannot be fenced.
- An identity change without adoption loses data.
- WAL locality: adoption needs the new owner to read the old owner's WAL. If the WAL is on a pod's local
  disk and the pod dies, its pending batches are lost. The WAL must be on shared storage, or the
  uploaded files must be recoverable another way.
- Commit-loop cost grows with active file count (F12). Compaction must keep the hot table's file count
  bounded, or the writer's memory and commit latency grow without limit.
- Orphan blobs: files uploaded but never committed (aborted uploads, fenced zombies) need cleanup.
- appId expiry (exp 8): `txn` entries for retired appIds accumulate in the snapshot. Not measured.
- Process ceiling: one process with 1 commit worker delivered about 330 commits/s at 5000 tables. The
  limit's cause and how it scales with worker threads are not measured.
- Blocking pool via DNS: new connections resolve DNS on a blocking thread. With the cap in place the pool
  never fills, so this path stays closed. Without the cap it is a possible second deadlock route; it was
  not observed.
- Production decode CPU is not in the exp 3 numbers.

## Findings log

Each entry: what was observed, the evidence, and what changed.

| # | Finding | Evidence | Action |
|---|---|---|---|
| F1 | A blind recommit after a failed landed check duplicates data. | Local fault suite before the fix. | Added resolve-before-recommit. |
| F2 | The old owner of an appId must be fenced. "Old owner is dead" cannot be assumed. | Zombie test: without a fence the zombie keeps committing. | Fence through the Delta log. |
| F3 | Storage accounts cannot be created in westus3 on this subscription. | Policy deny `deny-unsupported-global-resource-locations`. Allowed regions include westus2. | Moved everything to westus2. |
| F4 | Producer uploads occasionally fail with a connection error that object_store does not retry. | 1 of 20 uploads on the flat account during the smoke test. | Out of scope: the producer stands in for the client. Counted per cell as `upload_failed` to validate offered load. |
| F5 | `DeltaTableState::transaction_version` replays every commit since the last checkpoint. Calling it per commit costs `version + 2` log GETs, which was 102 GETs per commit at v100. The scan-log check read up to 100 commits sequentially per commit. | Trace: kernel span `snap.get_app_id_version` GETs commits N down to 0 concurrently. Before: commit p50 182 ms at 1.2 files/s. After: 2 GETs per commit and p50 83 ms (txn), 69 ms (scan-log). Old cell kept in `results/exp1-v0-prefix-bug`. | The loop skips the check while its snapshot only advanced through its own commits. The library's conflict checker has already screened those for the loop's appId. |
| F6 | Checkpoints on Azure stall for about 30 s when the commit loop runs on a current-thread tokio runtime. The kernel's nested `block_on` stops draining about 50 concurrent streaming GETs until the 30 s client timeout fires. | Checkpoint at v50: 33.3 s and 31.7 s with a current-thread runtime, 0.20 s and 0.19 s with a multi-thread runtime. 2 runs each. Locally (file://) it takes 72 ms either way. Repro cell: `exp1.sh <acct> findings`. | Default is now a multi-thread runtime. `--commit-runtime current` reproduces the stall. |
| F7 | Each commit makes 3 log LISTs. They come from the delta-rs post-commit path (snapshot update and expired-log cleanup). | Counters in exp 1. | Recorded. Matters for storage transaction cost at scale (exp 2). |
| F8 | The delta-rs post-commit hook runs `cleanup_expired_logs_for` on every commit unless `CommitProperties::with_cleanup_expired_logs(Some(false))` is set. It LISTs all of `_delta_log`. The default follows `delta.enableExpiredLogCleanup`, which is true. This is one of the 3 LISTs in F7. | `deltalake-core-1.1.1/src/kernel/transaction/mod.rs` lines 1060 to 1100. A/B on Azure (r25, 16 encoders, 180 s, old binary): cleanup off raised commits/s from 6.0 to 7.05 (flat) and 4.67 to 5.9 (HNS), and LISTs per commit from 3 to 2. Raw: `results/exp1-logcleanup-v1/`. | Added `--log-cleanup default\|on\|off`. A production loop should turn it off and clean logs on a maintenance schedule. Its cost grows with log entries kept (30 days by default). |
| F9 | Commit latency and memory grow linearly with total table version, across checkpoints. About 15 to 22 ms and 1 MB of RSS per 100 versions. At v1000: commit p50 230 to 290 ms, RSS about 1.1 GB. Cause: delta-rs updates the in-memory snapshot incrementally after each commit. It passes every earlier file batch through again and appends one batch per commit. Batches are never compacted, and checkpoints do not reset them, because the update seeds from the previous materialized files. | Bucketed exp 1 cells by version on both accounts. Reproduced on file:// (no storage). Code: `EagerSnapshot::update` then `materialize_files_with_engine(Some(seed))` then `scan_metadata_from(existing_version, existing_data)`. `update_incremental(Some(v))` after the checkpoint did not help: reload time grew too (19 ms to 152 ms). A fresh load (`state = None` then `update_incremental`) fixed it: on file://, commit p50 stayed 13 to 18 ms out to v2700 (was 165 ms at v1100), RSS 168 MB at v2775 (was 790 MB at v1164), reload 5 to 10 ms. | The loop now does a fresh load after each checkpoint it writes. The exp 1 ceiling and pacing cells from the first full run measured this artifact and are superseded. Design implication: a long-lived commit loop on delta-rs 1.1.1 must rebuild its snapshot periodically, and checkpoints are required for the writer as well as for readers. |
| F10 | A cell whose offered load exceeds the loop's ceiling builds a backlog that the loop drains after the producer stops. With F9 the drain slows as it goes. `ceiling-1file-txn-r100` (1 file per commit, 100 files/s) ran over 17 minutes. | progress.log on VM1 and VM2. | Bench design issue. See exp 1 plan. |
| F11 | The synthetic encoders fired in lockstep. With `MissedTickBehavior::Delay`, one main-runtime stall longer than an encoder period re-bases every encoder's schedule to the same instant, and they stay aligned. Arrivals became bursts: all 16 encoders every 640 ms at r25. | Upload start gaps in `run.sent.jsonl`: stalls repeat every 0.64 s exactly (16 / 25 files/s). In the first exp 1 run, the 4-encoder cells at r3 and r6 show gaps above 300 ms for exactly 1/2 and 1/4 of files. High-rate cells were not aligned. | Producer uses `MissedTickBehavior::Skip`. Burstier arrivals inflate files per commit and deflate commits/s, so group-pacing results from earlier runs are superseded. Raw evidence: `results/exp1-v2-aligned/`. |
| F12 | With F9 fixed, commit-loop memory and latency still grow with the table's active file count. About 11 KB of RSS and 1.2 ms of commit p50 per 1000 active files. Checkpoint reload grows from 5 to 129 ms. delta-rs 1.1.1 always holds every active file in the writer's snapshot. The old `require_files` option survives only as an ignored legacy field. | 1-hour local run (file://, 25 files/s, no compaction): 90000 files at v46195. RSS 216 MB at 8.5k files, 1137 MB at 90k. Commit p50 14 ms at v1, 125 ms at v46193. Raw: `results/findings/f12-rsslong/`. Code: `deltalake-core-1.1.1/src/table/mod.rs` line 93. | Design implication: commit-loop cost is bounded only if compaction bounds the table's active file count. A hot table at 1.5 files/s without compaction adds about 130k files a day, which means roughly 1.5 GB and 160 ms more per commit. A file-free commit path would need library work. |
| F13 | The bench WAL makes 3 fsyncs per commit: manifest file, pending directory, and the directory again after release. It calls them synchronously inside the commit runtime's async tasks. On the 30 GB Premium SSD OS disk each fsync takes about 4 ms. With 1 commit thread, 1000 tables saturated at 68 commits/s (offered 200) with commit p50 14.6 s, while the process used about a third of a core. With 4 threads: 116 commits/s, and the disk sat at 95% utilization, about 1030 writes/s with 3.6 ms await. | Exp 2 on flat, WAL on the OS disk: `results/findings/f13-wal-disk/t1-osdisk/` (1 thread; the 5000-table cell also hit the 1024 fd limit) and `t4-osdisk/` (4 threads). `top -H` showed commit threads in D state. `iostat -dx` on VM4. Code: `src/wal.rs` `write` and `release`. | Exp 2 reruns with the WAL on tmpfs to measure the commit loop alone. Design implication: a durable per-table WAL costs disk syncs on every commit. Across many tables, the WAL device's sync latency and IOPS limit commits per pod before CPU does. The production WAL needs a low-latency device, sync calls off the async workers, and probably one group sync across tables. Where the WAL lives also decides whether a new owner can adopt it. |
| F14 | Exp 2 at 1000 tables (flat, WAL on tmpfs, 1 commit worker) deadlocked permanently. Every table stopped at version 49, the commit just before its first checkpoint (`--checkpoint-every 50`). Mechanism, proven from the thread dump and source: (1) the commit runtime's blocking pool holds at most 512 + 1 threads (`max_blocking_threads` plus workers, tokio `builder.rs:2014`); (2) delta-rs runs each kernel snapshot build or update on a blocking thread, and inside it the kernel waits synchronously (`block_in_place` + channel `recv`) for async storage work on the runtime; (3) `create_checkpoint` calls the kernel's synchronous `Snapshot::checkpoint` directly on an async worker (`checkpoints.rs:43`); (4) blocking on a worker makes tokio hand that worker's scheduler to a new pool thread (`worker.rs:489`); (5) with the pool full, the handoff queues forever, no task on the runtime is polled again, and every blocked thread waits on a future that never runs. Request timeouts cannot fire because the same stalled scheduler owns them. Raising `max_blocking_threads` moves the threshold without removing it, because nothing in delta-rs bounds concurrent operations. | `results/findings/f14-blocking-pool-deadlock/`: gdb of all threads on VM4 after 12 hours. 513 `commit` threads, all in `block_in_place`; 24 of them are async workers inside `Snapshot::checkpoint`; the rest are pool threads in kernel snapshot builds (list, last-checkpoint hint, JSON reads). Commit-thread CPU frozen at 374.3 s from 02:55 to 15:07 UTC; stderr silent after 02:55:24 despite 30 s request timeouts. At 100 tables the pool never filled and the cell completed. An earlier DNS theory (connections queued behind the pool) is not supported: no thread was resolving. | Added `--max-ops`, `--max-checkpoints`, `--checkpoint-exec` and `--start-jitter-ms`, plus a stall watchdog in `exp2.sh`. The follow-up (exp 2) reproduced the deadlock on both accounts, showed that a cap of 32 to 128 alone prevents it at no commit p50 cost, and that `spawn_blocking` checkpoints alone avoid it but stall commits for about 60 s per checkpoint round. Design implication: a process-wide cap on concurrent Delta operations is required, and checkpoints should be staggered. |
| F15 | Exp 3 NIC throughput was double counted. With accelerated networking, the VF interface (`enP*`) is enslaved to `eth0`, and both report the same bytes. | `/sys/class/net/<if>/master` exists for the VF. Halved NIC numbers match the Parquet output rate (1.96 GB/s at 64 vCPU). | `nic_tx_bytes` skips enslaved interfaces. Exp 3 ran with the old binary, so the report halves its NIC column. |
