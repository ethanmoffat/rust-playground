# Delta commit benchmark report

Status: complete for the design questions in scope, including the follow-up questions Q1 to Q4 on ArrowCol
ownership, appId lifetime and manifest durability. The findings log at the end records every result in
the order it was found, including bench defects and their fixes.

## Design summary

| Question | Answer | Evidence |
|---|---|---|
| One commit loop per table (option C) | Supported. One loop sustains about 12.5 commits/s on flat and 10 on HNS, and group commit absorbs 160 times the hot-table file rate. A second writer adds contention with no throughput gain. | Exp 1, exp 5 |
| Guarantees G1 to G4 | Hold in every cell on both accounts, including faults, zombies and identity changes, with `txn` idempotency, resolve-before-recommit, the fence and adoption. Self-drain with tenure epochs also holds them (follow-up). G2 fails only through `txn` retention expiry or table recreation. | Exp 1, 2, 3, 5, 7, Q1 to Q3 |
| Ownership moves | The fence is required when a new owner adopts the old owner's WAL. It is a Delta commit and needs no separate service. Ack/nack covers graceful moves only. With ArrowCol's per-instance WAL (T1, T2), each identity drains its own WAL under epoch claims, and cross-identity fencing is not needed for G1 to G4. | Exp 7, "Ownership moves", Q1, Q2, F24 |
| Ownership detection | No router signal is needed. Inferred fencing works only with tenure epochs, and does not guarantee one owner. | Q2, F17, F24 |
| appId and fence lifetime | 10,000 appIds cost no commit latency. Only `txn` retention and table recreation remove a fence. Leave retention unset and check `metaData.id`. | Q3, F18 to F20 |
| Many tables in one process | Works only with a process-wide cap on concurrent Delta operations. Without it, delta-rs deadlocks the tokio blocking pool at the first checkpoint round (F14). A cap of 32 to 128 removes the deadlock with no commit p50 cost. | Exp 2, F14 |
| Checkpoint timing | Stagger checkpoints across tables. Aligned checkpoints push visibility p99 to about 23 s at 1000 tables; staggered, 0.6 s on HNS. | Exp 2 F14 follow-up |
| Ingest throughput per VM | Encode CPU is the limit. About 330 to 360 MB/s of raw input per physical core, 210 to 260 MB/s per vCPU past the physical core count. The 6 cores per GB/s planning number holds. NIC and commit loop are far from their limits. | Exp 3 |
| Flat vs HNS | Same guarantees and ingest throughput. Flat commits about 35 to 45% faster at p50. | Exp 1, 2, 3 |
| Per-table WAL | Per-commit fsyncs saturate at 1000 tables on Premium SSD v2 and ZRS. One group sync across tables, off the async workers, removes the WAL as the bottleneck, and manifests can share the data WAL's syncs. With self-drain the WAL does not need to be readable by another instance. | F13, Q4, F23 |
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

There are two kinds of ownership move. Each needs a different mechanism. This section assumes the new
owner can read the old owner's WAL. The follow-up (Q1, Q2) covers ArrowCol's per-instance WAL.

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
| Self-drain replaces fence-and-adopt for per-instance WALs | Holds. G1 to G4 in every Q1 scenario, including two holders of one WAL. | Q1 on local, flat and HNS. |
| Inferred fencing keeps commits flowing under routing noise | Holds with tenure epochs. Fails G1 with one owner appId per identity (F17). | Q2. |
| Group sync removes the WAL as the commit bottleneck | Holds on Premium SSD v2 and ZRS at 1000 tables. | Q4. |

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

**1000 tables (200 files/s offered).** Every completed cell committed all 120000 files and passes G2 to G4.
The checker's G1 result in exp 2 was vacuous (F21). The run records support G1: the committed file count
equals the offered file count in every completed cell.
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

## Follow-up: ArrowCol ownership, appId lifetime and manifest durability

The follow-up tests four questions against ArrowCol's design, which differs from the bench model above:

- Each instance writes its WAL to its own volume (T1 locally redundant, T2 zone redundant). Another
  instance cannot read it. After a failure the volume re-attaches to the restarted pod or to an adopter
  pod. T3 uses a remote WAL that any instance can read.
- Manifests go into Quiver, which group-commits across tables per core with one sync.
- The router pins a table to one instance and gives the server no ownership signal.
- The appId is `(instance identity, table)`.

**Setup changes.** 2 x Standard_D16s_v5 in zone 1 (Premium SSD v2 needs a zonal VM). Each VM has a
Premium SSD v2 64 GiB disk (3000 IOPS, 125 MB/s baseline) at `/mnt/pv2` and a Premium SSD ZRS P30 1 TiB
disk at `/mnt/pzrs`, both with host caching off and ext4. Added crate: chrono 0.4.45 (for zero-retention
VACUUM). Q1 and Q2 ran local (file://) and on both accounts. Q3 fence lifetime ran local only; it tests
log semantics, which do not depend on storage. Q3 accumulation and Q4 ran on the flat account.

**Bench additions.**

| Addition | Flag or script |
|---|---|
| Epoch modes. `drain`: the owner commits under `(X, T)`, and only a drain claims an epoch (the follow-up as written). `tenure`: every ownership tenure and every drain claims a new epoch appId `(X, T, e)`. | `--epochs off\|drain\|tenure` |
| Epoch claim: one commit that bumps the counter `(X, T)#epoch` and fences `(X, T)` and `(X, T, e_last)` | built into drain and acquire |
| Inferred fencing: on the first routed file, fence the previous owner-kind committer found in the last `--scan-window` log entries | `--infer-fence` |
| Router simulation: produce only while a route file names this instance | `--route-file` |
| Self-drain only, no producer | `--drain-only` |
| Group WAL: one process-wide append log, one `fdatasync` per batch on a dedicated thread, optional synthetic data appends through the same syncs | `--wal-sync group`, `--group-sync-ms`, `--data-wal-mbps` |
| Fence lifetime tools | `seed-txn`, `txn-probe`, `cleanup-log`, `vacuum` subcommands |
| Scenario scripts and summaries | `scripts/ownership.sh`, `scripts/own_sum.py`, `scripts/fencelife.sh`, `scripts/txnacc.sh`, `scripts/txnacc_sum.py`, `scripts/q4sum.py` |

### Q1: who commits a failed pod's pending files

Self-drain: Y fences `(X, T)` and commits only new files. X, restarted or replaced by an adopter with
X's volume, claims a new epoch, resolves each pending manifest against the log, and recommits only
what did not land. Y never reads X's WAL. Each scenario ran in drain and tenure mode.

Summaries: `results/followup/local/ownership-fencelife.md`, `results/followup/azure/ownership-flat.md`,
`results/followup/azure/ownership-hns.md`.

| Scenario | G1 to G4 (local, flat, HNS; both modes) | X drain ms, tenure (local / flat / HNS) | Y first commit after switch ms, tenure (local / flat / HNS) |
|---|---|---|---|
| X crashes before its log PUT, Y fences, X restarts and drains | pass | 20 / 62 / 114 | 93 / 552 / 1553 |
| X crashes after its log PUT | pass | 2 / 1 / 2 | 994 / 1403 / 679 |
| X drains a backlog while Y commits at 1.2 files/s | pass | 10 / 1 / 2 | 834 / 597 / 951 |
| Same at 12 files/s | pass | 2 / 2 / 1 | 121 / 452 / 787 |
| X paused with a backlog and a commit in flight, Y fences, X resumes | pass | 4799 / 4572 / 2941 | 267 / 982 / 1127 |
| Two holders: X paused, adopter A drains X's WAL, X resumes and drains the same WAL (A log delay 0, 100, 300 ms) | pass | 1390 to 4807 / 5022 to 5697 / 3333 to 6550 | 462 to 1272 / 292 to 502 / 489 to 1336 |

Drain ms is the time from X's first claim to its last pending manifest committed. The backlog scenarios
slow X's log writes by 150 ms, so their drain time measures the backlog. "Y first commit" includes Y's
process start, table load, fence and claim.

- **Self-drain keeps G1 to G4 in every scenario,** on both accounts and in both modes. No duplicates.
- **Two holders of one WAL are safe without volume exclusivity.** X and A both claim epochs through the
  same counter appId, and each claim fences the previous epoch appId. The two claims conflict in the
  delta-rs conflict checker, so at most one holder commits under any epoch. Each holder resolves every
  manifest against the log before recommitting. This needs the claim to fence the previous epoch even
  when that epoch has no commits yet (F16).
- **Epoch rule.** Epochs are per identity and table, numbered by the counter `(X, T)#epoch`. A claim
  reads the counter, then commits counter + 1 together with fences on `(X, T)` and `(X, T, e_last)`.
  Only the same identity claims its own epochs. A cross-identity fence names the other identity's
  current epoch appId.
- **Fence-and-adopt is needed only for T3.** T1 and T2 need no adoption.
- **Pending data waits** for X's restart or volume re-attach, plus a drain of milliseconds (crash) to a
  few seconds (large backlog).

### Q2: how a new owner knows it should fence, and what a misroute costs

Inferred fencing, measured with misroutes (T goes to Y for 5, 30 or 120 s, then back to X) and a rapid flip
(T alternates every 1 s for 60 s). A third run turned cross-identity fencing off (`XFENCE=off`): no
`--infer-fence`, no `--fence-identity`. Same-identity epoch claims stay. This run followed F24.

Summaries: as Q1, plus `results/followup/local/ownership-nofence.md` and
`results/followup/azure/ownership-flat-nofence.md`.

| Scenario | Mode | G1 (local, flat, HNS) | Accepted files never committed | appIds | Switch lag ms local / flat / HNS (to Y, back to X) |
|---|---|---|---|---|---|
| Misroute 5 s | drain | FAIL | 198 to 200 | 4 | |
| Misroute 30 s | drain | FAIL | 198 to 199 | 4 | |
| Misroute 120 s | drain | FAIL | 195 to 198 | 4 | |
| Flip | drain | FAIL | 497 to 501 | 4 | |
| Misroute 5 s | tenure | pass | 0 | 6 | 74, 150 / 711, 202 / 797, 399 |
| Misroute 30 s | tenure | pass | 0 | 6 | 51, 81 / 583, 315 / 1039, 373 |
| Misroute 120 s | tenure | pass | 0 | 6 | 87, 301 / 658, 449 / 1079, 919 |
| Flip | tenure | pass | 0 | 122 / 90 / 10 | p50 102 max 218 / p50 191 max 567 / p50 314 max 908 |
| Misroute 5 s | no cross fence | pass (local, flat) | 0 | 4 | 136, 71 / 1018, 516 |
| Misroute 30 s | no cross fence | pass (local, flat) | 0 | 4 | 179, 267 / 1286, 1729 |
| Misroute 120 s | no cross fence | pass (local, flat) | 0 | 4 | 91, 631 / 1081, 7007 |
| Flip | no cross fence | pass (local, flat) | 0 | 4 | p50 145 max 228 / p50 227 max 1340 |

G2, G3 and G4 pass in every row. Switch lag is the time from the route change to the new side's first
successful commit.

- **Drain mode (the follow-up as written) loses data under routing noise** (F17). When T returns to X,
  X's owner appId `(X, T)` is already fenced, so X can never commit new files under it again. The
  files routed to X after the return stay in its WAL.
- **Tenure epochs fix it.** Each switch costs one fence by the new side and one claim. A misroute costs
  2 fences and 4 claims. Neither side is blocked longer than the switch lag. Pending data is drained by
  the side that wrote it.
- **Fences do not pile up.** Each switch adds about 2 epoch appIds. 60 switches added 90 to 122 appIds
  (Q3 shows 10,000 cost little).
- **Inferred fencing does not converge to one owner** (F24). In the HNS flip run, a file delayed by
  upload latency reached X after Y claimed but before Y committed data. X found itself as the latest
  committer, claimed without fencing Y, and both stayed owners for the last 55 s. G1 to G4 still held.
- **Cross-identity fencing is not needed for G1 to G4 on T1 and T2.** With it off, every Q1 and Q2
  scenario passes locally and on flat. X and Y commit disjoint files under disjoint appIds, which delta-rs
  never treats as a conflict (exp 5). The claim that fences a WAL's previous holder is still needed; it is
  same-identity. Without cross fences an instance that sat idle commits from a stale snapshot and pays a
  conflict check over every commit it missed: 6.8 s and 1229 log GETs after 120 s idle on flat (F25).
- **Router minimum: none** for correctness. A hint header would cut epoch churn and is optional.
  Signaled assignment buys nothing for G1 to G4 on T1 and T2. T3 adoption still needs a fence on the
  adopted identity, which the adopter applies explicitly.

### Q3: appId and fence lifetime (experiment 8)

**Accumulation.** One table with 0 to 10,000 extra appIds seeded as `txn` entries, then 10 files/s for
60 s with a checkpoint every 50 versions. Summaries: `results/followup/local/txnacc.md`,
`results/followup/azure/txnacc-flat.md`.

| appIds | Commit p50 / p99 ms (local / flat) | Checkpoint ms p50 (local / flat) | Checkpoint bytes | Reader load ms p50 (local / flat) | Writer RSS MB max (local / flat) |
|---|---|---|---|---|---|
| 0 | 14 / 26, 40 / 99 | 73 / 193 | 68,600 | 5 / 102 | 56 / 148 |
| 100 | 14 / 26, 42 / 85 | 72 / 211 | 71,079 | 7 / 123 | 44 / 134 |
| 1,000 | 13 / 26, 40 / 81 | 79 / 181 | 83,355 to 83,763 | 7 / 122 | 48 / 173 |
| 10,000 | 14 / 26, 42 / 91 | 91 / 208 | 219,110 to 219,120 | 13 / 129 | 52 / 166 |

- 10,000 appIds leave commit latency unchanged. Each appId adds about 15 bytes to the checkpoint. Reader
  load grows by 8 ms locally and 27 ms on flat. RSS varies by run more than by appId count.

**Fence lifetime.** X crashes right after a log write with its manifest still pending. Y fences X. Then one
mechanism removes or hides entries, and X restarts (or resumes, for `inproc`). Local, retention 10 and
20 s. Summary: `results/followup/local/ownership-fencelife.md`.

| Mechanism | Fence survives | Result, epochs off | Result, tenure | Reader effect |
|---|---|---|---|---|
| Checkpoint, then log cleanup deletes all earlier commits | yes (checkpoint keeps `txn`) | pass | pass | none |
| OPTIMIZE (remove N, add 1) and a checkpoint | yes | pass | pass | none |
| X stays running while retention expires, then resumes | no | G1 fails, 1 file stranded (off mode needs adoption; this scenario has none) | pass | none |
| Retention expires, X restarts | no | G2 fails: X recommits its landed batch, then commits new files under its formerly fenced appId | G2 fails: X reclaims epoch 1 (the fenced epoch) and recommits | Same path twice in the log. The snapshot keeps one add. No duplicate rows. |
| Retention expires, then checkpoint and log cleanup, X restarts | no | X recommits as above. The checker passes because cleanup deleted the original commit. | same | none |
| OPTIMIZE removes X's batch, retention expires, X restarts | no | G2 fails | G2 fails | X re-adds a compacted file. Readers see its rows twice. |
| Same, then VACUUM deletes the removed files | no | G2 and G4 fail | G2 and G4 fail | The re-added file no longer exists. Reads fail. |
| Table deleted and recreated at the same path | no | X commits 4 batches into the new table | 3 batches | Old data appears in the new table. |

- **What removes a fence:** only `txn` retention and table recreation. Checkpoints, log cleanup and
  OPTIMIZE keep `txn` entries. VACUUM ran only after retention expired. It deletes data files and never
  rewrites the log, so it cannot remove `txn` entries.
- **The stale owner that stays running is protected regardless of retention.** Its snapshot predates
  the fence, and the conflict checker sees the fence in the winning commits.
- **A restarted owner is not protected.** Expiry removes both the fence and X's own last `txn` version,
  so resolve-before-recommit cannot tell that the batch landed (F19).
- **Retention also expires the epoch counter** (F18). Its last update changes only on a claim. With
  10 s retention a claim reused epoch 1.

### Q4: manifest durability with group sync

Exp 2 at 100 and 1000 tables, 0.2 files/s per table, 300 s, commit cap 64, checkpoint cap 8, checkpoints
on the async worker, aligned starts. The WAL is on the data disk. Modes: `per-commit` (the F13 WAL, a file
per manifest with 3 fsyncs on the commit task), `group` (one process-wide log, one `fdatasync` per batch
on a dedicated thread, `--group-sync-ms 0`), and `group` plus 20 MB/s of synthetic data appended through
the same log. Summary: `results/followup/azure/q4.csv`.

| Disk | Mode | Tables | Commits/s (offered 20 / 200 files/s) | Commit p50 / p99 ms | Visibility p50 / p99 ms | Sync p50 / p99 ms | Syncs/s | Manifests per sync | Disk util % | Write await ms |
|---|---|---|---|---|---|---|---|---|---|---|
| PSSD v2 | per-commit | 100 | 20 | 36 / 87 | 40 / 93 | | | | 10.6 | 1.05 |
| PSSD v2 | per-commit | 1000 | 125.5 | 502 / 677 | 13367 / 17968 | | | | 63.6 | 1.83 |
| PSSD v2 | group | 100 | 20 | 37 / 86 | 39 / 89 | 2.4 / 104 | 20 | 1.0 | 4.7 | 0.82 |
| PSSD v2 | group | 1000 | 185.2 | 52 / 385 | 57 / 22135 | 2.3 / 5.2 | 162 | 1.1 | 35.9 | 0.74 |
| PSSD v2 | group + data | 100 | 20 | 34 / 81 | 36 / 88 | 2.6 / 5.0 | 120 | 0.2 | 29.9 | 0.81 |
| PSSD v2 | group + data | 1000 | 189.6 | 46 / 365 | 50 / 15719 | 2.3 / 4.2 | 243 | 0.8 | 58.2 | 0.76 |
| PSSD ZRS | per-commit | 100 | 20 | 53 / 139 | 66 / 153 | | | | 36.7 | 2.84 |
| PSSD ZRS | per-commit | 1000 | 51.9 | 1253 / 1600 | 37010 / 40422 | | | | 88.6 | 4.47 |
| PSSD ZRS | group | 100 | 20 | 36 / 92 | 45 / 103 | 8.4 / 41 | 20 | 1.0 | 17.3 | 2.9 |
| PSSD ZRS | group | 1000 | 188.8 | 47 / 348 | 61 / 18501 | 8.1 / 26 | 98 | 1.9 | 81.1 | 2.75 |
| PSSD ZRS | group + data | 100 | 20 | 35 / 84 | 48 / 100 | 8.6 / 15 | 108 | 0.2 | 92.4 | 2.85 |
| PSSD ZRS | group + data | 1000 | 190 | 43 / 370 | 57 / 15197 | 7.9 / 22 | 118 | 1.6 | 96.1 | 2.7 |

Every cell passes G1 to G4. Commits/s at 1000 tables is below 200 in group cells because some commits
carry 2 files; all 60,000 files committed. Per-commit cells at 1000 tables did not keep up (F23).

- **Group sync removes the WAL as the commit bottleneck.** At 1000 tables, group cells match the exp 2
  tmpfs cap64 cell on flat (187 commits/s, commit p50 46 ms) on both disks. Per-commit saturates at
  125.5 commits/s on PSSD v2 and 51.9 on ZRS.
- **This comparison changes two things at once:** the sync count (3 fsyncs per commit against 1 per
  batch) and where the sync runs (the commit task against a dedicated thread). The bench does not
  separate them.
- **A ZRS sync costs about 3.5 times a PSSD v2 sync** (8 ms against 2.3 ms p50). Group commit absorbs it:
  ZRS batches more manifests per sync (1.9 against 1.1), and commit latency is the same.
- **A manifest can share the data's sync.** With 20 MB/s of data through the same log, most syncs carry
  data and manifests ride on them (0.8 and 1.6 manifests per sync). Commit p50 did not rise. The commit
  loop needs only the ack of the sync that covers its manifest. A dedicated sync per commit is not needed.
- **Visibility p99 of 15 to 22 s** comes from aligned checkpoints, as in exp 2. Staggering fixes it.
- The handoff between a persist core and a commit core was a channel to one sync thread here. Quiver's
  per-core logs and cross-core wakeup latency were not measured.

### G2 failure paths under the recommended design

The recommended design is self-drain with tenure epochs, resolve-before-recommit and `txn` idempotency.

| Path | How often | Reader effect | Detection | What closes it |
|---|---|---|---|---|
| `txn` retention expires before a crashed owner restarts, then it recommits a landed batch (F19) | Only if `delta.setTransactionRetentionDuration` is set and shorter than the outage | None while the file stays in the table | The same path added twice in the log | Leave retention unset, or set it far above the longest outage |
| Same, after OPTIMIZE removed the batch | As above, plus a compaction in the window | Duplicate rows | Same path re-added after a remove | As above |
| Same, after VACUUM deleted the file | As above, plus VACUUM in the window | Reads fail (G4) | Add of a missing file | As above |
| Table deleted and recreated at the same path (F20) | Operator action | Old batches in the new table | `metaData.id` differs from the one in the manifest | Record the table id in each manifest and verify it at claim and before commit |
| Client resends after a lost ack | Outside the commit loop | Duplicate rows | Client session id and sequence in the data | Client-side idempotency keys, deduplicated at ingest |
| Library 5xx retry recommits a landed commit | Closed by `txn` | None | | Already closed |

### Rules for the ArrowCol design doc

**Recovery after pod failure**

1. Each identity drains its own WAL. The restarted pod or the adopter with the volume claims a new epoch
   and recommits only manifests the log does not show as landed.
2. An epoch claim is one commit: counter `(X, T)#epoch` + 1, a fence on `(X, T)` and a fence on
   `(X, T, e_last)`, even if `e_last` has no commits.
3. Two holders of one volume need no exclusivity. Epoch claims serialize them.
4. Fence-and-adopt applies only to T3.

**Ownership detection**

5. No router signal is needed. An instance commits for any table it receives files for.
6. Use tenure epochs: every ownership tenure claims a new epoch. Never reuse a fenced owner appId.
7. Cross-identity fencing is optional on T1 and T2. If it is kept, it must fence the other identity's
   current epoch appId, and it does not guarantee a single owner.
8. Refresh the snapshot before the first commit after an idle period.

**Fence and appId lifetime**

9. Leave `delta.setTransactionRetentionDuration` unset. If it must be set, make it far longer than the
   longest outage plus the longest tenure.
10. If retention is set, refresh the epoch counter on owner commits, or epochs get reused.
11. Checkpoints, log cleanup, OPTIMIZE and VACUUM are safe for fences.
12. Record `metaData.id` in each manifest. Verify it at claim and before each commit.
13. Epoch appIds accumulate at about 2 per ownership switch. 10,000 cost about 150 KB of checkpoint and
    no commit latency. Accept the growth, or retire them with a long retention.

**Manifest durability**

14. Group-sync manifests with the data WAL. One sync per batch, on a thread off the async workers.
15. The commit loop waits for the ack of the sync that covers its manifest, then commits.
16. Premium SSD v2 and Premium SSD ZRS both sustain 1000 tables with group sync. ZRS syncs take 3.5 times
    longer and batch more.

## Experiments not run

| Experiment | Reason |
|---|---|
| 4 (zombie writer) | Covered by the fence tests in exp 7 on both accounts. |
| 6 | Out of scope for the design questions. |

## Recommendation

1. **Adopt option C:** one commit loop per table, group commit, `txn` idempotency with
   resolve-before-recommit. For per-instance WALs (T1, T2), recover by self-drain with tenure epochs
   (Q1, Q2). For T3, fence the old owner and adopt its WAL.
2. **Cap concurrent Delta operations per process.** Size the commit cap with Little's law (commits/s
   times commit time, with headroom). 64 covers about 1000 commits/s at 50 ms. Use a separate small
   checkpoint cap. Keep the sum far below the tokio blocking pool limit (512 by default).
3. **Stagger checkpoints across tables**, for example by offsetting each table's checkpoint version by a
   hash of its id. Aligned checkpoints are the main source of visibility p99 at scale.
4. **Report the async-worker checkpoint to delta-rs.** `create_checkpoint` blocks an async worker. With
   the cap in place this is not required for correctness.
5. **Keep expired-log cleanup out of the commit path** (F8). Run it with table maintenance.
6. **Bound the active file count with compaction** (F12).
7. **Group-sync manifests with the data WAL on a low-latency device, off the async workers** (F13, Q4).
8. **Plan ingest at about 300 MB/s of raw input per physical core** plus decode cost. The commit loop and
   NIC are not the limits.
9. **Choose the account type on product needs.** Both meet the guarantees. Flat commits faster.
10. **Leave `delta.setTransactionRetentionDuration` unset and record `metaData.id` in each manifest**
    (Q3, F19, F20).
11. **Refresh the snapshot before committing after an idle period** (F25).
12. **Follow the ArrowCol rules list** in the follow-up section.

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
| WAL data disks | `scripts/infra.sh disks`, then `scripts/infra.sh mountdisks <n>` |
| Q1, Q2 | `EPOCHS=drain\|tenure [XFENCE=off] scripts/ownership.sh <table-base-url> <work-dir> [scenario...]`, then `python3 scripts/own_sum.py <work-dir>` |
| Q3 fence lifetime | `EPOCHS=off\|tenure RET=10 scripts/fencelife.sh file:///tmp/fl /tmp/fl-work [scenario...]` |
| Q3 accumulation | `scripts/txnacc.sh <table-base-url> <work-dir>`, then `python3 scripts/txnacc_sum.py <work-dir>` |
| Q4 | `TAG=q4-<disk>-<mode> DUR=300 RATE=0.2 MAX_OPS=64 MAX_CKPT=8 EXEC=worker WAL_ROOT=/mnt/<disk> WAL_ARGS='--wal-sync group [--data-wal-mbps 20]' scripts/exp2.sh <account> 100 1000`, then `python3 scripts/q4sum.py results` |

Raw run logs (`run.jsonl`) are not committed. Committed results are per-cell summaries, guarantee checks,
progress logs and thread dumps.

## Risks and open questions

- Scan-log idempotency duplicates under the library's default retries. It is safe only with library
  retries off, and it cannot be fenced.
- An identity change without adoption loses data.
- WAL locality: adoption needs the new owner to read the old owner's WAL. Self-drain (Q1) removes this
  need for T1 and T2. Pending data then waits for the pod restart or volume re-attach. If the volume is
  lost, its pending batches are lost.
- Commit-loop cost grows with active file count (F12). Compaction must keep the hot table's file count
  bounded, or the writer's memory and commit latency grow without limit.
- Orphan blobs: files uploaded but never committed (aborted uploads, fenced zombies) need cleanup.
- appId accumulation (exp 8, Q3): 10,000 appIds cost no commit latency and about 150 KB of checkpoint.
  Growth past that is not measured.
- `txn` retention: anyone who sets `delta.setTransactionRetentionDuration` on a table reopens the G2 paths
  in Q3. Treat it as a guarded table property.
- Persist-core to commit-core handoff latency in Quiver is not measured (Q4).
- Fence lifetime ran on file:// only. The log semantics it tests do not depend on storage.
- Fresh ext4 data disks initialize inode tables in the background for a while after mount. It added about
  45 MB/s of writes to the first ZRS per-commit cells (F26).
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
| F16 | An epoch claim that skips fencing an empty previous epoch lets two holders of one WAL commit under the same epoch. Holder A claims e2 and has not committed yet. Holder X reads the counter, sees e2 has no `txn`, and claims e3 without naming e2. A's first commit under e2 then does not conflict with X's claim. | Local two-holders runs during development. The pre-fix runs were not kept. | A claim always fences `(X, T)` and `(X, T, e_last)`, even when `e_last` has no commits. Naming the appId makes A's commit and X's claim conflict in the delta-rs checker. |
| F17 | With one owner appId per identity (drain mode, the follow-up as written), inferred fencing loses data under routing noise. Y fences `(X, T)`. When T's traffic returns to X, X cannot commit under its fenced owner appId, and drains claim epochs only for pending data. New files routed to X stay in its WAL. | Q2, local and both accounts: G1 fails in every misroute and flip run with 195 to 501 files stranded. Tenure mode passes every run. | Tenure epochs: every ownership tenure claims a new epoch appId. |
| F18 | `txn` retention also expires the epoch counter. Its last update changes only on a claim. After expiry the next claim starts again at epoch 1 and reuses a fenced epoch appId. | Q3 `restart-ret-tenure` at retention 10 s: the end probe shows `#e1` unfenced at version 18 and the counter back at epoch 1. | Leave retention unset, or refresh the counter on owner commits. |
| F19 | When `txn` retention expires before a crashed owner restarts, the fence and the owner's own last `txn` version are both gone. Resolve-before-recommit cannot see that the pending batch landed, and the owner recommits it. Same path: readers see no duplicate. After OPTIMIZE removed the batch, the re-add duplicates rows. After VACUUM deleted it, the table points at a missing file. In epochs-off mode the restarted owner also commits new files under its formerly fenced appId. | Q3 fencelife `restart-ret`, `restart-ret-optimize`, `restart-ret-vacuum`, `restart-cleanup-ret` in both modes. A stale owner that stays running is protected: its snapshot predates the fence, and the conflict checker sees the fence in the winning commits (`inproc-ret-tenure` passes). | Rule: leave `delta.setTransactionRetentionDuration` unset. |
| F20 | Deleting and recreating a table at the same path erases every fence. A restarted owner then commits its old pending batches into the new table. | Q3 `recreate`: 4 commits (off) and 3 (tenure) by the restarted owner into the new table. | Rule: record `metaData.id` in each manifest and verify it at claim and before commit. |
| F21 | Checker defects. (1) In exp 2 the checker found no manifests under the per-table WAL directories, so its accepted set was empty and G1 passed vacuously. (2) After log cleanup deleted early commits, active files had no surviving add and counted as missing. | exp2.sh passes each table's own WAL directory. The checker treated it as a root of per-appId directories and looked one level too deep, so it read no manifests. The committed exp 2 check files hold only the aggregate verdicts. Q3 cleanup scenarios showed (2). | Fixed in 9abe348. The checker reads per-appId and group WALs, and counts snapshot-only paths as committed when the log is truncated (duplicates among deleted commits stay invisible). Exp 2 G1 rests on run-record file counts. Q4 check files have non-zero manifests. |
| F22 | `kill -STOP $X` in the scenario scripts paused the bash subshell, which left dcb running. Early pause scenarios therefore tested nothing. | In the scripts, `$!` was the PID of the bash subshell that ran dcb. After the fix it is the dcb PID. | Scripts exec dcb inside the background subshell, so `$!` is the dcb process. Pause-inflight, two-holders and inproc-ret were rerun. |
| F23 | The per-commit WAL (F13) saturates at 1000 tables on data disks as well as the OS disk. 125.5 commits/s on Premium SSD v2 and 51.9 on Premium SSD ZRS, against 200 files/s offered, with commit p50 of 0.5 and 1.25 s. | Q4 per-commit cells. | Group sync: 185 to 190 commits/s on both disks with commit p50 43 to 52 ms. |
| F24 | Inferred fencing does not converge to one owner. If a file delayed by upload latency reaches X after Y claims but before Y commits data, X finds itself as the latest committer and claims without fencing Y. Neither side is fenced again, because fencing happens only on acquire. G1 to G4 still hold: X and Y commit disjoint files under disjoint appIds. | HNS flip-tenure: 10 appIds against 90 on flat; both sides committed about 40 times per 10 s for the last 55 s with no fence. A run with cross-identity fencing off passes every Q1 and Q2 scenario locally and on flat. | Cross-identity fencing is optional on T1 and T2. The same-identity epoch claim stays required. |
| F25 | A commit loop that sat idle commits from a stale snapshot. delta-rs then conflict-checks every commit since its base version, one log GET each. | Q2 misroute-120 with cross-identity fencing off, flat: X's first commit after 120 s idle was 1142 versions behind, made 1229 log GETs and took 6.8 s. With fencing, acquire refreshes the snapshot first and the same switch took 0.45 s. | Rule: refresh the snapshot before the first commit after an idle period. |
| F26 | A freshly formatted ext4 data disk initializes inode tables in the background after mount. On the 1 TiB ZRS disk this added about 45 MB/s of writes during the first Q4 cells. | Q4 ZRS per-commit at 100 tables: 45.8 MB/s written and 46% utilization for 20 commits/s. The rerun: 1.8 MB/s and 37%. At 1000 tables the rerun changed commits/s from 48.1 to 51.9. | Report uses the rerun. Format bench disks with `lazy_itable_init=0` or wait for init to finish. |
