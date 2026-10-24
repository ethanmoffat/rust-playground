# Delta commit benchmark report

Status: in progress. Sections fill in as experiments complete. The findings log at the end records every
result in the order it was found, including bench defects and their fixes.

## Setup

| Item | Value |
|---|---|
| Region | westus2. The plan was westus3. The subscription policy "Deny global resources in unsupported locations" blocks storage accounts there. |
| Storage, HNS | `emdcbhnsf35219`: StorageV2, Standard_LRS, Hot, hierarchical namespace on |
| Storage, flat | `emdcbflatf35219`: StorageV2, Standard_LRS, Hot, hierarchical namespace off |
| Storage security | Shared key off. Anonymous access off. Firewall default Deny with a VNet service endpoint rule. Network security perimeter in Enforced mode with one inbound rule for the subscription. |
| Auth | User-assigned managed identity with Storage Blob Data Contributor |
| VMs | Flexible VMSS, 3 x Standard_D16s_v5, Ubuntu 22.04 gen2, Trusted Launch, accelerated networking, no public IP. Access through Azure Bastion. Egress through a NAT gateway. |
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

## Hypotheses

| Hypothesis | Verdict | Evidence |
|---|---|---|
| No fencing service | Refuted as stated. A fence is required. It needs no separate service: the fence is a Delta commit. | Exp 7 zombie-fence, identity-change. Exp 4 pending. |
| Concurrency safety | Pending exp 4 and 5 on Azure. Holds locally for blind appends. | Exp 7 concurrent-optimize. |

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

## Experiment 1: commit loop characterization

Pending. Running on both accounts. Runner: `scripts/exp1.sh <account>`. Summaries: `scripts/summarize.py`.

## Experiments 2, 3, 4, 5, 6, 8

Pending.

## Recommendation

Pending.

## Risks and open questions

- Scan-log idempotency duplicates under the library's default retries. It is safe only with library
  retries off, and it cannot be fenced.
- An identity change without adoption loses data.
- Orphan blobs: files uploaded but never committed (aborted uploads, fenced zombies) need cleanup.
- appId expiry (exp 8) pending.

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
