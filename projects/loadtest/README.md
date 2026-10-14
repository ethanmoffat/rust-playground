# loadtest

```bash
npx nx run loadtest:run -- --warmup 2m --concurrency 8 --batch-rows 1024
```

Omit `--measure` to run continuously until Ctrl+C. SQL Server defaults to a
16 GiB memory budget; override it with `SQLSERVER_MEMORY_LIMIT_MB` (in MiB):

```bash
SQLSERVER_MEMORY_LIMIT_MB=8192 npx nx run loadtest:run -- --run-id smaller-sql-20261014 --warmup 2m --concurrency 8 --batch-rows 1024
```

Keep headroom for the collectors, Hive, observability, and the host. Changing the
memory environment variable requires recreating the SQL container, not merely
restarting it. Preserve the SQL volume and Delta roots when recovering a run;
use a new load-generator run ID and results directory to retain failure evidence
and avoid reusing logical batch identities.

Watch:

- Grafana: http://localhost:3000/d/arrowcol
- SQL Server: `localhost,14333` (host port `14333` mapped to container port `1433`) — username `sa` and password printed at startup from `projects/arrowcol-infra/secrets/sqlserver-sa-password.txt`. Set `SQLSERVER_PORT` to override the host port. SQL data remains mounted in the project-scoped `sqlserver-data` volume at `/var/opt/mssql`; `down -v` removes it.
- Portainer (container logs, stats, shells): https://localhost:9444 — `admin` / the SQL Server SA password, printed at startup and stored in `projects/arrowcol-infra/secrets/sqlserver-sa-password.txt`. Self-signed TLS; set `PORTAINER_PORT` if 9444 is taken. The default is 9444 rather than 9443 because VS Code's port forwarder tends to claim 9443 on the Windows host. If it times out while Grafana works, VS Code has forwarded the port into the devcontainer: stop that forward in the Ports panel, then `docker restart rust-playground-loadtest-latest-portainer-1`.
- Logs: `docker compose -p rust-playground-loadtest-latest logs -f`

Results: `onelake/benchmark/latest/results/`

Printed credentials are for local development only; do not share startup logs
containing passwords.

Before generating load, the runner prints the absolute paths to both Delta table
roots. The corresponding workspace-relative locations are:

```text
Delta tables on disk (workspace paths)
-------------------------------------
  Direct:   onelake/benchmark/latest/direct
  Dataflow: onelake/benchmark/latest/dataflow
```

Paths reflect your workspace and selected run ID. Open these directories to inspect
the tables while the loadtest runs.

## Isolated runs and retained history

`--run-id` defaults to `latest` for path/project compatibility. Explicit IDs must
be 1–38 lowercase ASCII letters, digits, underscores or hyphens, starting with a
letter or digit. IDs are never normalized or truncated, so distinct accepted IDs
produce distinct Compose projects, SQL volumes, helper scratch paths and Delta
roots. A run reserves `onelake/benchmark/<run-id>` before startup.

Existing directories (even empty reservations), containers, networks or retained
SQL volumes block reuse with an error recommending an unused `--run-id`.
The runner never tears down an old stack, regenerates its secrets during stop,
archives its directory or removes its volumes. New projects bootstrap fresh
tables in their own SQL volume. Retained stacks use separate available subnets;
to run several simultaneously, override their published host ports.

Stop a retained stack explicitly without deleting its SQL history:
`docker compose -p rust-playground-loadtest-latest down`
(use your selected project and its original environment/file settings). Do not
use `down -v` if you want to preserve SQL state.

For a finite measured 15-minute soak, using all 13 schema fixtures by default:

```bash
npx nx run loadtest:run -- --run-id all13tables8lanes1024rows-20261014 --warmup 2m --measure 15m --concurrency 8 --batch-rows 1024 --validate
```

Leave `LOADTEST_TABLES` unset to include all 13 tables. Warmup, schema preparation,
capture settling and final validation/drain are additional to the measured 15
minutes. The stack remains live afterward; the owned sampler stops.

## Resource samples and provenance

Before load generation, the runner starts a separate Node/TypeScript process and
checks its first sample/status. It samples every 10 seconds even while the runner
is blocked in the synchronous generator. `results/resource-samples.jsonl` contains
UTC timestamps, container/image IDs and host PID, `/proc/1/status` PID/RSS/RssAnon/
threads, cgroup v2 `memory.current`, `memory.stat`, working set
`max(0, current - inactive_file)`, cumulative `cpu.stat`, numeric Arrowcol runtime
counters/backlog from `/metrics`, and `/readyz` HTTP readiness. Counter series
with different labels are summed by metric name; labels and response bodies are
not retained. Counters are cumulative, not interval CPU percentages.

Every failed read/request is explicit (`ok: false` and a safe error kind/code);
missing cgroup v2 files are not reported as zero usage. `sampler-status.json`
records lifecycle/counts; `sampler.log` never prints secrets or raw command output.
The runner stops only its owned sampler on exit, leaving the stack intact; the
sampler also detects loss of its owning parent.
A per-results `sampler.lock` prevents two samplers from writing concurrently;
it is removed on orderly exit. An abandoned lock requires explicit operator
inspection rather than automatic replacement.

`results/provenance.json` records source/submodule revisions, dirty-diff and
untracked-source hashes, config hashes, actual image IDs/CPU allocations, safe
effective CPU/thread/message-size/SQL-memory settings, bounded flush/commit/
buffer/receipt policies and workload settings. Raw Compose config/environment
is never written because it can contain passwords. Startup/load failures are
recorded in `runner-failure.json`; samples remain available for failed runs.

Both collectors default to 2 CPUs (`LOADTEST_CPUS`); direct uses 2 Tokio workers
and DFE receives `--num-cores 2`, overriding the production engine policy's
`core_count: 1`. Provenance distinguishes source policy from resolved allocation.
DFE shares its process-wide stream permits across receiver cores, so the
configured 256-stream limit is aggregate, matching direct's 256 rather than
multiplying with core count. Direct's durable receipt contract remains distinct
from DFE's best-effort admission; tests do not weaken either delivery contract.

### Fair runtime buffer profile

The harness generates matching runtime policies without changing production
defaults: **128 MiB process-global Arrow capacity**, **32 MiB per table** and
**32 MiB per flush** for both collectors. Global row/batch limits are
4,000,000/2,048; per-table limits are 1,000,000/512, preserving the existing
1,000,000-row/512-batch flush thresholds within each table's capacity.
Registration advertises these same process-global limits.

DFE's exporter factories share one `DeltaBackend` keyed by Delta root, including
its capacity and flush slots. The 128 MiB budget is applied **once per process**,
not divided by or multiplied by its two cores. Adaptive sizing, age limits,
flush row/batch thresholds, four concurrent flushes, commit policies, stream
limits, delivery contracts and the two-CPU allocation are unchanged.

Generated files are `direct-state-1/direct.yaml`, `dataflow-state-1/dataflow.yaml`
and `router-state/compose-runtime.json` under the selected run directory. The
last file is a minimal Compose override mounting the DFE runtime YAML read-only
at its existing config path; it contains no secrets. The runner passes it as a
second Compose file using `LOADTEST_COMPOSE_OVERRIDE_FILE`. External recovery
commands must retain that override, otherwise DFE would revert to its production
buffer defaults. Provenance records the generated policies and their hashes.
Neither the production DFE config nor the direct template is rewritten.

Logical Arrow capacity is not an RSS limit: decoded channels, runtime overhead,
publication work and SDK schema replay also consume memory. The bounded profile
and schema-cache follow-up passed the finite continuation7 resource qualification
and independent frozen-output verification below. The
earlier V2 measurements used the original 2 GiB profile.

For a persistent continuous run with 30 seconds of warmup and all 13 tables:

```bash
npx nx run loadtest:run -- --run-id all13tables8lanes1024rows-live --warmup 30s --concurrency 8 --batch-rows 1024
```

This continues sampling and generating load until stopped; observe at least
15 minutes of post-warmup samples for the soak evidence. Use an unused ID and
leave `LOADTEST_TABLES` unset.

## Qualification findings

The recovered first run, `memory-fair-20261014`, passed its 15-minute memory and
thread gates but **failed the no-hard-pressure gate**; it was not a successful
qualification. About 1,382 direct routing refusals exposed pressure incorrectly
normalized by four flush-worker slots: four busy workers reported 100% pressure
despite a 256-entry commit budget and roughly 90 MiB of buffered data. Direct's
rich/admin pressure now uses the configured Delta `max_pending_files` budget;
four busy workers remain below 95%, while a truly full 256-entry queue reports
100%.

Three SQL receipt `CK_delivery_receipts_v2_times` errors also exposed a timestamp
rounding race. Receipt insertion now uses one database timestamp for explicit
`created_at`, `updated_at` and heartbeat values instead of separately evaluating
defaults and heartbeat time at `DATETIME2(3)` precision. Database-clock authority
expiry checks remain enforced, and already-expired receipt deadlines are
rejected.

### V2 measured profile

`memory-fair-20261014-v2` **passed the 15-minute soak verifier**: 900.342 seconds
after warmup, with 91 samples across all 13 tables, eight lanes and 1,024 rows per
batch. Evidence:
`onelake/benchmark/memory-fair-20261014-v2/results/memory-soak-verification.json`.
The original continuous-run command was:

```bash
npx --no-install tsx projects/loadtest/src/main.ts --run-id memory-fair-20261014-v2 --warmup 30s --concurrency 8 --batch-rows 1024
```

This ID is now retained, not reusable; do not replay the startup command against
its existing history.

| Collector | Mean RSS (MiB) | Peak RSS (MiB) | Maximum threads | Exported rows/s | Mean CPU (% of one core) |
| --- | ---: | ---: | ---: | ---: | ---: |
| Direct | 178.24 | 188.39 | 54 | 36,931.9 | 136.00 |
| DFE | 221.66 | 266.21 | 73 | 36,673.7 | 71.93 |

Export rates come from runtime counters, not final independent Delta validation.
CPU is relative to one core; both collectors have a two-CPU budget. The verifier
observed 64,714 manifest entries and 32,358 paired logical IDs, all recorded
attempts at one, matching row/payload checksums, and no error, NACK, readiness or
restart failures.

Queue variation was approximately 85 MiB. DFE's early steady-state RSS median was
212.02 MiB versus 227.17 MiB late (+15.15 MiB); minute averages fluctuated between
197 and 233 MiB rather than showing runaway growth in this window. This bounded
observation is **not proof of absence of a heap leak or an indefinite memory
guarantee**.

Full live SQL receipt integration passed. Final independent validation of the
original frozen V2 Delta roots **passed with exit 0** using the unchanged release reader:
13 schemas, complete rows, paired parity, receipts, routing, capabilities and
6,042 Delta files. All 43,753 direct durable receipts at that historical boundary
were terminal published. This historical validation does not validate later
continuation output.
The validator was built with
`cargo build --locked --release --package arrowcol-loadgen` and run with
`target/release/arrowcol-loadgen validate`, with the saved run environment
injected.

For the frozen V2 snapshot, the retained `row-parity-verification.json` verifies that all
admitted rows are present, both pools have the same row-hash sets, and the 12
non-probe tables have exact row counts and hash-multiset parity. The
`active_sessions` table includes intentional admission/duplicate probes:
2,048 extra direct rows and 16,384 extra DFE rows. Therefore, qualification
**does not claim identical whole-pool row multiplicities**.

Validated changes are published only after the full independent output check
passes, and CI must be green at the published SHA. All 34 CI checks passed at
`81a97d1`; that evidence applies only to that revision, not the later lease-refresh
patch, incremental snapshot cache or publication-capacity waiting changes.
Continuation7's independent output verification passed as documented below;
green CI is still required at the later changes' own published SHA.

### Longer continuation failures and recovery

`memory-fair-20261014-v2-continuation3` **failed after approximately 35 minutes**
with direct routing pressure, despite its initially healthy progress. Direct
shutdown exposed an expired cached flush-job lease after waiting for a flush
slot, even though SQL control had renewed the actual lease. It left 42 buffered
receipts and five incomplete groups without Delta publication. DFE remained
around 249 MiB RSS (peak 305 MiB, up to 76 threads) with zero NACKs; this was not
a DFE collector failure.

The Direct fix refreshes the existing table lease after acquiring the flush
slot. The table-lease manager requires the same publication fence and a valid
renewal; it cannot acquire a new owner or epoch to authorize stale work.
Regression coverage verifies renewal after a blocked slot and rejection of
foreign owner, epoch, lineage and expired leases. Direct readiness also excludes
poisoned tables. Passing these regressions does not establish the follow-up
continuous qualification by itself: continuation7's finite resource evidence
and independent frozen-output verification are documented separately below.

Continuation4 did not qualify while the old generation's routing leases remained
draining. Recovery used the existing `RoutingStore.transfer_drained_lease` API
and normal eligibility/CAS/fencing checks, not manual SQL updates or deletion.

`memory-fair-20261014-v2-continuation5` subsequently **failed the memory gate
after approximately nine minutes** on retained histories: DFE reached 2,437 MiB
RSS with 70 threads, 3,555 buffered batches and roughly 2.147 GB logical Arrow
backlog; direct reached roughly 998 MiB RSS with a 1.4-million-row backlog. The
generator was stopped gracefully; collectors, failure evidence and retained
data were not deleted.

Both collectors' original global capacity was 2 GiB, not 4 GiB for DFE: its
backend was already shared across cores. Filling that permitted budget is
incompatible with a 512 MiB RSS gate under slow retained-history schema replay.
The matching 128 MiB runtime profile and incremental SDK schema-cache follow-up
subsequently passed continuation7's finite resource gates, not an indefinite
memory guarantee.

Continuation6 was stopped after nine DFE NACKs despite initially healthy memory.
Temporary publication-capacity exhaustion could reject work after best-effort
admission. DFE now waits asynchronously for temporary exporter capacity instead
of NACKing that work; oversized batches still reject immediately, and cancelled
partial reservations return their capacity. This does not turn DFE admission
into Direct's durable receipt contract.

### Continuation7 verified finite resource and frozen-output qualification

`memory-fair-20261014-v2-continuation7` **passed the strict resource qualification**
on October 14, 2026: **2,711.043 seconds (45 minutes 11.043 seconds), 272 samples**,
measured from **06:14:12.759 UTC to 06:59:23.802 UTC**. The workload remained all
13 tables, eight lanes and 1,024 rows per batch, with two CPUs per collector and
the shared process-global 128 MiB / per-table 32 MiB / per-flush 32 MiB profile.
Evidence:
`onelake/benchmark/memory-fair-20261014-v2/results/continuation7/memory-soak-verification.json`.

| Collector | Mean RSS (MiB) | Peak RSS (MiB) | Maximum threads | Exported rows/s |
| --- | ---: | ---: | ---: | ---: |
| Direct | 202.6447 | 222.4844 | 33 | 35,652.84 |
| DFE | 211.1987 | 223.8398 | 43 | 35,661.53 |

The resource qualification observed no NACK, publication failure, readiness
failure, restart or progress stall. Manifests recorded 94,355 paired logical
batches with zero retries; the sampled window also contained five unpaired
boundary entries. The separate full-run frozen-output verification below passed
for every admitted batch, including batches outside the sampled window.
Export rates are runtime-counter deltas, not independent Parquet row counts.
Both pools reuse retained histories; they are not identical fresh table pools,
and these measurements do not establish whole-history row-multiset equality.

DFE's early RSS median was 203.4219 MiB and late median 219.3008 MiB:
**+15.8789 MiB**, narrowly below the predeclared **16 MiB** growth gate. This is
not flat memory, proof of no leak or a guarantee for longer runs. Direct and DFE
remained below the finite RSS/thread gates in this observed interval only.

The SDK now uses a fresh kernel snapshot builder from the previous snapshot
for incremental replay. Its guarded optimization is limited to local Unix
JSON transaction logs: historical log fingerprints include length, modification
time, device, inode and change time. Unsupported cases or historical mutations
fall back to a full snapshot build; errors propagate rather than silently
serving stale state. This is not a general object-store freshness guarantee or
content-hash verification of Parquet files. Historical fingerprint checks still
scale **O(history)**; incremental schema replay does not make retained-history
metadata scanning constant-time.

**Independent continuation7 frozen-output verification PASSED with exit 0.**
After graceful generator stop, flushed manifests, terminal published Direct
receipts and both pools drained, the endpoint snapshot fixed the verification
boundary. The independent reader verified scoped receipt evidence, exact
regenerated SDK payloads and all canonical row-hash multiplicities in both
Delta outputs:

- **98,496,512 newly written rows per pool**, across all **13 schemas**.
- **96,188 logical batches and 96,188 published Direct receipts** for the full
  admitted run interval, not just the sampled resource window.
- **6,733 Direct and 6,886 DFE new Parquet files**, excluding historical active
  files; differing file layouts do not change verified row multiplicities.
- Exact append-only commit/file containment between independent per-table
  baseline and endpoint versions, with no reported validation failures.

Session-only evidence bundle: `continuation-interval-20261014T070246Z-951136`,
including `scope.json` and `scoped-containment-validation.json`.
The output verifier explicitly does not check qualification duration; the
separate 2,711.043-second resource report above supplies that evidence.
This establishes exact parity for the new C7 interval, not identical retained
whole-pool histories. The original 6,042-file validation remains distinct
historical evidence. No new publication or CI-success claim is made here.

A later continuation may run while that frozen interval is verified only after
the endpoint capture completes. The verifier binds endpoint versions, schemas
and file paths, excluding later continuation files. Preserve those frozen files:
no vacuum, overwrite, removal or schema evolution during verification.

Failed continuation3 logs, samples and PID metadata remain under
`onelake/benchmark/memory-fair-20261014-v2/results/continuation3/`:
`generator.log`, `sampler.log`, `resource-samples.jsonl` and `continuation.json`.
Do not signal the failed run's historical PID. For a running continuation, set
`CONTINUATION_RESULTS` to its own results directory, read its `generatorPid`,
confirm it still identifies the generator, then stop it gracefully:

```bash
GENERATOR_PID=$(node -p 'JSON.parse(require("node:fs").readFileSync(require("node:path").join(process.argv[1], "continuation.json"), "utf8")).generatorPid' "${CONTINUATION_RESULTS:?Set the active continuation results directory}")
kill -INT "$GENERATOR_PID"
```

The helper then stops its owned sampler; collectors and retained data stay live.

To sample an already-retained stack independently (no stack mutations):

```bash
npx --no-install tsx projects/loadtest/src/resource-sampler.ts \
  --project rust-playground-loadtest-soak-20261014 \
  --results onelake/benchmark/soak-20261014/results \
  --direct-admin http://10.89.0.21:8081 \
  --dataflow-admin http://10.89.0.31:32011
```

Use the actual addresses from `router-state/network.json` and run from a network
that can reach the collectors. Stop this process with SIGTERM/SIGINT. The runner
adds `--owner-pid <parent PID>`; omit it for a standalone sampler.
