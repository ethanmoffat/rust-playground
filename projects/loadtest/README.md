# loadtest

```bash
npx nx run loadtest:run -- --run-id soak-20261014 --warmup 2m --concurrency 8 --batch-rows 1024
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
frozen Delta roots **passed with exit 0** using the unchanged release reader:
13 schemas, complete rows, paired parity, receipts, routing, capabilities and
6,042 Delta files. All 43,753 direct durable receipts are terminal published.
The validator was built with
`cargo build --locked --release --package arrowcol-loadgen` and run with
`target/release/arrowcol-loadgen validate`, with the saved run environment
injected.

The retained `row-parity-verification.json` additionally verifies that all
admitted rows are present, both pools have the same row-hash sets, and the 12
non-probe tables have exact row counts and hash-multiset parity. The
`active_sessions` table includes intentional admission/duplicate probes:
2,048 extra direct rows and 16,384 extra DFE rows. Therefore, qualification
**does not claim identical whole-pool row multiplicities**.

Validated changes are published only after the full independent output check
passes, and CI must be green at the published SHA.

The generator-only continuation uses the new logical run ID
`memory-fair-20261014-v2-continuation3`, the existing debug load generator and
unchanged workload settings. It preserves the V2 collectors, policies,
historical run directories and SQL volumes rather than restarting this harness
with the old ID. Continuation health was verified over five samples spanning
40 seconds: both collectors returned readiness HTTP 200 with zero restarts,
direct exports increased by 1,327,104 rows and DFE exports by 1,308,672 rows,
with no NACKs or failures. DFE had 73 threads and 274.4 MiB RSS, below the
512 MiB gate. This short live-progress check is separate from the completed
finite soak and independent output validation.

Persistent continuation logs, samples and PID metadata are under
`onelake/benchmark/memory-fair-20261014-v2/results/continuation3/`:
`generator.log`, `sampler.log`, `resource-samples.jsonl` and `continuation.json`.
Read the current `generatorPid` from that metadata rather than hard-coding a
PID, confirm it still identifies the generator, then stop it gracefully:

```bash
GENERATOR_PID=$(node -p 'JSON.parse(require("node:fs").readFileSync("onelake/benchmark/memory-fair-20261014-v2/results/continuation3/continuation.json", "utf8")).generatorPid')
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
