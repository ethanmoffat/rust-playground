# loadtest

```bash
npx nx run loadtest:run -- --warmup 2m --concurrency 8 --batch-rows 1024
```

This starts a fresh SQL Server, Hive, router, Direct collector, Dataflow
collector, Kusto emulator, and monitoring stack, then sends the same Arrow
batches to both collectors across all 13 tables. Kusto mirrors the committed
output into native tables for live functional inspection.

**Each start replaces the previous loadtest. Its SQL, Hive, and Kusto volumes,
Delta tables, and results are deleted.** Other Docker projects and workspace
files are left alone. You do not need to choose a name or clean up an earlier
test yourself.

The test runs until **Ctrl+C**. Stopping the generator leaves the stack running
so you can inspect the charts and output; the next start resets it.

## Options

| Option | Meaning | Default |
| --- | --- | --- |
| `--warmup` | Warmup before measurement | `30s` |
| `--concurrency` | Concurrent sending lanes | `4` |
| `--batch-rows` | Rows per Arrow batch | `8192` |
| `--measure` | Stop after a finite measurement, such as `5m` | Until Ctrl+C |
| `--validate` | Check Delta snapshots and native Kusto data after the generator finishes | Off |

Both collectors use two CPUs and matching buffer limits: 128 MiB overall,
32 MiB per table, and 32 MiB per flush. SQL Server defaults to a 16 GiB memory
budget. The Kusto container has a separate 4 GiB memory limit. Keep host memory
available for the other services.

To change the SQL budget (in MiB):

```bash
SQLSERVER_MEMORY_LIMIT_MB=8192 npx nx run loadtest:run -- --warmup 2m --concurrency 8 --batch-rows 1024
```

## Watch and inspect

- **Grafana:** <http://localhost:3000/d/arrowcol> (`admin` / `admin`).
- **SQL Server:** `localhost,14333`, username `sa`. The local password is printed
  at startup and stored in `projects/arrowcol-infra/secrets/sqlserver-sa-password.txt`.
- **Portainer:** <https://localhost:9444>, username `admin`, the SQL Server password.
  The local certificate is self-signed.
- **Kusto:** <http://localhost:8080>, database `loadtest`, no authentication.
  This is an HTTP query endpoint, not a browser UI.
- **Logs:** `docker logs -f rust-playground-loadtest-latest-dataflow-1-1`
  (use `direct-1-1` for the Direct collector).

Use `SQLSERVER_PORT`, `GRAFANA_PORT`, `PORTAINER_PORT`, or `KUSTO_PORT` to override
host ports. Published ports bind to host loopback only. Inside the devcontainer,
the runner uses the emulator's allocated `10.89.x.50:8080` address instead of
`localhost`; the host connection remains `http://localhost:${KUSTO_PORT:-8080}`.
Do not share startup logs containing local-development passwords.

Output is always under `onelake/benchmark/latest/`:

```text
direct/    Direct Delta tables
dataflow/  Dataflow Delta tables
results/   Metrics, manifests, diagnostics, and validation output
```

The runner prints the absolute Delta paths at startup. It samples memory,
threads, readiness, and row counters while the generator is running.

## Live Kusto inspection

The emulator requires Linux/amd64 Docker, a CPU supporting SSE4.2/AVX2, and
sufficient host memory. Its first image download is over 1 GB compressed. The
Compose service pins an immutable digest of Microsoft's documented
`mcr.microsoft.com/azuredataexplorer/kustainer-linux:latest` image and leaves its
startup command unchanged. It configures `ACCEPT_EULA=Y`; running this target
therefore accepts the [Microsoft emulator license terms][emulator-install].
The emulator supports HTTP only, without Microsoft Entra authentication.

The database is `loadtest`. Each of the 13 SQL table names appears in both pools:
`direct_<sqlserver_table>` and `dataflow_<sqlserver_table>`, for 26 native Kusto
tables. For example:

```kusto
direct_sqlserver_wait_stats
| take 5
```

Committed Parquet files become queryable incrementally while generation runs.
Tables may be pending until their first Delta commit; readiness of the cluster
does not establish completeness of the mirror. Only the current fixed-schema,
append-only workload is supported. Schema changes or removal of previously
ingested active files degrade synchronization rather than silently leaving
stale tables. Timestamp `__unix_nanos` companion columns retain the original
integer nanoseconds; Kusto `datetime` alone is not nanosecond-exact.

To inspect all native-table counts from the Docker host:

```bash
curl --fail-with-body --silent --show-error \
  "http://localhost:${KUSTO_PORT:-8080}/v2/rest/query" \
  -H 'Content-Type: application/json' \
  -d '{"db":"loadtest","csl":"union withsource=TableName direct_*, dataflow_* | summarize Rows=count() by TableName | order by TableName asc"}'
```

Kusto.Explorer can also connect to `http://localhost:8080` with unsafe HTTP
connections enabled and `AAD Federated Security=True` removed from its
connection string. No desktop client, Azure subscription, or cloud credentials
are required.

### Failures, validation, and retained data

Kusto starts and is probed separately from the mandatory SQL/Hive/collector
startup. The readiness probe is `POST /v1/rest/mgmt` with
`{"csl":".show cluster"}`, without requiring the database to exist yet.
An image, startup, readiness, or synchronization failure does **not** stop
ordinary collector load generation. The runner prints an explicit
`Kusto DEGRADED` outcome rather than claiming the full integration succeeded;
an ordinary run may still exit successfully.

`--validate` and `loadtest:e2e` require real post-run KQL correctness assertions
for all 26 tables and an in-run live-query observation. They fail if Kusto is
unavailable, incomplete, inconsistent, or unverifiable. Counts are compared with
each pool's captured Delta snapshot, not with acknowledgements or an assumption
that the two pools have equal physical row multiplicities.

Run the focused verification suite with:

```bash
npx nx run loadtest:verify
```

Its functional E2E uses 1,024-row batches while retaining all 13 schemas and all
26 native-table assertions. Ordinary `loadtest:run` defaults are unchanged.

Inspect these files under `onelake/benchmark/latest/results/`:

| Artifact | Meaning |
| --- | --- |
| `kusto-startup.json` | Independent, sanitized emulator start/probe outcome |
| `kusto-status.json` | Rust synchronization state, live visibility, file count, and any error |
| `kusto-ingestions.jsonl` | Committed-file ingestion and replay evidence |
| `kusto-queries.jsonl` | Executed KQL and query results, including live observations |
| `kusto-validation.json` | Final native-table correctness assertions |
| `provenance.json` | Image identities and sanitized effective resource configuration |

Startup diagnostics remain separate if synchronization subsequently recovers.
Missing or malformed final synchronization status is degraded, never assumed
healthy. Save evidence outside `onelake/benchmark/` before another run resets it.

The running emulator and native data remain available after finite runs and
Ctrl+C. Its owned `kusto-data` volume is mounted at `/kustodata`; the persistent
database uses `/kustodata/dbs/loadtest/{md,data}`. Only the Direct and Dataflow
output roots are exposed at `/delta/direct` and `/delta/dataflow`, both read-only.
The next loadtest start removes the owned volume after checking its project,
name, and workspace identity; it does not clean up other Docker projects.

Override the emulator memory limit and published port when needed:

```bash
KUSTO_MEMORY_LIMIT=4g KUSTO_PORT=18080 npx nx run loadtest:run -- --measure 5m --validate
```

### Functional inspection only

[Microsoft's emulator license prohibits benchmark tests][emulator-overview].
Do not measure or publish Kusto emulator throughput or latency; it is not a
production Azure Data Explorer performance proxy. This integration exercises
native local-file ingestion and query correctness only, not managed/queued or
streaming ingestion. Live mirroring consumes shared host resources, so collector
measurements with this stack must not be described as unaffected by Kusto.
Its configured resource limits are retained in provenance.

[emulator-install]: https://learn.microsoft.com/en-us/azure/data-explorer/kusto-emulator-install
[emulator-overview]: https://learn.microsoft.com/en-us/azure/data-explorer/kusto-emulator-overview
