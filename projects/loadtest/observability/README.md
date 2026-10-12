# Local LGTM observability overlay

This directory is a self-contained, version-and-digest-pinned local
Prometheus/Grafana/Loki/Tempo overlay for the Arrow Flight load test. It covers
the router, three direct collectors, three DFE collectors, the load driver,
SQL Server, Hive Metastore, and container metrics. The existing
`otelcol-rust` collector is an optional, separately provisioned baseline.

The overlay expects these main-stack service names and internal ports:

| Service | Expected name(s) | Metrics/readiness |
| --- | --- | --- |
| Router | `router` | `:50050/metrics`, `:50050/readyz` |
| Direct collectors | `direct-1` .. `direct-3` | `:8081/metrics`, `:8081/readyz` |
| DFE collectors | `dataflow-1` .. `dataflow-3` | `:32011/metrics`, `:32011/readyz`, capability/pressure JSON |
| Load driver | `load-driver` | cAdvisor container metrics and Docker logs |
| SQL / Hive | `sqlserver`, `hive-metastore` | TCP `1433`, TCP `9083` |
| Optional OTEL baseline | `otelcol-rust` | `:8080/api/v1/metrics` |

If the main stack chooses different service names or ports, update the static
targets in `prometheus/prometheus.yml`; do not add guessed aliases or guessed
metric names.

Overlay services reserve `10.89.0.100` through `10.89.0.108` on the main
loadtest subnet so dynamic allocation cannot claim the main stack's fixed
router/collector/SQL/Hive addresses.

## Start and URLs

The loadtest runner should pass both Compose files with the same project name:

```bash
docker compose \
  --project-name rust-playground-loadtest \
  --file projects/loadtest/compose.yaml \
  --file projects/loadtest/compose.observability.yaml \
  up --detach --wait
```

The predictable host endpoints are loopback-only:

- Grafana: `http://127.0.0.1:${GRAFANA_PORT:-3000}`
- Prometheus: `http://127.0.0.1:${PROMETHEUS_PORT:-9090}`

Grafana anonymous access is read-only. The local admin password defaults to
`arrowcol-local` and can be changed with `GRAFANA_ADMIN_PASSWORD`.

The benchmark runner's `LOADTEST_RUN_ID` is reused as the metrics/logs run
label. `LOADTEST_OBSERVABILITY_SCENARIO` defaults to `comparison`; it is a
bounded stack-level label, while the driver may execute direct, dataflow, and
paired phases within that run. Values are sanitized to low-cardinality
label-safe text.

## Provisioned folders and dashboards

### Arrow Flight

`Arrow Flight Platform & Collector Comparison` is the Grafana home dashboard.
It includes:

- logical rows/bytes, stream concurrency, and cAdvisor network bytes as an
  explicitly labeled wire-traffic proxy;
- router decisions/failures, SQL-backed table ownership, lease age/version
  churn, collector pressure inputs, and snapshot staleness;
- direct receipt state, publish latency, durable row/file totals, and failures;
- table buffer rows/bytes/age, observed bytes per Delta data file, and direct
  flushing-to-published latency;
- DFE channel occupancy/capacity, Ack/Nack rates, and capability truth;
- schema saga failures and schema compatibility state;
- container CPU/RSS/disk, dependency reachability, target health, logs, and
  traces when emitted.

The `Run`, `Scenario`, `Table`, `Implementation pool`, `Instance`, and
`Container service` variables use bounded `topk` queries. Receipt IDs and lease
UUIDs are never metric labels.

### OTEL Collector Baseline

`OTEL Collector Baseline` is provisioned in its own folder, not under Arrow
Flight. It shows only the optional collector's DFE engine metrics, container
resources, logs, and traces. A missing baseline target is expected when the
main runner does not request that service.

## Logs and traces

Grafana Alloy discovers only the named loadtest/observability Compose services
and forwards Docker logs to Loki. `run_id`, `scenario`, service, project,
container, stream, and bounded log level are labels. Trace/span IDs stay in log
content or structured metadata, not metric labels. Loki's derived TraceID field
links to Tempo.

Tempo accepts OTLP gRPC on `tempo:4317` and OTLP HTTP on `tempo:4318`. Services
that already emit OTLP traces can use, for example:

```text
OTEL_EXPORTER_OTLP_ENDPOINT=http://tempo:4318
```

No trace panels claim data for services that do not emit traces.

Alloy needs read-only access to `/var/run/docker.sock`. Docker socket access is
sensitive even when mounted read-only because the Docker API is powerful; use
this overlay only for local development.

## Retention and stack lifetime

- Prometheus scrape and rule evaluation intervals: 2 seconds.
- Prometheus TSDB retention: 6 hours and 4 GB.
- Loki log retention: 6 hours.
- Tempo trace retention override: 6 hours.
- Tempo-generated span metrics use native histograms where supported, and
  Prometheus accepts native histograms and converts classic histograms to
  native custom-bucket histograms.

Prometheus, Loki, Tempo, Grafana, and Alloy data directories are `tmpfs`
mounts. Data exists only while the Compose containers remain up. The main
runner should skip `docker compose down` only for its explicit `keep-stack`
mode; normal cleanup destroys the containers and therefore all observability
data. The overlay defines no persistent named volumes.

## Honest limitations

- **DFE receipts are unsupported.** DFE completion is best-effort admission;
  there is no SQL delivery receipt, receipt polling, or at-least-once claim.
- **Native Quiver for arbitrary Arrow is unsupported.** The DFE and direct
  implementations reject unsupported persistence rather than converting
  arbitrary Arrow to OTAP.
- **Direct flush reasons are not exported.** The dashboard has an explicit
  unsupported panel instead of inferring a reason from size or age.
- **Configured adaptive file targets are not exported.** The dashboard shows
  actual published bytes per data file only.
- **Exact routing weights are not exported.** The dashboard shows the maximum
  bounded pressure input and staleness used by routing.
- **Wire bytes are a proxy.** cAdvisor container receive bytes include
  protocol/header overhead.
- **SQL/Hive health is least-privilege reachability plus container metrics.**
  No server-state privilege is added to the application SQL principal.

cAdvisor is the only privileged overlay service. It uses the standard
read-only host root, `/var/run`, `/sys`, and Docker data mounts needed for
container cgroups/filesystem metrics. No other overlay service is privileged.

## Validation

Run every in-scope validation target from any directory:

```bash
projects/loadtest/observability/scripts/validate.sh
```

It validates YAML/JSON, pinned images, tmpfs retention, cAdvisor privilege
scope, unique dashboard UIDs/folders, Arrow/OTEL folder separation, home
dashboard selection, bounded variables, required panel coverage, source metric
contracts, absence of receipt-ID labels, Prometheus config/rules and dashboard
PromQL, Alloy/Loki/Tempo/exporter configs, and the merged Compose config.

`metrics-contract.json` points at the checked-in router/direct/DFE metric
definitions. A renamed or removed expected service metric fails validation
instead of silently leaving a dashboard backed by invented data.
