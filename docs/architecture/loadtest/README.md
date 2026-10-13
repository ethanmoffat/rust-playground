# Arrow load generator

[![Arrow load generator architecture](arrow-load-generator.png)](arrow-load-generator.html)

The load generator registers 13 table schemas through Flight SQL, exports the
immutable catalog artifacts, generates Rust and TypeScript SDKs, and builds
every `RecordBatch` from the generated schema. A deterministic scheduler sends
logical batches through the Pingora router to three direct collectors and three
DFE collectors.

In paired mode, both collector pools receive the same logical batch:

- the direct path polls each SQL-backed receipt until `published` or a
  structured terminal failure; and
- the DFE path records only `best_effort_admission`.

The two pools write separate Delta roots. The independent validator compares
schemas, rows, logical batch manifests, paired payload hashes, routing leases,
collector participation, and snapshot-selected Delta files.

## Prerequisites

Run from the repository root in the source devcontainer:

```bash
git submodule update --init --recursive
npm ci
```

The benchmark bootstraps its local SQL Server/Hive secrets automatically. It
uses source-built `:dev` images and does not pull the published GHCR images.

## Finite benchmark

Run direct, DFE, and paired phases, then validate and remove the scoped stack:

```bash
npx --no-install nx run loadtest:run -- \
  --warmup 2m \
  --measure 1m
```

Results are retained at:

```text
onelake/benchmark/bench-1/results/
```

Useful files include:

- `sent-batches.jsonl`: logical batch IDs, table, schema, rows, Arrow bytes,
  attempts, and outcomes;
- `receipts.jsonl`: direct receipt state and publication results;
- `routing.json`: collector registration and table ownership;
- `schema-contracts.json`: normalized generated schema evidence;
- `validation.json`: final Delta and parity result; and
- `compose.log` / `compose-ps.json`: retained diagnostics.

## Continuous comparison with Grafana

Run the requested live paired comparison:

```bash
npx --no-install nx run loadtest:run -- \
  --run-id live \
  --warmup 2m \
  --continuous \
  --keep-stack
```

Continuous mode:

- selects paired Arrow traffic;
- ignores `--measure`;
- runs until `Ctrl+C`;
- drains direct receipts before the client exits; and
- retains the stack because `--keep-stack` is set.

Open:

- Grafana: `http://127.0.0.1:3000`
- Prometheus: `http://127.0.0.1:9090`

Grafana provisions separate **Arrow Flight** and **OTEL Collector Baseline**
folders. Add `--otel-baseline` if you also want the OTEL workload and its
separate dashboard.

Stop the retained stack:

```bash
npx --no-install tsx projects/loadtest/src/main.ts \
  benchmark-down --run-id live
```

Remove retained loadtest output only after stopping the stack:

```bash
npx --no-install nx run loadtest:clean
```

## Focus a scenario

```bash
# Durable direct collector only.
npx --no-install nx run loadtest:run -- \
  --run-id direct-only \
  --warmup 30s \
  --measure 2m \
  --scenarios direct

# DFE collector only.
npx --no-install nx run loadtest:run -- \
  --run-id dfe-only \
  --warmup 30s \
  --measure 2m \
  --scenarios dataflow

# Identical logical batches sent concurrently to both pools.
npx --no-install nx run loadtest:run -- \
  --run-id paired \
  --warmup 30s \
  --measure 2m \
  --scenarios paired
```

## Tune load

```bash
npx --no-install nx run loadtest:run -- \
  --run-id tuned \
  --warmup 1m \
  --measure 5m \
  --lanes 8 \
  --batch-rows 16384 \
  --seed 20260923
```

Supported flags:

| Flag | Purpose |
| --- | --- |
| `--run-id` | Stable run, Compose project, output, and dashboard identity. |
| `--warmup` | Warmup duration; accepts seconds or `30s`, `2m`, `1h`. |
| `--measure` | Finite measurement duration. |
| `--capture-settle` | Delay before final capture/validation. |
| `--drain-timeout` | Bound for receipt and shutdown drain. |
| `--request-timeout` | Bound for each client operation. |
| `--continuous` | Run paired traffic until interrupted. |
| `--keep-stack` | Retain Compose and LGTM after the client stops. |
| `--lanes` | Concurrent scheduling lanes. |
| `--batch-rows` | Rows in each generated `RecordBatch`. |
| `--seed` | Deterministic table order and synthetic values. |
| `--scenarios` | Comma-separated `direct`, `dataflow`, and/or `paired`. |
| `--otel-baseline` | Start the separate OTEL workload and dashboard. |

CLI values override `LOADTEST_*` environment values, which override built-in
defaults.

## Full acceptance and chaos

Run the complete OTEL plus Arrow qualification:

```bash
npx --no-install nx run loadtest:e2e
```

Run the Arrow failure/recovery sequence directly:

```bash
npx --no-install nx run loadtest:chaos -- \
  --run-id chaos-1 \
  --warmup 0 \
  --measure 3s \
  --batch-rows 64
```

The chaos sequence covers owner kill, lease expiry, resend, stale publication
fencing, greater-generation restart, graceful collector drain, bounded SQL
outage, and SQL recovery.

For implementation and fixture details, see
[`projects/loadtest/README.md`](../../../projects/loadtest/README.md).
