# loadtest

```bash
npx nx run loadtest:run -- --warmup 2m --concurrency 8 --batch-rows 1024
```

This starts a fresh SQL Server, Hive, router, Direct collector, Dataflow
collector, and monitoring stack, then sends the same Arrow batches to both
collectors across all 13 tables.

**Each start replaces the previous loadtest. Its SQL volume, Delta tables,
and results are deleted.** Other Docker projects and workspace files are left
alone. You do not need to choose a name or clean up an earlier test yourself.

The test runs until **Ctrl+C**. Stopping the generator leaves the stack running
so you can inspect the charts and output; the next start resets it.

## Options

| Option | Meaning | Default |
| --- | --- | --- |
| `--warmup` | Warmup before measurement | `30s` |
| `--concurrency` | Concurrent sending lanes | `4` |
| `--batch-rows` | Rows per Arrow batch | `8192` |
| `--measure` | Stop after a finite measurement, such as `5m` | Until Ctrl+C |
| `--validate` | Check stored output after the generator finishes | Off |

Both collectors use two CPUs and matching buffer limits: 128 MiB overall,
32 MiB per table, and 32 MiB per flush. SQL Server defaults to a 16 GiB memory
budget. Keep host memory available for the other services.

To change the SQL budget (in MiB):

```bash
SQLSERVER_MEMORY_LIMIT_MB=8192 npx nx run loadtest:run -- --warmup 2m --concurrency 8 --batch-rows 1024
```

## Watch and inspect

- **Grafana:** http://localhost:3000/d/arrowcol (`admin` / `admin`).
- **SQL Server:** `localhost,14333`, username `sa`. The local password is printed
  at startup and stored in `projects/arrowcol-infra/secrets/sqlserver-sa-password.txt`.
- **Portainer:** https://localhost:9444, username `admin`, the SQL Server password.
  The local certificate is self-signed.
- **Logs:** `docker logs -f rust-playground-loadtest-latest-dataflow-1-1`
  (use `direct-1-1` for the Direct collector).

Use `SQLSERVER_PORT`, `GRAFANA_PORT`, or `PORTAINER_PORT` to override host ports.
Do not share startup logs containing local-development passwords.

Output is always under `onelake/benchmark/latest/`:

```text
direct/    Direct Delta tables
dataflow/  Dataflow Delta tables
results/   Metrics, manifests, diagnostics, and validation output
```

The runner prints the absolute Delta paths at startup. It samples memory,
threads, readiness, and row counters while the generator is running.
