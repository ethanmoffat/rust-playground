# loadtest

```bash
npx nx run loadtest:run -- --warmup 2m --concurrency 8 --batch-rows 1024
```

Watch:

- Grafana: http://localhost:3000/d/arrowcol
- SQL Server: `localhost,14333` (host port `14333` mapped to container port `1433`) — username `sa` and password printed at startup from `projects/arrowcol-infra/secrets/sqlserver-sa-password.txt`. Set `SQLSERVER_PORT` to override the host port. SQL data remains mounted in the project-scoped `sqlserver-data` volume at `/var/opt/mssql`; `down -v` removes it.
- Portainer (container logs, stats, shells): https://localhost:9444 — `admin` / the SQL Server SA password, printed at startup and stored in `projects/arrowcol-infra/secrets/sqlserver-sa-password.txt`. Self-signed TLS; set `PORTAINER_PORT` if 9444 is taken. The default is 9444 rather than 9443 because VS Code's port forwarder tends to claim 9443 on the Windows host. If it times out while Grafana works, VS Code has forwarded the port into the devcontainer: stop that forward in the Ports panel, then `docker restart rust-playground-loadtest-latest-portainer-1`.
- Logs: `docker compose -p rust-playground-loadtest-latest logs -f`

Results: `onelake/benchmark/latest/results/`

Printed credentials are for local development only; do not share startup logs
containing passwords.

Before generating load, the runner prints the absolute workspace paths to both
Delta table roots:

```text
Delta tables on disk (workspace paths)
-------------------------------------
  Direct:   /workspaces/rust-playground/onelake/benchmark/latest/direct
  Dataflow: /workspaces/rust-playground/onelake/benchmark/latest/dataflow
```

Paths reflect your workspace and selected run ID. Open these directories to inspect
the tables while the loadtest runs.

Stop: `docker compose -p rust-playground-loadtest-latest down -v`
