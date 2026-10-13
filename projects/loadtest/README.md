# loadtest

```bash
npx nx run loadtest:run -- --warmup 2m --concurrency 8 --batch-rows 1024
```

Watch:

- Grafana: http://localhost:3000/d/arrowcol
- Logs: `docker compose -p rust-playground-loadtest-latest logs -f`

Results: `onelake/benchmark/latest/results/`

Stop: `docker compose -p rust-playground-loadtest-latest down -v`
