# arrowcol-sql-server

Opt-in, bounded SQL Server connection reuse using Deadpool managed pools and
Tiberius 0.13's existing Tokio TCP transport. Construct independent pool instances
for receipt, routing, and catalog budgets; clones share their instance's limit.
There is no background maintenance task, fallback connection, or mutation retry.

```rust,no_run
use std::time::Duration;
use arrowcol_sql_server::{ConnectionConfig, PoolConfig, SqlPool};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let connection = ConnectionConfig::new(
    "sql.example.internal", 1433, "application", "application_user", "secret",
);
let pool = SqlPool::new(connection, PoolConfig {
    max_size: 4,
    acquire_timeout: Duration::from_secs(5),
})?;
let mut client = pool.acquire().await?;
client.simple_query("SELECT 1").await?.into_results().await?;
client.finish().await?;
drop(client);
pool.close();
# Ok(())
# }
```

## Return and error contract

- Every checkout starts **dirty**. Errors, early returns, task cancellation, and
  timeouts drop/detach its client synchronously and recover Deadpool capacity.
  No asynchronous rollback, commit, cancellation, or mutation replay happens on
  drop. Closing the socket does not prove whether an uncertain mutation committed.
- Consume **every operation result** and complete the transaction before calling
  `finish()`. For pooled clients, successful finish verifies `@@TRANCOUNT` and `XACT_STATE()` are both zero
  and isolation has been reset to read committed. An active or doomed transaction
  is rejected, never silently rolled back. Reset errors/cancellation leave the
  checkout dirty. Any later mutable client access makes it dirty again.
- Tiberius does not expose pending-result state. A subsequent request flushes an
  abandoned stream's raw packets and can lose its server diagnostics. Therefore
  `finish()` is **not** an abandoned-stream recovery API and must not be called
  after an abandoned/failed/timed-out operation. It fully consumes its own reset
  response, not a caller's lost response. Store integrations must enforce this
  successful-operation precondition.
- A failure to reset after a confirmed commit is a cleanup failure, not evidence
  that the commit failed. Report the cleanup failure and discard without replaying
  the mutation; preserve the caller's domain success where it is already confirmed.
- Deadpool performs a bounded health/reset check before reusing an idle client.
  It discards failed health checks and attempts replacement before returning a
  checkout. Such replacement is not a retry of application work. Health failures
  are visible in discarded/created statistics; Deadpool does not return individual
  recycle diagnostics if replacement succeeds.
- This is intended for the shared stores' fixed SQL sessions. It resets isolation,
  not arbitrary application session changes (temporary tables, `SESSION_CONTEXT`,
  impersonation, `SET` options, etc.). Do not reuse sessions that make those changes.
- TLS encryption is required. Certificate trust defaults to verified. Credentials
  stay private, configuration equality includes credentials/TLS/timeouts, and
  diagnostic/debug rendering excludes or redacts credentials.

TCP plus TLS/login share one total connection deadline (default 15 seconds).
Driver per-response command bounds default to 30 seconds; session verification
also has a total 30-second bound. `with_timeouts(connect, command)` overrides both.
Acquisition has one total deadline across waiting, recycling, and creation.
Zero durations/limits and semaphore-overflow limits are rejected before IO.

`SqlConnection::connect(&config)` retains the fresh, unpooled path with the same
driver/transport and connection bounds. Its `finish()` is a no-op: unpooled
clients always close on drop, without extra session-validation queries.
`SqlPool::matches_config(&config)` checks
all connection identity/security/bound fields before store injection.

`close()` closes idle clients, wakes waiting acquisitions, and rejects future
acquisitions across all clones. Existing operations are not interrupted and their
clients close on guard drop. Drain application work before closing. Snapshot
`max_size` is zero after close, as reported by Deadpool.

## Statistics

`stats()` returns plain cumulative atomic counters `created`, `acquired`, `reused`,
`discarded`, `acquire_failures`, and `acquire_wait_seconds`, plus Deadpool's current
`size`, `available`, and `max_size`. Creation counts successful physical clients;
reuse counts successful checkouts of previously returned clients. Discard counts
all physical client drops, including shutdown. Acquisition failures/latency include
cancelled acquisitions. Latency includes creation/recycle, not just permit waiting.
Snapshots are not transactionally consistent and have no metrics dependency.

## Verification

```bash
cargo fmt --package arrowcol-sql-server -- --check
cargo clippy --locked --package arrowcol-sql-server --all-targets -- -D warnings
cargo test --locked --package arrowcol-sql-server
RUSTDOCFLAGS='-D warnings' cargo doc --locked --package arrowcol-sql-server --no-deps
```

Default tests exercise the real Deadpool managed lifecycle with deterministic
fake clients: clean reuse, dirty return, task abort, clone bounds, exhaustion,
creation/recycle failures, acquisition cancellation, permit recovery, and close.
A loopback-only stalled handshake checks the real TCP/login deadline.

Live tests are ignored and do not start/stop infrastructure or kill sessions.
Run only against an **isolated** SQL Server you own, never an active benchmark.
Use the repository's standard test environment: `ARROWCOL_SQLSERVER_TEST=1`,
explicit `ARROWCOL_SQLSERVER_HOST` and `ARROWCOL_SQLSERVER_PORT`, and
`ARROWCOL_APP_PASSWORD_FILE` (absolute or repository-relative). This uses database
`arrowcol`, login `arrowcol_app`, and trusts the isolated fixture's certificate.
The password is read without displaying it; trailing CR/LF characters are removed.

Optional overrides are `ARROWCOL_SQL_TEST_DATABASE`,
`ARROWCOL_SQL_TEST_USERNAME`, and `ARROWCOL_SQL_TEST_TRUST_CERT` (`0` to verify,
`1` to trust). Alternative independent-fixture aliases remain supported:
`ARROWCOL_SQL_TEST_ISOLATED=1`, `ARROWCOL_SQL_TEST_HOST`,
`ARROWCOL_SQL_TEST_PORT`, and `ARROWCOL_SQL_TEST_PASSWORD`. That alias mode
verifies certificates unless explicitly overridden. The catalog convention
`ARROWCOL_SQLSERVER_PASSWORD_FILE` is also accepted as a password-file fallback.

```bash
cargo test --locked --package arrowcol-sql-server live_ -- --ignored
```

These check real SPID reuse, serializable-isolation reset, rejection/discard of
active transactions, cancellation, command timeout, and subsequent permit/session
recovery. They issue only session-local transaction/diagnostic/WAITFOR statements.
