# Pingora Arrow Flight routing spike

This standalone Cargo workspace proves header-only routing for Arrow Flight
`DoPut` over cleartext HTTP/2 (`h2c`).

## Confirmed protocol decision

Generated clients place the routing hint in gRPC request metadata before the
body:

```text
x-arrowcol-table: alpha
```

The proxy accepts at most 64 ASCII bytes matching `[A-Za-z0-9_.-]+`. It selects
a backend from the HTTP method, gRPC path, and request headers only. It never
decodes or inspects `FlightData`. The backend independently decodes the Flight
stream and checks the real Arrow schema, so the header is a routing hint rather
than schema or authorization evidence.

The driver also sends `x-arrowcol-expected-batches` to make an abruptly
truncated toy upload detectable. This is not used for routing.

## Contents

- `arrowcol-backend`: toy tonic Arrow Flight server. Two instances identify
  themselves in response metadata and every `PutResult`, decode one
  `RecordBatch` at a time, and retain only bounded counters plus decoder state.
- `arrowcol-proxy`: Pingora 0.9 proxy with downstream h2c enabled, bounded
  downstream H2 settings, and upstream `ALPN::H2` with
  `max_h2_streams = 64`.
- `arrowcol-driver`: starts two backends and the proxy, generates real Arrow
  record batches lazily, and runs routing, multiplexing, failure, cancellation,
  memory, and drain checks.
- `tests/e2e.rs`: ignored multiprocess wrapper around the quick driver profile.

The workspace is intentionally nested and self-contained. It does not require
changes to the repository's root workspace.

## Exact commands

Run from this directory:

```bash
cd projects/arrowcol-router-rust/spike

cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-targets

cargo build --locked --bins
cargo test --locked --test e2e -- --ignored --nocapture

# Larger qualification: 512 MiB logical upload in the memory case.
cargo run --locked --bin arrowcol-driver -- --profile full
```

The driver writes logs and event evidence below
`target/spike-evidence-<timestamp>-<pid>/`. It binds only ephemeral loopback
ports and needs no external service.

## What the driver proves

1. Large, multi-message Arrow `RecordBatch` streams for different routing keys
   reach only the selected backend.
2. Initial gRPC response metadata, streamed `PutResult.application_metadata`,
   successful `grpc-status: 0` trailers, and custom error trailer metadata
   survive the proxy.
3. Concurrent streams reuse and multiplex upstream H2 connections. The proxy's
   `connected_to_upstream` callback records `reused=true`; multiple concurrent
   streams for each backend use the same file descriptor.
4. An injected backend failure after several batches returns `ABORTED` and the
   backend identity carried in trailer metadata.
5. An unavailable backend fails promptly and does not leak the request to
   either live backend.
6. Killing a dedicated client process mid-upload produces a downstream reset at
   Pingora and an interrupted/truncated backend operation.
7. A tonic backend drains an active stream after `SIGTERM`.
8. Pingora drains an active downstream H2 stream during its configured
   three-second grace period before runtime teardown.

## Bounded-memory check

On Linux, the driver samples `/proc/<proxy-pid>/status` (`VmRSS`) every 10 ms
while logical payload volume grows. It compares a small and large upload and
requires:

- the large workload to be at least 8x the small workload;
- large-run RSS growth to stay below 96 MiB (`quick`) or 128 MiB (`full`);
- large-run growth not to exceed small-run growth by more than 64 MiB.

Representative local result on October 11, 2026, using the `full` profile:

```text
small logical_mib=32.0  baseline_mib=15.0  peak_mib=15.0  samples=41
large logical_mib=512.0 baseline_mib=15.0  peak_mib=15.0  samples=607
```

This is observational evidence, **not** proof of zero buffering. RSS includes
allocator and process effects, sampling can miss short spikes, Linux reports
page-granularity values, and the toy workload is not a production benchmark.
Production qualification still needs representative schemas, concurrency,
allocator/build settings, and sustained load.

## Findings and limitations

- **Routing is available before body forwarding.** Pingora selects the peer in
  `upstream_peer` from bounded metadata and does not parse Arrow payloads.
- **H2 multiplexing works.** Warmed alpha and beta connections are reused by
  concurrent streams while responses and trailers remain intact.
- **Do not retry `DoPut` automatically.** The spike deliberately does not mark
  connection/proxy errors retryable because a partially forwarded upload is not
  generally idempotent.
- **Cancellation has a Pingora 0.9 caveat.** The proxy records the downstream
  reset, but the upstream tonic service can observe either a failed response
  send or a clean early request EOF rather than a distinct transport
  cancellation. The toy `x-arrowcol-expected-batches` contract classifies the
  latter as `truncated_cancel`. Production must either establish an equivalent
  application-level completeness/commit contract or verify/fix upstream
  `RST_STREAM` propagation.
- **Pingora drain is time-budgeted, not request-aware.** Its public
  process lifecycle broadcasts shutdown, waits the configured grace period,
  then tears down runtimes. A stream exceeding that grace can be interrupted.
- **Error responses are not yet a gRPC gateway policy.** Generic Pingora
  routing/connect errors may become HTTP errors that tonic maps to a generic
  status. Production should emit canonical gRPC status/trailer responses.
- This spike uses h2c on loopback. It does not cover TLS, authentication,
  authorization, dynamic discovery, health checking, load balancing, or
  production telemetry.
- The metadata hint must be authenticated/authorized at the edge as needed, and
  the selected backend must continue to validate schema and tenancy.

## Production decision gate

**Decision:** GO for the header-routing protocol direction and a production
implementation phase; **NO-GO for deploying this spike as production code**.

Production promotion requires all of the following:

1. Resolve and test cancellation/truncation semantics without ambiguous partial
   commits.
2. Return canonical gRPC errors for invalid routes and unavailable backends.
3. Add TLS plus authentication/authorization for the routing hint.
4. Define idempotency, commit, and retry behavior for interrupted `DoPut`
   streams.
5. Re-run memory/backpressure and H2 saturation tests with representative Arrow
   schemas, concurrency, allocator, and deployment limits.
6. Set and observe a shutdown grace budget that covers the accepted maximum
   stream duration, or add explicit application-level drain coordination.
