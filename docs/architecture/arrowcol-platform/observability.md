# Metrics and dashboards

[Architecture index](README.md) | [Load generator operations](../loadtest/README.md)

Metrics combine low-cardinality process observations with SQL-derived durable
state. Receipt IDs and lease UUIDs are **not** metric labels.

| Group | Observations |
| --- | --- |
| Router | Decisions/failures, active streams, cached leases/events, maximum collector pressure |
| Direct process | Streams, decoder bytes, received batches/rows/bytes, control failures, reconciled receipts, incomplete shutdown groups |
| DFE | Channel occupancy, downstream Ack/Nack, pending commits, Delta files/rows/bytes/failures, delivery-guarantee truth |
| SQL receipts | State/count/rows/Arrow bytes, published totals, failures, creation-to-publication and flushing-to-publication latency |
| SQL collectors/tables | Pressure, snapshot age, routing owner, lease age, time to expiry |

DFE advertises
`arrowcol_delivery_guarantee_info{delivery_guarantee="best_effort",...}` with
`receipt_polling="false"` and `at_least_once="false"`. Internal Ack/Nack metrics
are not a per-delivery client purge API.

## Dashboard separation

| Grafana folder | Contents |
| --- | --- |
| **Arrow Flight** (home dashboard) | Router, direct receipts, direct-vs-DFE throughput/pressure, Delta files, schema state, resources, logs, traces |
| **OTEL Collector Baseline** | Optional OTEL collector DFE engine, resources, logs, traces only |

The optional OTEL baseline is never mixed into arbitrary-Arrow receipt/delivery
panels. For finite/continuous runs, retained results, Grafana access, validation,
and chaos commands, use the [load generator guide](../loadtest/README.md).

Sources: [direct metrics](../../../projects/arrowcol-rust/src/metrics.rs),
[DFE metrics/capabilities](../../../crates/arrowcol-dataflow-components/src/metrics.rs),
[router metrics](../../../projects/arrowcol-router-rust/src/metrics.rs).
