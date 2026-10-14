# Arrowcol DFE components

`arrowcol-dataflow-components` links two external OTAP Dataflow Engine (DFE)
nodes:

- `receiver:arrow_flight`
- `exporter:arrow_deltalake`

The receiver uses `arrowcol-flight` for Arrow Flight transport, strict catalog
validation, nullable-append alignment, bounded decoding, Flight SQL DDL, and
graceful drain. Each validated `RecordBatch` is moved into
`PdataArrowRecordSet` as representation `arrowcol.flight@1.0`. No Arrow IPC,
protobuf, or value-buffer copy occurs. DFE fan-out clones share Arrow arrays.
The current pluggable-pdata API still requires an OTLP `SignalType`; Arrowcol
uses `Logs` only as an engine discriminator and advertises
`representation_local_not_otap`. The exporter never interprets the batch as an
OTAP logs layout.

The representation-local schema ID carries the logical table, source and target
catalog schema versions, routing lease ID, collector owner, and fencing token.
The schema fingerprint is recomputed before export. The generic Delta exporter
accepts only this pluggable representation and sends the original arbitrary
batch to `arrow-deltalake`; it does not invoke OTAP schema preparation.

## Delivery semantics

Flight completion is **best-effort admission**. A successful final `PutResult`
means every batch entered its bounded DFE route. It does not mean Delta
publication completed.

- no SQL delivery receipt is created;
- receipt polling is not supported;
- no at-least-once claim is made; and
- downstream Ack/Nack feedback updates pressure and failure metrics after the
  Flight response may already have completed.

These statements are repeated by `/v1/capabilities`, `/readyz`, and the
`arrowcol_delivery_guarantee_info` metric.

## Fencing and ownership

The Flight admission hook reads the authoritative routing lease and rejects
non-owners before emitting pdata. Delta publication uses one process-shared
table owner and a commit-boundary `PublicationFence`. The synchronous Delta
fence sends a bounded request to a dedicated routing worker, which reads the
authoritative lease and verifies lease ID, holder, fencing epoch, and expiry.

The pressure document counts active Flight streams, Ack-tracked DFE batches,
and pending Delta commits per table. This lets router drain/reassignment wait
for the old owner to empty before a new owner publishes.

## Native Quiver gap

Native durable-buffer persistence is intentionally unavailable for this
representation. The completed DFE foundation returns:

```text
Quiver capability `persist` is unavailable for pdata representation `arrowcol.flight@1.0`
```

Setting `native_quiver: true` fails strict node validation with that exact
feature gap. Wiring `processor:durable_buffer` after the receiver also fails
DFE representation-contract validation. The component does not serialize the
batch into another format or invent a persistence layer.

## Public integration APIs

- `DfeAdmission` implements `arrowcol_flight::BatchAdmission`.
- `PdataSender` is the bounded DFE send seam used by focused tests.
- `StoreLeaseResolver` implements authoritative admission fencing.
- `ArrowcolBatchEnvelope`, `encode_record_batch`, and `decode_record_set`
  define the representation contract.
- `GenericDeltaExporter` owns arbitrary Arrow-to-Delta publication.
- `RuntimeMetrics` supplies registration, rich pressure, capability, and
  Prometheus snapshots.

## Validation

```bash
npx --no-install nx run arrowcol-dataflow-components:verify
```

The focused suite covers generated schemas, fingerprint validation, shared
Arrow ownership, channel backpressure, generic Delta output, stale fencing,
native Quiver rejection, capability honesty, and Ack-tracked pressure.
