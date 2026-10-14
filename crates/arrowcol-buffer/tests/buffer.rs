use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::array::{ArrayRef, Int64Array, RecordBatch};
use arrow::datatypes::SchemaRef;
use arrow_deltalake::CompletedFileObservation;
use arrow_deltalake::record_batch_logical_bytes;
use arrow_deltalake::sizing::AdaptiveFileSizeConfig;
use arrowcol_buffer::{
    Admission, BufferConfig, BufferErrorKind, BufferManager, CapacityLimits, FlushReason, PublicationContext,
    PublicationOutcome, PublicationSession, PublicationSink, SinkError,
};
use arrowcol_catalog::{
    CatalogControlPlane, CatalogStore, DdlParser, FlightSqlDdlHandler, FlightSqlDdlRequest, MemoryCatalogStore,
    MemoryMetastore, SchemaValidator, ValidatedIncomingSchema,
};
use arrowcol_protocol::{
    ClaimedSchemaFingerprint, CollectorInstanceId, DeltaPublicationResult, FencingToken, ImplementationPool, LeaseId,
    Measurement, ReceiptId, ReceiptState, TableLease, UnixMillis, fingerprint_schema,
};
use arrowcol_receipts::{
    FailureDisposition, OwnerDrain, OwnerRegistration, PublicationBatch, ReceiptError, ReceiptErrorKind,
    ReceiptFailure, ReceiptRecord, ReceiptResult, ReceiptStore, ReceiptVersion, ReconciliationReport,
    TableLeaseRequest, ValidatedDelivery,
};
use async_trait::async_trait;
use tokio::sync::{Mutex as AsyncMutex, Notify};

#[derive(Clone, Copy)]
enum ReceiptFailurePoint {
    None = 0,
    Buffer = 1,
    Flush = 2,
    Publish = 3,
    Fail = 4,
}

#[derive(Default)]
struct FakeReceipts {
    records: AsyncMutex<HashMap<ReceiptId, ReceiptRecord>>,
    publications: AsyncMutex<Vec<Vec<ReceiptId>>>,
    failure: AtomicU8,
}

impl FakeReceipts {
    async fn seed(&self, record: ReceiptRecord) {
        self.records.lock().await.insert(record.receipt_id, record);
    }

    fn fail_next(&self, point: ReceiptFailurePoint) {
        self.failure.store(point as u8, Ordering::Release);
    }

    fn take_failure(&self, point: ReceiptFailurePoint) -> ReceiptResult<()> {
        if self
            .failure
            .compare_exchange(
                point as u8,
                ReceiptFailurePoint::None as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            Err(ReceiptError::new(
                ReceiptErrorKind::Database,
                "injected receipt-store failure",
            ))
        } else {
            Ok(())
        }
    }

    async fn transition(
        &self,
        receipts: &[ReceiptVersion],
        from: ReceiptState,
        to: ReceiptState,
        deadline: UnixMillis,
    ) -> ReceiptResult<Vec<ReceiptRecord>> {
        let mut records = self.records.lock().await;
        let mut next = Vec::with_capacity(receipts.len());
        for expected in receipts {
            let record = records
                .get_mut(&expected.receipt_id)
                .ok_or_else(|| ReceiptError::new(ReceiptErrorKind::NotFound, "fake receipt missing"))?;
            if record.revision != expected.revision || record.state != from {
                return Err(ReceiptError::new(
                    ReceiptErrorKind::Conflict,
                    "fake optimistic transition conflict",
                ));
            }
            record.state = to;
            record.revision += 1;
            record.lease_deadline_unix_ms = deadline;
            record.heartbeat_at_unix_ms =
                now().map_err(|cause| ReceiptError::new(ReceiptErrorKind::CorruptState, cause.to_string()))?;
            record.updated_at_unix_ms = record.heartbeat_at_unix_ms;
            next.push(record.clone());
        }
        Ok(next)
    }
}

#[async_trait]
impl ReceiptStore for FakeReceipts {
    async fn register_owner(&self, _registration: OwnerRegistration) -> ReceiptResult<()> {
        Ok(())
    }

    async fn heartbeat_owner(&self, _owner: CollectorInstanceId, _lease_deadline: UnixMillis) -> ReceiptResult<()> {
        Ok(())
    }

    async fn start_owner_drain(&self, _owner: CollectorInstanceId) -> ReceiptResult<OwnerDrain> {
        Ok(OwnerDrain {
            active_receipts: 0,
            active_table_leases: 0,
        })
    }

    async fn finish_owner_drain(&self, _owner: CollectorInstanceId, _stopped_at: UnixMillis) -> ReceiptResult<()> {
        Ok(())
    }

    async fn acquire_table_lease(&self, _request: TableLeaseRequest) -> ReceiptResult<TableLease> {
        Err(ReceiptError::new(
            ReceiptErrorKind::InvalidConfiguration,
            "fake lease acquisition is unused",
        ))
    }

    async fn renew_table_lease(&self, _lease: &TableLease, _expires_at: UnixMillis) -> ReceiptResult<TableLease> {
        Err(ReceiptError::new(
            ReceiptErrorKind::InvalidConfiguration,
            "fake lease renewal is unused",
        ))
    }

    async fn release_table_lease(&self, _lease: &TableLease, _released_at: UnixMillis) -> ReceiptResult<()> {
        Ok(())
    }

    async fn create_flight_batch(
        &self,
        _owner: CollectorInstanceId,
        _lease: &TableLease,
        _lease_deadline: UnixMillis,
        _deliveries: &[ValidatedDelivery],
    ) -> ReceiptResult<Vec<ReceiptRecord>> {
        Err(ReceiptError::new(
            ReceiptErrorKind::InvalidConfiguration,
            "fake creation is unused",
        ))
    }

    async fn mark_buffered(
        &self,
        _owner: CollectorInstanceId,
        receipts: &[ReceiptVersion],
        lease_deadline: UnixMillis,
    ) -> ReceiptResult<Vec<ReceiptRecord>> {
        self.take_failure(ReceiptFailurePoint::Buffer)?;
        self.transition(receipts, ReceiptState::Accepted, ReceiptState::Buffered, lease_deadline)
            .await
    }

    async fn mark_flushing(
        &self,
        _owner: CollectorInstanceId,
        receipts: &[ReceiptVersion],
        lease_deadline: UnixMillis,
    ) -> ReceiptResult<Vec<ReceiptRecord>> {
        self.take_failure(ReceiptFailurePoint::Flush)?;
        self.transition(
            receipts,
            ReceiptState::Buffered,
            ReceiptState::Publishing,
            lease_deadline,
        )
        .await
    }

    async fn heartbeat_receipts(
        &self,
        _owner: CollectorInstanceId,
        _receipts: &[ReceiptVersion],
        _lease_deadline: UnixMillis,
    ) -> ReceiptResult<()> {
        Ok(())
    }

    async fn publish_commit(
        &self,
        _owner: CollectorInstanceId,
        batch: &PublicationBatch,
    ) -> ReceiptResult<Vec<ReceiptRecord>> {
        self.take_failure(ReceiptFailurePoint::Publish)?;
        let mut records = self.records.lock().await;
        let mut published = Vec::with_capacity(batch.receipts.len());
        for expected in &batch.receipts {
            let record = records
                .get_mut(&expected.receipt_id)
                .ok_or_else(|| ReceiptError::new(ReceiptErrorKind::NotFound, "fake receipt missing"))?;
            if record.revision != expected.revision || record.state != ReceiptState::Publishing {
                return Err(ReceiptError::new(
                    ReceiptErrorKind::Conflict,
                    "fake publication conflict",
                ));
            }
            record.state = ReceiptState::Published;
            record.revision += 1;
            record.publication = Some(batch.publication.clone());
            record.updated_at_unix_ms = batch.publication.published_at_unix_ms();
            published.push(record.clone());
        }
        self.publications
            .lock()
            .await
            .push(batch.receipts.iter().map(|receipt| receipt.receipt_id).collect());
        Ok(published)
    }

    async fn fail_batch(
        &self,
        _owner: CollectorInstanceId,
        receipts: &[ReceiptVersion],
        failure: &ReceiptFailure,
        failed_at: UnixMillis,
    ) -> ReceiptResult<Vec<ReceiptRecord>> {
        self.take_failure(ReceiptFailurePoint::Fail)?;
        let mut records = self.records.lock().await;
        let mut failed = Vec::with_capacity(receipts.len());
        for expected in receipts {
            let record = records
                .get_mut(&expected.receipt_id)
                .ok_or_else(|| ReceiptError::new(ReceiptErrorKind::NotFound, "fake receipt missing"))?;
            if record.revision != expected.revision {
                return Err(ReceiptError::new(ReceiptErrorKind::Conflict, "fake failure conflict"));
            }
            record.state = if failure.error().code() == arrowcol_protocol::ErrorCode::PublicationOutcomeUnknown {
                ReceiptState::PublicationUnknown
            } else {
                ReceiptState::Failed
            };
            record.failure_disposition = Some(failure.disposition());
            record.error = Some(failure.error().clone().with_receipt_id(record.receipt_id));
            record.revision += 1;
            record.updated_at_unix_ms = failed_at;
            failed.push(record.clone());
        }
        Ok(failed)
    }

    async fn receipt(&self, receipt_id: ReceiptId) -> ReceiptResult<Option<ReceiptRecord>> {
        Ok(self.records.lock().await.get(&receipt_id).cloned())
    }

    async fn receipts(&self, receipt_ids: &[ReceiptId]) -> ReceiptResult<Vec<Option<ReceiptRecord>>> {
        let records = self.records.lock().await;
        Ok(receipt_ids.iter().map(|id| records.get(id).cloned()).collect())
    }

    async fn reconcile_expired(&self, _now: UnixMillis, _limit: usize) -> ReceiptResult<ReconciliationReport> {
        Ok(ReconciliationReport {
            reconciled: Vec::new(),
            limit_reached: false,
        })
    }
}

#[derive(Clone)]
enum SinkBehavior {
    Success { parquet_divisor: u64 },
    Fail(SinkError),
    Block { started: Arc<Notify>, release: Arc<Notify> },
}

struct FakeSink {
    behavior: Mutex<VecDeque<SinkBehavior>>,
    leases: Mutex<HashMap<arrowcol_protocol::TableIdentity, TableLease>>,
    reasons: Arc<AsyncMutex<Vec<FlushReason>>>,
    expected_array: Mutex<Option<ArrayRef>>,
    zero_copy_seen: Arc<AtomicU8>,
    version: Arc<AtomicU64>,
}

impl FakeSink {
    fn succeeding() -> Arc<Self> {
        Arc::new(Self {
            behavior: Mutex::new(VecDeque::new()),
            leases: Mutex::new(HashMap::new()),
            reasons: Arc::new(AsyncMutex::new(Vec::new())),
            expected_array: Mutex::new(None),
            zero_copy_seen: Arc::new(AtomicU8::new(0)),
            version: Arc::new(AtomicU64::new(0)),
        })
    }

    fn push(&self, behavior: SinkBehavior) -> anyhow::Result<()> {
        self.behavior
            .lock()
            .map_err(|_| anyhow::anyhow!("fake sink behavior lock poisoned"))?
            .push_back(behavior);
        Ok(())
    }

    fn expect_array(&self, array: ArrayRef) -> anyhow::Result<()> {
        *self
            .expected_array
            .lock()
            .map_err(|_| anyhow::anyhow!("fake expected-array lock poisoned"))? = Some(array);
        Ok(())
    }
}

#[async_trait]
impl PublicationSink for FakeSink {
    async fn current_lease(
        &self,
        table: &arrowcol_protocol::TableIdentity,
        admitted: &TableLease,
    ) -> Result<TableLease, SinkError> {
        Ok(self
            .leases
            .lock()
            .map_err(|_| SinkError::fenced("fake lease cache lock poisoned"))?
            .get(table)
            .cloned()
            .unwrap_or_else(|| admitted.clone()))
    }

    async fn begin(&self, context: PublicationContext) -> Result<Box<dyn PublicationSession>, SinkError> {
        self.reasons.lock().await.push(context.flush_reason);
        let behavior = self
            .behavior
            .lock()
            .map_err(|_| SinkError::retryable("fake sink behavior lock poisoned"))?
            .pop_front()
            .unwrap_or(SinkBehavior::Success { parquet_divisor: 2 });
        let expected_array = self
            .expected_array
            .lock()
            .map_err(|_| SinkError::retryable("fake expected-array lock poisoned"))?
            .take();
        Ok(Box::new(FakeSession {
            behavior,
            context,
            expected_array,
            zero_copy_seen: Arc::clone(&self.zero_copy_seen),
            version: Arc::clone(&self.version),
            rows: 0,
            bytes: 0,
        }))
    }
}

struct FakeSession {
    behavior: SinkBehavior,
    context: PublicationContext,
    expected_array: Option<ArrayRef>,
    zero_copy_seen: Arc<AtomicU8>,
    version: Arc<AtomicU64>,
    rows: usize,
    bytes: u64,
}

#[async_trait]
impl PublicationSession for FakeSession {
    async fn write(&mut self, batch: &RecordBatch) -> Result<(), SinkError> {
        if let Some(expected) = &self.expected_array
            && Arc::ptr_eq(expected, batch.column(0))
        {
            self.zero_copy_seen.store(1, Ordering::Release);
        }
        self.rows += batch.num_rows();
        self.bytes += record_batch_logical_bytes(batch)
            .map_err(|cause| SinkError::retryable(format!("measure fake batch: {cause:#}")))?;
        Ok(())
    }

    async fn finish(self: Box<Self>) -> Result<PublicationOutcome, SinkError> {
        let divisor = match &self.behavior {
            SinkBehavior::Success { parquet_divisor } => *parquet_divisor,
            SinkBehavior::Fail(error) => return Err(error.clone()),
            SinkBehavior::Block { started, release } => {
                started.notify_waiters();
                release.notified().await;
                2
            }
        };
        let version = self.version.fetch_add(1, Ordering::AcqRel) + 1;
        let parquet_bytes = (self.bytes / divisor.max(1)).max(1);
        let observation = CompletedFileObservation {
            rows: self.rows,
            input_bytes: self.bytes,
            parquet_bytes,
        };
        Ok(PublicationOutcome {
            publication: DeltaPublicationResult::new(
                Measurement::new(version).map_err(|cause| SinkError::publication_unknown(cause.to_string()))?,
                Measurement::new(1).map_err(|cause| SinkError::publication_unknown(cause.to_string()))?,
                Measurement::new(
                    u64::try_from(self.rows)
                        .map_err(|_| SinkError::publication_unknown("fake row count exceeds u64"))?,
                )
                .map_err(|cause| SinkError::publication_unknown(cause.to_string()))?,
                Measurement::new(parquet_bytes).map_err(|cause| SinkError::publication_unknown(cause.to_string()))?,
                self.context.schema_fingerprint,
                now().map_err(|cause| SinkError::publication_unknown(cause.to_string()))?,
                self.context.fence,
            ),
            file_observation: observation,
        })
    }
}

#[tokio::test]
async fn refreshes_renewed_lease_after_waiting_for_a_flush_slot() -> anyhow::Result<()> {
    let first = validation("lease_blocker").await?;
    let queued = validation("lease_waiter").await?;
    let first_lease = lease(first.registered().identity())?;
    let renewed = lease(queued.registered().identity())?;
    let admitted = TableLease::new(
        renewed.lease_id(),
        renewed.table().clone(),
        renewed.holder_instance_id(),
        renewed.implementation_pool(),
        renewed.fencing_token(),
        renewed.issued_at_unix_ms(),
        UnixMillis::new(now()?.get() + 1_000)?,
    )?;
    let receipts = Arc::new(FakeReceipts::default());
    let sink = FakeSink::succeeding();
    sink.leases
        .lock()
        .map_err(|_| anyhow::anyhow!("fake lease cache lock poisoned"))?
        .insert(renewed.table().clone(), renewed);
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    sink.push(SinkBehavior::Block {
        started: Arc::clone(&started),
        release: Arc::clone(&release),
    })?;
    let mut config = config_large();
    config.max_concurrent_flushes = 1;
    config.max_rows_per_flush = 1;
    let manager = manager(config, Arc::clone(&receipts), Arc::clone(&sink))?;
    let (blocker, _) = admission(&first, &first_lease, &[1], &receipts).await?;
    manager.admit(blocker).await?;
    tokio::time::timeout(Duration::from_secs(2), started.notified()).await?;
    let (waiting, _) = admission(&queued, &admitted, &[2], &receipts).await?;
    let receipt_id = waiting.receipt.receipt_id;
    manager.admit(waiting).await?;
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    assert!(!admitted.is_active_at(now()?));
    release.notify_waiters();
    let report = tokio::time::timeout(Duration::from_secs(3), manager.shutdown()).await??;
    assert!(report.incomplete.is_empty());
    assert_eq!(
        receipts
            .receipt(receipt_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("queued receipt is missing"))?
            .state,
        ReceiptState::Published
    );
    assert_eq!(report.pressure.global.bytes, 0);
    assert!(report.pressure.tables.iter().all(|table| !table.poisoned));
    Ok(())
}

#[tokio::test]
async fn lease_refresh_cannot_rebind_queued_work_to_new_ownership() -> anyhow::Result<()> {
    for change in ["owner", "epoch", "lineage", "expired"] {
        let validation = validation(&format!("lease_change_{change}")).await?;
        let admitted = lease(validation.registered().identity())?;
        let replacement = TableLease::new(
            if change == "lineage" {
                LeaseId::new()
            } else {
                admitted.lease_id()
            },
            admitted.table().clone(),
            if change == "owner" {
                CollectorInstanceId::new()
            } else {
                admitted.holder_instance_id()
            },
            admitted.implementation_pool(),
            FencingToken::new(if change == "epoch" { 2 } else { 1 })?,
            admitted.issued_at_unix_ms(),
            if change == "expired" {
                UnixMillis::new(now()?.get() - 1)?
            } else {
                admitted.expires_at_unix_ms()
            },
        )?;
        let receipts = Arc::new(FakeReceipts::default());
        let sink = FakeSink::succeeding();
        sink.leases
            .lock()
            .map_err(|_| anyhow::anyhow!("fake lease cache lock poisoned"))?
            .insert(admitted.table().clone(), replacement);
        let mut config = config_large();
        config.max_rows_per_flush = 1;
        let manager = manager(config, Arc::clone(&receipts), Arc::clone(&sink))?;
        let (delivery, _) = admission(&validation, &admitted, &[1], &receipts).await?;
        manager.admit(delivery).await?;
        let report = manager.shutdown().await?;
        assert!(!report.incomplete.is_empty(), "{change}");
        assert!(sink.reasons.lock().await.is_empty(), "{change}");
        assert!(report.pressure.tables.iter().any(|table| table.poisoned), "{change}");
        assert_eq!(report.pressure.global.bytes, 0, "{change}");
    }
    Ok(())
}

#[tokio::test]
async fn retains_arrow_arrays_by_reference_and_groups_receipts() -> anyhow::Result<()> {
    let validation = validation("zero_copy").await?;
    let lease = lease(validation.registered().identity())?;
    let receipts = Arc::new(FakeReceipts::default());
    let sink = FakeSink::succeeding();
    let (first, first_array) = admission(&validation, &lease, &[1, 2], &receipts).await?;
    sink.expect_array(first_array)?;
    let (second, _) = admission(&validation, &lease, &[3, 4], &receipts).await?;
    let manager = manager(config_large(), Arc::clone(&receipts), Arc::clone(&sink))?;

    manager.admit(first).await?;
    manager.admit(second).await?;
    manager.drain().await?;

    assert_eq!(sink.zero_copy_seen.load(Ordering::Acquire), 1);
    let publications = receipts.publications.lock().await;
    assert_eq!(publications.len(), 1);
    assert_eq!(publications[0].len(), 2);
    assert_eq!(manager.pressure()?.global.bytes, 0);
    Ok(())
}

#[tokio::test]
async fn capacity_backpressure_waits_and_times_out() -> anyhow::Result<()> {
    let validation = validation("capacity").await?;
    let lease = lease(validation.registered().identity())?;
    let receipts = Arc::new(FakeReceipts::default());
    let sink = FakeSink::succeeding();
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    sink.push(SinkBehavior::Block {
        started: Arc::clone(&started),
        release: Arc::clone(&release),
    })?;
    let mut config = config_large();
    config.global_capacity.rows = 1;
    config.per_table_capacity.rows = 1;
    config.max_rows_per_flush = 1;
    let manager = manager(config, Arc::clone(&receipts), Arc::clone(&sink))?;
    let (first, _) = admission(&validation, &lease, &[1], &receipts).await?;
    manager.admit(first).await?;
    started.notified().await;
    let pressure = manager.pressure()?;
    assert_eq!(pressure.global.rows, 1);
    assert_eq!(pressure.active_flushes, 1);

    let (second, _) = admission(&validation, &lease, &[2], &receipts).await?;
    let Err(error) = manager.admit_with_timeout(second, Duration::from_millis(20)).await else {
        anyhow::bail!("second admission must time out while capacity is retained");
    };
    assert_eq!(error.kind(), BufferErrorKind::BackpressureTimeout);
    release.notify_waiters();
    let report = manager.shutdown().await?;
    assert_eq!(report.pressure.global.rows, 0);
    Ok(())
}

#[tokio::test]
async fn all_threshold_and_age_flush_reasons_are_reported() -> anyhow::Result<()> {
    assert_trigger(FlushReason::AdaptiveCompressedSize, Trigger::Adaptive).await?;
    assert_trigger(FlushReason::ArrowMemory, Trigger::Memory).await?;
    assert_trigger(FlushReason::Rows, Trigger::Rows).await?;
    assert_trigger(FlushReason::Batches, Trigger::Batches).await?;
    assert_trigger(FlushReason::MaxAge, Trigger::Age).await?;
    Ok(())
}

#[tokio::test]
async fn schema_boundary_isolates_registered_versions() -> anyhow::Result<()> {
    let (historical, current) = historical_and_current("schema_boundary").await?;
    let lease = lease(historical.registered().identity())?;
    let receipts = Arc::new(FakeReceipts::default());
    let sink = FakeSink::succeeding();
    let manager = manager(config_large(), Arc::clone(&receipts), Arc::clone(&sink))?;
    let (old, _) = admission(&historical, &lease, &[1], &receipts).await?;
    let (new, _) = admission(&current, &lease, &[1, 2], &receipts).await?;
    manager.admit(old).await?;
    manager.admit(new).await?;
    manager.drain().await?;
    let reasons = sink.reasons.lock().await.clone();
    assert_eq!(reasons[0], FlushReason::SchemaBoundary);
    assert_eq!(reasons[1], FlushReason::ExplicitDrain);
    Ok(())
}

#[tokio::test]
async fn adaptive_observation_changes_the_next_trigger() -> anyhow::Result<()> {
    let validation = validation("adaptive").await?;
    let lease = lease(validation.registered().identity())?;
    let receipts = Arc::new(FakeReceipts::default());
    let sink = FakeSink::succeeding();
    sink.push(SinkBehavior::Success { parquet_divisor: 4 })?;
    sink.push(SinkBehavior::Success { parquet_divisor: 4 })?;
    let mut config = config_large();
    config.adaptive_size = adaptive(16, 16, 1_024, 1.0, 0.5);
    let manager = manager(config, Arc::clone(&receipts), Arc::clone(&sink))?;
    let (first, _) = admission(&validation, &lease, &[1, 2], &receipts).await?;
    manager.admit(first).await?;
    wait_for_reason(&sink, 1).await;
    let (second, _) = admission(&validation, &lease, &[3, 4], &receipts).await?;
    manager.admit(second).await?;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(sink.reasons.lock().await.len(), 1);
    manager.drain().await?;
    assert_eq!(
        sink.reasons.lock().await.as_slice(),
        [FlushReason::AdaptiveCompressedSize, FlushReason::ExplicitDrain]
    );
    Ok(())
}

#[tokio::test]
async fn receipt_failure_before_and_after_publication_is_visible() -> anyhow::Result<()> {
    let validation = validation("sql_failure").await?;
    let lease = lease(validation.registered().identity())?;
    let receipts = Arc::new(FakeReceipts::default());
    let sink = FakeSink::succeeding();
    let mut config = config_large();
    config.max_batches_per_flush = 1;
    config.adaptive_size = adaptive(10_000, 10_000, 20_000, 1.0, 0.2);
    let manager = manager(config, Arc::clone(&receipts), Arc::clone(&sink))?;
    receipts.fail_next(ReceiptFailurePoint::Publish);
    let (delivery, _) = admission(&validation, &lease, &[1], &receipts).await?;
    manager.admit(delivery).await?;
    let report = manager.shutdown().await?;
    assert_eq!(report.incomplete.len(), 1);
    assert!(report.incomplete[0].durable_publication.is_some());
    assert_eq!(report.pressure.global.bytes, 0);
    Ok(())
}

#[tokio::test]
async fn sink_failure_stale_fence_and_cancellation_have_typed_behavior() -> anyhow::Result<()> {
    let validation = validation("sink_failures").await?;
    let lease = lease(validation.registered().identity())?;

    let retry_receipts = Arc::new(FakeReceipts::default());
    let retry_sink = FakeSink::succeeding();
    retry_sink.push(SinkBehavior::Fail(SinkError::retryable("known write failure")))?;
    let mut config = config_large();
    config.max_batches_per_flush = 1;
    config.adaptive_size = adaptive(10_000, 10_000, 20_000, 1.0, 0.2);
    let retry_manager = manager(config.clone(), Arc::clone(&retry_receipts), Arc::clone(&retry_sink))?;
    let (retry, _) = admission(&validation, &lease, &[1], &retry_receipts).await?;
    let retry_id = retry.receipt.receipt_id;
    retry_manager.admit(retry).await?;
    retry_manager.shutdown().await?;
    assert_eq!(
        retry_receipts
            .receipt(retry_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("missing retry receipt"))?
            .failure_disposition,
        Some(FailureDisposition::Retryable)
    );

    let cancelled_receipts = Arc::new(FakeReceipts::default());
    let cancelled_sink = FakeSink::succeeding();
    cancelled_sink.push(SinkBehavior::Fail(SinkError::cancelled("shutdown cancellation")))?;
    let cancelled_manager = manager(
        config.clone(),
        Arc::clone(&cancelled_receipts),
        Arc::clone(&cancelled_sink),
    )?;
    let (cancelled, _) = admission(&validation, &lease, &[2], &cancelled_receipts).await?;
    cancelled_manager.admit(cancelled).await?;
    let cancelled_report = cancelled_manager.shutdown().await?;
    assert!(cancelled_report.incomplete.is_empty());

    let fenced_receipts = Arc::new(FakeReceipts::default());
    let fenced_sink = FakeSink::succeeding();
    fenced_sink.push(SinkBehavior::Fail(SinkError::fenced("stale SQL fencing epoch")))?;
    let fenced_manager = manager(config, Arc::clone(&fenced_receipts), Arc::clone(&fenced_sink))?;
    let (fenced, _) = admission(&validation, &lease, &[3], &fenced_receipts).await?;
    fenced_manager.admit(fenced).await?;
    tokio::time::sleep(Duration::from_millis(20)).await;
    let (rejected, _) = admission(&validation, &lease, &[4], &fenced_receipts).await?;
    let Err(error) = fenced_manager.admit(rejected).await else {
        anyhow::bail!("stale fencing must poison the table");
    };
    assert_eq!(error.kind(), BufferErrorKind::TablePoisoned);
    fenced_manager.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn explicit_drain_and_shutdown_stop_admission() -> anyhow::Result<()> {
    let validation = validation("drain").await?;
    let lease = lease(validation.registered().identity())?;
    let receipts = Arc::new(FakeReceipts::default());
    let sink = FakeSink::succeeding();
    let manager = manager(config_large(), Arc::clone(&receipts), Arc::clone(&sink))?;
    let (delivery, _) = admission(&validation, &lease, &[1, 2], &receipts).await?;
    manager.admit(delivery).await?;
    manager.drain().await?;
    assert_eq!(sink.reasons.lock().await.as_slice(), [FlushReason::ExplicitDrain]);
    let report = manager.shutdown().await?;
    assert_eq!(report.pressure.global.bytes, 0);
    let (late, _) = admission(&validation, &lease, &[3], &receipts).await?;
    let Err(error) = manager.admit(late).await else {
        anyhow::bail!("shutdown must stop admission");
    };
    assert_eq!(error.kind(), BufferErrorKind::AdmissionClosed);
    Ok(())
}

#[tokio::test]
async fn shutdown_flushes_queued_work_and_concurrency_is_bounded() -> anyhow::Result<()> {
    let first_validation = validation("bounded_first").await?;
    let second_validation = validation("bounded_second").await?;
    let first_lease = lease(first_validation.registered().identity())?;
    let second_lease = lease(second_validation.registered().identity())?;
    let receipts = Arc::new(FakeReceipts::default());
    let sink = FakeSink::succeeding();
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    sink.push(SinkBehavior::Block {
        started: Arc::clone(&started),
        release: Arc::clone(&release),
    })?;
    sink.push(SinkBehavior::Success { parquet_divisor: 2 })?;
    let mut config = config_large();
    config.max_concurrent_flushes = 1;
    config.max_batches_per_flush = 1;
    config.adaptive_size = adaptive(10_000, 10_000, 20_000, 1.0, 0.2);
    let bounded_manager = manager(config, Arc::clone(&receipts), Arc::clone(&sink))?;
    let (first, _) = admission(&first_validation, &first_lease, &[1], &receipts).await?;
    let (second, _) = admission(&second_validation, &second_lease, &[2], &receipts).await?;
    bounded_manager.admit(first).await?;
    started.notified().await;
    bounded_manager.admit(second).await?;
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(sink.reasons.lock().await.len(), 1);
    release.notify_waiters();
    bounded_manager.shutdown().await?;
    assert_eq!(sink.reasons.lock().await.len(), 2);

    let shutdown_receipts = Arc::new(FakeReceipts::default());
    let shutdown_sink = FakeSink::succeeding();
    let shutdown_manager = manager(
        config_large(),
        Arc::clone(&shutdown_receipts),
        Arc::clone(&shutdown_sink),
    )?;
    let (queued, _) = admission(&first_validation, &first_lease, &[3], &shutdown_receipts).await?;
    shutdown_manager.admit(queued).await?;
    shutdown_manager.shutdown().await?;
    assert_eq!(shutdown_sink.reasons.lock().await.as_slice(), [FlushReason::Shutdown]);
    Ok(())
}

enum Trigger {
    Adaptive,
    Memory,
    Rows,
    Batches,
    Age,
}

async fn assert_trigger(expected: FlushReason, trigger: Trigger) -> anyhow::Result<()> {
    let validation = validation(&format!("trigger_{expected:?}").to_lowercase()).await?;
    let lease = lease(validation.registered().identity())?;
    let receipts = Arc::new(FakeReceipts::default());
    let sink = FakeSink::succeeding();
    let mut config = config_large();
    config.adaptive_size = adaptive(10_000, 10_000, 20_000, 1.0, 0.2);
    match trigger {
        Trigger::Adaptive => config.adaptive_size = adaptive(8, 8, 1_024, 1.0, 0.2),
        Trigger::Memory => config.max_arrow_memory_per_flush = 8,
        Trigger::Rows => config.max_rows_per_flush = 1,
        Trigger::Batches => config.max_batches_per_flush = 1,
        Trigger::Age => {
            config.max_age = Duration::from_millis(15);
            config.age_check_interval = Duration::from_millis(2);
        }
    }
    let manager = manager(config, Arc::clone(&receipts), Arc::clone(&sink))?;
    let (delivery, _) = admission(&validation, &lease, &[1], &receipts).await?;
    manager.admit(delivery).await?;
    wait_for_reason(&sink, 1).await;
    assert_eq!(sink.reasons.lock().await[0], expected);
    manager.shutdown().await?;
    Ok(())
}

fn manager(
    config: BufferConfig,
    receipts: Arc<FakeReceipts>,
    sink: Arc<FakeSink>,
) -> anyhow::Result<Arc<BufferManager>> {
    Ok(BufferManager::new(receipts_config(config), receipts, sink)?)
}

fn receipts_config(config: BufferConfig) -> BufferConfig {
    config
}

fn config_large() -> BufferConfig {
    BufferConfig {
        global_capacity: CapacityLimits {
            bytes: 1_024,
            rows: 128,
            batches: 64,
        },
        per_table_capacity: CapacityLimits {
            bytes: 1_024,
            rows: 128,
            batches: 64,
        },
        max_arrow_memory_per_flush: 1_024,
        max_rows_per_flush: 128,
        max_batches_per_flush: 64,
        max_age: Duration::from_secs(60),
        age_check_interval: Duration::from_millis(5),
        max_concurrent_flushes: 2,
        receipt_lease_duration: Duration::from_secs(30),
        adaptive_size: adaptive(10_000, 10_000, 20_000, 1.0, 0.2),
    }
}

const fn adaptive(target: u64, minimum: u64, maximum: u64, cold_ratio: f64, weight: f64) -> AdaptiveFileSizeConfig {
    AdaptiveFileSizeConfig {
        target_parquet_bytes: target,
        min_input_bytes: minimum,
        max_input_bytes: maximum,
        cold_start_parquet_ratio: cold_ratio,
        ewma_weight: weight,
        min_parquet_ratio: 0.01,
        max_parquet_ratio: 4.0,
    }
}

async fn validation(name: &str) -> anyhow::Result<ValidatedIncomingSchema> {
    let store = Arc::new(MemoryCatalogStore::new());
    let service = CatalogControlPlane::new(
        DdlParser::default(),
        Arc::<MemoryCatalogStore>::clone(&store),
        Arc::new(MemoryMetastore::new()),
    );
    let completion = service
        .execute_ddl(FlightSqlDdlRequest::new(format!(
            "CREATE TABLE telemetry.buffer.{name} (value BIGINT NOT NULL)"
        )))
        .await?;
    let schema = store
        .current_schema(completion.identity())
        .await?
        .ok_or_else(|| anyhow::anyhow!("created schema missing"))?
        .arrow_schema()?;
    Ok(SchemaValidator::new(store)
        .validate(
            completion.identity(),
            ClaimedSchemaFingerprint::new(fingerprint_schema(&schema)),
            &schema,
        )
        .await?)
}

async fn historical_and_current(name: &str) -> anyhow::Result<(ValidatedIncomingSchema, ValidatedIncomingSchema)> {
    let store = Arc::new(MemoryCatalogStore::new());
    let service = CatalogControlPlane::new(
        DdlParser::default(),
        Arc::<MemoryCatalogStore>::clone(&store),
        Arc::new(MemoryMetastore::new()),
    );
    let created = service
        .execute_ddl(FlightSqlDdlRequest::new(format!(
            "CREATE TABLE telemetry.buffer.{name} (value BIGINT NOT NULL)"
        )))
        .await?;
    let historical = store
        .current_schema(created.identity())
        .await?
        .ok_or_else(|| anyhow::anyhow!("historical schema missing"))?;
    service
        .execute_ddl(FlightSqlDdlRequest::new(format!(
            "ALTER TABLE telemetry.buffer.{name} ADD COLUMNS (extra BIGINT)"
        )))
        .await?;
    let current = store
        .current_schema(created.identity())
        .await?
        .ok_or_else(|| anyhow::anyhow!("current schema missing"))?;
    let validator = SchemaValidator::new(store);
    let historical_arrow = historical.arrow_schema()?;
    let current_arrow = current.arrow_schema()?;
    Ok((
        validator
            .validate(
                created.identity(),
                ClaimedSchemaFingerprint::new(fingerprint_schema(&historical_arrow)),
                &historical_arrow,
            )
            .await?,
        validator
            .validate(
                created.identity(),
                ClaimedSchemaFingerprint::new(fingerprint_schema(&current_arrow)),
                &current_arrow,
            )
            .await?,
    ))
}

async fn admission(
    validation: &ValidatedIncomingSchema,
    lease: &TableLease,
    values: &[i64],
    receipts: &Arc<FakeReceipts>,
) -> anyhow::Result<(Admission, ArrayRef)> {
    let schema: SchemaRef = Arc::new(validation.registered().arrow_schema()?);
    let array: ArrayRef = Arc::new(Int64Array::from(values.to_vec()));
    let arrays = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, _)| {
            if index == 0 {
                Arc::clone(&array)
            } else {
                Arc::new(Int64Array::from(vec![None; values.len()])) as ArrayRef
            }
        })
        .collect();
    let batch = RecordBatch::try_new(schema, arrays)?;
    let bytes = record_batch_logical_bytes(&batch)?;
    let delivery = ValidatedDelivery::new(validation, u64::try_from(batch.num_rows())?, 1, bytes)?;
    let receipt_id = ReceiptId::new();
    let created = now()?;
    receipts
        .seed(ReceiptRecord {
            receipt_id,
            table_id: delivery.table_id(),
            table: delivery.table().clone(),
            implementation_pool: lease.implementation_pool(),
            owner_instance_id: lease.holder_instance_id(),
            lease_id: lease.lease_id(),
            fencing_epoch: lease.fencing_token(),
            schema_version: delivery.schema_version(),
            schema_fingerprint: delivery.schema_fingerprint(),
            state: ReceiptState::Accepted,
            failure_disposition: None,
            revision: 0,
            rows: delivery.rows(),
            batches: delivery.batches(),
            arrow_memory_bytes: delivery.arrow_memory_bytes(),
            lease_deadline_unix_ms: lease.expires_at_unix_ms(),
            heartbeat_at_unix_ms: created,
            created_at_unix_ms: created,
            updated_at_unix_ms: created,
            publication: None,
            error: None,
        })
        .await;
    Ok((
        Admission {
            receipt: ReceiptVersion::new(receipt_id, 0),
            delivery,
            lease: lease.clone(),
            batches: vec![batch],
        },
        array,
    ))
}

fn lease(table: &arrowcol_protocol::TableIdentity) -> anyhow::Result<TableLease> {
    let now = now()?;
    Ok(TableLease::new(
        LeaseId::new(),
        table.clone(),
        CollectorInstanceId::new(),
        ImplementationPool::Direct,
        FencingToken::new(1)?,
        UnixMillis::new(now.get().saturating_sub(1_000))?,
        UnixMillis::new(now.get().saturating_add(120_000))?,
    )?)
}

fn now() -> anyhow::Result<UnixMillis> {
    let millis = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    Ok(UnixMillis::new(u64::try_from(millis)?)?)
}

async fn wait_for_reason(sink: &FakeSink, count: usize) {
    for _ in 0..100 {
        if sink.reasons.lock().await.len() >= count {
            return;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}
