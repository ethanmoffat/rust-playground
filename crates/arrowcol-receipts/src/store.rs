use arrowcol_protocol::{CollectorInstanceId, ReceiptId, TableLease, UnixMillis};
use async_trait::async_trait;

use crate::{
    OwnerDrain, OwnerRegistration, PublicationBatch, ReceiptFailure, ReceiptRecord, ReceiptResult, ReceiptVersion,
    ReconciliationReport, TableLeaseRequest, ValidatedDelivery,
};

/// Durable receipt operations used by direct collector and buffer workers.
#[async_trait]
pub trait ReceiptStore: Send + Sync {
    /// Registers a process lifetime as active for receipt ownership.
    async fn register_owner(&self, registration: OwnerRegistration) -> ReceiptResult<()>;

    /// Extends an active owner lease.
    async fn heartbeat_owner(&self, owner: CollectorInstanceId, lease_deadline: UnixMillis) -> ReceiptResult<()>;

    /// Starts graceful drain and returns outstanding owned work.
    async fn start_owner_drain(&self, owner: CollectorInstanceId) -> ReceiptResult<OwnerDrain>;

    /// Marks a fully drained owner stopped and releases its table leases.
    async fn finish_owner_drain(&self, owner: CollectorInstanceId, stopped_at: UnixMillis) -> ReceiptResult<()>;

    /// Acquires a fresh table lease and monotonically advances its fence.
    async fn acquire_table_lease(&self, request: TableLeaseRequest) -> ReceiptResult<TableLease>;

    /// Extends one active table lease without changing its fencing epoch.
    async fn renew_table_lease(&self, lease: &TableLease, expires_at: UnixMillis) -> ReceiptResult<TableLease>;

    /// Releases one active table lease.
    async fn release_table_lease(&self, lease: &TableLease, released_at: UnixMillis) -> ReceiptResult<()>;

    /// Transactionally creates fresh server receipt IDs for validated streams.
    ///
    /// Duplicate metadata always creates independent receipts.
    async fn create_flight_batch(
        &self,
        owner: CollectorInstanceId,
        lease: &TableLease,
        lease_deadline: UnixMillis,
        deliveries: &[ValidatedDelivery],
    ) -> ReceiptResult<Vec<ReceiptRecord>>;

    /// Transactionally transitions accepted receipts into the bounded buffer.
    async fn mark_buffered(
        &self,
        owner: CollectorInstanceId,
        receipts: &[ReceiptVersion],
        lease_deadline: UnixMillis,
    ) -> ReceiptResult<Vec<ReceiptRecord>>;

    /// Transactionally transitions buffered receipts into a flush attempt.
    async fn mark_flushing(
        &self,
        owner: CollectorInstanceId,
        receipts: &[ReceiptVersion],
        lease_deadline: UnixMillis,
    ) -> ReceiptResult<Vec<ReceiptRecord>>;

    /// Extends in-process receipt ownership without changing state revision.
    async fn heartbeat_receipts(
        &self,
        owner: CollectorInstanceId,
        receipts: &[ReceiptVersion],
        lease_deadline: UnixMillis,
    ) -> ReceiptResult<()>;

    /// Atomically completes every receipt made visible by one Delta commit.
    async fn publish_commit(
        &self,
        owner: CollectorInstanceId,
        batch: &PublicationBatch,
    ) -> ReceiptResult<Vec<ReceiptRecord>>;

    /// Atomically records a structured retryable or permanent failure.
    async fn fail_batch(
        &self,
        owner: CollectorInstanceId,
        receipts: &[ReceiptVersion],
        failure: &ReceiptFailure,
        failed_at: UnixMillis,
    ) -> ReceiptResult<Vec<ReceiptRecord>>;

    /// Returns one receipt status.
    async fn receipt(&self, receipt_id: ReceiptId) -> ReceiptResult<Option<ReceiptRecord>>;

    /// Returns a bounded set of receipt statuses in request order.
    async fn receipts(&self, receipt_ids: &[ReceiptId]) -> ReceiptResult<Vec<Option<ReceiptRecord>>>;

    /// Reconciles expired in-process work without blind publication replay.
    ///
    /// Accepted or buffered work becomes retryable. Expired flushing work is
    /// recorded as publication-outcome-unknown and requires explicit operator
    /// or storage reconciliation before any resend.
    async fn reconcile_expired(&self, now: UnixMillis, limit: usize) -> ReceiptResult<ReconciliationReport>;
}
