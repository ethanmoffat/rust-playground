use std::collections::HashSet;

use arrowcol_catalog::{SchemaVersion, ValidatedIncomingSchema};
use arrowcol_protocol::{
    CollectorInstanceId, DeltaPublicationResult, ErrorStatus, ImplementationPool, LeaseId, Measurement,
    PublicationFence, ReceiptId, ReceiptState, ReceiptStatus, TableIdentity, TableLease, UnixMillis,
};
use uuid::Uuid;

use crate::{ReceiptError, ReceiptErrorKind, ReceiptResult};

/// Largest durable recovery window an owner may register: one day.
///
/// Matches the `CK_collector_instances_recovery_window` SQL constraint.
pub const MAX_DURABLE_RECOVERY_WINDOW_MS: u64 = 86_400_000;

/// Immutable delivery metadata admitted only after catalog schema validation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedDelivery {
    table_id: Uuid,
    table: TableIdentity,
    schema_version: SchemaVersion,
    schema_fingerprint: arrowcol_protocol::SchemaFingerprint,
    rows: Measurement,
    batches: Measurement,
    arrow_memory_bytes: Measurement,
}

impl ValidatedDelivery {
    /// Captures receipt metadata from a successfully validated incoming schema.
    ///
    /// # Errors
    ///
    /// Returns an invalid-configuration error for zero batches or when a count
    /// exceeds the shared SQL/wire measurement range.
    pub fn new(
        validation: &ValidatedIncomingSchema,
        rows: u64,
        batches: u64,
        arrow_memory_bytes: u64,
    ) -> ReceiptResult<Self> {
        let registered = validation.registered();
        Self::recovered(
            registered.table_id(),
            registered.identity().clone(),
            registered.version(),
            registered.fingerprint(),
            rows,
            batches,
            arrow_memory_bytes,
        )
    }

    /// Rebuilds a delivery whose schema was validated before the batch was
    /// persisted to the collector's local durable buffer.
    ///
    /// Use this only for crash replay of batches that were admitted through
    /// [`Self::new`]; persist the values returned by this type's getters.
    /// Receipt creation still requires the table, schema version, and
    /// fingerprint to be registered and to match the caller's table lease.
    ///
    /// # Errors
    ///
    /// Returns an invalid-configuration error for zero batches or counts outside
    /// the shared SQL/wire measurement range (same rules as `new`).
    pub fn recovered(
        table_id: Uuid,
        table: TableIdentity,
        schema_version: SchemaVersion,
        schema_fingerprint: arrowcol_protocol::SchemaFingerprint,
        rows: u64,
        batches: u64,
        arrow_memory_bytes: u64,
    ) -> ReceiptResult<Self> {
        if batches == 0 {
            return Err(ReceiptError::new(
                ReceiptErrorKind::InvalidConfiguration,
                "validated delivery must contain at least one record batch",
            ));
        }
        Ok(Self {
            table_id,
            table,
            schema_version,
            schema_fingerprint,
            rows: measurement(rows, "row count")?,
            batches: measurement(batches, "batch count")?,
            arrow_memory_bytes: measurement(arrow_memory_bytes, "Arrow memory byte count")?,
        })
    }

    /// Returns the deterministic catalog table identifier.
    #[must_use]
    pub const fn table_id(&self) -> Uuid {
        self.table_id
    }

    /// Returns the validated portable table identity.
    #[must_use]
    pub const fn table(&self) -> &TableIdentity {
        &self.table
    }

    /// Returns the exact registered incoming schema version.
    #[must_use]
    pub const fn schema_version(&self) -> SchemaVersion {
        self.schema_version
    }

    /// Returns the exact registered incoming schema fingerprint.
    #[must_use]
    pub const fn schema_fingerprint(&self) -> arrowcol_protocol::SchemaFingerprint {
        self.schema_fingerprint
    }

    /// Returns rows represented by the delivery.
    #[must_use]
    pub const fn rows(&self) -> Measurement {
        self.rows
    }

    /// Returns record batches represented by the delivery.
    #[must_use]
    pub const fn batches(&self) -> Measurement {
        self.batches
    }

    /// Returns logical Arrow bytes held by the in-memory buffer.
    #[must_use]
    pub const fn arrow_memory_bytes(&self) -> Measurement {
        self.arrow_memory_bytes
    }
}

/// Optimistic receipt identity and logical revision.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ReceiptVersion {
    /// Server-generated receipt identifier.
    pub receipt_id: ReceiptId,
    /// Expected append-only event/state revision.
    pub revision: u64,
}

impl ReceiptVersion {
    /// Creates an expected receipt revision.
    #[must_use]
    pub const fn new(receipt_id: ReceiptId, revision: u64) -> Self {
        Self { receipt_id, revision }
    }
}

/// Whether a known receipt failure invites a new delivery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FailureDisposition {
    /// The client may submit a new delivery with a new receipt.
    Retryable,
    /// Retrying without an external change is not useful.
    Permanent,
}

/// Validated structured failure for a receipt transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReceiptFailure {
    disposition: FailureDisposition,
    error: ErrorStatus,
}

impl ReceiptFailure {
    /// Creates a failure whose disposition agrees with the shared error code.
    ///
    /// # Errors
    ///
    /// Returns an invalid-configuration error on a retryability mismatch.
    pub fn new(disposition: FailureDisposition, error: ErrorStatus) -> ReceiptResult<Self> {
        let retryable = disposition == FailureDisposition::Retryable;
        if error.is_retryable() != retryable {
            return Err(ReceiptError::new(
                ReceiptErrorKind::InvalidConfiguration,
                "failure disposition does not match the protocol error retryability",
            ));
        }
        Ok(Self { disposition, error })
    }

    /// Returns retryable or permanent disposition.
    #[must_use]
    pub const fn disposition(&self) -> FailureDisposition {
        self.disposition
    }

    /// Returns the shared structured error payload.
    #[must_use]
    pub const fn error(&self) -> &ErrorStatus {
        &self.error
    }
}

/// One durable receipt and its publication/failure observations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReceiptRecord {
    /// Server-generated receipt identifier.
    pub receipt_id: ReceiptId,
    /// Deterministic catalog table identifier.
    pub table_id: Uuid,
    /// Portable table identity.
    pub table: TableIdentity,
    /// Collector implementation pool.
    pub implementation_pool: ImplementationPool,
    /// Current receipt owner.
    pub owner_instance_id: CollectorInstanceId,
    /// Lease lineage used to fence publication.
    pub lease_id: LeaseId,
    /// Monotonic table fencing epoch.
    pub fencing_epoch: arrowcol_protocol::FencingToken,
    /// Exact registered incoming schema version.
    pub schema_version: SchemaVersion,
    /// Exact registered incoming schema fingerprint.
    pub schema_fingerprint: arrowcol_protocol::SchemaFingerprint,
    /// Current shared receipt state.
    pub state: ReceiptState,
    /// Retryability of a failed or uncertain state.
    pub failure_disposition: Option<FailureDisposition>,
    /// Monotonic state/event revision.
    pub revision: u64,
    /// Rows in the accepted delivery.
    pub rows: Measurement,
    /// Record batches in the accepted delivery.
    pub batches: Measurement,
    /// Logical Arrow bytes admitted to memory.
    pub arrow_memory_bytes: Measurement,
    /// Exclusive deadline for recoverable in-process ownership.
    pub lease_deadline_unix_ms: UnixMillis,
    /// Last receipt lease heartbeat.
    pub heartbeat_at_unix_ms: UnixMillis,
    /// Receipt creation time.
    pub created_at_unix_ms: UnixMillis,
    /// Last state transition time.
    pub updated_at_unix_ms: UnixMillis,
    /// Durable Delta publication metadata.
    pub publication: Option<DeltaPublicationResult>,
    /// Structured known or uncertain failure.
    pub error: Option<ErrorStatus>,
}

impl ReceiptRecord {
    /// Returns whether a client may purge its source copy.
    #[must_use]
    pub fn can_client_purge(&self) -> bool {
        self.state == ReceiptState::Published
    }

    /// Rebuilds the shared protocol status from persisted transition data.
    ///
    /// # Errors
    ///
    /// Returns corrupt-state when the persisted revision, payload, or state
    /// cannot be produced by the protocol transition graph.
    pub fn protocol_status(&self) -> ReceiptResult<ReceiptStatus> {
        let mut status = ReceiptStatus::accepted(
            self.receipt_id,
            self.table.clone(),
            self.implementation_pool,
            self.created_at_unix_ms,
        );
        match self.state {
            ReceiptState::Accepted => {}
            ReceiptState::Buffered => {
                status = transition(&status, ReceiptState::Buffered, self.updated_at_unix_ms, None, None)?;
            }
            ReceiptState::Publishing => {
                status = transition(&status, ReceiptState::Buffered, self.created_at_unix_ms, None, None)?;
                status = transition(&status, ReceiptState::Publishing, self.updated_at_unix_ms, None, None)?;
            }
            ReceiptState::Published => {
                status = transition(&status, ReceiptState::Buffered, self.created_at_unix_ms, None, None)?;
                status = transition(&status, ReceiptState::Publishing, self.created_at_unix_ms, None, None)?;
                status = transition(
                    &status,
                    ReceiptState::Published,
                    self.updated_at_unix_ms,
                    self.publication.clone(),
                    None,
                )?;
            }
            ReceiptState::Failed | ReceiptState::PublicationUnknown => {
                let target = self.state;
                match self.revision {
                    1 => {}
                    2 => {
                        status = transition(&status, ReceiptState::Buffered, self.created_at_unix_ms, None, None)?;
                    }
                    3 => {
                        status = transition(&status, ReceiptState::Buffered, self.created_at_unix_ms, None, None)?;
                        status = transition(&status, ReceiptState::Publishing, self.created_at_unix_ms, None, None)?;
                    }
                    _ => {
                        return Err(ReceiptError::new(
                            ReceiptErrorKind::CorruptState,
                            "failed receipt has an unsupported transition revision",
                        ));
                    }
                }
                status = transition(&status, target, self.updated_at_unix_ms, None, self.error.clone())?;
            }
        }
        if status.revision().get() != self.revision {
            return Err(ReceiptError::new(
                ReceiptErrorKind::CorruptState,
                "persisted receipt revision disagrees with protocol transition history",
            ));
        }
        Ok(status)
    }

    /// Returns the protocol publication fence represented by persisted columns.
    ///
    /// # Errors
    ///
    /// Returns corrupt-state if persisted lease timestamps are invalid.
    pub fn publication_fence(&self) -> ReceiptResult<PublicationFence> {
        TableLease::new(
            self.lease_id,
            self.table.clone(),
            self.owner_instance_id,
            self.implementation_pool,
            self.fencing_epoch,
            self.created_at_unix_ms,
            self.lease_deadline_unix_ms,
        )
        .map(|lease| lease.publication_fence())
        .map_err(|cause| ReceiptError::new(ReceiptErrorKind::CorruptState, cause.to_string()))
    }
}

/// Owner process registration for receipt admission and graceful drain.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OwnerRegistration {
    /// Process-lifetime identifier.
    pub instance_id: CollectorInstanceId,
    /// Stable deployment/replica key.
    pub instance_key: String,
    /// Positive generation for that key.
    pub generation: u64,
    /// Optional host label.
    pub host_name: Option<String>,
    /// Optional build label.
    pub build_version: Option<String>,
    /// Registration time.
    pub started_at_unix_ms: UnixMillis,
    /// Exclusive owner lease deadline.
    pub lease_deadline_unix_ms: UnixMillis,
    /// Optional durable-buffer recovery window in milliseconds.
    ///
    /// `Some(window)` declares that the owner persists accepted batches and
    /// their receipt identities locally. Expiry reconciliation then skips the
    /// owner's active receipts while its owner lease is unexpired and for
    /// `window` milliseconds after that lease expires, so durable backlog may
    /// outlive receipt deadlines and a restarted same-key successor can adopt
    /// it with [`crate::ReceiptStore::adopt_receipts`]. `None` keeps
    /// receipt-deadline reconciliation. At most
    /// [`MAX_DURABLE_RECOVERY_WINDOW_MS`].
    pub durable_recovery_window_ms: Option<u64>,
}

/// Request to acquire a new monotonically fenced table lease.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableLeaseRequest {
    /// Deterministic catalog table identifier.
    pub table_id: Uuid,
    /// Portable identity that must resolve to the table identifier.
    pub table: TableIdentity,
    /// Requesting process.
    pub owner_instance_id: CollectorInstanceId,
    /// Requesting implementation pool.
    pub implementation_pool: ImplementationPool,
    /// Lease issue time.
    pub issued_at_unix_ms: UnixMillis,
    /// Exclusive lease expiry.
    pub expires_at_unix_ms: UnixMillis,
}

/// All receipts made visible by one durable Delta commit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicationBatch {
    /// Optimistic receipt revisions, all expected to be flushing.
    pub receipts: Vec<ReceiptVersion>,
    /// Shared durable Delta publication observation.
    pub publication: DeltaPublicationResult,
}

/// Result of starting or observing graceful owner drain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OwnerDrain {
    /// Active accepted, buffered, or flushing receipts still owned.
    pub active_receipts: u64,
    /// Active table leases still held by the owner.
    pub active_table_leases: u64,
}

/// Bounded expiry reconciliation result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReconciliationReport {
    /// Receipts atomically changed to retryable failure.
    pub reconciled: Vec<ReceiptRecord>,
    /// True when the caller's limit was filled and more work may remain.
    pub limit_reached: bool,
}

/// Last known identity of one receipt that a successor asks to adopt.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct AdoptionCandidate {
    /// Server-generated receipt identifier.
    pub receipt_id: ReceiptId,
    /// Owner expected to hold the receipt: a failed or stopped same-key
    /// predecessor, or the successor itself when rebinding to a newer lease.
    pub expected_owner: CollectorInstanceId,
    /// Expected active state: accepted, buffered, or publishing (SQL `flushing`).
    pub expected_state: ReceiptState,
    /// Expected revision, which must agree with the state (0, 1, or 2).
    pub expected_revision: u64,
}

impl AdoptionCandidate {
    /// Creates an optimistic adoption expectation.
    #[must_use]
    pub const fn new(
        receipt_id: ReceiptId,
        expected_owner: CollectorInstanceId,
        expected_state: ReceiptState,
        expected_revision: u64,
    ) -> Self {
        Self {
            receipt_id,
            expected_owner,
            expected_state,
            expected_revision,
        }
    }

    /// Expects the current owner, state, and revision of a durable record.
    #[must_use]
    pub fn from_record(record: &ReceiptRecord) -> Self {
        Self::new(
            record.receipt_id,
            record.owner_instance_id,
            record.state,
            record.revision,
        )
    }

    /// Returns whether `record` still has the expected owner, state, and revision.
    #[must_use]
    pub fn matches(&self, record: &ReceiptRecord) -> bool {
        record.receipt_id == self.receipt_id
            && record.owner_instance_id == self.expected_owner
            && record.state == self.expected_state
            && record.revision == self.expected_revision
    }
}

/// Request to rebind recovered active receipts to a successor's table lease.
///
/// See [`crate::ReceiptStore::adopt_receipts`] for the adoption rules.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReceiptAdoption {
    /// Registered successor that becomes the receipt owner.
    pub successor: CollectorInstanceId,
    /// Successor's current SQL table lease for the candidates' table.
    pub lease: TableLease,
    /// New exclusive receipt deadline, bounded by the owner and table leases.
    pub lease_deadline: UnixMillis,
    /// Adoption time recorded as the receipt heartbeat and update time.
    pub adopted_at: UnixMillis,
    /// Receipts to adopt, each with its optimistic expectation.
    pub receipts: Vec<AdoptionCandidate>,
}

impl ReceiptAdoption {
    /// Validates store-independent request invariants.
    ///
    /// # Errors
    ///
    /// Returns fenced when the lease is not held by the successor. Returns
    /// invalid configuration when the deadline does not follow `adopted_at`
    /// or outlives the table lease, or when a candidate is duplicated or does
    /// not expect accepted/0, buffered/1, or publishing/2.
    pub fn validate(&self) -> ReceiptResult<()> {
        if self.lease.holder_instance_id() != self.successor {
            return Err(ReceiptError::new(
                ReceiptErrorKind::Fenced,
                "adoption lease is not held by the successor owner",
            ));
        }
        if self.lease_deadline <= self.adopted_at {
            return Err(ReceiptError::new(
                ReceiptErrorKind::InvalidConfiguration,
                "receipt adoption deadline must follow the adoption time",
            ));
        }
        if self.lease_deadline > self.lease.expires_at_unix_ms() {
            return Err(ReceiptError::new(
                ReceiptErrorKind::InvalidConfiguration,
                "receipt lease cannot outlive its table lease",
            ));
        }
        let mut seen = HashSet::with_capacity(self.receipts.len());
        for candidate in &self.receipts {
            if active_revision(candidate.expected_state) != Some(candidate.expected_revision) {
                return Err(ReceiptError::new(
                    ReceiptErrorKind::InvalidConfiguration,
                    format!(
                        "adoption candidate {} must expect accepted/0, buffered/1, or publishing/2",
                        candidate.receipt_id
                    ),
                ));
            }
            if !seen.insert(candidate.receipt_id) {
                return Err(ReceiptError::new(
                    ReceiptErrorKind::InvalidConfiguration,
                    format!("adoption candidate {} is duplicated", candidate.receipt_id),
                ));
            }
        }
        Ok(())
    }
}

/// Candidate left unchanged by an adoption request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RejectedAdoption {
    /// Candidate receipt identifier.
    pub receipt_id: ReceiptId,
    /// Current durable record, or `None` when the receipt does not exist.
    pub current: Option<ReceiptRecord>,
}

/// Outcome of one atomic adoption request.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReceiptAdoptionReport {
    /// Receipts now owned by the successor, in request order.
    pub adopted: Vec<ReceiptRecord>,
    /// Candidates whose expectation, lease, or owner lineage did not hold, in
    /// request order.
    pub rejected: Vec<RejectedAdoption>,
}

const fn active_revision(state: ReceiptState) -> Option<u64> {
    match state {
        ReceiptState::Accepted => Some(0),
        ReceiptState::Buffered => Some(1),
        ReceiptState::Publishing => Some(2),
        ReceiptState::Published | ReceiptState::Failed | ReceiptState::PublicationUnknown => None,
    }
}

fn transition(
    status: &ReceiptStatus,
    target: ReceiptState,
    at: UnixMillis,
    publication: Option<DeltaPublicationResult>,
    error: Option<ErrorStatus>,
) -> ReceiptResult<ReceiptStatus> {
    status
        .transition_to(target, at, publication, error)
        .map_err(|cause| ReceiptError::new(ReceiptErrorKind::CorruptState, cause.to_string()))
}

fn measurement(value: u64, label: &str) -> ReceiptResult<Measurement> {
    Measurement::new(value).map_err(|cause| {
        ReceiptError::new(
            ReceiptErrorKind::InvalidConfiguration,
            format!("{label} is invalid: {cause}"),
        )
    })
}
