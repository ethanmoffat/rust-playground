use arrowcol_catalog::{SchemaVersion, ValidatedIncomingSchema};
use arrowcol_protocol::{
    CollectorInstanceId, DeltaPublicationResult, ErrorStatus, ImplementationPool, LeaseId, Measurement,
    PublicationFence, ReceiptId, ReceiptState, ReceiptStatus, TableIdentity, TableLease, UnixMillis,
};
use uuid::Uuid;

use crate::{ReceiptError, ReceiptErrorKind, ReceiptResult};

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
    /// Returns an invalid-configuration error when a count exceeds the shared
    /// SQL/wire measurement range.
    pub fn new(
        validation: &ValidatedIncomingSchema,
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
        let registered = validation.registered();
        Ok(Self {
            table_id: registered.table_id(),
            table: registered.identity().clone(),
            schema_version: registered.version(),
            schema_fingerprint: registered.fingerprint(),
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
