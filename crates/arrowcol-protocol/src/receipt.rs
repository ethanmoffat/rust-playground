use serde::{Deserialize, Deserializer, Serialize, de};
use thiserror::Error;

use crate::{
    ErrorCode, ErrorStatus, ImplementationPool, Measurement, ProtocolVersion, PublicationFence, ReceiptId,
    SchemaFingerprint, TableIdentity, UnixMillis,
};

/// Metadata proving the result of a successful fenced Delta publication.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DeltaPublicationResult {
    protocol_version: ProtocolVersion,
    delta_version: Measurement,
    data_files_added: Measurement,
    rows_published: Measurement,
    bytes_published: Measurement,
    schema_fingerprint: SchemaFingerprint,
    published_at_unix_ms: UnixMillis,
    fence: PublicationFence,
}

impl DeltaPublicationResult {
    /// Creates publication metadata for a successful Delta commit.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        delta_version: Measurement,
        data_files_added: Measurement,
        rows_published: Measurement,
        bytes_published: Measurement,
        schema_fingerprint: SchemaFingerprint,
        published_at_unix_ms: UnixMillis,
        fence: PublicationFence,
    ) -> Self {
        Self {
            protocol_version: ProtocolVersion::CURRENT,
            delta_version,
            data_files_added,
            rows_published,
            bytes_published,
            schema_fingerprint,
            published_at_unix_ms,
            fence,
        }
    }

    /// Validates that this publication belongs to a receipt's table.
    ///
    /// # Errors
    ///
    /// Returns [`PublicationResultError::TableMismatch`] when the fence names a
    /// different table.
    pub fn validate_for_table(&self, table: &TableIdentity) -> Result<(), PublicationResultError> {
        if self.fence.table() != table {
            return Err(PublicationResultError::TableMismatch);
        }
        Ok(())
    }

    /// Returns the publication payload version.
    #[must_use]
    pub const fn protocol_version(&self) -> ProtocolVersion {
        self.protocol_version
    }

    /// Returns the committed Delta table version.
    #[must_use]
    pub const fn delta_version(&self) -> Measurement {
        self.delta_version
    }

    /// Returns the number of data files added by the commit.
    #[must_use]
    pub const fn data_files_added(&self) -> Measurement {
        self.data_files_added
    }

    /// Returns the number of rows represented by the commit.
    #[must_use]
    pub const fn rows_published(&self) -> Measurement {
        self.rows_published
    }

    /// Returns the number of published data-file bytes.
    #[must_use]
    pub const fn bytes_published(&self) -> Measurement {
        self.bytes_published
    }

    /// Returns the computed schema fingerprint published by the server.
    #[must_use]
    pub const fn schema_fingerprint(&self) -> SchemaFingerprint {
        self.schema_fingerprint
    }

    /// Returns when publication completed.
    #[must_use]
    pub const fn published_at_unix_ms(&self) -> UnixMillis {
        self.published_at_unix_ms
    }

    /// Returns the lease fence checked for publication.
    #[must_use]
    pub const fn fence(&self) -> &PublicationFence {
        &self.fence
    }
}

/// Failure to associate publication metadata with a receipt.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum PublicationResultError {
    /// The publication fence names a different table.
    #[error("publication result table does not match receipt table")]
    TableMismatch,
}

/// State of one accepted delivery receipt.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReceiptState {
    /// The server admitted the delivery and created the receipt.
    Accepted,
    /// The delivery is held in a bounded table buffer.
    Buffered,
    /// Publication has begun and the outcome is not yet known.
    Publishing,
    /// Delta publication completed successfully.
    Published,
    /// The delivery failed with a known non-success outcome.
    Failed,
    /// Publication may have committed and requires reconciliation.
    PublicationUnknown,
}

impl ReceiptState {
    /// Validates a state transition.
    ///
    /// # Errors
    ///
    /// Returns [`ReceiptTransitionError::Invalid`] for an unsupported edge.
    pub const fn validate_transition(self, next: Self) -> Result<(), ReceiptTransitionError> {
        let valid = matches!(
            (self, next),
            (Self::Accepted, Self::Buffered | Self::Publishing | Self::Failed)
                | (Self::Buffered, Self::Publishing | Self::Failed)
                | (
                    Self::Publishing,
                    Self::Published | Self::Failed | Self::PublicationUnknown
                )
                | (Self::PublicationUnknown, Self::Published | Self::Failed)
        );
        if valid {
            Ok(())
        } else {
            Err(ReceiptTransitionError::Invalid { from: self, to: next })
        }
    }
}

/// Versioned status payload for one server-generated receipt.
///
/// Duplicate client payloads are independent deliveries and receive independent
/// receipt IDs. No field in this payload is a client idempotency key.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ReceiptStatus {
    protocol_version: ProtocolVersion,
    receipt_id: ReceiptId,
    table: TableIdentity,
    implementation_pool: ImplementationPool,
    state: ReceiptState,
    revision: Measurement,
    updated_at_unix_ms: UnixMillis,
    #[serde(skip_serializing_if = "Option::is_none")]
    publication: Option<DeltaPublicationResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<ErrorStatus>,
}

impl ReceiptStatus {
    /// Creates the initial accepted status for a newly admitted delivery.
    #[must_use]
    pub fn accepted(
        receipt_id: ReceiptId,
        table: TableIdentity,
        implementation_pool: ImplementationPool,
        accepted_at_unix_ms: UnixMillis,
    ) -> Self {
        Self {
            protocol_version: ProtocolVersion::CURRENT,
            receipt_id,
            table,
            implementation_pool,
            state: ReceiptState::Accepted,
            revision: Measurement::ZERO,
            updated_at_unix_ms: accepted_at_unix_ms,
            publication: None,
            error: None,
        }
    }

    /// Applies a validated state transition and increments the revision.
    ///
    /// `publication` is required only for `published`; `error` is required only
    /// for `failed` and `publication_unknown`.
    ///
    /// # Errors
    ///
    /// Returns [`ReceiptTransitionError`] for an invalid edge, regressing
    /// timestamp, exhausted revision, or payload/state mismatch.
    pub fn transition_to(
        &self,
        next: ReceiptState,
        updated_at_unix_ms: UnixMillis,
        publication: Option<DeltaPublicationResult>,
        error: Option<ErrorStatus>,
    ) -> Result<Self, ReceiptTransitionError> {
        self.state.validate_transition(next)?;
        if updated_at_unix_ms < self.updated_at_unix_ms {
            return Err(ReceiptTransitionError::TimestampRegressed);
        }
        let next_revision = self
            .revision
            .get()
            .checked_add(1)
            .ok_or(ReceiptTransitionError::RevisionExhausted)?;
        let next_revision = Measurement::new(next_revision).map_err(|_| ReceiptTransitionError::RevisionExhausted)?;
        validate_payload(
            next,
            next_revision,
            self.receipt_id,
            updated_at_unix_ms,
            &self.table,
            publication.as_ref(),
            error.as_ref(),
        )
        .map_err(ReceiptTransitionError::InvalidPayload)?;

        Ok(Self {
            protocol_version: self.protocol_version,
            receipt_id: self.receipt_id,
            table: self.table.clone(),
            implementation_pool: self.implementation_pool,
            state: next,
            revision: next_revision,
            updated_at_unix_ms,
            publication,
            error,
        })
    }

    /// Returns the receipt payload protocol version.
    #[must_use]
    pub const fn protocol_version(&self) -> ProtocolVersion {
        self.protocol_version
    }

    /// Returns the server-generated receipt identifier.
    #[must_use]
    pub const fn receipt_id(&self) -> ReceiptId {
        self.receipt_id
    }

    /// Returns the delivery table.
    #[must_use]
    pub const fn table(&self) -> &TableIdentity {
        &self.table
    }

    /// Returns the implementation pool that admitted the delivery.
    #[must_use]
    pub const fn implementation_pool(&self) -> ImplementationPool {
        self.implementation_pool
    }

    /// Returns the current receipt state.
    #[must_use]
    pub const fn state(&self) -> ReceiptState {
        self.state
    }

    /// Returns the monotonically increasing state revision.
    #[must_use]
    pub const fn revision(&self) -> Measurement {
        self.revision
    }

    /// Returns when the status was last updated.
    #[must_use]
    pub const fn updated_at_unix_ms(&self) -> UnixMillis {
        self.updated_at_unix_ms
    }

    /// Returns successful publication metadata.
    #[must_use]
    pub const fn publication(&self) -> Option<&DeltaPublicationResult> {
        self.publication.as_ref()
    }

    /// Returns terminal or uncertain failure metadata.
    #[must_use]
    pub const fn error(&self) -> Option<&ErrorStatus> {
        self.error.as_ref()
    }
}

impl<'de> Deserialize<'de> for ReceiptStatus {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Wire {
            protocol_version: ProtocolVersion,
            receipt_id: ReceiptId,
            table: TableIdentity,
            implementation_pool: ImplementationPool,
            state: ReceiptState,
            revision: Measurement,
            updated_at_unix_ms: UnixMillis,
            #[serde(default)]
            publication: Option<DeltaPublicationResult>,
            #[serde(default)]
            error: Option<ErrorStatus>,
        }

        let wire = Wire::deserialize(deserializer)?;
        validate_payload(
            wire.state,
            wire.revision,
            wire.receipt_id,
            wire.updated_at_unix_ms,
            &wire.table,
            wire.publication.as_ref(),
            wire.error.as_ref(),
        )
        .map_err(de::Error::custom)?;
        Ok(Self {
            protocol_version: wire.protocol_version,
            receipt_id: wire.receipt_id,
            table: wire.table,
            implementation_pool: wire.implementation_pool,
            state: wire.state,
            revision: wire.revision,
            updated_at_unix_ms: wire.updated_at_unix_ms,
            publication: wire.publication,
            error: wire.error,
        })
    }
}

/// Failure to validate a receipt status payload.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ReceiptStatusError {
    /// The accepted state must have revision zero.
    #[error("accepted receipt must have revision zero")]
    AcceptedRevisionNotZero,
    /// A state after acceptance must have a positive revision.
    #[error("transitioned receipt must have a positive revision")]
    TransitionedRevisionZero,
    /// Publication metadata was present outside the published state.
    #[error("publication metadata is only valid for a published receipt")]
    UnexpectedPublication,
    /// A published state lacked publication metadata.
    #[error("published receipt requires publication metadata")]
    MissingPublication,
    /// Error metadata was present in a non-error state.
    #[error("error metadata is only valid for failed or uncertain receipts")]
    UnexpectedError,
    /// An error state lacked error metadata.
    #[error("failed or uncertain receipt requires error metadata")]
    MissingError,
    /// Publication metadata names a different table.
    #[error("publication result table does not match receipt table")]
    PublicationTableMismatch,
    /// Publication completion was later than the enclosing status update.
    #[error("publication completion is later than receipt status update")]
    PublicationAfterStatusUpdate,
    /// Nested error metadata named a different receipt.
    #[error("error status receipt does not match enclosing receipt")]
    ErrorReceiptMismatch,
    /// An uncertain state used a code other than publication outcome unknown.
    #[error("publication_unknown receipt requires publication_outcome_unknown error")]
    UnknownStateCodeMismatch,
    /// A known failed state used the uncertain-outcome code.
    #[error("failed receipt cannot use publication_outcome_unknown error")]
    FailedStateUsesUnknownCode,
}

/// Failure to apply a receipt state transition.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ReceiptTransitionError {
    /// The requested state edge is invalid.
    #[error("invalid receipt transition from {from:?} to {to:?}")]
    Invalid {
        /// Existing state.
        from: ReceiptState,
        /// Requested state.
        to: ReceiptState,
    },
    /// The new update time went backwards.
    #[error("receipt update timestamp regressed")]
    TimestampRegressed,
    /// The monotonic receipt revision cannot be incremented.
    #[error("receipt revision is exhausted")]
    RevisionExhausted,
    /// The target state's payload did not satisfy its invariants.
    #[error(transparent)]
    InvalidPayload(ReceiptStatusError),
}

fn validate_payload(
    state: ReceiptState,
    revision: Measurement,
    receipt_id: ReceiptId,
    updated_at_unix_ms: UnixMillis,
    table: &TableIdentity,
    publication: Option<&DeltaPublicationResult>,
    error: Option<&ErrorStatus>,
) -> Result<(), ReceiptStatusError> {
    if state == ReceiptState::Accepted && revision.get() != 0 {
        return Err(ReceiptStatusError::AcceptedRevisionNotZero);
    }
    if state != ReceiptState::Accepted && revision.get() == 0 {
        return Err(ReceiptStatusError::TransitionedRevisionZero);
    }

    match state {
        ReceiptState::Published => {
            let publication = publication.ok_or(ReceiptStatusError::MissingPublication)?;
            publication
                .validate_for_table(table)
                .map_err(|_| ReceiptStatusError::PublicationTableMismatch)?;
            if publication.published_at_unix_ms() > updated_at_unix_ms {
                return Err(ReceiptStatusError::PublicationAfterStatusUpdate);
            }
            if error.is_some() {
                return Err(ReceiptStatusError::UnexpectedError);
            }
        }
        ReceiptState::Failed => {
            if publication.is_some() {
                return Err(ReceiptStatusError::UnexpectedPublication);
            }
            let error = error.ok_or(ReceiptStatusError::MissingError)?;
            if error.code() == ErrorCode::PublicationOutcomeUnknown {
                return Err(ReceiptStatusError::FailedStateUsesUnknownCode);
            }
        }
        ReceiptState::PublicationUnknown => {
            if publication.is_some() {
                return Err(ReceiptStatusError::UnexpectedPublication);
            }
            let error = error.ok_or(ReceiptStatusError::MissingError)?;
            if error.code() != ErrorCode::PublicationOutcomeUnknown {
                return Err(ReceiptStatusError::UnknownStateCodeMismatch);
            }
        }
        ReceiptState::Accepted | ReceiptState::Buffered | ReceiptState::Publishing => {
            if publication.is_some() {
                return Err(ReceiptStatusError::UnexpectedPublication);
            }
            if error.is_some() {
                return Err(ReceiptStatusError::UnexpectedError);
            }
        }
    }
    if error
        .and_then(ErrorStatus::receipt_id)
        .is_some_and(|error_receipt_id| error_receipt_id != receipt_id)
    {
        return Err(ReceiptStatusError::ErrorReceiptMismatch);
    }
    Ok(())
}
