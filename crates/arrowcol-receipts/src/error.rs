use thiserror::Error;

/// Stable category for receipt persistence and transition failures.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ReceiptErrorKind {
    /// Configuration or a caller-supplied bound was invalid.
    #[error("invalid receipt configuration")]
    InvalidConfiguration,
    /// A bounded request exceeded its configured maximum.
    #[error("receipt request exceeds its configured bound")]
    RequestTooLarge,
    /// A requested receipt does not exist.
    #[error("receipt was not found")]
    NotFound,
    /// An optimistic revision or state transition raced.
    #[error("receipt transition conflicted")]
    Conflict,
    /// The owner or table lease is no longer authoritative.
    #[error("receipt operation was fenced")]
    Fenced,
    /// The owner is draining and cannot accept new deliveries.
    #[error("receipt owner is draining")]
    Draining,
    /// Persisted receipt data violated an invariant.
    #[error("receipt persistence state is corrupt")]
    CorruptState,
    /// SQL Server was unavailable or rejected an operation.
    #[error("receipt database operation failed")]
    Database,
}

/// Structured receipt-store error with a safe diagnostic.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("{kind}: {message}")]
pub struct ReceiptError {
    kind: ReceiptErrorKind,
    message: String,
}

impl ReceiptError {
    /// Creates a structured receipt error.
    #[must_use]
    pub fn new(kind: ReceiptErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    /// Returns the machine-readable category.
    #[must_use]
    pub const fn kind(&self) -> ReceiptErrorKind {
        self.kind
    }

    /// Returns safe diagnostic detail.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

/// Result type used by receipt durability APIs.
pub type ReceiptResult<T> = Result<T, ReceiptError>;
