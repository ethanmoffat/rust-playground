use thiserror::Error;

/// Stable buffer/admission failure category.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum BufferErrorKind {
    /// Buffer settings or admission metadata are invalid.
    #[error("invalid buffer configuration or admission")]
    InvalidConfiguration,
    /// One delivery can never fit the configured capacity.
    #[error("delivery exceeds buffer capacity")]
    CapacityExceeded,
    /// Admission timed out while waiting for bounded capacity.
    #[error("buffer admission timed out")]
    BackpressureTimeout,
    /// New admission has stopped for drain/shutdown.
    #[error("buffer admission is closed")]
    AdmissionClosed,
    /// A table was poisoned by a nonrecoverable publication outcome.
    #[error("table buffer is poisoned")]
    TablePoisoned,
    /// Receipt persistence failed.
    #[error("receipt durability operation failed")]
    ReceiptStore,
    /// Publication sink failed.
    #[error("publication sink failed")]
    Publication,
    /// The requested persistence capability is unavailable.
    #[error("persistence capability is unsupported")]
    UnsupportedCapability,
    /// The local durable stage (Quiver) failed.
    #[error("local durable storage failed")]
    DurableStorage,
    /// Internal synchronization state was poisoned.
    #[error("buffer synchronization failed")]
    Synchronization,
}

/// Structured buffer error.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("{kind}: {message}")]
pub struct BufferError {
    kind: BufferErrorKind,
    message: String,
}

impl BufferError {
    /// Creates a structured buffer error.
    #[must_use]
    pub fn new(kind: BufferErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    /// Returns the machine-readable category.
    #[must_use]
    pub const fn kind(&self) -> BufferErrorKind {
        self.kind
    }

    /// Returns safe diagnostic detail.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

/// Result type used by buffer APIs.
pub type BufferResult<T> = Result<T, BufferError>;
