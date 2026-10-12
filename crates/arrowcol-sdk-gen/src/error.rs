use std::{io, path::PathBuf};

use thiserror::Error;

/// Result type returned by SDK generation operations.
pub type SdkGenResult<T> = Result<T, SdkGenError>;

/// Deterministic SDK generation failure.
#[derive(Debug, Error)]
pub enum SdkGenError {
    /// A catalog artifact was invalid or inconsistent.
    #[error("catalog artifact error: {0}")]
    Artifact(String),
    /// A schema evolution was incompatible.
    #[error("incompatible schema evolution: {0}")]
    Compatibility(String),
    /// A catalog type cannot be represented without loss in both SDKs.
    #[error("unsupported SDK type at {path}: {reason}")]
    UnsupportedType {
        /// Stable field path.
        path: String,
        /// Rejection reason.
        reason: String,
    },
    /// Sanitized generated identifiers collided.
    #[error("generated identifier collision in {scope}: {identifier}")]
    IdentifierCollision {
        /// Scope containing the collision.
        scope: String,
        /// Colliding sanitized identifier.
        identifier: String,
    },
    /// A user-selected output path was unsafe.
    #[error("unsafe output path {path}: {reason}")]
    UnsafeOutputPath {
        /// Rejected path.
        path: PathBuf,
        /// Rejection reason.
        reason: String,
    },
    /// Existing generated output differs from deterministic regeneration.
    #[error("generated output mismatch at {path}: {reason}")]
    OutputMismatch {
        /// Mismatching output path.
        path: PathBuf,
        /// Mismatch reason.
        reason: String,
    },
    /// A generated relative path attempted to escape its output directory.
    #[error("invalid generated relative path {0}")]
    InvalidGeneratedPath(PathBuf),
    /// JSON serialization or parsing failed.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// Protobuf descriptor decoding failed.
    #[error(transparent)]
    Protobuf(#[from] prost::DecodeError),
    /// Filesystem access failed.
    #[error("I/O error at {path}: {source}")]
    Io {
        /// Path being accessed.
        path: PathBuf,
        /// Underlying error.
        #[source]
        source: io::Error,
    },
}

impl SdkGenError {
    pub(crate) fn io(path: impl Into<PathBuf>, source: io::Error) -> Self {
        Self::Io {
            path: path.into(),
            source,
        }
    }
}
