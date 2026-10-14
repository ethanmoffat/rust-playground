use arrowcol_protocol::AuthContext;
use async_trait::async_trait;
use tonic::metadata::MetadataMap;

use crate::{RuntimeError, RuntimeResult};

/// Reserved metadata key for an opaque internal trusted-identity assertion.
///
/// A request carrying this key is rejected unless a configured
/// [`TrustedIdentityValidator`] authenticates it. External clients must not set
/// this metadata.
pub const INTERNAL_IDENTITY_METADATA_KEY: &str = "x-arrowcol-internal-identity-bin";

/// Authentication inputs extracted before the first request body message.
#[derive(Clone)]
pub struct AuthenticationRequest {
    metadata: MetadataMap,
    trusted_identity: Option<AuthContext>,
}

impl AuthenticationRequest {
    pub(crate) fn new(metadata: MetadataMap, trusted_identity: Option<AuthContext>) -> Self {
        Self {
            metadata,
            trusted_identity,
        }
    }

    /// Returns the original bounded gRPC metadata.
    #[must_use]
    pub const fn metadata(&self) -> &MetadataMap {
        &self.metadata
    }

    /// Returns a previously validated internal identity, when supplied.
    #[must_use]
    pub const fn trusted_identity(&self) -> Option<&AuthContext> {
        self.trusted_identity.as_ref()
    }
}

/// Asynchronous authentication boundary used by both collector binaries.
#[async_trait]
pub trait Authenticator: Send + Sync {
    /// Authenticates a request without retaining credentials.
    ///
    /// # Errors
    ///
    /// Returns a safe [`RuntimeError`] for missing, invalid, forbidden, or
    /// internally failed authentication.
    async fn authenticate(&self, request: AuthenticationRequest) -> RuntimeResult<AuthContext>;
}

/// Hook that validates internal trusted-identity assertions before auth.
#[async_trait]
pub trait TrustedIdentityValidator: Send + Sync {
    /// Validates any internal identity assertion in the metadata.
    ///
    /// Returning `None` means no internal identity was supplied. A validator
    /// must fail closed when an assertion is present but cannot be verified.
    ///
    /// # Errors
    ///
    /// Returns a safe authentication failure.
    async fn validate(&self, metadata: &MetadataMap) -> RuntimeResult<Option<AuthContext>>;
}

/// Default validator that rejects every claimed internal identity.
#[derive(Clone, Copy, Debug, Default)]
pub struct RejectUntrustedIdentityClaims;

#[async_trait]
impl TrustedIdentityValidator for RejectUntrustedIdentityClaims {
    async fn validate(&self, metadata: &MetadataMap) -> RuntimeResult<Option<AuthContext>> {
        if metadata.contains_key(INTERNAL_IDENTITY_METADATA_KEY) {
            return Err(RuntimeError::new(
                arrowcol_protocol::ErrorCode::AuthenticationInvalid,
                tonic::Code::Unauthenticated,
                "internal identity assertion was not trusted",
            ));
        }
        Ok(None)
    }
}

/// Explicit allow-all provider for local development and focused tests.
///
/// Production binaries should supply an authenticator that verifies bearer,
/// mTLS, or workload credentials. A validated internal identity takes
/// precedence over the configured fallback context.
#[derive(Clone, Debug)]
pub struct AllowAllAuthenticator {
    fallback: AuthContext,
}

impl AllowAllAuthenticator {
    /// Creates an allow-all provider returning the supplied identity.
    #[must_use]
    pub const fn new(fallback: AuthContext) -> Self {
        Self { fallback }
    }
}

#[async_trait]
impl Authenticator for AllowAllAuthenticator {
    async fn authenticate(&self, request: AuthenticationRequest) -> RuntimeResult<AuthContext> {
        Ok(request.trusted_identity.unwrap_or_else(|| self.fallback.clone()))
    }
}
