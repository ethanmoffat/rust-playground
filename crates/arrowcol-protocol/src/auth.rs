use std::{collections::BTreeSet, fmt, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, de};
use thiserror::Error;

use crate::{ProtocolVersion, UnixMillis};

const MAX_PRINCIPAL_TEXT_BYTES: usize = 256;
const MAX_SCOPE_BYTES: usize = 128;
const MAX_SCOPES: usize = 64;

macro_rules! principal_text {
    ($name:ident, $label:literal, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            #[doc = concat!("Creates a validated ", $label, ".")]
            ///
            /// # Errors
            ///
            /// Returns [`PrincipalError`] for empty, oversized, surrounding
            /// whitespace, or control-bearing text.
            pub fn new(value: impl Into<String>) -> Result<Self, PrincipalError> {
                let value = value.into();
                validate_principal_text($label, &value, MAX_PRINCIPAL_TEXT_BYTES)?;
                Ok(Self(value))
            }

            #[doc = concat!("Returns the ", $label, " text.")]
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }

        impl FromStr for $name {
            type Err = PrincipalError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                Self::new(value).map_err(de::Error::custom)
            }
        }
    };
}

principal_text!(
    PrincipalId,
    "principal subject",
    "A validated stable subject identifier for an authenticated principal."
);
principal_text!(
    PrincipalIssuer,
    "principal issuer",
    "A validated identifier for the authority that authenticated a principal."
);

/// A validated authorization scope.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct PrincipalScope(String);

impl PrincipalScope {
    /// Creates a portable authorization scope.
    ///
    /// # Errors
    ///
    /// Returns [`PrincipalError`] when the value is empty, oversized, or uses
    /// characters outside `A-Z`, `a-z`, digits, `_`, `-`, `.`, `/`, or `:`.
    pub fn new(value: impl Into<String>) -> Result<Self, PrincipalError> {
        let value = value.into();
        validate_principal_text("principal scope", &value, MAX_SCOPE_BYTES)?;
        if value
            .chars()
            .any(|character| !(character.is_ascii_alphanumeric() || matches!(character, '_' | '-' | '.' | '/' | ':')))
        {
            return Err(PrincipalError::InvalidScope);
        }
        Ok(Self(value))
    }

    /// Returns the scope text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PrincipalScope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for PrincipalScope {
    type Err = PrincipalError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl<'de> Deserialize<'de> for PrincipalScope {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(de::Error::custom)
    }
}

/// Authentication mechanism that established a principal.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthMethod {
    /// A bearer access token was validated.
    BearerToken,
    /// A mutually authenticated TLS identity was validated.
    MutualTls,
    /// A workload identity assertion was validated.
    WorkloadIdentity,
}

/// An authenticated subject and its bounded authorization scopes.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct AuthenticatedPrincipal {
    subject: PrincipalId,
    issuer: PrincipalIssuer,
    scopes: BTreeSet<PrincipalScope>,
}

impl AuthenticatedPrincipal {
    /// Creates an authenticated principal.
    ///
    /// # Errors
    ///
    /// Returns [`PrincipalError::TooManyScopes`] above 64 scopes.
    pub fn new(
        subject: PrincipalId,
        issuer: PrincipalIssuer,
        scopes: BTreeSet<PrincipalScope>,
    ) -> Result<Self, PrincipalError> {
        if scopes.len() > MAX_SCOPES {
            return Err(PrincipalError::TooManyScopes);
        }
        Ok(Self {
            subject,
            issuer,
            scopes,
        })
    }

    /// Returns the authenticated subject.
    #[must_use]
    pub const fn subject(&self) -> &PrincipalId {
        &self.subject
    }

    /// Returns the authenticating issuer.
    #[must_use]
    pub const fn issuer(&self) -> &PrincipalIssuer {
        &self.issuer
    }

    /// Returns the authorization scopes.
    #[must_use]
    pub const fn scopes(&self) -> &BTreeSet<PrincipalScope> {
        &self.scopes
    }

    /// Returns whether the principal has an exact scope.
    #[must_use]
    pub fn has_scope(&self, scope: &PrincipalScope) -> bool {
        self.scopes.contains(scope)
    }
}

impl<'de> Deserialize<'de> for AuthenticatedPrincipal {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Wire {
            subject: PrincipalId,
            issuer: PrincipalIssuer,
            scopes: Vec<PrincipalScope>,
        }

        let wire = Wire::deserialize(deserializer)?;
        if wire.scopes.len() > MAX_SCOPES {
            return Err(de::Error::custom(PrincipalError::TooManyScopes));
        }
        let scope_count = wire.scopes.len();
        let scopes: BTreeSet<_> = wire.scopes.into_iter().collect();
        if scopes.len() != scope_count {
            return Err(de::Error::custom(PrincipalError::DuplicateScope));
        }
        Self::new(wire.subject, wire.issuer, scopes).map_err(de::Error::custom)
    }
}

/// Versioned authentication context attached to an authorized request.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct AuthContext {
    protocol_version: ProtocolVersion,
    principal: AuthenticatedPrincipal,
    method: AuthMethod,
    authenticated_at_unix_ms: UnixMillis,
    expires_at_unix_ms: Option<UnixMillis>,
}

impl AuthContext {
    /// Creates an authentication context without storing credentials.
    ///
    /// # Errors
    ///
    /// Returns [`AuthContextError::ExpirationNotAfterAuthentication`] when an
    /// expiry is not later than the authentication timestamp.
    pub fn new(
        principal: AuthenticatedPrincipal,
        method: AuthMethod,
        authenticated_at_unix_ms: UnixMillis,
        expires_at_unix_ms: Option<UnixMillis>,
    ) -> Result<Self, AuthContextError> {
        if expires_at_unix_ms.is_some_and(|expiry| expiry <= authenticated_at_unix_ms) {
            return Err(AuthContextError::ExpirationNotAfterAuthentication);
        }
        Ok(Self {
            protocol_version: ProtocolVersion::CURRENT,
            principal,
            method,
            authenticated_at_unix_ms,
            expires_at_unix_ms,
        })
    }

    /// Returns the contract version.
    #[must_use]
    pub const fn protocol_version(&self) -> ProtocolVersion {
        self.protocol_version
    }

    /// Returns the authenticated principal.
    #[must_use]
    pub const fn principal(&self) -> &AuthenticatedPrincipal {
        &self.principal
    }

    /// Returns the authentication method.
    #[must_use]
    pub const fn method(&self) -> AuthMethod {
        self.method
    }

    /// Returns when authentication completed.
    #[must_use]
    pub const fn authenticated_at_unix_ms(&self) -> UnixMillis {
        self.authenticated_at_unix_ms
    }

    /// Returns the credential expiry, when applicable.
    #[must_use]
    pub const fn expires_at_unix_ms(&self) -> Option<UnixMillis> {
        self.expires_at_unix_ms
    }

    /// Returns whether this context is expired at the supplied time.
    #[must_use]
    pub fn is_expired_at(&self, now: UnixMillis) -> bool {
        self.expires_at_unix_ms.is_some_and(|expiry| now >= expiry)
    }
}

impl<'de> Deserialize<'de> for AuthContext {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Wire {
            protocol_version: ProtocolVersion,
            principal: AuthenticatedPrincipal,
            method: AuthMethod,
            authenticated_at_unix_ms: UnixMillis,
            #[serde(default)]
            expires_at_unix_ms: Option<UnixMillis>,
        }

        let wire = Wire::deserialize(deserializer)?;
        let mut context = Self::new(
            wire.principal,
            wire.method,
            wire.authenticated_at_unix_ms,
            wire.expires_at_unix_ms,
        )
        .map_err(de::Error::custom)?;
        context.protocol_version = wire.protocol_version;
        Ok(context)
    }
}

/// Failure to validate principal data.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum PrincipalError {
    /// A required principal field was empty.
    #[error("{0} must not be empty")]
    Empty(&'static str),
    /// A principal field exceeded its bound.
    #[error("{0} is too long")]
    TooLong(&'static str),
    /// A principal field had surrounding whitespace.
    #[error("{0} must not have surrounding whitespace")]
    SurroundingWhitespace(&'static str),
    /// A principal field contained a control character.
    #[error("{0} contains a control character")]
    ControlCharacter(&'static str),
    /// A scope used a non-portable character.
    #[error("principal scope contains an invalid character")]
    InvalidScope,
    /// More than 64 authorization scopes were supplied.
    #[error("principal has more than 64 scopes")]
    TooManyScopes,
    /// A serialized scope appeared more than once.
    #[error("principal contains a duplicate scope")]
    DuplicateScope,
}

/// Failure to validate authentication timestamps.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum AuthContextError {
    /// Expiration must be strictly later than authentication.
    #[error("authentication expiry must be later than authentication time")]
    ExpirationNotAfterAuthentication,
}

fn validate_principal_text(label: &'static str, value: &str, maximum: usize) -> Result<(), PrincipalError> {
    if value.is_empty() {
        return Err(PrincipalError::Empty(label));
    }
    if value.len() > maximum {
        return Err(PrincipalError::TooLong(label));
    }
    if value.trim() != value {
        return Err(PrincipalError::SurroundingWhitespace(label));
    }
    if value.chars().any(char::is_control) {
        return Err(PrincipalError::ControlCharacter(label));
    }
    Ok(())
}
