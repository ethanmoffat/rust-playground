use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use thiserror::Error;

use crate::{CollectorInstanceId, ImplementationPool, LeaseId, ProtocolVersion, TableIdentity, UnixMillis};

const MAX_FENCING_TOKEN: u64 = 9_223_372_036_854_775_807;

/// A positive, monotonically increasing table publication fencing token.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FencingToken(u64);

impl FencingToken {
    /// Creates a valid positive fencing token.
    ///
    /// # Errors
    ///
    /// Returns [`LeaseError`] for zero or values outside the interoperable range.
    pub const fn new(value: u64) -> Result<Self, LeaseError> {
        if value == 0 {
            return Err(LeaseError::ZeroFencingToken);
        }
        if value > MAX_FENCING_TOKEN {
            return Err(LeaseError::FencingTokenTooLarge);
        }
        Ok(Self(value))
    }

    /// Returns the numeric token.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the next token, failing instead of wrapping.
    ///
    /// # Errors
    ///
    /// Returns [`LeaseError::FencingTokenExhausted`] at the maximum token.
    pub const fn next(self) -> Result<Self, LeaseError> {
        if self.0 == MAX_FENCING_TOKEN {
            return Err(LeaseError::FencingTokenExhausted);
        }
        Ok(Self(self.0 + 1))
    }
}

impl Serialize for FencingToken {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(self.0)
    }
}

impl<'de> Deserialize<'de> for FencingToken {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = u64::deserialize(deserializer)?;
        Self::new(value).map_err(de::Error::custom)
    }
}

/// Active ownership lease for publishing one table.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct TableLease {
    protocol_version: ProtocolVersion,
    lease_id: LeaseId,
    table: TableIdentity,
    holder_instance_id: CollectorInstanceId,
    implementation_pool: ImplementationPool,
    fencing_token: FencingToken,
    issued_at_unix_ms: UnixMillis,
    expires_at_unix_ms: UnixMillis,
}

impl TableLease {
    /// Creates a table lease with a strictly positive validity interval.
    ///
    /// # Errors
    ///
    /// Returns [`LeaseError::InvalidTimeRange`] unless expiry is later than
    /// issuance.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        lease_id: LeaseId,
        table: TableIdentity,
        holder_instance_id: CollectorInstanceId,
        implementation_pool: ImplementationPool,
        fencing_token: FencingToken,
        issued_at_unix_ms: UnixMillis,
        expires_at_unix_ms: UnixMillis,
    ) -> Result<Self, LeaseError> {
        if expires_at_unix_ms <= issued_at_unix_ms {
            return Err(LeaseError::InvalidTimeRange);
        }
        Ok(Self {
            protocol_version: ProtocolVersion::CURRENT,
            lease_id,
            table,
            holder_instance_id,
            implementation_pool,
            fencing_token,
            issued_at_unix_ms,
            expires_at_unix_ms,
        })
    }

    /// Validates a renewal or fenced replacement of this lease.
    ///
    /// # Errors
    ///
    /// Returns [`TableLeaseSuccessorError`] if identity, ownership, time, or
    /// fencing progression is invalid.
    pub fn validate_successor(&self, next: &Self) -> Result<(), TableLeaseSuccessorError> {
        if self.table != next.table {
            return Err(TableLeaseSuccessorError::TableChanged);
        }
        if next.issued_at_unix_ms < self.issued_at_unix_ms {
            return Err(TableLeaseSuccessorError::IssuedAtRegressed);
        }

        if self.lease_id == next.lease_id {
            if self.holder_instance_id != next.holder_instance_id
                || self.implementation_pool != next.implementation_pool
            {
                return Err(TableLeaseSuccessorError::RenewalOwnerChanged);
            }
            if self.fencing_token != next.fencing_token {
                return Err(TableLeaseSuccessorError::RenewalTokenChanged);
            }
            if next.expires_at_unix_ms <= self.expires_at_unix_ms {
                return Err(TableLeaseSuccessorError::RenewalDidNotExtend);
            }
        } else if next.fencing_token <= self.fencing_token {
            return Err(TableLeaseSuccessorError::ReplacementNotFenced);
        }
        Ok(())
    }

    /// Returns whether the lease is active at the given instant.
    #[must_use]
    pub fn is_active_at(&self, instant: UnixMillis) -> bool {
        instant >= self.issued_at_unix_ms && instant < self.expires_at_unix_ms
    }

    /// Returns the contract version.
    #[must_use]
    pub const fn protocol_version(&self) -> ProtocolVersion {
        self.protocol_version
    }

    /// Returns the lease identifier.
    #[must_use]
    pub const fn lease_id(&self) -> LeaseId {
        self.lease_id
    }

    /// Returns the leased table.
    #[must_use]
    pub const fn table(&self) -> &TableIdentity {
        &self.table
    }

    /// Returns the holder instance.
    #[must_use]
    pub const fn holder_instance_id(&self) -> CollectorInstanceId {
        self.holder_instance_id
    }

    /// Returns the holder implementation pool.
    #[must_use]
    pub const fn implementation_pool(&self) -> ImplementationPool {
        self.implementation_pool
    }

    /// Returns the publication fencing token.
    #[must_use]
    pub const fn fencing_token(&self) -> FencingToken {
        self.fencing_token
    }

    /// Returns when the lease was issued.
    #[must_use]
    pub const fn issued_at_unix_ms(&self) -> UnixMillis {
        self.issued_at_unix_ms
    }

    /// Returns the exclusive expiry time.
    #[must_use]
    pub const fn expires_at_unix_ms(&self) -> UnixMillis {
        self.expires_at_unix_ms
    }

    /// Creates publication fencing metadata from this lease.
    #[must_use]
    pub fn publication_fence(&self) -> PublicationFence {
        PublicationFence {
            table: self.table.clone(),
            lease_id: self.lease_id,
            holder_instance_id: self.holder_instance_id,
            fencing_token: self.fencing_token,
        }
    }
}

impl<'de> Deserialize<'de> for TableLease {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Wire {
            protocol_version: ProtocolVersion,
            lease_id: LeaseId,
            table: TableIdentity,
            holder_instance_id: CollectorInstanceId,
            implementation_pool: ImplementationPool,
            fencing_token: FencingToken,
            issued_at_unix_ms: UnixMillis,
            expires_at_unix_ms: UnixMillis,
        }

        let wire = Wire::deserialize(deserializer)?;
        let mut lease = Self::new(
            wire.lease_id,
            wire.table,
            wire.holder_instance_id,
            wire.implementation_pool,
            wire.fencing_token,
            wire.issued_at_unix_ms,
            wire.expires_at_unix_ms,
        )
        .map_err(de::Error::custom)?;
        lease.protocol_version = wire.protocol_version;
        Ok(lease)
    }
}

/// Metadata that must match the active lease before Delta publication.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PublicationFence {
    table: TableIdentity,
    lease_id: LeaseId,
    holder_instance_id: CollectorInstanceId,
    fencing_token: FencingToken,
}

impl PublicationFence {
    /// Returns the fenced table.
    #[must_use]
    pub const fn table(&self) -> &TableIdentity {
        &self.table
    }

    /// Returns the lease identifier.
    #[must_use]
    pub const fn lease_id(&self) -> LeaseId {
        self.lease_id
    }

    /// Returns the expected publisher instance.
    #[must_use]
    pub const fn holder_instance_id(&self) -> CollectorInstanceId {
        self.holder_instance_id
    }

    /// Returns the expected fencing token.
    #[must_use]
    pub const fn fencing_token(&self) -> FencingToken {
        self.fencing_token
    }

    /// Returns whether every fencing component matches a lease document.
    #[must_use]
    pub fn matches_lease(&self, lease: &TableLease) -> bool {
        self.table == lease.table
            && self.lease_id == lease.lease_id
            && self.holder_instance_id == lease.holder_instance_id
            && self.fencing_token == lease.fencing_token
    }

    /// Returns whether the fence matches a lease that is active at an instant.
    #[must_use]
    pub fn matches_active_lease_at(&self, lease: &TableLease, instant: UnixMillis) -> bool {
        self.matches_lease(lease) && lease.is_active_at(instant)
    }
}

/// Failure to construct lease or fencing values.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum LeaseError {
    /// Fencing tokens are positive.
    #[error("fencing token must be positive")]
    ZeroFencingToken,
    /// The fencing token exceeded the interoperable range.
    #[error("fencing token exceeds the supported maximum")]
    FencingTokenTooLarge,
    /// No greater fencing token can be represented.
    #[error("fencing token space is exhausted")]
    FencingTokenExhausted,
    /// Lease expiry was not later than issuance.
    #[error("lease expiry must be later than issuance")]
    InvalidTimeRange,
}

/// Failure to validate a lease successor.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum TableLeaseSuccessorError {
    /// A successor must describe the same table.
    #[error("lease successor changed table")]
    TableChanged,
    /// Issuance time went backwards.
    #[error("lease successor issuance time regressed")]
    IssuedAtRegressed,
    /// Renewal changed holder or implementation pool.
    #[error("lease renewal changed owner")]
    RenewalOwnerChanged,
    /// Renewal changed its fencing token.
    #[error("lease renewal changed fencing token")]
    RenewalTokenChanged,
    /// Renewal did not extend exclusive expiry.
    #[error("lease renewal did not extend expiry")]
    RenewalDidNotExtend,
    /// Replacement did not advance the fencing token.
    #[error("replacement lease did not advance fencing token")]
    ReplacementNotFenced,
}
