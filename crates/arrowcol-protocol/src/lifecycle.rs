use serde::{Deserialize, Deserializer, Serialize, de};
use thiserror::Error;

use crate::{CollectorInstanceId, ImplementationPool, ProtocolVersion, UnixMillis};

const MAX_LIFECYCLE_REASON_BYTES: usize = 512;

/// Lifecycle state of one collector process instance.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InstanceLifecycleState {
    /// The process is initializing and must not receive deliveries.
    Starting,
    /// The process is healthy and eligible for assignment.
    Ready,
    /// The process is refusing new work while admitted work drains.
    Draining,
    /// The process has stopped and owns no active work.
    Stopped,
    /// The process cannot safely accept or continue work.
    Failed,
}

impl InstanceLifecycleState {
    /// Validates a lifecycle transition.
    ///
    /// # Errors
    ///
    /// Returns [`InstanceTransitionError`] when the transition is not part of
    /// the production lifecycle.
    pub const fn validate_transition(self, next: Self) -> Result<(), InstanceTransitionError> {
        let valid = matches!(
            (self, next),
            (Self::Starting, Self::Ready | Self::Draining | Self::Failed)
                | (Self::Ready, Self::Draining | Self::Failed)
                | (Self::Draining, Self::Stopped | Self::Failed)
                | (Self::Failed, Self::Stopped)
        );
        if valid {
            Ok(())
        } else {
            Err(InstanceTransitionError::Invalid { from: self, to: next })
        }
    }
}

/// Versioned lifecycle status for a collector instance.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CollectorInstanceStatus {
    protocol_version: ProtocolVersion,
    instance_id: CollectorInstanceId,
    implementation_pool: ImplementationPool,
    state: InstanceLifecycleState,
    changed_at_unix_ms: UnixMillis,
    reason: Option<String>,
}

impl CollectorInstanceStatus {
    /// Creates a validated instance status.
    ///
    /// # Errors
    ///
    /// Returns [`InstanceStatusError`] for an invalid reason payload or a
    /// failed status without a reason.
    pub fn new(
        instance_id: CollectorInstanceId,
        implementation_pool: ImplementationPool,
        state: InstanceLifecycleState,
        changed_at_unix_ms: UnixMillis,
        reason: Option<String>,
    ) -> Result<Self, InstanceStatusError> {
        validate_reason(state, reason.as_deref())?;
        Ok(Self {
            protocol_version: ProtocolVersion::CURRENT,
            instance_id,
            implementation_pool,
            state,
            changed_at_unix_ms,
            reason,
        })
    }

    /// Returns a new status after validating state and timestamp progression.
    ///
    /// # Errors
    ///
    /// Returns [`InstanceTransitionError`] for an invalid state transition,
    /// regressing timestamp, or invalid reason.
    pub fn transition_to(
        &self,
        next: InstanceLifecycleState,
        changed_at_unix_ms: UnixMillis,
        reason: Option<String>,
    ) -> Result<Self, InstanceTransitionError> {
        self.state.validate_transition(next)?;
        if changed_at_unix_ms < self.changed_at_unix_ms {
            return Err(InstanceTransitionError::TimestampRegressed);
        }
        Self::new(
            self.instance_id,
            self.implementation_pool,
            next,
            changed_at_unix_ms,
            reason,
        )
        .map_err(InstanceTransitionError::InvalidStatus)
    }

    /// Returns the status protocol version.
    #[must_use]
    pub const fn protocol_version(&self) -> ProtocolVersion {
        self.protocol_version
    }

    /// Returns the collector instance identifier.
    #[must_use]
    pub const fn instance_id(&self) -> CollectorInstanceId {
        self.instance_id
    }

    /// Returns the implementation pool.
    #[must_use]
    pub const fn implementation_pool(&self) -> ImplementationPool {
        self.implementation_pool
    }

    /// Returns the lifecycle state.
    #[must_use]
    pub const fn state(&self) -> InstanceLifecycleState {
        self.state
    }

    /// Returns when the state last changed.
    #[must_use]
    pub const fn changed_at_unix_ms(&self) -> UnixMillis {
        self.changed_at_unix_ms
    }

    /// Returns a safe operator-facing reason, when present.
    #[must_use]
    pub fn reason(&self) -> Option<&str> {
        self.reason.as_deref()
    }
}

impl<'de> Deserialize<'de> for CollectorInstanceStatus {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Wire {
            protocol_version: ProtocolVersion,
            instance_id: CollectorInstanceId,
            implementation_pool: ImplementationPool,
            state: InstanceLifecycleState,
            changed_at_unix_ms: UnixMillis,
            #[serde(default)]
            reason: Option<String>,
        }

        let wire = Wire::deserialize(deserializer)?;
        let mut status = Self::new(
            wire.instance_id,
            wire.implementation_pool,
            wire.state,
            wire.changed_at_unix_ms,
            wire.reason,
        )
        .map_err(de::Error::custom)?;
        status.protocol_version = wire.protocol_version;
        Ok(status)
    }
}

/// Failure to validate an instance status payload.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum InstanceStatusError {
    /// Failed instances require an operator-safe reason.
    #[error("failed instance status requires a reason")]
    FailureReasonRequired,
    /// An empty reason is not meaningful.
    #[error("instance status reason must not be empty")]
    EmptyReason,
    /// The reason exceeded its wire bound.
    #[error("instance status reason exceeds 512 bytes")]
    ReasonTooLong,
    /// The reason contained a control character.
    #[error("instance status reason contains a control character")]
    ReasonContainsControl,
}

/// Failure to apply an instance lifecycle transition.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum InstanceTransitionError {
    /// The state edge is not permitted.
    #[error("invalid instance transition from {from:?} to {to:?}")]
    Invalid {
        /// Existing state.
        from: InstanceLifecycleState,
        /// Requested state.
        to: InstanceLifecycleState,
    },
    /// The new observation timestamp went backwards.
    #[error("instance status timestamp regressed")]
    TimestampRegressed,
    /// The target status payload was invalid.
    #[error(transparent)]
    InvalidStatus(InstanceStatusError),
}

fn validate_reason(state: InstanceLifecycleState, reason: Option<&str>) -> Result<(), InstanceStatusError> {
    if state == InstanceLifecycleState::Failed && reason.is_none() {
        return Err(InstanceStatusError::FailureReasonRequired);
    }
    if let Some(reason) = reason {
        if reason.is_empty() {
            return Err(InstanceStatusError::EmptyReason);
        }
        if reason.len() > MAX_LIFECYCLE_REASON_BYTES {
            return Err(InstanceStatusError::ReasonTooLong);
        }
        if reason.chars().any(char::is_control) {
            return Err(InstanceStatusError::ReasonContainsControl);
        }
    }
    Ok(())
}
