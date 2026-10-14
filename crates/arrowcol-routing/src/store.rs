use arrowcol_protocol::{CollectorInstanceId, UnixMillis};
use async_trait::async_trait;

use crate::{
    CollectorRegistration, CollectorState, LeaseKey, RichPressureSnapshot, RoutingLease, RoutingResult, RoutingView,
};

/// Request to create or observe an active lease.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LeaseClaim {
    /// Ownership key.
    pub key: LeaseKey,
    /// Deterministically selected candidate.
    pub candidate_instance_id: CollectorInstanceId,
    /// Lease issue time.
    pub now_unix_ms: UnixMillis,
    /// Exclusive lease expiry.
    pub expires_at_unix_ms: UnixMillis,
    /// Oldest pressure timestamp acceptable at compare-and-set time.
    pub minimum_snapshot_at_unix_ms: UnixMillis,
}

/// Compare-and-set request to begin drain.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LeaseCas {
    /// Ownership key.
    pub key: LeaseKey,
    /// Expected logical version.
    pub expected_version: u64,
    /// Mutation time.
    pub now_unix_ms: UnixMillis,
}

/// Compare-and-set request to transfer a fully drained lease.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LeaseTransfer {
    /// Ownership key.
    pub key: LeaseKey,
    /// Expected draining version.
    pub expected_version: u64,
    /// New owner.
    pub candidate_instance_id: CollectorInstanceId,
    /// New lease issue time.
    pub now_unix_ms: UnixMillis,
    /// New exclusive expiry.
    pub expires_at_unix_ms: UnixMillis,
    /// Oldest pressure timestamp acceptable for the new owner.
    pub minimum_snapshot_at_unix_ms: UnixMillis,
}

/// Persistence contract shared by router replicas.
#[async_trait]
pub trait RoutingStore: Send + Sync {
    /// Registers or refreshes one collector process lifetime.
    async fn register_collector(&self, registration: &CollectorRegistration) -> RoutingResult<()>;

    /// Extends a collector registration lease.
    async fn heartbeat_collector(
        &self,
        instance_id: CollectorInstanceId,
        heartbeat_at: UnixMillis,
        lease_expires_at: UnixMillis,
    ) -> RoutingResult<()>;

    /// Changes collector lifecycle state.
    async fn set_collector_state(
        &self,
        instance_id: CollectorInstanceId,
        state: CollectorState,
        changed_at: UnixMillis,
    ) -> RoutingResult<()>;

    /// Publishes a validated latest pressure snapshot plus history.
    async fn publish_pressure(&self, snapshot: &RichPressureSnapshot) -> RoutingResult<()>;

    /// Loads the complete immutable routing view.
    async fn load_view(&self, loaded_at: UnixMillis) -> RoutingResult<RoutingView>;

    /// Atomically creates a lease or returns the unexpired winner.
    async fn claim_lease(&self, claim: &LeaseClaim) -> RoutingResult<RoutingLease>;

    /// Atomically changes an active lease to draining.
    async fn begin_lease_drain(&self, request: &LeaseCas) -> RoutingResult<RoutingLease>;

    /// Atomically transfers a draining lease with a greater fencing epoch.
    async fn transfer_drained_lease(&self, request: &LeaseTransfer) -> RoutingResult<RoutingLease>;
}
