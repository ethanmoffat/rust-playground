use std::sync::mpsc::{Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow_deltalake::delta::{PublicationFence, PublicationFenceContext, PublicationFenceError};
use arrowcol_flight::{RuntimeError, RuntimeResult};
use arrowcol_protocol::{CollectorInstanceId, ErrorCode, FencingToken, ImplementationPool, LeaseId, TableIdentity};
use arrowcol_routing::{LeaseKey, LeaseState, RoutingLease, RoutingStore};
use async_trait::async_trait;
use tonic::Code;

use crate::config::now_unix_millis;
use crate::envelope::ArrowcolBatchEnvelope;

/// Resolves authoritative table ownership before a Flight stream is admitted.
#[async_trait]
pub trait LeaseResolver: Send + Sync {
    /// Returns the active Dataflow lease held by this collector.
    async fn resolve(&self, table: &TableIdentity) -> RuntimeResult<RoutingLease>;
}

/// Routing-store-backed lease resolver.
pub struct StoreLeaseResolver {
    store: Arc<dyn RoutingStore>,
    instance_id: CollectorInstanceId,
}

impl StoreLeaseResolver {
    /// Creates a resolver for one collector process lifetime.
    #[must_use]
    pub fn new(store: Arc<dyn RoutingStore>, instance_id: CollectorInstanceId) -> Self {
        Self { store, instance_id }
    }
}

#[async_trait]
impl LeaseResolver for StoreLeaseResolver {
    async fn resolve(&self, table: &TableIdentity) -> RuntimeResult<RoutingLease> {
        let now = now_unix_millis().map_err(|_| RuntimeError::internal())?;
        let view = self.store.load_view(now).await.map_err(|_| {
            RuntimeError::new(
                ErrorCode::PoolUnavailable,
                Code::Unavailable,
                "routing lease could not be verified",
            )
        })?;
        let key = LeaseKey::new(ImplementationPool::Dataflow, table.clone());
        let lease = view.leases.get(&key).cloned().ok_or_else(|| {
            RuntimeError::new(
                ErrorCode::LeaseFenced,
                Code::FailedPrecondition,
                "collector does not own an active table lease",
            )
        })?;
        if lease.holder_instance_id != self.instance_id
            || lease.state != LeaseState::Active
            || !lease.is_routable_at(now)
        {
            return Err(RuntimeError::new(
                ErrorCode::LeaseFenced,
                Code::FailedPrecondition,
                "collector does not own an active table lease",
            ));
        }
        Ok(lease)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LeaseEvidence {
    table: TableIdentity,
    lease_id: LeaseId,
    holder_instance_id: CollectorInstanceId,
    fencing_token: FencingToken,
}

impl LeaseEvidence {
    pub(crate) fn from_envelope(envelope: &ArrowcolBatchEnvelope) -> Self {
        Self {
            table: envelope.table.clone(),
            lease_id: envelope.lease_id,
            holder_instance_id: envelope.holder_instance_id,
            fencing_token: envelope.fencing_token,
        }
    }
}

struct AuthorityRequest {
    evidence: LeaseEvidence,
    reply: std::sync::mpsc::Sender<Result<(), PublicationFenceError>>,
}

/// Synchronous commit-boundary facade backed by a dedicated async routing worker.
#[derive(Clone)]
pub(crate) struct AuthorityClient {
    requests: SyncSender<AuthorityRequest>,
    timeout: Duration,
}

impl AuthorityClient {
    pub(crate) fn start(
        store: Arc<dyn RoutingStore>,
        timeout: Duration,
        queue_capacity: usize,
    ) -> Result<Self, String> {
        let (requests, receiver) = std::sync::mpsc::sync_channel(queue_capacity.max(1));
        std::thread::Builder::new()
            .name("arrowcol-lease-authority".to_owned())
            .spawn(move || authority_worker(store, receiver))
            .map_err(|error| format!("start routing authority worker: {error}"))?;
        Ok(Self { requests, timeout })
    }

    fn check(&self, evidence: LeaseEvidence) -> Result<(), PublicationFenceError> {
        let (reply, response) = std::sync::mpsc::channel();
        self.requests
            .try_send(AuthorityRequest { evidence, reply })
            .map_err(|error| {
                PublicationFenceError::check_failed(format!("routing authority queue unavailable: {error}"))
            })?;
        response.recv_timeout(self.timeout).map_err(|error| {
            PublicationFenceError::check_failed(format!("routing authority check timed out: {error}"))
        })?
    }
}

fn authority_worker(store: Arc<dyn RoutingStore>, receiver: Receiver<AuthorityRequest>) {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build();
    let Ok(runtime) = runtime else {
        while let Ok(request) = receiver.recv() {
            let _ = request.reply.send(Err(PublicationFenceError::check_failed(
                "routing authority runtime could not start",
            )));
        }
        return;
    };
    while let Ok(request) = receiver.recv() {
        let result = runtime.block_on(check_authority(&store, &request.evidence));
        let _ = request.reply.send(result);
    }
}

async fn check_authority(store: &Arc<dyn RoutingStore>, expected: &LeaseEvidence) -> Result<(), PublicationFenceError> {
    let now = now_unix_millis().map_err(PublicationFenceError::check_failed)?;
    let view = store
        .load_view(now)
        .await
        .map_err(|error| PublicationFenceError::check_failed(error.to_string()))?;
    let key = LeaseKey::new(ImplementationPool::Dataflow, expected.table.clone());
    let lease = view
        .leases
        .get(&key)
        .ok_or_else(|| PublicationFenceError::stale("authoritative routing lease is absent"))?;
    let exact = lease.lease_id == expected.lease_id
        && lease.holder_instance_id == expected.holder_instance_id
        && lease.fencing_epoch == expected.fencing_token
        && lease.is_valid_at(now);
    if !exact {
        return Err(PublicationFenceError::stale(
            "authoritative routing lease no longer matches the admitted fence",
        ));
    }
    Ok(())
}

pub(crate) struct RoutingPublicationFence {
    expected: Mutex<LeaseEvidence>,
    authority: AuthorityClient,
}

impl RoutingPublicationFence {
    pub(crate) fn new(evidence: LeaseEvidence, authority: AuthorityClient) -> Self {
        Self {
            expected: Mutex::new(evidence),
            authority,
        }
    }

    pub(crate) fn replace_expected(&self, evidence: LeaseEvidence) -> Result<(), String> {
        *self
            .expected
            .lock()
            .map_err(|_| "Delta publication fence lock poisoned".to_owned())? = evidence;
        Ok(())
    }
}

impl PublicationFence for RoutingPublicationFence {
    fn check_before_commit(&self, _context: &PublicationFenceContext<'_>) -> Result<(), PublicationFenceError> {
        let expected = self
            .expected
            .lock()
            .map(|evidence| evidence.clone())
            .map_err(|_| PublicationFenceError::check_failed("Delta publication fence lock poisoned"))?;
        self.authority.check(expected)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use arrowcol_protocol::{
        BasisPoints, CatalogName, Measurement, PositiveMeasurement, PressureGauge, SchemaName, TableName, UnixMillis,
    };
    use arrowcol_routing::{
        CollectorRegistration, MemoryRoutingStore, RichPressureSnapshot, RoutingStore, SubsystemState,
    };

    use super::*;

    fn time(value: u64) -> UnixMillis {
        UnixMillis::new(value).expect("time")
    }

    fn table() -> TableIdentity {
        TableIdentity::new(
            CatalogName::new("demo").expect("catalog"),
            SchemaName::new("generated").expect("schema"),
            TableName::new("events").expect("table"),
        )
    }

    async fn leased_store(holder: CollectorInstanceId) -> (Arc<MemoryRoutingStore>, RoutingLease) {
        let store = Arc::new(MemoryRoutingStore::default());
        store
            .register_collector(&CollectorRegistration {
                instance_id: holder,
                instance_key: "collector".to_owned(),
                implementation_pool: ImplementationPool::Dataflow,
                flight_endpoint: "127.0.0.1:32010".parse().expect("address"),
                catalog_capable: true,
                max_concurrent_streams: PositiveMeasurement::new(4).expect("positive"),
                started_at_unix_ms: time(1),
                heartbeat_at_unix_ms: time(1),
                lease_expires_at_unix_ms: time(u64::MAX / 4),
            })
            .await
            .expect("register");
        let zero =
            PressureGauge::new(Measurement::ZERO, PositiveMeasurement::new(4).expect("positive")).expect("gauge");
        store
            .publish_pressure(&RichPressureSnapshot {
                instance_id: holder,
                observed_at_unix_ms: time(1),
                cpu_utilization_bps: BasisPoints::new(0).expect("bps"),
                rss_bytes: Measurement::ZERO,
                rss_limit_bytes: PositiveMeasurement::new(1).expect("positive"),
                active_streams: zero,
                received_bytes: Measurement::ZERO,
                received_rows: Measurement::ZERO,
                received_batches: Measurement::ZERO,
                buffered_bytes: zero,
                buffered_rows: zero,
                buffered_batches: zero,
                available_permits: PressureGauge::new(
                    Measurement::new(4).expect("measurement"),
                    PositiveMeasurement::new(4).expect("positive"),
                )
                .expect("gauge"),
                pending_commits: zero,
                oldest_buffer_age_ms: Measurement::ZERO,
                delta_pressure_bps: BasisPoints::new(0).expect("bps"),
                quiver_pressure_bps: BasisPoints::new(0).expect("bps"),
                dfe_pressure_bps: BasisPoints::new(0).expect("bps"),
                delta_state: SubsystemState::Ready,
                quiver_state: SubsystemState::Disabled,
                dfe_state: SubsystemState::Ready,
                accepting_deliveries: true,
                tables: Vec::new(),
            })
            .await
            .expect("pressure");
        let now = now_unix_millis().expect("now");
        let lease = store
            .claim_lease(&arrowcol_routing::LeaseClaim {
                key: LeaseKey::new(ImplementationPool::Dataflow, table()),
                candidate_instance_id: holder,
                now_unix_ms: now,
                expires_at_unix_ms: UnixMillis::new(now.get() + 60_000).expect("expiry"),
                minimum_snapshot_at_unix_ms: time(0),
            })
            .await
            .expect("claim");
        (store, lease)
    }

    /// Scenario: A Flight stream reaches the wrong Dataflow collector.
    /// Guarantees: Admission rejects it before pdata enters a DFE channel.
    #[tokio::test]
    async fn resolver_rejects_non_owner() {
        let owner = CollectorInstanceId::new();
        let (store, _) = leased_store(owner).await;
        let resolver = StoreLeaseResolver::new(store as Arc<dyn RoutingStore>, CollectorInstanceId::new());
        let error = resolver.resolve(&table()).await.expect_err("fenced");
        assert_eq!(error.code(), ErrorCode::LeaseFenced);
    }

    /// Scenario: Routing ownership changes before a Delta publication boundary.
    /// Guarantees: The dedicated authority worker returns a typed stale-ownership fence.
    #[tokio::test]
    async fn authority_rejects_stale_evidence() {
        let owner = CollectorInstanceId::new();
        let (store, lease) = leased_store(owner).await;
        let authority =
            AuthorityClient::start(store as Arc<dyn RoutingStore>, Duration::from_secs(1), 1).expect("authority");
        let mut evidence = LeaseEvidence {
            table: table(),
            lease_id: lease.lease_id,
            holder_instance_id: owner,
            fencing_token: lease.fencing_epoch,
        };
        authority.check(evidence.clone()).expect("current lease");
        evidence.fencing_token = evidence.fencing_token.next().expect("next");
        let error = authority.check(evidence).expect_err("stale");
        assert_eq!(
            error.kind(),
            arrow_deltalake::delta::PublicationFenceErrorKind::StaleOwnership
        );
    }
}
