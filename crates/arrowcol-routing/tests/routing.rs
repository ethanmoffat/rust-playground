use std::sync::Arc;

use arrowcol_protocol::{
    BasisPoints, CatalogName, ClaimedSchemaFingerprint, CollectorInstanceId, ImplementationPool, IngestMetadata,
    Measurement, PositiveMeasurement, PressureGauge, ProtocolVersion, SchemaFingerprint, SchemaName, TableIdentity,
    TableName, UnixMillis,
};
use arrowcol_routing::{
    CollectorRegistration, CollectorState, LeaseClaim, LeaseKey, MemoryRoutingStore, RichPressureSnapshot, RouteSource,
    RoutingEngine, RoutingErrorKind, RoutingPolicy, RoutingStore, SubsystemState, TablePressure, rank_collectors,
};

fn millis(value: u64) -> UnixMillis {
    UnixMillis::new(value).unwrap_or_else(|error| panic!("invalid test timestamp: {error}"))
}

fn measurement(value: u64) -> Measurement {
    Measurement::new(value).unwrap_or_else(|error| panic!("invalid test measurement: {error}"))
}

fn positive(value: u64) -> PositiveMeasurement {
    PositiveMeasurement::new(value).unwrap_or_else(|error| panic!("invalid test positive measurement: {error}"))
}

fn gauge(current: u64, limit: u64) -> PressureGauge {
    PressureGauge::new(measurement(current), positive(limit))
        .unwrap_or_else(|error| panic!("invalid test gauge: {error}"))
}

fn table() -> TableIdentity {
    TableIdentity::new(
        CatalogName::new("telemetry").unwrap_or_else(|error| panic!("invalid catalog: {error}")),
        SchemaName::new("prod").unwrap_or_else(|error| panic!("invalid schema: {error}")),
        TableName::new("traces").unwrap_or_else(|error| panic!("invalid table: {error}")),
    )
}

fn metadata() -> IngestMetadata {
    IngestMetadata::new(
        ProtocolVersion::CURRENT,
        ImplementationPool::Direct,
        table(),
        ClaimedSchemaFingerprint::new(SchemaFingerprint::from_bytes([7; 32])),
    )
}

fn registration(instance_id: CollectorInstanceId, port: u16, started_at: u64) -> CollectorRegistration {
    CollectorRegistration {
        instance_id,
        instance_key: format!("collector-{port}"),
        implementation_pool: ImplementationPool::Direct,
        flight_endpoint: format!("127.0.0.1:{port}")
            .parse()
            .unwrap_or_else(|error| panic!("invalid endpoint: {error}")),
        catalog_capable: true,
        max_concurrent_streams: positive(64),
        started_at_unix_ms: millis(started_at),
        heartbeat_at_unix_ms: millis(100_000),
        lease_expires_at_unix_ms: millis(200_000),
    }
}

#[derive(Clone, Copy)]
struct TableWork {
    active_streams: u64,
    buffered_bytes: u64,
    buffered_rows: u64,
    buffered_batches: u64,
    pending_commits: u64,
}

impl TableWork {
    const DRAINED: Self = Self {
        active_streams: 0,
        buffered_bytes: 0,
        buffered_rows: 0,
        buffered_batches: 0,
        pending_commits: 0,
    };

    const fn queued(active_streams: u64) -> Self {
        Self {
            active_streams,
            buffered_bytes: 10,
            buffered_rows: 10,
            buffered_batches: 1,
            pending_commits: 0,
        }
    }
}

fn snapshot(
    instance_id: CollectorInstanceId,
    observed_at: u64,
    cpu_bps: u16,
    table_streams: u64,
) -> RichPressureSnapshot {
    snapshot_with_work(instance_id, observed_at, cpu_bps, TableWork::queued(table_streams))
}

fn drained_snapshot(instance_id: CollectorInstanceId, observed_at: u64, cpu_bps: u16) -> RichPressureSnapshot {
    snapshot_with_work(instance_id, observed_at, cpu_bps, TableWork::DRAINED)
}

fn global_snapshot_with_work(
    instance_id: CollectorInstanceId,
    observed_at: u64,
    cpu_bps: u16,
    work: TableWork,
) -> RichPressureSnapshot {
    let mut snapshot = snapshot_with_work(instance_id, observed_at, cpu_bps, work);
    snapshot.tables.clear();
    snapshot
}

fn snapshot_with_work(
    instance_id: CollectorInstanceId,
    observed_at: u64,
    cpu_bps: u16,
    work: TableWork,
) -> RichPressureSnapshot {
    let oldest_age = u64::from(work.buffered_batches > 0);
    RichPressureSnapshot {
        instance_id,
        observed_at_unix_ms: millis(observed_at),
        cpu_utilization_bps: BasisPoints::new(cpu_bps).unwrap_or_else(|error| panic!("invalid CPU pressure: {error}")),
        rss_bytes: measurement(100),
        rss_limit_bytes: positive(1_000),
        active_streams: gauge(work.active_streams, 64),
        received_bytes: measurement(1_000),
        received_rows: measurement(100),
        received_batches: measurement(10),
        buffered_bytes: gauge(work.buffered_bytes, 1_000),
        buffered_rows: gauge(work.buffered_rows, 1_000),
        buffered_batches: gauge(work.buffered_batches, 100),
        available_permits: gauge(64_u64.saturating_sub(work.active_streams), 64),
        pending_commits: gauge(work.pending_commits, 16),
        oldest_buffer_age_ms: measurement(oldest_age),
        delta_pressure_bps: BasisPoints::new(100).unwrap_or_else(|error| panic!("invalid Delta pressure: {error}")),
        quiver_pressure_bps: BasisPoints::new(100).unwrap_or_else(|error| panic!("invalid Quiver pressure: {error}")),
        dfe_pressure_bps: BasisPoints::new(0).unwrap_or_else(|error| panic!("invalid DFE pressure: {error}")),
        delta_state: SubsystemState::Ready,
        quiver_state: SubsystemState::Ready,
        dfe_state: SubsystemState::Disabled,
        accepting_deliveries: true,
        tables: vec![TablePressure {
            table: table(),
            active_streams: measurement(work.active_streams),
            buffered_bytes: measurement(work.buffered_bytes),
            buffered_rows: measurement(work.buffered_rows),
            buffered_batches: measurement(work.buffered_batches),
            oldest_buffer_age_ms: measurement(oldest_age),
            pending_commits: measurement(work.pending_commits),
        }],
    }
}

async fn seeded_store() -> (Arc<MemoryRoutingStore>, CollectorInstanceId, CollectorInstanceId) {
    let store = Arc::new(MemoryRoutingStore::default());
    let first = CollectorInstanceId::new();
    let second = CollectorInstanceId::new();
    store
        .register_collector(&registration(first, 5001, 20_000))
        .await
        .unwrap_or_else(|error| panic!("register first collector: {error}"));
    store
        .register_collector(&registration(second, 5002, 20_000))
        .await
        .unwrap_or_else(|error| panic!("register second collector: {error}"));
    store
        .publish_pressure(&snapshot(first, 99_000, 1_000, 0))
        .await
        .unwrap_or_else(|error| panic!("publish first pressure: {error}"));
    store
        .publish_pressure(&snapshot(second, 99_000, 1_000, 0))
        .await
        .unwrap_or_else(|error| panic!("publish second pressure: {error}"));
    (store, first, second)
}

#[tokio::test]
async fn valid_cached_lease_avoids_per_request_store_access() {
    let (store, _, _) = seeded_store().await;
    let engine_store: Arc<dyn RoutingStore> = Arc::<MemoryRoutingStore>::clone(&store);
    let engine = RoutingEngine::new(engine_store, RoutingPolicy::default())
        .unwrap_or_else(|error| panic!("create routing engine: {error}"));
    engine
        .refresh(millis(100_000))
        .await
        .unwrap_or_else(|error| panic!("initial refresh: {error}"));

    let first = engine
        .route_ingest(&metadata(), millis(100_001))
        .await
        .unwrap_or_else(|error| panic!("initial route: {error}"));
    assert_eq!(first.source, RouteSource::LeaseClaim);
    let after_claim = store.operation_counts();

    let cached = engine
        .route_ingest(&metadata(), millis(100_002))
        .await
        .unwrap_or_else(|error| panic!("cached route: {error}"));
    assert_eq!(cached.source, RouteSource::CachedLease);
    assert_eq!(cached.instance_id, first.instance_id);
    assert_eq!(store.operation_counts(), after_claim);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_router_replicas_converge_on_one_sql_winner() {
    let (store, _, _) = seeded_store().await;
    let left_store: Arc<dyn RoutingStore> = Arc::<MemoryRoutingStore>::clone(&store);
    let right_store: Arc<dyn RoutingStore> = Arc::<MemoryRoutingStore>::clone(&store);
    let left = Arc::new(
        RoutingEngine::new(left_store, RoutingPolicy::default())
            .unwrap_or_else(|error| panic!("create left engine: {error}")),
    );
    let right = Arc::new(
        RoutingEngine::new(right_store, RoutingPolicy::default())
            .unwrap_or_else(|error| panic!("create right engine: {error}")),
    );
    left.refresh(millis(100_000))
        .await
        .unwrap_or_else(|error| panic!("refresh left: {error}"));
    right
        .refresh(millis(100_000))
        .await
        .unwrap_or_else(|error| panic!("refresh right: {error}"));

    let left_task = {
        let left = Arc::clone(&left);
        tokio::spawn(async move { left.route_ingest(&metadata(), millis(100_001)).await })
    };
    let right_task = {
        let right = Arc::clone(&right);
        tokio::spawn(async move { right.route_ingest(&metadata(), millis(100_001)).await })
    };
    let left_route = left_task
        .await
        .unwrap_or_else(|error| panic!("left task failed: {error}"))
        .unwrap_or_else(|error| panic!("left route failed: {error}"));
    let right_route = right_task
        .await
        .unwrap_or_else(|error| panic!("right task failed: {error}"))
        .unwrap_or_else(|error| panic!("right route failed: {error}"));
    assert_eq!(left_route.lease, right_route.lease);
    assert_eq!(left_route.instance_id, right_route.instance_id);
}

#[tokio::test]
async fn valid_lease_winner_does_not_depend_on_a_losing_candidate() {
    let (store, first, second) = seeded_store().await;
    let key = LeaseKey::new(ImplementationPool::Direct, table());
    let winner = store
        .claim_lease(&LeaseClaim {
            key: key.clone(),
            candidate_instance_id: first,
            now_unix_ms: millis(100_001),
            expires_at_unix_ms: millis(130_001),
            minimum_snapshot_at_unix_ms: millis(99_000),
        })
        .await
        .unwrap_or_else(|error| panic!("claim initial lease: {error}"));
    store
        .set_collector_state(second, CollectorState::Draining, millis(100_002))
        .await
        .unwrap_or_else(|error| panic!("drain losing candidate: {error}"));

    let observed = store
        .claim_lease(&LeaseClaim {
            key,
            candidate_instance_id: second,
            now_unix_ms: millis(100_003),
            expires_at_unix_ms: millis(130_003),
            minimum_snapshot_at_unix_ms: millis(99_000),
        })
        .await
        .unwrap_or_else(|error| panic!("read authoritative lease winner: {error}"));

    assert_eq!(observed, winner);
}

#[tokio::test]
async fn eligible_holder_is_not_drained_for_soft_pressure_imbalance() {
    let (store, _, _) = seeded_store().await;
    let engine_store: Arc<dyn RoutingStore> = Arc::<MemoryRoutingStore>::clone(&store);
    let engine = RoutingEngine::new(engine_store, RoutingPolicy::default())
        .unwrap_or_else(|error| panic!("create engine: {error}"));
    engine
        .refresh(millis(100_000))
        .await
        .unwrap_or_else(|error| panic!("refresh: {error}"));
    let assigned = engine
        .route_ingest(&metadata(), millis(100_001))
        .await
        .unwrap_or_else(|error| panic!("initial assignment: {error}"));
    store
        .publish_pressure(&snapshot(assigned.instance_id, 100_002, 9_499, 0))
        .await
        .unwrap_or_else(|error| panic!("publish soft pressure: {error}"));

    let report = engine
        .rebalance(millis(100_003))
        .await
        .unwrap_or_else(|error| panic!("rebalance: {error}"));
    assert_eq!(report.drains_started, 0);
    let routed = engine
        .route_ingest(&metadata(), millis(100_004))
        .await
        .unwrap_or_else(|error| panic!("route stable lease: {error}"));
    assert_eq!(routed.instance_id, assigned.instance_id);
}

#[tokio::test]
async fn hard_pressure_starts_drain_then_fully_drained_snapshot_allows_transfer() {
    let (store, first, second) = seeded_store().await;
    let engine_store: Arc<dyn RoutingStore> = Arc::<MemoryRoutingStore>::clone(&store);
    let engine = RoutingEngine::new(engine_store, RoutingPolicy::default())
        .unwrap_or_else(|error| panic!("create engine: {error}"));
    engine
        .refresh(millis(100_000))
        .await
        .unwrap_or_else(|error| panic!("refresh: {error}"));
    let assigned = engine
        .route_ingest(&metadata(), millis(100_001))
        .await
        .unwrap_or_else(|error| panic!("initial assignment: {error}"));
    let holder = assigned.instance_id;
    let fallback = if holder == first { second } else { first };

    store
        .publish_pressure(&snapshot(holder, 100_002, 9_900, 1))
        .await
        .unwrap_or_else(|error| panic!("publish hard pressure: {error}"));
    let first_pass = engine
        .rebalance(millis(100_003))
        .await
        .unwrap_or_else(|error| panic!("start drain: {error}"));
    assert_eq!(first_pass.drains_started, 1);
    let Err(draining) = engine.route_ingest(&metadata(), millis(100_004)).await else {
        panic!("new stream should be rejected during drain");
    };
    assert_eq!(draining.kind(), RoutingErrorKind::LeaseDraining);

    store
        .publish_pressure(&drained_snapshot(holder, 100_005, 9_900))
        .await
        .unwrap_or_else(|error| panic!("publish drained pressure: {error}"));
    let second_pass = engine
        .rebalance(millis(100_006))
        .await
        .unwrap_or_else(|error| panic!("transfer drained lease: {error}"));
    assert_eq!(second_pass.transfers_completed, 1);
    let transferred = engine
        .route_ingest(&metadata(), millis(100_007))
        .await
        .unwrap_or_else(|error| panic!("route after transfer: {error}"));
    assert_eq!(transferred.instance_id, fallback);
    let old_epoch = assigned
        .lease
        .as_ref()
        .unwrap_or_else(|| panic!("initial route must have lease"))
        .fencing_epoch;
    let new_epoch = transferred
        .lease
        .as_ref()
        .unwrap_or_else(|| panic!("transferred route must have lease"))
        .fencing_epoch;
    assert!(new_epoch > old_epoch);
}

#[tokio::test]
async fn pre_drain_zero_snapshot_cannot_authorize_transfer() {
    let (store, first, second) = seeded_store().await;
    let engine_store: Arc<dyn RoutingStore> = Arc::<MemoryRoutingStore>::clone(&store);
    let engine = RoutingEngine::new(engine_store, RoutingPolicy::default())
        .unwrap_or_else(|error| panic!("create engine: {error}"));
    engine
        .refresh(millis(100_000))
        .await
        .unwrap_or_else(|error| panic!("refresh: {error}"));
    let assigned = engine
        .route_ingest(&metadata(), millis(100_001))
        .await
        .unwrap_or_else(|error| panic!("initial assignment: {error}"));
    let holder = assigned.instance_id;
    let fallback = if holder == first { second } else { first };

    store
        .publish_pressure(&drained_snapshot(holder, 100_002, 9_900))
        .await
        .unwrap_or_else(|error| panic!("publish pre-drain zero pressure: {error}"));
    let draining = engine
        .rebalance(millis(100_003))
        .await
        .unwrap_or_else(|error| panic!("start drain: {error}"));
    assert_eq!(draining.drains_started, 1);

    let premature = engine
        .rebalance(millis(100_004))
        .await
        .unwrap_or_else(|error| panic!("recheck pre-drain snapshot: {error}"));
    assert_eq!(premature.transfers_completed, 0);

    store
        .publish_pressure(&drained_snapshot(holder, 100_005, 9_900))
        .await
        .unwrap_or_else(|error| panic!("publish post-drain zero pressure: {error}"));
    let transferred = engine
        .rebalance(millis(100_006))
        .await
        .unwrap_or_else(|error| panic!("transfer after post-drain snapshot: {error}"));
    assert_eq!(transferred.transfers_completed, 1);
    let routed = engine
        .route_ingest(&metadata(), millis(100_007))
        .await
        .unwrap_or_else(|error| panic!("route after safe transfer: {error}"));
    assert_eq!(routed.instance_id, fallback);
}

#[tokio::test]
async fn zero_streams_with_buffered_or_committing_work_cannot_transfer() {
    let (store, first, second) = seeded_store().await;
    let engine_store: Arc<dyn RoutingStore> = Arc::<MemoryRoutingStore>::clone(&store);
    let engine = RoutingEngine::new(engine_store, RoutingPolicy::default())
        .unwrap_or_else(|error| panic!("create engine: {error}"));
    engine
        .refresh(millis(100_000))
        .await
        .unwrap_or_else(|error| panic!("refresh: {error}"));
    let assigned = engine
        .route_ingest(&metadata(), millis(100_001))
        .await
        .unwrap_or_else(|error| panic!("initial assignment: {error}"));
    let holder = assigned.instance_id;
    let fallback = if holder == first { second } else { first };

    store
        .publish_pressure(&snapshot(holder, 100_002, 9_900, 1))
        .await
        .unwrap_or_else(|error| panic!("publish pressure: {error}"));
    engine
        .rebalance(millis(100_003))
        .await
        .unwrap_or_else(|error| panic!("start drain: {error}"));

    store
        .publish_pressure(&snapshot_with_work(
            holder,
            100_004,
            9_900,
            TableWork {
                active_streams: 0,
                buffered_bytes: 10,
                buffered_rows: 10,
                buffered_batches: 1,
                pending_commits: 1,
            },
        ))
        .await
        .unwrap_or_else(|error| panic!("publish queued drain pressure: {error}"));
    let blocked = engine
        .rebalance(millis(100_005))
        .await
        .unwrap_or_else(|error| panic!("evaluate queued drain pressure: {error}"));
    assert_eq!(blocked.transfers_completed, 0);

    store
        .publish_pressure(&drained_snapshot(holder, 100_006, 9_900))
        .await
        .unwrap_or_else(|error| panic!("publish fully drained pressure: {error}"));
    let transferred = engine
        .rebalance(millis(100_007))
        .await
        .unwrap_or_else(|error| panic!("transfer fully drained lease: {error}"));
    assert_eq!(transferred.transfers_completed, 1);
    let routed = engine
        .route_ingest(&metadata(), millis(100_008))
        .await
        .unwrap_or_else(|error| panic!("route after fully drained transfer: {error}"));
    assert_eq!(routed.instance_id, fallback);
}

#[tokio::test]
async fn global_fallback_requires_every_drain_counter_to_reach_zero() {
    let (store, first, second) = seeded_store().await;
    let engine_store: Arc<dyn RoutingStore> = Arc::<MemoryRoutingStore>::clone(&store);
    let engine = RoutingEngine::new(engine_store, RoutingPolicy::default())
        .unwrap_or_else(|error| panic!("create engine: {error}"));
    engine
        .refresh(millis(100_000))
        .await
        .unwrap_or_else(|error| panic!("refresh: {error}"));
    let assigned = engine
        .route_ingest(&metadata(), millis(100_001))
        .await
        .unwrap_or_else(|error| panic!("initial assignment: {error}"));
    let holder = assigned.instance_id;
    let fallback = if holder == first { second } else { first };

    store
        .publish_pressure(&snapshot(holder, 100_002, 9_900, 1))
        .await
        .unwrap_or_else(|error| panic!("publish pressure: {error}"));
    engine
        .rebalance(millis(100_003))
        .await
        .unwrap_or_else(|error| panic!("start drain: {error}"));

    store
        .publish_pressure(&global_snapshot_with_work(
            holder,
            100_004,
            9_900,
            TableWork {
                active_streams: 0,
                buffered_bytes: 0,
                buffered_rows: 0,
                buffered_batches: 0,
                pending_commits: 1,
            },
        ))
        .await
        .unwrap_or_else(|error| panic!("publish global pending-commit pressure: {error}"));
    let blocked = engine
        .rebalance(millis(100_005))
        .await
        .unwrap_or_else(|error| panic!("evaluate global drain pressure: {error}"));
    assert_eq!(blocked.transfers_completed, 0);

    store
        .publish_pressure(&global_snapshot_with_work(holder, 100_006, 9_900, TableWork::DRAINED))
        .await
        .unwrap_or_else(|error| panic!("publish global fully drained pressure: {error}"));
    let transferred = engine
        .rebalance(millis(100_007))
        .await
        .unwrap_or_else(|error| panic!("transfer globally drained lease: {error}"));
    assert_eq!(transferred.transfers_completed, 1);
    let routed = engine
        .route_ingest(&metadata(), millis(100_008))
        .await
        .unwrap_or_else(|error| panic!("route after global fallback transfer: {error}"));
    assert_eq!(routed.instance_id, fallback);
}

#[tokio::test]
async fn stale_snapshots_and_expired_leases_fail_closed() {
    let (store, _, _) = seeded_store().await;
    let mut policy = RoutingPolicy::default();
    policy.pressure.max_snapshot_age_ms = 100;
    policy.lease_ttl_ms = 50;
    let engine_store: Arc<dyn RoutingStore> = Arc::<MemoryRoutingStore>::clone(&store);
    let engine = RoutingEngine::new(engine_store, policy).unwrap_or_else(|error| panic!("create engine: {error}"));
    engine
        .refresh(millis(99_050))
        .await
        .unwrap_or_else(|error| panic!("refresh: {error}"));
    let route = engine
        .route_ingest(&metadata(), millis(99_050))
        .await
        .unwrap_or_else(|error| panic!("route with fresh pressure: {error}"));
    assert!(route.lease.is_some());

    let Err(stale) = engine.route_ingest(&metadata(), millis(99_200)).await else {
        panic!("expired lease and stale pressure must fail closed");
    };
    assert_eq!(stale.kind(), RoutingErrorKind::PoolUnavailable);
}

#[tokio::test]
async fn sql_outage_uses_only_unexpired_cached_lease_then_fails_closed() {
    let (store, _, _) = seeded_store().await;
    let policy = RoutingPolicy {
        lease_ttl_ms: 50,
        ..RoutingPolicy::default()
    };
    let engine_store: Arc<dyn RoutingStore> = Arc::<MemoryRoutingStore>::clone(&store);
    let engine = RoutingEngine::new(engine_store, policy).unwrap_or_else(|error| panic!("create engine: {error}"));
    engine
        .refresh(millis(100_000))
        .await
        .unwrap_or_else(|error| panic!("refresh: {error}"));
    engine
        .route_ingest(&metadata(), millis(100_001))
        .await
        .unwrap_or_else(|error| panic!("initial route: {error}"));

    store.set_available(false);
    let cached = engine
        .route_ingest(&metadata(), millis(100_025))
        .await
        .unwrap_or_else(|error| panic!("valid cached lease should survive SQL outage: {error}"));
    assert_eq!(cached.source, RouteSource::CachedLease);

    let Err(expired) = engine.route_ingest(&metadata(), millis(100_060)).await else {
        panic!("expired cached lease must not invent ownership during SQL outage");
    };
    assert_eq!(expired.kind(), RoutingErrorKind::Database);
}

#[tokio::test]
async fn weighted_rendezvous_is_deterministic_and_excludes_hard_pressure() {
    let (store, first, second) = seeded_store().await;
    store
        .publish_pressure(&snapshot(first, 100_000, 9_900, 0))
        .await
        .unwrap_or_else(|error| panic!("publish hard pressure: {error}"));
    let view = store
        .load_view(millis(100_001))
        .await
        .unwrap_or_else(|error| panic!("load view: {error}"));
    let key = LeaseKey::new(ImplementationPool::Direct, table());
    let left = rank_collectors(&view, &key, millis(100_001), &RoutingPolicy::default(), false);
    let right = rank_collectors(&view, &key, millis(100_001), &RoutingPolicy::default(), false);
    assert_eq!(left, right);
    assert_eq!(left, vec![second]);
}

#[test]
fn pressure_validation_rejects_duplicate_tables_and_future_observations() {
    let instance = CollectorInstanceId::new();
    let mut pressure = snapshot(instance, 100_000, 100, 0);
    pressure.tables.push(pressure.tables[0].clone());
    let Err(duplicate) = pressure.validate_at(millis(100_000), &arrowcol_routing::PressurePolicy::default()) else {
        panic!("duplicate table must fail");
    };
    assert_eq!(duplicate.kind(), RoutingErrorKind::InvalidSnapshot);

    let future = snapshot(instance, 200_000, 100, 0);
    let Err(future_error) = future.validate_at(millis(100_000), &arrowcol_routing::PressurePolicy::default()) else {
        panic!("far-future observation must fail");
    };
    assert_eq!(future_error.kind(), RoutingErrorKind::InvalidSnapshot);
}
