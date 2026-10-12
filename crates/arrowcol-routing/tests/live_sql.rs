use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrowcol_protocol::{
    BasisPoints, CatalogName, ClaimedSchemaFingerprint, CollectorInstanceId, ImplementationPool, IngestMetadata,
    Measurement, PositiveMeasurement, PressureGauge, ProtocolVersion, SchemaFingerprint, SchemaName, TableIdentity,
    TableName, UnixMillis,
};
use arrowcol_routing::{
    CollectorRegistration, RichPressureSnapshot, RoutingEngine, RoutingPolicy, RoutingStore, SqlServerRoutingConfig,
    SqlServerRoutingStore, SubsystemState, TablePressure,
};
use uuid::Uuid;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the live arrowcol-infra SQL Server"]
async fn live_sql_coordinates_router_races_and_fenced_drain_transfer() -> anyhow::Result<()> {
    if std::env::var("ARROWCOL_SQLSERVER_TEST").as_deref() != Ok("1") {
        return Ok(());
    }
    let store = Arc::new(SqlServerRoutingStore::new(sql_config()?));
    let now = now()?;
    let first = CollectorInstanceId::new();
    let second = CollectorInstanceId::new();
    for (instance, port) in [(first, 51_001), (second, 51_002)] {
        store
            .register_collector(&CollectorRegistration {
                instance_id: instance,
                instance_key: format!("live-router-{}", Uuid::new_v4().simple()),
                implementation_pool: ImplementationPool::Direct,
                flight_endpoint: format!("127.0.0.1:{port}").parse()?,
                catalog_capable: true,
                max_concurrent_streams: positive(64)?,
                started_at_unix_ms: subtract(now, 1_000)?,
                heartbeat_at_unix_ms: now,
                lease_expires_at_unix_ms: add(now, 180_000)?,
            })
            .await?;
        store.publish_pressure(&snapshot(instance, now, 500, 0, None)?).await?;
    }

    let table = TableIdentity::new(
        CatalogName::new("telemetry")?,
        SchemaName::new("live")?,
        TableName::new(format!("route_{}", &Uuid::new_v4().simple().to_string()[..12]))?,
    );
    let metadata = IngestMetadata::new(
        ProtocolVersion::CURRENT,
        ImplementationPool::Direct,
        table.clone(),
        ClaimedSchemaFingerprint::new(SchemaFingerprint::from_bytes([11; 32])),
    );
    let policy = RoutingPolicy {
        warmup_ms: 0,
        lease_ttl_ms: 60_000,
        ..RoutingPolicy::default()
    };
    let left_store: Arc<dyn RoutingStore> = Arc::<SqlServerRoutingStore>::clone(&store);
    let right_store: Arc<dyn RoutingStore> = Arc::<SqlServerRoutingStore>::clone(&store);
    let left = Arc::new(RoutingEngine::new(left_store, policy.clone())?);
    let right = Arc::new(RoutingEngine::new(right_store, policy)?);
    left.refresh(now).await?;
    right.refresh(now).await?;
    let claim_at = add(now, 1)?;

    let left_route = {
        let left = Arc::clone(&left);
        let metadata = metadata.clone();
        tokio::spawn(async move { left.route_ingest(&metadata, claim_at).await })
    };
    let right_route = {
        let right = Arc::clone(&right);
        let metadata = metadata.clone();
        tokio::spawn(async move { right.route_ingest(&metadata, claim_at).await })
    };
    let left_route = left_route.await??;
    let right_route = right_route.await??;
    assert_eq!(left_route.lease, right_route.lease);
    let holder = left_route.instance_id;
    let fallback = if holder == first { second } else { first };

    store
        .publish_pressure(&snapshot(holder, add(now, 2)?, 9_900, 1, Some(table.clone()))?)
        .await?;
    let draining = left.rebalance(add(now, 3)?).await?;
    assert_eq!(draining.drains_started, 1);
    assert!(left.route_ingest(&metadata, add(now, 4)?).await.is_err());

    store
        .publish_pressure(&snapshot(holder, add(now, 5)?, 9_900, 0, Some(table.clone()))?)
        .await?;
    let transferred = right.rebalance(add(now, 6)?).await?;
    assert_eq!(transferred.transfers_completed, 1);
    let final_route = right.route_ingest(&metadata, add(now, 7)?).await?;
    assert_eq!(final_route.instance_id, fallback);
    assert!(final_route.lease.as_ref().is_some_and(|lease| lease.fencing_epoch
        > left_route.lease.as_ref().map_or_else(
            || unreachable!("initial SQL route has a lease"),
            |lease| lease.fencing_epoch
        )));
    Ok(())
}

fn sql_config() -> anyhow::Result<SqlServerRoutingConfig> {
    let host = std::env::var("ARROWCOL_SQLSERVER_HOST").unwrap_or_else(|_| "127.0.0.1".to_owned());
    let port = std::env::var("ARROWCOL_SQLSERVER_PORT")
        .ok()
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(14_333);
    Ok(
        SqlServerRoutingConfig::with_password_file(host, port, "arrowcol", "arrowcol_app", secret_path())?
            .trust_server_certificate(true)
            .with_timeouts(Duration::from_secs(5), Duration::from_secs(15)),
    )
}

fn secret_path() -> PathBuf {
    std::env::var_os("ARROWCOL_APP_PASSWORD_FILE").map_or_else(
        || {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../projects/arrowcol-infra/secrets/arrowcol-app-password.txt")
        },
        PathBuf::from,
    )
}

fn snapshot(
    instance_id: CollectorInstanceId,
    observed_at: UnixMillis,
    cpu_bps: u16,
    table_streams: u64,
    table: Option<TableIdentity>,
) -> anyhow::Result<RichPressureSnapshot> {
    let drained = table_streams == 0 && table.is_some();
    let buffered_bytes = if drained { 0 } else { 10 };
    let buffered_rows = if drained { 0 } else { 10 };
    let buffered_batches = if drained { 0 } else { 1 };
    let oldest_buffer_age_ms = if drained { 0 } else { 10 };
    Ok(RichPressureSnapshot {
        instance_id,
        observed_at_unix_ms: observed_at,
        cpu_utilization_bps: BasisPoints::new(cpu_bps)?,
        rss_bytes: measurement(100)?,
        rss_limit_bytes: positive(1_000)?,
        active_streams: gauge(table_streams, 64)?,
        received_bytes: measurement(1_000)?,
        received_rows: measurement(100)?,
        received_batches: measurement(10)?,
        buffered_bytes: gauge(buffered_bytes, 1_000)?,
        buffered_rows: gauge(buffered_rows, 1_000)?,
        buffered_batches: gauge(buffered_batches, 100)?,
        available_permits: gauge(64_u64.saturating_sub(table_streams), 64)?,
        pending_commits: gauge(0, 16)?,
        oldest_buffer_age_ms: measurement(oldest_buffer_age_ms)?,
        delta_pressure_bps: BasisPoints::new(100)?,
        quiver_pressure_bps: BasisPoints::new(100)?,
        dfe_pressure_bps: BasisPoints::new(0)?,
        delta_state: SubsystemState::Ready,
        quiver_state: SubsystemState::Ready,
        dfe_state: SubsystemState::Disabled,
        accepting_deliveries: true,
        tables: table
            .map(|table| {
                vec![TablePressure {
                    table,
                    active_streams: measurement(table_streams)
                        .unwrap_or_else(|error| panic!("invalid test table stream count: {error}")),
                    buffered_bytes: measurement(buffered_bytes)
                        .unwrap_or_else(|error| panic!("invalid test buffered bytes: {error}")),
                    buffered_rows: measurement(buffered_rows)
                        .unwrap_or_else(|error| panic!("invalid test buffered rows: {error}")),
                    buffered_batches: measurement(buffered_batches)
                        .unwrap_or_else(|error| panic!("invalid test buffered batches: {error}")),
                    oldest_buffer_age_ms: measurement(oldest_buffer_age_ms)
                        .unwrap_or_else(|error| panic!("invalid test oldest age: {error}")),
                    pending_commits: measurement(0)
                        .unwrap_or_else(|error| panic!("invalid test pending commits: {error}")),
                }]
            })
            .unwrap_or_default(),
    })
}

fn measurement(value: u64) -> anyhow::Result<Measurement> {
    Ok(Measurement::new(value)?)
}

fn positive(value: u64) -> anyhow::Result<PositiveMeasurement> {
    Ok(PositiveMeasurement::new(value)?)
}

fn gauge(current: u64, limit: u64) -> anyhow::Result<PressureGauge> {
    Ok(PressureGauge::new(measurement(current)?, positive(limit)?)?)
}

fn now() -> anyhow::Result<UnixMillis> {
    let millis = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    Ok(UnixMillis::new(u64::try_from(millis)?)?)
}

fn add(value: UnixMillis, millis: u64) -> anyhow::Result<UnixMillis> {
    Ok(UnixMillis::new(
        value
            .get()
            .checked_add(millis)
            .ok_or_else(|| anyhow::anyhow!("timestamp overflow"))?,
    )?)
}

fn subtract(value: UnixMillis, millis: u64) -> anyhow::Result<UnixMillis> {
    Ok(UnixMillis::new(value.get().saturating_sub(millis))?)
}
