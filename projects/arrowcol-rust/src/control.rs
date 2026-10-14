use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use arrowcol_buffer::{BufferManager, BufferPressureSnapshot};
use arrowcol_catalog::{CatalogStore, Metastore};
use arrowcol_flight::{
    CapabilityReporter, DrainController, HealthReporter, HealthStatus, PressureReporter, ReadinessReporter,
    ReadinessStatus, RuntimeError, RuntimeResult,
};
use arrowcol_protocol::{
    BasisPoints, Capability, CollectorCapabilities, CollectorInstanceId, ImplementationPool, Measurement,
    PositiveMeasurement, PressureGauge, PressureSnapshot, ReceiptState, TableIdentity, UnixMillis,
};
use arrowcol_receipts::{OwnerRegistration, ReceiptStore};
use arrowcol_routing::{
    CollectorRegistration, CollectorState, RichPressureSnapshot, RoutingStore, SubsystemState, TablePressure,
};
use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::{error, warn};

use crate::config::{CollectorConfig, RoutingConfig};
use crate::delta_sink::verify_writable_root;
use crate::lease::{TableLeaseManager, add_duration, now_unix_millis};
use crate::metrics::CollectorMetrics;

/// Exact active Flight stream counts keyed by table identity.
#[derive(Default)]
pub struct TableActivity {
    active: Mutex<HashMap<TableIdentity, u64>>,
}

impl TableActivity {
    /// Enters one table-scoped stream and returns a drop guard.
    ///
    /// # Errors
    ///
    /// Returns an error if the activity lock was poisoned or the counter
    /// overflowed.
    pub fn enter(self: &Arc<Self>, table: TableIdentity) -> Result<ActiveTableStream> {
        let mut active = self
            .active
            .lock()
            .map_err(|_| anyhow!("table activity lock poisoned"))?;
        let count = active.entry(table.clone()).or_default();
        *count = count.checked_add(1).context("table activity counter overflow")?;
        Ok(ActiveTableStream {
            activity: Arc::clone(self),
            table,
        })
    }

    fn snapshot(&self) -> Result<HashMap<TableIdentity, u64>> {
        self.active
            .lock()
            .map(|active| active.clone())
            .map_err(|_| anyhow!("table activity lock poisoned"))
    }
}

/// Drop guard for one active table stream.
pub struct ActiveTableStream {
    activity: Arc<TableActivity>,
    table: TableIdentity,
}

impl Drop for ActiveTableStream {
    fn drop(&mut self) {
        let Ok(mut active) = self.activity.active.lock() else {
            return;
        };
        if let Some(count) = active.get_mut(&self.table) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                active.remove(&self.table);
            }
        }
    }
}

/// Synchronous health/readiness state plus async pressure/capability reporters.
pub struct CollectorStatus {
    instance_id: CollectorInstanceId,
    drain: DrainController,
    buffer: Arc<BufferManager>,
    leases: Arc<TableLeaseManager>,
    activity: Arc<TableActivity>,
    metrics: CollectorMetrics,
    max_request_bytes: PositiveMeasurement,
    max_concurrent_streams: PositiveMeasurement,
    max_pending_receipts: PositiveMeasurement,
    max_pending_publications: u64,
    routing: RoutingConfig,
    flags: RwLock<StatusFlags>,
}

#[derive(Clone, Debug)]
struct StatusFlags {
    healthy: bool,
    fatal_reason: Option<String>,
    dependencies_ready: bool,
    registration_ready: bool,
    pressure_ready: bool,
    lease_loop_ready: bool,
    delta_writable: bool,
}

impl CollectorStatus {
    /// Creates process status before dependency startup completes.
    ///
    /// # Errors
    ///
    /// Rejects protocol measurement overflow.
    pub fn new(
        config: &CollectorConfig,
        instance_id: CollectorInstanceId,
        drain: DrainController,
        buffer: Arc<BufferManager>,
        leases: Arc<TableLeaseManager>,
        activity: Arc<TableActivity>,
        metrics: CollectorMetrics,
    ) -> Result<Self> {
        Ok(Self {
            instance_id,
            drain,
            buffer,
            leases,
            activity,
            metrics,
            max_request_bytes: PositiveMeasurement::new(
                u64::try_from(config.flight.max_message_bytes).context("Flight request size exceeds protocol range")?,
            )?,
            max_concurrent_streams: PositiveMeasurement::new(config.flight.max_concurrent_streams)?,
            max_pending_receipts: PositiveMeasurement::new(config.buffer.global_capacity.batches)?,
            max_pending_publications: publication_capacity(config)?,
            routing: config.routing.clone(),
            flags: RwLock::new(StatusFlags {
                healthy: true,
                fatal_reason: None,
                dependencies_ready: false,
                registration_ready: false,
                pressure_ready: false,
                lease_loop_ready: false,
                delta_writable: false,
            }),
        })
    }

    /// Marks SQL/catalog/Hive dependency readiness.
    pub fn set_dependencies_ready(&self, ready: bool) {
        if let Ok(mut flags) = self.flags.write() {
            flags.dependencies_ready = ready;
        }
    }

    /// Marks routing and receipt owner registration readiness.
    pub fn set_registration_ready(&self, ready: bool) {
        if let Ok(mut flags) = self.flags.write() {
            flags.registration_ready = ready;
        }
    }

    /// Marks successful rich pressure publication readiness.
    pub fn set_pressure_ready(&self, ready: bool) {
        if let Ok(mut flags) = self.flags.write() {
            flags.pressure_ready = ready;
        }
    }

    /// Marks the routing/publication lease renewal loop readiness.
    pub fn set_lease_loop_ready(&self, ready: bool) {
        if let Ok(mut flags) = self.flags.write() {
            flags.lease_loop_ready = ready;
        }
    }

    /// Marks the Delta root writable probe.
    pub fn set_delta_writable(&self, ready: bool) {
        if let Ok(mut flags) = self.flags.write() {
            flags.delta_writable = ready;
        }
    }

    /// Fails health/readiness closed with a safe operator reason.
    pub fn set_fatal(&self, reason: impl Into<String>) {
        if let Ok(mut flags) = self.flags.write() {
            flags.healthy = false;
            flags.fatal_reason = Some(reason.into());
            flags.registration_ready = false;
            flags.pressure_ready = false;
            flags.lease_loop_ready = false;
        }
        self.drain.begin_drain();
    }

    /// Returns whether every required startup/control signal is currently ready.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        let Ok(flags) = self.flags.read() else {
            return false;
        };
        flags.healthy
            && flags.dependencies_ready
            && flags.registration_ready
            && flags.pressure_ready
            && flags.lease_loop_ready
            && flags.delta_writable
            && self.drain.is_accepting()
            && self
                .buffer
                .pressure()
                .is_ok_and(|pressure| pressure.accepting && pressure.tables.iter().all(|table| !table.poisoned))
    }

    /// Builds the richer SQL routing pressure snapshot.
    ///
    /// # Errors
    ///
    /// Returns buffer synchronization or protocol measurement failures.
    pub async fn rich_pressure(&self) -> Result<RichPressureSnapshot> {
        let now = now_unix_millis()?;
        let pressure = self.buffer.pressure().map_err(|error| anyhow!(error))?;
        let known: HashMap<_, _> = self.leases.known_tables().await.into_iter().collect();
        let active_by_table = self.activity.snapshot()?;
        let mut seen = BTreeSet::new();
        let mut tables = pressure
            .tables
            .iter()
            .filter_map(|table_pressure| {
                let table_id = table_pressure.table_id?;
                let table = known.get(&table_id)?.clone();
                seen.insert(table.clone());
                Some(table_pressure_document(
                    table.clone(),
                    table_pressure,
                    active_by_table.get(&table).copied().unwrap_or(0),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        for (table, active) in active_by_table {
            if seen.insert(table.clone()) {
                tables.push(TablePressure {
                    table,
                    active_streams: Measurement::new(active)?,
                    buffered_bytes: Measurement::ZERO,
                    buffered_rows: Measurement::ZERO,
                    buffered_batches: Measurement::ZERO,
                    oldest_buffer_age_ms: Measurement::ZERO,
                    pending_commits: Measurement::ZERO,
                });
            }
        }
        tables.sort_by(|left, right| left.table.cmp(&right.table));
        tables.truncate(self.routing.max_table_entries);

        let active = u64::try_from(self.drain.active_streams()).unwrap_or(u64::MAX);
        let active = gauge(active, self.max_concurrent_streams.get())?;
        let buffered_bytes = gauge(pressure.global.bytes, pressure.global.limits.bytes)?;
        let buffered_rows = gauge(pressure.global.rows, pressure.global.limits.rows)?;
        let buffered_batches = gauge(pressure.global.batches, pressure.global.limits.batches)?;
        let active_flushes = u64::try_from(pressure.active_flushes).unwrap_or(u64::MAX);
        let pending_commits = gauge(active_flushes, self.max_pending_publications)?;
        let available = self.max_concurrent_streams.get().saturating_sub(active.current().get());
        let available_permits = gauge(available, self.max_concurrent_streams.get())?;
        let rss = read_rss_bytes().unwrap_or(0).min(self.routing.rss_limit_bytes);
        let delta_pressure = pending_commits.utilization();
        let buffer_pressure = [
            buffered_bytes.utilization().get(),
            buffered_rows.utilization().get(),
            buffered_batches.utilization().get(),
        ]
        .into_iter()
        .max()
        .unwrap_or(0);
        let poisoned = pressure.tables.iter().any(|table| table.poisoned);
        Ok(RichPressureSnapshot {
            instance_id: self.instance_id,
            observed_at_unix_ms: now,
            cpu_utilization_bps: BasisPoints::new(0)?,
            rss_bytes: Measurement::new(rss)?,
            rss_limit_bytes: PositiveMeasurement::new(self.routing.rss_limit_bytes)?,
            active_streams: active,
            received_bytes: Measurement::new(self.metrics.received_bytes())?,
            received_rows: Measurement::new(self.metrics.received_rows())?,
            received_batches: Measurement::new(self.metrics.received_batches())?,
            buffered_bytes,
            buffered_rows,
            buffered_batches,
            available_permits,
            pending_commits,
            oldest_buffer_age_ms: Measurement::ZERO,
            delta_pressure_bps: delta_pressure,
            quiver_pressure_bps: BasisPoints::new(buffer_pressure)?,
            dfe_pressure_bps: BasisPoints::new(0)?,
            delta_state: if poisoned {
                SubsystemState::Blocked
            } else if delta_pressure.get() >= 8_000 {
                SubsystemState::Degraded
            } else {
                SubsystemState::Ready
            },
            quiver_state: if poisoned {
                SubsystemState::Blocked
            } else if buffer_pressure >= 8_000 {
                SubsystemState::Degraded
            } else {
                SubsystemState::Ready
            },
            dfe_state: SubsystemState::Disabled,
            accepting_deliveries: self.is_ready() && pressure.accepting,
            tables,
        })
    }
}

impl HealthReporter for CollectorStatus {
    fn health(&self) -> HealthStatus {
        match self.flags.read() {
            Ok(flags) if flags.healthy => HealthStatus::healthy(),
            Ok(flags) => HealthStatus::unhealthy(
                flags
                    .fatal_reason
                    .clone()
                    .unwrap_or_else(|| "collector control plane failed".to_owned()),
            ),
            Err(_) => HealthStatus::unhealthy("collector status lock failed"),
        }
    }
}

impl ReadinessReporter for CollectorStatus {
    fn readiness(&self) -> ReadinessStatus {
        let active = self.drain.active_streams();
        let accepting = self.drain.is_accepting();
        let ready = self.is_ready();
        let reason = if ready {
            None
        } else if !accepting {
            Some("collector is draining".to_owned())
        } else {
            Some("SQL/catalog, registration, lease, or Delta readiness is incomplete".to_owned())
        };
        ReadinessStatus::new(ready, accepting && ready, active, reason)
    }
}

#[async_trait]
impl CapabilityReporter for CollectorStatus {
    async fn capabilities(&self) -> RuntimeResult<CollectorCapabilities> {
        Ok(CollectorCapabilities::new(
            self.instance_id,
            ImplementationPool::Direct,
            self.max_request_bytes,
            self.max_concurrent_streams,
            self.max_pending_receipts,
            BTreeSet::from([
                Capability::ReceiptStatus,
                Capability::DeltaPublication,
                Capability::TableLeaseFencing,
                Capability::PressureReporting,
            ]),
        ))
    }
}

#[async_trait]
impl PressureReporter for CollectorStatus {
    async fn pressure(&self) -> RuntimeResult<PressureSnapshot> {
        let now = now_unix_millis().map_err(|_| RuntimeError::internal())?;
        let pressure = self.buffer.pressure().map_err(|_| RuntimeError::internal())?;
        let active = gauge(
            u64::try_from(self.drain.active_streams()).unwrap_or(u64::MAX),
            self.max_concurrent_streams.get(),
        )
        .map_err(|_| RuntimeError::internal())?;
        let buffered =
            gauge(pressure.global.bytes, pressure.global.limits.bytes).map_err(|_| RuntimeError::internal())?;
        let publication = gauge(
            u64::try_from(pressure.active_flushes).unwrap_or(u64::MAX),
            self.max_pending_publications,
        )
        .map_err(|_| RuntimeError::internal())?;
        let memory = buffered.utilization();
        Ok(PressureSnapshot::new(
            self.instance_id,
            now,
            active,
            buffered,
            publication,
            BasisPoints::new(0).map_err(|_| RuntimeError::internal())?,
            memory,
            self.is_ready() && pressure.accepting,
        ))
    }
}

/// Background loops and their fatal-error receiver.
pub struct ControlLoops {
    /// Loop tasks.
    pub tasks: Vec<JoinHandle<()>>,
    /// First fail-closed loop error.
    pub fatal: mpsc::UnboundedReceiver<anyhow::Error>,
}

/// Registers the process, reconciles receipts, and starts heartbeat, pressure,
/// lease-renewal, and dependency-probe loops.
///
/// # Errors
///
/// Returns startup registration or reconciliation failures.
#[allow(clippy::too_many_arguments)]
pub async fn start_control_loops(
    config: &CollectorConfig,
    instance_id: CollectorInstanceId,
    status: Arc<CollectorStatus>,
    routing: Arc<dyn RoutingStore>,
    receipts: Arc<dyn ReceiptStore>,
    catalog: Arc<dyn CatalogStore>,
    metastore: Arc<dyn Metastore>,
    leases: Arc<TableLeaseManager>,
    drain: DrainController,
    metrics: CollectorMetrics,
    cancellation: CancellationToken,
) -> Result<ControlLoops> {
    reconcile_receipts(
        Arc::clone(&receipts),
        config.receipts.reconciliation_batch_size,
        &metrics,
    )
    .await?;
    let now = now_unix_millis()?;
    let owner_expiry = add_duration(now, Duration::from_millis(config.receipts.owner_lease_ttl_ms))?;
    receipts
        .register_owner(OwnerRegistration {
            instance_id,
            instance_key: config.identity.instance_key.clone(),
            generation: config.identity.generation,
            host_name: config.identity.host_name.clone(),
            build_version: config.identity.build_version.clone(),
            started_at_unix_ms: now,
            lease_deadline_unix_ms: owner_expiry,
        })
        .await
        .context("register receipt owner")?;
    let registration = registration(config, instance_id, now)?;
    routing
        .register_collector(&registration)
        .await
        .context("register routing collector")?;
    status.set_registration_ready(true);
    leases.renew_all().await?;
    status.set_lease_loop_ready(true);
    let initial_pressure = status.rich_pressure().await?;
    routing
        .publish_pressure(&initial_pressure)
        .await
        .context("publish initial routing pressure")?;
    status.set_pressure_ready(true);

    let (fatal_tx, fatal) = mpsc::unbounded_channel();
    let tasks = vec![
        tokio::spawn(owner_heartbeat_loop(
            config.receipts.owner_heartbeat_ms,
            config.receipts.owner_lease_ttl_ms,
            instance_id,
            Arc::clone(&receipts),
            Arc::clone(&status),
            Arc::clone(&leases),
            drain.clone(),
            metrics.clone(),
            cancellation.clone(),
            fatal_tx.clone(),
        )),
        tokio::spawn(routing_loop(
            config.routing.registration_heartbeat_ms,
            config.routing.registration_ttl_ms,
            config.routing.pressure_interval_ms,
            instance_id,
            Arc::clone(&routing),
            Arc::clone(&status),
            Arc::clone(&leases),
            drain.clone(),
            metrics.clone(),
            cancellation.clone(),
            fatal_tx.clone(),
        )),
        tokio::spawn(lease_loop(
            config.receipts.table_lease_renew_ms,
            Arc::clone(&leases),
            Arc::clone(&status),
            drain.clone(),
            metrics.clone(),
            cancellation.clone(),
            fatal_tx.clone(),
        )),
        tokio::spawn(dependency_loop(
            config.routing.dependency_probe_interval_ms,
            config.delta.root.clone(),
            catalog,
            metastore,
            status,
            metrics,
            cancellation,
        )),
    ];
    Ok(ControlLoops { tasks, fatal })
}

async fn reconcile_receipts(receipts: Arc<dyn ReceiptStore>, limit: usize, metrics: &CollectorMetrics) -> Result<()> {
    loop {
        let report = receipts
            .reconcile_expired(now_unix_millis()?, limit)
            .await
            .context("reconcile expired receipts")?;
        for record in &report.reconciled {
            metrics.reconciled_receipt(match record.state {
                ReceiptState::Failed => "failed_retryable",
                ReceiptState::PublicationUnknown => "publication_unknown",
                _ => "unexpected",
            });
        }
        if !report.limit_reached {
            return Ok(());
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn owner_heartbeat_loop(
    interval_ms: u64,
    ttl_ms: u64,
    instance_id: CollectorInstanceId,
    receipts: Arc<dyn ReceiptStore>,
    status: Arc<CollectorStatus>,
    leases: Arc<TableLeaseManager>,
    drain: DrainController,
    metrics: CollectorMetrics,
    cancellation: CancellationToken,
    fatal: mpsc::UnboundedSender<anyhow::Error>,
) {
    let mut interval = tokio::time::interval(Duration::from_millis(interval_ms));
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    interval.tick().await;
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => return,
            _ = interval.tick() => {
                let result = async {
                    let now = now_unix_millis()?;
                    receipts
                        .heartbeat_owner(instance_id, add_duration(now, Duration::from_millis(ttl_ms))?)
                        .await
                        .context("heartbeat receipt owner")
                }.await;
                if let Err(error) = result {
                    fail_closed("receipt_owner", error, &status, &leases, &drain, &metrics, &fatal).await;
                    return;
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn routing_loop(
    heartbeat_ms: u64,
    ttl_ms: u64,
    pressure_ms: u64,
    instance_id: CollectorInstanceId,
    routing: Arc<dyn RoutingStore>,
    status: Arc<CollectorStatus>,
    leases: Arc<TableLeaseManager>,
    drain: DrainController,
    metrics: CollectorMetrics,
    cancellation: CancellationToken,
    fatal: mpsc::UnboundedSender<anyhow::Error>,
) {
    let mut heartbeat = tokio::time::interval(Duration::from_millis(heartbeat_ms));
    let mut pressure = tokio::time::interval(Duration::from_millis(pressure_ms));
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    pressure.set_missed_tick_behavior(MissedTickBehavior::Delay);
    heartbeat.tick().await;
    pressure.tick().await;
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => return,
            _ = heartbeat.tick() => {
                let result = async {
                    let now = now_unix_millis()?;
                    routing
                        .heartbeat_collector(instance_id, now, add_duration(now, Duration::from_millis(ttl_ms))?)
                        .await
                        .context("heartbeat routing registration")
                }.await;
                match result {
                    Ok(()) => status.set_registration_ready(true),
                    Err(error) => {
                        fail_closed("routing_registration", error, &status, &leases, &drain, &metrics, &fatal).await;
                        return;
                    }
                }
            }
            _ = pressure.tick() => {
                match status.rich_pressure().await {
                    Ok(snapshot) => {
                        if let Err(error) = routing.publish_pressure(&snapshot).await {
                            status.set_pressure_ready(false);
                            metrics.control_failure("pressure");
                            warn!(%error, "could not publish routing pressure snapshot");
                        } else {
                            status.set_pressure_ready(true);
                        }
                    }
                    Err(error) => {
                        status.set_pressure_ready(false);
                        metrics.control_failure("pressure");
                        warn!(%error, "could not construct routing pressure snapshot");
                    }
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn lease_loop(
    renew_ms: u64,
    leases: Arc<TableLeaseManager>,
    status: Arc<CollectorStatus>,
    drain: DrainController,
    metrics: CollectorMetrics,
    cancellation: CancellationToken,
    fatal: mpsc::UnboundedSender<anyhow::Error>,
) {
    let mut interval = tokio::time::interval(Duration::from_millis(renew_ms));
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    interval.tick().await;
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => return,
            _ = interval.tick() => {
                match leases.renew_all().await {
                    Ok(()) => status.set_lease_loop_ready(true),
                    Err(error) => {
                        status.set_lease_loop_ready(false);
                        fail_closed("table_lease", error, &status, &leases, &drain, &metrics, &fatal).await;
                        return;
                    }
                }
            }
        }
    }
}

async fn dependency_loop(
    interval_ms: u64,
    delta_root: std::path::PathBuf,
    catalog: Arc<dyn CatalogStore>,
    metastore: Arc<dyn Metastore>,
    status: Arc<CollectorStatus>,
    metrics: CollectorMetrics,
    cancellation: CancellationToken,
) {
    let probe_table = match health_probe_table() {
        Ok(table) => table,
        Err(error) => {
            status.set_fatal("could not construct dependency probe identity");
            error!(%error, "dependency probe identity is invalid");
            return;
        }
    };
    let mut interval = tokio::time::interval(Duration::from_millis(interval_ms));
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => return,
            _ = interval.tick() => {
                let catalog_ready = catalog.incomplete_sagas().await.is_ok();
                let hive_ready = metastore.table(&probe_table).await.is_ok();
                let delta_ready = verify_writable_root(&delta_root).await.is_ok();
                status.set_dependencies_ready(catalog_ready && hive_ready);
                status.set_delta_writable(delta_ready);
                if !catalog_ready || !hive_ready {
                    metrics.control_failure("dependency_probe");
                }
                if !delta_ready {
                    metrics.control_failure("delta_probe");
                }
            }
        }
    }
}

async fn fail_closed(
    subsystem: &'static str,
    error: anyhow::Error,
    status: &CollectorStatus,
    leases: &TableLeaseManager,
    drain: &DrainController,
    metrics: &CollectorMetrics,
    fatal: &mpsc::UnboundedSender<anyhow::Error>,
) {
    metrics.control_failure(subsystem);
    status.set_fatal(format!("{subsystem} control loop failed"));
    drain.begin_drain();
    leases.invalidate_all(&format!("{subsystem} control loop failed")).await;
    error!(%error, subsystem, "collector failed closed");
    let _ = fatal.send(error.context(format!("{subsystem} control loop failed")));
}

fn registration(
    config: &CollectorConfig,
    instance_id: CollectorInstanceId,
    now: UnixMillis,
) -> Result<CollectorRegistration> {
    Ok(CollectorRegistration {
        instance_id,
        instance_key: config.identity.instance_key.clone(),
        implementation_pool: ImplementationPool::Direct,
        flight_endpoint: config.flight.advertised_endpoint,
        catalog_capable: true,
        max_concurrent_streams: PositiveMeasurement::new(config.flight.max_concurrent_streams)?,
        started_at_unix_ms: now,
        heartbeat_at_unix_ms: now,
        lease_expires_at_unix_ms: add_duration(now, Duration::from_millis(config.routing.registration_ttl_ms))?,
    })
}

fn table_pressure_document(
    table: TableIdentity,
    pressure: &arrowcol_buffer::TablePressureSnapshot,
    active_streams: u64,
) -> Result<TablePressure> {
    Ok(TablePressure {
        table,
        active_streams: Measurement::new(active_streams)?,
        buffered_bytes: Measurement::new(pressure.bytes)?,
        buffered_rows: Measurement::new(pressure.rows)?,
        buffered_batches: Measurement::new(pressure.batches)?,
        oldest_buffer_age_ms: Measurement::ZERO,
        pending_commits: Measurement::ZERO,
    })
}

fn gauge(current: u64, limit: u64) -> Result<PressureGauge> {
    let limit = PositiveMeasurement::new(limit)?;
    PressureGauge::new(Measurement::new(current.min(limit.get()))?, limit).map_err(Into::into)
}

fn publication_capacity(config: &CollectorConfig) -> Result<u64> {
    u64::try_from(config.delta.max_pending_files).context("publication queue capacity exceeds protocol range")
}

/// Builds the portable table identity used only for dependency read probes.
///
/// # Errors
///
/// Returns an identity-validation error if a checked-in static component is
/// changed to an invalid value.
pub fn health_probe_table() -> Result<TableIdentity> {
    use arrowcol_protocol::{CatalogName, SchemaName, TableName};
    Ok(TableIdentity::new(
        CatalogName::new("_health")?,
        SchemaName::new("probe")?,
        TableName::new("connectivity")?,
    ))
}

fn read_rss_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|line| line.starts_with("VmRSS:"))?;
    let kib = line.split_whitespace().nth(1)?.parse::<u64>().ok()?;
    kib.checked_mul(1_024)
}

/// Marks the routing registration as draining or terminal.
///
/// # Errors
///
/// Returns routing-store or timestamp failures.
pub async fn set_routing_state(
    routing: &dyn RoutingStore,
    instance_id: CollectorInstanceId,
    state: CollectorState,
) -> Result<()> {
    routing
        .set_collector_state(instance_id, state, now_unix_millis()?)
        .await
        .context("update routing collector state")
}

/// Begins receipt-owner drain.
///
/// # Errors
///
/// Returns receipt-store failures.
pub async fn start_owner_drain(
    receipts: &dyn ReceiptStore,
    instance_id: CollectorInstanceId,
) -> Result<arrowcol_receipts::OwnerDrain> {
    receipts
        .start_owner_drain(instance_id)
        .await
        .context("start receipt owner drain")
}

/// Completes receipt-owner drain.
///
/// # Errors
///
/// Returns receipt-store or timestamp failures.
pub async fn finish_owner_drain(receipts: &dyn ReceiptStore, instance_id: CollectorInstanceId) -> Result<()> {
    receipts
        .finish_owner_drain(instance_id, now_unix_millis()?)
        .await
        .context("finish receipt owner drain")
}

/// Converts a buffer pressure snapshot into a bounded pending count.
#[must_use]
pub fn pending_receipts(pressure: &BufferPressureSnapshot) -> u64 {
    pressure.global.batches
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use super::*;

    /// Scenario: Every bounded file-writing slot is doing useful work.
    /// Guarantees: Normal worker utilization does not become hard publication-queue pressure.
    #[test]
    fn busy_flush_workers_are_not_a_full_publication_queue() {
        let config = CollectorConfig::from_yaml(include_bytes!("../config.yaml")).expect("configuration");
        let capacity = publication_capacity(&config).expect("capacity");
        assert_eq!(capacity, config.delta.max_pending_files as u64);
        let busy = gauge(config.buffer.max_concurrent_flushes as u64, capacity).expect("busy");
        assert!(busy.utilization().get() < 9_500);
        let full = gauge(capacity, capacity).expect("full");
        assert_eq!(full.utilization().get(), BasisPoints::FULL_SCALE);
    }
}
