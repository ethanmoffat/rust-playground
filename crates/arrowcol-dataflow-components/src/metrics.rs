use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};

use arrowcol_catalog::SchemaVersion;
use arrowcol_protocol::{
    BasisPoints, Capability, CollectorInstanceId, ImplementationPool, Measurement, PositiveMeasurement, PressureGauge,
    ProtocolVersion, TableIdentity,
};
use arrowcol_routing::{RichPressureSnapshot, SubsystemState, TablePressure};
use serde::Serialize;

use crate::QUIVER_UNSUPPORTED_GAP;
use crate::config::now_unix_millis;

static PROCESS_INSTANCE_ID: LazyLock<CollectorInstanceId> = LazyLock::new(CollectorInstanceId::new);
static GLOBAL_METRICS: LazyLock<Arc<RuntimeMetrics>> =
    LazyLock::new(|| Arc::new(RuntimeMetrics::new(*PROCESS_INSTANCE_ID)));
const MAX_TABLE_PRESSURE_ENTRIES: usize = 256;

/// Returns the process-lifetime collector identifier shared by every DFE core.
#[must_use]
pub fn process_instance_id() -> CollectorInstanceId {
    *PROCESS_INSTANCE_ID
}

/// Returns process-wide Arrowcol component metrics.
#[must_use]
pub fn global_metrics() -> Arc<RuntimeMetrics> {
    Arc::clone(&GLOBAL_METRICS)
}

#[derive(Clone, Debug, Default)]
struct TableState {
    active_streams: u64,
    buffered_bytes: u64,
    buffered_rows: u64,
    buffered_batches: u64,
    pending_commits: u64,
    oldest_buffered_at_ms: Option<u64>,
    source_schema_version: Option<u64>,
    target_schema_version: Option<u64>,
    last_delta_version: Option<u64>,
    publication_failures: u64,
}

/// Atomic process-wide counters shared by receiver, exporter, admin, and registration.
pub struct RuntimeMetrics {
    instance_id: CollectorInstanceId,
    accepting: AtomicBool,
    healthy: AtomicBool,
    catalog_ready: AtomicBool,
    registered: AtomicBool,
    active_streams: AtomicU64,
    received_bytes: AtomicU64,
    received_rows: AtomicU64,
    received_batches: AtomicU64,
    buffered_bytes: AtomicU64,
    buffered_rows: AtomicU64,
    buffered_batches: AtomicU64,
    pending_commits: AtomicU64,
    admitted_streams: AtomicU64,
    aborted_streams: AtomicU64,
    downstream_acks: AtomicU64,
    downstream_nacks: AtomicU64,
    delta_files: AtomicU64,
    delta_rows: AtomicU64,
    delta_bytes: AtomicU64,
    delta_failures: AtomicU64,
    max_concurrent_streams: AtomicU64,
    rss_limit_bytes: AtomicU64,
    buffered_bytes_limit: AtomicU64,
    buffered_rows_limit: AtomicU64,
    buffered_batches_limit: AtomicU64,
    max_pending_commits: AtomicU64,
    dfe_channel_capacity: AtomicU64,
    tables: Mutex<BTreeMap<TableIdentity, TableState>>,
}

impl RuntimeMetrics {
    /// Creates isolated metrics for tests or an embedded collector.
    #[must_use]
    pub fn new(instance_id: CollectorInstanceId) -> Self {
        Self {
            instance_id,
            accepting: AtomicBool::new(true),
            healthy: AtomicBool::new(true),
            catalog_ready: AtomicBool::new(false),
            registered: AtomicBool::new(false),
            active_streams: AtomicU64::new(0),
            received_bytes: AtomicU64::new(0),
            received_rows: AtomicU64::new(0),
            received_batches: AtomicU64::new(0),
            buffered_bytes: AtomicU64::new(0),
            buffered_rows: AtomicU64::new(0),
            buffered_batches: AtomicU64::new(0),
            pending_commits: AtomicU64::new(0),
            admitted_streams: AtomicU64::new(0),
            aborted_streams: AtomicU64::new(0),
            downstream_acks: AtomicU64::new(0),
            downstream_nacks: AtomicU64::new(0),
            delta_files: AtomicU64::new(0),
            delta_rows: AtomicU64::new(0),
            delta_bytes: AtomicU64::new(0),
            delta_failures: AtomicU64::new(0),
            max_concurrent_streams: AtomicU64::new(1),
            rss_limit_bytes: AtomicU64::new(1),
            buffered_bytes_limit: AtomicU64::new(1),
            buffered_rows_limit: AtomicU64::new(1),
            buffered_batches_limit: AtomicU64::new(1),
            max_pending_commits: AtomicU64::new(1),
            dfe_channel_capacity: AtomicU64::new(1),
            tables: Mutex::new(BTreeMap::new()),
        }
    }

    pub(crate) fn configure_receiver(
        &self,
        max_concurrent_streams: usize,
        rss_limit_bytes: u64,
        buffered_bytes_limit: u64,
        buffered_rows_limit: u64,
        buffered_batches_limit: u64,
        dfe_channel_capacity: usize,
    ) {
        self.max_concurrent_streams.store(
            u64::try_from(max_concurrent_streams).unwrap_or(u64::MAX),
            Ordering::Release,
        );
        self.rss_limit_bytes.store(rss_limit_bytes, Ordering::Release);
        self.buffered_bytes_limit.store(buffered_bytes_limit, Ordering::Release);
        self.buffered_rows_limit.store(buffered_rows_limit, Ordering::Release);
        self.buffered_batches_limit
            .store(buffered_batches_limit, Ordering::Release);
        self.dfe_channel_capacity.store(
            u64::try_from(dfe_channel_capacity).unwrap_or(u64::MAX),
            Ordering::Release,
        );
    }

    pub(crate) fn configure_exporter(&self, max_pending_commits: usize) {
        self.max_pending_commits.store(
            u64::try_from(max_pending_commits).unwrap_or(u64::MAX),
            Ordering::Release,
        );
    }

    pub(crate) fn set_accepting(&self, accepting: bool) {
        self.accepting.store(accepting, Ordering::Release);
    }

    pub(crate) fn set_healthy(&self, healthy: bool) {
        self.healthy.store(healthy, Ordering::Release);
    }

    pub(crate) fn set_catalog_ready(&self, ready: bool) {
        self.catalog_ready.store(ready, Ordering::Release);
    }

    pub(crate) fn set_registered(&self, registered: bool) {
        self.registered.store(registered, Ordering::Release);
    }

    pub(crate) fn stream_started(&self, table: &TableIdentity) {
        self.active_streams.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut tables) = self.tables.lock() {
            let state = tables.entry(table.clone()).or_default();
            state.active_streams = state.active_streams.saturating_add(1);
        }
    }

    pub(crate) fn stream_completed(&self, table: &TableIdentity) {
        saturating_sub_atomic(&self.active_streams, 1);
        self.admitted_streams.fetch_add(1, Ordering::Relaxed);
        self.update_table(table, |state| {
            state.active_streams = state.active_streams.saturating_sub(1);
        });
    }

    pub(crate) fn stream_aborted(&self, table: &TableIdentity) {
        saturating_sub_atomic(&self.active_streams, 1);
        self.aborted_streams.fetch_add(1, Ordering::Relaxed);
        self.update_table(table, |state| {
            state.active_streams = state.active_streams.saturating_sub(1);
        });
    }

    pub(crate) fn batch_received(&self, rows: u64, bytes: u64) {
        self.received_batches.fetch_add(1, Ordering::Relaxed);
        self.received_rows.fetch_add(rows, Ordering::Relaxed);
        self.received_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub(crate) fn batch_buffered(
        &self,
        table: &TableIdentity,
        rows: u64,
        bytes: u64,
        source: SchemaVersion,
        target: SchemaVersion,
        enqueued_at_ms: u64,
    ) {
        self.buffered_batches.fetch_add(1, Ordering::Relaxed);
        self.buffered_rows.fetch_add(rows, Ordering::Relaxed);
        self.buffered_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.update_table(table, |state| {
            state.buffered_batches = state.buffered_batches.saturating_add(1);
            state.buffered_rows = state.buffered_rows.saturating_add(rows);
            state.buffered_bytes = state.buffered_bytes.saturating_add(bytes);
            state.oldest_buffered_at_ms = Some(
                state
                    .oldest_buffered_at_ms
                    .map_or(enqueued_at_ms, |oldest| oldest.min(enqueued_at_ms)),
            );
            state.source_schema_version = Some(source.get());
            state.target_schema_version = Some(target.get());
        });
    }

    pub(crate) fn batch_resolved(
        &self,
        table: &TableIdentity,
        rows: u64,
        bytes: u64,
        downstream_ack: bool,
        remaining_oldest: Option<u64>,
    ) {
        saturating_sub_atomic(&self.buffered_batches, 1);
        saturating_sub_atomic(&self.buffered_rows, rows);
        saturating_sub_atomic(&self.buffered_bytes, bytes);
        if downstream_ack {
            self.downstream_acks.fetch_add(1, Ordering::Relaxed);
        } else {
            self.downstream_nacks.fetch_add(1, Ordering::Relaxed);
        }
        self.update_table(table, |state| {
            state.buffered_batches = state.buffered_batches.saturating_sub(1);
            state.buffered_rows = state.buffered_rows.saturating_sub(rows);
            state.buffered_bytes = state.buffered_bytes.saturating_sub(bytes);
            state.oldest_buffered_at_ms = remaining_oldest;
        });
    }

    pub(crate) fn publication_started(&self, table: &TableIdentity) {
        self.pending_commits.fetch_add(1, Ordering::Relaxed);
        self.update_table(table, |state| {
            state.pending_commits = state.pending_commits.saturating_add(1);
        });
    }

    pub(crate) fn observe_schema_versions(&self, table: &TableIdentity, source: SchemaVersion, target: SchemaVersion) {
        self.update_table(table, |state| {
            state.source_schema_version = Some(source.get());
            state.target_schema_version = Some(target.get());
        });
    }

    pub(crate) fn publication_finished(
        &self,
        table: &TableIdentity,
        rows: u64,
        bytes: u64,
        delta_version: Option<u64>,
    ) {
        saturating_sub_atomic(&self.pending_commits, 1);
        self.update_table(table, |state| {
            state.pending_commits = state.pending_commits.saturating_sub(1);
            if let Some(version) = delta_version {
                state.last_delta_version = Some(version);
            } else {
                state.publication_failures = state.publication_failures.saturating_add(1);
            }
        });
        if delta_version.is_some() {
            self.delta_files.fetch_add(1, Ordering::Relaxed);
            self.delta_rows.fetch_add(rows, Ordering::Relaxed);
            self.delta_bytes.fetch_add(bytes, Ordering::Relaxed);
        } else {
            self.delta_failures.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn update_table(&self, table: &TableIdentity, update: impl FnOnce(&mut TableState)) {
        if let Ok(mut tables) = self.tables.lock() {
            update(tables.entry(table.clone()).or_default());
        }
    }

    /// Returns current process health.
    #[must_use]
    pub fn healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
    }

    /// Returns routing readiness and admission state.
    #[must_use]
    pub fn ready(&self) -> bool {
        self.healthy()
            && self.catalog_ready.load(Ordering::Acquire)
            && self.registered.load(Ordering::Acquire)
            && self.accepting.load(Ordering::Acquire)
    }

    /// Returns the dashboard capability document with explicit delivery semantics.
    #[must_use]
    pub fn capabilities(&self, max_request_bytes: u64) -> DashboardCapabilities {
        DashboardCapabilities {
            protocol_version: ProtocolVersion::CURRENT,
            instance_id: self.instance_id,
            implementation_pool: ImplementationPool::Dataflow,
            delivery_guarantee: "best_effort",
            completion_mode: "best_effort_admission",
            receipt_polling: false,
            sql_delivery_receipts: false,
            at_least_once: false,
            max_request_bytes,
            max_concurrent_streams: self.max_concurrent_streams.load(Ordering::Acquire),
            capabilities: [
                Capability::DeltaPublication,
                Capability::TableLeaseFencing,
                Capability::PressureReporting,
            ],
            arbitrary_arrow: true,
            pdata_signal_discriminator: "logs",
            pdata_signal_semantics: "representation_local_not_otap",
            strict_catalog_validation: true,
            flight_sql_control: true,
            dfe_backpressure: true,
            native_quiver: NativeQuiverCapability {
                supported: false,
                feature_gap: QUIVER_UNSUPPORTED_GAP,
            },
        }
    }

    /// Builds the bounded router pressure document and dashboard annotations.
    ///
    /// # Errors
    ///
    /// Returns a clock or bounded-measurement error when a snapshot cannot be
    /// represented by the routing contract.
    pub fn pressure(&self) -> Result<DashboardPressure, String> {
        let now = now_unix_millis()?;
        let active = self.active_streams.load(Ordering::Acquire);
        let buffered_bytes = self.buffered_bytes.load(Ordering::Acquire);
        let buffered_rows = self.buffered_rows.load(Ordering::Acquire);
        let buffered_batches = self.buffered_batches.load(Ordering::Acquire);
        let pending_commits = self.pending_commits.load(Ordering::Acquire);
        let max_streams = self.max_concurrent_streams.load(Ordering::Acquire).max(1);
        let rss_limit = self.rss_limit_bytes.load(Ordering::Acquire).max(1);
        let actual_rss = resident_memory_bytes().unwrap_or(0);
        let bounded_rss = actual_rss.min(rss_limit);
        let bytes_limit = self.buffered_bytes_limit.load(Ordering::Acquire).max(1);
        let rows_limit = self.buffered_rows_limit.load(Ordering::Acquire).max(1);
        let batches_limit = self.buffered_batches_limit.load(Ordering::Acquire).max(1);
        let commit_limit = self.max_pending_commits.load(Ordering::Acquire).max(1);
        let available = max_streams.saturating_sub(active.min(max_streams));
        let oldest_age = self.oldest_buffer_age(now.get());
        let accepting = self.accepting.load(Ordering::Acquire)
            && actual_rss <= rss_limit
            && buffered_bytes <= bytes_limit
            && buffered_rows <= rows_limit
            && buffered_batches <= batches_limit
            && pending_commits <= commit_limit;
        let tables = self.table_pressure(now.get());
        let dfe_pressure = ratio_bps(buffered_batches.min(batches_limit), batches_limit)?;
        let delta_pressure = ratio_bps(pending_commits.min(commit_limit), commit_limit)?;
        let rich = RichPressureSnapshot {
            instance_id: self.instance_id,
            observed_at_unix_ms: now,
            cpu_utilization_bps: BasisPoints::new(0).map_err(|error| error.to_string())?,
            rss_bytes: measurement(bounded_rss)?,
            rss_limit_bytes: positive(rss_limit)?,
            active_streams: gauge(active, max_streams)?,
            received_bytes: measurement(self.received_bytes.load(Ordering::Acquire))?,
            received_rows: measurement(self.received_rows.load(Ordering::Acquire))?,
            received_batches: measurement(self.received_batches.load(Ordering::Acquire))?,
            buffered_bytes: gauge(buffered_bytes, bytes_limit)?,
            buffered_rows: gauge(buffered_rows, rows_limit)?,
            buffered_batches: gauge(buffered_batches, batches_limit)?,
            available_permits: gauge(available, max_streams)?,
            pending_commits: gauge(pending_commits, commit_limit)?,
            oldest_buffer_age_ms: measurement(oldest_age)?,
            delta_pressure_bps: delta_pressure,
            quiver_pressure_bps: BasisPoints::new(0).map_err(|error| error.to_string())?,
            dfe_pressure_bps: dfe_pressure,
            delta_state: if pending_commits > commit_limit {
                SubsystemState::Blocked
            } else if pending_commits > 0 {
                SubsystemState::Degraded
            } else {
                SubsystemState::Ready
            },
            quiver_state: SubsystemState::Disabled,
            dfe_state: if !accepting {
                SubsystemState::Blocked
            } else if buffered_batches > 0 {
                SubsystemState::Degraded
            } else {
                SubsystemState::Ready
            },
            accepting_deliveries: accepting,
            tables,
        };
        Ok(DashboardPressure {
            rich,
            table_versions: self.table_versions(),
            actual_rss_bytes: actual_rss,
            dfe_channel_capacity: self.dfe_channel_capacity.load(Ordering::Acquire),
            dfe_channel_occupancy: buffered_batches,
            dfe_channel_occupancy_source: "ack_tracked_end_to_end",
            native_quiver_buffered_bytes: None,
            native_quiver_feature_gap: QUIVER_UNSUPPORTED_GAP,
        })
    }

    fn oldest_buffer_age(&self, now_ms: u64) -> u64 {
        self.tables
            .lock()
            .ok()
            .and_then(|tables| tables.values().filter_map(|state| state.oldest_buffered_at_ms).min())
            .map_or(0, |oldest| now_ms.saturating_sub(oldest))
    }

    fn table_pressure(&self, now_ms: u64) -> Vec<TablePressure> {
        self.tables.lock().map_or_else(
            |_| Vec::new(),
            |tables| {
                tables
                    .iter()
                    .take(MAX_TABLE_PRESSURE_ENTRIES)
                    .filter_map(|(table, state)| {
                        let effective_active = state
                            .active_streams
                            .saturating_add(state.buffered_batches)
                            .saturating_add(state.pending_commits);
                        Some(TablePressure {
                            table: table.clone(),
                            active_streams: Measurement::new(effective_active).ok()?,
                            buffered_bytes: Measurement::new(state.buffered_bytes).ok()?,
                            buffered_rows: Measurement::new(state.buffered_rows).ok()?,
                            buffered_batches: Measurement::new(state.buffered_batches).ok()?,
                            oldest_buffer_age_ms: Measurement::new(
                                state
                                    .oldest_buffered_at_ms
                                    .map_or(0, |oldest| now_ms.saturating_sub(oldest)),
                            )
                            .ok()?,
                            pending_commits: Measurement::new(state.pending_commits).ok()?,
                        })
                    })
                    .collect()
            },
        )
    }

    fn table_versions(&self) -> Vec<TableVersionStatus> {
        self.tables.lock().map_or_else(
            |_| Vec::new(),
            |tables| {
                tables
                    .iter()
                    .take(MAX_TABLE_PRESSURE_ENTRIES)
                    .map(|(table, state)| TableVersionStatus {
                        table: table.clone(),
                        source_schema_version: state.source_schema_version,
                        target_schema_version: state.target_schema_version,
                        last_delta_version: state.last_delta_version,
                        publication_failures: state.publication_failures,
                    })
                    .collect()
            },
        )
    }

    /// Encodes a compact Prometheus text exposition.
    #[must_use]
    pub fn prometheus(&self) -> String {
        let mut output = String::new();
        for (name, value) in [
            (
                "arrowcol_flight_active_streams",
                self.active_streams.load(Ordering::Acquire),
            ),
            (
                "arrowcol_received_batches_total",
                self.received_batches.load(Ordering::Acquire),
            ),
            (
                "arrowcol_received_rows_total",
                self.received_rows.load(Ordering::Acquire),
            ),
            (
                "arrowcol_received_bytes_total",
                self.received_bytes.load(Ordering::Acquire),
            ),
            (
                "arrowcol_dfe_buffered_batches",
                self.buffered_batches.load(Ordering::Acquire),
            ),
            (
                "arrowcol_dfe_buffered_bytes",
                self.buffered_bytes.load(Ordering::Acquire),
            ),
            (
                "arrowcol_delta_pending_commits",
                self.pending_commits.load(Ordering::Acquire),
            ),
            ("arrowcol_delta_files_total", self.delta_files.load(Ordering::Acquire)),
            ("arrowcol_delta_rows_total", self.delta_rows.load(Ordering::Acquire)),
            ("arrowcol_delta_bytes_total", self.delta_bytes.load(Ordering::Acquire)),
            (
                "arrowcol_delta_failures_total",
                self.delta_failures.load(Ordering::Acquire),
            ),
            (
                "arrowcol_downstream_acks_total",
                self.downstream_acks.load(Ordering::Acquire),
            ),
            (
                "arrowcol_downstream_nacks_total",
                self.downstream_nacks.load(Ordering::Acquire),
            ),
        ] {
            let _ = writeln!(output, "# TYPE {name} gauge");
            let _ = writeln!(output, "{name} {value}");
        }
        output
            .push_str("# HELP arrowcol_delivery_guarantee_info Admission is best-effort and has no receipt polling.\n");
        output.push_str("# TYPE arrowcol_delivery_guarantee_info gauge\n");
        output.push_str(
            "arrowcol_delivery_guarantee_info{delivery_guarantee=\"best_effort\",completion_mode=\"best_effort_admission\",receipt_polling=\"false\",at_least_once=\"false\"} 1\n",
        );
        output
    }
}

/// Dashboard-facing capability document.
#[derive(Clone, Debug, Serialize)]
pub struct DashboardCapabilities {
    protocol_version: ProtocolVersion,
    instance_id: CollectorInstanceId,
    implementation_pool: ImplementationPool,
    delivery_guarantee: &'static str,
    completion_mode: &'static str,
    receipt_polling: bool,
    sql_delivery_receipts: bool,
    at_least_once: bool,
    max_request_bytes: u64,
    max_concurrent_streams: u64,
    capabilities: [Capability; 3],
    arbitrary_arrow: bool,
    pdata_signal_discriminator: &'static str,
    pdata_signal_semantics: &'static str,
    strict_catalog_validation: bool,
    flight_sql_control: bool,
    dfe_backpressure: bool,
    native_quiver: NativeQuiverCapability,
}

/// Native durable-buffer capability.
#[derive(Clone, Debug, Serialize)]
pub struct NativeQuiverCapability {
    supported: bool,
    feature_gap: &'static str,
}

/// Rich pressure plus implementation-specific occupancy provenance.
#[derive(Clone, Debug, Serialize)]
pub struct DashboardPressure {
    /// Router-consumable rich pressure document.
    pub rich: RichPressureSnapshot,
    /// Catalog and Delta versions observed per bounded table entry.
    pub table_versions: Vec<TableVersionStatus>,
    actual_rss_bytes: u64,
    dfe_channel_capacity: u64,
    dfe_channel_occupancy: u64,
    dfe_channel_occupancy_source: &'static str,
    native_quiver_buffered_bytes: Option<u64>,
    native_quiver_feature_gap: &'static str,
}

/// Version lineage retained for one logical table.
#[derive(Clone, Debug, Serialize)]
pub struct TableVersionStatus {
    table: TableIdentity,
    source_schema_version: Option<u64>,
    target_schema_version: Option<u64>,
    last_delta_version: Option<u64>,
    publication_failures: u64,
}

#[derive(Clone, Debug)]
struct PendingBatch {
    table: TableIdentity,
    rows: u64,
    bytes: u64,
    enqueued_at_ms: u64,
}

/// Correlates DFE Ack/Nack feedback with pressure accounting.
pub(crate) struct PendingTracker {
    metrics: Arc<RuntimeMetrics>,
    next_id: AtomicU64,
    pending: Mutex<HashMap<u64, PendingBatch>>,
}

impl PendingTracker {
    pub(crate) fn new(metrics: Arc<RuntimeMetrics>) -> Self {
        Self {
            metrics,
            next_id: AtomicU64::new(1),
            pending: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn insert(
        &self,
        table: TableIdentity,
        rows: u64,
        bytes: u64,
        source: SchemaVersion,
        target: SchemaVersion,
    ) -> Result<u64, String> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        if id == u64::MAX {
            return Err("DFE message correlation ID exhausted".to_owned());
        }
        let now = now_unix_millis()?.get();
        self.metrics.batch_buffered(&table, rows, bytes, source, target, now);
        self.pending
            .lock()
            .map_err(|_| "pending DFE tracker lock poisoned".to_owned())?
            .insert(
                id,
                PendingBatch {
                    table,
                    rows,
                    bytes,
                    enqueued_at_ms: now,
                },
            );
        Ok(id)
    }

    pub(crate) fn resolve(&self, id: u64, ack: bool) {
        let Ok(mut pending) = self.pending.lock() else {
            return;
        };
        let Some(batch) = pending.remove(&id) else {
            return;
        };
        let remaining_oldest = pending
            .values()
            .filter(|other| other.table == batch.table)
            .map(|other| other.enqueued_at_ms)
            .min();
        drop(pending);
        self.metrics
            .batch_resolved(&batch.table, batch.rows, batch.bytes, ack, remaining_oldest);
    }
}

fn measurement(value: u64) -> Result<Measurement, String> {
    Measurement::new(value).map_err(|error| error.to_string())
}

fn positive(value: u64) -> Result<PositiveMeasurement, String> {
    PositiveMeasurement::new(value).map_err(|error| error.to_string())
}

fn gauge(current: u64, limit: u64) -> Result<PressureGauge, String> {
    PressureGauge::new(measurement(current.min(limit))?, positive(limit)?).map_err(|error| error.to_string())
}

fn ratio_bps(current: u64, limit: u64) -> Result<BasisPoints, String> {
    let scaled = u128::from(current) * u128::from(BasisPoints::FULL_SCALE);
    let ratio = scaled / u128::from(limit.max(1));
    let ratio = u16::try_from(ratio)
        .unwrap_or(BasisPoints::FULL_SCALE)
        .min(BasisPoints::FULL_SCALE);
    BasisPoints::new(ratio).map_err(|error| error.to_string())
}

fn saturating_sub_atomic(value: &AtomicU64, decrement: u64) {
    let _ = value.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_sub(decrement))
    });
}

fn resident_memory_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|line| line.starts_with("VmRSS:"))?;
    let kib = line.split_whitespace().nth(1)?.parse::<u64>().ok()?;
    kib.checked_mul(1_024)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use arrowcol_protocol::{CatalogName, SchemaName, TableName};

    use super::*;

    fn table() -> TableIdentity {
        TableIdentity::new(
            CatalogName::new("demo").expect("catalog"),
            SchemaName::new("generated").expect("schema"),
            TableName::new("events").expect("table"),
        )
    }

    /// Scenario: DFE work remains outstanding after Flight best-effort completion.
    /// Guarantees: Pressure remains nonzero until downstream Ack/Nack feedback resolves it.
    #[test]
    fn ack_tracked_pressure_is_honest() {
        let metrics = Arc::new(RuntimeMetrics::new(CollectorInstanceId::new()));
        metrics.configure_receiver(4, 1_000_000, 1_000_000, 1_000, 10, 10);
        metrics.configure_exporter(4);
        let tracker = PendingTracker::new(Arc::clone(&metrics));
        let id = tracker
            .insert(table(), 3, 128, SchemaVersion::FIRST, SchemaVersion::FIRST)
            .expect("insert");
        let before = metrics.pressure().expect("pressure");
        assert_eq!(before.rich.buffered_batches.current().get(), 1);
        tracker.resolve(id, true);
        let after = metrics.pressure().expect("pressure");
        assert_eq!(after.rich.buffered_batches.current().get(), 0);
    }

    /// Scenario: Operators inspect DFE delivery claims.
    /// Guarantees: Capability metadata explicitly denies receipt polling and at-least-once delivery.
    #[test]
    fn capabilities_label_best_effort() {
        let metrics = RuntimeMetrics::new(CollectorInstanceId::new());
        let encoded = serde_json::to_value(metrics.capabilities(1024)).expect("capabilities JSON");
        assert_eq!(encoded["delivery_guarantee"], "best_effort");
        assert_eq!(encoded["completion_mode"], "best_effort_admission");
        assert_eq!(encoded["receipt_polling"], false);
        assert_eq!(encoded["sql_delivery_receipts"], false);
        assert_eq!(encoded["at_least_once"], false);
        assert_eq!(encoded["pdata_signal_discriminator"], "logs");
        assert_eq!(encoded["pdata_signal_semantics"], "representation_local_not_otap");
        assert_eq!(encoded["native_quiver"]["feature_gap"], QUIVER_UNSUPPORTED_GAP);
    }
}
