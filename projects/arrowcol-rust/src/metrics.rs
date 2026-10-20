use std::sync::Arc;

use arrowcol_buffer::QuiverStage;
use arrowcol_flight::{PressureEvent, PressureHooks, StreamOutcome};
use arrowcol_sql_server::SqlPool;
use prometheus::core::{Collector, Desc};
use prometheus::proto::{Counter, Gauge, LabelPair, Metric, MetricFamily, MetricType};
use prometheus::{
    Encoder, IntCounter, IntCounterVec, IntGauge, Opts, Registry, TextEncoder, register_int_counter_vec_with_registry,
    register_int_counter_with_registry, register_int_gauge_with_registry,
};

/// Low-cardinality direct collector metrics and Flight pressure hooks.
#[derive(Clone)]
pub struct CollectorMetrics {
    registry: Arc<Registry>,
    active_streams: IntGauge,
    decoded_channel_bytes: IntGauge,
    received_batches: IntCounter,
    received_rows: IntCounter,
    received_bytes: IntCounter,
    exported_rows: IntCounter,
    stream_outcomes: IntCounterVec,
    control_failures: IntCounterVec,
    reconciled_receipts: IntCounterVec,
    incomplete_shutdown: IntGauge,
}

impl CollectorMetrics {
    /// Creates an isolated Prometheus registry.
    ///
    /// # Errors
    ///
    /// Returns a metric registration error.
    pub fn new() -> Result<Self, prometheus::Error> {
        let registry = Arc::new(Registry::new());
        let active_streams = register_int_gauge_with_registry!(
            "arrowcol_direct_active_streams",
            "Currently admitted Arrow Flight streams",
            registry.as_ref()
        )?;
        let decoded_channel_bytes = register_int_gauge_with_registry!(
            "arrowcol_direct_decoded_channel_bytes",
            "Approximate Arrow bytes waiting in the bounded Flight decoder channel",
            registry.as_ref()
        )?;
        let received_batches = register_int_counter_with_registry!(
            "arrowcol_direct_received_batches_total",
            "Record batches admitted by completed or failed streams",
            registry.as_ref()
        )?;
        let received_rows = register_int_counter_with_registry!(
            "arrowcol_direct_received_rows_total",
            "Rows admitted by completed or failed streams",
            registry.as_ref()
        )?;
        let received_bytes = register_int_counter_with_registry!(
            "arrowcol_direct_received_bytes_total",
            "Approximate Arrow bytes admitted by completed or failed streams",
            registry.as_ref()
        )?;
        let exported_rows = register_int_counter_with_registry!(
            "arrowcol_direct_exported_rows_total",
            "Rows durably committed to Delta",
            registry.as_ref()
        )?;
        let stream_outcomes = register_int_counter_vec_with_registry!(
            Opts::new(
                "arrowcol_direct_stream_outcomes_total",
                "Arrow Flight streams by final runtime outcome"
            ),
            &["outcome"],
            registry.as_ref()
        )?;
        let control_failures = register_int_counter_vec_with_registry!(
            Opts::new(
                "arrowcol_direct_control_failures_total",
                "Background control-loop failures by subsystem"
            ),
            &["subsystem"],
            registry.as_ref()
        )?;
        let reconciled_receipts = register_int_counter_vec_with_registry!(
            Opts::new(
                "arrowcol_direct_reconciled_receipts_total",
                "Startup receipt reconciliation by resulting state"
            ),
            &["state"],
            registry.as_ref()
        )?;
        let incomplete_shutdown = register_int_gauge_with_registry!(
            "arrowcol_direct_incomplete_shutdown_groups",
            "Publication groups still incomplete after graceful shutdown",
            registry.as_ref()
        )?;
        Ok(Self {
            registry,
            active_streams,
            decoded_channel_bytes,
            received_batches,
            received_rows,
            received_bytes,
            exported_rows,
            stream_outcomes,
            control_failures,
            reconciled_receipts,
            incomplete_shutdown,
        })
    }

    /// Records rows durably committed to Delta.
    pub fn exported_rows(&self, rows: u64) {
        self.exported_rows.inc_by(rows);
    }

    /// Registers fixed-role SQL pool snapshots without a sampling task.
    ///
    /// # Errors
    ///
    /// Returns an error if pool metric registration fails.
    pub fn register_sql_pools(&self, pools: Vec<(&'static str, SqlPool)>) -> Result<(), prometheus::Error> {
        self.registry.register(Box::new(SqlPoolCollector::new(pools)?))
    }

    /// Registers scrape-time Quiver durable-stage metrics.
    ///
    /// # Errors
    ///
    /// Returns an error if Quiver metric registration fails.
    pub fn register_quiver(&self, stage: Arc<QuiverStage>) -> Result<(), prometheus::Error> {
        self.registry.register(Box::new(QuiverCollector::new(stage)?))
    }

    /// Encodes Prometheus text exposition.
    ///
    /// # Errors
    ///
    /// Returns an encoding failure.
    pub fn encode(&self) -> Result<Vec<u8>, prometheus::Error> {
        let families = self.registry.gather();
        let mut output = Vec::new();
        TextEncoder::new().encode(&families, &mut output)?;
        Ok(output)
    }
}

struct SqlPoolCollector {
    pools: Vec<(&'static str, SqlPool)>,
    descriptors: Vec<Desc>,
}

const SQL_POOL_METRICS: [(&str, &str, MetricType); 9] = [
    ("created_total", "SQL connections created", MetricType::COUNTER),
    (
        "acquired_total",
        "Successful SQL connection checkouts",
        MetricType::COUNTER,
    ),
    (
        "reused_total",
        "SQL checkouts reusing an existing connection",
        MetricType::COUNTER,
    ),
    (
        "discarded_total",
        "SQL connections discarded rather than reused",
        MetricType::COUNTER,
    ),
    (
        "acquire_failures_total",
        "Failed SQL connection checkouts",
        MetricType::COUNTER,
    ),
    (
        "acquire_wait_seconds_total",
        "Cumulative SQL checkout wait in seconds",
        MetricType::COUNTER,
    ),
    ("size", "Current SQL pool connection count", MetricType::GAUGE),
    (
        "available",
        "SQL pool connections available for checkout",
        MetricType::GAUGE,
    ),
    ("max_size", "Configured SQL pool connection limit", MetricType::GAUGE),
];

impl SqlPoolCollector {
    fn new(pools: Vec<(&'static str, SqlPool)>) -> Result<Self, prometheus::Error> {
        let descriptors = SQL_POOL_METRICS
            .iter()
            .map(|(name, help, _)| {
                Desc::new(
                    format!("arrowcol_direct_sql_pool_{name}"),
                    (*help).to_owned(),
                    vec!["role".to_owned()],
                    Default::default(),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { pools, descriptors })
    }
}

impl Collector for SqlPoolCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descriptors.iter().collect()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let snapshots = self
            .pools
            .iter()
            .map(|(role, pool)| (*role, pool.stats()))
            .collect::<Vec<_>>();
        SQL_POOL_METRICS
            .iter()
            .enumerate()
            .map(|(index, (name, help, kind))| {
                let mut family = MetricFamily::default();
                family.set_name(format!("arrowcol_direct_sql_pool_{name}"));
                family.set_help((*help).to_owned());
                family.set_field_type(*kind);
                for (role, stats) in &snapshots {
                    let value = match index {
                        0 => stats.created as f64,
                        1 => stats.acquired as f64,
                        2 => stats.reused as f64,
                        3 => stats.discarded as f64,
                        4 => stats.acquire_failures as f64,
                        5 => stats.acquire_wait_seconds,
                        6 => stats.size as f64,
                        7 => stats.available as f64,
                        _ => stats.max_size as f64,
                    };
                    let mut label = LabelPair::default();
                    label.set_name("role".to_owned());
                    label.set_value((*role).to_owned());
                    let mut metric = Metric::default();
                    metric.set_label(vec![label]);
                    if *kind == MetricType::COUNTER {
                        let mut counter = Counter::default();
                        counter.set_value(value);
                        metric.set_counter(counter);
                    } else {
                        let mut gauge = Gauge::default();
                        gauge.set_value(value);
                        metric.set_gauge(gauge);
                    }
                    family.mut_metric().push(metric);
                }
                family
            })
            .collect()
    }
}

struct QuiverCollector {
    stage: Arc<QuiverStage>,
    descriptors: Vec<Desc>,
}

const QUIVER_METRICS: [(&str, &str, MetricType); 11] = [
    (
        "disk_used_bytes",
        "Bytes used on disk by the Quiver WAL and segments",
        MetricType::GAUGE,
    ),
    (
        "disk_capacity_bytes",
        "Configured Quiver disk capacity",
        MetricType::GAUGE,
    ),
    (
        "backlog_bundles",
        "Persisted batches not yet published to Delta",
        MetricType::GAUGE,
    ),
    (
        "persisted_bundles_total",
        "Batches persisted to Quiver",
        MetricType::COUNTER,
    ),
    (
        "persisted_bytes_total",
        "Logical Arrow bytes persisted to Quiver",
        MetricType::COUNTER,
    ),
    (
        "published_bundles_total",
        "Persisted batches published to Delta and removed from the backlog",
        MetricType::COUNTER,
    ),
    (
        "rejected_bundles_total",
        "Persisted batches dropped after a terminal receipt failure",
        MetricType::COUNTER,
    ),
    (
        "deferred_bundles_total",
        "Persisted batch deliveries deferred for a retry",
        MetricType::COUNTER,
    ),
    (
        "recovered_bundles_total",
        "Distinct persisted batches a crashed predecessor left unacknowledged on disk and this process replayed",
        MetricType::COUNTER,
    ),
    (
        "adopted_receipts_total",
        "Receipts adopted from a crashed predecessor or rebound to a newer lease",
        MetricType::COUNTER,
    ),
    (
        "backpressure_waits_total",
        "Persistence attempts that waited for Quiver disk capacity",
        MetricType::COUNTER,
    ),
];

impl QuiverCollector {
    fn new(stage: Arc<QuiverStage>) -> Result<Self, prometheus::Error> {
        let descriptors = QUIVER_METRICS
            .iter()
            .map(|(name, help, _)| {
                Desc::new(
                    format!("arrowcol_direct_quiver_{name}"),
                    (*help).to_owned(),
                    Vec::new(),
                    Default::default(),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { stage, descriptors })
    }
}

impl Collector for QuiverCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descriptors.iter().collect()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let snapshot = self.stage.snapshot();
        let values = [
            snapshot.disk_used_bytes,
            snapshot.disk_capacity_bytes,
            snapshot.backlog_bundles,
            snapshot.persisted_bundles,
            snapshot.persisted_bytes,
            snapshot.published_bundles,
            snapshot.rejected_bundles,
            snapshot.deferred_bundles,
            snapshot.recovered_bundles,
            snapshot.adopted_receipts,
            snapshot.backpressure_waits,
        ];
        QUIVER_METRICS
            .iter()
            .zip(values)
            .map(|((name, help, kind), value)| {
                let mut family = MetricFamily::default();
                family.set_name(format!("arrowcol_direct_quiver_{name}"));
                family.set_help((*help).to_owned());
                family.set_field_type(*kind);
                let mut metric = Metric::default();
                if *kind == MetricType::COUNTER {
                    let mut counter = Counter::default();
                    counter.set_value(value as f64);
                    metric.set_counter(counter);
                } else {
                    let mut gauge = Gauge::default();
                    gauge.set_value(value as f64);
                    metric.set_gauge(gauge);
                }
                family.mut_metric().push(metric);
                family
            })
            .collect()
    }
}

impl CollectorMetrics {
    /// Returns cumulative admitted batch count.
    #[must_use]
    pub fn received_batches(&self) -> u64 {
        self.received_batches.get()
    }

    /// Returns cumulative admitted row count.
    #[must_use]
    pub fn received_rows(&self) -> u64 {
        self.received_rows.get()
    }

    /// Returns cumulative admitted Arrow byte count.
    #[must_use]
    pub fn received_bytes(&self) -> u64 {
        self.received_bytes.get()
    }

    /// Records one background control failure.
    pub fn control_failure(&self, subsystem: &str) {
        self.control_failures.with_label_values(&[subsystem]).inc();
    }

    /// Records one reconciled receipt state.
    pub fn reconciled_receipt(&self, state: &str) {
        self.reconciled_receipts.with_label_values(&[state]).inc();
    }

    /// Records the final incomplete publication-group count.
    pub fn incomplete_shutdown(&self, count: usize) {
        self.incomplete_shutdown.set(i64::try_from(count).unwrap_or(i64::MAX));
    }
}

impl PressureHooks for CollectorMetrics {
    fn observe(&self, event: PressureEvent) {
        match event {
            PressureEvent::StreamStarted => self.active_streams.inc(),
            PressureEvent::BatchQueued { bytes } => {
                self.decoded_channel_bytes.add(i64::try_from(bytes).unwrap_or(i64::MAX));
            }
            PressureEvent::BatchDequeued { bytes } => {
                self.decoded_channel_bytes.sub(i64::try_from(bytes).unwrap_or(i64::MAX));
            }
            PressureEvent::StreamFinished { summary, outcome } => {
                self.active_streams.dec();
                self.received_batches.inc_by(summary.batches());
                self.received_rows.inc_by(summary.rows());
                self.received_bytes.inc_by(summary.bytes());
                self.stream_outcomes.with_label_values(&[outcome_name(outcome)]).inc();
            }
        }
    }
}

const fn outcome_name(outcome: StreamOutcome) -> &'static str {
    match outcome {
        StreamOutcome::Completed => "completed",
        StreamOutcome::Cancelled => "cancelled",
        StreamOutcome::DeadlineExceeded => "deadline_exceeded",
        StreamOutcome::Shutdown => "shutdown",
        StreamOutcome::DecodeFailed => "decode_failed",
        StreamOutcome::AdmissionFailed => "admission_failed",
    }
}
