use std::sync::Arc;

use arrowcol_flight::{PressureEvent, PressureHooks, StreamOutcome};
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
            stream_outcomes,
            control_failures,
            reconciled_receipts,
            incomplete_shutdown,
        })
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
