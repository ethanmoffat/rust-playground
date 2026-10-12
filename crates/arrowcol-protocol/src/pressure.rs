use serde::{Deserialize, Serialize};

use crate::{BasisPoints, CollectorInstanceId, PressureGauge, ProtocolVersion, UnixMillis};

/// Versioned bounded pressure observations for one collector instance.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PressureSnapshot {
    protocol_version: ProtocolVersion,
    instance_id: CollectorInstanceId,
    observed_at_unix_ms: UnixMillis,
    active_streams: PressureGauge,
    buffered_bytes: PressureGauge,
    publication_queue: PressureGauge,
    cpu_utilization_bps: BasisPoints,
    memory_utilization_bps: BasisPoints,
    accepting_deliveries: bool,
}

impl PressureSnapshot {
    /// Creates a pressure snapshot from already bounded measurements.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        instance_id: CollectorInstanceId,
        observed_at_unix_ms: UnixMillis,
        active_streams: PressureGauge,
        buffered_bytes: PressureGauge,
        publication_queue: PressureGauge,
        cpu_utilization_bps: BasisPoints,
        memory_utilization_bps: BasisPoints,
        accepting_deliveries: bool,
    ) -> Self {
        Self {
            protocol_version: ProtocolVersion::CURRENT,
            instance_id,
            observed_at_unix_ms,
            active_streams,
            buffered_bytes,
            publication_queue,
            cpu_utilization_bps,
            memory_utilization_bps,
            accepting_deliveries,
        }
    }

    /// Returns the snapshot protocol version.
    #[must_use]
    pub const fn protocol_version(&self) -> ProtocolVersion {
        self.protocol_version
    }

    /// Returns the observed collector instance.
    #[must_use]
    pub const fn instance_id(&self) -> CollectorInstanceId {
        self.instance_id
    }

    /// Returns the observation timestamp.
    #[must_use]
    pub const fn observed_at_unix_ms(&self) -> UnixMillis {
        self.observed_at_unix_ms
    }

    /// Returns active inbound stream pressure.
    #[must_use]
    pub const fn active_streams(&self) -> PressureGauge {
        self.active_streams
    }

    /// Returns buffered-byte pressure.
    #[must_use]
    pub const fn buffered_bytes(&self) -> PressureGauge {
        self.buffered_bytes
    }

    /// Returns publication-queue pressure.
    #[must_use]
    pub const fn publication_queue(&self) -> PressureGauge {
        self.publication_queue
    }

    /// Returns observed CPU utilization.
    #[must_use]
    pub const fn cpu_utilization_bps(&self) -> BasisPoints {
        self.cpu_utilization_bps
    }

    /// Returns observed memory utilization.
    #[must_use]
    pub const fn memory_utilization_bps(&self) -> BasisPoints {
        self.memory_utilization_bps
    }

    /// Returns whether the instance currently accepts new deliveries.
    #[must_use]
    pub const fn accepting_deliveries(&self) -> bool {
        self.accepting_deliveries
    }
}
