//! DFE-native Arrow Flight ingress and arbitrary Arrow Delta export.
//!
//! The receiver validates every schema through `arrowcol-catalog`, moves the
//! resulting Arrow batches into pluggable DFE pdata without serialization or
//! buffer copies, and reports best-effort admission. The exporter consumes only
//! that representation, preserves its table/schema/lease envelope, and writes
//! the original batch through `arrow-deltalake`.

#![deny(unsafe_code)]

mod config;
mod envelope;
mod exporter;
mod metrics;
mod receiver;
mod routing;

pub use arrow_deltalake::delta::initialize_shared_io;
pub use config::{
    AdaptiveFileConfig, BatchCapacity, CatalogConfig, CommitConfig, DeltaExporterConfig, FileBatchConfig,
    FlightReceiverConfig, HiveMetastoreConnectionConfig, RegistrationConfig, RoutingStoreConfig, RuntimeLimitsConfig,
    SqlServerConnectionConfig,
};
pub use envelope::{
    ARROWCOL_FLIGHT_REPRESENTATION_ID, ARROWCOL_FLIGHT_REPRESENTATION_VERSION, ArrowcolBatchEnvelope,
    DecodedArrowBatch, EnvelopeError, arrowcol_flight_representation, decode_record_set, encode_record_batch,
};
pub use exporter::{ARROW_DELTALAKE_EXPORTER, ARROW_DELTALAKE_EXPORTER_URN, GenericDeltaExporter};
pub use metrics::{
    DashboardCapabilities, DashboardPressure, RuntimeMetrics, TableVersionStatus, global_metrics, process_instance_id,
};
pub use receiver::{ARROW_FLIGHT_RECEIVER, ARROW_FLIGHT_RECEIVER_URN, DfeAdmission, PdataSendError, PdataSender};
pub use routing::{LeaseResolver, StoreLeaseResolver};

/// Exact native Quiver feature gap exposed by config validation and capability metadata.
pub const QUIVER_UNSUPPORTED_GAP: &str =
    "Quiver capability `persist` is unavailable for pdata representation `arrowcol.flight@1.0`";
