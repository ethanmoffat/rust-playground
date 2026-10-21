//! Opens a Quiver engine exactly as the Direct collector's Quiver stage does.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use arrowcol_buffer::{DELTA_SUBSCRIBER, DeliveryManifest, QuiverStageConfig};
use otel_arrow_dfe_quiver::config::RetentionPolicy;
use otel_arrow_dfe_quiver::record_bundle::RecordBundle;
use otel_arrow_dfe_quiver::subscriber::SubscriberId;
use otel_arrow_dfe_quiver::{DiskBudget, QuiverEngine, QuiverEngineBuilder};

/// Disk budget the loadtest gives each collector's Quiver directory.
pub(crate) const CAPACITY_BYTES: u64 = 4 * 1024 * 1024 * 1024;
/// Segment open time that never finalizes during a short run, so segment
/// boundaries are chosen explicitly (or never reached, as in a crash). The
/// Direct collector finalizes every 500 ms instead; the bytes are the same.
pub(crate) const MANUAL_SEGMENTS: Duration = Duration::from_secs(3_600);

/// Opens (and crash-recovers) the engine with the Direct stage's settings:
/// WAL fsync before every acknowledgement, a 4 GiB backpressure budget, and the
/// `arrowcol-delta` subscriber registered.
pub(crate) async fn open(dir: &Path, segment_max_open: Duration) -> Result<Arc<QuiverEngine>> {
    let stage = QuiverStageConfig {
        directory: dir.to_path_buf(),
        capacity_bytes: CAPACITY_BYTES,
        wal_flush_interval: Duration::ZERO,
        segment_max_open,
        max_in_flight: 512,
        retry_initial: Duration::from_millis(250),
        retry_max: Duration::from_secs(5),
    };
    stage.validate()?;
    let config = stage.quiver_config();
    let budget = DiskBudget::for_config(CAPACITY_BYTES, &config, RetentionPolicy::Backpressure)?;
    let engine = QuiverEngineBuilder::new(config)
        .with_budget(Arc::new(budget))
        .with_wal_item_counter(Arc::new(|bundle: &dyn RecordBundle| {
            manifest(bundle).map(|manifest| manifest.rows)
        }))
        .with_wal_byte_counter(Arc::new(|bundle: &dyn RecordBundle| {
            manifest(bundle).map(|manifest| manifest.arrow_memory_bytes)
        }))
        .build()
        .await?;
    let subscriber = SubscriberId::new(DELTA_SUBSCRIBER)?;
    engine.register_subscriber(subscriber.clone())?;
    engine.activate_subscriber(&subscriber)?;
    Ok(engine)
}

fn manifest(bundle: &dyn RecordBundle) -> Option<DeliveryManifest> {
    bundle.metadata().and_then(|bytes| DeliveryManifest::decode(bytes).ok())
}
