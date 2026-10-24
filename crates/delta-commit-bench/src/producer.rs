//! Synthetic encoders: upload real Parquet files and send file-ready messages.

use crate::table::{now_millis, small_parquet};
use crate::wal::FileReady;
use anyhow::Result;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use std::io::Write;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;

/// Encoder settings for one table.
#[derive(Clone, Debug)]
pub struct ProducerConfig {
    pub table_name: String,
    pub encoders: usize,
    /// Files per second across all encoders of the table.
    pub rate: f64,
    pub rows: usize,
    pub multipart: bool,
    pub duration: Duration,
}

/// Shared log of every file-ready message sent (input to the checker).
pub type SentLog = Arc<Mutex<std::fs::File>>;

static NEXT_ID: AtomicI64 = AtomicI64::new(0);

/// Run the encoders of one table until `cfg.duration` elapses, then drop `tx`.
pub async fn produce(
    store: Arc<dyn ObjectStore>,
    table_root: Path,
    cfg: ProducerConfig,
    tx: mpsc::Sender<FileReady>,
    sent: Option<SentLog>,
) -> Result<u64> {
    let deadline = tokio::time::Instant::now() + cfg.duration;
    let mut tasks = Vec::new();
    for encoder in 0..cfg.encoders {
        let store = Arc::clone(&store);
        let root = table_root.clone();
        let tx = tx.clone();
        let sent = sent.clone();
        let cfg = cfg.clone();
        tasks.push(tokio::spawn(async move {
            #[allow(clippy::cast_precision_loss)]
            let period = Duration::from_secs_f64(cfg.encoders as f64 / cfg.rate);
            let phase = period.mul_f64(rand::random::<f64>());
            let mut interval = tokio::time::interval_at(tokio::time::Instant::now() + phase, period);
            // Skip keeps each encoder's random phase. Delay re-bases the schedule on the late tick,
            // so one runtime stall longer than a period aligns every encoder for the rest of the run.
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let name = format!("{}-e{encoder}", cfg.table_name);
            let mut produced = 0u64;
            loop {
                interval.tick().await;
                if tokio::time::Instant::now() >= deadline {
                    break;
                }
                let rows = i64::try_from(cfg.rows).unwrap_or(0);
                let first = NEXT_ID.fetch_add(rows, Ordering::Relaxed);
                let bytes = match small_parquet(first, cfg.rows, &name) {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        tracing::error!(%error, "encode failed");
                        continue;
                    }
                };
                let rel = format!("data/{name}-{produced:08}-{}.parquet", uuid::Uuid::new_v4());
                let size = i64::try_from(bytes.len()).unwrap_or(0);
                let start = now_millis();
                let location = Path::from(format!("{}/{rel}", root.as_ref()));
                let uploaded = if cfg.multipart {
                    upload_two_parts(store.as_ref(), &location, bytes.into()).await
                } else {
                    store.put(&location, PutPayload::from(bytes)).await.map(|_| ())
                };
                if let Err(error) = uploaded {
                    tracing::warn!(%error, "upload failed");
                    continue;
                }
                let file = FileReady {
                    path: rel,
                    size,
                    rows,
                    encoder: name.clone(),
                    upload_start_ms: start,
                    upload_done_ms: now_millis(),
                    ..FileReady::default()
                };
                if let Some(sent) = &sent
                    && let Ok(line) = serde_json::to_string(&file)
                    && let Ok(mut f) = sent.lock()
                {
                    let _ = writeln!(f, "{line}");
                }
                if tx.send(file).await.is_err() {
                    break;
                }
                produced += 1;
            }
            produced
        }));
    }
    drop(tx);
    let mut total = 0;
    for task in tasks {
        total += task.await?;
    }
    Ok(total)
}

async fn upload_two_parts(store: &dyn ObjectStore, location: &Path, bytes: bytes::Bytes) -> object_store::Result<()> {
    let mut upload = store.put_multipart(location).await?;
    let mid = bytes.len() / 2;
    upload.put_part(PutPayload::from(bytes.slice(..mid))).await?;
    upload.put_part(PutPayload::from(bytes.slice(mid..))).await?;
    upload.complete().await.map(|_| ())
}
