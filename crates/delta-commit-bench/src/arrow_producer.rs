//! Arrow encode path (experiment 3): a pool of in-memory log-like `RecordBatch`es, encoded to
//! large ZSTD(1) Parquet files on dedicated `encoder` threads and streamed to storage as
//! multipart uploads on the main runtime.
//!
//! Raw Arrow bytes are the logical buffer bytes of each batch (`ArrayData::get_slice_memory_size`
//! summed over columns: values, offsets, and validity), not allocated capacity.

use crate::producer::SentLog;
use crate::table::{now_micros, now_millis};
use crate::wal::FileReady;
use anyhow::{Context, Result};
use bytes::Bytes;
use deltalake_core::arrow::array::{
    Array, ArrayRef, FixedSizeBinaryBuilder, Int32Builder, Int64Builder, RecordBatch, StringBuilder,
    TimestampMicrosecondBuilder,
};
use deltalake_core::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use deltalake_core::kernel::PrimitiveType;
use deltalake_core::parquet::arrow::ArrowWriter;
use deltalake_core::parquet::basic::{Compression, ZstdLevel};
use deltalake_core::parquet::file::properties::WriterProperties;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use object_store::path::Path;
use object_store::{MultipartUpload, ObjectStore, ObjectStoreExt, PutPayload};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::Serialize;
use std::fmt::Write as _;
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::runtime::Handle;
use tokio::sync::mpsc;

/// Encode and upload settings for one table.
#[derive(Clone, Debug)]
pub struct ArrowConfig {
    pub table_name: String,
    pub encoders: usize,
    /// Files per second across all encoders; 0 encodes continuously.
    pub rate: f64,
    pub duration: Duration,
    pub file_bytes: usize,
    pub row_group_bytes: usize,
    pub part_bytes: usize,
    /// Parts in flight per encoder (also the depth of the queue in front of the uploader).
    pub upload_concurrency: usize,
}

/// Arrow schema of the log-like data files.
pub fn logs_schema() -> SchemaRef {
    let utf8 = |name: &str| Field::new(name, DataType::Utf8, false);
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
        utf8("service"),
        utf8("host"),
        utf8("region"),
        utf8("level"),
        Field::new("trace_id", DataType::FixedSizeBinary(16), false),
        Field::new("span_id", DataType::FixedSizeBinary(8), false),
        Field::new("status_code", DataType::Int32, false),
        Field::new("duration_us", DataType::Int64, false),
        Field::new("bytes", DataType::Int64, false),
        utf8("message"),
        utf8("attributes"),
    ]))
}

/// Delta columns matching [`logs_schema`].
pub fn delta_columns() -> Vec<(&'static str, PrimitiveType)> {
    vec![
        ("ts", PrimitiveType::Timestamp),
        ("service", PrimitiveType::String),
        ("host", PrimitiveType::String),
        ("region", PrimitiveType::String),
        ("level", PrimitiveType::String),
        ("trace_id", PrimitiveType::Binary),
        ("span_id", PrimitiveType::Binary),
        ("status_code", PrimitiveType::Integer),
        ("duration_us", PrimitiveType::Long),
        ("bytes", PrimitiveType::Long),
        ("message", PrimitiveType::String),
        ("attributes", PrimitiveType::String),
    ]
}

/// Pre-generated batches, reused round-robin by every encoder.
pub struct BatchPool {
    batches: Vec<(RecordBatch, u64)>,
}

#[derive(Debug, Serialize)]
pub struct PoolInfo {
    pub batches: usize,
    pub rows: u64,
    /// Logical buffer bytes (the raw-bytes measure).
    pub raw_bytes: u64,
    /// `RecordBatch::get_array_memory_size`, including allocation slack.
    pub allocated_bytes: u64,
    pub generate_s: f64,
}

impl BatchPool {
    /// Generate at least `target_bytes` raw bytes of `batch_rows`-row batches on all cores.
    ///
    /// # Errors
    /// Fails if a batch cannot be built.
    pub fn generate(target_bytes: u64, batch_rows: usize) -> Result<(Self, PoolInfo)> {
        let started = Instant::now();
        let total = AtomicU64::new(0);
        let seq = AtomicUsize::new(0);
        let threads = std::thread::available_parallelism().map_or(4, std::num::NonZero::get);
        let base_us = now_micros();
        let mut batches: Vec<(usize, RecordBatch, u64)> = std::thread::scope(|s| {
            let handles: Vec<_> = (0..threads)
                .map(|_| {
                    std::thread::Builder::new().name("pool-gen".to_owned()).spawn_scoped(s, || {
                        let mut out = Vec::new();
                        while total.load(Ordering::Relaxed) < target_bytes {
                            let i = seq.fetch_add(1, Ordering::Relaxed);
                            let batch = log_batch(i as u64, batch_rows, base_us)?;
                            let raw = raw_bytes(&batch);
                            total.fetch_add(raw, Ordering::Relaxed);
                            out.push((i, batch, raw));
                        }
                        Ok::<_, anyhow::Error>(out)
                    })
                })
                .collect::<std::io::Result<_>>()?;
            let mut all = Vec::new();
            for h in handles {
                all.extend(h.join().map_err(|_| anyhow::anyhow!("pool generator panicked"))??);
            }
            Ok::<_, anyhow::Error>(all)
        })?;
        batches.sort_by_key(|(i, _, _)| *i);
        let info = PoolInfo {
            batches: batches.len(),
            rows: batches.iter().map(|(_, b, _)| b.num_rows() as u64).sum(),
            raw_bytes: batches.iter().map(|(_, _, r)| r).sum(),
            allocated_bytes: batches.iter().map(|(_, b, _)| b.get_array_memory_size() as u64).sum(),
            generate_s: started.elapsed().as_secs_f64(),
        };
        Ok((Self { batches: batches.into_iter().map(|(_, b, r)| (b, r)).collect() }, info))
    }

    fn get(&self, i: usize) -> &(RecordBatch, u64) {
        &self.batches[i % self.batches.len()]
    }
}

fn raw_bytes(batch: &RecordBatch) -> u64 {
    batch.columns().iter().map(|c| c.to_data().get_slice_memory_size().unwrap_or(0) as u64).sum()
}

const SERVICES: &[&str] = &[
    "checkout-api", "cart-svc", "payments", "auth-gateway", "search-query", "ingest-worker", "billing",
    "inventory", "shipping", "notifications", "user-profile", "recommendations", "ads-serving", "media-transcode",
    "edge-proxy", "metrics-agent", "scheduler", "kv-store", "feature-flags", "audit-log",
];
const REGIONS: &[&str] = &[
    "eastus", "eastus2", "westus2", "westus3", "centralus", "northeurope", "westeurope", "uksouth", "japaneast",
    "southeastasia",
];
const LEVELS: &[(&str, u32)] = &[("INFO", 80), ("DEBUG", 10), ("WARN", 6), ("ERROR", 3), ("TRACE", 1)];
const STATUS: &[(i32, u32)] = &[(200, 82), (201, 5), (204, 4), (400, 3), (404, 3), (500, 2), (503, 1)];
const ROUTES: &[&str] = &[
    "/api/v1/orders", "/api/v1/cart", "/api/v2/search", "/api/v1/users", "/healthz", "/api/v1/payments",
    "/api/v1/items", "/internal/flush", "/api/v3/recommend", "/api/v1/session",
];
const METHODS: &[&str] = &["GET", "GET", "GET", "POST", "PUT", "DELETE"];
const TIERS: &[&str] = &["free", "basic", "gold", "platinum"];

fn weighted<T: Copy>(rng: &mut StdRng, items: &[(T, u32)]) -> T {
    let total: u32 = items.iter().map(|(_, w)| w).sum();
    let mut x = rng.random_range(0..total);
    for (item, w) in items {
        if x < *w {
            return *item;
        }
        x -= w;
    }
    items[0].0
}

/// Index skewed towards small values, like real per-service traffic.
fn skewed(rng: &mut StdRng, n: usize) -> usize {
    let u: f64 = rng.random();
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss, clippy::cast_precision_loss)]
    let i = (u * u * n as f64) as usize;
    i.min(n - 1)
}

fn log_batch(seed: u64, rows: usize, base_us: i64) -> Result<RecordBatch> {
    let mut rng = StdRng::seed_from_u64(seed ^ 0x5eed_1234_abcd);
    let mut ts = TimestampMicrosecondBuilder::with_capacity(rows);
    let mut service = StringBuilder::with_capacity(rows, rows * 12);
    let mut host = StringBuilder::with_capacity(rows, rows * 32);
    let mut region = StringBuilder::with_capacity(rows, rows * 8);
    let mut level = StringBuilder::with_capacity(rows, rows * 5);
    let mut trace = FixedSizeBinaryBuilder::with_capacity(rows, 16);
    let mut span = FixedSizeBinaryBuilder::with_capacity(rows, 8);
    let mut status = Int32Builder::with_capacity(rows);
    let mut duration = Int64Builder::with_capacity(rows);
    let mut size = Int64Builder::with_capacity(rows);
    let mut message = StringBuilder::with_capacity(rows, rows * 80);
    let mut attributes = StringBuilder::with_capacity(rows, rows * 120);
    #[allow(clippy::cast_possible_wrap)]
    let mut t = base_us + (seed as i64) * 1_000_000;
    let mut trace_id = [0u8; 16];
    let mut trace_left = 0u32;
    let mut span_id = [0u8; 8];
    let mut line = String::with_capacity(256);
    for _ in 0..rows {
        t += rng.random_range(0..200);
        ts.append_value(t);
        let svc_i = skewed(&mut rng, SERVICES.len());
        let svc = SERVICES[svc_i];
        service.append_value(svc);
        let region_i = skewed(&mut rng, REGIONS.len());
        line.clear();
        write!(line, "{svc}-{:04x}.{}.prod.internal", rng.random_range(0..50u32), REGIONS[region_i])?;
        host.append_value(&line);
        region.append_value(REGIONS[region_i]);
        let lvl = weighted(&mut rng, LEVELS);
        level.append_value(lvl);
        if trace_left == 0 {
            rng.fill(&mut trace_id);
            trace_left = rng.random_range(1..8);
        }
        trace_left -= 1;
        trace.append_value(trace_id)?;
        rng.fill(&mut span_id);
        span.append_value(span_id)?;
        let code = weighted(&mut rng, STATUS);
        status.append_value(code);
        #[allow(clippy::cast_possible_truncation)]
        let dur = (-(1.0 - rng.random::<f64>()).ln() * 4000.0) as i64 + 50;
        duration.append_value(dur);
        let nbytes: i64 = rng.random_range(0..65_536);
        size.append_value(nbytes);
        let route = ROUTES[skewed(&mut rng, ROUTES.len())];
        let method = METHODS[rng.random_range(0..METHODS.len())];
        line.clear();
        match rng.random_range(0..8u32) {
            0..=2 => write!(
                line,
                "{method} {route}/{} completed in {}ms status={code} bytes={nbytes}",
                rng.random_range(0..10_000_000u32),
                dur / 1000
            )?,
            3 => write!(
                line,
                "user {} authenticated from 10.{}.{}.{} via oauth2 client={:08x}",
                rng.random_range(0..5_000_000u32),
                rng.random_range(0..256u32),
                rng.random_range(0..256u32),
                rng.random_range(0..256u32),
                rng.random::<u32>()
            )?,
            4 => write!(
                line,
                "cache miss for key session:{:016x} ttl={}s shard={}",
                rng.random::<u64>(),
                rng.random_range(30..3600u32),
                rng.random_range(0..64u32)
            )?,
            5 => write!(
                line,
                "processed batch {} records={} lag={}ms partition={}",
                rng.random_range(0..1_000_000u32),
                rng.random_range(1..5000u32),
                rng.random_range(0..2000u32),
                rng.random_range(0..128u32)
            )?,
            6 => write!(
                line,
                "connection to {}:{} reset after {} retries; backing off {}ms",
                SERVICES[skewed(&mut rng, SERVICES.len())],
                rng.random_range(8000..9000u32),
                rng.random_range(1..6u32),
                rng.random_range(10..5000u32)
            )?,
            _ => write!(
                line,
                "{lvl}: request {:032x} exceeded budget {}ms on {svc}",
                rng.random::<u128>(),
                rng.random_range(100..1000u32)
            )?,
        }
        message.append_value(&line);
        line.clear();
        write!(
            line,
            r#"{{"http.method":"{method}","http.route":"{route}","customer.tier":"{}","k8s.pod":"{svc}-{:05x}","sdk.version":"1.{}.0"}}"#,
            TIERS[skewed(&mut rng, TIERS.len())],
            rng.random_range(0..0x10000u32),
            rng.random_range(20..24u32)
        )?;
        attributes.append_value(&line);
    }
    let columns: Vec<ArrayRef> = vec![
        Arc::new(ts.finish().with_timezone("UTC")),
        Arc::new(service.finish()),
        Arc::new(host.finish()),
        Arc::new(region.finish()),
        Arc::new(level.finish()),
        Arc::new(trace.finish()),
        Arc::new(span.finish()),
        Arc::new(status.finish()),
        Arc::new(duration.finish()),
        Arc::new(size.finish()),
        Arc::new(message.finish()),
        Arc::new(attributes.finish()),
    ];
    Ok(RecordBatch::try_new(logs_schema(), columns)?)
}

/// Process-wide producer counters, updated as work happens.
#[derive(Default)]
pub struct ProducerStats {
    raw_bytes: AtomicU64,
    parquet_bytes: AtomicU64,
    uploaded_bytes: AtomicU64,
    rows: AtomicU64,
    files: AtomicU64,
    upload_wait_us: AtomicU64,
    encoder_cpu_ms: AtomicU64,
    upload_errors: AtomicU64,
}

#[derive(Clone, Debug, Serialize)]
pub struct StatsSnapshot {
    /// Raw Arrow bytes handed to the Parquet writers.
    pub raw_bytes: u64,
    /// Parquet bytes produced by the writers.
    pub parquet_bytes: u64,
    /// Bytes of acknowledged upload parts.
    pub uploaded_bytes: u64,
    pub rows: u64,
    /// Files whose multipart upload completed.
    pub files: u64,
    /// Encoder time blocked on upload backpressure, summed over encoders.
    pub upload_wait_s: f64,
    /// CPU of encoder threads that have exited (live threads appear in the `encoder` CPU group).
    pub encoder_cpu_exited_s: f64,
    pub upload_errors: u64,
    /// Bytes sent by all non-loopback interfaces since boot (/proc/net/dev). Interfaces enslaved to another
    /// (the Azure accelerated-networking VF under eth0) are skipped: their bytes are also counted on the master.
    pub nic_tx_bytes: Option<u64>,
}

impl ProducerStats {
    pub fn snapshot(&self) -> StatsSnapshot {
        let get = |a: &AtomicU64| a.load(Ordering::Relaxed);
        #[allow(clippy::cast_precision_loss)]
        StatsSnapshot {
            raw_bytes: get(&self.raw_bytes),
            parquet_bytes: get(&self.parquet_bytes),
            uploaded_bytes: get(&self.uploaded_bytes),
            rows: get(&self.rows),
            files: get(&self.files),
            upload_wait_s: get(&self.upload_wait_us) as f64 / 1e6,
            encoder_cpu_exited_s: get(&self.encoder_cpu_ms) as f64 / 1e3,
            upload_errors: get(&self.upload_errors),
            nic_tx_bytes: nic_tx_bytes(),
        }
    }
}

fn nic_tx_bytes() -> Option<u64> {
    let dev = std::fs::read_to_string("/proc/net/dev").ok()?;
    let mut total = 0;
    for line in dev.lines().skip(2) {
        let (iface, rest) = line.split_once(':')?;
        let iface = iface.trim();
        if iface == "lo" || std::path::Path::new(&format!("/sys/class/net/{iface}/master")).exists() {
            continue;
        }
        total += rest.split_whitespace().nth(8)?.parse::<u64>().ok()?;
    }
    Some(total)
}

/// CPU seconds of the calling thread (Linux only).
fn own_thread_cpu_s() -> f64 {
    let Ok(stat) = std::fs::read_to_string("/proc/thread-self/stat") else { return 0.0 };
    let Some(close) = stat.rfind(')') else { return 0.0 };
    let fields: Vec<&str> = stat[close + 2..].split_whitespace().collect();
    let ticks = |i: usize| fields.get(i).and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0);
    (ticks(11) + ticks(12)) / 100.0
}

/// `Write` sink that cuts the Parquet stream into fixed-size parts and queues them for upload.
/// Blocks when the queue is full, which bounds memory per encoder.
struct PartSink {
    buf: Vec<u8>,
    part_bytes: usize,
    tx: mpsc::Sender<Bytes>,
    written: usize,
    wait: Duration,
    stats: Arc<ProducerStats>,
}

impl PartSink {
    fn send(&mut self) -> std::io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let chunk = std::mem::replace(&mut self.buf, Vec::with_capacity(self.part_bytes));
        let started = Instant::now();
        let sent = self.tx.blocking_send(Bytes::from(chunk));
        self.wait += started.elapsed();
        sent.map_err(|_| std::io::Error::other("uploader stopped"))
    }
}

impl Write for PartSink {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        let mut rest = data;
        while !rest.is_empty() {
            let take = rest.len().min(self.part_bytes - self.buf.len());
            self.buf.extend_from_slice(&rest[..take]);
            rest = &rest[take..];
            if self.buf.len() >= self.part_bytes {
                self.send()?;
            }
        }
        self.written += data.len();
        self.stats.parquet_bytes.fetch_add(data.len() as u64, Ordering::Relaxed);
        Ok(data.len())
    }

    // Parts are cut by size only; the final part is sent by `encode_file`.
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Upload queued parts with up to `concurrency` in flight. Returns the upload unfinished so the
/// caller decides between complete and abort.
async fn upload_parts(
    mut upload: Box<dyn MultipartUpload>,
    mut rx: mpsc::Receiver<Bytes>,
    concurrency: usize,
    stats: Arc<ProducerStats>,
) -> (Box<dyn MultipartUpload>, object_store::Result<()>) {
    let mut inflight = FuturesUnordered::new();
    let result = async {
        loop {
            if inflight.len() >= concurrency {
                if let Some(done) = inflight.next().await {
                    let n: u64 = done?;
                    stats.uploaded_bytes.fetch_add(n, Ordering::Relaxed);
                }
                continue;
            }
            tokio::select! {
                Some(done) = inflight.next() => {
                    let n: u64 = done?;
                    stats.uploaded_bytes.fetch_add(n, Ordering::Relaxed);
                }
                part = rx.recv() => match part {
                    Some(bytes) => {
                        let n = bytes.len() as u64;
                        let fut = upload.put_part(PutPayload::from(bytes));
                        inflight.push(async move { fut.await.map(|()| n) });
                    }
                    None => break,
                },
            }
        }
        while let Some(done) = inflight.next().await {
            stats.uploaded_bytes.fetch_add(done?, Ordering::Relaxed);
        }
        Ok(())
    }
    .await;
    rx.close();
    (upload, result)
}

struct Encoder {
    index: usize,
    name: String,
    cfg: ArrowConfig,
    pool: Arc<BatchPool>,
    cursor: usize,
    store: Arc<dyn ObjectStore>,
    root: Path,
    handle: Handle,
    stats: Arc<ProducerStats>,
    props: WriterProperties,
}

impl Encoder {
    /// Encode and upload one file; stop early at `deadline` with a valid, smaller file.
    fn encode_file(&mut self, seq: u64, deadline: Instant) -> Result<FileReady> {
        let rel = format!("data/{}-{seq:08}-{}.parquet", self.name, uuid::Uuid::new_v4());
        let location = Path::from(format!("{}/{rel}", self.root.as_ref()));
        let upload_start_ms = now_millis();
        let upload = self.handle.block_on(self.store.put_multipart(&location))?;
        let (part_tx, part_rx) = mpsc::channel(self.cfg.upload_concurrency.max(1));
        let uploader = self.handle.spawn(upload_parts(
            upload,
            part_rx,
            self.cfg.upload_concurrency.max(1),
            Arc::clone(&self.stats),
        ));
        let sink = PartSink {
            buf: Vec::with_capacity(self.cfg.part_bytes),
            part_bytes: self.cfg.part_bytes,
            tx: part_tx,
            written: 0,
            wait: Duration::ZERO,
            stats: Arc::clone(&self.stats),
        };
        let encode_start = Instant::now();
        let mut raw = 0u64;
        let mut rows = 0u64;
        let mut row_groups = 0usize;
        let encoded = (|| -> Result<PartSink> {
            let mut writer = ArrowWriter::try_new(sink, logs_schema(), Some(self.props.clone()))?;
            let mut batches = 0usize;
            loop {
                let (batch, batch_raw) = self.pool.get(self.cursor);
                self.cursor += 1;
                batches += 1;
                writer.write(batch)?;
                raw += batch_raw;
                rows += batch.num_rows() as u64;
                self.stats.raw_bytes.fetch_add(*batch_raw, Ordering::Relaxed);
                self.stats.rows.fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
                let flushed = writer.in_progress_size() >= self.cfg.row_group_bytes;
                if flushed {
                    writer.flush()?;
                }
                // Stop at the batch boundary nearest the target, and skip a final row group of
                // less than half the target, so files do not end in a small tail row group.
                let size = writer.bytes_written() + writer.in_progress_size();
                let tail = flushed && size + self.cfg.row_group_bytes / 2 > self.cfg.file_bytes;
                if tail || size + size / batches / 2 >= self.cfg.file_bytes || Instant::now() >= deadline {
                    break;
                }
            }
            writer.flush()?;
            row_groups = writer.flushed_row_groups().len();
            let mut sink = writer.into_inner()?;
            sink.send()?;
            Ok(sink)
        })();
        let encode_ms = encode_start.elapsed().as_secs_f64() * 1000.0;
        // Dropping the sink (on success or error) closes the part queue.
        let (size, mut wait, encode_error) = match encoded {
            Ok(sink) => (sink.written, sink.wait, None),
            Err(error) => (0, Duration::ZERO, Some(error)),
        };
        let finish_start = Instant::now();
        let (mut upload, uploaded) = self.handle.block_on(uploader).context("uploader task")?;
        let completed = match (encode_error, uploaded) {
            (None, Ok(())) => self.handle.block_on(upload.complete()).map(|_| ()).map_err(anyhow::Error::from),
            (_, Err(error)) => Err(error.into()),
            (Some(error), Ok(())) => Err(error.context("encode failed")),
        };
        if let Err(error) = completed {
            let _ = self.handle.block_on(upload.abort());
            return Err(error);
        }
        wait += finish_start.elapsed();
        self.stats.files.fetch_add(1, Ordering::Relaxed);
        self.stats.upload_wait_us.fetch_add(u64::try_from(wait.as_micros()).unwrap_or(u64::MAX), Ordering::Relaxed);
        Ok(FileReady {
            path: rel,
            size: i64::try_from(size)?,
            rows: i64::try_from(rows)?,
            encoder: format!("{}#{}", self.name, self.index),
            upload_start_ms,
            upload_done_ms: now_millis(),
            raw_bytes: Some(i64::try_from(raw)?),
            row_groups: Some(i64::try_from(row_groups)?),
            encode_ms: Some(encode_ms),
            upload_wait_ms: Some(wait.as_secs_f64() * 1000.0),
        })
    }
}

/// Run `cfg.encoders` encoder threads for one table until `cfg.duration` elapses, then drop `tx`.
///
/// # Errors
/// Fails if an encoder thread cannot be spawned or panics.
pub async fn produce(
    store: Arc<dyn ObjectStore>,
    table_root: Path,
    cfg: ArrowConfig,
    pool: Arc<BatchPool>,
    tx: mpsc::Sender<FileReady>,
    sent: Option<SentLog>,
    stats: Arc<ProducerStats>,
) -> Result<u64> {
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(1)?))
        .set_max_row_group_row_count(None)
        .set_max_row_group_bytes(None)
        .build();
    let deadline = Instant::now() + cfg.duration;
    let handle = Handle::current();
    let mut threads = Vec::new();
    for index in 0..cfg.encoders {
        let mut encoder = Encoder {
            index,
            name: cfg.table_name.clone(),
            cfg: cfg.clone(),
            cursor: index * pool.batches.len() / cfg.encoders.max(1),
            pool: Arc::clone(&pool),
            store: Arc::clone(&store),
            root: table_root.clone(),
            handle: handle.clone(),
            stats: Arc::clone(&stats),
            props: props.clone(),
        };
        let tx = tx.clone();
        let sent = sent.clone();
        threads.push(std::thread::Builder::new().name(format!("encoder-{index}")).spawn(move || {
            let cpu_start = own_thread_cpu_s();
            let period = (encoder.cfg.rate > 0.0)
                .then(|| Duration::from_secs_f64(encoder.cfg.encoders as f64 / encoder.cfg.rate));
            let mut next = Instant::now() + period.map_or(Duration::ZERO, |p| p.mul_f64(rand::random::<f64>()));
            let mut produced = 0u64;
            let mut seq = 0u64;
            loop {
                if let Some(period) = period {
                    std::thread::sleep(next.saturating_duration_since(Instant::now()));
                    next += period;
                }
                if Instant::now() >= deadline {
                    break;
                }
                seq += 1;
                let file = match encoder.encode_file(seq, deadline) {
                    Ok(file) => file,
                    Err(error) => {
                        encoder.stats.upload_errors.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(%error, "upload failed");
                        continue;
                    }
                };
                if let Some(sent) = &sent
                    && let Ok(line) = serde_json::to_string(&file)
                    && let Ok(mut f) = sent.lock()
                {
                    let _ = writeln!(f, "{line}");
                }
                if tx.blocking_send(file).is_err() {
                    break;
                }
                produced += 1;
            }
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let cpu_ms = ((own_thread_cpu_s() - cpu_start) * 1000.0) as u64;
            encoder.stats.encoder_cpu_ms.fetch_add(cpu_ms, Ordering::Relaxed);
            produced
        })?);
    }
    drop(tx);
    tokio::task::spawn_blocking(move || {
        let mut total = 0;
        for t in threads {
            total += t.join().map_err(|_| anyhow::anyhow!("encoder thread panicked"))?;
        }
        Ok(total)
    })
    .await?
}
