//! Local WAL output manifests. A manifest fixes a commit's txn version and exact file list
//! before any commit attempt, so replay recommits the same batch under the same version.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File};
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, mpsc};
use std::time::{Duration, Instant};

/// Metadata an encoder sends to the commit loop after a data file is fully uploaded.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct FileReady {
    /// Path relative to the table root.
    pub path: String,
    pub size: i64,
    pub rows: i64,
    pub encoder: String,
    pub upload_start_ms: i64,
    pub upload_done_ms: i64,
    /// Arrow producer only: logical Arrow buffer bytes encoded into the file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_bytes: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_groups: Option<i64>,
    /// Arrow producer only: wall time from first batch to the Parquet footer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encode_ms: Option<f64>,
    /// Arrow producer only: time the encoder blocked on upload backpressure, including the
    /// final wait for outstanding parts and the multipart complete.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upload_wait_ms: Option<f64>,
}

/// One batch with its assigned txn version.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Manifest {
    pub app_id: String,
    pub version: i64,
    pub created_ms: i64,
    pub files: Vec<FileReady>,
}

/// Manifests for one appId: a directory per appId with three fsyncs per manifest (per-commit
/// mode), or a key in the process-wide group log (group mode).
pub enum Wal {
    Dir { pending: PathBuf, released: PathBuf },
    Group { key: String, log: Arc<GroupLog> },
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

static GROUP: OnceLock<Arc<GroupLog>> = OnceLock::new();
const GROUP_FILE: &str = "group.log";

/// Switch every later `Wal::open` in this process to one group-synced log under `root`.
pub fn init_group(root: &Path, window: Duration, data_mbps: f64) -> Result<Arc<GroupLog>> {
    let log = GroupLog::start(root, window, data_mbps)?;
    GROUP
        .set(Arc::clone(&log))
        .map_err(|_| anyhow::anyhow!("group WAL already initialized"))?;
    Ok(log)
}

#[derive(Default)]
struct KeyState {
    pending: BTreeMap<i64, Manifest>,
    released: BTreeMap<i64, Manifest>,
}

#[derive(Serialize, Deserialize)]
struct GroupRecord {
    k: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    w: Option<Manifest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    r: Option<i64>,
}

struct Req {
    bytes: Vec<u8>,
    /// Manifests and data need the sync; releases ride along without one.
    sync: bool,
    ack: Option<tokio::sync::oneshot::Sender<std::result::Result<(), String>>>,
}

/// Sync statistics, cumulative since start.
#[derive(Clone, Debug, Default, Serialize)]
pub struct SyncStats {
    pub syncs: u64,
    pub sync_ms_total: f64,
    pub manifests: u64,
    pub releases: u64,
    pub data_bytes: u64,
    pub bytes: u64,
    /// Per-sync latency in ms since the last `take_latencies`.
    #[serde(skip)]
    pub latencies: Vec<f64>,
}

/// One append-only file shared by every table in the process. A dedicated thread batches all
/// queued appends, writes them, and issues one `fdatasync` per batch.
pub struct GroupLog {
    tx: Mutex<mpsc::Sender<Req>>,
    state: Mutex<HashMap<String, KeyState>>,
    stats: Arc<Mutex<SyncStats>>,
}

impl GroupLog {
    fn start(root: &Path, window: Duration, data_mbps: f64) -> Result<Arc<Self>> {
        fs::create_dir_all(root)?;
        let path = root.join(GROUP_FILE);
        let state = replay_group(&path)?;
        let file = fs::OpenOptions::new().create(true).append(true).open(&path)?;
        let (tx, rx) = mpsc::channel::<Req>();
        let stats = Arc::new(Mutex::new(SyncStats::default()));
        let thread_stats = Arc::clone(&stats);
        std::thread::Builder::new()
            .name("wal-sync".into())
            .spawn(move || sync_thread(file, rx, window, &thread_stats))?;
        if data_mbps > 0.0 {
            let data_tx = tx.clone();
            std::thread::Builder::new()
                .name("wal-data".into())
                .spawn(move || data_thread(&data_tx, data_mbps))?;
        }
        Ok(Arc::new(Self {
            tx: Mutex::new(tx),
            state: Mutex::new(state),
            stats,
        }))
    }

    fn send(&self, req: Req) -> Result<()> {
        lock(&self.tx)
            .send(req)
            .map_err(|_| anyhow::anyhow!("wal sync thread stopped"))
    }

    pub fn stats(&self) -> SyncStats {
        let mut stats = lock(&self.stats);
        let snapshot = stats.clone();
        stats.latencies.clear();
        snapshot
    }
}

fn sync_thread(mut file: File, rx: mpsc::Receiver<Req>, window: Duration, stats: &Mutex<SyncStats>) {
    while let Ok(first) = rx.recv() {
        let mut batch = vec![first];
        if !window.is_zero() {
            let deadline = Instant::now() + window;
            while let Some(left) = deadline.checked_duration_since(Instant::now()) {
                match rx.recv_timeout(left) {
                    Ok(req) => batch.push(req),
                    Err(_) => break,
                }
            }
        }
        while let Ok(req) = rx.try_recv() {
            batch.push(req);
        }
        let mut buf = Vec::new();
        let (mut manifests, mut releases, mut data, mut sync) = (0, 0, 0, false);
        for req in &batch {
            buf.extend_from_slice(&req.bytes);
            sync |= req.sync;
            match (&req.ack, req.sync) {
                (Some(_), _) => manifests += 1,
                (None, true) => data += req.bytes.len() as u64,
                (None, false) => releases += 1,
            }
        }
        let started = Instant::now();
        let result = file
            .write_all(&buf)
            .and_then(|()| if sync { file.sync_data() } else { Ok(()) });
        let ms = started.elapsed().as_secs_f64() * 1000.0;
        {
            let mut s = lock(stats);
            if sync {
                s.syncs += 1;
                s.sync_ms_total += ms;
                s.latencies.push(ms);
            }
            s.manifests += manifests;
            s.releases += releases;
            s.data_bytes += data;
            s.bytes += buf.len() as u64;
        }
        let result = result.map_err(|e| e.to_string());
        for req in batch {
            if let Some(ack) = req.ack {
                let _ = ack.send(result.clone());
            }
        }
    }
}

/// Synthetic data appends at `mbps` MB/s through the same log, every 10 ms.
fn data_thread(tx: &mpsc::Sender<Req>, mbps: f64) {
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let chunk = ((mbps * 1e6 / 100.0) as usize).max(2);
    let mut line = vec![b'#'; chunk - 1];
    line.push(b'\n');
    let mut next = Instant::now();
    loop {
        if tx
            .send(Req {
                bytes: line.clone(),
                sync: true,
                ack: None,
            })
            .is_err()
        {
            return;
        }
        next += Duration::from_millis(10);
        if let Some(wait) = next.checked_duration_since(Instant::now()) {
            std::thread::sleep(wait);
        }
    }
}

fn replay_group(path: &Path) -> Result<HashMap<String, KeyState>> {
    let mut state: HashMap<String, KeyState> = HashMap::new();
    let Ok(file) = File::open(path) else { return Ok(state) };
    for line in std::io::BufReader::new(file).split(b'\n') {
        let line = line?;
        if line.first() != Some(&b'{') {
            continue;
        }
        // A torn final record is an append that was never acknowledged.
        let Ok(rec) = serde_json::from_slice::<GroupRecord>(&line) else {
            continue;
        };
        let entry = state.entry(rec.k).or_default();
        if let Some(m) = rec.w {
            entry.pending.insert(m.version, m);
        }
        if let Some(v) = rec.r
            && let Some(m) = entry.pending.remove(&v)
        {
            entry.released.insert(v, m);
        }
    }
    Ok(state)
}

fn sanitize(app_id: &str) -> String {
    app_id
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' { c } else { '_' })
        .collect()
}

fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

fn file_name(version: i64) -> String {
    format!("{version:020}.json")
}

fn read_dir_manifests(dir: &Path) -> Result<Vec<Manifest>> {
    let mut manifests = Vec::new();
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().is_some_and(|e| e == "json") {
            // Another holder of the same WAL may release it between the listing and the read.
            let Ok(bytes) = fs::read(&path) else { continue };
            manifests.push(serde_json::from_slice(&bytes).with_context(|| format!("parsing {}", path.display()))?);
        }
    }
    manifests.sort_by_key(|m: &Manifest| m.version);
    Ok(manifests)
}

impl Wal {
    /// Open or create the manifest directory for `app_id` under `root`.
    pub fn open(root: &Path, app_id: &str) -> Result<Self> {
        if let Some(log) = GROUP.get() {
            return Ok(Self::Group {
                key: sanitize(app_id),
                log: Arc::clone(log),
            });
        }
        let base = root.join(sanitize(app_id));
        let pending = base.join("pending");
        let released = base.join("released");
        fs::create_dir_all(&pending)?;
        fs::create_dir_all(&released)?;
        Ok(Self::Dir { pending, released })
    }

    /// Durably record `manifest` as pending. Per-commit mode syncs inline on the calling thread;
    /// group mode waits for the sync thread's next batch.
    pub async fn write(&self, manifest: &Manifest) -> Result<()> {
        match self {
            Self::Dir { pending, .. } => Self::write_dir(pending, manifest),
            Self::Group { key, log } => {
                let mut bytes = serde_json::to_vec(&GroupRecord {
                    k: key.clone(),
                    w: Some(manifest.clone()),
                    r: None,
                })?;
                bytes.push(b'\n');
                let (ack, done) = tokio::sync::oneshot::channel();
                log.send(Req {
                    bytes,
                    sync: true,
                    ack: Some(ack),
                })?;
                done.await
                    .context("wal sync thread stopped")?
                    .map_err(|e| anyhow::anyhow!("wal append: {e}"))?;
                lock(&log.state)
                    .entry(key.clone())
                    .or_default()
                    .pending
                    .insert(manifest.version, manifest.clone());
                Ok(())
            }
        }
    }

    fn write_dir(pending: &Path, manifest: &Manifest) -> Result<()> {
        let tmp = pending.join(format!(".{}.tmp", file_name(manifest.version)));
        let mut file = File::create(&tmp)?;
        file.write_all(&serde_json::to_vec(manifest)?)?;
        file.sync_all()?;
        fs::rename(&tmp, pending.join(file_name(manifest.version)))?;
        sync_dir(pending)
    }

    /// Mark the manifest at `version` as committed.
    pub fn release(&self, version: i64) -> Result<()> {
        let (pending, released) = match self {
            Self::Dir { pending, released } => (pending, released),
            Self::Group { key, log } => {
                // Not synced: a lost release only causes a replay that finds the commit landed.
                let mut bytes = serde_json::to_vec(&GroupRecord {
                    k: key.clone(),
                    w: None,
                    r: Some(version),
                })?;
                bytes.push(b'\n');
                log.send(Req {
                    bytes,
                    sync: false,
                    ack: None,
                })?;
                let mut state = lock(&log.state);
                let entry = state.entry(key.clone()).or_default();
                if let Some(m) = entry.pending.remove(&version) {
                    entry.released.insert(version, m);
                }
                return Ok(());
            }
        };
        let name = file_name(version);
        match fs::rename(pending.join(&name), released.join(&name)) {
            Ok(()) => sync_dir(pending),
            // Another holder of the same WAL released it first.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    /// Pending manifests in version order.
    pub fn pending(&self) -> Result<Vec<Manifest>> {
        match self {
            Self::Dir { pending, .. } => read_dir_manifests(pending),
            Self::Group { key, log } => Ok(lock(&log.state)
                .get(key)
                .map_or_else(Vec::new, |s| s.pending.values().cloned().collect())),
        }
    }

    /// Released manifests in version order.
    pub fn released(&self) -> Result<Vec<Manifest>> {
        match self {
            Self::Dir { released, .. } => read_dir_manifests(released),
            Self::Group { key, log } => Ok(lock(&log.state)
                .get(key)
                .map_or_else(Vec::new, |s| s.released.values().cloned().collect())),
        }
    }

    /// Highest version ever assigned, pending or released.
    pub fn max_version(&self) -> Result<Option<i64>> {
        let pending = self.pending()?.last().map(|m| m.version);
        let released = self.released()?.last().map(|m| m.version);
        Ok(pending.max(released))
    }
}

/// Group-log state for `root`: the whole log under `root`, or, when `root` names one appId's
/// directory under a group-mode WAL root, that appId's entry in the parent's log.
fn group_states(root: &Path) -> Result<Vec<KeyState>> {
    if root.join(GROUP_FILE).exists() {
        return Ok(replay_group(&root.join(GROUP_FILE))?.into_values().collect());
    }
    if !root.exists()
        && let (Some(parent), Some(key)) = (root.parent(), root.file_name().and_then(|k| k.to_str()))
        && parent.join(GROUP_FILE).exists()
    {
        return Ok(replay_group(&parent.join(GROUP_FILE))?
            .remove(key)
            .into_iter()
            .collect());
    }
    Ok(Vec::new())
}

/// Every manifest (pending and released) under `root`, across all appIds.
pub fn all_manifests(root: &Path) -> Result<Vec<Manifest>> {
    let mut all = Vec::new();
    for state in group_states(root)? {
        all.extend(state.pending.into_values().chain(state.released.into_values()));
    }
    if !root.exists() {
        return Ok(all);
    }
    // `root` may also be a single appId's directory.
    if root.join("pending").is_dir() {
        all.extend(read_dir_manifests(&root.join("pending"))?);
        all.extend(read_dir_manifests(&root.join("released"))?);
        return Ok(all);
    }
    for entry in fs::read_dir(root)? {
        let base = entry?.path();
        if !base.is_dir() {
            continue;
        }
        for sub in ["pending", "released"] {
            let dir = base.join(sub);
            if dir.is_dir() {
                all.extend(read_dir_manifests(&dir)?);
            }
        }
    }
    Ok(all)
}

/// Pending manifests in a group log under `root`.
pub fn group_pending(root: &Path) -> Result<usize> {
    Ok(group_states(root)?.iter().map(|s| s.pending.len()).sum())
}

/// Cumulative /proc/diskstats counters for the block device holding `path` (Linux only).
#[derive(Clone, Debug, Default, Serialize)]
pub struct DiskStats {
    pub device: String,
    pub writes: u64,
    pub sectors_written: u64,
    pub write_ticks_ms: u64,
    pub io_ticks_ms: u64,
}

pub fn disk_stats(path: &Path) -> Option<DiskStats> {
    use std::os::unix::fs::MetadataExt;
    let dev = fs::metadata(path).ok()?.dev();
    let (major, minor) = (libc_major(dev), libc_minor(dev));
    let uevent = fs::read_to_string(format!("/sys/dev/block/{major}:{minor}/uevent")).ok()?;
    let name = uevent.lines().find_map(|l| l.strip_prefix("DEVNAME="))?.to_owned();
    let stats = fs::read_to_string("/proc/diskstats").ok()?;
    let fields: Vec<&str> = stats.lines().map(str::split_whitespace).find_map(|mut f| {
        let v: Vec<&str> = f.by_ref().collect();
        (v.get(2) == Some(&name.as_str())).then_some(v)
    })?;
    let n = |i: usize| fields.get(i).and_then(|v| v.parse().ok()).unwrap_or(0);
    Some(DiskStats {
        device: name,
        writes: n(7),
        sectors_written: n(9),
        write_ticks_ms: n(10),
        io_ticks_ms: n(12),
    })
}

fn libc_major(dev: u64) -> u64 {
    ((dev >> 32) & 0xffff_f000) | ((dev >> 8) & 0xfff)
}

fn libc_minor(dev: u64) -> u64 {
    ((dev >> 12) & 0xffff_ff00) | (dev & 0xff)
}
