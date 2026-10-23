//! The per-table commit loop under test.

use crate::store::{CounterSnapshot, Counters};
use crate::table::{BenchTable, now_millis};
use crate::wal::{FileReady, Manifest, Wal};
use anyhow::{Context, Result};
use deltalake_core::DeltaTable;
use deltalake_core::kernel::transaction::{CommitBuilder, CommitProperties};
use deltalake_core::kernel::{Action, Add, Transaction};
use deltalake_core::protocol::{DeltaOperation, SaveMode};
use serde::Serialize;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

/// When the loop cuts a batch.
#[derive(Clone, Copy, Debug)]
pub enum Pacing {
    /// Commit everything waiting; files that arrive during a commit form the next batch.
    Group,
    /// Commit everything waiting on a fixed interval.
    Interval(Duration),
}

/// How the loop decides whether a batch already landed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, clap::ValueEnum)]
pub enum Idempotency {
    /// `txn` action with this loop's appId; landed when the table's txn version >= batch version.
    Txn,
    /// No `txn`; landed when a recent log entry adds the batch's first file.
    ScanLog,
}

/// Loop settings.
#[derive(Clone, Debug)]
pub struct LoopConfig {
    pub app_id: String,
    pub table_name: String,
    pub pacing: Pacing,
    pub idempotency: Idempotency,
    /// delta-rs internal retry limit (VersionAlreadyExists).
    pub library_max_retries: usize,
    /// Attempts by the loop itself after a commit error.
    pub loop_max_attempts: u32,
    /// Let delta-rs write checkpoints inside the commit call.
    pub inline_checkpoint: bool,
    /// Write a checkpoint after the commit is acknowledged, every N table versions.
    pub separate_checkpoint_every: Option<u64>,
    pub max_files_per_commit: usize,
    /// Log versions scanned back by [`Idempotency::ScanLog`].
    pub scan_window: u64,
}

/// One JSONL record per commit attempt sequence.
#[derive(Clone, Debug, Serialize)]
pub struct CommitRecord {
    pub kind: &'static str,
    pub ts_ms: i64,
    pub table: String,
    pub app_id: String,
    pub txn_version: i64,
    pub delta_version: Option<u64>,
    pub files: usize,
    pub rows: i64,
    pub recovered: bool,
    /// Oldest file: upload done to commit start.
    pub queue_ms: f64,
    /// Manifest write time.
    pub manifest_ms: f64,
    /// Commit call(s) until success.
    pub commit_ms: f64,
    /// Oldest file: upload done to commit acknowledged (writer-side visibility).
    pub visible_oldest_ms: f64,
    /// Newest file: upload done to commit acknowledged.
    pub visible_newest_ms: f64,
    pub library_retries: u64,
    pub loop_attempts: u32,
    pub outcome: &'static str,
    pub errors: Vec<String>,
    pub storage: CounterSnapshot,
    pub checkpoint_ms: Option<f64>,
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

fn add_action(file: &FileReady) -> Action {
    Action::Add(Add {
        path: file.path.clone(),
        partition_values: Default::default(),
        size: file.size,
        modification_time: file.upload_done_ms,
        data_change: true,
        stats: Some(format!("{{\"numRecords\":{}}}", file.rows)),
        tags: None,
        deletion_vector: None,
        base_row_id: None,
        default_row_commit_version: None,
        clustering_provider: None,
    })
}

/// `txn` versions at or above this mark an appId as fenced. The fence stores the fenced
/// owner's last committed version as `FENCE_OFFSET + last + 1` (`last` = -1 if none).
pub const FENCE_OFFSET: i64 = 1 << 62;

fn decode_fence(raw: i64) -> Option<i64> {
    let last = raw - FENCE_OFFSET - 1;
    (last >= 0).then_some(last)
}

/// Another identity's WAL to take over. Its appId is fenced before any manifest moves.
pub struct Adopt {
    pub wal: Wal,
    pub app_id: String,
}

/// Whether a manifest is in the table, from this loop's point of view.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Landed {
    Yes,
    No,
    /// This appId was fenced (or its `txn` entry vanished) and the batch is not known to have
    /// landed. The loop must stop; the manifest stays pending for the adopting owner.
    Fenced,
}

/// The commit loop for one table.
pub struct CommitLoop {
    table: DeltaTable,
    counters: Arc<Counters>,
    wal: Wal,
    cfg: LoopConfig,
    next_version: i64,
    /// Last `txn` version this loop knows it committed. Used to detect fencing and expiry.
    last_committed: Option<i64>,
    /// Snapshot version whose log has been checked for this loop's batches and fences. Every
    /// commit up to it is either this loop's own or passed the delta-rs conflict checker, which
    /// rejects winning commits that carry this loop's appId. While the snapshot stays at this
    /// version, the pre-commit landed check is skipped: the kernel's `txn` lookup replays the
    /// whole log since the last checkpoint, and the scan-log check reads up to `scan_window`
    /// commits one by one.
    verified_through: Option<u64>,
    out: mpsc::UnboundedSender<CommitRecord>,
}

impl CommitLoop {
    /// Build the loop and replay any pending manifests. With `adopt`, fence the previous
    /// identity in the table log, then recommit its unlanded manifests under this loop's appId.
    pub async fn start(
        bench: BenchTable,
        wal: Wal,
        cfg: LoopConfig,
        adopt: Option<Adopt>,
        out: mpsc::UnboundedSender<CommitRecord>,
    ) -> Result<Self> {
        let mut this = Self {
            table: bench.table,
            counters: bench.counters,
            wal,
            cfg,
            next_version: 0,
            last_committed: None,
            verified_through: None,
            out,
        };
        this.recover(adopt).await?;
        Ok(this)
    }

    async fn txn_version_of(&self, app_id: &str) -> Result<Option<i64>> {
        let Ok(state) = self.table.snapshot() else { return Ok(None) };
        Ok(state.transaction_version(self.table.log_store().as_ref(), app_id).await?)
    }

    async fn committed_txn_version(&self) -> Result<Option<i64>> {
        self.txn_version_of(&self.cfg.app_id).await
    }

    /// Commit a `txn`-only entry that fences `old_app_id`, retrying until it lands or the appId
    /// is already fenced. Returns the fenced owner's last committed version.
    ///
    /// The fence carries the same appId as the old owner's commits, so the delta-rs conflict
    /// checker rejects whichever of the two loses the race with ConcurrentTransaction. Every
    /// old-owner commit is therefore either below the fence (and counted in the returned
    /// version) or rejected.
    async fn fence(&mut self, old_app_id: &str) -> Result<Option<i64>> {
        let mut delay = Duration::from_millis(100);
        for attempt in 1..=30 {
            self.refresh().await?;
            let raw = self.txn_version_of(old_app_id).await?;
            if let Some(raw) = raw.filter(|v| *v >= FENCE_OFFSET) {
                return Ok(decode_fence(raw));
            }
            let marker = FENCE_OFFSET + raw.unwrap_or(-1) + 1;
            let props = CommitProperties::default().with_application_transaction(Transaction::new_with_last_update(
                old_app_id,
                marker,
                Some(now_millis()),
            ));
            let result = match self.table.snapshot() {
                Ok(state) => {
                    CommitBuilder::from(props)
                        .with_actions(Vec::new())
                        .build(
                            Some(state),
                            self.table.log_store(),
                            DeltaOperation::Write { mode: SaveMode::Append, partition_by: None, predicate: None },
                        )
                        .await
                }
                Err(error) => Err(error),
            };
            match result {
                Ok(finalized) => {
                    self.table.state = Some(finalized.snapshot());
                    tracing::info!(old_app_id, last = ?raw, "fenced");
                }
                Err(error) => {
                    tracing::warn!(%error, attempt, "fence commit failed; re-reading");
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(5));
                }
            }
        }
        anyhow::bail!("could not fence {old_app_id}")
    }

    async fn refresh(&mut self) -> Result<()> {
        let mut delay = Duration::from_millis(50);
        for _ in 0..20 {
            match self.table.update_incremental(None).await {
                Ok(()) => return Ok(()),
                Err(error) => {
                    tracing::warn!(%error, "refresh failed");
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(2));
                }
            }
        }
        self.table.update_incremental(None).await.context("refresh after retries")
    }

    async fn scan_log_for(&self, path: &str) -> Result<bool> {
        let latest = self.table.version().unwrap_or(0);
        let from = latest.saturating_sub(self.cfg.scan_window);
        let log_store = self.table.log_store();
        let needle = format!("\"path\":\"{path}\"");
        for version in (from..=latest).rev() {
            if let Some(bytes) = log_store.read_commit_entry(version).await?
                && bytes.windows(needle.len()).any(|w| w == needle.as_bytes())
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn landed(&self, manifest: &Manifest) -> Result<Landed> {
        match self.cfg.idempotency {
            Idempotency::Txn => {
                let raw = self.committed_txn_version().await?;
                Ok(match raw {
                    Some(v) if v >= FENCE_OFFSET => {
                        if decode_fence(v).is_some_and(|last| last >= manifest.version) {
                            Landed::Yes
                        } else {
                            Landed::Fenced
                        }
                    }
                    Some(v) if v >= manifest.version => Landed::Yes,
                    // The entry went backwards or vanished (retention expiry): someone else owns
                    // this appId's history now, so committing could duplicate.
                    v if self.last_committed.is_some_and(|last| v.is_none_or(|v| v < last)) => Landed::Fenced,
                    _ => Landed::No,
                })
            }
            Idempotency::ScanLog => match manifest.files.first() {
                Some(file) => Ok(if self.scan_log_for(&file.path).await? { Landed::Yes } else { Landed::No }),
                None => Ok(Landed::Yes),
            },
        }
    }

    async fn recover(&mut self, adopt: Option<Adopt>) -> Result<()> {
        let raw = self.committed_txn_version().await?;
        if raw.is_some_and(|v| v >= FENCE_OFFSET) {
            anyhow::bail!("fenced: appId {} was taken over; pending manifests belong to the new owner", self.cfg.app_id);
        }
        self.last_committed = raw;
        let wal_max = self.wal.max_version()?.unwrap_or(-1);
        self.next_version = raw.unwrap_or(-1).max(wal_max) + 1;
        for manifest in self.wal.pending()? {
            match self.landed(&manifest).await? {
                Landed::Yes => {
                    self.wal.release(manifest.version)?;
                    self.emit_replay(&manifest, "replay_already_landed");
                }
                Landed::No => self.commit_recovered(&manifest).await?,
                Landed::Fenced => anyhow::bail!("fenced during replay of txn version {}", manifest.version),
            }
        }
        if let Some(Adopt { wal: old_wal, app_id: old_app_id }) = adopt {
            let old_last = self.fence(&old_app_id).await?;
            // A restart in the middle of adoption may find a batch in both WALs.
            let mine: std::collections::HashSet<String> = self
                .wal
                .pending()?
                .into_iter()
                .chain(self.wal.released()?)
                .filter_map(|m| m.files.first().map(|f| f.path.clone()))
                .collect();
            for old in old_wal.pending()? {
                let old_version = old.version;
                let already_landed = old_last.is_some_and(|last| old_version <= last);
                let already_mine = old.files.first().is_some_and(|f| mine.contains(&f.path));
                if already_landed || already_mine {
                    old_wal.release(old_version)?;
                    if already_landed {
                        self.emit_replay(&old, "adopt_already_landed");
                    }
                    continue;
                }
                let manifest = Manifest { app_id: self.cfg.app_id.clone(), version: self.next_version, ..old };
                self.next_version += 1;
                self.wal.write(&manifest)?;
                old_wal.release(old_version)?;
                self.commit_recovered(&manifest).await?;
            }
        }
        Ok(())
    }

    async fn commit_recovered(&mut self, manifest: &Manifest) -> Result<()> {
        let record = self.commit_manifest(manifest, true).await;
        let outcome = record.outcome;
        let _ = self.out.send(record);
        if matches!(outcome, "failed" | "unresolved" | "fenced") {
            anyhow::bail!("replay of txn version {} did not complete: {outcome}", manifest.version);
        }
        self.wal.release(manifest.version)
    }

    fn emit_replay(&self, manifest: &Manifest, outcome: &'static str) {
        let _ = self.out.send(CommitRecord {
            kind: "commit",
            ts_ms: now_millis(),
            table: self.cfg.table_name.clone(),
            app_id: self.cfg.app_id.clone(),
            txn_version: manifest.version,
            delta_version: self.table.version(),
            files: manifest.files.len(),
            rows: manifest.files.iter().map(|f| f.rows).sum(),
            recovered: true,
            queue_ms: 0.0,
            manifest_ms: 0.0,
            commit_ms: 0.0,
            visible_oldest_ms: 0.0,
            visible_newest_ms: 0.0,
            library_retries: 0,
            loop_attempts: 0,
            outcome,
            errors: Vec::new(),
            storage: CounterSnapshot::default(),
            checkpoint_ms: None,
        });
    }

    /// After a failed commit, decide whether the batch landed. Only a fresh snapshot and a
    /// successful check count as an answer. `None` means storage never gave one; the caller
    /// must not recommit, because a blind retry can duplicate the batch.
    async fn resolve(&mut self, manifest: &Manifest, errors: &mut Vec<String>) -> Option<Landed> {
        let mut delay = Duration::from_millis(100);
        for _ in 0..20 {
            match self.refresh().await {
                Ok(()) => match self.landed(manifest).await {
                    Ok(answer) => return Some(answer),
                    Err(error) => errors.push(format!("landed check: {error}")),
                },
                Err(error) => errors.push(format!("refresh: {error}")),
            }
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(Duration::from_secs(5));
        }
        None
    }

    async fn commit_manifest(&mut self, manifest: &Manifest, recovered: bool) -> CommitRecord {
        let started = Instant::now();
        let before = self.counters.snapshot();
        let mut errors = Vec::new();
        let mut attempts = 0;
        let mut library_retries = 0;
        let mut outcome = "failed";
        let mut delay = Duration::from_millis(100);
        while attempts < self.cfg.loop_max_attempts {
            attempts += 1;
            // The conflict checker only sees commits after our snapshot. A fence already inside
            // the snapshot must be caught here, unless the snapshot is already verified.
            let verified = self.verified_through.is_some() && self.verified_through == self.table.version();
            let pre_check = if verified { Ok(Landed::No) } else { self.landed(manifest).await };
            match pre_check {
                Ok(Landed::No) => self.verified_through = self.table.version(),
                Ok(Landed::Yes) => {
                    outcome = "landed_after_error";
                    break;
                }
                Ok(Landed::Fenced) => {
                    outcome = "fenced";
                    break;
                }
                Err(error) => {
                    errors.push(format!("pre-commit check: {error}"));
                    match self.resolve(manifest, &mut errors).await {
                        Some(Landed::No) => {}
                        Some(Landed::Yes) => {
                            outcome = "landed_after_error";
                            break;
                        }
                        Some(Landed::Fenced) => {
                            outcome = "fenced";
                            break;
                        }
                        None => {
                            outcome = "unresolved";
                            break;
                        }
                    }
                }
            }
            let mut props = CommitProperties::default()
                .with_create_checkpoint(self.cfg.inline_checkpoint)
                .with_max_retries(self.cfg.library_max_retries);
            if self.cfg.idempotency == Idempotency::Txn {
                props = props.with_application_transaction(Transaction::new_with_last_update(
                    &self.cfg.app_id,
                    manifest.version,
                    Some(now_millis()),
                ));
            }
            let actions = manifest.files.iter().map(add_action).collect();
            let result = match self.table.snapshot() {
                Ok(state) => {
                    CommitBuilder::from(props)
                        .with_actions(actions)
                        .build(
                            Some(state),
                            self.table.log_store(),
                            DeltaOperation::Write { mode: SaveMode::Append, partition_by: None, predicate: None },
                        )
                        .await
                }
                Err(error) => Err(error),
            };
            match result {
                Ok(finalized) => {
                    library_retries += finalized.metrics.num_retries;
                    self.table.state = Some(finalized.snapshot());
                    self.verified_through = self.table.version();
                    outcome = if attempts == 1 { "ok" } else { "ok_after_loop_retry" };
                    break;
                }
                Err(error) => {
                    errors.push(error.to_string());
                    match self.resolve(manifest, &mut errors).await {
                        Some(Landed::Yes) => {
                            outcome = "landed_after_error";
                            break;
                        }
                        Some(Landed::Fenced) => {
                            outcome = "fenced";
                            break;
                        }
                        Some(Landed::No) => {}
                        None => {
                            outcome = "unresolved";
                            break;
                        }
                    }
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(5));
                }
            }
        }
        if matches!(outcome, "ok" | "ok_after_loop_retry" | "landed_after_error") {
            self.last_committed = Some(self.last_committed.map_or(manifest.version, |v| v.max(manifest.version)));
        }
        let commit_ms = ms(started.elapsed());
        let now = now_millis();
        let oldest = manifest.files.iter().map(|f| f.upload_done_ms).min().unwrap_or(now);
        let newest = manifest.files.iter().map(|f| f.upload_done_ms).max().unwrap_or(now);
        CommitRecord {
            kind: "commit",
            ts_ms: now,
            table: self.cfg.table_name.clone(),
            app_id: self.cfg.app_id.clone(),
            txn_version: manifest.version,
            delta_version: self.table.version(),
            files: manifest.files.len(),
            rows: manifest.files.iter().map(|f| f.rows).sum(),
            recovered,
            queue_ms: 0.0,
            manifest_ms: 0.0,
            commit_ms,
            #[allow(clippy::cast_precision_loss)]
            visible_oldest_ms: (now - oldest) as f64,
            #[allow(clippy::cast_precision_loss)]
            visible_newest_ms: (now - newest) as f64,
            library_retries,
            loop_attempts: attempts,
            outcome,
            errors,
            storage: self.counters.snapshot().since(&before),
            checkpoint_ms: None,
        }
    }

    async fn maybe_checkpoint(&mut self) -> Option<f64> {
        let every = self.cfg.separate_checkpoint_every?;
        let version = self.table.version()?;
        if version == 0 || version % every != 0 {
            return None;
        }
        let started = Instant::now();
        match deltalake_core::checkpoints::create_checkpoint(&self.table, None).await {
            Ok(()) => Some(ms(started.elapsed())),
            Err(error) => {
                tracing::warn!(%error, "checkpoint failed");
                None
            }
        }
    }

    /// Run until `rx` closes and drains.
    pub async fn run(mut self, mut rx: mpsc::Receiver<FileReady>) -> Result<()> {
        let mut next_tick = tokio::time::Instant::now();
        loop {
            let mut batch = Vec::new();
            match self.cfg.pacing {
                Pacing::Group => match rx.recv().await {
                    Some(file) => batch.push(file),
                    None => break,
                },
                Pacing::Interval(interval) => {
                    next_tick += interval;
                    tokio::time::sleep_until(next_tick).await;
                    if rx.is_closed() && rx.is_empty() {
                        break;
                    }
                }
            }
            while batch.len() < self.cfg.max_files_per_commit {
                match rx.try_recv() {
                    Ok(file) => batch.push(file),
                    Err(_) => break,
                }
            }
            if batch.is_empty() {
                continue;
            }
            let commit_start = now_millis();
            let manifest_started = Instant::now();
            let manifest = Manifest {
                app_id: self.cfg.app_id.clone(),
                version: self.next_version,
                created_ms: commit_start,
                files: batch,
            };
            self.next_version += 1;
            self.wal.write(&manifest)?;
            let manifest_ms = ms(manifest_started.elapsed());
            let mut record = self.commit_manifest(&manifest, false).await;
            record.manifest_ms = manifest_ms;
            #[allow(clippy::cast_precision_loss)]
            {
                record.queue_ms =
                    (commit_start - manifest.files.iter().map(|f| f.upload_done_ms).min().unwrap_or(commit_start)) as f64;
            }
            if matches!(record.outcome, "failed" | "unresolved" | "fenced") {
                let _ = self.out.send(record);
                anyhow::bail!("commit of txn version {} did not complete", manifest.version);
            }
            self.wal.release(manifest.version)?;
            record.checkpoint_ms = self.maybe_checkpoint().await;
            let _ = self.out.send(record);
        }
        Ok(())
    }
}
