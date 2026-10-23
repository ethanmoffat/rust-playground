//! Delivery checker for the four guarantees. It scans the raw commit JSON, because a
//! snapshot keys files by path and would hide a duplicate add.

use crate::store::FaultPlan;
use crate::table;
use crate::wal::{FileReady, Manifest, all_manifests};
use anyhow::{Context, Result};
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Instant;
use url::Url;

/// Checker output.
#[derive(Debug, Default, Serialize)]
pub struct CheckReport {
    pub table: String,
    pub log_commits: u64,
    pub latest_version: Option<u64>,
    pub snapshot_load_ms: f64,
    pub snapshot_files: usize,
    pub log_add_actions: u64,
    pub log_unique_added_paths: usize,
    pub log_removed_paths: usize,
    pub txn_app_ids: BTreeMap<String, i64>,
    pub manifests: usize,
    pub manifests_pending: usize,
    pub accepted_files: usize,
    pub sent_files: usize,
    pub sent_not_accepted: usize,
    /// Guarantee 1: accepted files never added.
    pub missing_accepted: Vec<String>,
    /// Guarantee 2: paths added by more than one commit.
    pub duplicate_paths: Vec<(String, Vec<u64>)>,
    /// Guarantee 3: manifests whose files landed in more than one version or only partly.
    pub split_manifests: Vec<String>,
    /// Guarantee 4: snapshot errors, or active files whose data object is missing.
    pub snapshot_error: Option<String>,
    pub dangling_files: Vec<String>,
    pub g1_no_loss: bool,
    pub g2_no_duplicates: bool,
    pub g3_atomic: bool,
    pub g4_valid: bool,
}

fn parse_version(path: &Path) -> Option<u64> {
    let name = path.filename()?;
    let stem = name.strip_suffix(".json")?;
    (stem.len() == 20).then(|| stem.parse().ok()).flatten()
}

/// Run the checker. `wal_roots` hold manifests of every committer; `sent_logs` hold sent files.
pub async fn check(url: &Url, wal_roots: &[std::path::PathBuf], sent_logs: &[std::path::PathBuf]) -> Result<CheckReport> {
    let root = table::root_store(url)?;
    let prefix = Path::from_url_path(url.path())?;
    let log_prefix = Path::from(format!("{}/_delta_log", prefix.as_ref()));
    let mut report = CheckReport { table: url.to_string(), ..CheckReport::default() };

    let commits: Vec<(u64, Path)> = root
        .list(Some(&log_prefix))
        .try_filter_map(|meta| async move { Ok(parse_version(&meta.location).map(|v| (v, meta.location))) })
        .try_collect()
        .await?;
    report.log_commits = commits.len() as u64;
    let bodies: Vec<(u64, bytes::Bytes)> = futures::stream::iter(commits)
        .map(|(version, path)| {
            let root = Arc::clone(&root);
            async move { Ok::<_, anyhow::Error>((version, root.get(&path).await?.bytes().await?)) }
        })
        .buffer_unordered(64)
        .try_collect()
        .await?;

    let mut adds: HashMap<String, Vec<u64>> = HashMap::new();
    let mut removed: BTreeSet<String> = BTreeSet::new();
    for (version, body) in &bodies {
        for line in body.split(|b| *b == b'\n').filter(|l| !l.is_empty()) {
            let value: serde_json::Value = serde_json::from_slice(line).context("log line")?;
            if let Some(path) = value.pointer("/add/path").and_then(|p| p.as_str()) {
                report.log_add_actions += 1;
                adds.entry(path.to_owned()).or_default().push(*version);
            }
            if let Some(path) = value.pointer("/remove/path").and_then(|p| p.as_str()) {
                removed.insert(path.to_owned());
            }
            if let (Some(app), Some(v)) = (
                value.pointer("/txn/appId").and_then(|p| p.as_str()),
                value.pointer("/txn/version").and_then(serde_json::Value::as_i64),
            ) {
                let entry = report.txn_app_ids.entry(app.to_owned()).or_insert(v);
                *entry = (*entry).max(v);
            }
        }
    }
    report.log_unique_added_paths = adds.len();
    report.log_removed_paths = removed.len();
    let mut dups: Vec<(String, Vec<u64>)> = adds
        .iter()
        .filter(|(_, versions)| versions.len() > 1)
        .map(|(path, versions)| {
            let mut v = versions.clone();
            v.sort_unstable();
            (path.clone(), v)
        })
        .collect();
    dups.sort();
    report.duplicate_paths = dups;

    let started = Instant::now();
    match table::open(url, Arc::clone(&root), Arc::new(FaultPlan::default())).await {
        Ok(bench) => {
            report.snapshot_load_ms = started.elapsed().as_secs_f64() * 1000.0;
            report.latest_version = bench.table.version();
            match table::active_paths(&bench.table) {
                Ok(active) => {
                    report.snapshot_files = active.len();
                    let missing: Vec<String> = futures::stream::iter(active)
                        .map(|rel| {
                            let root = Arc::clone(&root);
                            let path = Path::from(format!("{}/{rel}", prefix.as_ref()));
                            async move { root.head(&path).await.err().map(|_| rel) }
                        })
                        .buffer_unordered(64)
                        .filter_map(|m| async move { m })
                        .collect()
                        .await;
                    report.dangling_files = missing;
                }
                Err(error) => report.snapshot_error = Some(error.to_string()),
            }
        }
        Err(error) => report.snapshot_error = Some(error.to_string()),
    }

    let mut manifests: Vec<Manifest> = Vec::new();
    for root in wal_roots {
        manifests.extend(all_manifests(root)?);
        if let Ok(entries) = std::fs::read_dir(root) {
            for entry in entries.flatten() {
                let pending = entry.path().join("pending");
                if let Ok(p) = std::fs::read_dir(pending) {
                    report.manifests_pending += p.flatten().filter(|e| e.path().extension().is_some_and(|x| x == "json")).count();
                }
            }
        }
    }
    report.manifests = manifests.len();
    let mut accepted: BTreeSet<String> = BTreeSet::new();
    for manifest in &manifests {
        let versions: BTreeSet<Option<u64>> =
            manifest.files.iter().map(|f| adds.get(&f.path).and_then(|v| v.first().copied())).collect();
        if versions.len() > 1 {
            report.split_manifests.push(format!("{}@{}: {versions:?}", manifest.app_id, manifest.version));
        }
        accepted.extend(manifest.files.iter().map(|f| f.path.clone()));
    }
    report.accepted_files = accepted.len();
    report.missing_accepted = accepted.iter().filter(|p| !adds.contains_key(*p)).cloned().collect();

    let mut sent: BTreeSet<String> = BTreeSet::new();
    for log in sent_logs {
        let text = std::fs::read_to_string(log).with_context(|| format!("reading {}", log.display()))?;
        for line in text.lines().filter(|l| !l.is_empty()) {
            if let Ok(file) = serde_json::from_str::<FileReady>(line) {
                sent.insert(file.path);
            }
        }
    }
    report.sent_files = sent.len();
    report.sent_not_accepted = sent.difference(&accepted).count();

    report.g1_no_loss = report.missing_accepted.is_empty();
    report.g2_no_duplicates = report.duplicate_paths.is_empty();
    report.g3_atomic = report.split_manifests.is_empty();
    report.g4_valid = report.snapshot_error.is_none() && report.dangling_files.is_empty();
    Ok(report)
}
