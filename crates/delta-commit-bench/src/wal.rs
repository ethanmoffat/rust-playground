//! Local WAL output manifests. A manifest fixes a commit's txn version and exact file list
//! before any commit attempt, so replay recommits the same batch under the same version.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

/// Metadata an encoder sends to the commit loop after a data file is fully uploaded.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FileReady {
    /// Path relative to the table root.
    pub path: String,
    pub size: i64,
    pub rows: i64,
    pub encoder: String,
    pub upload_start_ms: i64,
    pub upload_done_ms: i64,
}

/// One batch with its assigned txn version.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Manifest {
    pub app_id: String,
    pub version: i64,
    pub created_ms: i64,
    pub files: Vec<FileReady>,
}

/// Directory of manifests for one appId.
pub struct Wal {
    pending: PathBuf,
    released: PathBuf,
}

fn sanitize(app_id: &str) -> String {
    app_id.chars().map(|c| if c.is_ascii_alphanumeric() || c == '-' { c } else { '_' }).collect()
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
            let bytes = fs::read(&path)?;
            manifests.push(serde_json::from_slice(&bytes).with_context(|| format!("parsing {}", path.display()))?);
        }
    }
    manifests.sort_by_key(|m: &Manifest| m.version);
    Ok(manifests)
}

impl Wal {
    /// Open or create the manifest directory for `app_id` under `root`.
    pub fn open(root: &Path, app_id: &str) -> Result<Self> {
        let base = root.join(sanitize(app_id));
        let pending = base.join("pending");
        let released = base.join("released");
        fs::create_dir_all(&pending)?;
        fs::create_dir_all(&released)?;
        Ok(Self { pending, released })
    }

    /// Durably record `manifest` as pending.
    pub fn write(&self, manifest: &Manifest) -> Result<()> {
        let tmp = self.pending.join(format!(".{}.tmp", file_name(manifest.version)));
        let mut file = File::create(&tmp)?;
        file.write_all(&serde_json::to_vec(manifest)?)?;
        file.sync_all()?;
        fs::rename(&tmp, self.pending.join(file_name(manifest.version)))?;
        sync_dir(&self.pending)
    }

    /// Mark the manifest at `version` as committed.
    pub fn release(&self, version: i64) -> Result<()> {
        let name = file_name(version);
        fs::rename(self.pending.join(&name), self.released.join(&name))?;
        sync_dir(&self.pending)
    }

    /// Pending manifests in version order.
    pub fn pending(&self) -> Result<Vec<Manifest>> {
        read_dir_manifests(&self.pending)
    }

    /// Released manifests in version order.
    pub fn released(&self) -> Result<Vec<Manifest>> {
        read_dir_manifests(&self.released)
    }

    /// Highest version ever assigned, pending or released.
    pub fn max_version(&self) -> Result<Option<i64>> {
        let pending = self.pending()?.last().map(|m| m.version);
        let released = self.released()?.last().map(|m| m.version);
        Ok(pending.max(released))
    }
}

/// Every manifest (pending and released) under `root`, across all appIds.
pub fn all_manifests(root: &Path) -> Result<Vec<Manifest>> {
    let mut all = Vec::new();
    if !root.exists() {
        return Ok(all);
    }
    for entry in fs::read_dir(root)? {
        let base = entry?.path();
        for sub in ["pending", "released"] {
            let dir = base.join(sub);
            if dir.is_dir() {
                all.extend(read_dir_manifests(&dir)?);
            }
        }
    }
    Ok(all)
}
