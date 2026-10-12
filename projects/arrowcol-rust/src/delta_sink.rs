use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_deltalake::commit::{BatchedTable, CommitOptions};
use arrow_deltalake::delta::PublicationFence as DeltaFence;
use arrowcol_buffer::{DeltaPublicationSink, PublicationContext, PublicationSession, PublicationSink, SinkError};
use async_trait::async_trait;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

use crate::lease::TableLeaseManager;

/// Multi-table Delta sink that opens one fenced coordinator per owned table.
pub struct ProductionDeltaSink {
    root: PathBuf,
    options: CommitOptions,
    leases: Arc<TableLeaseManager>,
}

impl ProductionDeltaSink {
    /// Creates a dynamic multi-table publication sink.
    #[must_use]
    pub fn new(root: PathBuf, options: CommitOptions, leases: Arc<TableLeaseManager>) -> Self {
        Self { root, options, leases }
    }

    /// Returns the physical table path.
    #[must_use]
    pub fn table_path(&self, table: &arrowcol_protocol::TableIdentity) -> PathBuf {
        self.root
            .join(table.catalog().as_str())
            .join(table.schema().as_str())
            .join(table.table().as_str())
    }
}

#[async_trait]
impl PublicationSink for ProductionDeltaSink {
    async fn begin(&self, context: PublicationContext) -> Result<Box<dyn PublicationSession>, SinkError> {
        let handle = self
            .leases
            .handle_for_fence(&context.table, &context.fence)
            .await
            .map_err(|error| SinkError::fenced(format!("validate table publication lease: {error:#}")))?;
        let table = if let Some(table) = handle.delta_table().await {
            table
        } else {
            let fence: Arc<dyn DeltaFence> = handle.delta_fence();
            let opened = BatchedTable::open_with_fence(self.table_path(&context.table), self.options.clone(), fence)
                .await
                .map_err(|error| SinkError::retryable(format!("open Delta table coordinator: {error:#}")))?;
            handle.install_delta_table(opened).await
        };
        DeltaPublicationSink::new(
            table,
            context.table.clone(),
            context.schema_fingerprint,
            context.fence.clone(),
        )
        .begin(context)
        .await
    }
}

/// Verifies that the configured Delta root can create, synchronize, and remove
/// a file on the target filesystem.
///
/// # Errors
///
/// Returns the failed filesystem operation.
pub async fn verify_writable_root(root: &Path) -> std::io::Result<()> {
    tokio::fs::create_dir_all(root).await?;
    let probe = root.join(format!(".arrowcol-write-probe-{}", Uuid::new_v4()));
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&probe)
        .await?;
    file.write_all(b"arrowcol").await?;
    file.sync_all().await?;
    drop(file);
    tokio::fs::remove_file(&probe).await?;
    tokio::fs::File::open(root).await?.sync_all().await
}
