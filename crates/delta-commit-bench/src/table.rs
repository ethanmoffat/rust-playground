//! Storage setup, table open and create, and synthetic Parquet data files.

use crate::store::{Counters, FaultPlan, InstrumentedStore};
use anyhow::{Context, Result, bail};
use deltalake_core::arrow::array::{Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use deltalake_core::arrow::datatypes::{DataType as ArrowType, Field, Schema, TimeUnit};
use deltalake_core::kernel::{DataType, PrimitiveType};
use deltalake_core::logstore::{LogStore, LogStoreFactory, StorageConfig, default_logstore, logstore_factories};
use deltalake_core::parquet::arrow::ArrowWriter;
use deltalake_core::parquet::basic::{Compression, ZstdLevel};
use deltalake_core::parquet::file::properties::WriterProperties;
use deltalake_core::{DeltaResult, DeltaTable, DeltaTableBuilder, DeltaTableError, ObjectStore};
use object_store::azure::MicrosoftAzureBuilder;
use object_store::local::LocalFileSystem;
use object_store::{ClientOptions, RetryConfig};
use std::sync::{Arc, Once};
use std::time::Duration;
use url::Url;

struct BenchLogStoreFactory;

impl LogStoreFactory for BenchLogStoreFactory {
    fn with_options(
        &self,
        prefixed_store: Arc<dyn ObjectStore>,
        root_store: Arc<dyn ObjectStore>,
        location: &Url,
        options: &StorageConfig,
    ) -> DeltaResult<Arc<dyn LogStore>> {
        Ok(default_logstore(prefixed_store, root_store, location, options))
    }
}

/// Register delta-rs's default (conditional put) log store for Azure URL schemes.
pub fn register_azure_schemes() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        for scheme in ["abfss://", "abfs://", "az://", "azure://"] {
            if let Ok(url) = Url::parse(scheme) {
                logstore_factories().insert(url, Arc::new(BenchLogStoreFactory));
            }
        }
    });
}

/// Build the container-rooted store for `url`. Azure auth comes from the environment
/// (`AZURE_USE_AZURE_CLI=true` on a workstation; managed identity on a VM).
pub fn root_store(url: &Url) -> Result<Arc<dyn ObjectStore>> {
    match url.scheme() {
        "file" => Ok(Arc::new(LocalFileSystem::new())),
        "abfss" | "abfs" | "az" | "azure" => {
            let store = MicrosoftAzureBuilder::from_env()
                .with_url(url.as_str())
                .with_retry(RetryConfig::default())
                .with_client_options(
                    ClientOptions::new()
                        .with_timeout(Duration::from_secs(30))
                        .with_pool_max_idle_per_host(256),
                )
                .build()
                .context("building Azure store")?;
            Ok(Arc::new(store))
        }
        other => bail!("unsupported scheme {other}"),
    }
}

/// A table plus its private request counters.
pub struct BenchTable {
    pub table: DeltaTable,
    pub counters: Arc<Counters>,
}

/// Open the table at `url` over `root`, wrapping storage with counters and `faults`.
/// A missing table is returned unloaded.
pub async fn open(url: &Url, root: Arc<dyn ObjectStore>, faults: Arc<FaultPlan>) -> Result<BenchTable> {
    register_azure_schemes();
    let counters = Arc::new(Counters::default());
    let store: Arc<dyn ObjectStore> = Arc::new(InstrumentedStore::new(root, Arc::clone(&counters), faults));
    let mut table = DeltaTableBuilder::from_url(url.clone())?
        .with_storage_backend(store, url.clone())
        .build()?;
    match table.load().await {
        Ok(()) | Err(DeltaTableError::NotATable(_)) => {}
        Err(error) => return Err(error.into()),
    }
    Ok(BenchTable { table, counters })
}

/// Active data file paths relative to the table root.
pub fn active_paths(table: &DeltaTable) -> Result<Vec<String>> {
    Ok(table.snapshot()?.log_data().into_iter().map(|f| f.object_store_path().to_string()).collect())
}

/// Open with retries, for loops that must survive transient storage errors at startup.
pub async fn open_with_retry(url: &Url, root: Arc<dyn ObjectStore>, faults: Arc<FaultPlan>) -> Result<BenchTable> {
    let mut delay = Duration::from_millis(100);
    let mut attempt = 0;
    loop {
        attempt += 1;
        match open(url, Arc::clone(&root), Arc::clone(&faults)).await {
            Ok(table) => return Ok(table),
            Err(error) if attempt < 30 => {
                tracing::warn!(%error, attempt, "open failed; retrying");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(5));
            }
            Err(error) => return Err(error),
        }
    }
}

/// Arrow schema of the synthetic data files.
pub fn arrow_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", ArrowType::Int64, false),
        Field::new("ts", ArrowType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
        Field::new("encoder", ArrowType::Utf8, false),
        Field::new("payload", ArrowType::Utf8, false),
    ]))
}

/// Column set of a benchmark table.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TableSchema {
    /// [`arrow_schema`], written by the small-file producer.
    #[default]
    Small,
    /// [`crate::arrow_producer::logs_schema`], written by the Arrow producer.
    Logs,
}

/// Create an append-only table with the benchmark schema and `properties`.
pub async fn create(
    url: &Url,
    root: Arc<dyn ObjectStore>,
    properties: &[(String, String)],
    schema: TableSchema,
) -> Result<u64> {
    if url.scheme() == "file" {
        std::fs::create_dir_all(url.path())?;
    }
    let bench = open(url, root, Arc::new(FaultPlan::default())).await?;
    if bench.table.version().is_some() {
        bail!("table already exists at {url}");
    }
    let columns: Vec<(&str, PrimitiveType)> = match schema {
        TableSchema::Small => vec![
            ("id", PrimitiveType::Long),
            ("ts", PrimitiveType::Timestamp),
            ("encoder", PrimitiveType::String),
            ("payload", PrimitiveType::String),
        ],
        TableSchema::Logs => crate::arrow_producer::delta_columns(),
    };
    let mut builder = bench.table.create().with_table_name("bench");
    for (name, ty) in columns {
        builder = builder.with_column(name, DataType::Primitive(ty), false, None);
    }
    let table = builder
        .with_configuration(properties.iter().map(|(k, v)| (k.clone(), Some(v.clone()))))
        .await?;
    table.version().context("created table has no version")
}

/// Encode a small real Parquet file with `rows` rows starting at `first_id`.
pub fn small_parquet(first_id: i64, rows: usize, encoder: &str) -> Result<Vec<u8>> {
    let ids: Vec<i64> = (first_id..first_id + i64::try_from(rows)?).collect();
    let now = now_micros();
    let batch = RecordBatch::try_new(
        arrow_schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(TimestampMicrosecondArray::from(vec![now; rows]).with_timezone("UTC")),
            Arc::new(StringArray::from(vec![encoder; rows])),
            Arc::new(StringArray::from(vec!["payload-0123456789abcdef"; rows])),
        ],
    )?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(1)?))
        .build();
    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, arrow_schema(), Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(buffer)
}

/// Wall clock in microseconds since the Unix epoch.
pub fn now_micros() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros(),
    )
    .unwrap_or(i64::MAX)
}

/// Wall clock in milliseconds since the Unix epoch.
pub fn now_millis() -> i64 {
    now_micros() / 1000
}
