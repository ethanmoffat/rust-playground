use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock, Mutex, Weak};

use arrow_deltalake::commit::{BatchedTable, CommitReceiptError};
use async_trait::async_trait;
use futures::future::{FutureExt, LocalBoxFuture};
use futures::stream::{FuturesUnordered, StreamExt};
use linkme::distributed_slice;
use otel_arrow_dfe_config::node::NodeUserConfig;
use otel_arrow_dfe_engine::config::ExporterConfig;
use otel_arrow_dfe_engine::context::PipelineContext;
use otel_arrow_dfe_engine::control::{AckMsg, NackCause, NackMsg, NodeControlMsg};
use otel_arrow_dfe_engine::error::Error;
use otel_arrow_dfe_engine::exporter::ExporterWrapper;
use otel_arrow_dfe_engine::local::exporter;
use otel_arrow_dfe_engine::message::{ExporterInbox, Message};
use otel_arrow_dfe_engine::node::NodeId;
use otel_arrow_dfe_engine::terminal_state::TerminalState;
use otel_arrow_dfe_engine::{ConsumerEffectHandlerExtension, ExporterFactory};
use otel_arrow_dfe_otap::OTAP_EXPORTER_FACTORIES;
use otel_arrow_dfe_otap::pdata::OtapPdata;
use otel_arrow_dfe_pdata::PayloadData;

use crate::DeltaExporterConfig;
use crate::config::{config_error, parse_config, table_path};
use crate::envelope::{DecodedArrowBatch, EnvelopeError, decode_record_set};
use crate::metrics::{RuntimeMetrics, global_metrics};
use crate::routing::{AuthorityClient, LeaseEvidence, RoutingPublicationFence};

/// DFE URN for arbitrary Arrow Delta output.
pub const ARROW_DELTALAKE_EXPORTER_URN: &str = "urn:otel:exporter:arrow_deltalake";

type BackendRegistry = Mutex<HashMap<DeltaExporterConfig, Weak<DeltaBackend>>>;
static BACKENDS: LazyLock<BackendRegistry> = LazyLock::new(|| Mutex::new(HashMap::new()));

/// Generic Delta exporter runtime.
pub struct GenericDeltaExporter {
    backend: Arc<DeltaBackend>,
}

impl GenericDeltaExporter {
    fn shared(config: DeltaExporterConfig, metrics: Arc<RuntimeMetrics>) -> Result<Self, String> {
        if let Some(existing) = BACKENDS
            .lock()
            .map_err(|_| "Delta backend registry lock poisoned".to_owned())?
            .get(&config)
            .and_then(Weak::upgrade)
        {
            return Ok(Self { backend: existing });
        }
        let store = config.routing.build()?;
        let options = config.commit.build()?;
        metrics.configure_exporter(options.max_pending_files);
        let authority = AuthorityClient::start(store, config.authority_timeout, options.max_pending_files)?;
        let backend = Arc::new(DeltaBackend {
            base_path: config.base_path.clone(),
            options,
            authority,
            metrics,
            tables: tokio::sync::Mutex::new(HashMap::new()),
        });
        BACKENDS
            .lock()
            .map_err(|_| "Delta backend registry lock poisoned".to_owned())?
            .insert(config, Arc::downgrade(&backend));
        Ok(Self { backend })
    }

    #[cfg(test)]
    fn with_store(
        base_path: PathBuf,
        store: Arc<dyn arrowcol_routing::RoutingStore>,
        options: arrow_deltalake::commit::CommitOptions,
        metrics: Arc<RuntimeMetrics>,
    ) -> Result<Self, String> {
        metrics.configure_exporter(options.max_pending_files);
        Ok(Self {
            backend: Arc::new(DeltaBackend {
                base_path,
                authority: AuthorityClient::start(store, std::time::Duration::from_secs(2), options.max_pending_files)?,
                options,
                metrics,
                tables: tokio::sync::Mutex::new(HashMap::new()),
            }),
        })
    }

    #[cfg(test)]
    async fn export(&self, pdata: OtapPdata) -> Result<(), ExportFailure> {
        self.stage(pdata).await?.wait().await
    }

    async fn stage(&self, pdata: OtapPdata) -> Result<PendingDelta, ExportFailure> {
        let (_, payload) = pdata.into_parts();
        let records = match payload.into_data() {
            PayloadData::PdataArrowRecords(records) => *records,
            PayloadData::OtlpBytes(_) | PayloadData::OtapArrowRecords(_) => {
                return Err(ExportFailure::Permanent(
                    "generic Delta exporter accepts only Arrowcol pluggable Arrow pdata".to_owned(),
                ));
            }
        };
        let mut decoded = decode_record_set(records).map_err(ExportFailure::Envelope)?;
        if decoded.len() != 1 {
            return Err(ExportFailure::Permanent(
                "Arrowcol Flight pdata must contain exactly one root table".to_owned(),
            ));
        }
        let table = decoded
            .pop()
            .ok_or_else(|| ExportFailure::Permanent("Arrowcol Flight pdata contains no root table".to_owned()))?;
        self.backend.stage_table(table).await
    }
}

struct DeltaBackend {
    base_path: PathBuf,
    options: arrow_deltalake::commit::CommitOptions,
    authority: AuthorityClient,
    metrics: Arc<RuntimeMetrics>,
    tables: tokio::sync::Mutex<HashMap<arrowcol_protocol::TableIdentity, Arc<TableOwner>>>,
}

impl DeltaBackend {
    async fn owner(&self, decoded: &DecodedArrowBatch) -> Result<Arc<TableOwner>, ExportFailure> {
        let mut tables = self.tables.lock().await;
        if let Some(owner) = tables.get(&decoded.envelope.table) {
            return Ok(Arc::clone(owner));
        }
        let evidence = LeaseEvidence::from_envelope(&decoded.envelope);
        let fence = Arc::new(RoutingPublicationFence::new(evidence.clone(), self.authority.clone()));
        let publication_fence: Arc<dyn arrow_deltalake::delta::PublicationFence> =
            Arc::clone(&fence) as Arc<dyn arrow_deltalake::delta::PublicationFence>;
        let table = BatchedTable::open_with_fence(
            table_path(&self.base_path, &decoded.envelope.table),
            self.options.clone(),
            publication_fence,
        )
        .await
        .map_err(|error| ExportFailure::Transient(error.to_string()))?;
        let owner = Arc::new(TableOwner {
            table,
            fence,
            state: Mutex::new(OwnerState { evidence, pending: 0 }),
        });
        tables.insert(decoded.envelope.table.clone(), Arc::clone(&owner));
        Ok(owner)
    }

    async fn stage_table(&self, decoded: DecodedArrowBatch) -> Result<PendingDelta, ExportFailure> {
        let owner = self.owner(&decoded).await?;
        let evidence = LeaseEvidence::from_envelope(&decoded.envelope);
        let pending_owner = owner.begin(evidence)?;
        let rows = u64::try_from(decoded.batch.num_rows()).unwrap_or(u64::MAX);
        let bytes = arrow_deltalake::record_batch_logical_bytes(&decoded.batch)
            .map_err(|error| ExportFailure::Permanent(error.to_string()))?;
        self.metrics.observe_schema_versions(
            &decoded.envelope.table,
            decoded.envelope.source_schema_version,
            decoded.envelope.target_schema_version,
        );
        self.metrics.publication_started(&decoded.envelope.table);
        let staged = async {
            let mut writer = owner
                .table
                .new_file(decoded.batch.schema())
                .await
                .map_err(|error| ExportFailure::Permanent(error.to_string()))?;
            writer
                .write(&decoded.batch)
                .await
                .map_err(|error| ExportFailure::Permanent(error.to_string()))?;
            let append = writer
                .stage()
                .await
                .map_err(|error| ExportFailure::Transient(error.to_string()))?;
            let receipt = owner
                .table
                .submit(append, false)
                .await
                .map_err(|error| ExportFailure::Transient(error.to_string()))?;
            Ok::<_, ExportFailure>(receipt)
        }
        .await;
        let receipt = match staged {
            Ok(receipt) => receipt,
            Err(error) => {
                self.metrics
                    .publication_finished(&decoded.envelope.table, rows, bytes, None);
                return Err(error);
            }
        };
        Ok(PendingDelta {
            receipt: Some(receipt),
            pending_owner: Some(pending_owner),
            metrics: Arc::clone(&self.metrics),
            table: decoded.envelope.table,
            rows,
            bytes,
            accounted: false,
        })
    }

    async fn start_draining(&self) {
        let tables = self.tables.lock().await;
        for owner in tables.values() {
            owner.table.start_draining();
        }
    }
}

struct PendingDelta {
    receipt: Option<arrow_deltalake::commit::CommitReceipt>,
    pending_owner: Option<PendingOwner>,
    metrics: Arc<RuntimeMetrics>,
    table: arrowcol_protocol::TableIdentity,
    rows: u64,
    bytes: u64,
    accounted: bool,
}

impl PendingDelta {
    async fn wait(mut self) -> Result<(), ExportFailure> {
        let receipt = self
            .receipt
            .take()
            .ok_or_else(|| ExportFailure::Transient("Delta receipt is missing".to_owned()))?;
        let result = receipt.wait_typed().await.map_err(map_receipt_error);
        match &result {
            Ok(append) => self
                .metrics
                .publication_finished(&self.table, self.rows, self.bytes, Some(append.version)),
            Err(_) => self
                .metrics
                .publication_finished(&self.table, self.rows, self.bytes, None),
        }
        self.accounted = true;
        let _ = self.pending_owner.take();
        result.map(|_| ())
    }
}

impl Drop for PendingDelta {
    fn drop(&mut self) {
        if !self.accounted {
            self.metrics
                .publication_finished(&self.table, self.rows, self.bytes, None);
        }
    }
}

struct OwnerState {
    evidence: LeaseEvidence,
    pending: usize,
}

struct TableOwner {
    table: Arc<BatchedTable>,
    fence: Arc<RoutingPublicationFence>,
    state: Mutex<OwnerState>,
}

impl TableOwner {
    fn begin(self: &Arc<Self>, evidence: LeaseEvidence) -> Result<PendingOwner, ExportFailure> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ExportFailure::Transient("table owner lock poisoned".to_owned()))?;
        if state.evidence != evidence {
            if state.pending != 0 {
                return Err(ExportFailure::Transient(
                    "table lease changed while prior publications remain pending".to_owned(),
                ));
            }
            self.fence
                .replace_expected(evidence.clone())
                .map_err(ExportFailure::Transient)?;
            state.evidence = evidence;
        }
        state.pending = state.pending.saturating_add(1);
        Ok(PendingOwner {
            owner: Arc::clone(self),
        })
    }
}

struct PendingOwner {
    owner: Arc<TableOwner>,
}

impl Drop for PendingOwner {
    fn drop(&mut self) {
        if let Ok(mut state) = self.owner.state.lock() {
            state.pending = state.pending.saturating_sub(1);
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum ExportFailure {
    #[error("{0}")]
    Permanent(String),
    #[error("{0}")]
    Transient(String),
    #[error("{0}")]
    Envelope(EnvelopeError),
}

impl ExportFailure {
    fn permanent(&self) -> bool {
        matches!(self, Self::Permanent(_) | Self::Envelope(_))
    }
}

fn map_receipt_error(error: CommitReceiptError) -> ExportFailure {
    match error {
        CommitReceiptError::PublicationFenced(error) => ExportFailure::Transient(error.to_string()),
        CommitReceiptError::CommitFailed { message } => ExportFailure::Transient(message),
        CommitReceiptError::WorkerStopped => ExportFailure::Transient("Delta commit worker stopped".to_owned()),
        _ => ExportFailure::Transient("Delta commit failed".to_owned()),
    }
}

fn validate_exporter_config(value: &serde_json::Value) -> Result<(), otel_arrow_dfe_config::error::Error> {
    let config: DeltaExporterConfig = parse_config(value)?;
    config.validate().map_err(config_error)
}

fn create_exporter(
    _pipeline: PipelineContext,
    node: NodeId,
    node_config: Arc<NodeUserConfig>,
    exporter_config: &ExporterConfig,
    _capabilities: &otel_arrow_dfe_engine::capability::registry::Capabilities,
) -> Result<ExporterWrapper<OtapPdata>, otel_arrow_dfe_config::error::Error> {
    let config: DeltaExporterConfig = parse_config(&node_config.config)?;
    config.validate().map_err(config_error)?;
    let exporter = GenericDeltaExporter::shared(config, global_metrics()).map_err(config_error)?;
    Ok(ExporterWrapper::local(exporter, node, node_config, exporter_config))
}

/// External DFE exporter registration.
#[allow(unsafe_code)]
#[otel_arrow_dfe_engine::component_inventory(category = Exporter)]
#[distributed_slice(OTAP_EXPORTER_FACTORIES)]
pub static ARROW_DELTALAKE_EXPORTER: ExporterFactory<OtapPdata> = ExporterFactory {
    name: ARROW_DELTALAKE_EXPORTER_URN,
    create: create_exporter,
    context_declarations: None,
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED
        .with_pdata(otel_arrow_dfe_engine::wiring_contract::PdataWiringContract::PLUGGABLE_ARROW_SINK),
    validate_config: validate_exporter_config,
};

#[async_trait(?Send)]
impl exporter::Exporter<OtapPdata> for GenericDeltaExporter {
    async fn start(
        self: Box<Self>,
        mut inbox: ExporterInbox<OtapPdata>,
        effect_handler: exporter::EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, Error> {
        let mut pending: FuturesUnordered<LocalBoxFuture<'static, (OtapPdata, Result<(), ExportFailure>)>> =
            FuturesUnordered::new();
        loop {
            let message = if pending.is_empty() {
                Some(inbox.recv().await?)
            } else {
                tokio::select! {
                    message = inbox.recv() => Some(message?),
                    completion = pending.next() => {
                        if let Some((pdata, result)) = completion {
                            complete_publication(&effect_handler, pdata, result).await?;
                        }
                        None
                    }
                }
            };
            let Some(message) = message else {
                continue;
            };
            match message {
                Message::PData(pdata) => {
                    let export_copy = pdata.clone();
                    match self.stage(export_copy).await {
                        Ok(publication) => pending.push(
                            async move {
                                let result = publication.wait().await;
                                (pdata, result)
                            }
                            .boxed_local(),
                        ),
                        Err(error) => {
                            complete_publication(&effect_handler, pdata, Err(error)).await?;
                        }
                    }
                }
                Message::Control(NodeControlMsg::Shutdown { .. }) => {
                    self.backend.start_draining().await;
                    while let Some((pdata, result)) = pending.next().await {
                        complete_publication(&effect_handler, pdata, result).await?;
                    }
                    break;
                }
                Message::Control(_) => {}
            }
        }
        Ok(TerminalState::default())
    }
}

async fn complete_publication(
    effect_handler: &exporter::EffectHandler<OtapPdata>,
    pdata: OtapPdata,
    result: Result<(), ExportFailure>,
) -> Result<(), Error> {
    match result {
        Ok(()) => effect_handler.notify_ack(AckMsg::new(pdata)).await,
        Err(error) if error.permanent() => {
            effect_handler
                .notify_nack(NackMsg::new_permanent_with_cause(
                    error.to_string(),
                    pdata,
                    NackCause::Refused,
                ))
                .await
        }
        Err(error) => effect_handler.notify_nack(NackMsg::new(error.to_string(), pdata)).await,
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use std::sync::Arc;

    use arrow_array::{ArrayRef, StringArray, UInt64Array};
    use arrow_schema::{DataType, Field, Schema};
    use arrowcol_catalog::SchemaVersion;
    use arrowcol_protocol::{
        BasisPoints, CatalogName, CollectorInstanceId, FencingToken, ImplementationPool, Measurement,
        PositiveMeasurement, PressureGauge, SchemaName, TableIdentity, TableName, UnixMillis,
    };
    use arrowcol_routing::{
        CollectorRegistration, LeaseClaim, LeaseKey, MemoryRoutingStore, RichPressureSnapshot, RoutingStore,
        SubsystemState,
    };
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    use super::*;
    use crate::envelope::{ArrowcolBatchEnvelope, encode_record_batch};
    use crate::metrics::RuntimeMetrics;

    fn now() -> UnixMillis {
        crate::config::now_unix_millis().expect("now")
    }

    fn table() -> TableIdentity {
        TableIdentity::new(
            CatalogName::new("demo").expect("catalog"),
            SchemaName::new("generated").expect("schema"),
            TableName::new("events").expect("table"),
        )
    }

    async fn lease(store: &Arc<MemoryRoutingStore>, holder: CollectorInstanceId) -> arrowcol_routing::RoutingLease {
        let observed = now();
        store
            .register_collector(&CollectorRegistration {
                instance_id: holder,
                instance_key: "test".to_owned(),
                implementation_pool: ImplementationPool::Dataflow,
                flight_endpoint: "127.0.0.1:32010".parse().expect("address"),
                catalog_capable: true,
                max_concurrent_streams: PositiveMeasurement::new(4).expect("positive"),
                started_at_unix_ms: observed,
                heartbeat_at_unix_ms: observed,
                lease_expires_at_unix_ms: UnixMillis::new(observed.get() + 60_000).expect("expiry"),
            })
            .await
            .expect("register");
        let zero =
            PressureGauge::new(Measurement::ZERO, PositiveMeasurement::new(4).expect("positive")).expect("gauge");
        store
            .publish_pressure(&RichPressureSnapshot {
                instance_id: holder,
                observed_at_unix_ms: observed,
                cpu_utilization_bps: BasisPoints::new(0).expect("bps"),
                rss_bytes: Measurement::ZERO,
                rss_limit_bytes: PositiveMeasurement::new(1).expect("positive"),
                active_streams: zero,
                received_bytes: Measurement::ZERO,
                received_rows: Measurement::ZERO,
                received_batches: Measurement::ZERO,
                buffered_bytes: zero,
                buffered_rows: zero,
                buffered_batches: zero,
                available_permits: PressureGauge::new(
                    Measurement::new(4).expect("measurement"),
                    PositiveMeasurement::new(4).expect("positive"),
                )
                .expect("gauge"),
                pending_commits: zero,
                oldest_buffer_age_ms: Measurement::ZERO,
                delta_pressure_bps: BasisPoints::new(0).expect("bps"),
                quiver_pressure_bps: BasisPoints::new(0).expect("bps"),
                dfe_pressure_bps: BasisPoints::new(0).expect("bps"),
                delta_state: SubsystemState::Ready,
                quiver_state: SubsystemState::Disabled,
                dfe_state: SubsystemState::Ready,
                accepting_deliveries: true,
                tables: Vec::new(),
            })
            .await
            .expect("pressure");
        store
            .claim_lease(&LeaseClaim {
                key: LeaseKey::new(ImplementationPool::Dataflow, table()),
                candidate_instance_id: holder,
                now_unix_ms: observed,
                expires_at_unix_ms: UnixMillis::new(observed.get() + 60_000).expect("expiry"),
                minimum_snapshot_at_unix_ms: observed,
            })
            .await
            .expect("lease")
    }

    fn pdata(lease: &arrowcol_routing::RoutingLease, fencing_token: FencingToken) -> OtapPdata {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("value", DataType::Utf8, true),
        ]));
        let batch = arrow_array::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("a"), None])) as ArrayRef,
            ],
        )
        .expect("batch");
        let records = encode_record_batch(
            ArrowcolBatchEnvelope {
                table: table(),
                source_schema_version: SchemaVersion::FIRST,
                target_schema_version: SchemaVersion::FIRST,
                lease_id: lease.lease_id,
                holder_instance_id: lease.holder_instance_id,
                fencing_token,
            },
            batch,
        )
        .expect("records");
        OtapPdata::new_todo_context(otel_arrow_dfe_pdata::OtapPayload::from_pdata_arrow(records))
    }

    /// Scenario: A generated non-OTAP Arrow schema reaches the generic Delta exporter.
    /// Guarantees: The original fields and values are written without OTAP normalization.
    #[tokio::test]
    async fn writes_generic_delta_output() {
        let directory = tempfile::tempdir().expect("directory");
        let store = Arc::new(MemoryRoutingStore::default());
        let holder = CollectorInstanceId::new();
        let lease = lease(&store, holder).await;
        let metrics = Arc::new(RuntimeMetrics::new(holder));
        let exporter = GenericDeltaExporter::with_store(
            directory.path().to_path_buf(),
            store as Arc<dyn RoutingStore>,
            arrow_deltalake::commit::CommitOptions {
                max_files_per_commit: 1,
                max_pending_files: 2,
                ..Default::default()
            },
            Arc::clone(&metrics),
        )
        .expect("exporter");

        exporter
            .export(pdata(&lease, lease.fencing_epoch))
            .await
            .expect("export");

        let data_file = std::fs::read_dir(directory.path().join("demo/generated/events"))
            .expect("table")
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .find(|path| path.extension().is_some_and(|extension| extension == "parquet"))
            .expect("parquet");
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(data_file).expect("file"))
            .expect("reader")
            .build()
            .expect("build");
        let batch = reader.next().expect("batch").expect("read");
        assert_eq!(batch.schema().field(0).name(), "id");
        assert_eq!(batch.schema().field(1).name(), "value");
        assert_eq!(batch.num_rows(), 2);
        let pressure = serde_json::to_value(metrics.pressure().expect("pressure")).expect("pressure JSON");
        assert_eq!(pressure["table_versions"][0]["source_schema_version"], 1);
        assert_eq!(pressure["table_versions"][0]["target_schema_version"], 1);
        assert_eq!(pressure["table_versions"][0]["last_delta_version"], 1);
    }

    /// Scenario: Pdata carries a fencing epoch not present in the authoritative routing store.
    /// Guarantees: Delta publication is rejected before a new log version becomes visible.
    #[tokio::test]
    async fn stale_fence_blocks_publication() {
        let directory = tempfile::tempdir().expect("directory");
        let store = Arc::new(MemoryRoutingStore::default());
        let holder = CollectorInstanceId::new();
        let lease = lease(&store, holder).await;
        let metrics = Arc::new(RuntimeMetrics::new(holder));
        let exporter = GenericDeltaExporter::with_store(
            directory.path().to_path_buf(),
            store as Arc<dyn RoutingStore>,
            arrow_deltalake::commit::CommitOptions {
                max_files_per_commit: 1,
                max_pending_files: 2,
                ..Default::default()
            },
            metrics,
        )
        .expect("exporter");

        let error = exporter
            .export(pdata(&lease, lease.fencing_epoch.next().expect("next fence")))
            .await
            .expect_err("fenced");
        assert!(error.to_string().contains("routing lease"));
        assert!(
            !directory
                .path()
                .join("demo/generated/events/_delta_log/00000000000000000001.json")
                .exists()
        );
    }

    /// Scenario: Shutdown begins with one file below the normal Delta batch threshold.
    /// Guarantees: Draining bypasses the collection window and publishes the bounded pending file.
    #[tokio::test]
    async fn graceful_drain_flushes_partial_publication() {
        let directory = tempfile::tempdir().expect("directory");
        let store = Arc::new(MemoryRoutingStore::default());
        let holder = CollectorInstanceId::new();
        let lease = lease(&store, holder).await;
        let exporter = GenericDeltaExporter::with_store(
            directory.path().to_path_buf(),
            store as Arc<dyn RoutingStore>,
            arrow_deltalake::commit::CommitOptions {
                batch_window: std::time::Duration::from_secs(60),
                max_files_per_commit: 2,
                max_pending_files: 2,
            },
            Arc::new(RuntimeMetrics::new(holder)),
        )
        .expect("exporter");
        let publication = exporter.stage(pdata(&lease, lease.fencing_epoch)).await.expect("stage");
        exporter.backend.start_draining().await;
        tokio::time::timeout(std::time::Duration::from_secs(3), publication.wait())
            .await
            .expect("bounded drain")
            .expect("publication");
        assert!(
            directory
                .path()
                .join("demo/generated/events/_delta_log/00000000000000000001.json")
                .exists()
        );
    }

    /// Scenario: Repository SQL Server, Hive Metastore, routing leases, and local Delta are live.
    /// Guarantees: Strict catalog validation and authoritative fencing precede generic Delta publication.
    #[tokio::test]
    #[ignore = "requires projects/arrowcol-infra"]
    async fn live_sql_hive_delta() {
        if std::env::var("ARROWCOL_RUN_LIVE_TESTS").as_deref() != Ok("1") {
            return;
        }
        use crate::{CatalogConfig, HiveMetastoreConnectionConfig, RoutingStoreConfig, SqlServerConnectionConfig};
        use arrow_array::Int64Array;
        use arrowcol_catalog::{FlightSqlDdlHandler, SchemaValidator};
        use arrowcol_protocol::{ClaimedSchemaFingerprint, fingerprint_schema};

        let repository_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let password_file = std::env::var("ARROWCOL_SQL_PASSWORD_FILE")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| repository_root.join("projects/arrowcol-infra/secrets/arrowcol-app-password.txt"));
        let sql = SqlServerConnectionConfig {
            host: std::env::var("ARROWCOL_SQL_HOST").unwrap_or_else(|_| "127.0.0.1".to_owned()),
            port: std::env::var("ARROWCOL_SQL_PORT")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(14_333),
            database: "arrowcol".to_owned(),
            username: "arrowcol_app".to_owned(),
            password_file,
            trust_server_certificate: true,
            connect_timeout: std::time::Duration::from_secs(15),
            command_timeout: std::time::Duration::from_secs(30),
        };
        let catalog_config = CatalogConfig::SqlHive {
            application_database: Box::new(sql.clone()),
            hive_metastore: Box::new(HiveMetastoreConnectionConfig {
                host: std::env::var("ARROWCOL_HIVE_HOST").unwrap_or_else(|_| "127.0.0.1".to_owned()),
                port: std::env::var("ARROWCOL_HIVE_PORT")
                    .ok()
                    .and_then(|value| value.parse().ok())
                    .unwrap_or(9_083),
                warehouse_root: "file:///opt/hive/data/warehouse".to_owned(),
                owner: "arrowcol".to_owned(),
                connect_timeout: std::time::Duration::from_secs(10),
            }),
            timestamp_timezone: "UTC".to_owned(),
        };
        let (catalog_store, control) = catalog_config.start().await.expect("start live catalog");
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let table_name = format!("dfe_events_{}", &suffix[..12]);
        let completion = control
            .execute_ddl(arrowcol_catalog::FlightSqlDdlRequest::new(format!(
                "CREATE TABLE arrowcol.live.{table_name} (id BIGINT NOT NULL, label STRING)"
            )))
            .await
            .expect("live DDL");
        let schema = catalog_store
            .current_schema(completion.identity())
            .await
            .expect("current schema")
            .expect("registered schema");
        let arrow_schema = Arc::new(schema.arrow_schema().expect("Arrow schema"));
        SchemaValidator::new(Arc::clone(&catalog_store))
            .validate(
                completion.identity(),
                ClaimedSchemaFingerprint::new(fingerprint_schema(arrow_schema.as_ref())),
                arrow_schema.as_ref(),
            )
            .await
            .expect("exact live validation");

        let routing_config = RoutingStoreConfig::SqlServer { connection: sql };
        let routing = routing_config.build().expect("routing store");
        let holder = CollectorInstanceId::new();
        let observed = now();
        routing
            .register_collector(&CollectorRegistration {
                instance_id: holder,
                instance_key: "dfe-live-e2e".to_owned(),
                implementation_pool: ImplementationPool::Dataflow,
                flight_endpoint: "127.0.0.1:32010".parse().expect("address"),
                catalog_capable: true,
                max_concurrent_streams: PositiveMeasurement::new(4).expect("positive"),
                started_at_unix_ms: observed,
                heartbeat_at_unix_ms: observed,
                lease_expires_at_unix_ms: UnixMillis::new(observed.get() + 60_000).expect("expiry"),
            })
            .await
            .expect("live registration");
        let zero =
            PressureGauge::new(Measurement::ZERO, PositiveMeasurement::new(4).expect("positive")).expect("gauge");
        routing
            .publish_pressure(&RichPressureSnapshot {
                instance_id: holder,
                observed_at_unix_ms: observed,
                cpu_utilization_bps: BasisPoints::new(0).expect("bps"),
                rss_bytes: Measurement::ZERO,
                rss_limit_bytes: PositiveMeasurement::new(1).expect("positive"),
                active_streams: zero,
                received_bytes: Measurement::ZERO,
                received_rows: Measurement::ZERO,
                received_batches: Measurement::ZERO,
                buffered_bytes: zero,
                buffered_rows: zero,
                buffered_batches: zero,
                available_permits: PressureGauge::new(
                    Measurement::new(4).expect("measurement"),
                    PositiveMeasurement::new(4).expect("positive"),
                )
                .expect("gauge"),
                pending_commits: zero,
                oldest_buffer_age_ms: Measurement::ZERO,
                delta_pressure_bps: BasisPoints::new(0).expect("bps"),
                quiver_pressure_bps: BasisPoints::new(0).expect("bps"),
                dfe_pressure_bps: BasisPoints::new(0).expect("bps"),
                delta_state: SubsystemState::Ready,
                quiver_state: SubsystemState::Disabled,
                dfe_state: SubsystemState::Ready,
                accepting_deliveries: true,
                tables: Vec::new(),
            })
            .await
            .expect("live pressure");
        let lease = routing
            .claim_lease(&LeaseClaim {
                key: LeaseKey::new(ImplementationPool::Dataflow, completion.identity().clone()),
                candidate_instance_id: holder,
                now_unix_ms: observed,
                expires_at_unix_ms: UnixMillis::new(observed.get() + 60_000).expect("expiry"),
                minimum_snapshot_at_unix_ms: observed,
            })
            .await
            .expect("live lease");

        let output = repository_root.join("projects/arrowcol-dataflow-rust/.temp/live-sql-hive-delta");
        let _ = std::fs::remove_dir_all(&output);
        std::fs::create_dir_all(&output).expect("create output");
        let delta_log = output
            .join("arrowcol/live")
            .join(&table_name)
            .join("_delta_log/00000000000000000001.json");
        struct Cleanup(std::path::PathBuf);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let _cleanup = Cleanup(output.clone());
        let exporter = GenericDeltaExporter::with_store(
            output,
            Arc::clone(&routing),
            arrow_deltalake::commit::CommitOptions {
                max_files_per_commit: 1,
                max_pending_files: 2,
                ..Default::default()
            },
            Arc::new(RuntimeMetrics::new(holder)),
        )
        .expect("live exporter");
        let batch = arrow_array::RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(Int64Array::from(vec![1_i64])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("live")])) as ArrayRef,
            ],
        )
        .expect("live batch");
        let records = encode_record_batch(
            ArrowcolBatchEnvelope {
                table: completion.identity().clone(),
                source_schema_version: schema.version(),
                target_schema_version: schema.version(),
                lease_id: lease.lease_id,
                holder_instance_id: lease.holder_instance_id,
                fencing_token: lease.fencing_epoch,
            },
            batch,
        )
        .expect("live pdata");
        exporter
            .export(OtapPdata::new_todo_context(
                otel_arrow_dfe_pdata::OtapPayload::from_pdata_arrow(records),
            ))
            .await
            .expect("live Delta export");
        assert!(delta_log.exists());
    }
}
