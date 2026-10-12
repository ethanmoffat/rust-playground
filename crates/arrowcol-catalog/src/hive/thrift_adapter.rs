use std::{
    collections::BTreeMap,
    net::{SocketAddr, TcpStream, ToSocketAddrs},
    time::Duration,
};

use arrowcol_protocol::{SchemaFingerprint, TableIdentity};
use sha2::{Digest, Sha256};
use thrift::{
    protocol::{TBinaryInputProtocol, TBinaryOutputProtocol},
    transport::{TBufferedReadTransport, TBufferedWriteTransport},
};

use crate::{
    CatalogError, CatalogErrorCode, CatalogResult, CatalogSchemaVersion, HiveTableSnapshot, Metastore, SchemaVersion,
};

#[path = "generated/hive_metastore_minimal.rs"]
mod generated;

use generated::{
    AlreadyExistsException, AlterTableRequest, Catalog, CreateCatalogRequest, CreateDatabaseRequest,
    CreateTableRequest, FieldSchema, GetCatalogRequest, GetDatabaseRequest, GetTableRequest, InvalidObjectException,
    MetaException, NoSuchObjectException, SerDeInfo, StorageDescriptor, TThriftHiveMetastoreSyncClient, Table,
    ThriftHiveMetastoreSyncClient,
};

const FINGERPRINT_PARAMETER: &str = "arrowcol.schema_fingerprint";
const VERSION_PARAMETER: &str = "arrowcol.schema_version";
const CANONICAL_DDL_PARAMETER: &str = "arrowcol.canonical_ddl_sha256";
const PROTOCOL_PARAMETER: &str = "arrowcol.catalog_contract";
const PROTOCOL_VALUE: &str = "1";
const PARQUET_INPUT_FORMAT: &str = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
const PARQUET_OUTPUT_FORMAT: &str = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat";
const PARQUET_SERDE: &str = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";

type SyncClient = ThriftHiveMetastoreSyncClient<
    TBinaryInputProtocol<TBufferedReadTransport<TcpStream>>,
    TBinaryOutputProtocol<TBufferedWriteTransport<TcpStream>>,
>;

/// Production Hive Metastore connection configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HiveMetastoreConfig {
    host: String,
    port: u16,
    warehouse_root: String,
    owner: String,
    connect_timeout: Duration,
}

impl HiveMetastoreConfig {
    /// Creates configuration for an unauthenticated Hive Metastore Thrift
    /// endpoint.
    #[must_use]
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
            warehouse_root: "file:///opt/hive/data/warehouse".to_owned(),
            owner: "arrowcol".to_owned(),
            connect_timeout: Duration::from_secs(10),
        }
    }

    /// Sets the warehouse URI used for new external tables.
    #[must_use]
    pub fn with_warehouse_root(mut self, warehouse_root: impl Into<String>) -> Self {
        self.warehouse_root = warehouse_root.into().trim_end_matches('/').to_owned();
        self
    }

    /// Sets the Hive table owner.
    #[must_use]
    pub fn with_owner(mut self, owner: impl Into<String>) -> Self {
        self.owner = owner.into();
        self
    }

    /// Sets the TCP connect/read/write timeout.
    #[must_use]
    pub const fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }
}

/// Narrow production adapter generated from the official Hive 4.0.1 Thrift
/// field IDs and RPC names.
#[derive(Clone, Debug)]
pub struct HiveMetastore {
    config: HiveMetastoreConfig,
}

impl HiveMetastore {
    /// Creates the adapter.
    #[must_use]
    pub const fn new(config: HiveMetastoreConfig) -> Self {
        Self { config }
    }

    async fn blocking<T, F>(&self, operation: F) -> CatalogResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&HiveMetastoreConfig, &mut SyncClient) -> CatalogResult<T> + Send + 'static,
    {
        let config = self.config.clone();
        tokio::task::spawn_blocking(move || {
            let mut client = connect(&config)?;
            operation(&config, &mut client)
        })
        .await
        .map_err(|error| {
            CatalogError::new(
                CatalogErrorCode::HiveMetastore,
                format!("Hive Metastore blocking task failed: {error}"),
            )
        })?
    }
}

#[async_trait::async_trait]
impl Metastore for HiveMetastore {
    async fn ensure_schema(
        &self,
        from_schema: Option<&CatalogSchemaVersion>,
        target_schema: &CatalogSchemaVersion,
    ) -> CatalogResult<HiveTableSnapshot> {
        let from_schema = from_schema.cloned();
        let target_schema = target_schema.clone();
        self.blocking(move |config, client| {
            ensure_namespace(config, client, target_schema.identity())?;
            match get_table(client, target_schema.identity())? {
                Some(table) => {
                    let snapshot = snapshot_from_table(target_schema.identity(), &table)?;
                    if snapshot_matches(&snapshot, &target_schema) {
                        verify_hive_columns(&snapshot, &target_schema)?;
                        return Ok(snapshot);
                    }
                    let Some(from_schema) = from_schema.as_ref() else {
                        return Err(CatalogError::new(
                            CatalogErrorCode::HiveMetastore,
                            format!(
                                "Hive table {} already exists with fingerprint {}",
                                target_schema.identity(),
                                snapshot.fingerprint()
                            ),
                        ));
                    };
                    if !snapshot_matches(&snapshot, from_schema) {
                        return Err(CatalogError::new(
                            CatalogErrorCode::HiveMetastore,
                            format!(
                                "Hive table {} drifted from application version {}",
                                target_schema.identity(),
                                from_schema.version()
                            ),
                        ));
                    }
                    let request = AlterTableRequest::new(
                        Some(target_schema.identity().catalog().to_string()),
                        target_schema.identity().schema().to_string(),
                        target_schema.identity().table().to_string(),
                        hive_table(config, &target_schema),
                        None,
                        Some(processor_capabilities()),
                        Some("arrowcol-catalog".to_owned()),
                        Some(FINGERPRINT_PARAMETER.to_owned()),
                        Some(from_schema.fingerprint().to_string()),
                    );
                    if let Err(error) = client.alter_table_req(request) {
                        let after = get_table(client, target_schema.identity())?;
                        if let Some(after) = after {
                            let snapshot = snapshot_from_table(target_schema.identity(), &after)?;
                            if snapshot_matches(&snapshot, &target_schema) {
                                verify_hive_columns(&snapshot, &target_schema)?;
                                return Ok(snapshot);
                            }
                        }
                        return Err(hive_error("alter_table_req", error));
                    }
                }
                None if from_schema.is_none() => {
                    let request = CreateTableRequest::new(
                        hive_table(config, &target_schema),
                        None,
                        Some(processor_capabilities()),
                        Some("arrowcol-catalog".to_owned()),
                    );
                    if let Err(error) = client.create_table_req(request)
                        && !is_user_error::<AlreadyExistsException>(&error)
                    {
                        return Err(hive_error("create_table_req", error));
                    }
                }
                None => {
                    return Err(CatalogError::new(
                        CatalogErrorCode::HiveMetastore,
                        format!("Hive table {} is missing during ALTER", target_schema.identity()),
                    ));
                }
            }

            let table = get_table(client, target_schema.identity())?.ok_or_else(|| {
                CatalogError::new(
                    CatalogErrorCode::HiveMetastore,
                    format!("Hive table {} was not visible after apply", target_schema.identity()),
                )
            })?;
            let snapshot = snapshot_from_table(target_schema.identity(), &table)?;
            if !snapshot_matches(&snapshot, &target_schema) {
                return Err(CatalogError::new(
                    CatalogErrorCode::HiveMetastore,
                    format!(
                        "Hive table {} did not reach target fingerprint {}",
                        target_schema.identity(),
                        target_schema.fingerprint()
                    ),
                ));
            }
            verify_hive_columns(&snapshot, &target_schema)?;
            Ok(snapshot)
        })
        .await
    }

    async fn table(&self, identity: &TableIdentity) -> CatalogResult<Option<HiveTableSnapshot>> {
        let identity = identity.clone();
        self.blocking(move |_config, client| {
            get_table(client, &identity)?
                .map(|table| snapshot_from_table(&identity, &table))
                .transpose()
        })
        .await
    }
}

fn connect(config: &HiveMetastoreConfig) -> CatalogResult<SyncClient> {
    let address = resolve_address(config)?;
    let stream = TcpStream::connect_timeout(&address, config.connect_timeout).map_err(|error| {
        CatalogError::new(
            CatalogErrorCode::HiveMetastore,
            format!("could not connect to Hive Metastore at {address}: {error}"),
        )
    })?;
    stream
        .set_read_timeout(Some(config.connect_timeout))
        .and_then(|()| stream.set_write_timeout(Some(config.connect_timeout)))
        .and_then(|()| stream.set_nodelay(true))
        .map_err(|error| {
            CatalogError::new(
                CatalogErrorCode::HiveMetastore,
                format!("could not configure Hive Metastore socket: {error}"),
            )
        })?;
    let read_stream = stream.try_clone().map_err(|error| {
        CatalogError::new(
            CatalogErrorCode::HiveMetastore,
            format!("could not clone Hive Metastore socket: {error}"),
        )
    })?;
    Ok(ThriftHiveMetastoreSyncClient::new(
        TBinaryInputProtocol::new(TBufferedReadTransport::new(read_stream), true),
        TBinaryOutputProtocol::new(TBufferedWriteTransport::new(stream), true),
    ))
}

fn resolve_address(config: &HiveMetastoreConfig) -> CatalogResult<SocketAddr> {
    (config.host.as_str(), config.port)
        .to_socket_addrs()
        .map_err(|error| {
            CatalogError::new(
                CatalogErrorCode::HiveMetastore,
                format!("could not resolve Hive Metastore host {}: {error}", config.host),
            )
        })?
        .next()
        .ok_or_else(|| {
            CatalogError::new(
                CatalogErrorCode::HiveMetastore,
                format!("Hive Metastore host {} resolved to no addresses", config.host),
            )
        })
}

fn ensure_namespace(
    config: &HiveMetastoreConfig,
    client: &mut SyncClient,
    identity: &TableIdentity,
) -> CatalogResult<()> {
    let catalog_name = identity.catalog().to_string();
    match client.get_catalog(GetCatalogRequest::new(catalog_name.clone())) {
        Ok(_) => {}
        Err(error) if is_user_error::<NoSuchObjectException>(&error) => {
            let request = CreateCatalogRequest::new(Catalog::new(
                Some(catalog_name.clone()),
                Some("Arrowcol managed catalog".to_owned()),
                Some(catalog_location(config, identity)),
                None,
            ));
            if let Err(error) = client.create_catalog(request)
                && !is_user_error::<AlreadyExistsException>(&error)
            {
                return Err(hive_error("create_catalog", error));
            }
        }
        Err(error) => return Err(hive_error("get_catalog", error)),
    }

    let database_request = GetDatabaseRequest::new(Some(identity.schema().to_string()), Some(catalog_name.clone()));
    match client.get_database_req(database_request) {
        Ok(_) => {}
        Err(error) if is_user_error::<NoSuchObjectException>(&error) => {
            let request = CreateDatabaseRequest::new(
                identity.schema().to_string(),
                Some("Arrowcol managed schema".to_owned()),
                Some(database_location(config, identity)),
                Some(BTreeMap::from([(
                    PROTOCOL_PARAMETER.to_owned(),
                    PROTOCOL_VALUE.to_owned(),
                )])),
                Some(catalog_name),
                None,
                Some(database_location(config, identity)),
            );
            if let Err(error) = client.create_database_req(request)
                && !is_user_error::<AlreadyExistsException>(&error)
            {
                return Err(hive_error("create_database_req", error));
            }
        }
        Err(error) => return Err(hive_error("get_database_req", error)),
    }
    Ok(())
}

fn get_table(client: &mut SyncClient, identity: &TableIdentity) -> CatalogResult<Option<Table>> {
    let request = GetTableRequest::new(
        identity.schema().to_string(),
        identity.table().to_string(),
        Some(identity.catalog().to_string()),
        Some(processor_capabilities()),
        Some("arrowcol-catalog".to_owned()),
        Some("arrowcol".to_owned()),
    );
    match client.get_table_req(request) {
        Ok(result) => Ok(Some(result.table)),
        Err(error) if is_user_error::<NoSuchObjectException>(&error) => Ok(None),
        Err(error) => Err(hive_error("get_table_req", error)),
    }
}

fn hive_table(config: &HiveMetastoreConfig, schema: &CatalogSchemaVersion) -> Table {
    let columns = schema
        .fields()
        .iter()
        .filter(|field| field.partition().is_none())
        .map(|field| {
            FieldSchema::new(
                Some(field.name().to_owned()),
                Some(field.mapping().hive().to_owned()),
                Some(format!(
                    "arrowcol_field_id={} arrow_type={}",
                    field.id(),
                    field.mapping().datafusion()
                )),
            )
        })
        .collect();
    let mut partition_keys = schema
        .fields()
        .iter()
        .filter_map(|field| {
            field.partition().map(|partition| {
                (
                    partition.ordinal(),
                    FieldSchema::new(
                        Some(field.name().to_owned()),
                        Some(field.mapping().hive().to_owned()),
                        Some(format!("arrowcol_field_id={}", field.id())),
                    ),
                )
            })
        })
        .collect::<Vec<_>>();
    partition_keys.sort_by_key(|(ordinal, _)| *ordinal);
    let parameters = BTreeMap::from([
        (PROTOCOL_PARAMETER.to_owned(), PROTOCOL_VALUE.to_owned()),
        (VERSION_PARAMETER.to_owned(), schema.version().to_string()),
        (FINGERPRINT_PARAMETER.to_owned(), schema.fingerprint().to_string()),
        (
            CANONICAL_DDL_PARAMETER.to_owned(),
            hex::encode(Sha256::digest(schema.canonical_ddl().as_bytes())),
        ),
        ("EXTERNAL".to_owned(), "TRUE".to_owned()),
        ("external.table.purge".to_owned(), "false".to_owned()),
    ]);
    let serde = SerDeInfo::new(
        Some(format!("{}_serde", schema.identity().table())),
        Some(PARQUET_SERDE.to_owned()),
        Some(BTreeMap::new()),
    );
    let storage = StorageDescriptor::new(
        Some(columns),
        Some(table_location(config, schema.identity())),
        Some(PARQUET_INPUT_FORMAT.to_owned()),
        Some(PARQUET_OUTPUT_FORMAT.to_owned()),
        Some(false),
        Some(0),
        Some(serde),
        Some(Vec::new()),
        Some(BTreeMap::new()),
    );
    Table::new(
        Some(schema.identity().table().to_string()),
        Some(schema.identity().schema().to_string()),
        Some(config.owner.clone()),
        Some(0),
        Some(0),
        Some(0),
        Some(storage),
        Some(partition_keys.into_iter().map(|(_, field)| field).collect()),
        Some(parameters),
        Some(String::new()),
        Some(String::new()),
        Some("EXTERNAL_TABLE".to_owned()),
        Some(false),
        Some(false),
        Some(schema.identity().catalog().to_string()),
    )
}

fn snapshot_from_table(identity: &TableIdentity, table: &Table) -> CatalogResult<HiveTableSnapshot> {
    let parameters = table.parameters.as_ref().ok_or_else(|| {
        CatalogError::new(
            CatalogErrorCode::HiveMetastore,
            format!("Hive table {identity} has no Arrowcol parameters"),
        )
    })?;
    let fingerprint = parameters
        .get(FINGERPRINT_PARAMETER)
        .ok_or_else(|| {
            CatalogError::new(
                CatalogErrorCode::HiveMetastore,
                format!("Hive table {identity} has no Arrowcol fingerprint"),
            )
        })?
        .parse::<SchemaFingerprint>()
        .map_err(|error| {
            CatalogError::new(
                CatalogErrorCode::HiveMetastore,
                format!("Hive table {identity} has an invalid fingerprint: {error}"),
            )
        })?;
    let schema_version = parameters
        .get(VERSION_PARAMETER)
        .ok_or_else(|| {
            CatalogError::new(
                CatalogErrorCode::HiveMetastore,
                format!("Hive table {identity} has no Arrowcol schema version"),
            )
        })?
        .parse::<u64>()
        .map_err(|error| {
            CatalogError::new(
                CatalogErrorCode::HiveMetastore,
                format!("Hive table {identity} has an invalid schema version: {error}"),
            )
        })
        .and_then(SchemaVersion::new)?;
    let fields = table
        .sd
        .as_ref()
        .and_then(|storage| storage.cols.as_ref())
        .ok_or_else(|| {
            CatalogError::new(
                CatalogErrorCode::HiveMetastore,
                format!("Hive table {identity} has no storage columns"),
            )
        })?
        .iter()
        .map(required_hive_field)
        .collect::<CatalogResult<Vec<_>>>()?;
    let partitions = table
        .partition_keys
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(required_hive_field)
        .collect::<CatalogResult<Vec<_>>>()?;
    Ok(HiveTableSnapshot {
        identity: identity.clone(),
        schema_version,
        fingerprint,
        fields,
        partitions,
    })
}

fn required_hive_field(field: &FieldSchema) -> CatalogResult<(String, String)> {
    Ok((
        field
            .name
            .clone()
            .ok_or_else(|| CatalogError::new(CatalogErrorCode::HiveMetastore, "Hive field is missing its name"))?,
        field
            .type_
            .clone()
            .ok_or_else(|| CatalogError::new(CatalogErrorCode::HiveMetastore, "Hive field is missing its type"))?,
    ))
}

fn snapshot_matches(snapshot: &HiveTableSnapshot, schema: &CatalogSchemaVersion) -> bool {
    snapshot.schema_version() == schema.version() && snapshot.fingerprint() == schema.fingerprint()
}

fn verify_hive_columns(snapshot: &HiveTableSnapshot, schema: &CatalogSchemaVersion) -> CatalogResult<()> {
    let expected = HiveTableSnapshot::from_schema(schema);
    if snapshot.fields() != expected.fields() || snapshot.partitions() != expected.partitions() {
        return Err(CatalogError::new(
            CatalogErrorCode::HiveMetastore,
            format!(
                "Hive table {} version/fingerprint parameters match but columns differ",
                schema.identity()
            ),
        ));
    }
    Ok(())
}

fn processor_capabilities() -> Vec<String> {
    vec!["EXTREAD".to_owned(), "EXTWRITE".to_owned()]
}

fn catalog_location(config: &HiveMetastoreConfig, identity: &TableIdentity) -> String {
    format!("{}/{}.catalog", config.warehouse_root, identity.catalog())
}

fn database_location(config: &HiveMetastoreConfig, identity: &TableIdentity) -> String {
    format!(
        "{}/{}.catalog/{}.db",
        config.warehouse_root,
        identity.catalog(),
        identity.schema()
    )
}

fn table_location(config: &HiveMetastoreConfig, identity: &TableIdentity) -> String {
    format!(
        "{}/{}.catalog/{}.db/{}",
        config.warehouse_root,
        identity.catalog(),
        identity.schema(),
        identity.table()
    )
}

fn is_user_error<T: std::error::Error + 'static>(error: &thrift::Error) -> bool {
    matches!(error, thrift::Error::User(source) if source.downcast_ref::<T>().is_some())
}

fn hive_error(operation: &str, error: thrift::Error) -> CatalogError {
    let detail = match &error {
        thrift::Error::User(source) => {
            if let Some(error) = source.downcast_ref::<AlreadyExistsException>() {
                error.message.as_deref().unwrap_or("already exists").to_owned()
            } else if let Some(error) = source.downcast_ref::<NoSuchObjectException>() {
                error.message.as_deref().unwrap_or("object does not exist").to_owned()
            } else if let Some(error) = source.downcast_ref::<InvalidObjectException>() {
                error.message.as_deref().unwrap_or("invalid object").to_owned()
            } else if let Some(error) = source.downcast_ref::<MetaException>() {
                error.message.as_deref().unwrap_or("metastore error").to_owned()
            } else {
                source.to_string()
            }
        }
        _ => error.to_string(),
    };
    CatalogError::new(
        CatalogErrorCode::HiveMetastore,
        format!("Hive Metastore {operation} failed: {detail}"),
    )
}
