use std::{collections::BTreeMap, fmt::Display, path::Path, time::Duration};

use arrowcol_protocol::{SchemaFingerprint, TableIdentity};
use sha2::Digest;
use tiberius::{AuthMethod, Client, Config, EncryptionLevel, IsolationLevel, Row};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use uuid::Uuid;

use crate::store::PersistedSagaRecord;
use crate::{
    ArtifactKind, CatalogError, CatalogErrorCode, CatalogField, CatalogResult, CatalogSchemaVersion, CatalogStore,
    CatalogType, ChangeReservation, DescriptorArtifacts, FieldPath, RequestHash, ReserveOutcome, SagaId, SagaRecord,
    SagaState, SagaStep, SchemaVersion,
};

const ARTIFACT_CHUNK_BYTES: usize = 8_000;

type SqlClient = Client<Compat<TcpStream>>;

/// SQL Server application-database connection configuration.
#[derive(Clone)]
pub struct SqlServerConfig {
    host: String,
    port: u16,
    database: String,
    username: String,
    password: String,
    trust_server_certificate: bool,
    connect_timeout: Duration,
    command_timeout: Duration,
}

fn validate_schema_json(schema: &CatalogSchemaVersion) -> CatalogResult<()> {
    if schema.table_id() != crate::deterministic_table_id(schema.identity())
        || arrowcol_protocol::fingerprint_schema(&schema.arrow_schema()?) != schema.fingerprint()
    {
        return Err(CatalogError::new(
            CatalogErrorCode::CorruptCatalogState,
            "persisted schema JSON has an invalid table id or fingerprint",
        ));
    }
    Ok(())
}

impl SqlServerConfig {
    /// Creates SQL authentication configuration.
    #[must_use]
    pub fn new(
        host: impl Into<String>,
        port: u16,
        database: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self {
            host: host.into(),
            port,
            database: database.into(),
            username: username.into(),
            password: password.into(),
            trust_server_certificate: false,
            connect_timeout: Duration::from_secs(15),
            command_timeout: Duration::from_secs(30),
        }
    }

    /// Reads the password from a single-line secret file.
    ///
    /// # Errors
    ///
    /// Returns an application-database error for an unreadable, empty, or
    /// multiline secret.
    pub fn with_password_file(
        host: impl Into<String>,
        port: u16,
        database: impl Into<String>,
        username: impl Into<String>,
        path: impl AsRef<Path>,
    ) -> CatalogResult<Self> {
        let password = std::fs::read_to_string(path.as_ref()).map_err(|error| {
            CatalogError::new(
                CatalogErrorCode::ApplicationDatabase,
                format!("could not read SQL Server password file: {error}"),
            )
        })?;
        let password = password.trim_end_matches(['\r', '\n']);
        if password.is_empty() || password.contains(['\r', '\n']) {
            return Err(CatalogError::new(
                CatalogErrorCode::ApplicationDatabase,
                "SQL Server password file must contain one non-empty line",
            ));
        }
        Ok(Self::new(host, port, database, username, password.to_owned()))
    }

    /// Enables certificate trust bypass for explicitly local development
    /// infrastructure.
    #[must_use]
    pub const fn trust_server_certificate(mut self, trust: bool) -> Self {
        self.trust_server_certificate = trust;
        self
    }

    /// Sets TCP/login and command timeouts.
    #[must_use]
    pub const fn with_timeouts(mut self, connect: Duration, command: Duration) -> Self {
        self.connect_timeout = connect;
        self.command_timeout = command;
        self
    }
}

/// Production SQL Server-backed immutable catalog store.
#[derive(Clone)]
pub struct SqlServerCatalogStore {
    config: SqlServerConfig,
}

impl SqlServerCatalogStore {
    /// Creates a SQL Server catalog store.
    #[must_use]
    pub const fn new(config: SqlServerConfig) -> Self {
        Self { config }
    }

    async fn connect(&self) -> CatalogResult<SqlClient> {
        let mut config = Config::new();
        config.host(&self.config.host);
        config.port(self.config.port);
        config.database(&self.config.database);
        config.authentication(AuthMethod::sql_server(
            self.config.username.clone(),
            self.config.password.clone(),
        ));
        config.encryption(EncryptionLevel::Required);
        config.handshake_timeout(Some(self.config.connect_timeout));
        config.command_timeout(Some(self.config.command_timeout));
        if self.config.trust_server_certificate {
            config.trust_cert();
        }
        let address = config.get_addr();
        let tcp = tokio::time::timeout(self.config.connect_timeout, TcpStream::connect(&address))
            .await
            .map_err(|_| {
                CatalogError::new(
                    CatalogErrorCode::ApplicationDatabase,
                    format!("SQL Server TCP connection to {address} timed out"),
                )
            })?
            .map_err(|error| db_error("connect TCP", error))?;
        tcp.set_nodelay(true)
            .map_err(|error| db_error("configure TCP", error))?;
        Client::connect(config, tcp.compat_write())
            .await
            .map_err(|error| db_error("login", error))
    }
}

#[async_trait::async_trait]
impl CatalogStore for SqlServerCatalogStore {
    async fn operation(
        &self,
        identity: &TableIdentity,
        request_hash: RequestHash,
    ) -> CatalogResult<Option<SagaRecord>> {
        operation_with_client(&mut self.connect().await?, identity, request_hash).await
    }

    async fn current_schema(&self, identity: &TableIdentity) -> CatalogResult<Option<CatalogSchemaVersion>> {
        current_schema_with_client(&mut self.connect().await?, identity, false).await
    }

    async fn schema_by_fingerprint(
        &self,
        identity: &TableIdentity,
        fingerprint: SchemaFingerprint,
    ) -> CatalogResult<Option<CatalogSchemaVersion>> {
        let mut client = self.connect().await?;
        let catalog = identity.catalog().to_string();
        let schema = identity.schema().to_string();
        let table = identity.table().to_string();
        let digest: &[u8] = fingerprint.as_bytes();
        let row = client
            .query(
                "SELECT v.schema_version, v.schema_sha256, v.schema_json \
                 FROM arrowcol.catalog_tables AS t \
                 INNER JOIN arrowcol.table_schema_versions AS v ON v.table_id = t.table_id \
                 WHERE t.catalog_name = @P1 AND t.schema_name = @P2 AND t.table_name = @P3 \
                   AND v.schema_sha256 = @P4 AND v.state IN ('active', 'superseded');",
                &[&catalog, &schema, &table, &digest],
            )
            .await
            .map_err(|error| db_error("query schema by fingerprint", error))?
            .into_row()
            .await
            .map_err(|error| db_error("read schema by fingerprint", error))?;
        row.map(|row| schema_from_row(&row, identity)).transpose()
    }

    async fn schema_history(&self, identity: &TableIdentity) -> CatalogResult<Vec<CatalogSchemaVersion>> {
        let mut client = self.connect().await?;
        let catalog = identity.catalog().to_string();
        let schema = identity.schema().to_string();
        let table = identity.table().to_string();
        client
            .query(
                "SELECT v.schema_version, v.schema_sha256, v.schema_json \
                 FROM arrowcol.catalog_tables AS t \
                 INNER JOIN arrowcol.table_schema_versions AS v ON v.table_id = t.table_id \
                 WHERE t.catalog_name = @P1 AND t.schema_name = @P2 AND t.table_name = @P3 \
                   AND v.state IN ('active', 'superseded') \
                 ORDER BY v.schema_version;",
                &[&catalog, &schema, &table],
            )
            .await
            .map_err(|error| db_error("query schema history", error))?
            .into_first_result()
            .await
            .map_err(|error| db_error("read schema history", error))?
            .iter()
            .map(|row| schema_from_row(row, identity))
            .collect()
    }

    async fn reserve_change(&self, reservation: ChangeReservation) -> CatalogResult<ReserveOutcome> {
        let mut client = self.connect().await?;
        let flat_fields = flatten_schema_fields(&reservation.target_schema)?;
        let schema_json = serde_json::to_string(&reservation.target_schema).map_err(|error| {
            CatalogError::new(
                CatalogErrorCode::ApplicationDatabase,
                format!("serialize pending catalog schema: {error}"),
            )
        })?;
        client
            .begin_transaction_with_isolation(IsolationLevel::Serializable)
            .await
            .map_err(|error| db_error("begin reserve transaction", error))?;
        let result = reserve_in_transaction(&mut client, reservation, &flat_fields, &schema_json).await;
        finish_transaction(&mut client, result, "reserve schema change").await
    }

    async fn saga(&self, saga_id: SagaId) -> CatalogResult<Option<SagaRecord>> {
        saga_with_client(&mut self.connect().await?, saga_id).await
    }

    async fn mark_hive_applied(&self, saga_id: SagaId) -> CatalogResult<SagaRecord> {
        let mut client = self.connect().await?;
        let saga_uuid = saga_id.as_uuid();
        client
            .execute(
                "UPDATE arrowcol.schema_change_sagas \
                 SET state = 'applying', current_step = 1, updated_at_utc = SYSUTCDATETIME(), \
                     last_error_code = NULL, last_error_message = NULL \
                 WHERE saga_id = @P1 AND completed_at_utc IS NULL AND current_step = 0;",
                &[&saga_uuid],
            )
            .await
            .map_err(|error| db_error("mark Hive schema applied", error))?;
        required_saga(&mut client, saga_id).await
    }

    async fn persist_artifacts(&self, saga_id: SagaId, artifacts: &DescriptorArtifacts) -> CatalogResult<SagaRecord> {
        let mut client = self.connect().await?;
        client
            .begin_transaction_with_isolation(IsolationLevel::Serializable)
            .await
            .map_err(|error| db_error("begin artifact transaction", error))?;
        let result = persist_artifacts_in_transaction(&mut client, saga_id, artifacts).await;
        finish_transaction(&mut client, result, "persist schema artifacts").await
    }

    async fn finalize(&self, saga_id: SagaId) -> CatalogResult<SagaRecord> {
        let mut client = self.connect().await?;
        client
            .begin_transaction_with_isolation(IsolationLevel::Serializable)
            .await
            .map_err(|error| db_error("begin finalize transaction", error))?;
        let result = finalize_in_transaction(&mut client, saga_id).await;
        finish_transaction(&mut client, result, "finalize schema change").await
    }

    async fn record_failure(&self, saga_id: SagaId, step: SagaStep, error: &CatalogError) -> CatalogResult<SagaRecord> {
        let mut client = self.connect().await?;
        let saga_uuid = saga_id.as_uuid();
        let step_number = step.number();
        let code = error.code().as_str();
        let message = truncate_chars(error.message(), 2_048);
        client
            .execute(
                "UPDATE arrowcol.schema_change_sagas \
                 SET state = 'failed', current_step = @P2, attempt_count = attempt_count + 1, \
                     updated_at_utc = SYSUTCDATETIME(), last_error_code = @P3, last_error_message = @P4 \
                 WHERE saga_id = @P1 AND completed_at_utc IS NULL AND current_step <= @P2;",
                &[&saga_uuid, &step_number, &code, &message],
            )
            .await
            .map_err(|db| db_error("record schema saga failure", db))?;
        required_saga(&mut client, saga_id).await
    }

    async fn incomplete_sagas(&self) -> CatalogResult<Vec<SagaRecord>> {
        let mut client = self.connect().await?;
        let rows = client
            .query(
                "SELECT saga_id FROM arrowcol.schema_change_sagas \
                 WHERE completed_at_utc IS NULL \
                 ORDER BY started_at_utc, saga_id;",
                &[],
            )
            .await
            .map_err(|error| db_error("query incomplete schema sagas", error))?
            .into_first_result()
            .await
            .map_err(|error| db_error("read incomplete schema sagas", error))?;
        let mut sagas = Vec::with_capacity(rows.len());
        for row in rows {
            let id = required_uuid(&row, "saga_id")?;
            sagas.push(required_saga(&mut client, SagaId::from_uuid(id)).await?);
        }
        Ok(sagas)
    }

    async fn artifacts(
        &self,
        identity: &TableIdentity,
        version: SchemaVersion,
    ) -> CatalogResult<Option<DescriptorArtifacts>> {
        let mut client = self.connect().await?;
        let catalog = identity.catalog().to_string();
        let schema = identity.schema().to_string();
        let table = identity.table().to_string();
        let version = version_to_i64(version)?;
        let rows = client
            .query(
                "SELECT a.artifact_kind, a.chunk_ordinal, a.artifact_sha256, \
                        a.artifact_size_bytes, a.chunk_data \
                 FROM arrowcol.catalog_tables AS t \
                 INNER JOIN arrowcol.schema_artifact_chunks AS a ON a.table_id = t.table_id \
                 WHERE t.catalog_name = @P1 AND t.schema_name = @P2 AND t.table_name = @P3 \
                   AND a.schema_version = @P4 \
                 ORDER BY a.artifact_kind, a.chunk_ordinal;",
                &[&catalog, &schema, &table, &version],
            )
            .await
            .map_err(|error| db_error("query schema artifacts", error))?
            .into_first_result()
            .await
            .map_err(|error| db_error("read schema artifacts", error))?;
        if rows.is_empty() {
            return Ok(None);
        }
        artifact_rows(rows).map(Some)
    }
}

async fn reserve_in_transaction(
    client: &mut SqlClient,
    reservation: ChangeReservation,
    flat_fields: &[FlatField],
    schema_json: &str,
) -> CatalogResult<ReserveOutcome> {
    let identity = reservation.target_schema.identity();
    acquire_lock(
        client,
        &format!("arrowcol.catalog.{}", reservation.target_schema.table_id()),
    )
    .await?;
    if let Some(existing) = operation_with_client(client, identity, reservation.request_hash).await? {
        return Ok(ReserveOutcome::Existing(existing));
    }

    let table_id = ensure_catalog_table(client, &reservation).await?;
    let current = current_schema_with_client(client, identity, true).await?;
    match (&reservation.from_schema, &current) {
        (None, Some(current)) if current.fingerprint() == reservation.target_schema.fingerprint() => {
            return Ok(ReserveOutcome::AlreadyCurrent(current.clone()));
        }
        (None, Some(_)) => {
            return Err(CatalogError::new(
                CatalogErrorCode::TableAlreadyExists,
                format!("catalog table {identity} already exists with a different schema"),
            ));
        }
        (Some(_), None) => {
            return Err(CatalogError::new(
                CatalogErrorCode::TableNotFound,
                format!("catalog table {identity} does not have an active schema"),
            ));
        }
        (Some(expected), Some(current))
            if expected.version() != current.version() || expected.fingerprint() != current.fingerprint() =>
        {
            return Err(CatalogError::new(
                CatalogErrorCode::SchemaVersionConflict,
                format!("schema head for {identity} changed before reservation"),
            ));
        }
        _ => {}
    }
    if let Some(current) = &current
        && current.fingerprint() == reservation.target_schema.fingerprint()
    {
        return Ok(ReserveOutcome::AlreadyCurrent(current.clone()));
    }

    let active = client
        .query(
            "SELECT saga_id FROM arrowcol.schema_change_sagas WITH (UPDLOCK, HOLDLOCK) \
             WHERE table_id = @P1 AND completed_at_utc IS NULL;",
            &[&table_id],
        )
        .await
        .map_err(|error| db_error("query active schema saga", error))?
        .into_row()
        .await
        .map_err(|error| db_error("read active schema saga", error))?;
    if let Some(row) = active {
        let active_id = SagaId::from_uuid(required_uuid(&row, "saga_id")?);
        let active = required_saga(client, active_id).await?;
        if active.request_hash() == reservation.request_hash {
            return Ok(ReserveOutcome::Existing(active));
        }
        return Err(CatalogError::new(
            CatalogErrorCode::SchemaChangeInProgress,
            format!("another schema change is active for {identity}"),
        ));
    }

    let saga_id = reservation.saga_id();
    let saga_uuid = saga_id.as_uuid();
    let request_uuid = saga_uuid;
    let from_version = reservation
        .from_schema
        .as_ref()
        .map(|schema| version_to_i64(schema.version()))
        .transpose()?;
    let target_version = version_to_i64(reservation.target_schema.version())?;
    let request_digest: &[u8] = reservation.request_hash.as_bytes();
    let operation_kind = if reservation.from_schema.is_some() {
        "add_columns"
    } else {
        "create"
    };
    client
        .execute(
            "INSERT INTO arrowcol.schema_change_sagas ( \
                saga_id, table_id, request_id, initiated_by_instance_id, \
                from_schema_version, target_schema_version, state, current_step, \
                attempt_count, request_sha256, operation_kind \
             ) VALUES (@P1, @P2, @P3, NULL, @P4, @P5, 'requested', 0, 0, @P6, @P7);",
            &[
                &saga_uuid,
                &table_id,
                &request_uuid,
                &from_version,
                &target_version,
                &request_digest,
                &operation_kind,
            ],
        )
        .await
        .map_err(|error| db_error("insert schema saga", error))?;

    let target_fingerprint = reservation.target_schema.fingerprint();
    let fingerprint: &[u8] = target_fingerprint.as_bytes();
    let compatibility = if reservation.from_schema.is_some() {
        "backward"
    } else {
        "exact"
    };
    let canonical_ddl = reservation.target_schema.canonical_ddl();
    client
        .execute(
            "INSERT INTO arrowcol.table_schema_versions ( \
                table_id, schema_version, schema_sha256, compatibility, state, saga_id, \
                canonical_ddl, schema_json \
             ) VALUES (@P1, @P2, @P3, @P4, 'pending', @P5, @P6, @P7);",
            &[
                &table_id,
                &target_version,
                &fingerprint,
                &compatibility,
                &saga_uuid,
                &canonical_ddl,
                &schema_json,
            ],
        )
        .await
        .map_err(|error| db_error("insert pending schema version", error))?;
    insert_flat_fields(client, table_id, target_version, flat_fields).await?;

    if reservation.from_schema.is_some() {
        client
            .execute(
                "UPDATE arrowcol.table_schema_heads \
                 SET current_saga_id = @P2, updated_at_utc = SYSUTCDATETIME() \
                 WHERE table_id = @P1;",
                &[&table_id, &saga_uuid],
            )
            .await
            .map_err(|error| db_error("mark active schema saga on head", error))?;
    }
    required_saga(client, saga_id).await.map(ReserveOutcome::Reserved)
}

async fn ensure_catalog_table(client: &mut SqlClient, reservation: &ChangeReservation) -> CatalogResult<Uuid> {
    let identity = reservation.target_schema.identity();
    let catalog = identity.catalog().to_string();
    let schema = identity.schema().to_string();
    let table = identity.table().to_string();
    let row = client
        .query(
            "SELECT table_id FROM arrowcol.catalog_tables WITH (UPDLOCK, HOLDLOCK) \
             WHERE catalog_name = @P1 AND schema_name = @P2 AND table_name = @P3;",
            &[&catalog, &schema, &table],
        )
        .await
        .map_err(|error| db_error("query catalog table identity", error))?
        .into_row()
        .await
        .map_err(|error| db_error("read catalog table identity", error))?;
    if let Some(row) = row {
        let table_id = required_uuid(&row, "table_id")?;
        if table_id != reservation.target_schema.table_id() {
            return Err(CatalogError::new(
                CatalogErrorCode::CorruptCatalogState,
                format!("catalog identity {identity} has a non-deterministic table id"),
            ));
        }
        return Ok(table_id);
    }
    if reservation.from_schema.is_some() {
        return Err(CatalogError::new(
            CatalogErrorCode::TableNotFound,
            format!("catalog table {identity} does not exist"),
        ));
    }
    let table_id = reservation.target_schema.table_id();
    client
        .execute(
            "INSERT INTO arrowcol.catalog_tables (table_id, catalog_name, schema_name, table_name) \
             VALUES (@P1, @P2, @P3, @P4);",
            &[&table_id, &catalog, &schema, &table],
        )
        .await
        .map_err(|error| db_error("insert catalog table identity", error))?;
    Ok(table_id)
}

async fn insert_flat_fields(
    client: &mut SqlClient,
    table_id: Uuid,
    version: i64,
    fields: &[FlatField],
) -> CatalogResult<()> {
    for flat in fields {
        let field_id = flat.field.id();
        let parent = flat.parent;
        let position = i32::try_from(flat.field.position()).map_err(|_| field_value_overflow())?;
        let number = i32::try_from(flat.field.field_number()).map_err(|_| field_value_overflow())?;
        let partition = flat
            .field
            .partition()
            .map(|metadata| i32::try_from(metadata.ordinal()))
            .transpose()
            .map_err(|_| field_value_overflow())?;
        let nullable = flat.field.is_nullable();
        let arrow_kind = arrow_kind(flat.field.mapping().arrow());
        let datafusion = flat.field.mapping().datafusion();
        let hive = flat.field.mapping().hive();
        let protobuf = flat.field.mapping().protobuf().as_str();
        let type_json = serde_json::to_string(flat.field.mapping()).map_err(|error| {
            CatalogError::new(
                CatalogErrorCode::ApplicationDatabase,
                format!("serialize catalog field mapping: {error}"),
            )
        })?;
        client
            .execute(
                "INSERT INTO arrowcol.catalog_fields ( \
                    table_id, schema_version, field_id, parent_field_id, field_path, field_name, \
                    field_position, field_number, is_nullable, partition_ordinal, arrow_type, \
                    datafusion_type, hive_type, protobuf_type, type_json \
                 ) VALUES ( \
                    @P1, @P2, @P3, @P4, @P5, @P6, @P7, @P8, @P9, @P10, @P11, @P12, @P13, @P14, @P15 \
                 );",
                &[
                    &table_id,
                    &version,
                    &field_id,
                    &parent,
                    &flat.path,
                    &flat.field.name(),
                    &position,
                    &number,
                    &nullable,
                    &partition,
                    &arrow_kind,
                    &datafusion,
                    &hive,
                    &protobuf,
                    &type_json,
                ],
            )
            .await
            .map_err(|error| db_error("insert normalized catalog field", error))?;
    }
    Ok(())
}

async fn persist_artifacts_in_transaction(
    client: &mut SqlClient,
    saga_id: SagaId,
    artifacts: &DescriptorArtifacts,
) -> CatalogResult<SagaRecord> {
    acquire_lock(client, &format!("arrowcol.saga.{saga_id}")).await?;
    let saga = required_saga(client, saga_id).await?;
    if saga.step() > SagaStep::PersistArtifacts {
        return Ok(saga);
    }
    if saga.step() < SagaStep::PersistArtifacts {
        return Err(CatalogError::new(
            CatalogErrorCode::CorruptCatalogState,
            format!("saga {saga_id} cannot persist artifacts before Hive application"),
        ));
    }
    let table_id = saga.target_schema().table_id();
    let version = version_to_i64(saga.target_schema().version())?;
    client
        .execute(
            "DELETE FROM arrowcol.schema_artifact_chunks \
             WHERE table_id = @P1 AND schema_version = @P2;",
            &[&table_id, &version],
        )
        .await
        .map_err(|error| db_error("replace schema artifact chunks", error))?;
    for kind in [
        ArtifactKind::ArrowSchema,
        ArtifactKind::ProtobufDescriptor,
        ArtifactKind::Manifest,
    ] {
        insert_artifact(client, table_id, version, kind, artifacts.get(kind)).await?;
    }
    let saga_uuid = saga_id.as_uuid();
    client
        .execute(
            "UPDATE arrowcol.schema_change_sagas \
             SET state = 'applying', current_step = 2, updated_at_utc = SYSUTCDATETIME(), \
                 last_error_code = NULL, last_error_message = NULL \
             WHERE saga_id = @P1 AND completed_at_utc IS NULL AND current_step = 1;",
            &[&saga_uuid],
        )
        .await
        .map_err(|error| db_error("mark schema artifacts persisted", error))?;
    required_saga(client, saga_id).await
}

async fn insert_artifact(
    client: &mut SqlClient,
    table_id: Uuid,
    version: i64,
    kind: ArtifactKind,
    bytes: &[u8],
) -> CatalogResult<()> {
    if bytes.is_empty() {
        return Err(CatalogError::new(
            CatalogErrorCode::DescriptorGeneration,
            format!("{} artifact must not be empty", kind.as_str()),
        ));
    }
    let digest = sha2::Sha256::digest(bytes);
    let digest: &[u8] = digest.as_ref();
    let size = i64::try_from(bytes.len()).map_err(|_| {
        CatalogError::new(
            CatalogErrorCode::DescriptorGeneration,
            "schema artifact length exceeds SQL bigint",
        )
    })?;
    for (ordinal, chunk) in bytes.chunks(ARTIFACT_CHUNK_BYTES).enumerate() {
        let ordinal = i32::try_from(ordinal).map_err(|_| {
            CatalogError::new(
                CatalogErrorCode::DescriptorGeneration,
                "schema artifact has too many chunks",
            )
        })?;
        let kind = kind.as_str();
        let chunk: &[u8] = chunk;
        client
            .execute(
                "INSERT INTO arrowcol.schema_artifact_chunks ( \
                    table_id, schema_version, artifact_kind, chunk_ordinal, \
                    artifact_sha256, artifact_size_bytes, chunk_data \
                 ) VALUES (@P1, @P2, @P3, @P4, @P5, @P6, @P7);",
                &[&table_id, &version, &kind, &ordinal, &digest, &size, &chunk],
            )
            .await
            .map_err(|error| db_error("insert schema artifact chunk", error))?;
    }
    Ok(())
}

async fn finalize_in_transaction(client: &mut SqlClient, saga_id: SagaId) -> CatalogResult<SagaRecord> {
    acquire_lock(client, &format!("arrowcol.saga.{saga_id}")).await?;
    let saga = required_saga(client, saga_id).await?;
    if saga.is_committed() {
        return Ok(saga);
    }
    if saga.step() != SagaStep::Finalize {
        return Err(CatalogError::new(
            CatalogErrorCode::CorruptCatalogState,
            format!("saga {saga_id} cannot finalize from step {:?}", saga.step()),
        ));
    }
    let table_id = saga.target_schema().table_id();
    let target_version = version_to_i64(saga.target_schema().version())?;
    let head = client
        .query(
            "SELECT current_schema_version FROM arrowcol.table_schema_heads WITH (UPDLOCK, HOLDLOCK) \
             WHERE table_id = @P1;",
            &[&table_id],
        )
        .await
        .map_err(|error| db_error("query schema head for finalize", error))?
        .into_row()
        .await
        .map_err(|error| db_error("read schema head for finalize", error))?;
    match (saga.from_schema(), head) {
        (None, None) => {
            client
                .execute(
                    "INSERT INTO arrowcol.table_schema_heads ( \
                        table_id, current_schema_version, current_saga_id \
                     ) VALUES (@P1, @P2, NULL);",
                    &[&table_id, &target_version],
                )
                .await
                .map_err(|error| db_error("insert first schema head", error))?;
        }
        (Some(from), Some(row)) => {
            let actual = required_i64(&row, "current_schema_version")?;
            let expected = version_to_i64(from.version())?;
            if actual != expected && actual != target_version {
                return Err(CatalogError::new(
                    CatalogErrorCode::SchemaVersionConflict,
                    format!(
                        "schema head changed from expected version {} during finalize",
                        from.version()
                    ),
                ));
            }
            client
                .execute(
                    "UPDATE arrowcol.table_schema_heads \
                     SET current_schema_version = @P2, current_saga_id = NULL, \
                         updated_at_utc = SYSUTCDATETIME() \
                     WHERE table_id = @P1;",
                    &[&table_id, &target_version],
                )
                .await
                .map_err(|error| db_error("activate schema head", error))?;
        }
        (None, Some(row)) if required_i64(&row, "current_schema_version")? == target_version => {}
        _ => {
            return Err(CatalogError::new(
                CatalogErrorCode::SchemaVersionConflict,
                "schema head presence did not match the reserved operation",
            ));
        }
    }
    if let Some(from) = saga.from_schema() {
        let from_version = version_to_i64(from.version())?;
        client
            .execute(
                "UPDATE arrowcol.table_schema_versions \
                 SET state = 'superseded', superseded_at_utc = SYSUTCDATETIME() \
                 WHERE table_id = @P1 AND schema_version = @P2 AND state = 'active';",
                &[&table_id, &from_version],
            )
            .await
            .map_err(|error| db_error("supersede previous schema version", error))?;
    }
    client
        .execute(
            "UPDATE arrowcol.table_schema_versions \
             SET state = 'active', activated_at_utc = COALESCE(activated_at_utc, SYSUTCDATETIME()) \
             WHERE table_id = @P1 AND schema_version = @P2 AND state IN ('pending', 'active');",
            &[&table_id, &target_version],
        )
        .await
        .map_err(|error| db_error("activate target schema version", error))?;
    let saga_uuid = saga_id.as_uuid();
    client
        .execute(
            "UPDATE arrowcol.schema_change_sagas \
             SET state = 'committed', current_step = 3, updated_at_utc = SYSUTCDATETIME(), \
                 completed_at_utc = COALESCE(completed_at_utc, SYSUTCDATETIME()), \
                 last_error_code = NULL, last_error_message = NULL \
             WHERE saga_id = @P1;",
            &[&saga_uuid],
        )
        .await
        .map_err(|error| db_error("commit schema saga", error))?;
    client
        .execute(
            "UPDATE arrowcol.catalog_tables SET updated_at_utc = SYSUTCDATETIME() \
             WHERE table_id = @P1;",
            &[&table_id],
        )
        .await
        .map_err(|error| db_error("update catalog table timestamp", error))?;
    required_saga(client, saga_id).await
}

async fn operation_with_client(
    client: &mut SqlClient,
    identity: &TableIdentity,
    request_hash: RequestHash,
) -> CatalogResult<Option<SagaRecord>> {
    let catalog = identity.catalog().to_string();
    let schema = identity.schema().to_string();
    let table = identity.table().to_string();
    let digest: &[u8] = request_hash.as_bytes();
    let row = client
        .query(
            "SELECT s.saga_id \
             FROM arrowcol.catalog_tables AS t \
             INNER JOIN arrowcol.schema_change_sagas AS s ON s.table_id = t.table_id \
             WHERE t.catalog_name = @P1 AND t.schema_name = @P2 AND t.table_name = @P3 \
               AND s.request_sha256 = @P4;",
            &[&catalog, &schema, &table, &digest],
        )
        .await
        .map_err(|error| db_error("query schema operation", error))?
        .into_row()
        .await
        .map_err(|error| db_error("read schema operation", error))?;
    match row {
        Some(row) => {
            let saga_id = SagaId::from_uuid(required_uuid(&row, "saga_id")?);
            required_saga(client, saga_id).await.map(Some)
        }
        None => Ok(None),
    }
}

async fn current_schema_with_client(
    client: &mut SqlClient,
    identity: &TableIdentity,
    locked: bool,
) -> CatalogResult<Option<CatalogSchemaVersion>> {
    let catalog = identity.catalog().to_string();
    let schema = identity.schema().to_string();
    let table = identity.table().to_string();
    let sql = if locked {
        "SELECT v.schema_version, v.schema_sha256, v.schema_json \
         FROM arrowcol.catalog_tables AS t WITH (UPDLOCK, HOLDLOCK) \
         INNER JOIN arrowcol.table_schema_heads AS h WITH (UPDLOCK, HOLDLOCK) ON h.table_id = t.table_id \
         INNER JOIN arrowcol.table_schema_versions AS v WITH (UPDLOCK, HOLDLOCK) \
             ON v.table_id = h.table_id AND v.schema_version = h.current_schema_version \
         WHERE t.catalog_name = @P1 AND t.schema_name = @P2 AND t.table_name = @P3;"
    } else {
        "SELECT v.schema_version, v.schema_sha256, v.schema_json \
         FROM arrowcol.catalog_tables AS t \
         INNER JOIN arrowcol.table_schema_heads AS h ON h.table_id = t.table_id \
         INNER JOIN arrowcol.table_schema_versions AS v \
             ON v.table_id = h.table_id AND v.schema_version = h.current_schema_version \
         WHERE t.catalog_name = @P1 AND t.schema_name = @P2 AND t.table_name = @P3;"
    };
    client
        .query(sql, &[&catalog, &schema, &table])
        .await
        .map_err(|error| db_error("query current catalog schema", error))?
        .into_row()
        .await
        .map_err(|error| db_error("read current catalog schema", error))?
        .map(|row| schema_from_row(&row, identity))
        .transpose()
}

async fn saga_with_client(client: &mut SqlClient, saga_id: SagaId) -> CatalogResult<Option<SagaRecord>> {
    let saga_uuid = saga_id.as_uuid();
    let row = client
        .query(
            "SELECT s.saga_id, s.request_sha256, s.state, s.current_step, s.attempt_count, \
                    s.last_error_code, s.last_error_message, \
                    target.schema_json AS target_schema_json, \
                    source.schema_json AS source_schema_json \
             FROM arrowcol.schema_change_sagas AS s \
             INNER JOIN arrowcol.table_schema_versions AS target \
                ON target.table_id = s.table_id AND target.schema_version = s.target_schema_version \
             LEFT JOIN arrowcol.table_schema_versions AS source \
                ON source.table_id = s.table_id AND source.schema_version = s.from_schema_version \
             WHERE s.saga_id = @P1;",
            &[&saga_uuid],
        )
        .await
        .map_err(|error| db_error("query schema saga", error))?
        .into_row()
        .await
        .map_err(|error| db_error("read schema saga", error))?;
    row.map(|row| saga_from_row(&row)).transpose()
}

async fn required_saga(client: &mut SqlClient, saga_id: SagaId) -> CatalogResult<SagaRecord> {
    saga_with_client(client, saga_id).await?.ok_or_else(|| {
        CatalogError::new(
            CatalogErrorCode::CorruptCatalogState,
            format!("schema saga {saga_id} does not exist"),
        )
    })
}

fn schema_from_row(row: &Row, expected_identity: &TableIdentity) -> CatalogResult<CatalogSchemaVersion> {
    let schema_json = required_str(row, "schema_json")?;
    let schema: CatalogSchemaVersion = serde_json::from_str(schema_json).map_err(|error| {
        CatalogError::new(
            CatalogErrorCode::CorruptCatalogState,
            format!("persisted schema JSON is invalid: {error}"),
        )
    })?;
    let version = schema_version_from_i64(required_i64(row, "schema_version")?)?;
    let fingerprint = fingerprint_from_bytes(required_bytes(row, "schema_sha256")?)?;
    if schema.identity() != expected_identity
        || schema.version() != version
        || schema.fingerprint() != fingerprint
        || schema.table_id() != crate::deterministic_table_id(expected_identity)
        || arrowcol_protocol::fingerprint_schema(&schema.arrow_schema()?) != schema.fingerprint()
    {
        return Err(CatalogError::new(
            CatalogErrorCode::CorruptCatalogState,
            "persisted schema JSON disagrees with normalized catalog columns",
        ));
    }
    Ok(schema)
}

fn saga_from_row(row: &Row) -> CatalogResult<SagaRecord> {
    let saga_id = SagaId::from_uuid(required_uuid(row, "saga_id")?);
    let request_hash = RequestHash::from_bytes(bytes32(required_bytes(row, "request_sha256")?)?);
    let target_schema: CatalogSchemaVersion =
        serde_json::from_str(required_str(row, "target_schema_json")?).map_err(|error| {
            CatalogError::new(
                CatalogErrorCode::CorruptCatalogState,
                format!("persisted target schema JSON is invalid: {error}"),
            )
        })?;
    validate_schema_json(&target_schema)?;
    let from_schema = row
        .try_get::<&str, _>("source_schema_json")
        .map_err(|error| db_error("read source schema JSON", error))?
        .map(|json| {
            serde_json::from_str(json).map_err(|error| {
                CatalogError::new(
                    CatalogErrorCode::CorruptCatalogState,
                    format!("persisted source schema JSON is invalid: {error}"),
                )
            })
        })
        .transpose()?;
    if let Some(schema) = &from_schema {
        validate_schema_json(schema)?;
    }
    let state_text = required_str(row, "state")?;
    let step = SagaStep::from_number(required_i16(row, "current_step")?)?;
    let state = match state_text {
        "requested" | "validating" => SagaState::Reserved,
        "applying" if step == SagaStep::PersistArtifacts => SagaState::HiveApplied,
        "applying" if step == SagaStep::Finalize => SagaState::ArtifactsPersisted,
        "committed" => SagaState::Committed,
        "failed" => SagaState::Failed,
        other => {
            return Err(CatalogError::new(
                CatalogErrorCode::CorruptCatalogState,
                format!("unsupported catalog saga state {other:?} at step {step:?}"),
            ));
        }
    };
    let attempts = required_i32(row, "attempt_count")?;
    let attempt_count = u32::try_from(attempts).map_err(|_| {
        CatalogError::new(
            CatalogErrorCode::CorruptCatalogState,
            "schema saga attempt count is negative",
        )
    })?;
    let error_code = row
        .try_get::<&str, _>("last_error_code")
        .map_err(|error| db_error("read saga error code", error))?;
    let error_message = row
        .try_get::<&str, _>("last_error_message")
        .map_err(|error| db_error("read saga error message", error))?;
    let last_error = match (error_code, error_message) {
        (Some(code), Some(message)) => Some(CatalogError::new(CatalogErrorCode::parse(code)?, message)),
        (None, None) => None,
        _ => {
            return Err(CatalogError::new(
                CatalogErrorCode::CorruptCatalogState,
                "schema saga has a partial persisted error",
            ));
        }
    };
    Ok(SagaRecord::from_persisted(PersistedSagaRecord {
        id: saga_id,
        request_hash,
        from_schema,
        target_schema,
        state,
        step,
        attempt_count,
        last_error,
    }))
}

async fn acquire_lock(client: &mut SqlClient, resource: &str) -> CatalogResult<()> {
    let row = client
        .query(
            "DECLARE @result int; \
             EXEC @result = sys.sp_getapplock \
                @Resource = @P1, @LockMode = 'Exclusive', @LockOwner = 'Transaction', \
                @LockTimeout = 30000, @DbPrincipal = 'public'; \
             SELECT @result AS lock_result;",
            &[&resource],
        )
        .await
        .map_err(|error| db_error("acquire catalog application lock", error))?
        .into_row()
        .await
        .map_err(|error| db_error("read catalog application lock result", error))?
        .ok_or_else(|| {
            CatalogError::new(
                CatalogErrorCode::ApplicationDatabase,
                "SQL Server did not return an application lock result",
            )
        })?;
    if required_i32(&row, "lock_result")? < 0 {
        return Err(CatalogError::new(
            CatalogErrorCode::SchemaChangeInProgress,
            format!("timed out acquiring catalog lock {resource}"),
        ));
    }
    Ok(())
}

async fn finish_transaction<T>(client: &mut SqlClient, result: CatalogResult<T>, context: &str) -> CatalogResult<T> {
    match result {
        Ok(value) => {
            client
                .commit_transaction()
                .await
                .map_err(|error| db_error(&format!("commit {context}"), error))?;
            Ok(value)
        }
        Err(error) => {
            let _rollback = client.rollback_transaction().await;
            Err(error)
        }
    }
}

fn artifact_rows(rows: Vec<Row>) -> CatalogResult<DescriptorArtifacts> {
    struct Accumulator {
        digest: [u8; 32],
        size: usize,
        next_ordinal: i32,
        bytes: Vec<u8>,
    }
    let mut artifacts = BTreeMap::<String, Accumulator>::new();
    for row in rows {
        let kind = required_str(&row, "artifact_kind")?.to_owned();
        let ordinal = required_i32(&row, "chunk_ordinal")?;
        let digest = bytes32(required_bytes(&row, "artifact_sha256")?)?;
        let size = usize::try_from(required_i64(&row, "artifact_size_bytes")?).map_err(|_| {
            CatalogError::new(
                CatalogErrorCode::CorruptCatalogState,
                "schema artifact size is negative or too large",
            )
        })?;
        let chunk = required_bytes(&row, "chunk_data")?;
        let entry = artifacts.entry(kind).or_insert_with(|| Accumulator {
            digest,
            size,
            next_ordinal: 0,
            bytes: Vec::with_capacity(size),
        });
        if entry.digest != digest || entry.size != size || ordinal != entry.next_ordinal {
            return Err(CatalogError::new(
                CatalogErrorCode::CorruptCatalogState,
                "schema artifact chunks have inconsistent metadata or ordering",
            ));
        }
        entry.bytes.extend_from_slice(chunk);
        entry.next_ordinal += 1;
    }
    let mut take = |kind: ArtifactKind| -> CatalogResult<Vec<u8>> {
        let artifact = artifacts.remove(kind.as_str()).ok_or_else(|| {
            CatalogError::new(
                CatalogErrorCode::CorruptCatalogState,
                format!("schema artifact {} is missing", kind.as_str()),
            )
        })?;
        if artifact.bytes.len() != artifact.size
            || <[u8; 32]>::from(sha2::Sha256::digest(&artifact.bytes)) != artifact.digest
        {
            return Err(CatalogError::new(
                CatalogErrorCode::CorruptCatalogState,
                format!("schema artifact {} failed length or digest validation", kind.as_str()),
            ));
        }
        Ok(artifact.bytes)
    };
    let arrow = take(ArtifactKind::ArrowSchema)?;
    let protobuf = take(ArtifactKind::ProtobufDescriptor)?;
    let manifest = take(ArtifactKind::Manifest)?;
    if !artifacts.is_empty() {
        return Err(CatalogError::new(
            CatalogErrorCode::CorruptCatalogState,
            "unknown schema artifact kind is persisted",
        ));
    }
    Ok(DescriptorArtifacts::from_parts(arrow, protobuf, manifest))
}

struct FlatField {
    parent: Option<Uuid>,
    path: String,
    field: CatalogField,
}

fn flatten_schema_fields(schema: &CatalogSchemaVersion) -> CatalogResult<Vec<FlatField>> {
    let mut output = Vec::new();
    for field in schema.fields() {
        let path = FieldPath::root(field.name());
        flatten_field(field, None, &path, &mut output)?;
    }
    Ok(output)
}

fn flatten_field(
    field: &CatalogField,
    parent: Option<Uuid>,
    path: &FieldPath,
    output: &mut Vec<FlatField>,
) -> CatalogResult<()> {
    if path.as_str().len() > 1_024 {
        return Err(CatalogError::new(
            CatalogErrorCode::TypeMappingUnsupported,
            "nested field path exceeds 1024 bytes",
        ));
    }
    output.push(FlatField {
        parent,
        path: path.as_str().to_owned(),
        field: field.clone(),
    });
    match field.mapping().arrow() {
        CatalogType::Struct { fields } => {
            for child in fields {
                flatten_field(child, Some(field.id()), &path.child(child.name()), output)?;
            }
        }
        CatalogType::List { element, .. } => {
            flatten_field(element, Some(field.id()), &path.child(element.name()), output)?;
        }
        CatalogType::Map { key, value, .. } => {
            flatten_field(key, Some(field.id()), &path.child(key.name()), output)?;
            flatten_field(value, Some(field.id()), &path.child(value.name()), output)?;
        }
        CatalogType::Dictionary { .. }
        | CatalogType::Boolean
        | CatalogType::Int8
        | CatalogType::Int16
        | CatalogType::Int32
        | CatalogType::Int64
        | CatalogType::UInt8
        | CatalogType::UInt16
        | CatalogType::UInt32
        | CatalogType::UInt64
        | CatalogType::Float32
        | CatalogType::Float64
        | CatalogType::Timestamp { .. }
        | CatalogType::Date32
        | CatalogType::Date64
        | CatalogType::Time32 { .. }
        | CatalogType::Time64 { .. }
        | CatalogType::Duration { .. }
        | CatalogType::Binary
        | CatalogType::FixedSizeBinary { .. }
        | CatalogType::LargeBinary
        | CatalogType::BinaryView
        | CatalogType::Utf8
        | CatalogType::LargeUtf8
        | CatalogType::Utf8View
        | CatalogType::Decimal128 { .. } => {}
    }
    Ok(())
}

const fn arrow_kind(data_type: &CatalogType) -> &'static str {
    match data_type {
        CatalogType::Boolean => "boolean",
        CatalogType::Int8 => "int8",
        CatalogType::Int16 => "int16",
        CatalogType::Int32 => "int32",
        CatalogType::Int64 => "int64",
        CatalogType::UInt8 => "uint8",
        CatalogType::UInt16 => "uint16",
        CatalogType::UInt32 => "uint32",
        CatalogType::UInt64 => "uint64",
        CatalogType::Float32 => "float32",
        CatalogType::Float64 => "float64",
        CatalogType::Timestamp { .. } => "timestamp",
        CatalogType::Date32 => "date32",
        CatalogType::Date64 => "date64",
        CatalogType::Time32 { .. } => "time32",
        CatalogType::Time64 { .. } => "time64",
        CatalogType::Duration { .. } => "duration",
        CatalogType::Binary => "binary",
        CatalogType::FixedSizeBinary { .. } => "fixed_size_binary",
        CatalogType::LargeBinary => "large_binary",
        CatalogType::BinaryView => "binary_view",
        CatalogType::Utf8 => "utf8",
        CatalogType::LargeUtf8 => "large_utf8",
        CatalogType::Utf8View => "utf8_view",
        CatalogType::Decimal128 { .. } => "decimal128",
        CatalogType::Struct { .. } => "struct",
        CatalogType::List { .. } => "list",
        CatalogType::Map { .. } => "map",
        CatalogType::Dictionary { .. } => "dictionary",
    }
}

fn required_uuid(row: &Row, column: &str) -> CatalogResult<Uuid> {
    row.try_get::<Uuid, _>(column)
        .map_err(|error| db_error(&format!("read {column}"), error))?
        .ok_or_else(|| missing_column(column))
}

fn required_i64(row: &Row, column: &str) -> CatalogResult<i64> {
    row.try_get::<i64, _>(column)
        .map_err(|error| db_error(&format!("read {column}"), error))?
        .ok_or_else(|| missing_column(column))
}

fn required_i32(row: &Row, column: &str) -> CatalogResult<i32> {
    row.try_get::<i32, _>(column)
        .map_err(|error| db_error(&format!("read {column}"), error))?
        .ok_or_else(|| missing_column(column))
}

fn required_i16(row: &Row, column: &str) -> CatalogResult<i16> {
    row.try_get::<i16, _>(column)
        .map_err(|error| db_error(&format!("read {column}"), error))?
        .ok_or_else(|| missing_column(column))
}

fn required_str<'a>(row: &'a Row, column: &str) -> CatalogResult<&'a str> {
    row.try_get::<&str, _>(column)
        .map_err(|error| db_error(&format!("read {column}"), error))?
        .ok_or_else(|| missing_column(column))
}

fn required_bytes<'a>(row: &'a Row, column: &str) -> CatalogResult<&'a [u8]> {
    row.try_get::<&[u8], _>(column)
        .map_err(|error| db_error(&format!("read {column}"), error))?
        .ok_or_else(|| missing_column(column))
}

fn missing_column(column: &str) -> CatalogError {
    CatalogError::new(
        CatalogErrorCode::CorruptCatalogState,
        format!("required SQL column {column} is NULL"),
    )
}

fn bytes32(bytes: &[u8]) -> CatalogResult<[u8; 32]> {
    bytes.try_into().map_err(|_| {
        CatalogError::new(
            CatalogErrorCode::CorruptCatalogState,
            format!("expected a 32-byte digest, received {} bytes", bytes.len()),
        )
    })
}

fn fingerprint_from_bytes(bytes: &[u8]) -> CatalogResult<SchemaFingerprint> {
    bytes32(bytes).map(SchemaFingerprint::from_bytes)
}

fn version_to_i64(version: SchemaVersion) -> CatalogResult<i64> {
    i64::try_from(version.get()).map_err(|_| {
        CatalogError::new(
            CatalogErrorCode::CorruptCatalogState,
            format!("schema version {version} exceeds SQL bigint"),
        )
    })
}

fn schema_version_from_i64(value: i64) -> CatalogResult<SchemaVersion> {
    u64::try_from(value)
        .map_err(|_| {
            CatalogError::new(
                CatalogErrorCode::CorruptCatalogState,
                format!("persisted schema version {value} is negative"),
            )
        })
        .and_then(SchemaVersion::new)
}

fn field_value_overflow() -> CatalogError {
    CatalogError::new(
        CatalogErrorCode::TypeMappingUnsupported,
        "field position, number, or partition ordinal exceeds SQL int",
    )
}

fn truncate_chars(value: &str, maximum: usize) -> String {
    value.chars().take(maximum).collect()
}

fn db_error(context: &str, error: impl Display) -> CatalogError {
    CatalogError::new(
        CatalogErrorCode::ApplicationDatabase,
        format!("SQL Server {context} failed: {error}"),
    )
}
