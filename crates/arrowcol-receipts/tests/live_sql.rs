use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrowcol_catalog::{
    CatalogControlPlane, CatalogStore, DdlParser, FlightSqlDdlHandler, FlightSqlDdlRequest, MemoryCatalogStore,
    MemoryMetastore, SchemaValidator,
};
use arrowcol_protocol::{
    ClaimedSchemaFingerprint, CollectorInstanceId, DeltaPublicationResult, ErrorCode, ErrorStatus, ImplementationPool,
    Measurement, ReceiptState, UnixMillis, fingerprint_schema,
};
use arrowcol_receipts::{
    FailureDisposition, OwnerRegistration, PublicationBatch, ReceiptErrorKind, ReceiptFailure, ReceiptStore,
    ReceiptVersion, SqlServerReceiptConfig, SqlServerReceiptStore, TableLeaseRequest, ValidatedDelivery,
};
use tiberius::{AuthMethod, Client, Config, EncryptionLevel, Row};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use uuid::Uuid;

type SqlClient = Client<Compat<TcpStream>>;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the live arrowcol-infra SQL Server"]
async fn live_receipts_prove_durability_races_and_hot_queries() -> anyhow::Result<()> {
    if std::env::var("ARROWCOL_SQLSERVER_TEST").as_deref() != Ok("1") {
        return Ok(());
    }
    let fixture = fixture().await?;
    let store = Arc::new(SqlServerReceiptStore::new(sql_config()?));
    seed_catalog(&fixture).await?;
    let owner = CollectorInstanceId::new();
    let start = now()?;
    let owner_deadline = plus(start, 180_000)?;
    store
        .register_owner(OwnerRegistration {
            instance_id: owner,
            instance_key: format!("live-receipts-{}", Uuid::new_v4().simple()),
            generation: 1,
            host_name: Some("live-test".to_owned()),
            build_version: Some(env!("CARGO_PKG_VERSION").to_owned()),
            started_at_unix_ms: start,
            lease_deadline_unix_ms: owner_deadline,
        })
        .await?;
    let lease = store
        .acquire_table_lease(TableLeaseRequest {
            table_id: fixture.validation.registered().table_id(),
            table: fixture.validation.registered().identity().clone(),
            owner_instance_id: owner,
            implementation_pool: ImplementationPool::Direct,
            issued_at_unix_ms: start,
            expires_at_unix_ms: owner_deadline,
        })
        .await?;
    let delivery = ValidatedDelivery::new(&fixture.validation, 3, 1, 24)?;

    let duplicates = store
        .create_flight_batch(
            owner,
            &lease,
            plus(start, 60_000)?,
            &[delivery.clone(), delivery.clone()],
        )
        .await?;
    assert_eq!(duplicates.len(), 2);
    assert_ne!(duplicates[0].receipt_id, duplicates[1].receipt_id);
    assert_eq!(duplicates[0].state, ReceiptState::Accepted);

    let first_version = ReceiptVersion::new(duplicates[0].receipt_id, 0);
    let left = {
        let store = Arc::clone(&store);
        let lease_deadline = plus(start, 70_000)?;
        tokio::spawn(async move { store.mark_buffered(owner, &[first_version], lease_deadline).await })
    };
    let right = {
        let store = Arc::clone(&store);
        let lease_deadline = plus(start, 70_000)?;
        tokio::spawn(async move { store.mark_buffered(owner, &[first_version], lease_deadline).await })
    };
    let raced = [left.await?, right.await?];
    assert_eq!(raced.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        raced
            .iter()
            .filter_map(|result| result.as_ref().err())
            .next()
            .ok_or_else(|| anyhow::anyhow!("missing optimistic race loser"))?
            .kind(),
        ReceiptErrorKind::Conflict
    );

    let second = store
        .mark_buffered(
            owner,
            &[ReceiptVersion::new(duplicates[1].receipt_id, 0)],
            plus(start, 70_000)?,
        )
        .await?;
    assert_eq!(second[0].revision, 1);
    let retryable = ReceiptFailure::new(
        FailureDisposition::Retryable,
        ErrorStatus::new(ErrorCode::PublicationFailed, "live retry race")?,
    )?;
    let first_buffered = store
        .receipt(duplicates[0].receipt_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("first raced receipt missing"))?;
    let first_failure_version = ReceiptVersion::new(first_buffered.receipt_id, first_buffered.revision);
    let failed_at = now()?;
    let left = {
        let store = Arc::clone(&store);
        let retryable = retryable.clone();
        tokio::spawn(async move {
            store
                .fail_batch(owner, &[first_failure_version], &retryable, failed_at)
                .await
        })
    };
    let right = {
        let store = Arc::clone(&store);
        let retryable = retryable.clone();
        tokio::spawn(async move {
            store
                .fail_batch(owner, &[first_failure_version], &retryable, failed_at)
                .await
        })
    };
    let retry_race = [left.await?, right.await?];
    assert_eq!(retry_race.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        retry_race
            .iter()
            .filter_map(|result| result.as_ref().err())
            .next()
            .ok_or_else(|| anyhow::anyhow!("missing retry race loser"))?
            .kind(),
        ReceiptErrorKind::Conflict
    );
    store
        .fail_batch(
            owner,
            &[ReceiptVersion::new(second[0].receipt_id, second[0].revision)],
            &retryable,
            now()?,
        )
        .await?;

    let publication_receipts = store
        .create_flight_batch(
            owner,
            &lease,
            plus(start, 80_000)?,
            &[delivery.clone(), delivery.clone()],
        )
        .await?;
    let accepted_versions: Vec<_> = publication_receipts
        .iter()
        .map(|record| ReceiptVersion::new(record.receipt_id, record.revision))
        .collect();
    let buffered = store
        .mark_buffered(owner, &accepted_versions, plus(start, 90_000)?)
        .await?;
    let buffered_versions: Vec<_> = buffered
        .iter()
        .map(|record| ReceiptVersion::new(record.receipt_id, record.revision))
        .collect();
    let flushing = store
        .mark_flushing(owner, &buffered_versions, plus(start, 100_000)?)
        .await?;
    let flushing_versions: Vec<_> = flushing
        .iter()
        .map(|record| ReceiptVersion::new(record.receipt_id, record.revision))
        .collect();
    let published_at = now()?;
    let published = store
        .publish_commit(
            owner,
            &PublicationBatch {
                receipts: flushing_versions,
                publication: DeltaPublicationResult::new(
                    Measurement::new(42)?,
                    Measurement::new(1)?,
                    Measurement::new(6)?,
                    Measurement::new(128)?,
                    delivery.schema_fingerprint(),
                    published_at,
                    lease.publication_fence(),
                ),
            },
        )
        .await?;
    assert_eq!(published.len(), 2);
    assert!(published.iter().all(|record| record.can_client_purge()));
    assert!(published.iter().all(|record| {
        record
            .publication
            .as_ref()
            .is_some_and(|result| result.delta_version().get() == 42)
    }));

    let expiring = store
        .create_flight_batch(owner, &lease, plus(now()?, 100)?, std::slice::from_ref(&delivery))
        .await?;
    tokio::time::sleep(Duration::from_millis(160)).await;
    let reconciled = store.reconcile_expired(now()?, 16).await?;
    assert!(
        reconciled
            .reconciled
            .iter()
            .any(|record| record.receipt_id == expiring[0].receipt_id
                && record.failure_disposition == Some(arrowcol_receipts::FailureDisposition::Retryable)
                && !record.can_client_purge())
    );

    let uncertain = store
        .create_flight_batch(owner, &lease, plus(now()?, 100)?, std::slice::from_ref(&delivery))
        .await?;
    let uncertain = store
        .mark_buffered(
            owner,
            &[ReceiptVersion::new(uncertain[0].receipt_id, uncertain[0].revision)],
            plus(now()?, 100)?,
        )
        .await?;
    let uncertain = store
        .mark_flushing(
            owner,
            &[ReceiptVersion::new(uncertain[0].receipt_id, uncertain[0].revision)],
            plus(now()?, 100)?,
        )
        .await?;
    tokio::time::sleep(Duration::from_millis(160)).await;
    let reconciled = store.reconcile_expired(now()?, 16).await?;
    assert!(reconciled.reconciled.iter().any(|record| {
        record.receipt_id == uncertain[0].receipt_id
            && record.state == arrowcol_protocol::ReceiptState::PublicationUnknown
            && record.failure_disposition == Some(arrowcol_receipts::FailureDisposition::Permanent)
            && !record.can_client_purge()
    }));

    let statuses = store
        .receipts(&[
            duplicates[0].receipt_id,
            duplicates[1].receipt_id,
            publication_receipts[0].receipt_id,
            publication_receipts[1].receipt_id,
            expiring[0].receipt_id,
            uncertain[0].receipt_id,
        ])
        .await?;
    assert_eq!(statuses.len(), 6);
    assert!(statuses.iter().all(Option::is_some));
    hot_query_and_append_only_checks(owner).await?;

    let drain = store.start_owner_drain(owner).await?;
    assert_eq!(drain.active_receipts, 0);
    assert_eq!(drain.active_table_leases, 1);
    store.finish_owner_drain(owner, now()?).await?;
    Ok(())
}

struct Fixture {
    validation: arrowcol_catalog::ValidatedIncomingSchema,
}

async fn fixture() -> anyhow::Result<Fixture> {
    let suffix = Uuid::new_v4().simple().to_string();
    let table_name = format!("receipt_{}", &suffix[..12]);
    let store = Arc::new(MemoryCatalogStore::new());
    let service = CatalogControlPlane::new(
        DdlParser::default(),
        Arc::<MemoryCatalogStore>::clone(&store),
        Arc::new(MemoryMetastore::new()),
    );
    let completion = service
        .execute_ddl(FlightSqlDdlRequest::new(format!(
            "CREATE TABLE telemetry.receipts.{table_name} (value BIGINT NOT NULL)"
        )))
        .await?;
    let schema = store
        .current_schema(completion.identity())
        .await?
        .ok_or_else(|| anyhow::anyhow!("live catalog schema missing"))?
        .arrow_schema()?;
    let validation = SchemaValidator::new(store)
        .validate(
            completion.identity(),
            ClaimedSchemaFingerprint::new(fingerprint_schema(&schema)),
            &schema,
        )
        .await?;
    Ok(Fixture { validation })
}

async fn seed_catalog(fixture: &Fixture) -> anyhow::Result<()> {
    let schema = fixture.validation.registered();
    let table_id = schema.table_id();
    let saga_id = Uuid::new_v4();
    let request_id = Uuid::new_v4();
    let version = i64::try_from(schema.version().get())?;
    let fingerprint = schema.fingerprint();
    let digest: &[u8] = fingerprint.as_bytes();
    let schema_json = serde_json::to_string(schema)?;
    let request_digest = [7_u8; 32];
    let mut client = connect().await?;
    client.begin_transaction().await?;
    let result: anyhow::Result<()> = async {
        client
            .execute(
                "INSERT INTO arrowcol.catalog_tables (table_id, catalog_name, schema_name, table_name) \
                 VALUES (@P1, @P2, @P3, @P4);",
                &[
                    &table_id,
                    &schema.identity().catalog().to_string(),
                    &schema.identity().schema().to_string(),
                    &schema.identity().table().to_string(),
                ],
            )
            .await?;
        client
            .execute(
                "INSERT INTO arrowcol.schema_change_sagas ( \
                    saga_id, table_id, request_id, initiated_by_instance_id, from_schema_version, \
                    target_schema_version, state, current_step, attempt_count, completed_at_utc, \
                    request_sha256, operation_kind \
                 ) VALUES (@P1, @P2, @P3, NULL, NULL, @P4, 'committed', 4, 0, SYSUTCDATETIME(), @P5, 'create');",
                &[&saga_id, &table_id, &request_id, &version, &&request_digest[..]],
            )
            .await?;
        client
            .execute(
                "INSERT INTO arrowcol.table_schema_versions ( \
                    table_id, schema_version, schema_sha256, compatibility, state, saga_id, \
                    activated_at_utc, canonical_ddl, schema_json \
                 ) VALUES (@P1, @P2, @P3, 'exact', 'active', @P4, SYSUTCDATETIME(), @P5, @P6);",
                &[
                    &table_id,
                    &version,
                    &digest,
                    &saga_id,
                    &schema.canonical_ddl(),
                    &schema_json,
                ],
            )
            .await?;
        client
            .execute(
                "INSERT INTO arrowcol.table_schema_heads (table_id, current_schema_version) VALUES (@P1, @P2);",
                &[&table_id, &version],
            )
            .await?;
        Ok(())
    }
    .await;
    match result {
        Ok(()) => {
            client.commit_transaction().await?;
            Ok(())
        }
        Err(error) => {
            let _rollback = client.rollback_transaction().await;
            Err(error)
        }
    }
}

async fn hot_query_and_append_only_checks(owner: CollectorInstanceId) -> anyhow::Result<()> {
    let mut client = connect().await?;
    let owner = owner.as_uuid();
    client
        .query(
            "SET ANSI_NULLS ON; SET ANSI_PADDING ON; SET ANSI_WARNINGS ON; SET ARITHABORT ON; \
             SET CONCAT_NULL_YIELDS_NULL ON; SET QUOTED_IDENTIFIER ON; SET NUMERIC_ROUNDABORT OFF; \
             SELECT TOP (8) receipt_id, state, revision \
             FROM arrowcol.delivery_receipts WITH (INDEX(IX_delivery_receipts_owner_active)) \
             WHERE owner_instance_id = @P1 AND state IN ('accepted', 'buffered', 'flushing') \
             ORDER BY receipt_lease_deadline_utc, receipt_id;",
            &[&owner],
        )
        .await?
        .into_first_result()
        .await?;
    client
        .query(
            "SELECT TOP (8) receipt_id, owner_instance_id, revision, state \
             FROM arrowcol.delivery_receipts WITH (INDEX(IX_delivery_receipts_expiry_active)) \
             WHERE receipt_lease_deadline_utc <= SYSUTCDATETIME() \
               AND state IN ('accepted', 'buffered', 'flushing') \
             ORDER BY receipt_lease_deadline_utc, receipt_id;",
            &[],
        )
        .await?
        .into_first_result()
        .await?;
    let event = client
        .query(
            "SELECT TOP (1) receipt_event_id FROM arrowcol.receipt_events ORDER BY receipt_event_id DESC;",
            &[],
        )
        .await?
        .into_row()
        .await?
        .ok_or_else(|| anyhow::anyhow!("receipt event history is empty"))?;
    let event_id = required_i64(&event, "receipt_event_id")?;
    let update = client
        .execute(
            "UPDATE arrowcol.receipt_events SET event_detail = N'forbidden' WHERE receipt_event_id = @P1;",
            &[&event_id],
        )
        .await;
    assert!(
        update.is_err(),
        "append-only receipt event update unexpectedly succeeded"
    );
    Ok(())
}

fn sql_config() -> anyhow::Result<SqlServerReceiptConfig> {
    Ok(SqlServerReceiptConfig::with_password_file(
        std::env::var("ARROWCOL_SQLSERVER_HOST").unwrap_or_else(|_| "127.0.0.1".to_owned()),
        std::env::var("ARROWCOL_SQLSERVER_PORT")
            .ok()
            .map(|value| value.parse())
            .transpose()?
            .unwrap_or(14_333),
        "arrowcol",
        "arrowcol_app",
        secret_path(),
    )?
    .trust_server_certificate(true))
}

async fn connect() -> anyhow::Result<SqlClient> {
    let host = std::env::var("ARROWCOL_SQLSERVER_HOST").unwrap_or_else(|_| "127.0.0.1".to_owned());
    let port = std::env::var("ARROWCOL_SQLSERVER_PORT")
        .ok()
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(14_333);
    let password_path = secret_path();
    let password = std::fs::read_to_string(password_path)?;
    let password = password.trim_end_matches(['\r', '\n']).to_owned();
    let mut config = Config::new();
    config.host(host);
    config.port(port);
    config.database("arrowcol");
    config.authentication(AuthMethod::sql_server("arrowcol_app", password));
    config.encryption(EncryptionLevel::Required);
    config.trust_cert();
    let tcp = TcpStream::connect(config.get_addr()).await?;
    tcp.set_nodelay(true)?;
    Ok(Client::connect(config, tcp.compat_write()).await?)
}

fn secret_path() -> PathBuf {
    std::env::var_os("ARROWCOL_APP_PASSWORD_FILE").map_or_else(
        || {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../projects/arrowcol-infra/secrets/arrowcol-app-password.txt")
        },
        PathBuf::from,
    )
}

fn required_i64(row: &Row, column: &str) -> anyhow::Result<i64> {
    row.try_get::<i64, _>(column)?
        .ok_or_else(|| anyhow::anyhow!("{column} was NULL"))
}

fn now() -> anyhow::Result<UnixMillis> {
    let millis = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    Ok(UnixMillis::new(u64::try_from(millis)?)?)
}

fn plus(value: UnixMillis, millis: u64) -> anyhow::Result<UnixMillis> {
    Ok(UnixMillis::new(
        value
            .get()
            .checked_add(millis)
            .ok_or_else(|| anyhow::anyhow!("timestamp overflow"))?,
    )?)
}
