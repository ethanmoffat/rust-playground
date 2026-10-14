use std::{env, sync::Arc, time::Duration};

use arrowcol_catalog::{
    CatalogControlPlane, CatalogStore, DdlParser, FlightSqlDdlHandler, FlightSqlDdlRequest, HiveMetastore,
    HiveMetastoreConfig, Metastore, SagaId, SqlServerCatalogStore, SqlServerConfig,
};
use arrowcol_sql_server::{ConnectionConfig, PoolConfig, SqlPool};
use uuid::Uuid;

static LIVE_CATALOG_CASE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test]
async fn live_sql_server_saga_creates_visible_hive_table() -> anyhow::Result<()> {
    live_catalog_case(false).await
}

#[tokio::test]
#[ignore = "requires the live arrowcol-infra SQL Server and Hive Metastore"]
async fn pooled_live_sql_server_saga_reuses_after_missing_saga_error() -> anyhow::Result<()> {
    live_catalog_case(true).await
}

async fn live_catalog_case(pooled: bool) -> anyhow::Result<()> {
    if env::var_os("ARROWCOL_CATALOG_LIVE_TEST").is_none() {
        return Ok(());
    }
    // Startup reconciliation scans every incomplete saga in the database.
    let _isolation = LIVE_CATALOG_CASE.lock().await;
    let sql_port = env::var("ARROWCOL_SQLSERVER_PORT")?.parse()?;
    let hive_port = env::var("ARROWCOL_HIVE_METASTORE_PORT")?.parse()?;
    let mut store = SqlServerCatalogStore::new(
        SqlServerConfig::with_password_file(
            env::var("ARROWCOL_SQLSERVER_HOST")?,
            sql_port,
            "arrowcol",
            "arrowcol_app",
            env::var("ARROWCOL_SQLSERVER_PASSWORD_FILE")?,
        )?
        .trust_server_certificate(true),
    );
    let pool = if pooled {
        let password = std::fs::read_to_string(env::var("ARROWCOL_SQLSERVER_PASSWORD_FILE")?)?;
        let pool = SqlPool::new(
            ConnectionConfig::new(
                env::var("ARROWCOL_SQLSERVER_HOST")?,
                sql_port,
                "arrowcol",
                "arrowcol_app",
                password.trim_end_matches(['\r', '\n']),
            )
            .trust_server_certificate(true),
            PoolConfig {
                max_size: 2,
                acquire_timeout: Duration::from_secs(5),
            },
        )?;
        store = store.with_pool(pool.clone())?;
        Some(pool)
    } else {
        None
    };
    let store = Arc::new(store.clone());
    let hive = Arc::new(HiveMetastore::new(HiveMetastoreConfig::new(
        env::var("ARROWCOL_HIVE_METASTORE_HOST")?,
        hive_port,
    )));
    let (service, startup) = CatalogControlPlane::start(
        DdlParser::default(),
        Arc::<SqlServerCatalogStore>::clone(&store),
        Arc::<HiveMetastore>::clone(&hive),
    )
    .await?;
    if !startup.failures.is_empty() {
        anyhow::bail!("startup reconciliation failures: {:?}", startup.failures);
    }

    let table = format!("catalog_control_plane_{}", &Uuid::new_v4().simple().to_string()[..12]);
    let created = service
        .execute_ddl(FlightSqlDdlRequest::new(format!(
            "CREATE TABLE arrowcol.integration.{table} ( \
                sequence BIGINT UNSIGNED NOT NULL, \
                observed_at TIMESTAMP(9) NOT NULL, \
                trace_id FIXED_SIZE_BINARY(16), \
                attributes MAP(VARCHAR, VARCHAR) \
             ) PARTITIONED BY (event_date DATE NOT NULL)",
        )))
        .await?;
    let altered = service
        .execute_ddl(FlightSqlDdlRequest::new(format!(
            "ALTER TABLE arrowcol.integration.{table} \
             ADD COLUMNS (optional_note VARCHAR)",
        )))
        .await?;
    assert!(altered.schema_version() >= created.schema_version());

    let history = store.schema_history(altered.identity()).await?;
    assert!(!history.is_empty());
    let current = store
        .current_schema(altered.identity())
        .await?
        .ok_or_else(|| anyhow::anyhow!("missing current SQL schema"))?;
    assert_eq!(current.fingerprint(), altered.fingerprint());
    assert!(
        store
            .artifacts(altered.identity(), altered.schema_version())
            .await?
            .is_some()
    );

    let hive_table = hive
        .table(altered.identity())
        .await?
        .ok_or_else(|| anyhow::anyhow!("Hive table is not visible"))?;
    assert_eq!(hive_table.schema_version(), altered.schema_version());
    assert_eq!(hive_table.fingerprint(), altered.fingerprint());
    assert!(
        hive_table
            .fields()
            .iter()
            .any(|(name, hive_type)| name == "optional_note" && hive_type == "string")
    );
    assert_eq!(hive_table.partitions(), &[("event_date".to_owned(), "date".to_owned())]);
    if let Some(pool) = pool {
        assert!(
            store
                .mark_hive_applied(SagaId::from_uuid(Uuid::new_v4()))
                .await
                .is_err()
        );
        assert!(store.current_schema(altered.identity()).await?.is_some());
        let stats = pool.stats();
        assert!(stats.reused > 0, "successful catalog operations must reuse sessions");
        assert!(stats.discarded > 0, "missing saga operation must discard its session");
        assert!(stats.size <= 2);
    }
    Ok(())
}
