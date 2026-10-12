use std::{env, sync::Arc};

use arrowcol_catalog::{
    CatalogControlPlane, CatalogStore, DdlParser, FlightSqlDdlHandler, FlightSqlDdlRequest, HiveMetastore,
    HiveMetastoreConfig, Metastore, SqlServerCatalogStore, SqlServerConfig,
};

#[tokio::test]
async fn live_sql_server_saga_creates_visible_hive_table() -> anyhow::Result<()> {
    if env::var_os("ARROWCOL_CATALOG_LIVE_TEST").is_none() {
        return Ok(());
    }
    let sql_port = env::var("ARROWCOL_SQLSERVER_PORT")?.parse()?;
    let hive_port = env::var("ARROWCOL_HIVE_METASTORE_PORT")?.parse()?;
    let store = Arc::new(SqlServerCatalogStore::new(
        SqlServerConfig::with_password_file(
            env::var("ARROWCOL_SQLSERVER_HOST")?,
            sql_port,
            "arrowcol",
            "arrowcol_app",
            env::var("ARROWCOL_SQLSERVER_PASSWORD_FILE")?,
        )?
        .trust_server_certificate(true),
    ));
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

    let created = service
        .execute_ddl(FlightSqlDdlRequest::new(
            "CREATE TABLE arrowcol.integration.catalog_control_plane ( \
                sequence BIGINT UNSIGNED NOT NULL, \
                observed_at TIMESTAMP(9) NOT NULL, \
                trace_id FIXED_SIZE_BINARY(16), \
                attributes MAP(VARCHAR, VARCHAR) \
             ) PARTITIONED BY (event_date DATE NOT NULL)",
        ))
        .await?;
    let altered = service
        .execute_ddl(FlightSqlDdlRequest::new(
            "ALTER TABLE arrowcol.integration.catalog_control_plane \
             ADD COLUMNS (optional_note VARCHAR)",
        ))
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
    Ok(())
}
