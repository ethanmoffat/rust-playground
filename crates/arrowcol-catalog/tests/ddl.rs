use arrow_schema::{DataType, TimeUnit};
use arrowcol_catalog::{CatalogErrorCode, CatalogType, DdlCommand, DdlParser, ListKind};

fn synthetic_create() -> &'static str {
    r#"
        CREATE TABLE telemetry.prod.synthetic (
            sequence BIGINT UNSIGNED NOT NULL,
            service VARCHAR,
            observed_at TIMESTAMP(9) NOT NULL,
            trace_id FIXED_SIZE_BINARY(16),
            summary ARRAY<STRUCT<quantile DOUBLE, value DOUBLE>>
        )
        PARTITIONED BY (event_date DATE NOT NULL)
    "#
}

#[test]
fn parses_synthetic_create_with_exact_cross_system_types() -> anyhow::Result<()> {
    let DdlCommand::CreateTable(create) = DdlParser::default().parse(synthetic_create())? else {
        anyhow::bail!("expected create table");
    };
    assert_eq!(create.identity().to_string(), "telemetry.prod.synthetic");
    assert_eq!(create.fields().len(), 6);
    assert_eq!(create.fields()[0].mapping().arrow(), &CatalogType::UInt64);
    assert_eq!(create.fields()[0].mapping().hive(), "decimal(20,0)");
    assert_eq!(
        create.fields()[2].mapping().arrow(),
        &CatalogType::Timestamp {
            unit: arrowcol_catalog::TimeUnit::Nanosecond,
            timezone: None,
        }
    );
    assert_eq!(
        create.fields()[3].mapping().arrow(),
        &CatalogType::FixedSizeBinary { bytes: 16 }
    );
    assert!(matches!(
        create.fields()[4].mapping().arrow(),
        CatalogType::List {
            list_kind: ListKind::List,
            ..
        }
    ));
    assert_eq!(create.fields()[5].partition().map(|p| p.ordinal()), Some(0));

    let schema = arrow_schema::Schema::new(
        create
            .fields()
            .iter()
            .map(|field| field.to_arrow())
            .collect::<Result<Vec<_>, _>>()?,
    );
    assert_eq!(schema.field(0).data_type(), &DataType::UInt64);
    assert_eq!(
        schema.field(2).data_type(),
        &DataType::Timestamp(TimeUnit::Nanosecond, None)
    );
    Ok(())
}

#[test]
fn parses_plural_nullable_add_columns() -> anyhow::Result<()> {
    let command = DdlParser::default().parse(
        "ALTER TABLE telemetry.prod.synthetic ADD COLUMNS \
         (attributes MAP(VARCHAR, VARCHAR), status SMALLINT NULL)",
    )?;
    let DdlCommand::AddColumns(add) = command else {
        anyhow::bail!("expected add columns");
    };
    assert_eq!(add.fields().len(), 2);
    assert!(add.fields().iter().all(|field| field.is_nullable()));
    assert_eq!(
        add.canonical_ddl(),
        "ALTER TABLE \"telemetry\".\"prod\".\"synthetic\" ADD COLUMNS \
         (\"attributes\" MAP(VARCHAR, VARCHAR) NULL, \"status\" SMALLINT NULL)"
    );
    Ok(())
}

#[test]
fn rejects_every_breaking_or_unsupported_mutation() {
    let cases = [
        (
            "DROP TABLE telemetry.prod.synthetic",
            CatalogErrorCode::DdlDestructiveStatement,
        ),
        (
            "ALTER TABLE telemetry.prod.synthetic DROP COLUMN service",
            CatalogErrorCode::DdlBreakingMutation,
        ),
        (
            "ALTER TABLE telemetry.prod.synthetic RENAME COLUMN service TO service_name",
            CatalogErrorCode::DdlBreakingMutation,
        ),
        (
            "ALTER TABLE telemetry.prod.synthetic ALTER COLUMN service TYPE BIGINT",
            CatalogErrorCode::DdlBreakingMutation,
        ),
        (
            "ALTER TABLE telemetry.prod.synthetic ALTER COLUMN service SET NOT NULL",
            CatalogErrorCode::DdlBreakingMutation,
        ),
        (
            "ALTER TABLE telemetry.prod.synthetic ADD COLUMN status VARCHAR FIRST",
            CatalogErrorCode::DdlFieldReorder,
        ),
        (
            "ALTER TABLE telemetry.prod.synthetic ADD PARTITION (event_date = '2026-01-01')",
            CatalogErrorCode::DdlPartitionMutation,
        ),
        (
            "ALTER TABLE telemetry.prod.synthetic ADD COLUMN status VARCHAR NOT NULL",
            CatalogErrorCode::DdlNonNullableAppend,
        ),
        (
            "CREATE TABLE telemetry.prod.lossy (half FLOAT16)",
            CatalogErrorCode::TypeMappingUnsupported,
        ),
        ("SELECT 1", CatalogErrorCode::DdlUnsupportedStatement),
    ];
    for (ddl, expected) in cases {
        let Err(error) = DdlParser::default().parse(ddl) else {
            panic!("mutation must be rejected: {ddl}");
        };
        assert_eq!(error.code(), expected, "DDL: {ddl}; error: {error}");
    }
}

#[test]
fn rejects_partition_changes_and_nonprimitive_partition_types() {
    let Err(error) = DdlParser::default().parse(
        "CREATE TABLE telemetry.prod.bad_partition (id INT) \
         PARTITIONED BY (labels ARRAY<VARCHAR>)",
    ) else {
        panic!("nested partition must fail");
    };
    assert_eq!(error.code(), CatalogErrorCode::TypeMappingUnsupported);
}

#[test]
fn rejects_lossy_lengths_and_decimal_defaults() {
    for ddl in [
        "CREATE TABLE telemetry.prod.lossy (name VARCHAR(10))",
        "CREATE TABLE telemetry.prod.lossy (amount DECIMAL)",
        "CREATE TABLE telemetry.prod.lossy (amount DECIMAL(20))",
        "CREATE TABLE telemetry.prod.lossy (bits BINARY(16))",
    ] {
        let Err(error) = DdlParser::default().parse(ddl) else {
            panic!("lossy mapping must fail closed: {ddl}");
        };
        assert_eq!(error.code(), CatalogErrorCode::TypeMappingUnsupported);
    }
}
