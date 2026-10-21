:on error exit

SET NOCOUNT ON;
SET XACT_ABORT ON;
SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
SET NUMERIC_ROUNDABORT OFF;

IF DB_ID(N'arrowcol') IS NULL
    THROW 52000, 'arrowcol database does not exist.', 1;

IF DB_ID(N'hive_metastore') IS NULL
    THROW 52001, 'hive_metastore database does not exist.', 1;

IF DB_ID(N'arrowcol') = DB_ID(N'hive_metastore')
    THROW 52002, 'Application and Hive metadata must use separate databases.', 1;

IF EXISTS (
    SELECT 1
    FROM sys.databases
    WHERE [name] IN (N'arrowcol', N'hive_metastore')
      AND [state_desc] <> N'ONLINE'
)
    THROW 52003, 'One or more infrastructure databases are not online.', 1;

IF SUSER_ID(N'arrowcol_app') IS NULL
    THROW 52004, 'arrowcol_app login is missing.', 1;

IF SUSER_ID(N'arrowcol_migrator') IS NULL
    THROW 52005, 'arrowcol_migrator login is missing.', 1;

IF SUSER_ID(N'hive_metastore') IS NULL
    THROW 52006, 'hive_metastore login is missing.', 1;

USE [arrowcol];

DECLARE @expected_tables table ([name] sysname PRIMARY KEY);
INSERT INTO @expected_tables ([name])
VALUES
    (N'schema_migrations'),
    (N'collector_instances'),
    (N'schema_change_sagas'),
    (N'table_schema_versions'),
    (N'table_schema_heads'),
    (N'collector_pressure_latest'),
    (N'collector_pressure_history'),
    (N'table_fencing_epochs'),
    (N'table_leases'),
    (N'delivery_receipts'),
    (N'receipt_events'),
    (N'catalog_tables'),
    (N'catalog_fields'),
    (N'schema_artifact_chunks'),
    (N'routing_collectors'),
    (N'routing_pressure_latest'),
    (N'routing_pressure_history'),
    (N'routing_table_leases');

IF EXISTS (
    SELECT 1
    FROM @expected_tables AS expected
    LEFT JOIN sys.tables AS tables
        ON tables.[name] = expected.[name]
       AND tables.[schema_id] = SCHEMA_ID(N'arrowcol')
    WHERE tables.[object_id] IS NULL
)
    THROW 52010, 'One or more expected Arrowcol tables are missing.', 1;

IF (SELECT COUNT_BIG(*) FROM [arrowcol].[schema_migrations]) <> 9
    THROW 52011, 'Unexpected Arrowcol migration count.', 1;

IF EXISTS (
    SELECT 1
    FROM [arrowcol].[schema_migrations]
    WHERE LEN([checksum_sha256]) <> 64
       OR [checksum_sha256] LIKE '%[^0-9a-f]%'
)
    THROW 52012, 'An Arrowcol migration checksum is invalid.', 1;

IF EXISTS (
    SELECT 1
    FROM sys.foreign_keys AS foreign_key
    INNER JOIN sys.tables AS tables
        ON tables.[object_id] = foreign_key.[parent_object_id]
    WHERE tables.[schema_id] = SCHEMA_ID(N'arrowcol')
      AND (foreign_key.[is_disabled] = 1 OR foreign_key.[is_not_trusted] = 1)
)
    THROW 52013, 'An Arrowcol foreign key is disabled or untrusted.', 1;

IF EXISTS (
    SELECT 1
    FROM sys.check_constraints AS check_constraint
    INNER JOIN sys.tables AS tables
        ON tables.[object_id] = check_constraint.[parent_object_id]
    WHERE tables.[schema_id] = SCHEMA_ID(N'arrowcol')
      AND (check_constraint.[is_disabled] = 1 OR check_constraint.[is_not_trusted] = 1)
)
    THROW 52014, 'An Arrowcol check constraint is disabled or untrusted.', 1;

DECLARE @expected_indexes table (
    [name] sysname PRIMARY KEY,
    [must_be_filtered] bit NOT NULL,
    [minimum_included_columns] int NOT NULL
);

INSERT INTO @expected_indexes ([name], [must_be_filtered], [minimum_included_columns])
VALUES
    (N'UX_collector_instances_active_key', 1, 5),
    (N'IX_collector_instances_active_lease', 1, 5),
    (N'IX_schema_change_sagas_active', 1, 6),
    (N'IX_table_schema_versions_lookup', 1, 5),
    (N'IX_table_schema_heads_current_saga', 1, 3),
    (N'IX_catalog_tables_lookup', 0, 2),
    (N'UX_schema_change_sagas_request_sha', 1, 6),
    (N'UX_schema_change_sagas_one_active', 1, 6),
    (N'UX_catalog_fields_sibling_position', 0, 4),
    (N'UX_catalog_fields_sibling_number', 0, 4),
    (N'UX_catalog_fields_partition_ordinal', 1, 3),
    (N'IX_catalog_fields_identity_history', 0, 7),
    (N'IX_schema_artifact_chunks_version', 0, 3),
    (N'IX_collector_pressure_latest_hot', 1, 5),
    (N'IX_collector_pressure_history_table', 0, 6),
    (N'UX_table_leases_one_active', 1, 4),
    (N'IX_table_leases_holder_active', 1, 4),
    (N'IX_table_leases_expiry_active', 1, 4),
    (N'UX_table_leases_receipt_fence', 0, 0),
    (N'IX_delivery_receipts_owner_active', 1, 6),
    (N'IX_delivery_receipts_expiry_active', 1, 6),
    (N'IX_delivery_receipts_table_schema_queue', 0, 8),
    (N'IX_receipt_events_receipt_history', 0, 5),
    (N'UX_receipt_events_revision', 1, 0),
    (N'IX_routing_collectors_pool_ready', 0, 6),
    (N'IX_routing_collectors_instance_key', 0, 4),
    (N'IX_routing_pressure_latest_fresh', 0, 8),
    (N'IX_routing_pressure_history_collector', 0, 6),
    (N'IX_routing_table_leases_holder', 0, 6),
    (N'IX_routing_table_leases_expiry', 0, 7);

IF EXISTS (
    SELECT 1
    FROM @expected_indexes AS expected
    LEFT JOIN sys.indexes AS indexes
        ON indexes.[name] = expected.[name]
       AND OBJECT_SCHEMA_NAME(indexes.[object_id]) = N'arrowcol'
    OUTER APPLY (
        SELECT COUNT(*) AS included_columns
        FROM sys.index_columns AS index_columns
        WHERE index_columns.[object_id] = indexes.[object_id]
          AND index_columns.[index_id] = indexes.[index_id]
          AND index_columns.[is_included_column] = 1
    ) AS included
    WHERE indexes.[index_id] IS NULL
       OR (expected.[must_be_filtered] = 1 AND indexes.[has_filter] = 0)
       OR included.included_columns < expected.[minimum_included_columns]
)
    THROW 52015, 'A required hot-path index is missing or not covering/filtered as expected.', 1;

IF EXISTS (
    SELECT 1
    FROM sys.columns AS columns
    INNER JOIN sys.tables AS tables
        ON tables.[object_id] = columns.[object_id]
    INNER JOIN sys.types AS types
        ON types.[user_type_id] = columns.[user_type_id]
    WHERE tables.[schema_id] = SCHEMA_ID(N'arrowcol')
      AND (
          types.[name] = N'image'
          OR (types.[name] IN (N'varbinary', N'binary') AND columns.[max_length] = -1)
          OR columns.[is_filestream] = 1
      )
)
    THROW 52016, 'Arrowcol SQL tables must not contain Arrow payload byte storage.', 1;

IF EXISTS (
    SELECT 1
    FROM sys.columns AS columns
    INNER JOIN sys.tables AS tables
        ON tables.[object_id] = columns.[object_id]
    INNER JOIN sys.types AS types
        ON types.[user_type_id] = columns.[user_type_id]
    WHERE tables.[schema_id] = SCHEMA_ID(N'arrowcol')
      AND types.[name] IN (N'datetime', N'smalldatetime')
)
    THROW 52017, 'Arrowcol timestamps must use datetime2 UTC columns.', 1;

IF EXISTS (
    SELECT 1
    FROM sys.columns AS columns
    INNER JOIN sys.tables AS tables
        ON tables.[object_id] = columns.[object_id]
    INNER JOIN sys.types AS types
        ON types.[user_type_id] = columns.[user_type_id]
    WHERE tables.[schema_id] = SCHEMA_ID(N'arrowcol')
      AND types.[name] = N'datetime2'
      AND columns.[name] NOT LIKE '%[_]utc'
)
    THROW 52018, 'Every Arrowcol datetime2 column must be explicitly named as UTC.', 1;

IF COL_LENGTH(N'arrowcol.delivery_receipts', N'idempotency_key') IS NOT NULL
   OR COL_LENGTH(N'arrowcol.delivery_receipts', N'arrow_batch_sha256') IS NOT NULL
   OR EXISTS (
       SELECT 1
       FROM sys.indexes
       WHERE [object_id] = OBJECT_ID(N'arrowcol.delivery_receipts')
         AND [name] LIKE N'%idempotency%'
   )
    THROW 52021, 'Delivery receipts must not implement payload idempotency or deduplication.', 1;

IF NOT EXISTS (
    SELECT 1
    FROM sys.default_constraints AS defaults
    INNER JOIN sys.columns AS columns
        ON columns.[object_id] = defaults.[parent_object_id]
       AND columns.[column_id] = defaults.[parent_column_id]
    WHERE defaults.[parent_object_id] = OBJECT_ID(N'arrowcol.delivery_receipts')
      AND columns.[name] = N'receipt_id'
      AND OBJECT_DEFINITION(defaults.[object_id]) LIKE N'%newsequentialid%'
)
    THROW 52022, 'Receipt IDs must be generated by SQL Server.', 1;

IF NOT EXISTS (
    SELECT 1
    FROM sys.triggers
    WHERE [parent_id] = OBJECT_ID(N'arrowcol.receipt_events')
      AND [name] = N'TR_receipt_events_append_only'
      AND [is_disabled] = 0
)
    THROW 52023, 'Receipt events must have an enabled append-only trigger.', 1;

IF COL_LENGTH(N'arrowcol.collector_instances', N'durable_recovery_window_ms') IS NULL
   OR COL_LENGTH(N'arrowcol.receipt_events', N'previous_owner_instance_id') IS NULL
   OR OBJECT_ID(N'arrowcol.CK_collector_instances_recovery_window', N'C') IS NULL
   OR OBJECT_ID(N'arrowcol.CK_receipt_events_adoption', N'C') IS NULL
   OR NOT EXISTS (
       SELECT 1
       FROM sys.indexes
       WHERE [object_id] = OBJECT_ID(N'arrowcol.receipt_events')
         AND [name] = N'UX_receipt_events_revision'
         AND [is_unique] = 1
         AND [has_filter] = 1
   )
    THROW 52024, 'Durable receipt recovery windows or adoption event constraints are missing.', 1;

SELECT TOP (0)
    [receipt_id],
    [state],
    [revision]
FROM [arrowcol].[delivery_receipts] WITH (INDEX([IX_delivery_receipts_owner_active]))
WHERE [owner_instance_id] = NEWID()
  AND [state] IN ('accepted', 'buffered', 'flushing');

SELECT TOP (0)
    receipt.[receipt_id],
    receipt.[owner_instance_id],
    receipt.[revision],
    receipt.[state]
FROM [arrowcol].[delivery_receipts] AS receipt WITH (INDEX([IX_delivery_receipts_expiry_active]))
WHERE receipt.[receipt_lease_deadline_utc] <= SYSUTCDATETIME()
  AND receipt.[state] IN ('accepted', 'buffered', 'flushing')
  AND NOT EXISTS (
      SELECT 1
      FROM [arrowcol].[collector_instances] AS durable_owner
      WHERE durable_owner.[instance_id] = receipt.[owner_instance_id]
        AND durable_owner.[durable_recovery_window_ms] IS NOT NULL
        AND durable_owner.[lease_expires_at_utc] >
            DATEADD(millisecond, -durable_owner.[durable_recovery_window_ms], SYSUTCDATETIME())
  )
ORDER BY receipt.[receipt_lease_deadline_utc], receipt.[receipt_id];

IF USER_ID(N'arrowcol_app') IS NULL OR USER_ID(N'arrowcol_migrator') IS NULL
    THROW 52019, 'Arrowcol database users are missing.', 1;

IF NOT EXISTS (
    SELECT 1
    FROM sys.database_role_members AS drm
    INNER JOIN sys.database_principals AS role_principal
        ON role_principal.[principal_id] = drm.[role_principal_id]
    INNER JOIN sys.database_principals AS member_principal
        ON member_principal.[principal_id] = drm.[member_principal_id]
    WHERE role_principal.[name] = N'db_owner'
      AND member_principal.[name] = N'arrowcol_migrator'
)
    THROW 52020, 'Arrowcol migrator lacks its local schema-owner role.', 1;

USE [hive_metastore];

IF USER_ID(N'hive_metastore') IS NULL
    THROW 52030, 'Hive Metastore database user is missing.', 1;

IF OBJECT_ID(N'dbo.VERSION', N'U') IS NULL
    THROW 52031, 'Hive vendor VERSION table is missing.', 1;

IF NOT EXISTS (
    SELECT 1
    FROM [dbo].[VERSION]
    WHERE [VER_ID] = 1
      AND [SCHEMA_VERSION] = N'4.0.0'
)
    THROW 52032, 'Hive vendor schema version is not 4.0.0.', 1;

IF OBJECT_ID(N'dbo.DBS', N'U') IS NULL
   OR OBJECT_ID(N'dbo.TBLS', N'U') IS NULL
   OR OBJECT_ID(N'dbo.SDS', N'U') IS NULL
   OR OBJECT_ID(N'dbo.PARTITIONS', N'U') IS NULL
    THROW 52033, 'Core Hive vendor schema tables are missing.', 1;

SELECT
    DB_ID(N'arrowcol') AS [arrowcol_database_id],
    DB_ID(N'hive_metastore') AS [hive_metastore_database_id],
    (SELECT COUNT_BIG(*) FROM [arrowcol].[arrowcol].[schema_migrations])
        AS [application_migrations],
    (SELECT [SCHEMA_VERSION] FROM [hive_metastore].[dbo].[VERSION] WHERE [VER_ID] = 1)
        AS [hive_schema_version];

PRINT N'Arrowcol SQL Server and Hive Metastore schema verification passed';
GO
