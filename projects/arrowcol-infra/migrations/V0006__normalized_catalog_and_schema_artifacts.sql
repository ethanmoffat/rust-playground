:on error exit
:setvar MigrationVersion "0006"
:setvar MigrationDescription "normalized catalog identities fields and descriptor artifacts"

SET NOCOUNT ON;
SET XACT_ABORT ON;
SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
SET NUMERIC_ROUNDABORT OFF;

BEGIN TRY
    BEGIN TRANSACTION;

    DECLARE @lock_result int;
    EXEC @lock_result = sys.sp_getapplock
        @Resource = N'arrowcol.schema_migrations',
        @LockMode = N'Exclusive',
        @LockOwner = N'Transaction',
        @LockTimeout = 60000,
        @DbPrincipal = N'public';

    IF @lock_result < 0
        THROW 51000, 'Could not acquire the Arrowcol migration lock.', 1;

    DECLARE @existing_checksum char(64);
    SELECT @existing_checksum = [checksum_sha256]
    FROM [arrowcol].[schema_migrations] WITH (UPDLOCK, HOLDLOCK)
    WHERE [version] = N'$(MigrationVersion)';

    IF @existing_checksum IS NOT NULL
    BEGIN
        IF @existing_checksum <> '$(MIGRATION_CHECKSUM)'
            THROW 51006, 'Migration 0006 checksum does not match the recorded version.', 1;

        COMMIT TRANSACTION;
        PRINT N'Migration 0006 already applied';
        RETURN;
    END;

    CREATE TABLE [arrowcol].[catalog_tables] (
        [table_id] uniqueidentifier NOT NULL,
        [catalog_name] nvarchar(128) COLLATE Latin1_General_100_BIN2 NOT NULL,
        [schema_name] nvarchar(128) COLLATE Latin1_General_100_BIN2 NOT NULL,
        [table_name] nvarchar(128) COLLATE Latin1_General_100_BIN2 NOT NULL,
        [created_at_utc] datetime2(7) NOT NULL
            CONSTRAINT [DF_catalog_tables_created_at] DEFAULT (SYSUTCDATETIME()),
        [updated_at_utc] datetime2(7) NOT NULL
            CONSTRAINT [DF_catalog_tables_updated_at] DEFAULT (SYSUTCDATETIME()),
        [row_version] rowversion NOT NULL,
        CONSTRAINT [PK_catalog_tables]
            PRIMARY KEY CLUSTERED ([table_id]),
        CONSTRAINT [UQ_catalog_tables_identity]
            UNIQUE NONCLUSTERED ([catalog_name], [schema_name], [table_name]),
        CONSTRAINT [CK_catalog_tables_names]
            CHECK (
                LEN([catalog_name]) BETWEEN 1 AND 128
                AND LEN([schema_name]) BETWEEN 1 AND 128
                AND LEN([table_name]) BETWEEN 1 AND 128
            ),
        CONSTRAINT [CK_catalog_tables_times]
            CHECK ([updated_at_utc] >= [created_at_utc])
    );

    CREATE INDEX [IX_catalog_tables_lookup]
        ON [arrowcol].[catalog_tables] (
            [catalog_name],
            [schema_name],
            [table_name]
        )
        INCLUDE ([table_id], [updated_at_utc]);

    ALTER TABLE [arrowcol].[schema_change_sagas]
        ALTER COLUMN [initiated_by_instance_id] uniqueidentifier NULL;

    ALTER TABLE [arrowcol].[schema_change_sagas]
        ADD
            [request_sha256] binary(32) NULL,
            [operation_kind] varchar(16) NULL;

    EXEC(N'
        ALTER TABLE [arrowcol].[schema_change_sagas]
            ADD CONSTRAINT [CK_schema_change_sagas_operation_kind]
                CHECK (
                    [operation_kind] IS NULL
                    OR [operation_kind] IN (''create'', ''add_columns'')
                );

        CREATE UNIQUE INDEX [UX_schema_change_sagas_request_sha]
            ON [arrowcol].[schema_change_sagas] ([table_id], [request_sha256])
            INCLUDE (
                [saga_id],
                [request_id],
                [target_schema_version],
                [state],
                [current_step],
                [attempt_count]
            )
            WHERE [request_sha256] IS NOT NULL;

        CREATE UNIQUE INDEX [UX_schema_change_sagas_one_active]
            ON [arrowcol].[schema_change_sagas] ([table_id])
            INCLUDE (
                [saga_id],
                [request_sha256],
                [target_schema_version],
                [state],
                [current_step],
                [updated_at_utc]
            )
            WHERE [completed_at_utc] IS NULL;
    ');

    ALTER TABLE [arrowcol].[table_schema_versions]
        ADD
            [canonical_ddl] nvarchar(max) NULL,
            [schema_json] nvarchar(max) NULL;

    CREATE TABLE [arrowcol].[catalog_fields] (
        [table_id] uniqueidentifier NOT NULL,
        [schema_version] bigint NOT NULL,
        [field_id] uniqueidentifier NOT NULL,
        [parent_field_id] uniqueidentifier NULL,
        [field_path] nvarchar(1024) COLLATE Latin1_General_100_BIN2 NOT NULL,
        [field_name] nvarchar(128) COLLATE Latin1_General_100_BIN2 NOT NULL,
        [field_position] int NOT NULL,
        [field_number] int NOT NULL,
        [is_nullable] bit NOT NULL,
        [partition_ordinal] int NULL,
        [arrow_type] nvarchar(4000) NOT NULL,
        [datafusion_type] nvarchar(4000) NOT NULL,
        [hive_type] nvarchar(4000) NOT NULL,
        [protobuf_type] varchar(32) NOT NULL,
        [type_json] nvarchar(max) NOT NULL,
        CONSTRAINT [PK_catalog_fields]
            PRIMARY KEY CLUSTERED ([table_id], [schema_version], [field_id]),
        CONSTRAINT [FK_catalog_fields_schema_version]
            FOREIGN KEY ([table_id], [schema_version])
            REFERENCES [arrowcol].[table_schema_versions] ([table_id], [schema_version]),
        CONSTRAINT [CK_catalog_fields_values]
            CHECK (
                LEN([field_path]) BETWEEN 1 AND 1024
                AND LEN([field_name]) BETWEEN 1 AND 128
                AND [field_position] >= 0
                AND [field_number] > 0
                AND ([partition_ordinal] IS NULL OR [partition_ordinal] >= 0)
                AND ([partition_ordinal] IS NULL OR [parent_field_id] IS NULL)
            ),
        CONSTRAINT [CK_catalog_fields_protobuf_type]
            CHECK ([protobuf_type] IN (
                'bool',
                'int32',
                'int64',
                'uint32',
                'uint64',
                'float',
                'double',
                'string',
                'bytes',
                'message',
                'repeated'
            ))
    );

    CREATE UNIQUE INDEX [UX_catalog_fields_path]
        ON [arrowcol].[catalog_fields] (
            [table_id],
            [schema_version],
            [field_path]
        );

    CREATE UNIQUE INDEX [UX_catalog_fields_sibling_position]
        ON [arrowcol].[catalog_fields] (
            [table_id],
            [schema_version],
            [parent_field_id],
            [field_position]
        )
        INCLUDE ([field_id], [field_name], [field_number], [is_nullable]);

    CREATE UNIQUE INDEX [UX_catalog_fields_sibling_number]
        ON [arrowcol].[catalog_fields] (
            [table_id],
            [schema_version],
            [parent_field_id],
            [field_number]
        )
        INCLUDE ([field_id], [field_name], [field_position], [protobuf_type]);

    CREATE UNIQUE INDEX [UX_catalog_fields_partition_ordinal]
        ON [arrowcol].[catalog_fields] (
            [table_id],
            [schema_version],
            [partition_ordinal]
        )
        INCLUDE ([field_id], [field_name], [hive_type])
        WHERE [partition_ordinal] IS NOT NULL;

    CREATE INDEX [IX_catalog_fields_identity_history]
        ON [arrowcol].[catalog_fields] (
            [table_id],
            [field_id],
            [schema_version]
        )
        INCLUDE (
            [parent_field_id],
            [field_path],
            [field_name],
            [field_position],
            [field_number],
            [is_nullable],
            [partition_ordinal]
        );

    CREATE TABLE [arrowcol].[schema_artifact_chunks] (
        [table_id] uniqueidentifier NOT NULL,
        [schema_version] bigint NOT NULL,
        [artifact_kind] varchar(32) NOT NULL,
        [chunk_ordinal] int NOT NULL,
        [artifact_sha256] binary(32) NOT NULL,
        [artifact_size_bytes] bigint NOT NULL,
        [chunk_data] varbinary(8000) NOT NULL,
        [created_at_utc] datetime2(7) NOT NULL
            CONSTRAINT [DF_schema_artifact_chunks_created_at] DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT [PK_schema_artifact_chunks]
            PRIMARY KEY CLUSTERED (
                [table_id],
                [schema_version],
                [artifact_kind],
                [chunk_ordinal]
            ),
        CONSTRAINT [FK_schema_artifact_chunks_schema_version]
            FOREIGN KEY ([table_id], [schema_version])
            REFERENCES [arrowcol].[table_schema_versions] ([table_id], [schema_version]),
        CONSTRAINT [CK_schema_artifact_chunks_kind]
            CHECK ([artifact_kind] IN (
                'arrow_schema',
                'protobuf_descriptor',
                'manifest'
            )),
        CONSTRAINT [CK_schema_artifact_chunks_values]
            CHECK (
                [chunk_ordinal] >= 0
                AND [artifact_size_bytes] > 0
                AND DATALENGTH([chunk_data]) BETWEEN 1 AND 8000
            )
    );

    CREATE INDEX [IX_schema_artifact_chunks_version]
        ON [arrowcol].[schema_artifact_chunks] (
            [table_id],
            [schema_version],
            [artifact_kind],
            [chunk_ordinal]
        )
        INCLUDE ([artifact_sha256], [artifact_size_bytes], [created_at_utc]);

    INSERT INTO [arrowcol].[schema_migrations] (
        [version],
        [description],
        [checksum_sha256]
    )
    VALUES (
        N'$(MigrationVersion)',
        N'$(MigrationDescription)',
        '$(MIGRATION_CHECKSUM)'
    );

    COMMIT TRANSACTION;
    PRINT N'Applied migration 0006';
END TRY
BEGIN CATCH
    IF XACT_STATE() <> 0
        ROLLBACK TRANSACTION;
    THROW;
END CATCH;
GO
