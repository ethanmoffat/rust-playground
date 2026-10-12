:on error exit
:setvar MigrationVersion "0002"
:setvar MigrationDescription "collector instances and schema change sagas"

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
            THROW 51002, 'Migration 0002 checksum does not match the recorded version.', 1;

        COMMIT TRANSACTION;
        PRINT N'Migration 0002 already applied';
        RETURN;
    END;

    CREATE TABLE [arrowcol].[collector_instances] (
        [instance_id] uniqueidentifier NOT NULL,
        [instance_key] nvarchar(200) NOT NULL,
        [generation] bigint NOT NULL,
        [state] varchar(16) NOT NULL,
        [host_name] nvarchar(255) NULL,
        [build_version] nvarchar(64) NULL,
        [started_at_utc] datetime2(7) NOT NULL
            CONSTRAINT [DF_collector_instances_started_at] DEFAULT (SYSUTCDATETIME()),
        [heartbeat_at_utc] datetime2(7) NOT NULL,
        [lease_expires_at_utc] datetime2(7) NOT NULL,
        [stopped_at_utc] datetime2(7) NULL,
        [row_version] rowversion NOT NULL,
        CONSTRAINT [PK_collector_instances]
            PRIMARY KEY CLUSTERED ([instance_id]),
        CONSTRAINT [UQ_collector_instances_key_generation]
            UNIQUE NONCLUSTERED ([instance_key], [generation]),
        CONSTRAINT [CK_collector_instances_generation]
            CHECK ([generation] > 0),
        CONSTRAINT [CK_collector_instances_state]
            CHECK ([state] IN ('starting', 'active', 'draining', 'stopped', 'failed')),
        CONSTRAINT [CK_collector_instances_times]
            CHECK (
                [heartbeat_at_utc] >= [started_at_utc]
                AND [lease_expires_at_utc] >= [heartbeat_at_utc]
                AND ([stopped_at_utc] IS NULL OR [stopped_at_utc] >= [started_at_utc])
            )
    );

    CREATE UNIQUE INDEX [UX_collector_instances_active_key]
        ON [arrowcol].[collector_instances] ([instance_key])
        INCLUDE (
            [instance_id],
            [generation],
            [state],
            [heartbeat_at_utc],
            [lease_expires_at_utc]
        )
        WHERE [stopped_at_utc] IS NULL;

    CREATE INDEX [IX_collector_instances_active_lease]
        ON [arrowcol].[collector_instances] (
            [lease_expires_at_utc],
            [heartbeat_at_utc]
        )
        INCLUDE (
            [instance_id],
            [instance_key],
            [generation],
            [state],
            [build_version]
        )
        WHERE [stopped_at_utc] IS NULL;

    CREATE TABLE [arrowcol].[schema_change_sagas] (
        [saga_id] uniqueidentifier NOT NULL,
        [table_id] uniqueidentifier NOT NULL,
        [request_id] uniqueidentifier NOT NULL,
        [initiated_by_instance_id] uniqueidentifier NOT NULL,
        [from_schema_version] bigint NULL,
        [target_schema_version] bigint NOT NULL,
        [state] varchar(20) NOT NULL,
        [current_step] smallint NOT NULL
            CONSTRAINT [DF_schema_change_sagas_current_step] DEFAULT (0),
        [attempt_count] int NOT NULL
            CONSTRAINT [DF_schema_change_sagas_attempt_count] DEFAULT (0),
        [started_at_utc] datetime2(7) NOT NULL
            CONSTRAINT [DF_schema_change_sagas_started_at] DEFAULT (SYSUTCDATETIME()),
        [updated_at_utc] datetime2(7) NOT NULL
            CONSTRAINT [DF_schema_change_sagas_updated_at] DEFAULT (SYSUTCDATETIME()),
        [completed_at_utc] datetime2(7) NULL,
        [last_error_code] nvarchar(128) NULL,
        [last_error_message] nvarchar(2048) NULL,
        [row_version] rowversion NOT NULL,
        CONSTRAINT [PK_schema_change_sagas]
            PRIMARY KEY CLUSTERED ([saga_id]),
        CONSTRAINT [UQ_schema_change_sagas_request]
            UNIQUE NONCLUSTERED ([table_id], [request_id]),
        CONSTRAINT [FK_schema_change_sagas_collector]
            FOREIGN KEY ([initiated_by_instance_id])
            REFERENCES [arrowcol].[collector_instances] ([instance_id]),
        CONSTRAINT [CK_schema_change_sagas_versions]
            CHECK (
                [target_schema_version] > 0
                AND ([from_schema_version] IS NULL OR [from_schema_version] >= 0)
                AND ([from_schema_version] IS NULL OR [target_schema_version] > [from_schema_version])
            ),
        CONSTRAINT [CK_schema_change_sagas_state]
            CHECK ([state] IN (
                'requested',
                'validating',
                'applying',
                'committed',
                'compensating',
                'failed',
                'cancelled'
            )),
        CONSTRAINT [CK_schema_change_sagas_progress]
            CHECK ([current_step] >= 0 AND [attempt_count] >= 0),
        CONSTRAINT [CK_schema_change_sagas_times]
            CHECK (
                [updated_at_utc] >= [started_at_utc]
                AND ([completed_at_utc] IS NULL OR [completed_at_utc] >= [started_at_utc])
            )
    );

    CREATE INDEX [IX_schema_change_sagas_active]
        ON [arrowcol].[schema_change_sagas] (
            [table_id],
            [updated_at_utc],
            [saga_id]
        )
        INCLUDE (
            [request_id],
            [target_schema_version],
            [state],
            [current_step],
            [attempt_count],
            [initiated_by_instance_id]
        )
        WHERE [completed_at_utc] IS NULL;

    CREATE TABLE [arrowcol].[table_schema_versions] (
        [table_id] uniqueidentifier NOT NULL,
        [schema_version] bigint NOT NULL,
        [schema_sha256] binary(32) NOT NULL,
        [compatibility] varchar(16) NOT NULL,
        [state] varchar(16) NOT NULL,
        [saga_id] uniqueidentifier NOT NULL,
        [schema_uri] nvarchar(2048) NULL,
        [created_at_utc] datetime2(7) NOT NULL
            CONSTRAINT [DF_table_schema_versions_created_at] DEFAULT (SYSUTCDATETIME()),
        [activated_at_utc] datetime2(7) NULL,
        [superseded_at_utc] datetime2(7) NULL,
        CONSTRAINT [PK_table_schema_versions]
            PRIMARY KEY CLUSTERED ([table_id], [schema_version]),
        CONSTRAINT [UQ_table_schema_versions_hash]
            UNIQUE NONCLUSTERED ([table_id], [schema_sha256]),
        CONSTRAINT [FK_table_schema_versions_saga]
            FOREIGN KEY ([saga_id])
            REFERENCES [arrowcol].[schema_change_sagas] ([saga_id]),
        CONSTRAINT [CK_table_schema_versions_version]
            CHECK ([schema_version] > 0),
        CONSTRAINT [CK_table_schema_versions_compatibility]
            CHECK ([compatibility] IN ('exact', 'backward', 'forward', 'full', 'breaking')),
        CONSTRAINT [CK_table_schema_versions_state]
            CHECK ([state] IN ('pending', 'active', 'superseded', 'rejected')),
        CONSTRAINT [CK_table_schema_versions_times]
            CHECK (
                ([activated_at_utc] IS NULL OR [activated_at_utc] >= [created_at_utc])
                AND (
                    [superseded_at_utc] IS NULL
                    OR (
                        [activated_at_utc] IS NOT NULL
                        AND [superseded_at_utc] >= [activated_at_utc]
                    )
                )
            )
    );

    CREATE INDEX [IX_table_schema_versions_lookup]
        ON [arrowcol].[table_schema_versions] (
            [table_id],
            [state],
            [schema_version] DESC
        )
        INCLUDE (
            [schema_sha256],
            [compatibility],
            [saga_id],
            [schema_uri],
            [activated_at_utc]
        )
        WHERE [state] IN ('pending', 'active');

    CREATE TABLE [arrowcol].[table_schema_heads] (
        [table_id] uniqueidentifier NOT NULL,
        [current_schema_version] bigint NOT NULL,
        [current_saga_id] uniqueidentifier NULL,
        [updated_at_utc] datetime2(7) NOT NULL
            CONSTRAINT [DF_table_schema_heads_updated_at] DEFAULT (SYSUTCDATETIME()),
        [row_version] rowversion NOT NULL,
        CONSTRAINT [PK_table_schema_heads]
            PRIMARY KEY CLUSTERED ([table_id]),
        CONSTRAINT [FK_table_schema_heads_version]
            FOREIGN KEY ([table_id], [current_schema_version])
            REFERENCES [arrowcol].[table_schema_versions] ([table_id], [schema_version]),
        CONSTRAINT [FK_table_schema_heads_saga]
            FOREIGN KEY ([current_saga_id])
            REFERENCES [arrowcol].[schema_change_sagas] ([saga_id]),
        CONSTRAINT [CK_table_schema_heads_version]
            CHECK ([current_schema_version] > 0)
    );

    CREATE INDEX [IX_table_schema_heads_current_saga]
        ON [arrowcol].[table_schema_heads] ([current_saga_id])
        INCLUDE ([table_id], [current_schema_version], [updated_at_utc])
        WHERE [current_saga_id] IS NOT NULL;

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
    PRINT N'Applied migration 0002';
END TRY
BEGIN CATCH
    IF XACT_STATE() <> 0
        ROLLBACK TRANSACTION;
    THROW;
END CATCH;
GO
