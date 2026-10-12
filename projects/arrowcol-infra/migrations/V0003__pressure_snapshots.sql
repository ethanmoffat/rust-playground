:on error exit
:setvar MigrationVersion "0003"
:setvar MigrationDescription "latest and historical collector pressure snapshots"

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
            THROW 51003, 'Migration 0003 checksum does not match the recorded version.', 1;

        COMMIT TRANSACTION;
        PRINT N'Migration 0003 already applied';
        RETURN;
    END;

    CREATE TABLE [arrowcol].[collector_pressure_latest] (
        [collector_instance_id] uniqueidentifier NOT NULL,
        [table_id] uniqueidentifier NOT NULL,
        [observed_at_utc] datetime2(7) NOT NULL,
        [queue_depth] bigint NOT NULL,
        [queued_bytes] bigint NOT NULL,
        [in_flight_deliveries] int NOT NULL,
        [rejected_total] bigint NOT NULL,
        [pressure_level] tinyint NOT NULL,
        [oldest_queued_at_utc] datetime2(7) NULL,
        [row_version] rowversion NOT NULL,
        CONSTRAINT [PK_collector_pressure_latest]
            PRIMARY KEY CLUSTERED ([collector_instance_id], [table_id]),
        CONSTRAINT [FK_collector_pressure_latest_collector]
            FOREIGN KEY ([collector_instance_id])
            REFERENCES [arrowcol].[collector_instances] ([instance_id]),
        CONSTRAINT [CK_collector_pressure_latest_values]
            CHECK (
                [queue_depth] >= 0
                AND [queued_bytes] >= 0
                AND [in_flight_deliveries] >= 0
                AND [rejected_total] >= 0
                AND [pressure_level] BETWEEN 0 AND 100
            ),
        CONSTRAINT [CK_collector_pressure_latest_oldest]
            CHECK (
                [oldest_queued_at_utc] IS NULL
                OR [oldest_queued_at_utc] <= [observed_at_utc]
            )
    );

    CREATE INDEX [IX_collector_pressure_latest_hot]
        ON [arrowcol].[collector_pressure_latest] (
            [table_id],
            [pressure_level] DESC,
            [observed_at_utc] DESC
        )
        INCLUDE (
            [collector_instance_id],
            [queue_depth],
            [queued_bytes],
            [in_flight_deliveries],
            [oldest_queued_at_utc]
        )
        WHERE [pressure_level] > 0;

    CREATE TABLE [arrowcol].[collector_pressure_history] (
        [snapshot_id] bigint IDENTITY(1, 1) NOT NULL,
        [collector_instance_id] uniqueidentifier NOT NULL,
        [table_id] uniqueidentifier NOT NULL,
        [observed_at_utc] datetime2(7) NOT NULL,
        [queue_depth] bigint NOT NULL,
        [queued_bytes] bigint NOT NULL,
        [in_flight_deliveries] int NOT NULL,
        [rejected_total] bigint NOT NULL,
        [pressure_level] tinyint NOT NULL,
        [oldest_queued_at_utc] datetime2(7) NULL,
        CONSTRAINT [PK_collector_pressure_history]
            PRIMARY KEY CLUSTERED ([snapshot_id]),
        CONSTRAINT [FK_collector_pressure_history_collector]
            FOREIGN KEY ([collector_instance_id])
            REFERENCES [arrowcol].[collector_instances] ([instance_id]),
        CONSTRAINT [CK_collector_pressure_history_values]
            CHECK (
                [queue_depth] >= 0
                AND [queued_bytes] >= 0
                AND [in_flight_deliveries] >= 0
                AND [rejected_total] >= 0
                AND [pressure_level] BETWEEN 0 AND 100
            ),
        CONSTRAINT [CK_collector_pressure_history_oldest]
            CHECK (
                [oldest_queued_at_utc] IS NULL
                OR [oldest_queued_at_utc] <= [observed_at_utc]
            )
    );

    CREATE INDEX [IX_collector_pressure_history_table]
        ON [arrowcol].[collector_pressure_history] (
            [table_id],
            [observed_at_utc] DESC,
            [snapshot_id] DESC
        )
        INCLUDE (
            [collector_instance_id],
            [queue_depth],
            [queued_bytes],
            [in_flight_deliveries],
            [pressure_level],
            [oldest_queued_at_utc]
        );

    CREATE INDEX [IX_collector_pressure_history_collector]
        ON [arrowcol].[collector_pressure_history] (
            [collector_instance_id],
            [observed_at_utc] DESC
        )
        INCLUDE (
            [table_id],
            [queue_depth],
            [in_flight_deliveries],
            [pressure_level]
        );

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
    PRINT N'Applied migration 0003';
END TRY
BEGIN CATCH
    IF XACT_STATE() <> 0
        ROLLBACK TRANSACTION;
    THROW;
END CATCH;
GO
