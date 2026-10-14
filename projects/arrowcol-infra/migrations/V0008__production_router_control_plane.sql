:on error exit
:setvar MigrationVersion "0008"
:setvar MigrationDescription "production router registrations pressure and fenced leases"

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
            THROW 51008, 'Migration 0008 checksum does not match the recorded version.', 1;

        COMMIT TRANSACTION;
        PRINT N'Migration 0008 already applied';
        RETURN;
    END;

    CREATE TABLE [arrowcol].[routing_collectors] (
        [instance_id] uniqueidentifier NOT NULL,
        [instance_key] nvarchar(200) NOT NULL,
        [implementation_pool] varchar(16) NOT NULL,
        [flight_endpoint] varchar(64) NOT NULL,
        [catalog_capable] bit NOT NULL,
        [max_concurrent_streams] bigint NOT NULL,
        [state] varchar(16) NOT NULL,
        [started_at_utc] datetime2(7) NOT NULL,
        [heartbeat_at_utc] datetime2(7) NOT NULL,
        [lease_expires_at_utc] datetime2(7) NOT NULL,
        [row_version] rowversion NOT NULL,
        CONSTRAINT [PK_routing_collectors]
            PRIMARY KEY CLUSTERED ([instance_id]),
        CONSTRAINT [CK_routing_collectors_pool]
            CHECK ([implementation_pool] IN ('direct', 'dataflow')),
        CONSTRAINT [CK_routing_collectors_state]
            CHECK ([state] IN ('ready', 'draining', 'stopped', 'failed')),
        CONSTRAINT [CK_routing_collectors_streams]
            CHECK ([max_concurrent_streams] > 0),
        CONSTRAINT [CK_routing_collectors_endpoint]
            CHECK (
                LEN([flight_endpoint]) BETWEEN 3 AND 64
            ),
        CONSTRAINT [CK_routing_collectors_times]
            CHECK (
                [heartbeat_at_utc] >= [started_at_utc]
                AND [lease_expires_at_utc] > [heartbeat_at_utc]
            )
    );

    CREATE INDEX [IX_routing_collectors_pool_ready]
        ON [arrowcol].[routing_collectors] (
            [implementation_pool],
            [state],
            [lease_expires_at_utc]
        )
        INCLUDE (
            [instance_id],
            [flight_endpoint],
            [catalog_capable],
            [max_concurrent_streams],
            [started_at_utc],
            [heartbeat_at_utc]
        );

    CREATE INDEX [IX_routing_collectors_instance_key]
        ON [arrowcol].[routing_collectors] (
            [instance_key],
            [heartbeat_at_utc] DESC
        )
        INCLUDE (
            [instance_id],
            [implementation_pool],
            [state],
            [lease_expires_at_utc]
        );

    CREATE TABLE [arrowcol].[routing_pressure_latest] (
        [collector_instance_id] uniqueidentifier NOT NULL,
        [observed_at_utc] datetime2(7) NOT NULL,
        [cpu_utilization_bps] smallint NOT NULL,
        [rss_bytes] bigint NOT NULL,
        [rss_limit_bytes] bigint NOT NULL,
        [active_streams] bigint NOT NULL,
        [active_stream_limit] bigint NOT NULL,
        [received_bytes] bigint NOT NULL,
        [received_rows] bigint NOT NULL,
        [received_batches] bigint NOT NULL,
        [buffered_bytes] bigint NOT NULL,
        [buffered_bytes_limit] bigint NOT NULL,
        [buffered_rows] bigint NOT NULL,
        [buffered_rows_limit] bigint NOT NULL,
        [buffered_batches] bigint NOT NULL,
        [buffered_batches_limit] bigint NOT NULL,
        [available_permits] bigint NOT NULL,
        [permit_limit] bigint NOT NULL,
        [pending_commits] bigint NOT NULL,
        [pending_commit_limit] bigint NOT NULL,
        [oldest_buffer_age_ms] bigint NOT NULL,
        [delta_pressure_bps] smallint NOT NULL,
        [quiver_pressure_bps] smallint NOT NULL,
        [dfe_pressure_bps] smallint NOT NULL,
        [delta_state] varchar(16) NOT NULL,
        [quiver_state] varchar(16) NOT NULL,
        [dfe_state] varchar(16) NOT NULL,
        [accepting_deliveries] bit NOT NULL,
        [table_pressure_json] nvarchar(max) NOT NULL,
        [row_version] rowversion NOT NULL,
        CONSTRAINT [PK_routing_pressure_latest]
            PRIMARY KEY CLUSTERED ([collector_instance_id]),
        CONSTRAINT [FK_routing_pressure_latest_collector]
            FOREIGN KEY ([collector_instance_id])
            REFERENCES [arrowcol].[routing_collectors] ([instance_id]),
        CONSTRAINT [CK_routing_pressure_latest_nonnegative]
            CHECK (
                [rss_bytes] >= 0
                AND [rss_limit_bytes] > 0
                AND [rss_bytes] <= [rss_limit_bytes]
                AND [active_streams] >= 0
                AND [active_stream_limit] > 0
                AND [active_streams] <= [active_stream_limit]
                AND [received_bytes] >= 0
                AND [received_rows] >= 0
                AND [received_batches] >= 0
                AND [buffered_bytes] >= 0
                AND [buffered_bytes_limit] > 0
                AND [buffered_bytes] <= [buffered_bytes_limit]
                AND [buffered_rows] >= 0
                AND [buffered_rows_limit] > 0
                AND [buffered_rows] <= [buffered_rows_limit]
                AND [buffered_batches] >= 0
                AND [buffered_batches_limit] > 0
                AND [buffered_batches] <= [buffered_batches_limit]
                AND [available_permits] >= 0
                AND [permit_limit] > 0
                AND [available_permits] <= [permit_limit]
                AND [pending_commits] >= 0
                AND [pending_commit_limit] > 0
                AND [pending_commits] <= [pending_commit_limit]
                AND [oldest_buffer_age_ms] >= 0
            ),
        CONSTRAINT [CK_routing_pressure_latest_bps]
            CHECK (
                [cpu_utilization_bps] BETWEEN 0 AND 10000
                AND [delta_pressure_bps] BETWEEN 0 AND 10000
                AND [quiver_pressure_bps] BETWEEN 0 AND 10000
                AND [dfe_pressure_bps] BETWEEN 0 AND 10000
            ),
        CONSTRAINT [CK_routing_pressure_latest_states]
            CHECK (
                [delta_state] IN ('ready', 'degraded', 'blocked', 'disabled')
                AND [quiver_state] IN ('ready', 'degraded', 'blocked', 'disabled')
                AND [dfe_state] IN ('ready', 'degraded', 'blocked', 'disabled')
            ),
        CONSTRAINT [CK_routing_pressure_latest_json]
            CHECK (ISJSON([table_pressure_json]) = 1)
    );

    CREATE INDEX [IX_routing_pressure_latest_fresh]
        ON [arrowcol].[routing_pressure_latest] (
            [observed_at_utc] DESC,
            [accepting_deliveries]
        )
        INCLUDE (
            [collector_instance_id],
            [cpu_utilization_bps],
            [rss_bytes],
            [rss_limit_bytes],
            [active_streams],
            [active_stream_limit],
            [pending_commits],
            [pending_commit_limit]
        );

    CREATE TABLE [arrowcol].[routing_pressure_history] (
        [snapshot_id] bigint IDENTITY(1, 1) NOT NULL,
        [collector_instance_id] uniqueidentifier NOT NULL,
        [observed_at_utc] datetime2(7) NOT NULL,
        [cpu_utilization_bps] smallint NOT NULL,
        [rss_bytes] bigint NOT NULL,
        [rss_limit_bytes] bigint NOT NULL,
        [active_streams] bigint NOT NULL,
        [active_stream_limit] bigint NOT NULL,
        [received_bytes] bigint NOT NULL,
        [received_rows] bigint NOT NULL,
        [received_batches] bigint NOT NULL,
        [buffered_bytes] bigint NOT NULL,
        [buffered_bytes_limit] bigint NOT NULL,
        [buffered_rows] bigint NOT NULL,
        [buffered_rows_limit] bigint NOT NULL,
        [buffered_batches] bigint NOT NULL,
        [buffered_batches_limit] bigint NOT NULL,
        [available_permits] bigint NOT NULL,
        [permit_limit] bigint NOT NULL,
        [pending_commits] bigint NOT NULL,
        [pending_commit_limit] bigint NOT NULL,
        [oldest_buffer_age_ms] bigint NOT NULL,
        [delta_pressure_bps] smallint NOT NULL,
        [quiver_pressure_bps] smallint NOT NULL,
        [dfe_pressure_bps] smallint NOT NULL,
        [delta_state] varchar(16) NOT NULL,
        [quiver_state] varchar(16) NOT NULL,
        [dfe_state] varchar(16) NOT NULL,
        [accepting_deliveries] bit NOT NULL,
        [table_pressure_json] nvarchar(max) NOT NULL,
        CONSTRAINT [PK_routing_pressure_history]
            PRIMARY KEY CLUSTERED ([snapshot_id]),
        CONSTRAINT [FK_routing_pressure_history_collector]
            FOREIGN KEY ([collector_instance_id])
            REFERENCES [arrowcol].[routing_collectors] ([instance_id]),
        CONSTRAINT [CK_routing_pressure_history_nonnegative]
            CHECK (
                [rss_bytes] >= 0
                AND [rss_limit_bytes] > 0
                AND [rss_bytes] <= [rss_limit_bytes]
                AND [active_streams] >= 0
                AND [active_stream_limit] > 0
                AND [active_streams] <= [active_stream_limit]
                AND [received_bytes] >= 0
                AND [received_rows] >= 0
                AND [received_batches] >= 0
                AND [buffered_bytes] >= 0
                AND [buffered_bytes_limit] > 0
                AND [buffered_bytes] <= [buffered_bytes_limit]
                AND [buffered_rows] >= 0
                AND [buffered_rows_limit] > 0
                AND [buffered_rows] <= [buffered_rows_limit]
                AND [buffered_batches] >= 0
                AND [buffered_batches_limit] > 0
                AND [buffered_batches] <= [buffered_batches_limit]
                AND [available_permits] >= 0
                AND [permit_limit] > 0
                AND [available_permits] <= [permit_limit]
                AND [pending_commits] >= 0
                AND [pending_commit_limit] > 0
                AND [pending_commits] <= [pending_commit_limit]
                AND [oldest_buffer_age_ms] >= 0
            ),
        CONSTRAINT [CK_routing_pressure_history_bps]
            CHECK (
                [cpu_utilization_bps] BETWEEN 0 AND 10000
                AND [delta_pressure_bps] BETWEEN 0 AND 10000
                AND [quiver_pressure_bps] BETWEEN 0 AND 10000
                AND [dfe_pressure_bps] BETWEEN 0 AND 10000
            ),
        CONSTRAINT [CK_routing_pressure_history_states]
            CHECK (
                [delta_state] IN ('ready', 'degraded', 'blocked', 'disabled')
                AND [quiver_state] IN ('ready', 'degraded', 'blocked', 'disabled')
                AND [dfe_state] IN ('ready', 'degraded', 'blocked', 'disabled')
            ),
        CONSTRAINT [CK_routing_pressure_history_json]
            CHECK (ISJSON([table_pressure_json]) = 1)
    );

    CREATE INDEX [IX_routing_pressure_history_collector]
        ON [arrowcol].[routing_pressure_history] (
            [collector_instance_id],
            [observed_at_utc] DESC,
            [snapshot_id] DESC
        )
        INCLUDE (
            [cpu_utilization_bps],
            [rss_bytes],
            [active_streams],
            [buffered_bytes],
            [pending_commits],
            [accepting_deliveries]
        );

    CREATE TABLE [arrowcol].[routing_table_leases] (
        [implementation_pool] varchar(16) NOT NULL,
        [catalog_name] nvarchar(128) NOT NULL,
        [schema_name] nvarchar(128) NOT NULL,
        [table_name] nvarchar(128) NOT NULL,
        [lease_id] uniqueidentifier NOT NULL,
        [holder_instance_id] uniqueidentifier NOT NULL,
        [fencing_epoch] bigint NOT NULL,
        [lease_version] bigint NOT NULL,
        [state] varchar(16) NOT NULL,
        [issued_at_utc] datetime2(7) NOT NULL,
        [renewed_at_utc] datetime2(7) NOT NULL,
        [expires_at_utc] datetime2(7) NOT NULL,
        [drain_started_at_utc] datetime2(7) NULL,
        [row_version] rowversion NOT NULL,
        CONSTRAINT [PK_routing_table_leases]
            PRIMARY KEY CLUSTERED (
                [implementation_pool],
                [catalog_name],
                [schema_name],
                [table_name]
            ),
        CONSTRAINT [UQ_routing_table_leases_id]
            UNIQUE NONCLUSTERED ([lease_id]),
        CONSTRAINT [FK_routing_table_leases_holder]
            FOREIGN KEY ([holder_instance_id])
            REFERENCES [arrowcol].[routing_collectors] ([instance_id]),
        CONSTRAINT [CK_routing_table_leases_pool]
            CHECK ([implementation_pool] IN ('direct', 'dataflow')),
        CONSTRAINT [CK_routing_table_leases_identity]
            CHECK (
                LEN([catalog_name]) BETWEEN 1 AND 128
                AND LEN([schema_name]) BETWEEN 1 AND 128
                AND LEN([table_name]) BETWEEN 1 AND 128
            ),
        CONSTRAINT [CK_routing_table_leases_versions]
            CHECK ([fencing_epoch] > 0 AND [lease_version] > 0),
        CONSTRAINT [CK_routing_table_leases_state]
            CHECK ([state] IN ('active', 'draining')),
        CONSTRAINT [CK_routing_table_leases_times]
            CHECK (
                [renewed_at_utc] >= [issued_at_utc]
                AND [expires_at_utc] > [renewed_at_utc]
                AND (
                    ([state] = 'active' AND [drain_started_at_utc] IS NULL)
                    OR (
                        [state] = 'draining'
                        AND [drain_started_at_utc] IS NOT NULL
                        AND [drain_started_at_utc] >= [issued_at_utc]
                        AND [drain_started_at_utc] <= [renewed_at_utc]
                    )
                )
            )
    );

    CREATE INDEX [IX_routing_table_leases_holder]
        ON [arrowcol].[routing_table_leases] (
            [holder_instance_id],
            [state],
            [expires_at_utc]
        )
        INCLUDE (
            [implementation_pool],
            [catalog_name],
            [schema_name],
            [table_name],
            [fencing_epoch],
            [lease_version]
        );

    CREATE INDEX [IX_routing_table_leases_expiry]
        ON [arrowcol].[routing_table_leases] (
            [expires_at_utc],
            [state]
        )
        INCLUDE (
            [implementation_pool],
            [catalog_name],
            [schema_name],
            [table_name],
            [holder_instance_id],
            [fencing_epoch],
            [lease_version]
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
    PRINT N'Applied migration 0008';
END TRY
BEGIN CATCH
    IF XACT_STATE() <> 0
        ROLLBACK TRANSACTION;
    THROW;
END CATCH;
GO
