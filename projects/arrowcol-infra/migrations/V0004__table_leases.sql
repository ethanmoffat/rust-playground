:on error exit
:setvar MigrationVersion "0004"
:setvar MigrationDescription "table leases and fencing epochs"

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
            THROW 51004, 'Migration 0004 checksum does not match the recorded version.', 1;

        COMMIT TRANSACTION;
        PRINT N'Migration 0004 already applied';
        RETURN;
    END;

    CREATE TABLE [arrowcol].[table_fencing_epochs] (
        [table_id] uniqueidentifier NOT NULL,
        [last_fencing_epoch] bigint NOT NULL
            CONSTRAINT [DF_table_fencing_epochs_last_epoch] DEFAULT (0),
        [updated_at_utc] datetime2(7) NOT NULL
            CONSTRAINT [DF_table_fencing_epochs_updated_at] DEFAULT (SYSUTCDATETIME()),
        [row_version] rowversion NOT NULL,
        CONSTRAINT [PK_table_fencing_epochs]
            PRIMARY KEY CLUSTERED ([table_id]),
        CONSTRAINT [CK_table_fencing_epochs_nonnegative]
            CHECK ([last_fencing_epoch] >= 0)
    );

    CREATE TABLE [arrowcol].[table_leases] (
        [table_id] uniqueidentifier NOT NULL,
        [fencing_epoch] bigint NOT NULL,
        [lease_id] uniqueidentifier NOT NULL,
        [holder_instance_id] uniqueidentifier NOT NULL,
        [acquired_at_utc] datetime2(7) NOT NULL
            CONSTRAINT [DF_table_leases_acquired_at] DEFAULT (SYSUTCDATETIME()),
        [renewed_at_utc] datetime2(7) NOT NULL,
        [expires_at_utc] datetime2(7) NOT NULL,
        [released_at_utc] datetime2(7) NULL,
        [release_reason] varchar(32) NULL,
        [row_version] rowversion NOT NULL,
        CONSTRAINT [PK_table_leases]
            PRIMARY KEY CLUSTERED ([table_id], [fencing_epoch]),
        CONSTRAINT [UQ_table_leases_lease_id]
            UNIQUE NONCLUSTERED ([lease_id]),
        CONSTRAINT [FK_table_leases_epoch]
            FOREIGN KEY ([table_id])
            REFERENCES [arrowcol].[table_fencing_epochs] ([table_id]),
        CONSTRAINT [FK_table_leases_holder]
            FOREIGN KEY ([holder_instance_id])
            REFERENCES [arrowcol].[collector_instances] ([instance_id]),
        CONSTRAINT [CK_table_leases_epoch]
            CHECK ([fencing_epoch] > 0),
        CONSTRAINT [CK_table_leases_times]
            CHECK (
                [renewed_at_utc] >= [acquired_at_utc]
                AND [expires_at_utc] > [renewed_at_utc]
                AND ([released_at_utc] IS NULL OR [released_at_utc] >= [acquired_at_utc])
            ),
        CONSTRAINT [CK_table_leases_release_reason]
            CHECK (
                ([released_at_utc] IS NULL AND [release_reason] IS NULL)
                OR (
                    [released_at_utc] IS NOT NULL
                    AND [release_reason] IN (
                        'released',
                        'expired',
                        'superseded',
                        'instance_stopped',
                        'lost'
                    )
                )
            )
    );

    CREATE UNIQUE INDEX [UX_table_leases_one_active]
        ON [arrowcol].[table_leases] ([table_id])
        INCLUDE (
            [fencing_epoch],
            [lease_id],
            [holder_instance_id],
            [expires_at_utc]
        )
        WHERE [released_at_utc] IS NULL;

    CREATE INDEX [IX_table_leases_holder_active]
        ON [arrowcol].[table_leases] (
            [holder_instance_id],
            [expires_at_utc]
        )
        INCLUDE (
            [table_id],
            [fencing_epoch],
            [lease_id],
            [renewed_at_utc]
        )
        WHERE [released_at_utc] IS NULL;

    CREATE INDEX [IX_table_leases_expiry_active]
        ON [arrowcol].[table_leases] (
            [expires_at_utc],
            [table_id]
        )
        INCLUDE (
            [fencing_epoch],
            [lease_id],
            [holder_instance_id],
            [renewed_at_utc]
        )
        WHERE [released_at_utc] IS NULL;

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
    PRINT N'Applied migration 0004';
END TRY
BEGIN CATCH
    IF XACT_STATE() <> 0
        ROLLBACK TRANSACTION;
    THROW;
END CATCH;
GO
