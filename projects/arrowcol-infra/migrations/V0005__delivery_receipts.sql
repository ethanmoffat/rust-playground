:on error exit
:setvar MigrationVersion "0005"
:setvar MigrationDescription "delivery receipts and receipt events"

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
            THROW 51005, 'Migration 0005 checksum does not match the recorded version.', 1;

        COMMIT TRANSACTION;
        PRINT N'Migration 0005 already applied';
        RETURN;
    END;

    CREATE TABLE [arrowcol].[delivery_receipts] (
        [receipt_id] uniqueidentifier NOT NULL,
        [table_id] uniqueidentifier NOT NULL,
        [collector_instance_id] uniqueidentifier NOT NULL,
        [schema_version] bigint NOT NULL,
        [fencing_epoch] bigint NOT NULL,
        [idempotency_key] binary(32) NOT NULL,
        [arrow_batch_sha256] binary(32) NOT NULL,
        [payload_size_bytes] bigint NOT NULL,
        [destination_uri] nvarchar(2048) NOT NULL,
        [status] varchar(20) NOT NULL,
        [attempt_count] int NOT NULL
            CONSTRAINT [DF_delivery_receipts_attempt_count] DEFAULT (0),
        [created_at_utc] datetime2(7) NOT NULL
            CONSTRAINT [DF_delivery_receipts_created_at] DEFAULT (SYSUTCDATETIME()),
        [available_at_utc] datetime2(7) NOT NULL,
        [claim_expires_at_utc] datetime2(7) NULL,
        [delivered_at_utc] datetime2(7) NULL,
        [last_error_code] nvarchar(128) NULL,
        [last_error_message] nvarchar(2048) NULL,
        [row_version] rowversion NOT NULL,
        CONSTRAINT [PK_delivery_receipts]
            PRIMARY KEY CLUSTERED ([receipt_id]),
        CONSTRAINT [FK_delivery_receipts_collector]
            FOREIGN KEY ([collector_instance_id])
            REFERENCES [arrowcol].[collector_instances] ([instance_id]),
        CONSTRAINT [FK_delivery_receipts_schema]
            FOREIGN KEY ([table_id], [schema_version])
            REFERENCES [arrowcol].[table_schema_versions] ([table_id], [schema_version]),
        CONSTRAINT [FK_delivery_receipts_lease]
            FOREIGN KEY ([table_id], [fencing_epoch])
            REFERENCES [arrowcol].[table_leases] ([table_id], [fencing_epoch]),
        CONSTRAINT [CK_delivery_receipts_values]
            CHECK (
                [schema_version] > 0
                AND [fencing_epoch] > 0
                AND [payload_size_bytes] >= 0
                AND [attempt_count] >= 0
            ),
        CONSTRAINT [CK_delivery_receipts_status]
            CHECK ([status] IN (
                'pending',
                'delivering',
                'delivered',
                'terminal_failed',
                'cancelled'
            )),
        CONSTRAINT [CK_delivery_receipts_claim]
            CHECK (
                ([status] = 'delivering' AND [claim_expires_at_utc] IS NOT NULL)
                OR ([status] <> 'delivering' AND [claim_expires_at_utc] IS NULL)
            ),
        CONSTRAINT [CK_delivery_receipts_delivery]
            CHECK (
                ([status] = 'delivered' AND [delivered_at_utc] IS NOT NULL)
                OR ([status] <> 'delivered' AND [delivered_at_utc] IS NULL)
            ),
        CONSTRAINT [CK_delivery_receipts_times]
            CHECK (
                [available_at_utc] >= [created_at_utc]
                AND (
                    [claim_expires_at_utc] IS NULL
                    OR [claim_expires_at_utc] > [created_at_utc]
                )
                AND (
                    [delivered_at_utc] IS NULL
                    OR [delivered_at_utc] >= [created_at_utc]
                )
            )
    );

    CREATE UNIQUE INDEX [UX_delivery_receipts_idempotency]
        ON [arrowcol].[delivery_receipts] ([table_id], [idempotency_key])
        INCLUDE (
            [receipt_id],
            [status],
            [schema_version],
            [fencing_epoch],
            [destination_uri]
        );

    CREATE INDEX [IX_delivery_receipts_ready]
        ON [arrowcol].[delivery_receipts] (
            [available_at_utc],
            [created_at_utc],
            [receipt_id]
        )
        INCLUDE (
            [table_id],
            [collector_instance_id],
            [schema_version],
            [fencing_epoch],
            [attempt_count],
            [destination_uri],
            [arrow_batch_sha256],
            [payload_size_bytes]
        )
        WHERE [status] = 'pending';

    CREATE INDEX [IX_delivery_receipts_inflight]
        ON [arrowcol].[delivery_receipts] (
            [claim_expires_at_utc],
            [receipt_id]
        )
        INCLUDE (
            [table_id],
            [collector_instance_id],
            [schema_version],
            [fencing_epoch],
            [attempt_count],
            [destination_uri]
        )
        WHERE [status] = 'delivering';

    CREATE INDEX [IX_delivery_receipts_instance_history]
        ON [arrowcol].[delivery_receipts] (
            [collector_instance_id],
            [created_at_utc] DESC,
            [receipt_id]
        )
        INCLUDE (
            [table_id],
            [status],
            [attempt_count],
            [delivered_at_utc],
            [last_error_code]
        );

    CREATE INDEX [IX_delivery_receipts_schema_lookup]
        ON [arrowcol].[delivery_receipts] (
            [table_id],
            [schema_version],
            [created_at_utc] DESC
        )
        INCLUDE (
            [receipt_id],
            [status],
            [fencing_epoch],
            [collector_instance_id],
            [destination_uri]
        );

    CREATE TABLE [arrowcol].[receipt_events] (
        [receipt_event_id] bigint IDENTITY(1, 1) NOT NULL,
        [receipt_id] uniqueidentifier NOT NULL,
        [sequence_number] int NOT NULL,
        [event_type] varchar(24) NOT NULL,
        [from_status] varchar(20) NULL,
        [to_status] varchar(20) NOT NULL,
        [attempt_number] int NOT NULL,
        [actor_instance_id] uniqueidentifier NULL,
        [occurred_at_utc] datetime2(7) NOT NULL
            CONSTRAINT [DF_receipt_events_occurred_at] DEFAULT (SYSUTCDATETIME()),
        [error_code] nvarchar(128) NULL,
        [event_detail] nvarchar(2048) NULL,
        CONSTRAINT [PK_receipt_events]
            PRIMARY KEY CLUSTERED ([receipt_event_id]),
        CONSTRAINT [FK_receipt_events_receipt]
            FOREIGN KEY ([receipt_id])
            REFERENCES [arrowcol].[delivery_receipts] ([receipt_id]),
        CONSTRAINT [FK_receipt_events_actor]
            FOREIGN KEY ([actor_instance_id])
            REFERENCES [arrowcol].[collector_instances] ([instance_id]),
        CONSTRAINT [CK_receipt_events_sequence]
            CHECK ([sequence_number] >= 0 AND [attempt_number] >= 0),
        CONSTRAINT [CK_receipt_events_type]
            CHECK ([event_type] IN (
                'created',
                'claimed',
                'attempt_failed',
                'delivered',
                'terminal_failed',
                'cancelled',
                'lease_lost'
            )),
        CONSTRAINT [CK_receipt_events_from_status]
            CHECK (
                [from_status] IS NULL
                OR [from_status] IN (
                    'pending',
                    'delivering',
                    'delivered',
                    'terminal_failed',
                    'cancelled'
                )
            ),
        CONSTRAINT [CK_receipt_events_to_status]
            CHECK ([to_status] IN (
                'pending',
                'delivering',
                'delivered',
                'terminal_failed',
                'cancelled'
            ))
    );

    CREATE UNIQUE INDEX [UX_receipt_events_sequence]
        ON [arrowcol].[receipt_events] ([receipt_id], [sequence_number]);

    CREATE INDEX [IX_receipt_events_receipt_history]
        ON [arrowcol].[receipt_events] (
            [receipt_id],
            [occurred_at_utc] DESC,
            [receipt_event_id] DESC
        )
        INCLUDE (
            [event_type],
            [from_status],
            [to_status],
            [attempt_number],
            [actor_instance_id],
            [error_code]
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
    PRINT N'Applied migration 0005';
END TRY
BEGIN CATCH
    IF XACT_STATE() <> 0
        ROLLBACK TRANSACTION;
    THROW;
END CATCH;
GO
