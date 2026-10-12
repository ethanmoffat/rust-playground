:on error exit
:setvar MigrationVersion "0007"
:setvar MigrationDescription "direct collector receipt durability and publication observations"

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
            THROW 51007, 'Migration 0007 checksum does not match the recorded version.', 1;

        COMMIT TRANSACTION;
        PRINT N'Migration 0007 already applied';
        RETURN;
    END;

    CREATE UNIQUE INDEX [UX_table_leases_receipt_fence]
        ON [arrowcol].[table_leases] (
            [table_id],
            [fencing_epoch],
            [lease_id],
            [holder_instance_id]
        );

    CREATE TABLE [arrowcol].[delivery_receipts_v2] (
        [receipt_id] uniqueidentifier NOT NULL
            CONSTRAINT [DF_delivery_receipts_v2_id] DEFAULT (NEWSEQUENTIALID()),
        [table_id] uniqueidentifier NOT NULL,
        [owner_instance_id] uniqueidentifier NOT NULL,
        [implementation_pool] varchar(16) NOT NULL,
        [schema_version] bigint NOT NULL,
        [schema_sha256] binary(32) NOT NULL,
        [lease_id] uniqueidentifier NOT NULL,
        [fencing_epoch] bigint NOT NULL,
        [state] varchar(24) NOT NULL,
        [revision] bigint NOT NULL,
        [row_count] bigint NOT NULL,
        [batch_count] bigint NOT NULL,
        [arrow_memory_bytes] bigint NOT NULL,
        [receipt_lease_deadline_utc] datetime2(7) NOT NULL,
        [heartbeat_at_utc] datetime2(7) NOT NULL
            CONSTRAINT [DF_delivery_receipts_v2_heartbeat] DEFAULT (SYSUTCDATETIME()),
        [created_at_utc] datetime2(7) NOT NULL
            CONSTRAINT [DF_delivery_receipts_v2_created] DEFAULT (SYSUTCDATETIME()),
        [updated_at_utc] datetime2(7) NOT NULL
            CONSTRAINT [DF_delivery_receipts_v2_updated] DEFAULT (SYSUTCDATETIME()),
        [failure_disposition] varchar(16) NULL,
        [error_code] varchar(64) NULL,
        [error_message] nvarchar(1024) NULL,
        [retry_after_ms] bigint NULL,
        [delta_version] bigint NULL,
        [data_files_added] bigint NULL,
        [rows_published] bigint NULL,
        [bytes_published] bigint NULL,
        [published_at_utc] datetime2(7) NULL,
        [row_version] rowversion NOT NULL,
        CONSTRAINT [PK_delivery_receipts_v2]
            PRIMARY KEY CLUSTERED ([receipt_id]),
        CONSTRAINT [FK_delivery_receipts_v2_owner]
            FOREIGN KEY ([owner_instance_id])
            REFERENCES [arrowcol].[collector_instances] ([instance_id]),
        CONSTRAINT [FK_delivery_receipts_v2_schema]
            FOREIGN KEY ([table_id], [schema_version])
            REFERENCES [arrowcol].[table_schema_versions] ([table_id], [schema_version]),
        CONSTRAINT [FK_delivery_receipts_v2_fence]
            FOREIGN KEY ([table_id], [fencing_epoch], [lease_id], [owner_instance_id])
            REFERENCES [arrowcol].[table_leases] (
                [table_id],
                [fencing_epoch],
                [lease_id],
                [holder_instance_id]
            ),
        CONSTRAINT [CK_delivery_receipts_v2_values]
            CHECK (
                [schema_version] > 0
                AND [fencing_epoch] > 0
                AND [revision] >= 0
                AND [row_count] >= 0
                AND [batch_count] > 0
                AND [arrow_memory_bytes] >= 0
                AND ([retry_after_ms] IS NULL OR [retry_after_ms] > 0)
                AND ([delta_version] IS NULL OR [delta_version] >= 0)
                AND ([data_files_added] IS NULL OR [data_files_added] > 0)
                AND ([rows_published] IS NULL OR [rows_published] >= 0)
                AND ([bytes_published] IS NULL OR [bytes_published] >= 0)
            ),
        CONSTRAINT [CK_delivery_receipts_v2_pool]
            CHECK ([implementation_pool] IN ('direct', 'dataflow')),
        CONSTRAINT [CK_delivery_receipts_v2_state]
            CHECK ([state] IN (
                'accepted',
                'buffered',
                'flushing',
                'published',
                'retryable_failure',
                'permanent_failure'
            )),
        CONSTRAINT [CK_delivery_receipts_v2_revision]
            CHECK (
                ([state] = 'accepted' AND [revision] = 0)
                OR ([state] = 'buffered' AND [revision] = 1)
                OR ([state] = 'flushing' AND [revision] = 2)
                OR ([state] = 'published' AND [revision] = 3)
                OR ([state] IN ('retryable_failure', 'permanent_failure') AND [revision] BETWEEN 1 AND 3)
            ),
        CONSTRAINT [CK_delivery_receipts_v2_times]
            CHECK (
                [updated_at_utc] >= [created_at_utc]
                AND [heartbeat_at_utc] >= [created_at_utc]
                AND (
                    [state] NOT IN ('accepted', 'buffered', 'flushing')
                    OR [receipt_lease_deadline_utc] >= [heartbeat_at_utc]
                )
                AND ([published_at_utc] IS NULL OR [published_at_utc] >= [created_at_utc])
            ),
        CONSTRAINT [CK_delivery_receipts_v2_failure]
            CHECK (
                (
                    [state] IN ('retryable_failure', 'permanent_failure')
                    AND [failure_disposition] = CASE
                        WHEN [state] = 'retryable_failure' THEN 'retryable'
                        ELSE 'permanent'
                    END
                    AND [error_code] IS NOT NULL
                    AND [error_message] IS NOT NULL
                )
                OR (
                    [state] NOT IN ('retryable_failure', 'permanent_failure')
                    AND [failure_disposition] IS NULL
                    AND [error_code] IS NULL
                    AND [error_message] IS NULL
                    AND [retry_after_ms] IS NULL
                )
            ),
        CONSTRAINT [CK_delivery_receipts_v2_publication]
            CHECK (
                (
                    [state] = 'published'
                    AND [delta_version] IS NOT NULL
                    AND [data_files_added] IS NOT NULL
                    AND [rows_published] IS NOT NULL
                    AND [bytes_published] IS NOT NULL
                    AND [published_at_utc] IS NOT NULL
                )
                OR (
                    [state] <> 'published'
                    AND [delta_version] IS NULL
                    AND [data_files_added] IS NULL
                    AND [rows_published] IS NULL
                    AND [bytes_published] IS NULL
                    AND [published_at_utc] IS NULL
                )
            )
    );

    CREATE INDEX [IX_delivery_receipts_owner_active]
        ON [arrowcol].[delivery_receipts_v2] (
            [owner_instance_id],
            [state],
            [receipt_lease_deadline_utc],
            [receipt_id]
        )
        INCLUDE (
            [table_id],
            [schema_version],
            [lease_id],
            [fencing_epoch],
            [revision],
            [heartbeat_at_utc]
        )
        WHERE [state] IN ('accepted', 'buffered', 'flushing');

    CREATE INDEX [IX_delivery_receipts_expiry_active]
        ON [arrowcol].[delivery_receipts_v2] (
            [receipt_lease_deadline_utc],
            [receipt_id]
        )
        INCLUDE (
            [owner_instance_id],
            [table_id],
            [state],
            [revision],
            [lease_id],
            [fencing_epoch]
        )
        WHERE [state] IN ('accepted', 'buffered', 'flushing');

    CREATE INDEX [IX_delivery_receipts_table_schema_queue]
        ON [arrowcol].[delivery_receipts_v2] (
            [table_id],
            [schema_version],
            [created_at_utc],
            [receipt_id]
        )
        INCLUDE (
            [state],
            [owner_instance_id],
            [lease_id],
            [fencing_epoch],
            [revision],
            [row_count],
            [batch_count],
            [arrow_memory_bytes]
        );

    INSERT INTO [arrowcol].[delivery_receipts_v2] (
        [receipt_id],
        [table_id],
        [owner_instance_id],
        [implementation_pool],
        [schema_version],
        [schema_sha256],
        [lease_id],
        [fencing_epoch],
        [state],
        [revision],
        [row_count],
        [batch_count],
        [arrow_memory_bytes],
        [receipt_lease_deadline_utc],
        [heartbeat_at_utc],
        [created_at_utc],
        [updated_at_utc],
        [failure_disposition],
        [error_code],
        [error_message],
        [retry_after_ms]
    )
    SELECT
        receipt.[receipt_id],
        receipt.[table_id],
        receipt.[collector_instance_id],
        'direct',
        receipt.[schema_version],
        schema_version.[schema_sha256],
        lease.[lease_id],
        receipt.[fencing_epoch],
        CASE
            WHEN receipt.[status] IN ('terminal_failed', 'cancelled') THEN 'permanent_failure'
            ELSE 'retryable_failure'
        END,
        CASE
            WHEN receipt.[status] IN ('delivering', 'delivered') THEN 3
            ELSE 1
        END,
        0,
        1,
        receipt.[payload_size_bytes],
        CASE
            WHEN COALESCE(receipt.[claim_expires_at_utc], receipt.[available_at_utc]) >= receipt.[created_at_utc]
                THEN COALESCE(receipt.[claim_expires_at_utc], receipt.[available_at_utc])
            ELSE receipt.[created_at_utc]
        END,
        receipt.[created_at_utc],
        receipt.[created_at_utc],
        COALESCE(receipt.[delivered_at_utc], receipt.[created_at_utc]),
        CASE
            WHEN receipt.[status] IN ('terminal_failed', 'cancelled') THEN 'permanent'
            ELSE 'retryable'
        END,
        CASE
            WHEN receipt.[status] = 'delivered' THEN 'publication_outcome_unknown'
            WHEN receipt.[status] IN ('terminal_failed', 'cancelled') THEN 'schema_incompatible'
            ELSE 'publication_failed'
        END,
        CASE
            WHEN receipt.[status] = 'delivered'
                THEN N'legacy delivered receipt lacks a durable Delta publication observation'
            WHEN receipt.[status] IN ('terminal_failed', 'cancelled')
                THEN COALESCE(receipt.[last_error_message], N'legacy receipt ended permanently')
            ELSE N'legacy in-process delivery requires a new receipt after durability migration'
        END,
        NULL
    FROM [arrowcol].[delivery_receipts] AS receipt
    INNER JOIN [arrowcol].[table_schema_versions] AS schema_version
        ON schema_version.[table_id] = receipt.[table_id]
       AND schema_version.[schema_version] = receipt.[schema_version]
    INNER JOIN [arrowcol].[table_leases] AS lease
        ON lease.[table_id] = receipt.[table_id]
       AND lease.[fencing_epoch] = receipt.[fencing_epoch];

    CREATE TABLE [arrowcol].[receipt_events_v2] (
        [receipt_event_id] bigint IDENTITY(1, 1) NOT NULL,
        [receipt_id] uniqueidentifier NOT NULL,
        [revision] bigint NOT NULL,
        [from_state] varchar(24) NULL,
        [to_state] varchar(24) NOT NULL,
        [actor_instance_id] uniqueidentifier NOT NULL,
        [failure_disposition] varchar(16) NULL,
        [occurred_at_utc] datetime2(7) NOT NULL
            CONSTRAINT [DF_receipt_events_v2_occurred] DEFAULT (SYSUTCDATETIME()),
        [event_detail] nvarchar(1024) NULL,
        CONSTRAINT [PK_receipt_events_v2]
            PRIMARY KEY CLUSTERED ([receipt_event_id]),
        CONSTRAINT [FK_receipt_events_v2_receipt]
            FOREIGN KEY ([receipt_id])
            REFERENCES [arrowcol].[delivery_receipts_v2] ([receipt_id]),
        CONSTRAINT [FK_receipt_events_v2_actor]
            FOREIGN KEY ([actor_instance_id])
            REFERENCES [arrowcol].[collector_instances] ([instance_id]),
        CONSTRAINT [CK_receipt_events_v2_revision]
            CHECK ([revision] >= 0),
        CONSTRAINT [CK_receipt_events_v2_states]
            CHECK (
                ([from_state] IS NULL OR [from_state] IN (
                    'accepted',
                    'buffered',
                    'flushing',
                    'published',
                    'retryable_failure',
                    'permanent_failure'
                ))
                AND [to_state] IN (
                    'accepted',
                    'buffered',
                    'flushing',
                    'published',
                    'retryable_failure',
                    'permanent_failure'
                )
            ),
        CONSTRAINT [CK_receipt_events_v2_failure]
            CHECK (
                [failure_disposition] IS NULL
                OR [failure_disposition] IN ('retryable', 'permanent')
            )
    );

    CREATE UNIQUE INDEX [UX_receipt_events_revision]
        ON [arrowcol].[receipt_events_v2] ([receipt_id], [revision]);

    CREATE INDEX [IX_receipt_events_receipt_history]
        ON [arrowcol].[receipt_events_v2] (
            [receipt_id],
            [revision] DESC,
            [receipt_event_id] DESC
        )
        INCLUDE (
            [from_state],
            [to_state],
            [actor_instance_id],
            [failure_disposition],
            [occurred_at_utc]
        );

    INSERT INTO [arrowcol].[receipt_events_v2] (
        [receipt_id],
        [revision],
        [from_state],
        [to_state],
        [actor_instance_id],
        [occurred_at_utc],
        [event_detail]
    )
    SELECT
        [receipt_id],
        0,
        NULL,
        'accepted',
        [owner_instance_id],
        [created_at_utc],
        N'synthetic acceptance event created while migrating legacy receipt metadata'
    FROM [arrowcol].[delivery_receipts_v2];

    INSERT INTO [arrowcol].[receipt_events_v2] (
        [receipt_id],
        [revision],
        [from_state],
        [to_state],
        [actor_instance_id],
        [occurred_at_utc],
        [event_detail]
    )
    SELECT
        [receipt_id],
        1,
        'accepted',
        'buffered',
        [owner_instance_id],
        [created_at_utc],
        N'synthetic buffered event created while migrating legacy receipt metadata'
    FROM [arrowcol].[delivery_receipts_v2]
    WHERE [revision] = 3;

    INSERT INTO [arrowcol].[receipt_events_v2] (
        [receipt_id],
        [revision],
        [from_state],
        [to_state],
        [actor_instance_id],
        [occurred_at_utc],
        [event_detail]
    )
    SELECT
        [receipt_id],
        2,
        'buffered',
        'flushing',
        [owner_instance_id],
        [created_at_utc],
        N'synthetic flushing event created while migrating legacy receipt metadata'
    FROM [arrowcol].[delivery_receipts_v2]
    WHERE [revision] = 3;

    INSERT INTO [arrowcol].[receipt_events_v2] (
        [receipt_id],
        [revision],
        [from_state],
        [to_state],
        [actor_instance_id],
        [failure_disposition],
        [occurred_at_utc],
        [event_detail]
    )
    SELECT
        [receipt_id],
        [revision],
        CASE WHEN [revision] = 3 THEN 'flushing' ELSE 'accepted' END,
        [state],
        [owner_instance_id],
        [failure_disposition],
        [updated_at_utc],
        N'legacy receipt made non-purgeable during direct durability migration'
    FROM [arrowcol].[delivery_receipts_v2];

    DROP TABLE [arrowcol].[receipt_events];
    DROP TABLE [arrowcol].[delivery_receipts];

    EXEC sys.sp_rename
        @objname = N'arrowcol.delivery_receipts_v2',
        @newname = N'delivery_receipts',
        @objtype = N'OBJECT';

    EXEC sys.sp_rename
        @objname = N'arrowcol.receipt_events_v2',
        @newname = N'receipt_events',
        @objtype = N'OBJECT';

    EXEC(N'
        CREATE TRIGGER [arrowcol].[TR_receipt_events_append_only]
        ON [arrowcol].[receipt_events]
        AFTER UPDATE, DELETE
        AS
        BEGIN
            SET NOCOUNT ON;
            THROW 51070, ''Receipt events are append-only.'', 1;
        END;
    ');

    DENY UPDATE, DELETE ON [arrowcol].[receipt_events] TO [arrowcol_app];

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
    PRINT N'Applied migration 0007';
END TRY
BEGIN CATCH
    IF XACT_STATE() <> 0
        ROLLBACK TRANSACTION;
    THROW;
END CATCH;
GO
