:on error exit
:setvar MigrationVersion "0009"
:setvar MigrationDescription "durable receipt recovery windows and adoption events"

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
            THROW 51009, 'Migration 0009 checksum does not match the recorded version.', 1;

        COMMIT TRANSACTION;
        PRINT N'Migration 0009 already applied';
        RETURN;
    END;

    -- A durable-buffer owner keeps its active receipts out of expiry
    -- reconciliation while its owner lease is unexpired and for this many
    -- milliseconds afterwards, so a same-key successor can adopt them.
    -- NULL preserves receipt-deadline reconciliation.
    ALTER TABLE [arrowcol].[collector_instances]
        ADD [durable_recovery_window_ms] int NULL
            CONSTRAINT [CK_collector_instances_recovery_window]
            CHECK (
                [durable_recovery_window_ms] IS NULL
                OR [durable_recovery_window_ms] BETWEEN 0 AND 86400000
            );

    -- Adoption rebinds owner, table lease, and deadline without changing the
    -- receipt state or revision; its event records the previous owner.
    -- SQL Server binds CHECK expressions when the batch compiles, so the
    -- column and the constraint that references it share one statement.
    ALTER TABLE [arrowcol].[receipt_events]
        ADD [previous_owner_instance_id] uniqueidentifier NULL
                CONSTRAINT [FK_receipt_events_previous_owner]
                REFERENCES [arrowcol].[collector_instances] ([instance_id]),
            CONSTRAINT [CK_receipt_events_adoption]
                CHECK (
                    (
                        [to_state] = 'adopted'
                        AND [from_state] IS NOT NULL
                        AND [from_state] IN ('accepted', 'buffered', 'flushing')
                        AND [previous_owner_instance_id] IS NOT NULL
                        AND [failure_disposition] IS NULL
                    )
                    OR (
                        [to_state] <> 'adopted'
                        AND [previous_owner_instance_id] IS NULL
                    )
                );

    ALTER TABLE [arrowcol].[receipt_events]
        DROP CONSTRAINT [CK_receipt_events_v2_states];

    ALTER TABLE [arrowcol].[receipt_events]
        ADD CONSTRAINT [CK_receipt_events_states]
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
                    'permanent_failure',
                    'adopted'
                )
            );

    -- Transition events stay unique per receipt revision; adoption events
    -- repeat the unchanged revision and are ordered by receipt_event_id.
    DROP INDEX [UX_receipt_events_revision] ON [arrowcol].[receipt_events];

    CREATE UNIQUE INDEX [UX_receipt_events_revision]
        ON [arrowcol].[receipt_events] ([receipt_id], [revision])
        WHERE [to_state] <> 'adopted';

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
    PRINT N'Applied migration 0009';
END TRY
BEGIN CATCH
    IF XACT_STATE() <> 0
        ROLLBACK TRANSACTION;
    THROW;
END CATCH;
GO
