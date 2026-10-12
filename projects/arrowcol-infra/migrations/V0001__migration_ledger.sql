:on error exit
:setvar MigrationVersion "0001"
:setvar MigrationDescription "migration ledger"

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

    IF SCHEMA_ID(N'arrowcol') IS NULL
        EXEC(N'CREATE SCHEMA [arrowcol] AUTHORIZATION [dbo];');

    IF OBJECT_ID(N'arrowcol.schema_migrations', N'U') IS NULL
    BEGIN
        CREATE TABLE [arrowcol].[schema_migrations] (
            [version] nvarchar(32) COLLATE Latin1_General_100_BIN2 NOT NULL,
            [description] nvarchar(256) NOT NULL,
            [checksum_sha256] char(64) COLLATE Latin1_General_100_BIN2 NOT NULL,
            [installed_by] sysname NOT NULL
                CONSTRAINT [DF_schema_migrations_installed_by] DEFAULT (ORIGINAL_LOGIN()),
            [applied_at_utc] datetime2(7) NOT NULL
                CONSTRAINT [DF_schema_migrations_applied_at] DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT [PK_schema_migrations]
                PRIMARY KEY CLUSTERED ([version]),
            CONSTRAINT [CK_schema_migrations_checksum]
                CHECK ([checksum_sha256] NOT LIKE '%[^0-9a-f]%')
        );
    END;

    DECLARE @existing_checksum char(64);
    SELECT @existing_checksum = [checksum_sha256]
    FROM [arrowcol].[schema_migrations] WITH (UPDLOCK, HOLDLOCK)
    WHERE [version] = N'$(MigrationVersion)';

    IF @existing_checksum IS NOT NULL
    BEGIN
        IF @existing_checksum <> '$(MIGRATION_CHECKSUM)'
            THROW 51001, 'Migration 0001 checksum does not match the recorded version.', 1;

        COMMIT TRANSACTION;
        PRINT N'Migration 0001 already applied';
        RETURN;
    END;

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
    PRINT N'Applied migration 0001';
END TRY
BEGIN CATCH
    IF XACT_STATE() <> 0
        ROLLBACK TRANSACTION;
    THROW;
END CATCH;
GO
