/*───────────────────────────────────────────────────────────────────────────
  05_test_helpers.sql  —  shared conditional-skip + host-info helpers

  Version-gated behaviour (APPROX_COUNT_DISTINCT needs SQL 2019+, the < 2012
  hard-error floor) can only be exercised for the branch that matches the host —
  the server version can't be faked. Tests that don't match the host skip with a
  message rather than fail. These helpers keep that consistent (write once, use
  everywhere) as test-inventory.md's "Notes before writing" asks.

  Usage in a test:
      IF tSQLtTest.HostMajorVersion() < 15
      BEGIN EXEC tSQLtTest.Skip 'needs SQL 2019+ for APPROX_COUNT_DISTINCT'; RETURN; END
───────────────────────────────────────────────────────────────────────────*/
USE [DataProfileTest];
GO

IF SCHEMA_ID('tSQLtTest') IS NULL EXEC('CREATE SCHEMA tSQLtTest;');
GO

/* Host SQL Server major version: 11=2012, 13=2016, 15=2019, 16=2022. */
IF OBJECT_ID('tSQLtTest.HostMajorVersion') IS NOT NULL DROP FUNCTION tSQLtTest.HostMajorVersion;
GO
CREATE FUNCTION tSQLtTest.HostMajorVersion() RETURNS INT
AS
BEGIN
    RETURN CAST(SERVERPROPERTY('ProductMajorVersion') AS INT);
END
GO

/* Effective compat level the proc would use for a given DB = MIN(master, that DB). */
IF OBJECT_ID('tSQLtTest.EffectiveCompatLevel') IS NOT NULL DROP FUNCTION tSQLtTest.EffectiveCompatLevel;
GO
CREATE FUNCTION tSQLtTest.EffectiveCompatLevel(@dbname SYSNAME) RETURNS INT
AS
BEGIN
    DECLARE @m INT = (SELECT compatibility_level FROM sys.databases WHERE name = 'master');
    DECLARE @d INT = (SELECT compatibility_level FROM sys.databases WHERE name = @dbname);
    RETURN CASE WHEN @m < @d THEN @m ELSE @d END;
END
GO

/* Skip the current test. Uses tSQLt.SkipTest when the installed tSQLt has it
   (marks the test SKIPPED in the report); otherwise passes quietly with a
   message. Always follow the call with RETURN in the test body. */
IF OBJECT_ID('tSQLtTest.Skip') IS NOT NULL DROP PROCEDURE tSQLtTest.Skip;
GO
CREATE PROCEDURE tSQLtTest.Skip @Message NVARCHAR(MAX)
AS
BEGIN
    IF OBJECT_ID('tSQLt.SkipTest') IS NOT NULL
        EXEC tSQLt.SkipTest @Message;              -- raises; control does not return
    ELSE
        RAISERROR('SKIPPED: %s', 0, 1, @Message) WITH NOWAIT;
END
GO

RAISERROR('tSQLtTest helpers (HostMajorVersion / EffectiveCompatLevel / Skip) created.', 0, 1) WITH NOWAIT;
GO
