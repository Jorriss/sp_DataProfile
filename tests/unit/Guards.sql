/*───────────────────────────────────────────────────────────────────────────
  Guards.sql  —  parameter-validation and version-floor behaviour

  IMPORTANT nuance: the parameter guards RAISERROR at severity 1 (informational)
  then RETURN (sp_DataProfile.sql:139/146/153/160). Severity <= 10 is NOT a
  catchable exception, so tSQLt.ExpectException would never fire. The real,
  observable effect is an EARLY RETURN WITH NO RESULT SET — which we assert with
  INSERT...EXEC into a dummy table (zero result sets → zero rows inserted). These
  guards return before any mode body, so there is no INSERT...EXEC nesting.

  Only the version floor (< 2012) and the internal '@SQLString is null' checks use
  severity 16 (real errors) — but neither is reachable here: the floor can't be
  faked down on a modern host, and @SQLString-null is an internal defensive guard
  not reachable through public parameters. Both are handled with skip / host-branch.
───────────────────────────────────────────────────────────────────────────*/
USE [DataProfileTest];
GO
EXEC tSQLt.NewTestClass 'Guards';
GO

CREATE PROCEDURE Guards.[test_Guards_InvalidModeNotIn0To4_ReturnsNoResultSet]
AS
BEGIN
    CREATE TABLE #x (dummy INT);
    INSERT INTO #x
        EXEC master.dbo.sp_DataProfile @TableName='AllTypes', @Mode=9, @DatabaseName='DataProfileTest';
    EXEC tSQLt.AssertEmptyTable '#x';   -- guard fired → early RETURN → nothing selected
END
GO

CREATE PROCEDURE Guards.[test_Guards_Mode3WithoutColumnList_ReturnsNoResultSet]
AS
BEGIN
    CREATE TABLE #x (dummy INT);
    INSERT INTO #x
        EXEC master.dbo.sp_DataProfile @TableName='Keys', @Mode=3, @DatabaseName='DataProfileTest';
    EXEC tSQLt.AssertEmptyTable '#x';
END
GO

CREATE PROCEDURE Guards.[test_Guards_InvalidSampleType_ReturnsNoResultSet]
AS
BEGIN
    CREATE TABLE #x (dummy INT);
    INSERT INTO #x
        EXEC master.dbo.sp_DataProfile @TableName='AllTypes', @Mode=0,
             @DatabaseName='DataProfileTest', @SampleType='NONSENSE';
    EXEC tSQLt.AssertEmptyTable '#x';
END
GO

CREATE PROCEDURE Guards.[test_Guards_SampleValueOutOfRange_ReturnsNoResultSet]
AS
BEGIN
    CREATE TABLE #x (dummy INT);
    INSERT INTO #x
        EXEC master.dbo.sp_DataProfile @TableName='AllTypes', @Mode=0,
             @DatabaseName='DataProfileTest', @SampleValue=250;
    EXEC tSQLt.AssertEmptyTable '#x';
END
GO

CREATE PROCEDURE Guards.[test_Guards_SqlStringNullGuard_NotReachableViaParameters]
AS
BEGIN
    /* The '@SQLString is null' severity-16 guards are internal defensive checks; there
       is no public-parameter input that makes @SQLString null. Documented as a skip so
       the intent is recorded without a false-green test. */
    EXEC tSQLtTest.Skip 'internal @SQLString-null guard not reachable through public parameters';
END
GO

CREATE PROCEDURE Guards.[test_Guards_SupportedHost_VersionFloorDoesNotFire]
AS
BEGIN
    /* The < 2012 hard-error (severity 16) cannot be provoked on a 2012+ host (version
       can't be faked down). Assert the host branch: a normal Mode 0 call succeeds and
       returns its metadata row (the floor was NOT tripped). */
    IF tSQLtTest.HostMajorVersion() < 11
    BEGIN EXEC tSQLtTest.Skip 'host is below SQL 2012 — the floor branch is the live one'; RETURN; END

    CREATE TABLE #meta (
        object_id INT, schema_name NVARCHAR(128), table_name NVARCHAR(128),
        row_count BIGINT, is_sample NVARCHAR(10)
    );
    EXEC tSQLtTest.CaptureProfile @TargetTable='#meta', @TableName='AllTypes', @Mode=0, @ResultSetNo=1;
    DECLARE @rows INT = (SELECT COUNT(*) FROM #meta);
    EXEC tSQLt.AssertEquals 1, @rows;
END
GO
