/*───────────────────────────────────────────────────────────────────────────
  Smoke.sql  —  install/callability + the capture-path proof (build order step 1)

  These tests prove the harness plumbing end to end BEFORE any value assertions
  depend on it: the proc is installed, Mode 0 runs and has the expected shape,
  and — critically — the Mode 3 loopback capture works (the INSERT...EXEC nesting
  workaround). If test 4 is red on your instance, fix the loopback (server name /
  Ad Hoc Distributed Queries) before building the rest.

  Prereq: run tests/install/01–04 first.
───────────────────────────────────────────────────────────────────────────*/
USE [DataProfileTest];
GO
EXEC tSQLt.NewTestClass 'Smoke';
GO

CREATE PROCEDURE Smoke.[test_spDataProfile_InstalledInMaster_IsCallableBySpName]
AS
BEGIN
    IF OBJECT_ID('master.dbo.sp_DataProfile') IS NULL
        EXEC tSQLt.Fail 'master.dbo.sp_DataProfile is not installed. Run sp_DataProfile.sql (F5) first.';
END
GO

CREATE PROCEDURE Smoke.[test_Mode0_AllTypesDefault_RunsWithoutError]
AS
BEGIN
    CREATE TABLE #actual (
        column_id INT, name NVARCHAR(128), user_type NVARCHAR(128), system_type NVARCHAR(128),
        [length] NVARCHAR(50) NULL, [precision] INT, scale INT, is_nullable BIT, collation NVARCHAR(128) NULL
    );
    EXEC tSQLtTest.CaptureProfile @TargetTable = '#actual', @TableName = 'AllTypes',
                                  @Mode = 0, @ResultSetNo = 2;

    /* AllTypes has 13 columns → 13 detail rows. A successful capture into this exact
       9-column shape is itself the Mode 0 detail shape check (INSERT...EXEC errors on
       a column-count mismatch). */
    DECLARE @rows INT = (SELECT COUNT(*) FROM #actual);
    EXEC tSQLt.AssertEquals 13, @rows;
END
GO

CREATE PROCEDURE Smoke.[test_Mode0_AllTypes_ReturnsExpectedOverviewColumnSet]
AS
BEGIN
    CREATE TABLE #actual (
        column_id INT, name NVARCHAR(128), user_type NVARCHAR(128), system_type NVARCHAR(128),
        [length] NVARCHAR(50) NULL, [precision] INT, scale INT, is_nullable BIT, collation NVARCHAR(128) NULL
    );
    EXEC tSQLtTest.CaptureProfile @TargetTable = '#actual', @TableName = 'AllTypes',
                                  @Mode = 0, @ResultSetNo = 2;

    /* Lock a known column's overview values. */
    SELECT system_type, is_nullable INTO #got FROM #actual WHERE name = 'c_bit';
    SELECT system_type = CAST('bit' AS NVARCHAR(128)), is_nullable = CAST(0 AS BIT) INTO #exp;
    EXEC tSQLt.AssertEqualsTable '#exp', '#got';
END
GO

CREATE PROCEDURE Smoke.[test_Mode3_CaptureProfileLoopbackHelper_ReturnsRowset]
AS
BEGIN
    /* Proves the Mode 3 capture path (loopback OPENROWSET → ResultSetFilter →
       proc whose own INSERT...EXEC would otherwise nest). Keys (k1,v) has one
       duplicate group: (1,10) appears twice. */
    CREATE TABLE #actual ( row_count INT, k1 INT, v INT, view_data_sql NVARCHAR(MAX) );
    EXEC tSQLtTest.CaptureProfile @TargetTable = '#actual', @TableName = 'Keys',
                                  @Mode = 3, @ColumnList = 'k1,v', @ResultSetNo = 2;

    DECLARE @groups INT = (SELECT COUNT(*) FROM #actual);
    EXEC tSQLt.AssertEquals 1, @groups;       -- one duplicate group
    DECLARE @rc INT = (SELECT row_count FROM #actual);
    EXEC tSQLt.AssertEquals 2, @rc;      -- that group has 2 rows
END
GO
