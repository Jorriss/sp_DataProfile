/*───────────────────────────────────────────────────────────────────────────
  StackOverflowSmoke.sql  —  "runs without error against a real large DB"

  Shape / no-exception only, never value assertions (the StackOverflow snapshot
  drifts). Every test skips cleanly when a [StackOverflow] database with the
  expected tables isn't attached, so the suite stays green on machines without it.
  Heavy modes use a tiny sample to stay fast.
───────────────────────────────────────────────────────────────────────────*/
USE [DataProfileTest];
GO
EXEC tSQLt.NewTestClass 'StackOverflowSmoke';
GO

CREATE PROCEDURE StackOverflowSmoke.[test_Mode0_PostsTable_RunsAndReturnsShape]
AS
BEGIN
    IF DB_ID('StackOverflow') IS NULL OR OBJECT_ID('StackOverflow.dbo.Posts') IS NULL
    BEGIN EXEC tSQLtTest.Skip 'StackOverflow.dbo.Posts not present'; RETURN; END

    CREATE TABLE #actual (
        column_id INT, name NVARCHAR(128), user_type NVARCHAR(128), system_type NVARCHAR(128),
        [length] NVARCHAR(50) NULL, [precision] INT, scale INT, is_nullable BIT, collation NVARCHAR(128) NULL
    );
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='Posts',
                                  @Mode=0, @DatabaseName='StackOverflow', @ResultSetNo=2;
    IF (SELECT COUNT(*) FROM #actual) < 1 EXEC tSQLt.Fail 'Mode 0 on Posts returned no columns.';
END
GO

CREATE PROCEDURE StackOverflowSmoke.[test_Mode1_UsersTable_RunsWithoutException]
AS
BEGIN
    IF DB_ID('StackOverflow') IS NULL OR OBJECT_ID('StackOverflow.dbo.Users') IS NULL
    BEGIN EXEC tSQLtTest.Skip 'StackOverflow.dbo.Users not present'; RETURN; END

    CREATE TABLE #actual (
        column_id INT, name NVARCHAR(128), user_type NVARCHAR(128), system_type NVARCHAR(128),
        [length] NVARCHAR(50) NULL, [precision] INT, scale INT, is_nullable BIT,
        num_unique_values BIGINT, unique_ratio DECIMAL(25,5), cardinality NVARCHAR(30),
        num_nulls BIGINT, nulls_ratio DECIMAL(25,5),
        num_blank BIGINT, blank_ratio DECIMAL(25,5), num_whitespace BIGINT, whitespace_ratio DECIMAL(25,5),
        num_zero BIGINT, zero_ratio DECIMAL(25,5), num_negative BIGINT, negative_ratio DECIMAL(25,5),
        min_length INT, max_length INT, min_value NVARCHAR(100), max_value NVARCHAR(100)
    );
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='Users',
                                  @Mode=1, @DatabaseName='StackOverflow',
                                  @SampleValue=1, @ResultSetNo=2;
    IF (SELECT COUNT(*) FROM #actual) < 1 EXEC tSQLt.Fail 'Mode 1 on Users returned no columns.';
END
GO

CREATE PROCEDURE StackOverflowSmoke.[test_AllModes0To4_UsersTable_ReturnWithoutError]
AS
BEGIN
    IF DB_ID('StackOverflow') IS NULL OR OBJECT_ID('StackOverflow.dbo.Users') IS NULL
    BEGIN EXEC tSQLtTest.Skip 'StackOverflow.dbo.Users not present'; RETURN; END

    /* Capture each mode's metadata set (result set 1) = proof it ran. Modes 3/4 need a
       column; Id works. Small sample keeps the scan cheap. */
    CREATE TABLE #m5 ( object_id INT, schema_name NVARCHAR(128), table_name NVARCHAR(128),
                       row_count BIGINT, is_sample NVARCHAR(10) );
    CREATE TABLE #m7 ( object_id INT, schema_name NVARCHAR(128), table_name NVARCHAR(128),
                       row_count BIGINT, column_name NVARCHAR(128), distinct_row_count BIGINT, is_sample NVARCHAR(10) );

    EXEC tSQLtTest.CaptureProfile @TargetTable='#m5', @TableName='Users', @Mode=0, @DatabaseName='StackOverflow', @SampleValue=1, @ResultSetNo=1;
    EXEC tSQLtTest.CaptureProfile @TargetTable='#m5', @TableName='Users', @Mode=1, @DatabaseName='StackOverflow', @SampleValue=1, @ResultSetNo=1;
    EXEC tSQLtTest.CaptureProfile @TargetTable='#m5', @TableName='Users', @Mode=2, @DatabaseName='StackOverflow', @SampleValue=1, @ResultSetNo=1;
    EXEC tSQLtTest.CaptureProfile @TargetTable='#m5', @TableName='Users', @Mode=3, @DatabaseName='StackOverflow', @ColumnList='Id', @SampleValue=1, @ResultSetNo=1;
    EXEC tSQLtTest.CaptureProfile @TargetTable='#m7', @TableName='Users', @Mode=4, @DatabaseName='StackOverflow', @ColumnList='Id', @SampleValue=1, @ResultSetNo=1;

    DECLARE @m5rows INT = (SELECT COUNT(*) FROM #m5);
    EXEC tSQLt.AssertEquals 4, @m5rows;   -- modes 0,1,2,3 metadata rows
    DECLARE @m7rows INT = (SELECT COUNT(*) FROM #m7);
    EXEC tSQLt.AssertEquals 1, @m7rows;   -- mode 4 metadata row
END
GO
