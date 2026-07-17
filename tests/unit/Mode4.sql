/*───────────────────────────────────────────────────────────────────────────
  Mode4.sql  —  Column Value Distribution

  Metadata set (1): ..., column_name, distinct_row_count, is_sample.
  Detail set (2): <column>, Count(INT), Percentage(DECIMAL(18,4)), grouped, ORDER BY Count DESC.

  Cardinality.cat_col = A,A,B,B,C,A over 6 rows → A:3/50.0000, B:2/33.3333, C:1/16.6667.
───────────────────────────────────────────────────────────────────────────*/
USE [DataProfileTest];
GO
EXEC tSQLt.NewTestClass 'Mode4';
GO

CREATE PROCEDURE Mode4.[test Mode 4 distinct values on a Cardinality column]
AS
BEGIN
    CREATE TABLE #actual ( column_value VARCHAR(10), [Count] INT, Percentage DECIMAL(18,4) );
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='Cardinality',
                                  @Mode=4, @ColumnList='cat_col', @ResultSetNo=2;

    DECLARE @rows INT = (SELECT COUNT(*) FROM #actual);
    EXEC tSQLt.AssertEquals 3, @rows;   -- three distinct values A/B/C
END
GO

CREATE PROCEDURE Mode4.[test Mode 4 distribution counts on a Cardinality column]
AS
BEGIN
    CREATE TABLE #actual ( column_value VARCHAR(10), [Count] INT, Percentage DECIMAL(18,4) );
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='Cardinality',
                                  @Mode=4, @ColumnList='cat_col', @ResultSetNo=2;

    SELECT column_value, [Count], Percentage INTO #got FROM #actual;
    CREATE TABLE #exp ( column_value VARCHAR(10), [Count] INT, Percentage DECIMAL(18,4) );
    INSERT INTO #exp VALUES ('A',3,50.0000),('B',2,33.3333),('C',1,16.6667);
    EXEC tSQLt.AssertEqualsTable '#exp', '#got';
END
GO

CREATE PROCEDURE Mode4.[test Mode 4 ApproxDistinct on SQL 2019 plus is close to exact]
AS
BEGIN
    IF tSQLtTest.HostMajorVersion() < 15
    BEGIN EXEC tSQLtTest.Skip 'APPROX_COUNT_DISTINCT needs SQL Server 2019+ (major 15)'; RETURN; END

    /* Metadata set (1) carries distinct_row_count. Assert closeness, not equality. */
    CREATE TABLE #actual (
        object_id INT, schema_name NVARCHAR(128), table_name NVARCHAR(128), row_count BIGINT,
        column_name NVARCHAR(128), distinct_row_count BIGINT, is_sample NVARCHAR(10)
    );
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='Cardinality',
                                  @Mode=4, @ColumnList='cat_col', @ApproxDistinct=1, @ResultSetNo=1;

    DECLARE @d BIGINT = (SELECT distinct_row_count FROM #actual);
    IF @d NOT BETWEEN 2 AND 4      -- exact is 3; allow approximation slack
        EXEC tSQLt.Fail 'APPROX_COUNT_DISTINCT distinct_row_count not close to 3';
END
GO
