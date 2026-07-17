/*───────────────────────────────────────────────────────────────────────────
  VersionMatrix.sql  —  version/compat-adaptive branches

  Two levers, two testabilities (design constraint #3):
    - Compat level is settable, so the median branch is asserted on BOTH sides via
      the committed compat-100 fixture DB (no in-test ALTER DATABASE).
    - Server major version can't be faked, so the APPROX_COUNT_DISTINCT branch is
      asserted only for the branch that matches THIS host.
───────────────────────────────────────────────────────────────────────────*/
USE [DataProfileTest];
GO
EXEC tSQLt.NewTestClass 'VersionMatrix';
GO

CREATE PROCEDURE VersionMatrix.[test compat-gated median present at 110 and skipped below]
AS
BEGIN
    IF tSQLtTest.EffectiveCompatLevel('DataProfileTest') < 110
    BEGIN EXEC tSQLtTest.Skip 'host effective compat < 110 — cannot assert the >=110 side'; RETURN; END

    /* >= 110 side: median column present and populated. */
    CREATE TABLE #hi (
        column_id INT, name NVARCHAR(128), user_type NVARCHAR(128), system_type NVARCHAR(128),
        [length] NVARCHAR(50) NULL, [precision] INT, scale INT, is_nullable BIT,
        min_value NVARCHAR(100), max_value NVARCHAR(100), mean NVARCHAR(100),
        median NVARCHAR(100), std_dev NVARCHAR(100)
    );
    EXEC tSQLtTest.CaptureProfile @TargetTable='#hi', @TableName='Stats', @Mode=2, @ResultSetNo=2;
    DECLARE @median NVARCHAR(100) = (SELECT median FROM #hi WHERE name='val');
    EXEC tSQLt.AssertEqualsString '3', @median;

    /* < 110 side: median column dropped → the 12-column (no median) shape captures cleanly. */
    CREATE TABLE #lo (
        column_id INT, name NVARCHAR(128), user_type NVARCHAR(128), system_type NVARCHAR(128),
        [length] NVARCHAR(50) NULL, [precision] INT, scale INT, is_nullable BIT,
        min_value NVARCHAR(100), max_value NVARCHAR(100), mean NVARCHAR(100), std_dev NVARCHAR(100)
    );
    EXEC tSQLtTest.CaptureProfile @TargetTable='#lo', @TableName='Stats', @Mode=2,
                                  @DatabaseName='DataProfileTest_Compat100', @ResultSetNo=2;
    DECLARE @stddev NVARCHAR(100) = (SELECT std_dev FROM #lo WHERE name='val');
    EXEC tSQLt.AssertEqualsString '2.0000', @stddev;  -- stats still work
END
GO

CREATE PROCEDURE VersionMatrix.[test server-version-gated approx distinct matches host]
AS
BEGIN
    CREATE TABLE #actual (
        object_id INT, schema_name NVARCHAR(128), table_name NVARCHAR(128), row_count BIGINT,
        column_name NVARCHAR(128), distinct_row_count BIGINT, is_sample NVARCHAR(10)
    );
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='Cardinality',
                                  @Mode=4, @ColumnList='cat_col', @ApproxDistinct=1, @ResultSetNo=1;

    DECLARE @d BIGINT = (SELECT distinct_row_count FROM #actual);
    IF tSQLtTest.HostMajorVersion() >= 15
    BEGIN
        /* 2019+: APPROX_COUNT_DISTINCT is used — assert closeness (exact is 3). */
        IF @d NOT BETWEEN 2 AND 4 EXEC tSQLt.Fail 'APPROX distinct not close to 3 on a 2019+ host';
    END
    ELSE
    BEGIN
        /* pre-2019: @ApproxDistinct falls back to exact COUNT(DISTINCT). */
        EXEC tSQLt.AssertEquals 3, @d;
    END
END
GO
