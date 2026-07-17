/*───────────────────────────────────────────────────────────────────────────
  Mode2.sql  —  Column Statistics (min/max/mean/median/std_dev)

  Detail set = result set 2. All stats are stored/emitted as NVARCHAR(100).
  Shape at compat >= 110 (13 cols, includes median):
    column_id,name,user_type,system_type,length,precision,scale,is_nullable,
    min_value,max_value,mean,median,std_dev
  Below 110 the median column is dropped (12 cols).

  Stats seed {1,1,3,5,5}: min='1' max='5' mean='3' median='3' std_dev='2.0000'.
───────────────────────────────────────────────────────────────────────────*/
USE [DataProfileTest];
GO
EXEC tSQLt.NewTestClass 'Mode2';
GO

CREATE PROCEDURE Mode2.[test Mode 2 min max against Stats]
AS
BEGIN
    IF tSQLtTest.EffectiveCompatLevel('DataProfileTest') < 110
    BEGIN EXEC tSQLtTest.Skip 'DataProfileTest effective compat < 110'; RETURN; END

    CREATE TABLE #actual (
        column_id INT, name NVARCHAR(128), user_type NVARCHAR(128), system_type NVARCHAR(128),
        [length] NVARCHAR(50) NULL, [precision] INT, scale INT, is_nullable BIT,
        min_value NVARCHAR(100), max_value NVARCHAR(100), mean NVARCHAR(100),
        median NVARCHAR(100), std_dev NVARCHAR(100)
    );
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='Stats', @Mode=2, @ResultSetNo=2;

    DECLARE @minv NVARCHAR(100) = (SELECT min_value FROM #actual WHERE name='val');
    EXEC tSQLt.AssertEqualsString '1', @minv;
    DECLARE @maxv NVARCHAR(100) = (SELECT max_value FROM #actual WHERE name='val');
    EXEC tSQLt.AssertEqualsString '5', @maxv;
END
GO

CREATE PROCEDURE Mode2.[test Mode 2 mean against Stats]
AS
BEGIN
    IF tSQLtTest.EffectiveCompatLevel('DataProfileTest') < 110
    BEGIN EXEC tSQLtTest.Skip 'DataProfileTest effective compat < 110'; RETURN; END

    CREATE TABLE #actual (
        column_id INT, name NVARCHAR(128), user_type NVARCHAR(128), system_type NVARCHAR(128),
        [length] NVARCHAR(50) NULL, [precision] INT, scale INT, is_nullable BIT,
        min_value NVARCHAR(100), max_value NVARCHAR(100), mean NVARCHAR(100),
        median NVARCHAR(100), std_dev NVARCHAR(100)
    );
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='Stats', @Mode=2, @ResultSetNo=2;

    /* AVG over INT is integer division; 15/5 = 3 exactly here (locks the int-mean behaviour). */
    DECLARE @mean NVARCHAR(100) = (SELECT mean FROM #actual WHERE name='val');
    EXEC tSQLt.AssertEqualsString '3', @mean;
END
GO

CREATE PROCEDURE Mode2.[test Mode 2 stddev against Stats]
AS
BEGIN
    IF tSQLtTest.EffectiveCompatLevel('DataProfileTest') < 110
    BEGIN EXEC tSQLtTest.Skip 'DataProfileTest effective compat < 110'; RETURN; END

    CREATE TABLE #actual (
        column_id INT, name NVARCHAR(128), user_type NVARCHAR(128), system_type NVARCHAR(128),
        [length] NVARCHAR(50) NULL, [precision] INT, scale INT, is_nullable BIT,
        min_value NVARCHAR(100), max_value NVARCHAR(100), mean NVARCHAR(100),
        median NVARCHAR(100), std_dev NVARCHAR(100)
    );
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='Stats', @Mode=2, @ResultSetNo=2;

    /* sample STDEV of {1,1,3,5,5} = sqrt(16/4) = 2, cast NUMERIC(18,4) → '2.0000'. */
    DECLARE @stddev NVARCHAR(100) = (SELECT std_dev FROM #actual WHERE name='val');
    EXEC tSQLt.AssertEqualsString '2.0000', @stddev;
END
GO

CREATE PROCEDURE Mode2.[test Mode 2 median at compat 110 or higher against Stats]
AS
BEGIN
    IF tSQLtTest.EffectiveCompatLevel('DataProfileTest') < 110
    BEGIN EXEC tSQLtTest.Skip 'DataProfileTest effective compat < 110'; RETURN; END

    CREATE TABLE #actual (
        column_id INT, name NVARCHAR(128), user_type NVARCHAR(128), system_type NVARCHAR(128),
        [length] NVARCHAR(50) NULL, [precision] INT, scale INT, is_nullable BIT,
        min_value NVARCHAR(100), max_value NVARCHAR(100), mean NVARCHAR(100),
        median NVARCHAR(100), std_dev NVARCHAR(100)
    );
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='Stats', @Mode=2, @ResultSetNo=2;

    DECLARE @median NVARCHAR(100) = (SELECT median FROM #actual WHERE name='val');
    EXEC tSQLt.AssertEqualsString '3', @median;
END
GO

CREATE PROCEDURE Mode2.[test Mode 2 median gracefully degrades below compat 110]
AS
BEGIN
    /* No in-test ALTER DATABASE (not allowed in tSQLt's transaction). Instead profile
       the committed compat-100 fixture DB: effective compat = MIN(master,100) < 110, so
       the proc drops the median column. Capturing into a 12-column (no median) shape
       succeeds; a 13-column capture would fail — that IS the degradation assertion. */
    CREATE TABLE #actual (
        column_id INT, name NVARCHAR(128), user_type NVARCHAR(128), system_type NVARCHAR(128),
        [length] NVARCHAR(50) NULL, [precision] INT, scale INT, is_nullable BIT,
        min_value NVARCHAR(100), max_value NVARCHAR(100), mean NVARCHAR(100), std_dev NVARCHAR(100)
    );
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='Stats',
                                  @Mode=2, @DatabaseName='DataProfileTest_Compat100', @ResultSetNo=2;

    DECLARE @rows INT = (SELECT COUNT(*) FROM #actual WHERE name='val');
    EXEC tSQLt.AssertEquals 1, @rows;
    DECLARE @minv NVARCHAR(100) = (SELECT min_value FROM #actual WHERE name='val');
    EXEC tSQLt.AssertEqualsString '1', @minv;
    DECLARE @maxv NVARCHAR(100) = (SELECT max_value FROM #actual WHERE name='val');
    EXEC tSQLt.AssertEqualsString '5', @maxv;
    DECLARE @mean NVARCHAR(100) = (SELECT mean FROM #actual WHERE name='val');
    EXEC tSQLt.AssertEqualsString '3', @mean;
END
GO
