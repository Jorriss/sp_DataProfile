/*───────────────────────────────────────────────────────────────────────────
  Quoting.sql  —  QUOTENAME regression against [Odd Names]

  Table [Odd Names] has a spaced name, a spaced column [Order Date], a reserved
  word column [Select], and a nullable [My Col]. These exercise QUOTENAME on
  schema/table/column across Modes 1/2/3.

  Seed (3 rows):
    [Order Date] = 3 distinct dates ; [Select] = 1,2,2 ; [My Col] = 'aa',NULL,'cccc'
───────────────────────────────────────────────────────────────────────────*/
USE [DataProfileTest];
GO
EXEC tSQLt.NewTestClass 'Quoting';
GO

CREATE PROCEDURE Quoting.[test Mode 1 against Odd Names succeeds with correct rows]
AS
BEGIN
    CREATE TABLE #actual (
        column_id INT, name NVARCHAR(128), user_type NVARCHAR(128), system_type NVARCHAR(128),
        [length] NVARCHAR(50) NULL, [precision] INT, scale INT, is_nullable BIT,
        num_unique_values BIGINT, unique_ratio DECIMAL(25,5), num_nulls BIGINT,
        nulls_ratio DECIMAL(25,5), min_length INT, max_length INT
    );
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='[Odd Names]', @Mode=1, @ResultSetNo=2;

    SELECT name, num_unique_values, num_nulls INTO #got FROM #actual;
    CREATE TABLE #exp (name NVARCHAR(128), num_unique_values BIGINT, num_nulls BIGINT);
    INSERT INTO #exp VALUES
      ('Order Date', 3, NULL),   -- NOT NULL date, 3 distinct
      ('Select',     2, NULL),   -- reserved word col; values 1,2,2 → 2 distinct
      ('My Col',     2, 1);      -- 'aa','cccc' distinct = 2, one NULL
    EXEC tSQLt.AssertEqualsTable '#exp', '#got';
END
GO

CREATE PROCEDURE Quoting.[test Mode 2 against Odd Names succeeds]
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
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='[Odd Names]', @Mode=2, @ResultSetNo=2;

    /* [Select] is numeric → min/max computed; proves the reserved word was quoted. */
    DECLARE @minv NVARCHAR(100) = (SELECT min_value FROM #actual WHERE name='Select');
    EXEC tSQLt.AssertEqualsString '1', @minv;
    DECLARE @maxv NVARCHAR(100) = (SELECT max_value FROM #actual WHERE name='Select');
    EXEC tSQLt.AssertEqualsString '2', @maxv;
END
GO

CREATE PROCEDURE Quoting.[test Mode 3 against Odd Names succeeds]
AS
BEGIN
    CREATE TABLE #actual ( row_count INT, [Select] INT, view_data_sql NVARCHAR(MAX) );
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='[Odd Names]',
                                  @Mode=3, @ColumnList='[Select]', @ResultSetNo=2;

    /* [Select] = 1,2,2 → the value 2 duplicates → one group of size 2. */
    DECLARE @groups INT = (SELECT COUNT(*) FROM #actual);
    EXEC tSQLt.AssertEquals 1, @groups;
    DECLARE @rc INT = (SELECT row_count FROM #actual);
    EXEC tSQLt.AssertEquals 2, @rc;
    DECLARE @sel INT = (SELECT [Select] FROM #actual);
    EXEC tSQLt.AssertEquals 2, @sel;
END
GO
