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

CREATE PROCEDURE Quoting.[test_Mode1_OddNamesTable_ReturnsCorrectPerColumnRows]
AS
BEGIN
    CREATE TABLE #actual (
        column_id INT, name NVARCHAR(128), user_type NVARCHAR(128), system_type NVARCHAR(128),
        [length] NVARCHAR(50) NULL, [precision] INT, scale INT, is_nullable BIT,
        num_unique_values BIGINT, unique_ratio DECIMAL(25,5), num_nulls BIGINT,
        nulls_ratio DECIMAL(25,5), min_length INT, max_length INT
    );
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='[Odd Names]', @Mode=1, @ResultSetNo=2;

    /* Also pins is_nullable (both states: the two NOT NULL cols → 0, [My Col] → 1) and the derived
       unique_ratio / nulls_ratio (= count / num_rows, DECIMAL(25,5)). [Odd Names] has 3 rows; NOT
       NULL columns report num_nulls/nulls_ratio NULL. */
    SELECT name, is_nullable, num_unique_values, unique_ratio, num_nulls, nulls_ratio INTO #got FROM #actual;
    CREATE TABLE #exp (
        name NVARCHAR(128), is_nullable BIT, num_unique_values BIGINT, unique_ratio DECIMAL(25,5),
        num_nulls BIGINT, nulls_ratio DECIMAL(25,5)
    );
    INSERT INTO #exp VALUES
      ('Order Date', 0, 3, 1.00000, NULL, NULL),      -- NOT NULL date, 3 distinct / 3
      ('Select',     0, 2, 0.66667, NULL, NULL),      -- reserved word col; 1,2,2 → 2 distinct / 3
      ('My Col',     1, 2, 0.66667, 1,    0.33333);   -- 'aa','cccc' distinct = 2 / 3, one NULL / 3
    EXEC tSQLt.AssertEqualsTable '#exp', '#got';
END
GO

CREATE PROCEDURE Quoting.[test_Mode2_OddNamesTable_QuotesReservedWordColumn]
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

CREATE PROCEDURE Quoting.[test_Mode3_OddNamesTable_ReturnsDuplicateGroup]
AS
BEGIN
    CREATE TABLE #actual ( row_count INT, [Select] INT, view_data_sql NVARCHAR(MAX) );
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='[Odd Names]',
                                  @Mode=3, @ColumnList='[Select]', @ResultSetNo=2;

    /* [Select] = 1,2,2 → the value 2 duplicates → one group of size 2. Assert the whole row
       (AssertEqualsTable implicitly asserts exactly one group); view_data_sql projected out. */
    SELECT row_count, [Select] INTO #got FROM #actual;
    CREATE TABLE #exp ( row_count INT, [Select] INT );
    INSERT INTO #exp VALUES (2, 2);
    EXEC tSQLt.AssertEqualsTable '#exp', '#got';
END
GO
