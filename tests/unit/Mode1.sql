/*───────────────────────────────────────────────────────────────────────────
  Mode1.sql  —  Column Detail (num_unique_values, num_nulls, min/max length)

  Detail set = result set 2. Shape (14 cols):
    column_id, name, user_type, system_type, length, precision, scale, is_nullable,
    num_unique_values(BIGINT), unique_ratio(DEC(25,5)), num_nulls(BIGINT),
    nulls_ratio(DEC(25,5)), min_length(INT), max_length(INT)
  Tests capture the full shape then project the deterministic columns.
───────────────────────────────────────────────────────────────────────────*/
USE [DataProfileTest];
GO
EXEC tSQLt.NewTestClass 'Mode1';
GO

/* Reusable capture into a full-shape #actual is inlined per test (temp tables
   don't survive a helper), but the shape is identical everywhere below. */

CREATE PROCEDURE Mode1.[test Mode 1 num_nulls per column against Nullable]
AS
BEGIN
    CREATE TABLE #actual (
        column_id INT, name NVARCHAR(128), user_type NVARCHAR(128), system_type NVARCHAR(128),
        [length] NVARCHAR(50) NULL, [precision] INT, scale INT, is_nullable BIT,
        num_unique_values BIGINT, unique_ratio DECIMAL(25,5), num_nulls BIGINT,
        nulls_ratio DECIMAL(25,5), min_length INT, max_length INT
    );
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='Nullable', @Mode=1, @ResultSetNo=2;

    SELECT name, num_nulls INTO #got FROM #actual;
    CREATE TABLE #exp (name NVARCHAR(128), num_nulls BIGINT);
    INSERT INTO #exp VALUES ('id', NULL), ('s', 2), ('soft', 1);   -- id is NOT NULL → NULL, not 0
    EXEC tSQLt.AssertEqualsTable '#exp', '#got';
END
GO

CREATE PROCEDURE Mode1.[test Mode 1 distinct count per column against Cardinality]
AS
BEGIN
    CREATE TABLE #actual (
        column_id INT, name NVARCHAR(128), user_type NVARCHAR(128), system_type NVARCHAR(128),
        [length] NVARCHAR(50) NULL, [precision] INT, scale INT, is_nullable BIT,
        num_unique_values BIGINT, unique_ratio DECIMAL(25,5), num_nulls BIGINT,
        nulls_ratio DECIMAL(25,5), min_length INT, max_length INT
    );
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='Cardinality', @Mode=1, @ResultSetNo=2;

    SELECT name, num_unique_values INTO #got FROM #actual;
    CREATE TABLE #exp (name NVARCHAR(128), num_unique_values BIGINT);
    INSERT INTO #exp VALUES ('const_col',1),('bin_col',2),('uniq_col',6),('cat_col',3);
    EXEC tSQLt.AssertEqualsTable '#exp', '#got';
END
GO

CREATE PROCEDURE Mode1.[test Mode 1 min max length against AllTypes string columns]
AS
BEGIN
    CREATE TABLE #actual (
        column_id INT, name NVARCHAR(128), user_type NVARCHAR(128), system_type NVARCHAR(128),
        [length] NVARCHAR(50) NULL, [precision] INT, scale INT, is_nullable BIT,
        num_unique_values BIGINT, unique_ratio DECIMAL(25,5), num_nulls BIGINT,
        nulls_ratio DECIMAL(25,5), min_length INT, max_length INT
    );
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='AllTypes', @Mode=1, @ResultSetNo=2;

    SELECT name, min_length, max_length INTO #got FROM #actual WHERE name IN ('c_varchar','c_nvarchar');
    CREATE TABLE #exp (name NVARCHAR(128), min_length INT, max_length INT);
    INSERT INTO #exp VALUES ('c_varchar',1,3),('c_nvarchar',1,3);
    EXEC tSQLt.AssertEqualsTable '#exp', '#got';
END
GO

CREATE PROCEDURE Mode1.[test Mode 1 handles each supported type in AllTypes without error]
AS
BEGIN
    CREATE TABLE #actual (
        column_id INT, name NVARCHAR(128), user_type NVARCHAR(128), system_type NVARCHAR(128),
        [length] NVARCHAR(50) NULL, [precision] INT, scale INT, is_nullable BIT,
        num_unique_values BIGINT, unique_ratio DECIMAL(25,5), num_nulls BIGINT,
        nulls_ratio DECIMAL(25,5), min_length INT, max_length INT
    );
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='AllTypes', @Mode=1, @ResultSetNo=2;
    DECLARE @rows INT = (SELECT COUNT(*) FROM #actual);
    EXEC tSQLt.AssertEquals 13, @rows;
END
GO

CREATE PROCEDURE Mode1.[test Mode 1 ColumnList restricts output to named columns]
AS
BEGIN
    CREATE TABLE #actual (
        column_id INT, name NVARCHAR(128), user_type NVARCHAR(128), system_type NVARCHAR(128),
        [length] NVARCHAR(50) NULL, [precision] INT, scale INT, is_nullable BIT,
        num_unique_values BIGINT, unique_ratio DECIMAL(25,5), num_nulls BIGINT,
        nulls_ratio DECIMAL(25,5), min_length INT, max_length INT
    );
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='Cardinality',
                                  @Mode=1, @ColumnList='cat_col', @ResultSetNo=2;

    DECLARE @rows INT = (SELECT COUNT(*) FROM #actual);
    EXEC tSQLt.AssertEquals 1, @rows;
    DECLARE @name NVARCHAR(128) = (SELECT name FROM #actual);
    EXEC tSQLt.AssertEqualsString 'cat_col', @name;
END
GO
