/*───────────────────────────────────────────────────────────────────────────
  CrossDatabase.sql  —  cross-DB call guards the QUOTENAME(@DatabaseName) path

  The loopback path runs the proc on a session whose current database is NOT
  DataProfileTest, so @DatabaseName='DataProfileTest' must be qualified onto every
  catalog/base-table reference (sp_DataProfile.sql:172-173). We force loopback for
  a non-Mode-3 mode to exercise that qualification and assert the values still land.
───────────────────────────────────────────────────────────────────────────*/
USE [DataProfileTest];
GO
EXEC tSQLt.NewTestClass 'CrossDatabase';
GO

CREATE PROCEDURE CrossDatabase.[test_Mode1_CrossDatabaseCall_EqualsInContextRun]
AS
BEGIN
    CREATE TABLE #actual (
        column_id INT, name NVARCHAR(128), user_type NVARCHAR(128), system_type NVARCHAR(128),
        [length] NVARCHAR(50) NULL, [precision] INT, scale INT, is_nullable BIT,
        num_unique_values BIGINT, unique_ratio DECIMAL(25,5), cardinality NVARCHAR(30),
        num_nulls BIGINT, nulls_ratio DECIMAL(25,5),
        num_blank BIGINT, blank_ratio DECIMAL(25,5), num_whitespace BIGINT, whitespace_ratio DECIMAL(25,5),
        num_zero BIGINT, zero_ratio DECIMAL(25,5), num_negative BIGINT, negative_ratio DECIMAL(25,5),
        min_length INT, max_length INT, min_value NVARCHAR(100), max_value NVARCHAR(100)
    );
    /* @ForceLoopback=1 → proc runs from master context; DataProfileTest must be qualified. */
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='Cardinality',
                                  @Mode=1, @DatabaseName='DataProfileTest',
                                  @ResultSetNo=2, @ForceLoopback=1;

    SELECT name, num_unique_values INTO #got FROM #actual;
    CREATE TABLE #exp (name NVARCHAR(128), num_unique_values BIGINT);
    INSERT INTO #exp VALUES ('const_col',1),('bin_col',2),('uniq_col',6),('cat_col',3);
    EXEC tSQLt.AssertEqualsTable '#exp', '#got';
END
GO
