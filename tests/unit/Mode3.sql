/*───────────────────────────────────────────────────────────────────────────
  Mode3.sql  —  Candidate Key Check (captured via the loopback path)

  Mode 3 runs its own INSERT...EXEC internally (sp_DataProfile.sql:856), so
  CaptureProfile routes it through the loopback OPENROWSET automatically.
  Detail set = result set 2: COUNT(*) AS row_count, <@ColumnList cols>, view_data_sql,
  grouped with HAVING COUNT(*) > 1 (i.e. duplicate groups only).

  Keys: (k1,k2) is unique → 0 duplicate rows (candidate key);
        (k1,v) has the duplicate (1,10) → 1 duplicate group of size 2.
───────────────────────────────────────────────────────────────────────────*/
USE [DataProfileTest];
GO
EXEC tSQLt.NewTestClass 'Mode3';
GO

CREATE PROCEDURE Mode3.[test Mode 3 flags Keys composite as a candidate key]
AS
BEGIN
    CREATE TABLE #actual ( row_count INT, k1 INT, k2 INT, view_data_sql NVARCHAR(MAX) );
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='Keys',
                                  @Mode=3, @ColumnList='k1,k2', @ResultSetNo=2;

    /* No duplicate groups → (k1,k2) is a candidate key. */
    EXEC tSQLt.AssertEmptyTable '#actual';
END
GO

CREATE PROCEDURE Mode3.[test Mode 3 does not flag the non-key combo]
AS
BEGIN
    CREATE TABLE #actual ( row_count INT, k1 INT, v INT, view_data_sql NVARCHAR(MAX) );
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='Keys',
                                  @Mode=3, @ColumnList='k1,v', @ResultSetNo=2;

    DECLARE @groups INT = (SELECT COUNT(*) FROM #actual);
    EXEC tSQLt.AssertEquals 1, @groups;           -- one duplicate group
    DECLARE @rc INT = (SELECT row_count FROM #actual);
    EXEC tSQLt.AssertEquals 2, @rc;          -- of size 2: (k1=1, v=10)
    DECLARE @dup INT = (SELECT COUNT(*) FROM #actual WHERE k1=1 AND v=10);
    EXEC tSQLt.AssertEquals 1, @dup;
END
GO

CREATE PROCEDURE Mode3.[test Mode 3 output captured via loopback]
AS
BEGIN
    /* Explicit regression lock for the INSERT...EXEC nesting workaround: the loopback
       capture must return the detail rowset (not the metadata set). */
    CREATE TABLE #actual ( row_count INT, k1 INT, v INT, view_data_sql NVARCHAR(MAX) );
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='Keys',
                                  @Mode=3, @ColumnList='k1,v', @ResultSetNo=2;

    DECLARE @groups INT = (SELECT COUNT(*) FROM #actual);
    EXEC tSQLt.AssertEquals 1, @groups;
    IF NOT EXISTS (SELECT 1 FROM #actual WHERE view_data_sql LIKE 'SELECT * FROM %')
        EXEC tSQLt.Fail 'view_data_sql was not populated — captured the wrong result set?';
END
GO
