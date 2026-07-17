/*───────────────────────────────────────────────────────────────────────────
  Mode0.sql  —  Table Overview (metadata + optional FK / index result sets)

  Result-set order per mode: 1 = metadata, 2 = column schema, then FK (if
  @ShowForeignKeys=1) and index (if @ShowIndexes=1) in that order.
───────────────────────────────────────────────────────────────────────────*/
USE [DataProfileTest];
GO
EXEC tSQLt.NewTestClass 'Mode0';
GO

CREATE PROCEDURE Mode0.[test Mode 0 overview row shape values for AllTypes]
AS
BEGIN
    CREATE TABLE #actual (
        column_id INT, name NVARCHAR(128), user_type NVARCHAR(128), system_type NVARCHAR(128),
        [length] NVARCHAR(50) NULL, [precision] INT, scale INT, is_nullable BIT, collation NVARCHAR(128) NULL
    );
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='AllTypes', @Mode=0, @ResultSetNo=2;

    SELECT name, system_type, is_nullable INTO #got FROM #actual;

    CREATE TABLE #exp (name NVARCHAR(128), system_type NVARCHAR(128), is_nullable BIT);
    INSERT INTO #exp VALUES
      ('c_int','int',0),('c_bigint','bigint',0),('c_decimal','decimal',0),('c_bit','bit',0),
      ('c_date','date',0),('c_time','time',0),('c_datetime2','datetime2',0),
      ('c_datetimeoffset','datetimeoffset',0),('c_smalldatetime','smalldatetime',0),
      ('c_datetime','datetime',0),('c_varchar','varchar',0),('c_nvarchar','nvarchar',0),
      ('c_uid','uniqueidentifier',0);

    EXEC tSQLt.AssertEqualsTable '#exp', '#got';
END
GO

CREATE PROCEDURE Mode0.[test Mode 0 with ShowForeignKeys emits FK rows for Parent Child]
AS
BEGIN
    /* FK is result set 3 when @ShowForeignKeys=1. */
    CREATE TABLE #actual (
        relationship_type NVARCHAR(60), fk_name NVARCHAR(128), parent_table NVARCHAR(128),
        parent_column_name NVARCHAR(128), parent_column_id INT, referrenced_table NVARCHAR(128),
        referrenced_column_name NVARCHAR(128), referenced_column_id INT
    );
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='Child',
                                  @Mode=0, @ShowForeignKeys=1, @ResultSetNo=3;

    DECLARE @fkRows INT = (SELECT COUNT(*) FROM #actual WHERE fk_name = 'FK_Child_Parent');
    EXEC tSQLt.AssertEquals 1, @fkRows;
END
GO

CREATE PROCEDURE Mode0.[test Mode 0 with ShowIndexes emits index rows]
AS
BEGIN
    /* With only @ShowIndexes=1, the index set is result set 3. Child has PK_Child. */
    CREATE TABLE #actual (
        name NVARCHAR(128), index_id INT, type_desc NVARCHAR(60), is_primary_key BIT, is_unique BIT,
        is_unique_constraint BIT, is_disabled BIT, fill_factor INT, index_columns NVARCHAR(MAX),
        included_columns NVARCHAR(MAX), filter_definition NVARCHAR(MAX)
    );
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='Child',
                                  @Mode=0, @ShowIndexes=1, @ResultSetNo=3;

    DECLARE @pkRows INT =
        (SELECT COUNT(*) FROM #actual WHERE name = 'PK_Child' AND is_primary_key = 1);
    EXEC tSQLt.AssertEquals 1, @pkRows;
END
GO

CREATE PROCEDURE Mode0.[test Mode 0 with both flags off omits FK and index result sets]
AS
BEGIN
    /* With both flags off, Mode 0 emits exactly 2 result sets; asking ResultSetFilter
       for set 3 must fail. (If your tSQLt returns empty instead of erroring, relax this
       to an AssertEmptyTable on a set-3 capture.) */
    CREATE TABLE #actual (a INT, b INT, c INT, d INT, e INT, f INT, g INT, h INT);
    EXEC tSQLt.ExpectException @Message = NULL;   -- any error is acceptable
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='Child', @Mode=0, @ResultSetNo=3;
END
GO
