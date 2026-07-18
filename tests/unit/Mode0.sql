/*───────────────────────────────────────────────────────────────────────────
  Mode0.sql  —  Table Overview (metadata + optional FK / index result sets)

  Result-set order per mode: 1 = overview row, 2 = column schema, then FK (if
  @ShowForeignKeys=1) and index (if @ShowIndexes=1) in that order.

  Assertions here favour content (tSQLt.AssertEqualsTable against hand-computed
  expected rows) over COUNT(*) so the values under test — not just row presence —
  are actually verified. Captures use the helper default @SampleValue=100 (no
  TABLESAMPLE) for determinism. object_id is DB-dependent so it is asserted
  non-null rather than as a literal.
───────────────────────────────────────────────────────────────────────────*/
USE [DataProfileTest];
GO
EXEC tSQLt.NewTestClass 'Mode0';
GO

CREATE PROCEDURE Mode0.[test_Mode0_AllTypes_ReturnsExpectedRowShapeValues]
AS
BEGIN
    /* Column schema is result set 2. */
    CREATE TABLE #actual (
        column_id INT, name NVARCHAR(128), user_type NVARCHAR(128), system_type NVARCHAR(128),
        [length] NVARCHAR(50) NULL, [precision] INT, scale INT, is_nullable BIT, collation NVARCHAR(128) NULL
    );
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='AllTypes', @Mode=0, @ResultSetNo=2;

    /* (a) The full per-column set: name → user_type/system_type, plus is_nullable (all NOT NULL
           here → 0; the is_nullable=1 case is covered by the Nullable test below). All AllTypes
           columns use built-in types, so user_type = system_type for every row. */
    SELECT name, user_type, system_type, is_nullable INTO #gotSet FROM #actual;
    CREATE TABLE #expSet (name NVARCHAR(128), user_type NVARCHAR(128), system_type NVARCHAR(128), is_nullable BIT);
    INSERT INTO #expSet VALUES
      ('c_int','int','int',0),('c_bigint','bigint','bigint',0),('c_decimal','decimal','decimal',0),
      ('c_bit','bit','bit',0),('c_date','date','date',0),('c_time','time','time',0),
      ('c_datetime2','datetime2','datetime2',0),('c_datetimeoffset','datetimeoffset','datetimeoffset',0),
      ('c_smalldatetime','smalldatetime','smalldatetime',0),('c_datetime','datetime','datetime',0),
      ('c_varchar','varchar','varchar',0),('c_nvarchar','nvarchar','nvarchar',0),
      ('c_uid','uniqueidentifier','uniqueidentifier',0);
    EXEC tSQLt.AssertEqualsTable '#expSet', '#gotSet';

    /* (b) The schema-detail columns Test 1 previously discarded: length (the byte/char length the
           proc reports — nvarchar is halved at sp_DataProfile.sql:305), precision, scale, and
           whether a collation is present (string types have one, others NULL). Collation *name*
           is server-dependent, so assert only its presence as a bit. */
    SELECT name, [length], [precision], scale,
           has_collation = CASE WHEN collation IS NULL THEN 0 ELSE 1 END
    INTO #gotDetail
    FROM #actual
    WHERE name IN ('c_int','c_decimal','c_varchar','c_nvarchar');
    CREATE TABLE #expDetail (
        name NVARCHAR(128), [length] NVARCHAR(50) NULL, [precision] INT, scale INT, has_collation INT
    );
    INSERT INTO #expDetail VALUES
      ('c_int',      '4',  10, 0, 0),   -- int: 4 bytes, precision 10
      ('c_decimal',  '5',   9, 2, 0),   -- decimal(9,2): 5 bytes storage, precision 9 scale 2
      ('c_varchar',  '50',  0, 0, 1),   -- varchar(50): length 50, has collation
      ('c_nvarchar', '50',  0, 0, 1);   -- nvarchar(50): 100 bytes /2 = 50, has collation
    EXEC tSQLt.AssertEqualsTable '#expDetail', '#gotDetail';
END
GO

CREATE PROCEDURE Mode0.[test_Mode0_AllTypes_ReturnsOverviewRow]
AS
BEGIN
    /* The overview row is result set 1 — Mode 0's headline output. Pins schema/table/row_count
       and the is_sample 'True'/'False' formatting. object_id is DB-dependent → asserted non-null. */
    CREATE TABLE #actual (
        object_id INT, schema_name NVARCHAR(128), table_name NVARCHAR(128),
        row_count BIGINT, is_sample NVARCHAR(10)
    );
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='AllTypes', @Mode=0, @ResultSetNo=1;

    SELECT schema_name, table_name, row_count, is_sample INTO #got FROM #actual;
    CREATE TABLE #exp (schema_name NVARCHAR(128), table_name NVARCHAR(128), row_count BIGINT, is_sample NVARCHAR(10));
    /* is_sample = 'True' even though the capture uses @SampleValue=100: the proc sets @IsSample=1
       whenever @SampleValue IS NOT NULL (sp_DataProfile.sql:165-168), so a 100% "sample" is still
       flagged as sampled and a TABLESAMPLE(100 PERCENT) is applied (all rows, deterministic).
       This pins CURRENT behaviour; whether a 100% sample should report 'False' is a design call. */
    INSERT INTO #exp VALUES ('dbo', 'AllTypes', 3, 'True');   -- AllTypes has 3 rows (100% sample returns all)
    EXEC tSQLt.AssertEqualsTable '#exp', '#got';

    IF (SELECT object_id FROM #actual) IS NULL
        EXEC tSQLt.Fail 'overview object_id was NULL';
END
GO

CREATE PROCEDURE Mode0.[test_Mode0_NullableTable_ReturnsIsNullablePerColumn]
AS
BEGIN
    /* Exercises the is_nullable=1 path that AllTypes (all NOT NULL) cannot. Nullable: id NOT NULL,
       s/soft nullable. */
    CREATE TABLE #actual (
        column_id INT, name NVARCHAR(128), user_type NVARCHAR(128), system_type NVARCHAR(128),
        [length] NVARCHAR(50) NULL, [precision] INT, scale INT, is_nullable BIT, collation NVARCHAR(128) NULL
    );
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='Nullable', @Mode=0, @ResultSetNo=2;

    SELECT name, is_nullable INTO #got FROM #actual;
    CREATE TABLE #exp (name NVARCHAR(128), is_nullable BIT);
    INSERT INTO #exp VALUES ('id', 0), ('s', 1), ('soft', 1);
    EXEC tSQLt.AssertEqualsTable '#exp', '#got';
END
GO

CREATE PROCEDURE Mode0.[test_Mode0_ShowForeignKeysOnChild_EmitsForeignKeyRows]
AS
BEGIN
    /* FK is result set 3 when @ShowForeignKeys=1. Assert the whole row so the relationship's
       content (which table references which, on which columns, in which direction) is verified,
       not just that a row with the FK name exists.

       Child is the *parent_object* of the FK (it holds parent_id, referencing Parent), so from
       Child's perspective this is an OUTGOING foreign key — see the 'Outgoing' branch at
       sp_DataProfile.sql:405 (WHERE tp.name = @TableName). */
    CREATE TABLE #actual (
        relationship_type NVARCHAR(60), fk_name NVARCHAR(128), parent_table NVARCHAR(128),
        parent_column_name NVARCHAR(128), parent_column_id INT, referrenced_table NVARCHAR(128),
        referrenced_column_name NVARCHAR(128), referenced_column_id INT
    );
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='Child',
                                  @Mode=0, @ShowForeignKeys=1, @ResultSetNo=3;

    SELECT relationship_type, fk_name, parent_table, parent_column_name, parent_column_id,
           referrenced_table, referrenced_column_name, referenced_column_id
    INTO #got FROM #actual;
    CREATE TABLE #exp (
        relationship_type NVARCHAR(60), fk_name NVARCHAR(128), parent_table NVARCHAR(128),
        parent_column_name NVARCHAR(128), parent_column_id INT, referrenced_table NVARCHAR(128),
        referrenced_column_name NVARCHAR(128), referenced_column_id INT
    );
    INSERT INTO #exp VALUES
      ('Outgoing', 'FK_Child_Parent', 'Child', 'parent_id', 2, 'Parent', 'parent_id', 1);
    EXEC tSQLt.AssertEqualsTable '#exp', '#got';
END
GO

CREATE PROCEDURE Mode0.[test_Mode0_ShowIndexesOnChild_EmitsIndexRows]
AS
BEGIN
    /* With only @ShowIndexes=1, the index set is result set 3. Child has one index, PK_Child
       (clustered PK on child_id). Assert the whole row so the index_columns concatenation
       (STUFF/FOR XML at sp_DataProfile.sql:473-484) and the flags are actually verified. */
    CREATE TABLE #actual (
        name NVARCHAR(128), index_id INT, type_desc NVARCHAR(60), is_primary_key BIT, is_unique BIT,
        is_unique_constraint BIT, is_disabled BIT, fill_factor INT, index_columns NVARCHAR(MAX),
        included_columns NVARCHAR(MAX), filter_definition NVARCHAR(MAX)
    );
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='Child',
                                  @Mode=0, @ShowIndexes=1, @ResultSetNo=3;

    SELECT name, index_id, type_desc, is_primary_key, is_unique, is_unique_constraint,
           is_disabled, fill_factor, index_columns, included_columns, filter_definition
    INTO #got FROM #actual;
    CREATE TABLE #exp (
        name NVARCHAR(128), index_id INT, type_desc NVARCHAR(60), is_primary_key BIT, is_unique BIT,
        is_unique_constraint BIT, is_disabled BIT, fill_factor INT, index_columns NVARCHAR(MAX),
        included_columns NVARCHAR(MAX), filter_definition NVARCHAR(MAX)
    );
    /* index_columns = 'child_id ASC' (no leading space): the STUFF at sp_DataProfile.sql:484 uses
       length 2 to strip the full ', ' separator. A value assertion (vs the old COUNT(*)) is what
       pins this — a length-1 regression would reintroduce a leading space and fail here. */
    INSERT INTO #exp VALUES
      ('PK_Child', 1, 'CLUSTERED', 1, 1, 0, 0, 0, 'child_id ASC', NULL, NULL);
    EXEC tSQLt.AssertEqualsTable '#exp', '#got';
END
GO

CREATE PROCEDURE Mode0.[test_Mode0_BothFlagsOff_OmitsFkAndIndexResultSets]
AS
BEGIN
    /* Two parts:
       (1) Positive control — with both flags off, result set 2 is still the column schema (proves
           the capture path works AND that no FK/index set leaked into position 2). Child has 2
           columns.
       (2) Negative — result set 3 must not exist, so asking ResultSetFilter for set 3 errors.
           Because (1) just proved the same loopback path works for set 2, a failure here is
           attributable to the missing result set, not to broken plumbing (which would have failed
           the control first). */
    CREATE TABLE #schema (
        column_id INT, name NVARCHAR(128), user_type NVARCHAR(128), system_type NVARCHAR(128),
        [length] NVARCHAR(50) NULL, [precision] INT, scale INT, is_nullable BIT, collation NVARCHAR(128) NULL
    );
    EXEC tSQLtTest.CaptureProfile @TargetTable='#schema', @TableName='Child', @Mode=0, @ResultSetNo=2;

    SELECT name INTO #gotCols FROM #schema;
    CREATE TABLE #expCols (name NVARCHAR(128));
    INSERT INTO #expCols VALUES ('child_id'), ('parent_id');
    EXEC tSQLt.AssertEqualsTable '#expCols', '#gotCols';   -- set 2 is the column schema

    /* Now set 3 is out of range → the capture must throw. */
    CREATE TABLE #actual (a INT, b INT, c INT, d INT, e INT, f INT, g INT, h INT);
    EXEC tSQLt.ExpectException @Message = NULL;   -- any error is acceptable; the control above rules out plumbing failures
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='Child', @Mode=0, @ResultSetNo=3;
END
GO

CREATE PROCEDURE Mode0.[test_Mode0_BothFlagsOn_FkIsSet3AndIndexIsSet4]
AS
BEGIN
    /* Locks the documented result-set ordering: with both flags on, FK = set 3, index = set 4.
       Row content is already asserted by the dedicated FK/index tests above; here we only prove
       each set lands at the expected position. */
    CREATE TABLE #fk (
        relationship_type NVARCHAR(60), fk_name NVARCHAR(128), parent_table NVARCHAR(128),
        parent_column_name NVARCHAR(128), parent_column_id INT, referrenced_table NVARCHAR(128),
        referrenced_column_name NVARCHAR(128), referenced_column_id INT
    );
    EXEC tSQLtTest.CaptureProfile @TargetTable='#fk', @TableName='Child',
                                  @Mode=0, @ShowForeignKeys=1, @ShowIndexes=1, @ResultSetNo=3;
    IF (SELECT COUNT(*) FROM #fk WHERE fk_name = 'FK_Child_Parent') <> 1
        EXEC tSQLt.Fail 'FK_Child_Parent was not at result set 3 with both flags on';

    CREATE TABLE #ix (
        name NVARCHAR(128), index_id INT, type_desc NVARCHAR(60), is_primary_key BIT, is_unique BIT,
        is_unique_constraint BIT, is_disabled BIT, fill_factor INT, index_columns NVARCHAR(MAX),
        included_columns NVARCHAR(MAX), filter_definition NVARCHAR(MAX)
    );
    EXEC tSQLtTest.CaptureProfile @TargetTable='#ix', @TableName='Child',
                                  @Mode=0, @ShowForeignKeys=1, @ShowIndexes=1, @ResultSetNo=4;
    IF (SELECT COUNT(*) FROM #ix WHERE name = 'PK_Child' AND is_primary_key = 1) <> 1
        EXEC tSQLt.Fail 'PK_Child was not at result set 4 with both flags on';
END
GO
