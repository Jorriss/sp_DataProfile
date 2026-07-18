/*───────────────────────────────────────────────────────────────────────────
  Mode1.sql  —  Column Detail (distinct/cardinality, nulls, soft-nulls, min/max)

  Detail set = result set 2. Shape (24 cols):
    column_id, name, user_type, system_type, length, precision, scale, is_nullable,
    num_unique_values(BIGINT), unique_ratio(DEC(25,5)), cardinality(NVARCHAR),
    num_nulls(BIGINT), nulls_ratio(DEC(25,5)),
    num_blank(BIGINT), blank_ratio(DEC(25,5)), num_whitespace(BIGINT), whitespace_ratio(DEC(25,5)),
    num_zero(BIGINT), zero_ratio(DEC(25,5)), num_negative(BIGINT), negative_ratio(DEC(25,5)),
    min_length(INT), max_length(INT), min_value(NVARCHAR), max_value(NVARCHAR)
  Tests capture the full shape then project the deterministic columns.
───────────────────────────────────────────────────────────────────────────*/
USE [DataProfileTest];
GO
EXEC tSQLt.NewTestClass 'Mode1';
GO

/* Reusable capture into a full-shape #actual is inlined per test (temp tables
   don't survive a helper), but the shape is identical everywhere below. */

CREATE PROCEDURE Mode1.[test_Mode1_NullableTable_ReturnsOverviewRow]
AS
BEGIN
    /* The overview row is result set 1 (object_id, schema_name, table_name, row_count, is_sample).
       Never asserted for Mode 1 before this. object_id is DB-dependent → asserted non-null. */
    CREATE TABLE #actual (
        object_id INT, schema_name NVARCHAR(128), table_name NVARCHAR(128),
        row_count BIGINT, is_sample NVARCHAR(10)
    );
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='Nullable', @Mode=1, @ResultSetNo=1;

    SELECT schema_name, table_name, row_count, is_sample INTO #got FROM #actual;
    CREATE TABLE #exp (schema_name NVARCHAR(128), table_name NVARCHAR(128), row_count BIGINT, is_sample NVARCHAR(10));
    /* is_sample='True': the helper default @SampleValue=100 sets @IsSample=1 (a 100% sample is still
       flagged as sampled); row_count is the physical count, unaffected by the sample. */
    INSERT INTO #exp VALUES ('dbo', 'Nullable', 6, 'True');   -- Nullable has 6 rows
    EXEC tSQLt.AssertEqualsTable '#exp', '#got';

    IF (SELECT object_id FROM #actual) IS NULL
        EXEC tSQLt.Fail 'overview object_id was NULL';
END
GO

CREATE PROCEDURE Mode1.[test_Mode1_NullableTable_ReturnsNumNullsPerColumn]
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
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='Nullable', @Mode=1, @ResultSetNo=2;

    /* Pins is_nullable (both states here: id NOT NULL → 0, s/soft nullable → 1), num_nulls, and the
       derived nulls_ratio = num_nulls / num_rows (DECIMAL(25,5), sp_DataProfile.sql:259). NOT NULL
       columns report num_nulls = NULL → nulls_ratio = NULL (not 0). soft's unique_ratio is left
       unasserted — trailing-space collation makes its distinct count ambiguous (see fixture note). */
    SELECT name, is_nullable, num_nulls, nulls_ratio INTO #got FROM #actual;
    CREATE TABLE #exp (name NVARCHAR(128), is_nullable BIT, num_nulls BIGINT, nulls_ratio DECIMAL(25,5));
    INSERT INTO #exp VALUES
      ('id',   0, NULL, NULL),      -- NOT NULL → num_nulls/nulls_ratio NULL
      ('s',    1, 2,    0.33333),   -- 2 nulls / 6 rows
      ('soft', 1, 1,    0.16667);   -- 1 null  / 6 rows
    EXEC tSQLt.AssertEqualsTable '#exp', '#got';
END
GO

CREATE PROCEDURE Mode1.[test_Mode1_CardinalityTable_ReturnsDistinctCountPerColumn]
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
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='Cardinality', @Mode=1, @ResultSetNo=2;

    /* Also pins the derived unique_ratio = num_unique_values / num_rows (DECIMAL(25,5),
       sp_DataProfile.sql:257). All Cardinality columns are NOT NULL, so the counts are unambiguous. */
    SELECT name, num_unique_values, unique_ratio INTO #got FROM #actual;
    CREATE TABLE #exp (name NVARCHAR(128), num_unique_values BIGINT, unique_ratio DECIMAL(25,5));
    INSERT INTO #exp VALUES
      ('const_col', 1, 0.16667),   -- 1 / 6
      ('bin_col',   2, 0.33333),   -- 2 / 6
      ('uniq_col',  6, 1.00000),   -- 6 / 6
      ('cat_col',   3, 0.50000);   -- 3 / 6
    EXEC tSQLt.AssertEqualsTable '#exp', '#got';
END
GO

CREATE PROCEDURE Mode1.[test_Mode1_AllTypesStringColumns_ReturnsMinMaxLength]
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
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='AllTypes', @Mode=1, @ResultSetNo=2;

    SELECT name, min_length, max_length INTO #got FROM #actual WHERE name IN ('c_varchar','c_nvarchar');
    CREATE TABLE #exp (name NVARCHAR(128), min_length INT, max_length INT);
    INSERT INTO #exp VALUES ('c_varchar',1,3),('c_nvarchar',1,3);
    EXEC tSQLt.AssertEqualsTable '#exp', '#got';
END
GO

CREATE PROCEDURE Mode1.[test_Mode1_AllTypesSupportedTypes_ReturnsAllRowsWithoutError]
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
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='AllTypes', @Mode=1, @ResultSetNo=2;
    DECLARE @rows INT = (SELECT COUNT(*) FROM #actual);
    EXEC tSQLt.AssertEquals 13, @rows;
END
GO

CREATE PROCEDURE Mode1.[test_Mode1_ColumnListSpecified_RestrictsOutputToNamedColumns]
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
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='Cardinality',
                                  @Mode=1, @ColumnList='cat_col', @ResultSetNo=2;

    /* Assert the single expected row rather than COUNT+name: AssertEqualsTable implicitly asserts
       exactly one row (proving the column restriction) AND pins its computed values. */
    SELECT name, num_unique_values, unique_ratio, num_nulls, nulls_ratio, is_nullable INTO #got FROM #actual;
    CREATE TABLE #exp (
        name NVARCHAR(128), num_unique_values BIGINT, unique_ratio DECIMAL(25,5),
        num_nulls BIGINT, nulls_ratio DECIMAL(25,5), is_nullable BIT
    );
    INSERT INTO #exp VALUES ('cat_col', 3, 0.50000, NULL, NULL, 0);   -- cat_col: NOT NULL, 3 distinct / 6
    EXEC tSQLt.AssertEqualsTable '#exp', '#got';
END
GO

CREATE PROCEDURE Mode1.[test_Mode1_NullableTable_ReturnsBlankAndWhitespaceCounts]
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
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='Nullable', @Mode=1, @ResultSetNo=2;

    /* soft = {'x','','   ',NULL,'0','y'}: one empty ('' → num_blank) and one all-space
       ('   ' → num_whitespace). Also pins the derived ratios (count / 6 rows) and proves
       the counts are NULL for a numeric column (id) and 0 for a clean string column (s). */
    SELECT name, num_blank, blank_ratio, num_whitespace, whitespace_ratio INTO #got FROM #actual;
    CREATE TABLE #exp (
        name NVARCHAR(128), num_blank BIGINT, blank_ratio DECIMAL(25,5),
        num_whitespace BIGINT, whitespace_ratio DECIMAL(25,5)
    );
    INSERT INTO #exp VALUES
      ('id',   NULL, NULL,    NULL, NULL),      -- numeric → soft-string counts NULL
      ('s',    0,    0.00000, 0,    0.00000),   -- string, no blanks/whitespace
      ('soft', 1,    0.16667, 1,    0.16667);   -- 1 blank + 1 whitespace / 6 rows
    EXEC tSQLt.AssertEqualsTable '#exp', '#got';
END
GO

CREATE PROCEDURE Mode1.[test_Mode1_SoftNumbersTable_ReturnsZeroAndNegativeCounts]
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
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='SoftNumbers', @Mode=1, @ResultSetNo=2;

    /* n = {0,-5,3,0,-1}; n_nullable = {0,NULL,-2,4,0}. Zero/negative counts skip NULLs;
       num_blank is NULL on numeric columns. Ratios are count / 5 rows. */
    SELECT name, num_zero, zero_ratio, num_negative, negative_ratio, num_blank INTO #got FROM #actual;
    CREATE TABLE #exp (
        name NVARCHAR(128), num_zero BIGINT, zero_ratio DECIMAL(25,5),
        num_negative BIGINT, negative_ratio DECIMAL(25,5), num_blank BIGINT
    );
    INSERT INTO #exp VALUES
      ('n',          2, 0.40000, 2, 0.40000, NULL),   -- zeros rows 1,4; negs rows 2,5
      ('n_nullable', 2, 0.40000, 1, 0.20000, NULL);   -- zeros rows 1,5; neg row 3 (NULL skipped)
    EXEC tSQLt.AssertEqualsTable '#exp', '#got';
END
GO

CREATE PROCEDURE Mode1.[test_Mode1_CardinalityTable_ClassifiesCardinality]
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
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='Cardinality', @Mode=1, @ResultSetNo=2;

    /* Default @CategoricalMaxDistinct=50 over 6 rows. */
    SELECT name, cardinality INTO #got FROM #actual;
    CREATE TABLE #exp (name NVARCHAR(128), cardinality NVARCHAR(30));
    INSERT INTO #exp VALUES
      ('const_col', 'Constant'),        -- distinct 1
      ('bin_col',   'Binary'),          -- distinct 2
      ('uniq_col',  'Unique'),          -- distinct 6 = num_rows
      ('cat_col',   'Categorical');     -- distinct 3 <= 50
    EXEC tSQLt.AssertEqualsTable '#exp', '#got';
END
GO

CREATE PROCEDURE Mode1.[test_Mode1_LowThreshold_ReclassifiesCategoricalAsHighCardinality]
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
    /* @CategoricalMaxDistinct=2: cat_col (distinct 3) is now above the threshold and not
       Constant/Binary/Unique, so it flips to High-cardinality — proves the param path. */
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='Cardinality',
                                  @Mode=1, @CategoricalMaxDistinct=2, @ResultSetNo=2;

    SELECT name, cardinality INTO #got FROM #actual;
    CREATE TABLE #exp (name NVARCHAR(128), cardinality NVARCHAR(30));
    INSERT INTO #exp VALUES
      ('const_col', 'Constant'),
      ('bin_col',   'Binary'),
      ('uniq_col',  'Unique'),
      ('cat_col',   'High-cardinality');   -- distinct 3 > 2
    EXEC tSQLt.AssertEqualsTable '#exp', '#got';
END
GO

CREATE PROCEDURE Mode1.[test_Mode1_NullableTable_ReturnsMinMaxStringValue]
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
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='Nullable', @Mode=1, @ResultSetNo=2;

    /* s = {'apple',NULL,'apple','pear',NULL,'kiwi'} → alphabetical MIN 'apple', MAX 'pear'.
       min/max value are NULL for the numeric id column. */
    SELECT name, min_value, max_value INTO #got FROM #actual WHERE name IN ('id','s');
    CREATE TABLE #exp (name NVARCHAR(128), min_value NVARCHAR(100), max_value NVARCHAR(100));
    INSERT INTO #exp VALUES
      ('id', NULL,    NULL),
      ('s',  'apple', 'pear');
    EXEC tSQLt.AssertEqualsTable '#exp', '#got';
END
GO
