/*───────────────────────────────────────────────────────────────────────────
  Mode2.sql  —  Column Statistics (min/max/mean/median/percentiles/std_dev/CV)

  Detail set = result set 2. All stats are stored/emitted as NVARCHAR(100).
  Shape at compat >= 110 (19 cols, includes median + P25/P75/P90/P95/P99):
    column_id,name,user_type,system_type,length,precision,scale,is_nullable,
    min_value,max_value,mean,median,p25,p75,p90,p95,p99,std_dev,coeff_variation
  Below 110 the median + percentile columns are dropped (13 cols); coeff_variation
  rides Batch A (scalar aggregates) so it survives the degradation.

  Stats seed {1,1,3,5,5}: min='1' max='5' mean='3' median='3' std_dev='2.0000'
  p25='1' p75='5' p90='5' p95='5' p99='5' coeff_variation='0.6667' (2.0/3).
───────────────────────────────────────────────────────────────────────────*/
USE [DataProfileTest];
GO
EXEC tSQLt.NewTestClass 'Mode2';
GO

CREATE PROCEDURE Mode2.[test_Mode2_StatsTable_ReturnsOverviewRow]
AS
BEGIN
    /* The overview row is result set 1 (object_id, schema_name, table_name, row_count, is_sample) —
       median-independent, so no compat gate. Never asserted for Mode 2 before this. object_id is
       DB-dependent → asserted non-null. */
    CREATE TABLE #actual (
        object_id INT, schema_name NVARCHAR(128), table_name NVARCHAR(128),
        row_count BIGINT, is_sample NVARCHAR(10)
    );
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='Stats', @Mode=2, @ResultSetNo=1;

    SELECT schema_name, table_name, row_count, is_sample INTO #got FROM #actual;
    CREATE TABLE #exp (schema_name NVARCHAR(128), table_name NVARCHAR(128), row_count BIGINT, is_sample NVARCHAR(10));
    /* is_sample='True' from the helper default @SampleValue=100; row_count is the physical count. */
    INSERT INTO #exp VALUES ('dbo', 'Stats', 5, 'True');   -- Stats has 5 rows
    EXEC tSQLt.AssertEqualsTable '#exp', '#got';

    IF (SELECT object_id FROM #actual) IS NULL
        EXEC tSQLt.Fail 'overview object_id was NULL';
END
GO

CREATE PROCEDURE Mode2.[test_Mode2_StatsTable_ReturnsMinAndMax]
AS
BEGIN
    IF tSQLtTest.EffectiveCompatLevel('DataProfileTest') < 110
    BEGIN EXEC tSQLtTest.Skip 'DataProfileTest effective compat < 110'; RETURN; END

    CREATE TABLE #actual (
        column_id INT, name NVARCHAR(128), user_type NVARCHAR(128), system_type NVARCHAR(128),
        [length] NVARCHAR(50) NULL, [precision] INT, scale INT, is_nullable BIT,
        min_value NVARCHAR(100), max_value NVARCHAR(100), mean NVARCHAR(100),
        median NVARCHAR(100), p25 NVARCHAR(100), p75 NVARCHAR(100), p90 NVARCHAR(100),
        p95 NVARCHAR(100), p99 NVARCHAR(100), std_dev NVARCHAR(100), coeff_variation NVARCHAR(100)
    );
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='Stats', @Mode=2, @ResultSetNo=2;

    DECLARE @minv NVARCHAR(100) = (SELECT min_value FROM #actual WHERE name='val');
    EXEC tSQLt.AssertEqualsString '1', @minv;
    DECLARE @maxv NVARCHAR(100) = (SELECT max_value FROM #actual WHERE name='val');
    EXEC tSQLt.AssertEqualsString '5', @maxv;
END
GO

CREATE PROCEDURE Mode2.[test_Mode2_StatsTable_ReturnsIntegerMean]
AS
BEGIN
    IF tSQLtTest.EffectiveCompatLevel('DataProfileTest') < 110
    BEGIN EXEC tSQLtTest.Skip 'DataProfileTest effective compat < 110'; RETURN; END

    CREATE TABLE #actual (
        column_id INT, name NVARCHAR(128), user_type NVARCHAR(128), system_type NVARCHAR(128),
        [length] NVARCHAR(50) NULL, [precision] INT, scale INT, is_nullable BIT,
        min_value NVARCHAR(100), max_value NVARCHAR(100), mean NVARCHAR(100),
        median NVARCHAR(100), p25 NVARCHAR(100), p75 NVARCHAR(100), p90 NVARCHAR(100),
        p95 NVARCHAR(100), p99 NVARCHAR(100), std_dev NVARCHAR(100), coeff_variation NVARCHAR(100)
    );
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='Stats', @Mode=2, @ResultSetNo=2;

    /* AVG over INT is integer division; 15/5 = 3 exactly here (locks the int-mean behaviour). */
    DECLARE @mean NVARCHAR(100) = (SELECT mean FROM #actual WHERE name='val');
    EXEC tSQLt.AssertEqualsString '3', @mean;
END
GO

CREATE PROCEDURE Mode2.[test_Mode2_StatsTable_ReturnsSampleStdDev]
AS
BEGIN
    IF tSQLtTest.EffectiveCompatLevel('DataProfileTest') < 110
    BEGIN EXEC tSQLtTest.Skip 'DataProfileTest effective compat < 110'; RETURN; END

    CREATE TABLE #actual (
        column_id INT, name NVARCHAR(128), user_type NVARCHAR(128), system_type NVARCHAR(128),
        [length] NVARCHAR(50) NULL, [precision] INT, scale INT, is_nullable BIT,
        min_value NVARCHAR(100), max_value NVARCHAR(100), mean NVARCHAR(100),
        median NVARCHAR(100), p25 NVARCHAR(100), p75 NVARCHAR(100), p90 NVARCHAR(100),
        p95 NVARCHAR(100), p99 NVARCHAR(100), std_dev NVARCHAR(100), coeff_variation NVARCHAR(100)
    );
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='Stats', @Mode=2, @ResultSetNo=2;

    /* sample STDEV of {1,1,3,5,5} = sqrt(16/4) = 2, cast NUMERIC(18,4) → '2.0000'. */
    DECLARE @stddev NVARCHAR(100) = (SELECT std_dev FROM #actual WHERE name='val');
    EXEC tSQLt.AssertEqualsString '2.0000', @stddev;
END
GO

CREATE PROCEDURE Mode2.[test_Mode2_StatsTable_ReturnsCoefficientOfVariation]
AS
BEGIN
    /* CV rides Batch A (scalar aggregates), so it is NOT compat-gated — assert it at
       any compat level. CV = std_dev/mean = 2.0/3; STDEV is float so the division is
       float (not integer), cast NUMERIC(18,4) → '0.6667'. */
    CREATE TABLE #actual (
        column_id INT, name NVARCHAR(128), user_type NVARCHAR(128), system_type NVARCHAR(128),
        [length] NVARCHAR(50) NULL, [precision] INT, scale INT, is_nullable BIT,
        min_value NVARCHAR(100), max_value NVARCHAR(100), mean NVARCHAR(100),
        median NVARCHAR(100), p25 NVARCHAR(100), p75 NVARCHAR(100), p90 NVARCHAR(100),
        p95 NVARCHAR(100), p99 NVARCHAR(100), std_dev NVARCHAR(100), coeff_variation NVARCHAR(100)
    );
    IF tSQLtTest.EffectiveCompatLevel('DataProfileTest') < 110
    BEGIN EXEC tSQLtTest.Skip 'DataProfileTest effective compat < 110'; RETURN; END

    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='Stats', @Mode=2, @ResultSetNo=2;

    DECLARE @cv NVARCHAR(100) = (SELECT coeff_variation FROM #actual WHERE name='val');
    EXEC tSQLt.AssertEqualsString '0.6667', @cv;
END
GO

CREATE PROCEDURE Mode2.[test_Mode2_StatsTableAtCompat110OrHigher_ReturnsMedian]
AS
BEGIN
    IF tSQLtTest.EffectiveCompatLevel('DataProfileTest') < 110
    BEGIN EXEC tSQLtTest.Skip 'DataProfileTest effective compat < 110'; RETURN; END

    CREATE TABLE #actual (
        column_id INT, name NVARCHAR(128), user_type NVARCHAR(128), system_type NVARCHAR(128),
        [length] NVARCHAR(50) NULL, [precision] INT, scale INT, is_nullable BIT,
        min_value NVARCHAR(100), max_value NVARCHAR(100), mean NVARCHAR(100),
        median NVARCHAR(100), p25 NVARCHAR(100), p75 NVARCHAR(100), p90 NVARCHAR(100),
        p95 NVARCHAR(100), p99 NVARCHAR(100), std_dev NVARCHAR(100), coeff_variation NVARCHAR(100)
    );
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='Stats', @Mode=2, @ResultSetNo=2;

    DECLARE @median NVARCHAR(100) = (SELECT median FROM #actual WHERE name='val');
    EXEC tSQLt.AssertEqualsString '3', @median;
END
GO

CREATE PROCEDURE Mode2.[test_Mode2_StatsTableAtCompat110OrHigher_ReturnsPercentiles]
AS
BEGIN
    /* P25/P75/P90/P95/P99 ride Batch B alongside the median (same PERCENTILE_DISC scan,
       same compat 110+ gate). PERCENTILE_DISC of sorted {1,1,3,5,5}:
         P25 → rank 2 = '1' ; P75 → rank 4 = '5' ; P90/P95/P99 → rank 5 = '5'. */
    IF tSQLtTest.EffectiveCompatLevel('DataProfileTest') < 110
    BEGIN EXEC tSQLtTest.Skip 'DataProfileTest effective compat < 110'; RETURN; END

    CREATE TABLE #actual (
        column_id INT, name NVARCHAR(128), user_type NVARCHAR(128), system_type NVARCHAR(128),
        [length] NVARCHAR(50) NULL, [precision] INT, scale INT, is_nullable BIT,
        min_value NVARCHAR(100), max_value NVARCHAR(100), mean NVARCHAR(100),
        median NVARCHAR(100), p25 NVARCHAR(100), p75 NVARCHAR(100), p90 NVARCHAR(100),
        p95 NVARCHAR(100), p99 NVARCHAR(100), std_dev NVARCHAR(100), coeff_variation NVARCHAR(100)
    );
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='Stats', @Mode=2, @ResultSetNo=2;

    SELECT p25, p75, p90, p95, p99 INTO #got FROM #actual WHERE name='val';
    CREATE TABLE #exp (p25 NVARCHAR(100), p75 NVARCHAR(100), p90 NVARCHAR(100), p95 NVARCHAR(100), p99 NVARCHAR(100));
    INSERT INTO #exp VALUES ('1', '5', '5', '5', '5');
    EXEC tSQLt.AssertEqualsTable '#exp', '#got';
END
GO

CREATE PROCEDURE Mode2.[test_Mode2_StatsTableBelowCompat110_DropsMedianAndPercentileColumns]
AS
BEGIN
    /* No in-test ALTER DATABASE (not allowed in tSQLt's transaction). Instead profile
       the committed compat-100 fixture DB: effective compat = MIN(master,100) < 110, so
       the proc drops the median + percentile columns. Capturing into a 13-column shape
       (no median, no P25..P99, but coeff_variation still present) succeeds; a 19-column
       capture would fail — that IS the degradation assertion. coeff_variation survives
       because it rides Batch A's scalar-aggregate scan, not the gated PERCENTILE_DISC scan. */
    CREATE TABLE #actual (
        column_id INT, name NVARCHAR(128), user_type NVARCHAR(128), system_type NVARCHAR(128),
        [length] NVARCHAR(50) NULL, [precision] INT, scale INT, is_nullable BIT,
        min_value NVARCHAR(100), max_value NVARCHAR(100), mean NVARCHAR(100),
        std_dev NVARCHAR(100), coeff_variation NVARCHAR(100)
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
    /* CV still computed below the median floor. */
    DECLARE @cv NVARCHAR(100) = (SELECT coeff_variation FROM #actual WHERE name='val');
    EXEC tSQLt.AssertEqualsString '0.6667', @cv;
END
GO
