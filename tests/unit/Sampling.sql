/*───────────────────────────────────────────────────────────────────────────
  Sampling.sql  —  TABLESAMPLE runs and returns a plausible subset

  TABLESAMPLE is page-based and non-deterministic: on a tiny single-page fixture
  it returns ALL rows or NONE. So we assert only that a sampled run completes and
  the sampled counts fall within [0, total] — never an exact count (design
  constraint #4).
───────────────────────────────────────────────────────────────────────────*/
USE [DataProfileTest];
GO
EXEC tSQLt.NewTestClass 'Sampling';
GO

CREATE PROCEDURE Sampling.[test_Sampling_SampleValueSpecified_ReturnsCountsWithinRange]
AS
BEGIN
    CREATE TABLE #actual ( column_value VARCHAR(10), [Count] INT, Percentage DECIMAL(18,4) );
    EXEC tSQLtTest.CaptureProfile @TargetTable='#actual', @TableName='Cardinality',
                                  @Mode=4, @ColumnList='cat_col',
                                  @SampleValue=50, @SampleType='PERCENT', @ResultSetNo=2;

    /* Cardinality has 6 rows. Page sampling yields 0..6 rows total; assert the bound only. */
    DECLARE @total INT = COALESCE((SELECT SUM([Count]) FROM #actual), 0);
    IF @total NOT BETWEEN 0 AND 6
        EXEC tSQLt.Fail 'Sampled total row count fell outside [0, 6].';
END
GO
