/*───────────────────────────────────────────────────────────────────────────
  04_capture_helper.sql  —  tSQLtTest.CaptureProfile

  The single place that knows HOW to capture a sp_DataProfile result set into a
  table. Tests pass parameters (table, mode, columns, which result set); they
  never see connection strings or the Mode-3 workaround.

  Why this exists
  ---------------
  sp_DataProfile emits TWO result sets per mode: a metadata set (result set 1)
  then the detail set (result set 2). Two problems:
    (a) A plain INSERT...EXEC can't capture a single set (the two sets have
        different shapes), and OPENROWSET returns only the first set.
    (b) EVERY mode runs an internal INSERT...EXEC — the shared metadata load at
        sp_DataProfile.sql:328-340 (INSERT INTO #table_column_profile ... EXEC),
        plus Mode 3's own at :856. So wrapping the proc in an outer
        INSERT...EXEC (via `INSERT INTO <target> EXEC tSQLt.ResultSetFilter`)
        nests and fails with "An INSERT EXEC statement cannot be nested" — for
        ALL modes, not just Mode 3.

  Mechanism
  ---------
  Every capture runs the proc TOP-LEVEL on a separate session via a loopback
  OPENROWSET, so the proc's internal INSERT...EXEC has no active outer
  INSERT...EXEC to nest under. tSQLt.ResultSetFilter (CLR) isolates the requested
  result set; wrapping ResultSetFilter (not the proc) is what makes OPENROWSET
  surface set <n> instead of only the first set.

  Two things make the loopback actually work:
    - Database= in the connection string, so the loopback session runs in the DB
      that hosts tSQLt.ResultSetFilter (this test DB). Without it the session
      lands in the login's default DB and can't find ResultSetFilter.
    - WITH RESULT SETS on the remote call. OPENROWSET must describe the remote
      command's shape up front (sp_describe_first_result_set), which CANNOT
      introspect a CLR proc. We hand it an explicit shape derived from <target>'s
      own columns — which already match the captured set by construction (the
      final INSERT INTO <target> requires it), so no per-test schema is needed.

  @SampleValue defaults to 100 (no TABLESAMPLE) so captured values are
  deterministic. @ForceLoopback is retained for compatibility but is now a no-op:
  every mode is captured via the loopback.
───────────────────────────────────────────────────────────────────────────*/
USE [DataProfileTest];
GO

IF SCHEMA_ID('tSQLtTest') IS NULL EXEC('CREATE SCHEMA tSQLtTest;');
GO

IF OBJECT_ID('tSQLtTest.CaptureProfile') IS NOT NULL DROP PROCEDURE tSQLtTest.CaptureProfile;
GO

/* OPENROWSET (ad hoc distributed query) requires QUOTED_IDENTIFIER ON in the
   executing batch. A proc captures these SET options at CREATE time, so force
   them on here — otherwise every capture fails with "SET options have incorrect
   settings: 'QUOTED_IDENTIFIER'". */
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO
CREATE PROCEDURE tSQLtTest.CaptureProfile
    @TargetTable      NVARCHAR(300),            -- pre-created table (e.g. '#actual') to fill
    @TableName        NVARCHAR(500),
    @Mode             TINYINT        = 0,
    @ResultSetNo      INT            = 2,        -- 1 = metadata set, 2 = detail set
    @ColumnList       NVARCHAR(4000) = NULL,
    @DatabaseName     NVARCHAR(128)  = N'DataProfileTest',
    @ShowForeignKeys  BIT            = 0,
    @ShowIndexes      BIT            = 0,
    @ShowConstraints  BIT            = 0,
    @ApproxDistinct   BIT            = 0,
    @CategoricalMaxDistinct INT      = 50,       -- Mode 1 cardinality-classification threshold
    @SampleValue      INT            = 100,      -- 100 = full table, no TABLESAMPLE
    @SampleType       NVARCHAR(50)   = N'PERCENT',
    @ForceLoopback    BIT            = NULL,     -- retained for compatibility; no-op (always loopback)
    @LoopbackServer   NVARCHAR(200)  = N'(local)',
    @LoopbackProvider NVARCHAR(60)   = N'SQLNCLI', -- try 'MSOLEDBSQL' or 'SQLNCLI11' on newer hosts
    @LoopbackDatabase NVARCHAR(128)  = NULL       -- DB hosting tSQLt.ResultSetFilter; defaults to current DB
AS
BEGIN
    SET NOCOUNT ON;

    /* Build the sp_DataProfile call. String params use N'...' with quotes doubled. */
    DECLARE @ProcCall NVARCHAR(MAX) =
          N'EXEC master.dbo.sp_DataProfile '
        + N'@TableName = N''' + REPLACE(@TableName, '''', '''''') + N''''
        + N', @Mode = ' + CAST(@Mode AS NVARCHAR(3))
        + N', @DatabaseName = N''' + REPLACE(@DatabaseName, '''', '''''') + N''''
        + N', @SampleValue = ' + CAST(@SampleValue AS NVARCHAR(10))
        + N', @SampleType = N''' + REPLACE(@SampleType, '''', '''''') + N''''
        + N', @ShowForeignKeys = ' + CAST(@ShowForeignKeys AS NVARCHAR(1))
        + N', @ShowIndexes = ' + CAST(@ShowIndexes AS NVARCHAR(1))
        + N', @ShowConstraints = ' + CAST(@ShowConstraints AS NVARCHAR(1))
        + N', @ApproxDistinct = ' + CAST(@ApproxDistinct AS NVARCHAR(1))
        + N', @CategoricalMaxDistinct = ' + CAST(@CategoricalMaxDistinct AS NVARCHAR(10))
        + CASE WHEN @ColumnList IS NULL THEN N''
               ELSE N', @ColumnList = N''' + REPLACE(@ColumnList, '''', '''''') + N'''' END;

    /* Derive a WITH RESULT SETS shape from the target table's own columns. The
       target already matches the captured result set (the final INSERT requires
       it), and the explicit shape is what lets OPENROWSET describe a CLR proc. */
    DECLARE @ObjId INT = OBJECT_ID(N'tempdb..' + @TargetTable);
    IF @ObjId IS NULL
        RAISERROR('CaptureProfile: target table %s not found in tempdb.', 16, 1, @TargetTable);

    DECLARE @ColDefs NVARCHAR(MAX);
    SELECT @ColDefs = STUFF((
        SELECT N', ' + QUOTENAME(c.name) + N' ' + t.name
             + CASE
                 WHEN t.name IN ('varchar','char','varbinary','binary')
                      THEN N'(' + CASE WHEN c.max_length = -1 THEN N'max'
                                       ELSE CAST(c.max_length AS NVARCHAR(10)) END + N')'
                 WHEN t.name IN ('nvarchar','nchar')
                      THEN N'(' + CASE WHEN c.max_length = -1 THEN N'max'
                                       ELSE CAST(c.max_length / 2 AS NVARCHAR(10)) END + N')'
                 WHEN t.name IN ('decimal','numeric')
                      THEN N'(' + CAST(c.precision AS NVARCHAR(10)) + N',' + CAST(c.scale AS NVARCHAR(10)) + N')'
                 WHEN t.name IN ('datetime2','time','datetimeoffset')
                      THEN N'(' + CAST(c.scale AS NVARCHAR(10)) + N')'
                 ELSE N''
               END
             + N' NULL'   -- declare nullable regardless: values, not nullability, are asserted
        FROM tempdb.sys.columns c
        JOIN tempdb.sys.types   t ON t.user_type_id = c.user_type_id
        WHERE c.object_id = @ObjId
        ORDER BY c.column_id
        FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, N'');

    /* Loopback: run ResultSetFilter on a separate session via OPENROWSET, so the
       proc's internal INSERT...EXEC (shared metadata load; Mode 3) runs top-level
       and does not nest. WITH RESULT SETS gives OPENROWSET the shape it can't get
       from a CLR proc. Quoting layers: ProcCall → ResultSetFilter arg → OPENROWSET. */
    DECLARE @remoteCmd NVARCHAR(MAX) =
          N'EXEC tSQLt.ResultSetFilter ' + CAST(@ResultSetNo AS NVARCHAR(10))
        + N', ''' + REPLACE(@ProcCall, '''', '''''') + N''''
        + N' WITH RESULT SETS ( ( ' + @ColDefs + N' ) );';

    DECLARE @Db NVARCHAR(128) = COALESCE(@LoopbackDatabase, DB_NAME());

    DECLARE @sql NVARCHAR(MAX) =
          N'INSERT INTO ' + @TargetTable + N' SELECT * FROM OPENROWSET('
        + N'''' + @LoopbackProvider + N''', '
        + N'''Server=' + @LoopbackServer + N';Database=' + @Db + N';Trusted_Connection=yes;'', '
        + N'''' + REPLACE(@remoteCmd, '''', '''''') + N''');';
    EXEC (@sql);
END
GO

RAISERROR('tSQLtTest.CaptureProfile created in DataProfileTest.', 0, 1) WITH NOWAIT;
GO
