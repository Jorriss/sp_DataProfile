USE [master]
GO

IF OBJECT_ID('dbo.sp_DataProfile') IS NOT NULL 
  DROP PROCEDURE dbo.sp_DataProfile;
GO

CREATE PROCEDURE dbo.sp_DataProfile
   @TableName NVARCHAR(500) ,
   @Mode TINYINT = 0 , /* 0 = Table Overview, 1 = Column Detail, 2 = Column Statistics, 3 = Candidate Key Check, 4 = Column Value Distribution */
   @ColumnList NVARCHAR(4000) = NULL ,
   @DatabaseName NVARCHAR(128) = NULL ,
   @ShowForeignKeys BIT = 0 ,
   @ShowIndexes BIT = 0 ,
   @SampleValue INT = NULL ,
   @SampleType NVARCHAR(50) = 'PERCENT' ,
   @ExactRowCount BIT = 0 ,
   @ApproxDistinct BIT = 0 ,
   @CategoricalMaxDistinct INT = 50 ,
   @Verbose BIT = 0
/*
sp_DataProfile v0.5 - Jul 18, 2026

(C) 2026, Jorriss LLC
Released under the MIT License. See the LICENSE file for details.

Source is located at: https://github.com/Jorriss/sp_DataProfile

How to use:
Mode:
0 = Table Overview 
1 = Column Detail - Number Unique Values, Cardinality classification, Number Nulls,
    soft-null counts (blank/whitespace/zero/negative) + ratios, Min/Max Len, Min/Max Value
2 = Column Statistics - Min, Max, Mean, Median, Standard Deviation
3 = Candidate Key Check - You need a @ColumnList with this
4 = Column Value Distribution - You need to provide a single column name in @ColumnList. If more than one is provided only the first one is used.

You can use @ShowIndexes = 1 and @ShowForeignKeys = 1 in any mode to see all of the indexes and foreign keys.

@CategoricalMaxDistinct (default 50) tunes the Mode 1 cardinality classification: an eligible column
whose distinct count is <= this value (and that isn't Constant/Binary/Unique) is labeled 'Categorical';
above it, 'High-cardinality'. Lower it to be stricter about what counts as categorical.

Example usage:
Table Overview
sp_dataprofile 'Users', 0;

Table Overview with Indexes and FKs
sp_dataprofile 'Users', 0, @ShowIndexes=1, @ShowForeignKeys=1;

Column Detail
sp_dataprofile 'Users', 1

Column Statistics - Only using 10% of the table values
sp_dataprofile 'Users', 2, @SampleValue = 10

Candidate Key Check
sp_dataprofile 'Users', 3, 'DisplayName, Location, WebsiteUrl, CreationDate'

Column Value Distribution
sp_dataprofile 'Posts', 4, 'PostTypeId'

Sampling note:
When @SampleValue is supplied, sampling is done with TABLESAMPLE, which is page-based (it
returns all rows from a random set of pages, not a random set of rows). As a result num_rows,
the distinct/unique counts, the null counts, and any ratio columns derived from them reflect
the SAMPLE, not the full table. Omit @SampleValue for exact, full-table results.

*/
AS
BEGIN
  SET NOCOUNT ON;
  SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

  DECLARE @SQLString NVARCHAR(MAX);
  DECLARE @SQLStringFK NVARCHAR(MAX);
  DECLARE @SQLStringIndexes NVARCHAR(MAX);
  DECLARE @Schema NVARCHAR(100);
  DECLARE @SchemaPosition INT;
  DECLARE @Msg NVARCHAR(4000);
  DECLARE @ErrorSeverity INT;
  DECLARE @ErrorState INT;
  DECLARE @RowCount BIGINT;
  DECLARE @IsSample BIT = 0;
  DECLARE @TableSample NVARCHAR(300) = '';
  DECLARE @FromTableName NVARCHAR(300) = '';
  DECLARE @FromTableNameClean NVARCHAR(300) = '';   -- object name only, no TABLESAMPLE (for OBJECT_ID)
  DECLARE @ColumnListString NVARCHAR(MAX);
  DECLARE @ColumnNameFirst NVARCHAR(4000);
  DECLARE @SQLServerVersion NVARCHAR(100) = '';
  DECLARE @SQLCompatLevelMaster INT;
  DECLARE @SQLCompatLevelDB INT;
  DECLARE @SQLCompatLevelDBOut INT;
  DECLARE @SQLCompatLevel INT;
  DECLARE @SQLMajorVersion INT;
  DECLARE @ViewSQLDataString NVARCHAR(4000);

  BEGIN TRY

    /* Get that SQL Server Version son! 2012 or older up in here! */
    SELECT @SQLServerVersion = CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(128));

    SET @SQLMajorVersion = CAST(LEFT(@SQLServerVersion, CHARINDEX('.', @SQLServerVersion, 0) - 1) AS INT);

    IF @SQLMajorVersion < 11
    BEGIN
      SET @Msg = N'I''m sorry Dave. I can''t run on your version of SQL Server. I require a SQL Server 2012 and higher. The version of this instance is: ' + @SQLServerVersion + '. I promise I won''t open the airlock.';
      RAISERROR(@msg, 16, 1);
    END

    IF @DatabaseName IS NULL
      SET @DatabaseName = DB_NAME();

    /* Get Compat Level. We're going to use this later to determine if we can do the wierd stuff. */
    SELECT @SQLCompatLevelMaster = compatibility_level FROM sys.databases WHERE name = 'master';

    SET @SQLString = N'
      SELECT @SQLCompatLevelDBOut = compatibility_level 
      FROM sys.databases WHERE name = ''' + @DatabaseName + ''';'
    
    IF @SQLString IS NULL 
      RAISERROR('@SQLString is null', 16, 1);
    
    EXEC sp_executesql @SQLString, N'@SQLCompatLevelDBOut INT OUTPUT', @SQLCompatLevelDBOut = @SQLCompatLevelDB OUTPUT;
    
    IF @SQLCompatLevelMaster < @SQLCompatLevelDB 
      SET @SQLCompatLevel = @SQLCompatLevelMaster;
    ELSE
      SET @SQLCompatLevel = @SQLCompatLevelDB;
    
    IF @SQLCompatLevel < 110
    BEGIN 
      SET @Msg = 'Your compatibility level of ' + CAST(@SQLCompatLevel AS NVARCHAR(10)) + ' is a bit too low. I can''t perform median calculations for compatibility levels lower than 110. If this is unacceptable to you feel free to write the median calculation yourself. I accept pull requests. ;)';
      RAISERROR(@Msg, 0, 1);
    END

    SET @Schema = PARSENAME(@TableName, 2);
    SET @TableName = PARSENAME(@TableName, 1);
  
    IF @Schema IS NULL 
      SET @Schema = 'dbo';

    IF @Mode NOT IN (0, 1, 2, 3, 4) 
    BEGIN
      SET @Msg = 'Mode values should only be 0, 1, 2, 3 or 4. 0 = Table Overview, 1 = Table Detail, 2 = Column Statistics, 3 = Candidate Key Check, 4 = Column Value Distribution';
      RAISERROR(@Msg, 1, 1);
      RETURN;
    END 

    IF (@Mode IN (3, 4)) AND (@ColumnList IS NULL OR @ColumnList = '') 
    BEGIN      
      SET @Msg = 'It looks like you didn''t provide a ColumnList. A ColumnList is required for the Candidate Key Check and the Column Value Distribution modes.';
      RAISERROR(@Msg, 1, 1);
      RETURN;
    END 
  
    IF @SampleType NOT IN ('ROWS', 'PERCENT') 
    BEGIN
      SET @Msg = 'Did you mistype the SampleType value? SampleType should be either ''ROWS'' or ''PERCENT''';
      RAISERROR(@Msg, 1, 1);
      RETURN;
    END 

    IF @SampleValue < 0 OR @SampleValue > 100 
    BEGIN
      SET @Msg = 'Whoops. The SampleValue should be between 0 and 100.';
      RAISERROR(@Msg, 1, 1);
      RETURN;
    END

    IF @SampleValue IS NOT NULL
    BEGIN
      SET @IsSample = 1;
      SET @TableSample = ' TABLESAMPLE (' + CAST(@SampleValue AS NVARCHAR(3)) + ' ' + @SampleType + ') REPEATABLE(100) ';
    END

    SET @FromTableName      = QUOTENAME(@Schema) + '.' + QUOTENAME(@TableName) + @TableSample;
    SET @FromTableNameClean = QUOTENAME(@Schema) + '.' + QUOTENAME(@TableName);

    If DB_NAME() <> @DatabaseName
    BEGIN
      SET @FromTableName      = QUOTENAME(@DatabaseName) + '.' + @FromTableName;
      SET @FromTableNameClean = QUOTENAME(@DatabaseName) + '.' + @FromTableNameClean;
    END

    /* Format ColumnList  */
    DECLARE @ColumnListClean NVARCHAR(MAX);
    DECLARE @ColumnListComma NVARCHAR(MAX);
    DECLARE @CommaPos  INT;
    DECLARE @CommaPart NVARCHAR(MAX);
    
    SET @ColumnListComma = @ColumnList;
    SET @ColumnListClean = '';
    
    IF RIGHT(RTRIM(@ColumnListComma), 1) <> N','
      SET @ColumnListComma = @ColumnListComma + N',';
    
    SET @CommaPos =  PATINDEX(N'%,%', @ColumnListComma);
    WHILE @CommaPos <> 0 
    BEGIN
      SET @CommaPart = LEFT(@ColumnListComma, @CommaPos - 1);
      SET @ColumnListClean = @ColumnListClean + LTRIM(RTRIM(@CommaPart)) + ',';
      SET @ColumnListComma = STUFF(@ColumnListComma, 1, @CommaPos, '');
      SET @CommaPos = PATINDEX(N'%,%', @ColumnListComma);
    END
    
    SET @ColumnList = @ColumnListClean;
    
    IF RIGHT(@ColumnList, 1) = ','
      SET @ColumnList = LEFT(@ColumnList, LEN(@ColumnList) - 1);
    SET @ColumnListString = '''' + REPLACE(@ColumnList, ',', ''',''') + '''';

    IF @Verbose = 1
    BEGIN
      SET @Msg = N'ColumnListstring: ' + @ColumnList
      RAISERROR (@Msg, 0, 1) WITH NOWAIT;
    END

    /* Build a column-name filter set so Mode 1 can honor @ColumnList and
       profile only the requested columns instead of every column. When no
       @ColumnList is supplied the set stays empty and every column is profiled. */
    IF OBJECT_ID ('tempdb..#column_filter') IS NOT NULL
      DROP TABLE #column_filter;

    CREATE TABLE #column_filter ( col_name NVARCHAR(500) NOT NULL );

    IF @ColumnList IS NOT NULL AND @ColumnList <> ''
    BEGIN
      DECLARE @cfRemaining NVARCHAR(MAX);
      DECLARE @cfPos       INT;

      SET @cfRemaining = @ColumnList + N',';
      SET @cfPos = PATINDEX(N'%,%', @cfRemaining);

      WHILE @cfPos <> 0
      BEGIN
        SET @CommaPart = LTRIM(RTRIM(LEFT(@cfRemaining, @cfPos - 1)));

        IF @CommaPart <> ''
          INSERT INTO #column_filter (col_name) VALUES (@CommaPart);

        SET @cfRemaining = STUFF(@cfRemaining, 1, @cfPos, '');
        SET @cfPos = PATINDEX(N'%,%', @cfRemaining);
      END
    END

    IF OBJECT_ID ('tempdb..#table_column_profile') IS NOT NULL
      DROP TABLE #table_column_profile;
    
    CREATE TABLE #table_column_profile (
      [object_id]          INT           NOT NULL ,
      [column_id]          INT           NOT NULL , 
      [name]               NVARCHAR(128) NOT NULL , 
      [system_type]        NVARCHAR(100) NOT NULL ,
      [user_type]          NVARCHAR(100) NOT NULL ,
      [collation]          NVARCHAR(100) NULL ,
      [length]             INTEGER       NULL ,
      [precision]          INTEGER       NULL ,
      [scale]              INTEGER       NULL ,
      [is_nullable]        BIT           NOT NULL ,
      [num_rows]           BIGINT        NULL ,
      [num_unique_values]  BIGINT        NULL ,
      [unique_ratio] AS CAST((CAST([num_unique_values] AS DECIMAL(25,5)) / ISNULL(NULLIF([num_rows], 0), 1)) AS DECIMAL(25,5)) ,
      [num_nulls]          BIGINT        NULL ,
      [nulls_ratio] AS CAST((CAST([num_nulls] AS DECIMAL(25,5)) / ISNULL(NULLIF([num_rows], 0), 1)) AS DECIMAL(25,5)) ,
      [num_blank]          BIGINT        NULL ,
      [blank_ratio] AS CAST((CAST([num_blank] AS DECIMAL(25,5)) / ISNULL(NULLIF([num_rows], 0), 1)) AS DECIMAL(25,5)) ,
      [num_whitespace]     BIGINT        NULL ,
      [whitespace_ratio] AS CAST((CAST([num_whitespace] AS DECIMAL(25,5)) / ISNULL(NULLIF([num_rows], 0), 1)) AS DECIMAL(25,5)) ,
      [num_zero]           BIGINT        NULL ,
      [zero_ratio] AS CAST((CAST([num_zero] AS DECIMAL(25,5)) / ISNULL(NULLIF([num_rows], 0), 1)) AS DECIMAL(25,5)) ,
      [num_negative]       BIGINT        NULL ,
      [negative_ratio] AS CAST((CAST([num_negative] AS DECIMAL(25,5)) / ISNULL(NULLIF([num_rows], 0), 1)) AS DECIMAL(25,5)) ,
      [min_length]         INT           NULL ,
      [max_length]         INT           NULL ,
      [min_value]          NVARCHAR(100) NULL ,
      [max_value]          NVARCHAR(100) NULL ,
      [mean]               NVARCHAR(100) NULL ,
      [median]             NVARCHAR(100) NULL ,
      [std_dev]            NVARCHAR(100) NULL
    );

    CREATE TABLE #table_relationship (
     [relationship_type]         NVARCHAR(25)  NULL ,
     [fk_name]                   NVARCHAR(128) NOT NULL ,
     [parent_table]              NVARCHAR(128) NOT NULL ,
     [parent_column_name]        NVARCHAR(128) NOT NULL ,
     [parent_column_id]          INT           NULL ,
     [referrenced_table]         NVARCHAR(128) NOT NULL ,
     [referrenced_column_name]   NVARCHAR(128) NOT NULL ,
     [referenced_column_id]      INT           NULL
    );
  
    CREATE TABLE #table_indexes (
      [name]                 NVARCHAR(128)  NOT NULL ,
      [index_id]             INT            NULL ,
      [type_desc]            NVARCHAR(60)   NULL ,
      [is_primary_key]       BIT            NULL ,
      [is_unique]            BIT            NULL ,
      [is_unique_constraint] BIT            NULL ,
      [is_disabled]          BIT            NULL ,
      [fill_factor]          TINYINT        NULL ,
      [index_columns]        NVARCHAR(max)  NULL ,
      [included_columns]     NVARCHAR(max)  NULL ,
      [filter_definition]    NVARCHAR(max)
    );

    /* Inserting data into #table_column_profile */
    SET @SQLString = N'
      SELECT t.object_id ,
             c.column_id ,
             c.name ,
             sys.name,
             typ.name ,
             c.collation_name ,
             CAST(
               CASE  
                 WHEN c.max_length = -1 THEN c.max_length
                 WHEN sys.name = ''nvarchar'' THEN c.max_length / 2
                 WHEN sys.name = ''nchar'' THEN c.max_length / 2
                 ELSE c.max_length
               END
             AS NVARCHAR(100)) AS max_length ,
             c.precision ,
             c.scale ,
             c.is_nullable
    FROM   ' + QUOTENAME(@DatabaseName) + '.sys.tables  t
    JOIN   ' + QUOTENAME(@DatabaseName) + '.sys.columns c   ON  t.object_id = c.object_id
    JOIN   ' + QUOTENAME(@DatabaseName) + '.sys.types   typ ON  c.system_type_id = typ.system_type_id
                                                            AND c.user_type_id = typ.user_type_id
    JOIN   ' + QUOTENAME(@DatabaseName) + '.sys.types   sys ON  typ.system_type_id = sys.system_type_id
                                                            AND sys.user_type_id = sys.system_type_id
    JOIN   ' + QUOTENAME(@DatabaseName) + '.sys.schemas s   ON  t.schema_id = s.schema_id
                                                            AND s.name = ''' + @Schema + '''
    WHERE  t.name = ''' + @TableName + '''
    ORDER BY c.column_id;'

    IF @Verbose = 1
    BEGIN
      RAISERROR (N'Inserting data into #table_column_profile', 0, 1) WITH NOWAIT;
      RAISERROR (@SQLString, 0, 1) WITH NOWAIT;
    END

    IF @SQLString IS NULL 
      RAISERROR('@SQLString is null', 16, 1);

    INSERT INTO #table_column_profile (
      [object_id] ,
      [column_id] ,
      [name] ,
      [system_type] ,
      [user_type] ,
      [collation] ,
      [length] ,
      [precision] ,
      [scale] ,
      [is_nullable]
    ) 
    EXEC sp_executesql @SQLString;
  
    /* Update actual row count.
       Default path reads the row count from metadata (near-instant, no scan).
       Sampling or @ExactRowCount = 1 forces a real COUNT_BIG(*) over the (sampled) table. */
    IF @IsSample = 1 OR @ExactRowCount = 1
      SET @SQLString = N'
        UPDATE #table_column_profile
        SET num_rows = cnt
        FROM (SELECT COUNT_BIG(*) cnt
              FROM ' + @FromTableName + ') tablecount ;'
    ELSE
      SET @SQLString = N'
        UPDATE #table_column_profile
        SET num_rows = cnt
        FROM (SELECT SUM(ps.row_count) cnt
              FROM ' + QUOTENAME(@DatabaseName) + '.sys.dm_db_partition_stats ps
              WHERE ps.object_id = OBJECT_ID(''' + QUOTENAME(@DatabaseName) + '.' + QUOTENAME(@Schema) + '.' + QUOTENAME(@TableName) + ''')
              AND   ps.index_id IN (0,1)) tablecount ;'

    IF @Verbose = 1
    BEGIN
      RAISERROR (N'Updating data in #table_column_profile for table row counts', 0, 1) WITH NOWAIT;
      RAISERROR (@SQLString, 0, 1) WITH NOWAIT;
    END
    
    IF @SQLString IS NULL 
      RAISERROR('@SQLString is null', 16, 1);

    EXEC sp_executesql @SQLString;
      
    SELECT TOP 1 @RowCount = num_rows FROM #table_column_profile;
    
    /* Insert FK data into #table_relationship */
    IF @ShowForeignKeys = 1
    BEGIN

      SET @SQLString = N'
        SELECT      relationship_type = ''Incoming'',
                    fk_name = fk.name ,
                    parent_table = tp.name ,
                    parent_column_name = cp.name , 
                    parent_column_id = cp.column_id ,
                    referrenced_table = tr.name ,
                    referrenced_column_name = cr.name , 
                    referenced_column_id = cr.column_id
        FROM        ' + QUOTENAME(@DatabaseName) + '.sys.foreign_keys        fk
        JOIN        ' + QUOTENAME(@DatabaseName) + '.sys.tables              tp  ON  fk.parent_object_id = tp.object_id
        LEFT JOIN   ' + QUOTENAME(@DatabaseName) + '.sys.tables              tr  ON  fk.referenced_object_id = tr.object_id
        JOIN        ' + QUOTENAME(@DatabaseName) + '.sys.foreign_key_columns fkc ON  fkc.constraint_object_id = fk.object_id
        JOIN        ' + QUOTENAME(@DatabaseName) + '.sys.columns             cp  ON  fkc.parent_column_id = cp.column_id 
                                                AND fkc.parent_object_id = cp.object_id
        JOIN        ' + QUOTENAME(@DatabaseName) + '.sys.columns             cr  ON  fkc.referenced_column_id = cr.column_id 
                                                AND fkc.referenced_object_id = cr.object_id
        JOIN        ' + QUOTENAME(@DatabaseName) + '.sys.schemas             s   ON  tr.schema_id = s.schema_id
                                                                                 AND s.name = ''' + @Schema + '''
        WHERE       tr.name = ''' + @TableName + '''
        
        UNION ALL
        
        SELECT      RelationshipType = ''Outgoing'',
                    FKName = fk.name ,
                    ParentTable = tp.name ,
                    ParentColumnName = cp.name , 
                    ParentColumnID = cp.column_id ,
                    ReferencedTable = tr.name ,
                    ReferencedColumnName = cr.name , 
                    ReferencedColumnID = cr.column_id
        FROM        ' + QUOTENAME(@DatabaseName) + '.sys.foreign_keys        fk
        JOIN        ' + QUOTENAME(@DatabaseName) + '.sys.tables              tp  ON  fk.parent_object_id = tp.object_id
        LEFT JOIN   ' + QUOTENAME(@DatabaseName) + '.sys.tables              tr  ON  fk.referenced_object_id = tr.object_id
        JOIN        ' + QUOTENAME(@DatabaseName) + '.sys.foreign_key_columns fkc ON  fkc.constraint_object_id = fk.object_id
        JOIN        ' + QUOTENAME(@DatabaseName) + '.sys.columns             cp  ON  fkc.parent_column_id = cp.column_id AND fkc.parent_object_id = cp.object_id
        JOIN        ' + QUOTENAME(@DatabaseName) + '.sys.columns             cr  ON  fkc.referenced_column_id = cr.column_id AND fkc.referenced_object_id = cr.object_id
        JOIN        ' + QUOTENAME(@DatabaseName) + '.sys.schemas             s   ON  tp.schema_id = s.schema_id
                                                                                 AND s.name = ''' + @Schema + '''
        WHERE       tp.name = ''' + @TableName + ''''
    
      IF @Verbose = 1
      BEGIN
        RAISERROR (N'Insert FK data into #table_relationship', 0, 1) WITH NOWAIT;
        RAISERROR (@SQLString, 0, 1) WITH NOWAIT;
      END
    
      IF @SQLString IS NULL 
        RAISERROR('@SQLString is null', 16, 1);

      INSERT INTO #table_relationship (
        [relationship_type] ,
        [fk_name] ,
        [parent_table] ,
        [parent_column_name] ,
        [parent_column_id] ,
        [referrenced_table] ,
        [referrenced_column_name] ,
        [referenced_column_id]
      )
      EXEC sp_executesql @SQLString;

      SET @SQLStringFK = N'
        SELECT    [relationship_type] ,
                  [fk_name] ,
                  [parent_table] ,
                  [parent_column_name] ,
                  [parent_column_id] ,
                  [referrenced_table] ,
                  [referrenced_column_name] ,
                  [referenced_column_id]
        FROM      #table_relationship
        ORDER BY  relationship_type ,
                  parent_table ,
                  fk_name ;'

    END

    /* Insert Index data into #table_indexes */
    IF @ShowIndexes = 1
    BEGIN

      SET @SQLString = N'
        SELECT     i.name , 
                   i.index_id ,
                   i.type_desc ,
                   i.is_primary_key ,
                   i.is_unique ,
                   i.is_unique_constraint ,
                   i.is_disabled ,
                   i.fill_factor ,
                   index_columns = 
                    (SELECT STUFF(
                      (SELECT '', '' +  c.name + CASE WHEN ic.is_descending_key = 1 THEN '' DESC'' ELSE '' ASC'' END 
                       FROM   ' + QUOTENAME(@DatabaseName) + '.sys.index_columns ic
                       JOIN   ' + QUOTENAME(@DatabaseName) + '.sys.columns       c   ON  ic.object_id = c.object_id
                                                    AND ic.column_id = c.column_id
                       WHERE  i.object_id = ic.object_id
                       AND    i.index_id = ic.index_id
                       AND    ic.is_included_column = 0
                       ORDER BY ic.index_column_id
                       FOR XML PATH (''''))
                     , 1, 2, '''') ) ,
                   included_columns = 
                    (SELECT STUFF(
                      (SELECT '', '' +  c.name 
                      FROM   ' + QUOTENAME(@DatabaseName) + '.sys.index_columns ic
                      JOIN   ' + QUOTENAME(@DatabaseName) + '.sys.columns       c   ON  ic.object_id = c.object_id
                                                   AND ic.column_id = c.column_id
                      WHERE  i.object_id = ic.object_id
                      AND    i.index_id = ic.index_id
                      AND    ic.is_included_column = 1
                      ORDER BY ic.index_column_id
                      FOR XML PATH (''''))
                    , 1, 2, '''') ) ,
                   i.filter_definition
          FROM     ' + QUOTENAME(@DatabaseName) + '.sys.indexes       i
          WHERE    i.object_id = OBJECT_ID(''' + @FromTableNameClean + ''')
          ORDER BY i.index_id'
    
      IF @Verbose = 1
      BEGIN
        RAISERROR (N'Insert index data into #table_indexes', 0, 1) WITH NOWAIT;
        RAISERROR (@SQLString, 0, 1) WITH NOWAIT;
      END
    
      INSERT INTO #table_indexes (
        [name] ,
        [index_id] ,
        [type_desc] ,
        [is_primary_key] , 
        [is_unique] ,
        [is_unique_constraint] ,
        [is_disabled] ,
        [fill_factor] ,
        [index_columns] ,
        [included_columns] ,
        [filter_definition]
      )        
      EXEC sp_executesql @SQLString;

      IF @SQLString IS NULL 
        RAISERROR('@SQLString is null', 16, 1);

      SET @SQLStringIndexes = N'
        SELECT    [name] ,
                  [index_id] ,
                  [type_desc] ,
                  [is_primary_key] , 
                  [is_unique] ,
                  [is_unique_constraint] ,
                  [is_disabled] ,
                  [fill_factor] ,
                  [index_columns] ,
                  [included_columns] ,
                  [filter_definition]
        FROM      #table_indexes
        ORDER BY  index_id ;'        
    END

    IF @Mode = 1 /* Table Detail */
    BEGIN
      /* Single-pass rewrite: instead of one UPDATE (one table scan) per metric per
         column, build ONE wide aggregate SELECT that computes every Mode 1 metric for
         every eligible column, materialize it into a 1-row #agg (the only base-table
         scan), then reshape that row into #table_column_profile via CROSS APPLY (VALUES).
         Turns 100+ scans on a wide table into a single scan. */

      DECLARE @m1_col_name NVARCHAR(500) ,
              @m1_col_id   INTEGER ,
              @m1_col_type NVARCHAR(100) ,
              @m1_len      INTEGER ,
              @m1_nullable BIT;

      DECLARE @AggSelect NVARCHAR(MAX) = N'' ,  /* comma-separated aggregate expressions for #agg */
              @Unpivot   NVARCHAR(MAX) = N'' ,  /* comma-separated VALUES rows for the reshape */
              @m1_qn     NVARCHAR(300) ,
              @m1_cid    NVARCHAR(10) ,
              @uOK       BIT ,
              @nOK       BIT ,
              @lOK       BIT ,
              @vOK       BIT ,
              @numOK     BIT;

      DECLARE m1_cur CURSOR
        LOCAL STATIC FORWARD_ONLY READ_ONLY FOR
          SELECT p.name ,
                 p.column_id ,
                 p.system_type ,
                 p.length ,
                 p.is_nullable
          FROM   #table_column_profile p
          /* Honor @ColumnList when supplied; profile all columns when it isn't. */
          WHERE  (NOT EXISTS (SELECT 1 FROM #column_filter)
                  OR p.name IN (SELECT col_name FROM #column_filter));

      OPEN m1_cur;

      FETCH NEXT FROM m1_cur INTO @m1_col_name, @m1_col_id, @m1_col_type, @m1_len, @m1_nullable;

      WHILE @@FETCH_STATUS = 0
      BEGIN
        SET @m1_qn  = QUOTENAME(@m1_col_name);
        SET @m1_cid = CAST(@m1_col_id AS NVARCHAR(10));

        /* num_unique_values: valid types only, skipping LOB (max) where COUNT(DISTINCT) is expensive. */
        SET @uOK = CASE WHEN @m1_col_type IN ('uniqueidentifier', 'date', 'time', 'datetime2', 'datetimeoffset', 'tinyint', 'smallint', 'int', 'smalldatetime', 'real', 'money', 'datetime', 'float', 'sql_variant', 'bit', 'decimal', 'numeric', 'smallmoney', 'bigint', 'varbinary', 'varchar', 'binary', 'char', 'timestamp', 'nvarchar', 'nchar')
                        AND NOT (@m1_col_type IN ('nvarchar', 'varchar', 'varbinary') AND @m1_len = -1)
                        THEN 1 ELSE 0 END;
        /* num_nulls: nullable columns of any type. */
        SET @nOK = @m1_nullable;
        /* min/max length + blank/whitespace counts: string types only (LEN/DATALENGTH are valid on max). */
        SET @lOK = CASE WHEN @m1_col_type IN ('varchar', 'char', 'nvarchar', 'nchar') THEN 1 ELSE 0 END;
        /* min/max string VALUE: string types EXCLUDING (max) — MIN/MAX aggregates are invalid on LOB. */
        SET @vOK = CASE WHEN @lOK = 1 AND @m1_len <> -1 THEN 1 ELSE 0 END;
        /* zero/negative counts: numeric types only (bit excluded — boolean, not a quantity). */
        SET @numOK = CASE WHEN @m1_col_type IN ('tinyint', 'smallint', 'int', 'bigint', 'decimal', 'numeric', 'money', 'smallmoney', 'float', 'real') THEN 1 ELSE 0 END;

        IF @uOK = 1 OR @nOK = 1 OR @lOK = 1 OR @numOK = 1
        BEGIN
          IF @uOK = 1
            SET @AggSelect = @AggSelect + CASE WHEN @AggSelect <> N'' THEN N', ' ELSE N'' END
              + N'CAST(' + CASE WHEN @ApproxDistinct = 1 AND @SQLMajorVersion >= 15
                                THEN N'APPROX_COUNT_DISTINCT(' + @m1_qn + N')'
                                ELSE N'COUNT(DISTINCT ' + @m1_qn + N')' END
              + N' AS BIGINT) AS u' + @m1_cid;

          IF @nOK = 1
            SET @AggSelect = @AggSelect + CASE WHEN @AggSelect <> N'' THEN N', ' ELSE N'' END
              + N'CAST(COUNT_BIG(CASE WHEN ' + @m1_qn + N' IS NULL THEN 1 END) AS BIGINT) AS n' + @m1_cid;

          IF @lOK = 1
            /* min/max length; blank (empty) and whitespace-only (soft-null) counts; min/max
               string value truncated to 100. Note: SQL Server ignores trailing spaces in
               comparisons, so col = '' matches whitespace-only strings too — distinguish with
               DATALENGTH (truly empty) vs LEN = 0 AND DATALENGTH > 0 (all-space, non-empty).
               LEN strips trailing spaces only, so this catches space chars, not tabs/newlines. */
            SET @AggSelect = @AggSelect + CASE WHEN @AggSelect <> N'' THEN N', ' ELSE N'' END
              + N'CAST(MIN(LEN(' + @m1_qn + N')) AS INT) AS mnl' + @m1_cid
              + N', CAST(MAX(LEN(' + @m1_qn + N')) AS INT) AS mxl' + @m1_cid
              + N', CAST(COUNT_BIG(CASE WHEN DATALENGTH(' + @m1_qn + N') = 0 THEN 1 END) AS BIGINT) AS bl' + @m1_cid
              + N', CAST(COUNT_BIG(CASE WHEN LEN(' + @m1_qn + N') = 0 AND DATALENGTH(' + @m1_qn + N') > 0 THEN 1 END) AS BIGINT) AS ws' + @m1_cid;

          IF @vOK = 1
            /* min/max string value, truncated to 100 chars to fit min_value/max_value. */
            SET @AggSelect = @AggSelect + CASE WHEN @AggSelect <> N'' THEN N', ' ELSE N'' END
              + N'CAST(LEFT(MIN(' + @m1_qn + N'), 100) AS NVARCHAR(100)) AS mnv' + @m1_cid
              + N', CAST(LEFT(MAX(' + @m1_qn + N'), 100) AS NVARCHAR(100)) AS mxv' + @m1_cid;

          IF @numOK = 1
            SET @AggSelect = @AggSelect + CASE WHEN @AggSelect <> N'' THEN N', ' ELSE N'' END
              + N'CAST(COUNT_BIG(CASE WHEN ' + @m1_qn + N' = 0 THEN 1 END) AS BIGINT) AS z' + @m1_cid
              + N', CAST(COUNT_BIG(CASE WHEN ' + @m1_qn + N' < 0 THEN 1 END) AS BIGINT) AS ng' + @m1_cid;

          /* Matching VALUES row. Typed NULLs (never bare NULL) keep the unpivoted
             columns single-typed regardless of which metrics a column qualifies for. */
          SET @Unpivot = @Unpivot + CASE WHEN @Unpivot <> N'' THEN N',
            ' ELSE N'' END
            + N'(' + @m1_cid + N', '
            + CASE WHEN @uOK   = 1 THEN N'a.u'   + @m1_cid ELSE N'CAST(NULL AS BIGINT)'        END + N', '
            + CASE WHEN @nOK   = 1 THEN N'a.n'   + @m1_cid ELSE N'CAST(NULL AS BIGINT)'        END + N', '
            + CASE WHEN @lOK   = 1 THEN N'a.mnl' + @m1_cid ELSE N'CAST(NULL AS INT)'           END + N', '
            + CASE WHEN @lOK   = 1 THEN N'a.mxl' + @m1_cid ELSE N'CAST(NULL AS INT)'           END + N', '
            + CASE WHEN @lOK   = 1 THEN N'a.bl'  + @m1_cid ELSE N'CAST(NULL AS BIGINT)'        END + N', '
            + CASE WHEN @lOK   = 1 THEN N'a.ws'  + @m1_cid ELSE N'CAST(NULL AS BIGINT)'        END + N', '
            + CASE WHEN @numOK = 1 THEN N'a.z'   + @m1_cid ELSE N'CAST(NULL AS BIGINT)'        END + N', '
            + CASE WHEN @numOK = 1 THEN N'a.ng'  + @m1_cid ELSE N'CAST(NULL AS BIGINT)'        END + N', '
            + CASE WHEN @vOK   = 1 THEN N'a.mnv' + @m1_cid ELSE N'CAST(NULL AS NVARCHAR(100))' END + N', '
            + CASE WHEN @vOK   = 1 THEN N'a.mxv' + @m1_cid ELSE N'CAST(NULL AS NVARCHAR(100))' END + N')';
        END

        FETCH NEXT FROM m1_cur INTO @m1_col_name, @m1_col_id, @m1_col_type, @m1_len, @m1_nullable;
      END

      CLOSE m1_cur;
      DEALLOCATE m1_cur;

      /* Skip entirely when no column qualified for any metric (e.g. @ColumnList
         names only excluded columns) — an empty select list would be a syntax error. */
      IF @AggSelect <> N''
      BEGIN
        SET @SQLString = N'
          IF OBJECT_ID(''tempdb..#agg'') IS NOT NULL DROP TABLE #agg;

          SELECT ' + @AggSelect + N'
          INTO #agg
          FROM ' + @FromTableName + N';

          UPDATE p
          SET num_unique_values = v.uniq ,
              num_nulls         = v.nulls ,
              min_length        = v.minlen ,
              max_length        = v.maxlen ,
              num_blank         = v.blank ,
              num_whitespace    = v.ws ,
              num_zero          = v.zero ,
              num_negative      = v.neg ,
              min_value         = v.minval ,
              max_value         = v.maxval
          FROM #table_column_profile p
          JOIN #agg a ON 1 = 1
          CROSS APPLY (VALUES
            ' + @Unpivot + N'
          ) v(column_id, uniq, nulls, minlen, maxlen, blank, ws, zero, neg, minval, maxval)
          WHERE v.column_id = p.column_id;';

        IF @Verbose = 1
        BEGIN
          RAISERROR (N'Single-pass column detail: one scan for all metrics.', 0, 1) WITH NOWAIT;
          RAISERROR (@SQLString, 0, 1) WITH NOWAIT;
        END

        IF @SQLString IS NULL
          RAISERROR('@SQLString is null', 16, 1);

        EXECUTE sp_executesql @SQLString;
      END

    END /* Table Detail */
  
    IF @Mode = 2 /* Column Statistics */
    BEGIN
      /* Single-pass rewrite (mirrors Mode 1): instead of one UPDATE (one table scan)
         per metric per column, walk the columns once to build TWO wide aggregate
         SELECTs, then materialize each into a 1-row temp table and reshape via
         CROSS APPLY (VALUES). Batch A (#agg) computes min/max for every non-bit type
         plus mean/std_dev for numeric types in ONE scan. Batch B (#median) computes
         every numeric column's median in ONE scan (PERCENTILE_DISC is a window
         function, so it can't share Batch A's scalar-aggregate scan). Result: 2 scans
         total instead of ~2 per numeric column. */

      /* Determine Column Statistics */
      IF @Verbose = 1
        RAISERROR (N'Updating data in #table_column_profile for column statistics', 0, 1) WITH NOWAIT;

      DECLARE @stats_col_name NVARCHAR(500) ,
              @stats_col_num  INTEGER ,
              @stats_col_type NVARCHAR(50);

      DECLARE @StatSelect   NVARCHAR(MAX) = N'' ,  /* aggregate expressions for #agg     */
              @StatUnpivot  NVARCHAR(MAX) = N'' ,  /* VALUES rows for the min/max/mean/sd reshape */
              @MedSelect    NVARCHAR(MAX) = N'' ,  /* PERCENTILE_DISC expressions for #median */
              @MedUnpivot   NVARCHAR(MAX) = N'' ,  /* VALUES rows for the median reshape  */
              @s2_qn        NVARCHAR(300) ,
              @s2_cid       NVARCHAR(10) ,
              @s2_castcol   NVARCHAR(320) ,
              @s2_isnum     BIT;

      DECLARE stats_cur CURSOR LOCAL STATIC FORWARD_ONLY READ_ONLY FOR
      SELECT p.name,
             p.column_id,
             p.system_type
      FROM   #table_column_profile p
      /* bit is in the profiled-types set historically but never produced a stat, so
         it is excluded here — the loop only ever built min/max for non-bit types. */
      WHERE  p.system_type IN ('bigint', 'decimal', 'int', 'money', 'numeric', 'smallint', 'smallmoney', 'tinyint', 'float', 'real', 'date', 'datetime2', 'datetime', 'datetimeoffset', 'smalldatetime', 'time');

      OPEN stats_cur;

      FETCH NEXT FROM stats_cur INTO @stats_col_name, @stats_col_num, @stats_col_type;

      WHILE @@FETCH_STATUS = 0
      BEGIN
        SET @s2_qn  = QUOTENAME(@stats_col_name);
        SET @s2_cid = CAST(@stats_col_num AS NVARCHAR(10));

        SET @s2_isnum =
          CASE WHEN @stats_col_type IN ('bigint', 'decimal', 'int', 'money', 'numeric', 'smallint', 'smallmoney', 'tinyint', 'float', 'real')
               THEN 1 ELSE 0 END;

        /* AVG on int can overflow int; widen to bigint. Only 'int' needs this
           (AVG of smallint/tinyint already returns int, bigint returns bigint). */
        SET @s2_castcol = CASE WHEN @stats_col_type = 'int'
                               THEN N'CAST(' + @s2_qn + N' AS BIGINT)'
                               ELSE @s2_qn END;

        /* Batch A: min/max for every column; mean/std_dev for numerics only. */
        SET @StatSelect = @StatSelect + CASE WHEN @StatSelect <> N'' THEN N', ' ELSE N'' END
          + N'CAST(MIN(' + @s2_qn + N') AS NVARCHAR(100)) AS mn' + @s2_cid
          + N', CAST(MAX(' + @s2_qn + N') AS NVARCHAR(100)) AS mx' + @s2_cid
          + CASE WHEN @s2_isnum = 1 THEN
              N', CAST(AVG(' + @s2_castcol + N') AS NVARCHAR(100)) AS av' + @s2_cid
            + N', CAST(CAST(STDEV(' + @s2_qn + N') AS NUMERIC(18,4)) AS NVARCHAR(100)) AS sd' + @s2_cid
            ELSE N'' END;

        /* Matching VALUES row. Typed NULLs (never bare NULL) keep the unpivoted
           columns single-typed regardless of whether a column is numeric. */
        SET @StatUnpivot = @StatUnpivot + CASE WHEN @StatUnpivot <> N'' THEN N',
            ' ELSE N'' END
          + N'(' + @s2_cid + N', a.mn' + @s2_cid + N', a.mx' + @s2_cid + N', '
          + CASE WHEN @s2_isnum = 1 THEN N'a.av' + @s2_cid ELSE N'CAST(NULL AS NVARCHAR(100))' END + N', '
          + CASE WHEN @s2_isnum = 1 THEN N'a.sd' + @s2_cid ELSE N'CAST(NULL AS NVARCHAR(100))' END + N')';

        /* Batch B: median, numeric columns only, and only where PERCENTILE_DISC is
           supported (compat level 110+ — matching the prior per-column gate). */
        IF @s2_isnum = 1 AND @SQLCompatLevel >= 110
        BEGIN
          SET @MedSelect = @MedSelect + CASE WHEN @MedSelect <> N'' THEN N', ' ELSE N'' END
            + N'CAST(PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY ' + @s2_qn + N') OVER () AS NVARCHAR(100)) AS md' + @s2_cid;

          SET @MedUnpivot = @MedUnpivot + CASE WHEN @MedUnpivot <> N'' THEN N',
            ' ELSE N'' END
            + N'(' + @s2_cid + N', a.md' + @s2_cid + N')';
        END

        FETCH NEXT FROM stats_cur INTO @stats_col_name, @stats_col_num, @stats_col_type;
      END /* Column Statistics Loop */

      CLOSE stats_cur;
      DEALLOCATE stats_cur;

      /* Batch A — min/max/mean/std_dev in one scan. */
      IF @StatSelect <> N''
      BEGIN
        SET @SQLString = N'
          IF OBJECT_ID(''tempdb..#agg'') IS NOT NULL DROP TABLE #agg;

          SELECT ' + @StatSelect + N'
          INTO #agg
          FROM ' + @FromTableName + N';

          UPDATE p
          SET min_value = v.minv ,
              max_value = v.maxv ,
              mean      = v.meanv ,
              std_dev   = v.sdv
          FROM #table_column_profile p
          JOIN #agg a ON 1 = 1
          CROSS APPLY (VALUES
            ' + @StatUnpivot + N'
          ) v(column_id, minv, maxv, meanv, sdv)
          WHERE v.column_id = p.column_id;';

        IF @Verbose = 1
        BEGIN
          RAISERROR (N'Single-pass column statistics: one scan for min/max/mean/std_dev.', 0, 1) WITH NOWAIT;
          RAISERROR (@SQLString, 0, 1) WITH NOWAIT;
        END

        IF @SQLString IS NULL
          RAISERROR('@SQLString is null', 16, 1);

        EXECUTE sp_executesql @SQLString;
      END

      /* Batch B — all medians in one scan (guard already implies compat 110+ and
         at least one numeric column). */
      IF @MedSelect <> N''
      BEGIN
        SET @SQLString = N'
          IF OBJECT_ID(''tempdb..#median'') IS NOT NULL DROP TABLE #median;

          SELECT DISTINCT ' + @MedSelect + N'
          INTO #median
          FROM ' + @FromTableName + N';

          UPDATE p
          SET median = v.med
          FROM #table_column_profile p
          JOIN #median a ON 1 = 1
          CROSS APPLY (VALUES
            ' + @MedUnpivot + N'
          ) v(column_id, med)
          WHERE v.column_id = p.column_id;';

        IF @Verbose = 1
        BEGIN
          RAISERROR (N'Single-pass medians: one scan for all median columns.', 0, 1) WITH NOWAIT;
          RAISERROR (@SQLString, 0, 1) WITH NOWAIT;
        END

        IF @SQLString IS NULL
          RAISERROR('@SQLString is null', 16, 1);

        EXECUTE sp_executesql @SQLString;
      END

    END /* 2 - Column Statistics */

    IF @Mode = 3 /* 3 - Candidate Key Check */
    BEGIN

      DECLARE @WhereString NVARCHAR(MAX)
      DECLARE @WhereCtr INT;

      IF OBJECT_ID ('tempdb..#ColumnName') IS NOT NULL
        DROP TABLE #ColumnName;

      CREATE TABLE #ColumnName (
        column_name NVARCHAR(500),
        column_type NVARCHAR(100)
      );

      SET @WhereString = ' WHERE ';
      SET @WhereCtr = 0;

      SET @SQLString = N'
        SELECT c.name ,
               type = TYPE_NAME(c.system_type_id)
        FROM   ' + QUOTENAME(@DatabaseName) + '.sys.tables  t
        JOIN   ' + QUOTENAME(@DatabaseName) + '.sys.columns c ON  c.object_id = t.object_id
        JOIN   ' + QUOTENAME(@DatabaseName) + '.sys.schemas s ON  t.schema_id = s.schema_id
                                                             AND s.name = ''' + @Schema + '''
        WHERE  t.name = ''' + @TableName + '''
        AND    c.name IN (' + @ColumnListString + ');'

      IF @Verbose = 1
      BEGIN
        RAISERROR (N'Find data types for columns for Where clause Candidate Key Check', 0, 1) WITH NOWAIT;
        RAISERROR (@SQLString, 0, 1) WITH NOWAIT;;
      END

      IF @SQLString IS NULL
        RAISERROR('@SQLString is null', 16, 1);

      INSERT INTO #ColumnName
      EXECUTE sp_executesql @SQLString

      -- Determine unique values for each column with a valid type.
      DECLARE @where_col_name   NVARCHAR(500) ,
              @where_type_name  NVARCHAR(100) ,
              @where_col_value  NVARCHAR(500) ;
      
      DECLARE where_type_cur CURSOR
        LOCAL STATIC FORWARD_ONLY READ_ONLY FOR
           SELECT column_name ,
                  column_type
           FROM   #ColumnName
    
      OPEN where_type_cur;
      
      FETCH NEXT FROM where_type_cur INTO @where_col_name, @where_type_name;

      WHILE @@FETCH_STATUS = 0
      BEGIN      

        SET @WhereCtr = @WhereCtr + 1;
        IF @WhereCtr > 1
          SET @WhereString = @WhereString + ' AND ';

        IF @where_type_name IN ('datetime', 'datetime2', 'date', 'time', 'datetimeoffset', 'smalldatetime')
        BEGIN
          SET @where_col_value = 'CONVERT(NVARCHAR, ' + @where_col_name + ', 127)'
        END
        ELSE
          SET @where_col_value = 'CONVERT(NVARCHAR(MAX), ' + @where_col_name + ')'

        IF @where_type_name IN ('uniqueidentifier', 'date', 'time', 'datetime2', 'datetimeoffset', 'smalldatetime', 'datetime', 'sql_variant', 'varchar', 'char', 'timestamp', 'nvarchar', 'nchar') 
        BEGIN
          SET @WhereString = @WhereString + @where_col_name + ''' + COALESCE('' = '''''' + ' + @where_col_value + ' + '''''''', '' IS NULL'') + ''';
        END
        ELSE
        BEGIN
          SET @WhereString = @WhereString + @where_col_name + ''' + COALESCE('' = '' + ' + @where_col_value + ' + '''', '' IS NULL'') + ''';
        END

        FETCH NEXT FROM where_type_cur INTO @where_col_name, @where_type_name;
      END

      CLOSE where_type_cur;
      DEALLOCATE where_type_cur; 
  
      IF @Verbose = 1
      BEGIN
        SET @Msg = N'@WhereString: ' + @WhereString;
        RAISERROR (@Msg, 0, 1) WITH NOWAIT;
      END

    END /* 3 - Candidate Key Check */

    IF @Mode = 4 /* 4 - Column Value Distribution */
    BEGIN

      DECLARE @RowCountDistinct BIGINT;

      IF OBJECT_ID ('tempdb..#table_distinct_count') IS NOT NULL
        DROP TABLE #table_distinct_count;

      CREATE TABLE #table_distinct_count (
          [column_count] BIGINT NULL);

      /* Only process the first column identified */
      IF CHARINDEX(',', @ColumnList) > 0
        SET @ColumnNameFirst = LEFT(@ColumnList, CHARINDEX(',', @ColumnList) - 1)
      ELSE 
        SET @ColumnNameFirst = RTRIM(LTRIM(@ColumnList))

      IF RTRIM(LTRIM(@ColumnNameFirst)) <> RTRIM(LTRIM(@ColumnList))
      BEGIN
        RAISERROR(N'More than one column was supplied. Only the first column will be used in determining the column value distribution.', 0, 1);
      END 
      
      SELECT @SQLString = N'
        INSERT INTO #table_distinct_count (column_count)
        SELECT ' + CASE WHEN @ApproxDistinct = 1 AND @SQLMajorVersion >= 15
                        THEN 'APPROX_COUNT_DISTINCT(' + QUOTENAME(@ColumnNameFirst) + ')'
                        ELSE 'COUNT(DISTINCT ' + QUOTENAME(@ColumnNameFirst) + ')' END + ' val
        FROM ' + @FromTableName + '
      ';

      IF @Verbose = 1
      BEGIN
        RAISERROR (N'Insert distinct count for Column Value Distribution.', 0, 1) WITH NOWAIT;
        RAISERROR (@SQLString, 0, 1) WITH NOWAIT;;
      END
  
      IF @SQLString IS NULL 
        RAISERROR('@SQLString is null', 16, 1);
  
      EXEC sp_executesql @SQLString;

    END /* 4 - Column Value Distribution */

    /* Table schema output */  
    IF @Mode = 0
    BEGIN

      IF @Verbose = 1
        RAISERROR (N'Ouputting data for table schema output.', 0, 1) WITH NOWAIT;

      /* Table output */
      SELECT [object_id] = OBJECT_ID(QUOTENAME(@Schema) + '.' + QUOTENAME(@TableName)) ,
             [schema_name] = @Schema ,
             [table_name] = @TableName ,
             [row_count] = @RowCount ,
             [is_sample] = CASE @IsSample WHEN 1 THEN 'True' ELSE 'False' END;

      SELECT   [column_id] ,
               [name] ,
               [user_type] ,
               [system_type] ,
               [length] = 
                 CASE 
                   WHEN [length] = -1 AND [system_type] = 'xml' THEN NULL
                   WHEN [length] = -1 THEN 'max'
                   ELSE CAST([length] AS VARCHAR(50)) 
                 END,
               [precision] ,
               [scale] ,
               [is_nullable] ,
               [collation] 
      FROM #table_column_profile;

      IF @ShowForeignKeys = 1
      BEGIN
        IF @Verbose = 1
        BEGIN
          RAISERROR (N'Displaying Foreign Keys', 0, 1) WITH NOWAIT;
          RAISERROR (@SQLStringFK, 0, 1) WITH NOWAIT;
        END

        IF @SQLStringFK IS NULL 
          RAISERROR('@SQLStringFK is null', 16, 1);
  
        EXEC sp_executesql @SQLStringFK;
      END

      IF @ShowIndexes = 1
      BEGIN
        IF @Verbose = 1
        BEGIN
          RAISERROR (N'Displaying Indexes', 0, 1) WITH NOWAIT;
          RAISERROR (@SQLStringIndexes, 0, 1) WITH NOWAIT;
        END

        IF @SQLStringIndexes IS NULL 
          RAISERROR('@SQLStringIndexes is null', 16, 1);
  
        EXEC sp_executesql @SQLStringIndexes;
      END
    END /* Mode 0: Table schema output */
    
    /* Table detail output */  
    IF @Mode = 1
    BEGIN
  
      IF @Verbose = 1
        RAISERROR (N'Ouputting data for table detail output.', 0, 1) WITH NOWAIT;

      /* Table output */
      SELECT   [object_id] = OBJECT_ID(QUOTENAME(@Schema) + '.' + QUOTENAME(@TableName)) ,
               [schema_name] = @Schema ,
               [table_name] = @TableName ,
               [row_count] = @RowCount ,
               [is_sample] = CASE @IsSample WHEN 1 THEN 'True' ELSE 'False' END;

      SELECT   [column_id] ,
               [name] ,
               [user_type] ,
               [system_type] ,
               [length] = 
                 CASE 
                   WHEN [length] = -1 AND [system_type] = 'xml' THEN NULL
                   WHEN [length] = -1 THEN 'max'
                   ELSE CAST([length] AS VARCHAR(50)) 
                 END,
               [precision] ,
               [scale] ,
               [is_nullable] ,
               [num_unique_values] ,
               [unique_ratio] ,
               [cardinality] =
                 CASE
                   WHEN [num_unique_values] IS NULL                    THEN NULL   /* ineligible type (LOB/max) */
                   WHEN [num_rows] = 0 OR [num_unique_values] = 0      THEN NULL   /* empty table */
                   WHEN [num_unique_values] = 1                        THEN 'Constant'
                   WHEN [num_unique_values] = 2                        THEN 'Binary'
                   WHEN [num_unique_values] = [num_rows]               THEN 'Unique'
                   WHEN [num_unique_values] <= @CategoricalMaxDistinct THEN 'Categorical'
                   ELSE 'High-cardinality'
                 END ,
               [num_nulls] ,
               [nulls_ratio] ,
               [num_blank] ,
               [blank_ratio] ,
               [num_whitespace] ,
               [whitespace_ratio] ,
               [num_zero] ,
               [zero_ratio] ,
               [num_negative] ,
               [negative_ratio] ,
               [min_length] ,
               [max_length] ,
               [min_value] ,
               [max_value]
      FROM #table_column_profile
      /* Honor @ColumnList when supplied; display all columns when it isn't. */
      WHERE (NOT EXISTS (SELECT 1 FROM #column_filter)
             OR [name] IN (SELECT col_name FROM #column_filter))
      ORDER BY [column_id];

      IF @ShowForeignKeys = 1
      BEGIN
        IF @Verbose = 1
        BEGIN
          RAISERROR (N'Displaying Foreign Keys', 0, 1) WITH NOWAIT;
          RAISERROR (@SQLStringFK, 0, 1) WITH NOWAIT;
        END

        IF @SQLStringFK IS NULL 
          RAISERROR('@SQLStringFK is null', 16, 1);
  
        EXEC sp_executesql @SQLStringFK;
      END

      IF @ShowIndexes = 1
      BEGIN
        IF @Verbose = 1
        BEGIN
          RAISERROR (N'Displaying Indexes', 0, 1) WITH NOWAIT;
          RAISERROR (@SQLStringIndexes, 0, 1) WITH NOWAIT;
        END

        IF @SQLStringIndexes IS NULL 
          RAISERROR('@SQLStringIndexes is null', 16, 1);
  
        EXEC sp_executesql @SQLStringIndexes;
      END
             
    END /* Mode 1: Table detail output */
  
    /* Column statistics output */
    IF @Mode = 2
    BEGIN
  
      IF @Verbose = 1
        RAISERROR (N'Ouputting data for column statistics output.', 0, 1) WITH NOWAIT;

      /* Table output */
      SELECT [object_id] = OBJECT_ID(QUOTENAME(@Schema) + '.' + QUOTENAME(@TableName)) ,
             [schema_name] = @Schema ,
             [table_name] = @TableName ,
             [row_count] = @RowCount ,
             [is_sample] = CASE @IsSample WHEN 1 THEN 'True' ELSE 'False' END;

      SET @SQLString = N'
          SELECT [column_id] ,
                 [name] ,
                 [user_type] ,
                 [system_type] ,
                 [length] = 
                   CASE 
                     WHEN [length] = -1 AND [system_type] = ''xml'' THEN NULL
                     WHEN [length] = -1 THEN ''max''
                     ELSE CAST([length] AS VARCHAR(50)) 
                   END,
                 [precision] ,
                 [scale] ,
                 [is_nullable] ,
                 [min_value] ,
                 [max_value] ,
                 [mean] ,'
  
      IF @SQLCompatLevel >= 110
        SET @SQLString = @SQLString + N'
                 [median] ,'
  
      SET @SQLString = @SQLString + N'             
                 [std_dev]
          FROM #table_column_profile;'
  
      IF @SQLString IS NULL 
        RAISERROR('@SQLString is null', 16, 1);
  
      EXEC sp_executesql @SQLString;

      IF @ShowForeignKeys = 1
      BEGIN
        IF @Verbose = 1
        BEGIN
          RAISERROR (N'Displaying Foreign Keys', 0, 1) WITH NOWAIT;
          RAISERROR (@SQLStringFK, 0, 1) WITH NOWAIT;
        END

        IF @SQLStringFK IS NULL 
          RAISERROR('@SQLStringFK is null', 16, 1);
  
        EXEC sp_executesql @SQLStringFK;
      END

      IF @ShowIndexes = 1
      BEGIN
        IF @Verbose = 1
        BEGIN
          RAISERROR (N'Displaying Indexes', 0, 1) WITH NOWAIT;
          RAISERROR (@SQLStringIndexes, 0, 1) WITH NOWAIT;
        END

        IF @SQLStringIndexes IS NULL 
          RAISERROR('@SQLStringIndexes is null', 16, 1);
  
        EXEC sp_executesql @SQLStringIndexes;
      END

    END /* Mode 2: Column statistics output */
  
    /* Candidate Key Check */
    IF @Mode = 3
    BEGIN
 
      /* Table output */
      SELECT [object_id] = OBJECT_ID(QUOTENAME(@Schema) + '.' + QUOTENAME(@TableName)) ,
             [schema_name] = @Schema ,
             [table_name] = @TableName ,
             [row_count] = @RowCount ,
             [is_sample] = CASE @IsSample WHEN 1 THEN 'True' ELSE 'False' END;

      SET @SQLString = N'
        SELECT    COUNT(*) AS row_count ,
                  ' + @ColumnList + ' ,
                  view_data_sql = ''SELECT * FROM ' + @FromTableName + @WhereString + '''
        FROM      ' + @FromTableName + '
        GROUP BY  ' + @ColumnList + '
        HAVING    COUNT(*) > 1
        ORDER BY  1 DESC
       ';

      IF @Verbose = 1
      BEGIN
        RAISERROR (N'Ouputting data for candidate key check.', 0, 1) WITH NOWAIT;
        RAISERROR (@SQLString, 0, 1) WITH NOWAIT;;
      END

      IF @SQLString IS NULL 
        RAISERROR('@SQLString is null', 16, 1);
  
      EXEC sp_executesql @SQLString;

      IF @ShowForeignKeys = 1
      BEGIN
        IF @Verbose = 1
        BEGIN
          RAISERROR (N'Displaying Foreign Keys', 0, 1) WITH NOWAIT;
          RAISERROR (@SQLStringFK, 0, 1) WITH NOWAIT;
        END

        IF @SQLStringFK IS NULL 
          RAISERROR('@SQLStringFK is null', 16, 1);
  
        EXEC sp_executesql @SQLStringFK;
      END

      IF @ShowIndexes = 1
      BEGIN
        IF @Verbose = 1
        BEGIN
          RAISERROR (N'Displaying Indexes', 0, 1) WITH NOWAIT;
          RAISERROR (@SQLStringIndexes, 0, 1) WITH NOWAIT;
        END

        IF @SQLStringIndexes IS NULL 
          RAISERROR('@SQLStringIndexes is null', 16, 1);
  
        EXEC sp_executesql @SQLStringIndexes;
      END

    END /* 3 - Candidate Key Check */

    /* 4 - Column Value Distribution */
    IF @Mode = 4 
    BEGIN

      /* Table output */
      SELECT [object_id] = OBJECT_ID(QUOTENAME(@Schema) + '.' + QUOTENAME(@TableName)) ,
             [schema_name] = @Schema ,
             [table_name] = @TableName ,
             [row_count] = @RowCount ,
             [column_name] = @ColumnNameFirst ,
             [distinct_row_count] = (SELECT column_count FROM #table_distinct_count) ,
             [is_sample] = CASE @IsSample WHEN 1 THEN 'True' ELSE 'False' END ;

      SELECT @SQLString = N'
        SELECT ' + @ColumnNameFirst + ' ,
                Count = COUNT(*) ,
                Percentage = CAST(CAST(COUNT(*)AS DECIMAL(18,4)) * 100 / ' + CAST(@RowCount AS NVARCHAR(25)) + ' AS DECIMAL(18,4))
        FROM   ' + @FromTableName + '
        GROUP BY ' + @ColumnNameFirst + '
        ORDER BY 2 DESC, 1
      ';

      IF @Verbose = 1
      BEGIN
        RAISERROR (N'Ouputting data for column value distribution', 0, 1) WITH NOWAIT;
        RAISERROR (@SQLString, 0, 1) WITH NOWAIT;;
      END

      IF @SQLString IS NULL 
        RAISERROR('@SQLString is null', 16, 1);
  
      EXEC sp_executesql @SQLString;

      IF @ShowForeignKeys = 1
      BEGIN
        IF @Verbose = 1
        BEGIN
          RAISERROR (N'Displaying Foreign Keys', 0, 1) WITH NOWAIT;
          RAISERROR (@SQLStringFK, 0, 1) WITH NOWAIT;
        END

        IF @SQLStringFK IS NULL 
          RAISERROR('@SQLStringFK is null', 16, 1);
  
        EXEC sp_executesql @SQLStringFK;
      END

      IF @ShowIndexes = 1
      BEGIN
        IF @Verbose = 1
        BEGIN
          RAISERROR (N'Displaying Indexes', 0, 1) WITH NOWAIT;
          RAISERROR (@SQLStringIndexes, 0, 1) WITH NOWAIT;
        END

        IF @SQLStringIndexes IS NULL 
          RAISERROR('@SQLStringIndexes is null', 16, 1);
  
        EXEC sp_executesql @SQLStringIndexes;
      END

    END /* 4 - Column Value Distribution */

    DROP TABLE #table_column_profile;
    DROP TABLE #table_relationship;
    DROP TABLE #table_indexes
  
    SET NOCOUNT OFF;
  
  END TRY
  
  BEGIN CATCH
    RAISERROR (N'Uh oh. Something bad happend.', 0,1) WITH NOWAIT;
  
    SELECT  @Msg = ERROR_MESSAGE(), @ErrorSeverity = ERROR_SEVERITY(), @ErrorState = ERROR_STATE();
  
    RAISERROR (@Msg, @ErrorSeverity, @ErrorState);
    
    WHILE @@trancount > 0 
      ROLLBACK;
  
    RETURN;
  END CATCH;

END

GO
