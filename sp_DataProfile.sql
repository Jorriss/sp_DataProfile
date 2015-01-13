USE [master]
GO

IF OBJECT_ID('dbo.sp_DataProfile') IS NOT NULL 
  DROP PROCEDURE dbo.sp_DataProfile;
GO

CREATE PROCEDURE dbo.sp_DataProfile
   @TableName NVARCHAR(500) ,
   @Mode TINYINT = 0 , /* 0 = Table Overview, 1 = Table Detail, 2 = Column Statistics, 3 = Candidate Key Check, 4 - Column Value Distribution */
   @ColumnList NVARCHAR(4000) = NULL ,
   @DatabaseName NVARCHAR(128) = NULL ,
   @SampleValue INT = NULL ,
   @SampleType NVARCHAR(50) = 'PERCENT'
AS
BEGIN

  PRINT @TableName

  SET NOCOUNT ON;
  SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

  DECLARE @SQLString NVARCHAR(4000);
  DECLARE @Schema NVARCHAR(100);
  DECLARE @DatabaseID INT;
  DECLARE @SchemaPosition INT;
  DECLARE @Msg NVARCHAR(4000);
  DECLARE @ErrorSeverity INT;
  DECLARE @ErrorState INT;
  DECLARE @RowCount BIGINT;
  DECLARE @IsSample BIT = 0;
  DECLARE @TableSample NVARCHAR(100) = '';
  DECLARE @FromTableName NVARCHAR(100) = '';
  DECLARE @ColumnListString NVARCHAR(4000);
  DECLARE @ColumnNameFirst NVARCHAR(4000);
  DECLARE @SQLServerVersion NVARCHAR(100) = '';
  DECLARE @SQLCompatLevelMaster INT;
  DECLARE @SQLCompatLevelDB INT;
  DECLARE @SQLCompatLevelDBOut INT;
  DECLARE @SQLCompatLevel INT;

  BEGIN TRY

    /* Get that SQL Server Version son! 2005 or older up in here! */
    SELECT @SQLServerVersion = CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(128));

    IF (SELECT LEFT(@SQLServerVersion, CHARINDEX('.', @SQLServerVersion, 0) -1 )) <= 8
    BEGIN
      SET @msg = N'I''m sorry Dave. I can''t run on your version of SQL Server. I require a SQL Server 2005 and higher. The version of this instance is: ' + @SQLServerVersion + '. I promise I won''t open the airlock.';
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
      RAISERROR(@msg, 0, 1);
    END

    SET @Schema = PARSENAME(@TableName, 2);
    SET @TableName = PARSENAME(@TableName, 1);
  
    IF @Schema IS NULL 
      SET @Schema = 'dbo';

    IF @Mode NOT IN (0, 1, 2, 3, 4) 
    BEGIN
      SET @msg = 'Mode values should only be 0, 1, 2, 3 or 4. 0 = Table Overview, 1 = Table Detail, 2 = Column Statistics, 3 = Candidate Key Check, 4 = Column Value Distribution';
      RAISERROR(@msg, 1, 1);
      RETURN;
    END 
  
    IF @SampleType NOT IN ('ROWS', 'PERCENT') 
    BEGIN
      SET @msg = 'Did you mistype the SampleType value? SampleType should be either ''ROWS'' or ''PERCENT''';
      RAISERROR(@msg, 1, 1);
      RETURN;
    END 

    IF @SampleValue < 0 OR @SampleValue > 100 
    BEGIN
      SET @msg = 'Whoops. The SampleValue should be between 0 and 100.';
      RAISERROR(@msg, 1, 1);
      RETURN;
    END

    IF @SampleValue IS NOT NULL
    BEGIN
      SET @IsSample = 1;
      SET @TableSample = ' TABLESAMPLE (' + CAST(@SampleValue AS NVARCHAR(3)) + ' ' + @SampleType + ') REPEATABLE(100) ';
    END

    SET @FromTableName = QUOTENAME(@Schema) + '.' + QUOTENAME(@TableName) + @TableSample;

    If DB_NAME() <> @DatabaseName
      SET @FromTableName = QUOTENAME(@DatabaseName) + '.' + @FromTableName;

    SELECT  @DatabaseID = database_id
    FROM    sys.databases
    WHERE   [name] = @DatabaseName
    AND     user_access_desc = 'MULTI_USER'
    AND     state_desc = 'ONLINE';
          
    /* Format ColumnList  */
    SET @ColumnList = RTRIM(LTRIM(@ColumnList));
    IF RIGHT(@ColumnList, 1) = ','
      SET @ColumnList = LEFT(@ColumnList, LEN(@ColumnList) - 1);
    SET @ColumnListString = '''' + REPLACE(@ColumnList, ',', ''',''') + '''';

    IF OBJECT_ID ('tempdb..#table_column_profile') IS NOT NULL
      DROP TABLE #table_column_profile;
    
    CREATE TABLE #table_column_profile (
      [object_id]          INT           NOT NULL ,
      [column_id]          INT           NOT NULL , 
      [name]               NVARCHAR(500) NOT NULL , 
      [type]               NVARCHAR(100) NOT NULL ,
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
      [max_length]         INT           NULL ,
      [min_value]          NVARCHAR(100) NULL ,
      [max_value]          NVARCHAR(100) NULL ,
      [mean]               NVARCHAR(100) NULL ,
      [median]             NVARCHAR(100) NULL ,
      [std_dev]            NVARCHAR(100) NULL
    )
  
    SET @SQLString = N'
      SELECT t.object_id ,
             c.column_id ,
             c.name ,
             sys.name,
             typ.name ,
             c.collation_name ,
             CASE  
               WHEN sys.name = ''nvarchar'' THEN c.max_length / 2
               WHEN sys.name = ''nchar'' THEN c.max_length / 2
               ELSE c.max_length
             END AS max_length ,
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

    IF @SQLString IS NULL 
      RAISERROR('@SQLString is null', 16, 1);

    RAISERROR (N'Inserting data into #table_column_profile', 0, 1) WITH NOWAIT;
  
    INSERT INTO #table_column_profile (
      [object_id] ,
      [column_id] ,
      [name] ,
      [type] ,
      [user_type] ,
      [collation] ,
      [length] ,
      [precision] ,
      [scale] ,
      [is_nullable]
    ) 
    EXEC sp_executesql @SQLString;
  
    /* Update actual row count  */
    RAISERROR (N'Updating data in #table_column_profile for table row counts', 0, 1) WITH NOWAIT;
      
    SET @SQLString = N'
      UPDATE #table_column_profile  
      SET num_rows = cnt 
      FROM (SELECT COUNT_BIG(*) cnt 
            FROM ' + @FromTableName + ') tablecount ;'
    
    IF @SQLString IS NULL 
      RAISERROR('@SQLString is null', 16, 1);

    PRINT @SQLString
      
    EXEC sp_executesql @SQLString;
      
    SELECT TOP 1 @RowCount = num_rows FROM #table_column_profile;
    
    IF @Mode = 1 /* Table Detail */
    BEGIN         
      -- Determine unique values for each column with a valid type.
      DECLARE @uniq_col_name NVARCHAR(500) ,
              @uniq_col_id  INTEGER;
      
      DECLARE uniq_cur CURSOR
        LOCAL STATIC FORWARD_ONLY READ_ONLY FOR
          SELECT p.name,
                 p.column_id
          FROM   #table_column_profile p
          WHERE  type IN ('uniqueidentifier', 'date', 'time', 'datetime2', 'datetimeoffset', 'tinyint', 'smallint', 'int', 'smalldatetime', 'real', 'money', 'datetime', 'float', 'sql_variant', 'bit', 'decimal', 'numeric', 'smallmoney' ,'bigint', 'hierarchyid', 'geometry', 'geography', 'varbinary', 'varchar', 'binary', 'char', 'timestamp', 'nvarchar', 'nchar') ;
    
      OPEN uniq_cur;
      
      FETCH NEXT FROM uniq_cur INTO @uniq_col_name, @uniq_col_id;
  
   
      WHILE @@FETCH_STATUS = 0
      BEGIN
      
        RAISERROR (N'Determine unique values for each column with a valid type.', 0, 1) WITH NOWAIT;
    
        SELECT @SQLString = N'
          UPDATE #table_column_profile 
          SET num_unique_values = val 
          FROM (
            SELECT COUNT(DISTINCT ' + QUOTENAME(@uniq_col_name) + ') val 
            FROM ' + @FromTableName + ') uniq 
          WHERE column_id = ' + CAST(@uniq_col_id AS NVARCHAR(10)) 
      
        IF @SQLString IS NULL 
          RAISERROR('@SQLString is null', 16, 1);
    
  
       EXECUTE sp_executesql @SQLString;
    
        FETCH NEXT FROM uniq_cur INTO @uniq_col_name, @uniq_col_id;
      END
        
      -- Determine null values for each column   
      DECLARE @null_col_name NVARCHAR(500) ,
              @null_col_num  INTEGER;
    
      DECLARE null_cur CURSOR
        LOCAL STATIC FORWARD_ONLY READ_ONLY FOR
          SELECT p.name,
                 p.column_id
          FROM   #table_column_profile p
          WHERE  p.is_nullable = 1;
    
      OPEN null_cur;
      
      FETCH NEXT FROM null_cur INTO @null_col_name, @null_col_num;
      
      WHILE @@FETCH_STATUS = 0
      BEGIN
    
        RAISERROR (N'Updating data in #table_column_profile for column null row counts', 0, 1) WITH NOWAIT;
    
        SELECT @SQLString = 
          N'UPDATE #table_column_profile ' +
           'SET num_nulls = val ' + 
           'FROM (' +
           '  SELECT COUNT(*) val ' +
           '  FROM ' + @FromTableName + ' ' +
           '  WHERE ' + QUOTENAME(@null_col_name) + ' IS NULL ' +
           ') uniq ' +
           'WHERE column_id = ' + CAST(@null_col_num AS NVARCHAR(10))
    
        IF @SQLString IS NULL 
          RAISERROR('@SQLString is null', 16, 1);
    
        EXECUTE sp_executesql @SQLString;
        
        FETCH NEXT FROM null_cur INTO @null_col_name, @null_col_num;
      END
      
      CLOSE null_cur;
      DEALLOCATE null_cur;
    
      /* Determine max length values */
      RAISERROR (N'Updating data in #table_column_profile for column max length values', 0, 1) WITH NOWAIT;
     
      DECLARE @len_col_name NVARCHAR(500) ,
              @len_col_num  INTEGER;
    
      DECLARE len_cur CURSOR
  
       LOCAL STATIC FORWARD_ONLY READ_ONLY FOR
          SELECT p.name,
                 p.column_id
          FROM   #table_column_profile p
          WHERE  p.type IN ('varchar', 'char', 'nvarchar', 'nchar');
    
      OPEN len_cur;
      
      FETCH NEXT FROM len_cur INTO @len_col_name, @len_col_num;
      
      WHILE @@FETCH_STATUS = 0
      BEGIN
        
        RAISERROR (N'Updating data in #table_column_profile for column max length', 0, 1) WITH NOWAIT;
    
        SELECT @SQLString = 
          N'UPDATE #table_column_profile ' +
           'SET max_length = val ' + 
           'FROM (' +
           '  SELECT MAX(LEN(' + QUOTENAME(@len_col_name) + ')) val ' +
           '  FROM ' + @FromTableName + ' ' +
           ') uniq ' +
           'WHERE column_id = ' + CAST(@len_col_num AS NVARCHAR(10))
    
        IF @SQLString IS NULL 
          RAISERROR('@SQLString is null', 16, 1);
    
        EXECUTE sp_executesql @SQLString;
      
        FETCH NEXT FROM len_cur INTO @len_col_name, @len_col_num;
      END
      
      CLOSE len_cur;
      DEALLOCATE len_cur; 
  
    END /* Table Detail */
  
    IF @Mode = 2 /* Column Statistics */
    BEGIN
  
      /* Determine Column Statistics */
      RAISERROR (N'Updating data in #table_column_profile for column statistics', 0, 1) WITH NOWAIT;
     
      DECLARE @stats_col_name NVARCHAR(500) ,
              @stats_col_num  INTEGER ,
              @stats_col_type NVARCHAR(50);
    
      DECLARE stats_cur CURSOR LOCAL STATIC FORWARD_ONLY READ_ONLY FOR
      SELECT p.name,
             p.column_id,
             p.type
      FROM   #table_column_profile p
      WHERE  p.type IN ('bigint', 'bit', 'decimal', 'int', 'money', 'numeric', 'smallint', 'smallmoney', 'tinyint', 'float', 'real', 'date', 'datetime2', 'datetime', 'datetimeoffset', 'smalldatetime', 'time');
    
      OPEN stats_cur;
      
      FETCH NEXT FROM stats_cur INTO @stats_col_name, @stats_col_num, @stats_col_type;
      
      WHILE @@FETCH_STATUS = 0
      BEGIN
      
        RAISERROR (N'Updating data in #table_column_profile for column max length', 0, 1) WITH NOWAIT;
    
        SELECT @SQLString = N'  
          UPDATE #table_column_profile 
          SET max_value = max_val ,
              min_value = min_val 
          FROM (
            SELECT CAST(MAX(' + QUOTENAME(@stats_col_name) + ') AS NVARCHAR(100)) max_val  ,
                   CAST(MIN(' + QUOTENAME(@stats_col_name) + ') AS NVARCHAR(100)) min_val  
            FROM ' + @FromTableName + ' 
           ) stats 
          WHERE column_id = ' + CAST(@stats_col_num AS NVARCHAR(10))
  
        IF @SQLString IS NULL
          RAISERROR('@SQLString is null', 16, 1);
  
        IF @stats_col_type != 'bit'
          EXECUTE sp_executesql @SQLString;
  
        /* Update mean, standard deviation */
        DECLARE @col_name NVARCHAR(100) = QUOTENAME(@stats_col_name);
      
        IF @stats_col_type = 'int'
          SET @col_name = 'CAST(' + QUOTENAME(@stats_col_name) + ' AS BIGINT)';
  
        SELECT @SQLString = N'
          UPDATE #table_column_profile 
          SET mean = mean_val ,
              std_dev = std_dev_val
          FROM (
            SELECT mean_val = CAST(AVG(' + @col_name + ') AS NVARCHAR(100)) ,
                   std_dev_val = CAST(CAST(STDEV(' + QUOTENAME(@stats_col_name) + ') AS NUMERIC(18,4)) AS NVARCHAR(100)) 
            FROM ' + @FromTableName + ' 
          ) stats WHERE column_id = ' + CAST(@stats_col_num AS NVARCHAR(10))
      
        IF @SQLString IS NULL
          RAISERROR('@SQLString is null', 16, 1);
  
        IF @stats_col_type IN ('bigint', 'decimal', 'int', 'money', 'numeric', 'smallint', 'smallmoney', 'tinyint', 'float', 'real')
          EXECUTE sp_executesql @SQLString;
       
        /* Update median */
        IF @SQLCompatLevel >= 110
        BEGIN
        
          SELECT @SQLString = N'
            UPDATE #table_column_profile 
            SET median = median_val
            FROM (
              SELECT DISTINCT median_val = PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY ' + @stats_col_name + ') OVER ()
              FROM ' + @FromTableName + ' 
            ) stats 
            WHERE column_id = ' + CAST(@stats_col_num AS NVARCHAR(10))
  
          IF @SQLString IS NULL
            RAISERROR('@SQLString is null', 16, 1);
  
          IF @stats_col_type IN ('bigint', 'decimal', 'int', 'money', 'numeric', 'smallint', 'smallmoney', 'tinyint', 'float', 'real')
            EXECUTE sp_executesql @SQLString;
       
        END /* End Median Update */
  
        FETCH NEXT FROM stats_cur INTO @stats_col_name, @stats_col_num, @stats_col_type;
    
      END /* Column Statistics Loop */
      
      CLOSE stats_cur;
      DEALLOCATE stats_cur; 
  
    END /* 2 - Column Statistics */

    IF @Mode = 4 /* 4 - Column Value Distribution */
  BEGIN

      DECLARE @RowCountDistinct BIGINT;

      CREATE TABLE #table_distinct_count (
          [column_count] BIGINT NULL);

      /* Only process the first column identified */
      IF CHARINDEX(',', @ColumnList) > 0
        SET @ColumnNameFirst = LEFT(@ColumnList, CHARINDEX(',', @ColumnList) - 1)
      ELSE 
        SET @ColumnNameFirst = RTRIM(LTRIM(@ColumnList))
      
      SELECT @SQLString = N'
        INSERT INTO #table_distinct_count (column_count)
        SELECT COUNT(DISTINCT ' + QUOTENAME(@ColumnNameFirst) + ') val 
        FROM ' + @FromTableName + ' 
      ';

      IF @SQLString IS NULL 
        RAISERROR('@SQLString is null', 16, 1);
  
      EXEC sp_executesql @SQLString;

    END /* 4 - Column Value Distribution */

    /* Table schema output */  
    IF @Mode = 0
    BEGIN
  
      /* Table output */
      SELECT [object_id] = OBJECT_ID(QUOTENAME(@Schema) + '.' + QUOTENAME(@TableName)) ,
             [schema_name] = @Schema ,
             [table_name] = @TableName ,
             [row_count] = @RowCount ,
             [is_sample] = CASE @IsSample WHEN 1 THEN 'True' ELSE 'False' END;

      SELECT   [column_id] ,
               [name] ,
               [user_type] ,
               [type] ,
               [collation] ,
               [length] = 
                 CASE 
                   WHEN [length] = -1 AND [type] = 'xml' THEN NULL
                   WHEN [length] = -1 THEN 'max'
                   ELSE CAST([length] AS VARCHAR(50)) 
                 END,
               [precision] ,
               [scale] ,
               [is_nullable] 
      FROM #table_column_profile;
  
    END /* Mode 0: Table schema output */
    
    /* Table detail output */  
    IF @Mode = 1
    BEGIN
  
      /* Table output */
      SELECT [object_id] = OBJECT_ID(QUOTENAME(@Schema) + '.' + QUOTENAME(@TableName)) ,
             [schema_name] = @Schema ,
             [table_name] = @TableName ,
             [row_count] = @RowCount ,
             [is_sample] = CASE @IsSample WHEN 1 THEN 'True' ELSE 'False' END;

      SELECT   [column_id] ,
               [name] ,
               [user_type] ,
               [type] ,
               [collation] ,
               [length] = 
                 CASE 
                   WHEN [length] = -1 AND [type] = 'xml' THEN NULL
                   WHEN [length] = -1 THEN 'max'
                   ELSE CAST([length] AS VARCHAR(50)) 
                 END,
               [precision] ,
               [scale] ,
               [is_nullable] ,
               [num_unique_values] ,
               [unique_ratio] , 
               [num_nulls] , 
               [nulls_ratio] ,
               [max_length]
      FROM #table_column_profile;
  
    END /* Mode 1: Table detail output */
  
    /* Column statistics output */
    IF @Mode = 2
    BEGIN
  
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
                 [type] ,
                 [collation] ,
                 [length] = 
                   CASE 
                     WHEN [length] = -1 AND [type] = ''xml'' THEN NULL
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

      SELECT @SQLString = N'
        SELECT    COUNT(*) AS row_count ,
                  ' + @ColumnList + ' 
        FROM      ' + @FromTableName + '
        GROUP BY  ' + @ColumnList + '
        HAVING    COUNT(*) > 1
       ';

      IF @SQLString IS NULL 
        RAISERROR('@SQLString is null', 16, 1);
  
      EXEC sp_executesql @SQLString;

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
                COUNT(*) ,
               CAST((CAST(COUNT(*)AS DECIMAL(9,4)) / ' + CAST(@RowCount AS NVARCHAR(25)) + ') * 100 AS DECIMAL(9,4))
        FROM   ' + @FromTableName + '
        GROUP BY ' + @ColumnNameFirst + '
        ORDER BY 2 DESC, 1
      ';

      IF @SQLString IS NULL 
        RAISERROR('@SQLString is null', 16, 1);
  
      EXEC sp_executesql @SQLString;

    END /* 4 - Column Value Distribution */

    DROP TABLE #table_column_profile;
  
    SET NOCOUNT OFF;
  
  END TRY
  
  BEGIN CATCH
    RAISERROR (N'Uh oh. Something bad happend.', 0,1) WITH NOWAIT;
  
    SELECT  @msg = ERROR_MESSAGE(), @ErrorSeverity = ERROR_SEVERITY(), @ErrorState = ERROR_STATE();
  
    RAISERROR (@msg, @ErrorSeverity, @ErrorState);
    
    WHILE @@trancount > 0 
      ROLLBACK;
  
    RETURN;
  END CATCH;

END

GO
