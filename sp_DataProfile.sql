  
  DECLARE @tablename NVARCHAR(500),
          @SQLString NVARCHAR(4000);
  
  SET @tablename = N'Users';
  
  IF OBJECT_ID ('tempdb..#table_column_profile') IS NOT NULL
    DROP TABLE #table_column_profile;
  
  CREATE TABLE #table_column_profile (
    [object_id]          INT           NOT NULL ,
    [column_id]          INT           NOT NULL , 
    [name]               NVARCHAR(500) NOT NULL , 
    [type]               NVARCHAR(100) NOT NULL ,
    [collation]          NVARCHAR(100) NULL ,
    [max_len]            INTEGER       NULL ,
    [precision]          INTEGER       NULL ,
    [scale]              INTEGER       NULL ,
    [is_nullable]        BIT           NOT NULL ,
    [num_rows]           BIGINT        NULL ,
    [num_unique_values]  BIGINT        NULL ,
    [unique_ratio] AS CAST((CAST([num_unique_values] AS DECIMAL(25,5)) / [num_rows]) AS DECIMAL(25,5)) ,
    [num_nulls]          BIGINT        NULL ,
    [nulls_ratio] AS CAST((CAST([num_nulls] AS DECIMAL(25,5)) / [num_rows]) AS DECIMAL(25,5)) ,
    [max_length]         INT           NULL
  )

  INSERT INTO #table_column_profile (
    [object_id] ,
    [column_id] ,
    [name] ,
    [type] ,
    [collation] ,
    [max_len] ,
    [precision] ,
    [scale] ,
    [is_nullable]
  ) 
  SELECT t.object_id ,
         c.column_id ,
         c.name ,
         typ.name ,
         c.collation_name ,
         c.max_length ,
         c.precision ,
         c.scale ,
         c.is_nullable
  FROM   sys.tables t
  JOIN   sys.columns c ON  t.object_id = c.object_id
  JOIN   sys.types   typ ON c.system_type_id = typ.system_type_id
                         AND c.user_type_id = typ.user_type_id
  WHERE  t.name = 'Users' -- @tablename
  ORDER BY c.column_id
  ;
  
  SET @SQLString = 
    'UPDATE #table_column_profile ' + 
    'SET num_rows = cnt ' +
    'FROM (' +
    '  SELECT COUNT(*) cnt ' +
    '  FROM ' + QUOTENAME(@tablename) + ') tablecount';
  
  EXEC sp_executesql @SQLString;
  
  -- Determine unique values

  DECLARE 
     @min_column_id INT,
     @max_column_id INT,
     @cur_column_id INT = 0;
  
  SELECT @min_column_id = MIN([column_id]), @max_column_id = MAX([column_id]) FROM #table_column_profile;
  
  WHILE (@cur_column_id < @max_column_id)
  BEGIN
    SET @cur_column_id += 1;
  
    SELECT @SQLString = 
      N'UPDATE #table_column_profile ' +
       'SET num_unique_values = val ' + 
       'FROM (' +
       'SELECT COUNT(DISTINCT ' + QUOTENAME(p.name) + ') val FROM ' + QUOTENAME(@tablename) + ') uniq ' +
       'WHERE column_id = ' + CAST(p.column_id AS NVARCHAR(10))
    FROM #table_column_profile p
    WHERE [column_id] = @cur_column_id; 
  
    EXECUTE sp_executesql @SQLString;
  
    -- PRINT @sql_update_unique_val;
         
  END
  
  -- Determine null values
   
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

    SELECT @SQLString = 
      N'UPDATE #table_column_profile ' +
       'SET num_nulls = val ' + 
       'FROM (' +
       '  SELECT SUM(CASE WHEN ' + QUOTENAME(@null_col_name) + ' IS NULL THEN 1 ELSE 0 END) val ' +
       '  FROM ' + QUOTENAME(@tablename) + ' ' +
       '  WHERE ' + QUOTENAME(@null_col_name) + ' IS NULL ' +
       ') uniq ' +
       'WHERE column_id = ' + CAST(@null_col_num AS NVARCHAR(10))
  
    EXECUTE sp_executesql @SQLString;
  
    -- PRINT @SQLString;
  
    FETCH NEXT FROM null_cur INTO @null_col_name, @null_col_num;
  END
  
  CLOSE null_cur;
  DEALLOCATE null_cur;

  -- Determine max length values
 
  DECLARE @len_col_name NVARCHAR(500) ,
          @len_col_num  INTEGER;

  DECLARE len_cur CURSOR
    LOCAL STATIC FORWARD_ONLY READ_ONLY FOR
      SELECT p.name,
             p.column_id
      FROM   #table_column_profile p
      WHERE  p.type NOT IN ('binary','bit','date','datetime','datetime2','datetimeoffset','geography','geometry','image','ntext','smalldatetime','time','timestamp','uniqueidentifier','varbinary');

  OPEN len_cur;
  
  FETCH NEXT FROM len_cur INTO @len_col_name, @len_col_num;
  
  WHILE @@FETCH_STATUS = 0
  BEGIN
    
    SELECT @SQLString = 
      N'UPDATE #table_column_profile ' +
       'SET max_length = val ' + 
       'FROM (' +
       '  SELECT MAX(LEN(' + QUOTENAME(@len_col_name) + ')) val ' +
       '  FROM ' + QUOTENAME(@tablename) + ' ' +
       ') uniq ' +
       'WHERE column_id = ' + CAST(@len_col_num AS NVARCHAR(10))

    EXECUTE sp_executesql @SQLString;
  
    -- PRINT @SQLString;
  
    FETCH NEXT FROM len_cur INTO @len_col_name, @len_col_num;
  END
  
  CLOSE len_cur;
  DEALLOCATE len_cur;
  
    
  SELECT [object_id] ,
         [column_id] ,
         [name] ,
         [type] ,
         [collation] ,
         [max_len] ,
         [precision] ,
         [scale] ,
         [is_nullable] ,
         [num_rows] ,
         [num_unique_values] ,
         [unique_ratio] , 
         [num_nulls] , 
         [nulls_ratio] ,
         [max_length]
  FROM #table_column_profile;
  
  DROP TABLE #table_column_profile;
  