
DECLARE @tablename VARCHAR(500);
SET @tablename = 'Users';

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
  [unique_percent] AS CAST([num_unique_values] AS FLOAT) / [num_rows] ,
  [num_nulls]          BIGINT        NULL ,
  [nulls_percent] AS CAST([num_nulls] AS FLOAT) / [num_rows] ,
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
       t.name ,
       c.collation_name ,
       c.max_length ,
       c.precision ,
       c.scale ,
       c.is_nullable
FROM   sys.tables t
JOIN   sys.columns c ON  t.object_id = c.object_id
JOIN   sys.types   typ ON c.system_type_id = typ.system_type_id
                       AND c.user_type_id = typ.user_type_id
WHERE  t.name = @tablename
ORDER BY c.column_id
;

DECLARE @sql_update_num_rows NVARCHAR(1000);
SET @sql_update_num_rows = 'UPDATE #table_column_profile SET num_rows = cnt FROM (SELECT COUNT(*) cnt FROM ' + @tablename + ') tablecount';

EXECUTE sp_executesql @sql_update_num_rows;

-- Determine unique values

DECLARE 
   @min_column_id INT,
   @max_column_id INT,
   @cur_column_id INT = 0,
   @sql_update_unique_val NVARCHAR(1000) = N'';

SELECT @min_column_id = MIN([column_id]), @max_column_id = MAX([column_id]) FROM #table_column_profile;

WHILE (@cur_column_id < @max_column_id)
BEGIN
  SET @cur_column_id += 1;

  SELECT @sql_update_unique_val = 
    N'UPDATE #table_column_profile ' +
     'SET num_unique_values = val ' + 
     'FROM (' +
     'SELECT COUNT(DISTINCT ' + p.name + ') val FROM ' + @tablename + ') uniq ' +
     'WHERE column_id = ' + CAST(p.column_id AS NVARCHAR(10))
  FROM #table_column_profile p
  WHERE [column_id] = @cur_column_id; 

  EXECUTE sp_executesql @sql_update_unique_val;

--  PRINT @sql_update_unique_val;
       
END

-- Determine null values
-- TODO: Change to cursor, process only null values.

DECLARE @sql_update_null_val NVARCHAR(1000) = N'';
SET @cur_column_id  = 0;

WHILE (@cur_column_id < @max_column_id)
BEGIN
  SET @cur_column_id += 1;

  SELECT @sql_update_null_val = 
    N'UPDATE #table_column_profile ' +
     'SET num_nulls = val ' + 
     'FROM (' +
     'SELECT SUM(CASE WHEN ' + p.name + ' IS NULL THEN 1 ELSE 0 END) val FROM ' + @tablename + ' ' +
     'WHERE ' + p.name + ' IS NULL ' +
     ') uniq ' +
     'WHERE column_id = ' + CAST(p.column_id AS NVARCHAR(10))
  FROM #table_column_profile p
  WHERE [column_id] = @cur_column_id; 

  EXECUTE sp_executesql @sql_update_null_val;

  PRINT @sql_update_null_val;
       
END

-- Determine max length values
-- TODO: Change to cursor, only character types

DECLARE @sql_update_len_val NVARCHAR(1000) = N'';
SET @cur_column_id  = 0;

WHILE (@cur_column_id < @max_column_id)
BEGIN
  SET @cur_column_id += 1;

  SELECT @sql_update_len_val = 
    N'UPDATE #table_column_profile ' +
     'SET max_length = val ' + 
     'FROM (' +
     'SELECT MAX(LEN(' + p.name + ')) val FROM ' + @tablename + ' ' +
     ') uniq ' +
     'WHERE column_id = ' + CAST(p.column_id AS NVARCHAR(10))
  FROM #table_column_profile p
  WHERE [column_id] = @cur_column_id; 

  EXECUTE sp_executesql @sql_update_len_val;

  PRINT @sql_update_len_val;
       
END


SELECT * FROM #table_column_profile;

DROP TABLE #table_column_profile
;
