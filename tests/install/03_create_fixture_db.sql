/*───────────────────────────────────────────────────────────────────────────
  03_create_fixture_db.sql  —  the deterministic micro-fixture database

  Creates [DataProfileTest] and seeds tiny, hand-reasoned tables whose every
  profiled value is a known literal. These are COMMITTED (not per-test temp
  tables): the Mode 3 loopback capture opens a separate connection and can only
  see committed rows, so fixtures must live here, outside any test transaction.

  All seeding uses VALUES row constructors — safe at the SQL Server 2012 floor.

  Re-runnable: drops and recreates the tables (keeps the DB). TRUSTWORTHY ON is
  set because tSQLt's CLR install requires it (see 02_install_tsqlt.sql).

  Expected-value cheat sheet (asserted by tests/unit/ *.sql) is documented next to
  each table so the literals in the tests are traceable to the seed data.
───────────────────────────────────────────────────────────────────────────*/
SET NOCOUNT ON;

IF DB_ID('DataProfileTest') IS NULL
BEGIN
    RAISERROR('Creating database DataProfileTest...', 0, 1) WITH NOWAIT;
    EXEC('CREATE DATABASE [DataProfileTest];');
END
GO

ALTER DATABASE [DataProfileTest] SET TRUSTWORTHY ON;   /* required by tSQLt CLR install */
GO

/* Keep compat level modern so median (PERCENTILE_DISC, needs 110+) is exercised
   by default. Mode 2 / VersionMatrix tests toggle this deliberately and reset it. */
DECLARE @sql NVARCHAR(200) = N'ALTER DATABASE [DataProfileTest] SET COMPATIBILITY_LEVEL = '
    + CAST((SELECT CAST(SERVERPROPERTY('ProductMajorVersion') AS INT) * 10) AS NVARCHAR(10)) + N';';
/* ProductMajorVersion*10 → 2012=110 ... 2019=150 ... i.e. the host's native level. */
BEGIN TRY EXEC(@sql); END TRY BEGIN CATCH /* older host: leave as-is */ END CATCH;
GO

USE [DataProfileTest];
GO

/*═══════════════════════════════════════════════════════════════════════════
  AllTypes — one column per supported data type. Exercises type-branching in
  every mode. 3 rows. Only the string columns' min/max length are value-asserted;
  the rest are covered by "runs without error" (Mode 1 test 12).
═══════════════════════════════════════════════════════════════════════════*/
IF OBJECT_ID('dbo.AllTypes') IS NOT NULL DROP TABLE dbo.AllTypes;
CREATE TABLE dbo.AllTypes (
    c_int              INT              NOT NULL,
    c_bigint           BIGINT           NOT NULL,
    c_decimal          DECIMAL(9,2)     NOT NULL,
    c_bit              BIT              NOT NULL,
    c_date             DATE             NOT NULL,
    c_time             TIME             NOT NULL,
    c_datetime2        DATETIME2        NOT NULL,
    c_datetimeoffset   DATETIMEOFFSET   NOT NULL,
    c_smalldatetime    SMALLDATETIME    NOT NULL,
    c_datetime         DATETIME         NOT NULL,
    c_varchar          VARCHAR(50)      NOT NULL,
    c_nvarchar         NVARCHAR(50)     NOT NULL,
    c_uid              UNIQUEIDENTIFIER NOT NULL
);
INSERT INTO dbo.AllTypes
  (c_int, c_bigint, c_decimal, c_bit, c_date, c_time, c_datetime2, c_datetimeoffset,
   c_smalldatetime, c_datetime, c_varchar, c_nvarchar, c_uid)
VALUES
  (1, 100, 1.50, 0, '2020-01-01', '01:00:00', '2020-01-01T01:00:00', '2020-01-01T01:00:00+00:00',
   '2020-01-01', '2020-01-01T01:00:00', 'a',    N'x',      '11111111-1111-1111-1111-111111111111'),
  (2, 200, 2.50, 1, '2020-06-15', '12:30:00', '2020-06-15T12:30:00', '2020-06-15T12:30:00+00:00',
   '2020-06-15', '2020-06-15T12:30:00', 'bb',   N'yy',     '22222222-2222-2222-2222-222222222222'),
  (3, 300, 3.50, 0, '2020-12-31', '23:59:59', '2020-12-31T23:59:59', '2020-12-31T23:59:59+00:00',
   '2020-12-31', '2020-12-31T23:59:59', 'ccc',  N'zzz',    '33333333-3333-3333-3333-333333333333');
/* Mode 1 asserted values for the string columns:
     c_varchar : min_length = LEN('a')   = 1, max_length = LEN('ccc')  = 3
     c_nvarchar: min_length = LEN('x')   = 1, max_length = LEN('zzz')  = 3
   All columns NOT NULL → num_nulls = NULL for every column (locks the "NOT NULL
   yields NULL, not 0" behaviour). */
GO

/*═══════════════════════════════════════════════════════════════════════════
  Nullable — pins num_nulls / distinct / length. `s` is the clean asserted
  column; `soft` seeds empty/whitespace values that drive the soft-null feature
  (#1): num_blank (empty) and num_whitespace (all-space, non-empty). 6 rows.
═══════════════════════════════════════════════════════════════════════════*/
IF OBJECT_ID('dbo.Nullable') IS NOT NULL DROP TABLE dbo.Nullable;
CREATE TABLE dbo.Nullable (
    id    INT         NOT NULL,   -- NOT NULL → num_nulls = NULL
    s     VARCHAR(50)     NULL,   -- clean, value-asserted
    soft  VARCHAR(50)     NULL    -- '', whitespace seeds for future #1
);
INSERT INTO dbo.Nullable (id, s, soft) VALUES
  (1, 'apple', 'x'   ),
  (2,  NULL,   ''    ),
  (3, 'apple', '   ' ),   -- 3 spaces
  (4, 'pear',  NULL  ),
  (5,  NULL,   '0'   ),
  (6, 'kiwi',  'y'   );
/* Mode 1 asserted values:
     id  : num_nulls = NULL (NOT NULL), num_unique_values = 6
     s   : num_nulls = 2  (rows 2,5)
           num_unique_values = 3  (distinct {'apple','pear','kiwi'})
           min_length = LEN('pear'|'kiwi') = 4, max_length = LEN('apple') = 5
           min_value = 'apple', max_value = 'pear' (alphabetical extremes)
     soft: num_nulls = 1  (row 4)
           num_blank = 1  (row 2 '' → DATALENGTH 0)
           num_whitespace = 1  (row 3 '   ' → LEN 0, DATALENGTH 3)
           (distinct/length still un-asserted: '' and '   ' compare equal under
           SQL Server trailing-space collation, so those stay ambiguous.) */
GO

/*═══════════════════════════════════════════════════════════════════════════
  Cardinality — constant / binary / unique / categorical columns. Pins distinct
  counts and unique_ratio; also drives Mode 4 distribution. 6 rows.
═══════════════════════════════════════════════════════════════════════════*/
IF OBJECT_ID('dbo.Cardinality') IS NOT NULL DROP TABLE dbo.Cardinality;
CREATE TABLE dbo.Cardinality (
    const_col  INT         NOT NULL,   -- all 7        → distinct 1 (constant)
    bin_col    INT         NOT NULL,   -- 0/1          → distinct 2 (binary)
    uniq_col   INT         NOT NULL,   -- 1..6         → distinct 6, unique_ratio 1
    cat_col    VARCHAR(10) NOT NULL    -- A/B/C        → distinct 3 (categorical)
);
INSERT INTO dbo.Cardinality (const_col, bin_col, uniq_col, cat_col) VALUES
  (7, 0, 1, 'A'),
  (7, 1, 2, 'A'),
  (7, 0, 3, 'B'),
  (7, 1, 4, 'B'),
  (7, 0, 5, 'C'),
  (7, 1, 6, 'A');
/* Mode 1 num_unique_values: const_col=1, bin_col=2, uniq_col=6, cat_col=3.
   Mode 1 cardinality (default @CategoricalMaxDistinct=50, 6 rows):
     const_col → 'Constant' (distinct 1), bin_col → 'Binary' (distinct 2),
     uniq_col → 'Unique' (distinct = num_rows), cat_col → 'Categorical' (3 <= 50).
     With @CategoricalMaxDistinct=2, cat_col (distinct 3) → 'High-cardinality'.
   Mode 1 zero counts: bin_col num_zero=3 (rows 1,3,5); const_col/uniq_col num_zero=0.
   Mode 4 on cat_col: A=3 (50%), B=2 (33.33%), C=1 (16.67%); distinct_row_count=3. */
GO

/*═══════════════════════════════════════════════════════════════════════════
  Stats — numeric column with hand-computed statistics. Values {1,1,3,5,5}
  chosen so every Mode 2 metric is an exact literal (odd row count → unambiguous
  PERCENTILE_DISC). 5 rows.
═══════════════════════════════════════════════════════════════════════════*/
IF OBJECT_ID('dbo.Stats') IS NOT NULL DROP TABLE dbo.Stats;
CREATE TABLE dbo.Stats ( val INT NOT NULL );
INSERT INTO dbo.Stats (val) VALUES (1),(1),(3),(5),(5);
/* Mode 2 asserted values (proc stores each as NVARCHAR):
     min_value = '1'
     max_value = '5'
     mean      = '3'       (AVG over INT = 15/5 = 3, exact — no truncation here)
     median    = '3'       (PERCENTILE_DISC(0.5) of 1,1,3,5,5)
     std_dev   = '2.0000'  (sample STDEV: sum sq dev 16 / (n-1)=4 → var 4 → 2, cast NUMERIC(18,4)) */
GO

/*═══════════════════════════════════════════════════════════════════════════
  SoftNumbers — numeric soft-null counts (#1): zeros and negatives. `n` is NOT
  NULL so counts are unambiguous; `n_nullable` mixes a NULL to prove zero/negative
  counting skips NULLs. 5 rows.
═══════════════════════════════════════════════════════════════════════════*/
IF OBJECT_ID('dbo.SoftNumbers') IS NOT NULL DROP TABLE dbo.SoftNumbers;
CREATE TABLE dbo.SoftNumbers (
    n          INT NOT NULL,   -- {0,-5,3,0,-1}
    n_nullable INT     NULL    -- {0,NULL,-2,4,0}
);
INSERT INTO dbo.SoftNumbers (n, n_nullable) VALUES
  ( 0,  0   ),
  (-5,  NULL),
  ( 3, -2   ),
  ( 0,  4   ),
  (-1,  0   );
/* Mode 1 asserted values:
     n         : num_zero = 2 (rows 1,4), num_negative = 2 (rows 2,5), num_nulls = NULL,
                 min_value = -5, max_value = 3
     n_nullable: num_zero = 2 (rows 1,5), num_negative = 1 (row 3), num_nulls = 1 (row 2),
                 min_value = -2, max_value = 4 (NULLs skipped by MIN/MAX)
   Both are numeric → num_blank / num_whitespace = NULL; min_value / max_value are the
   numeric extremes rendered as NVARCHAR. */
GO

/*═══════════════════════════════════════════════════════════════════════════
  Keys — Mode 3 candidate-key check. (k1,k2) is unique (a candidate key);
  (k1,v) is not. 4 rows.
═══════════════════════════════════════════════════════════════════════════*/
IF OBJECT_ID('dbo.Keys') IS NOT NULL DROP TABLE dbo.Keys;
CREATE TABLE dbo.Keys ( k1 INT NOT NULL, k2 INT NOT NULL, v INT NOT NULL );
INSERT INTO dbo.Keys (k1, k2, v) VALUES
  (1, 1, 10),
  (1, 2, 10),
  (2, 1, 20),
  (2, 2, 10);
/* Mode 3:
     @ColumnList = 'k1,k2' → no GROUP BY ... HAVING COUNT(*)>1 rows → candidate key (0 dup rows).
     @ColumnList = 'k1,v'  → (1,10) appears twice → 1 duplicate group row → NOT a candidate key. */
GO

/*═══════════════════════════════════════════════════════════════════════════
  Parent / Child — real FK for Mode 0 FK output. FK added WITH NOCHECK so a
  deliberately orphaned child (parent_id 99) can be seeded for the future orphan
  check (#10). Mode 0 test only asserts the FK row is emitted.
═══════════════════════════════════════════════════════════════════════════*/
IF OBJECT_ID('dbo.Child')  IS NOT NULL DROP TABLE dbo.Child;
IF OBJECT_ID('dbo.Parent') IS NOT NULL DROP TABLE dbo.Parent;
CREATE TABLE dbo.Parent ( parent_id INT NOT NULL CONSTRAINT PK_Parent PRIMARY KEY );
INSERT INTO dbo.Parent (parent_id) VALUES (1),(2),(3);
CREATE TABLE dbo.Child (
    child_id  INT NOT NULL CONSTRAINT PK_Child PRIMARY KEY,
    parent_id INT NULL
);
INSERT INTO dbo.Child (child_id, parent_id) VALUES
  (10, 1),
  (11, 2),
  (12, 99);   -- orphan (no matching Parent) — seed for future #10
ALTER TABLE dbo.Child WITH NOCHECK
    ADD CONSTRAINT FK_Child_Parent FOREIGN KEY (parent_id) REFERENCES dbo.Parent (parent_id);
/* Mode 0 @ShowForeignKeys=1 on Child → one FK row naming Parent/parent_id. */
GO

/*═══════════════════════════════════════════════════════════════════════════
  [Odd Names] — QUOTENAME regression: spaced table name, spaced column, and a
  reserved word column. 3 rows.
═══════════════════════════════════════════════════════════════════════════*/
IF OBJECT_ID('dbo.[Odd Names]') IS NOT NULL DROP TABLE dbo.[Odd Names];
CREATE TABLE dbo.[Odd Names] (
    [Order Date] DATE        NOT NULL,
    [Select]     INT         NOT NULL,   -- reserved word
    [My Col]     VARCHAR(20)     NULL
);
INSERT INTO dbo.[Odd Names] ([Order Date], [Select], [My Col]) VALUES
  ('2021-01-01', 1, 'aa'),
  ('2021-02-02', 2, NULL),
  ('2021-03-03', 2, 'cccc');
/* Quoting tests assert Modes 1/2/3 succeed and return correct per-column rows:
     [Select]: num_unique_values = 2; [My Col]: num_nulls = 1, min_len 2 / max_len 4.
     Mode 3 @ColumnList='[Select]' → (2) duplicated → 1 duplicate group row. */
GO

/*═══════════════════════════════════════════════════════════════════════════
  DataProfileTest_Compat100 — a second committed DB pinned at compatibility
  level 100 (below the 110 median floor). The proc gates median on
  MIN(master compat, target-DB compat) (sp_DataProfile.sql:119), so profiling a
  Stats copy here forces the graceful-degradation path (median column dropped +
  severity-0 warning) WITHOUT toggling compat inside a test — ALTER DATABASE
  can't run inside tSQLt's per-test transaction. Mode2/VersionMatrix degradation
  tests point @DatabaseName at this DB.
═══════════════════════════════════════════════════════════════════════════*/
USE [master];
GO
IF DB_ID('DataProfileTest_Compat100') IS NULL
    EXEC('CREATE DATABASE [DataProfileTest_Compat100];');
GO
ALTER DATABASE [DataProfileTest_Compat100] SET COMPATIBILITY_LEVEL = 100;  -- valid on 2016–2022
GO
USE [DataProfileTest_Compat100];
GO
IF OBJECT_ID('dbo.Stats') IS NOT NULL DROP TABLE dbo.Stats;
CREATE TABLE dbo.Stats ( val INT NOT NULL );
INSERT INTO dbo.Stats (val) VALUES (1),(1),(3),(5),(5);   -- same seed as DataProfileTest.Stats
GO

USE [DataProfileTest];
GO
RAISERROR('DataProfileTest (+ _Compat100) fixtures created and committed.', 0, 1) WITH NOWAIT;
GO
