# sp_DataProfile

A single stored procedure for profiling data in Microsoft SQL Server. Point it at a table and it reports on the shape of your data — column metadata, uniqueness and NULL ratios, min/max/mean/median/standard deviation, candidate keys, and value distributions — without you having to hand-write the queries each time.

## Requirements

- SQL Server 2005 or higher (the proc refuses to run on 2000 and older).
- Median calculations (Mode 2) require compatibility level 110 or higher (SQL Server 2012+), since they rely on `PERCENTILE_DISC`. Lower compat levels run fine but skip the median column.

## Installation

Open [sp_DataProfile.sql](sp_DataProfile.sql) in SQL Server Management Studio (SSMS), connect to your instance, and execute the script (F5). It creates `dbo.sp_DataProfile` in the `master` database, so it can be called from any database and (because the name starts with `sp_`) resolves like a system procedure.

## Usage

```sql
sp_DataProfile @TableName, @Mode, @ColumnList, ...
```

The behavior is driven by `@Mode`:

| Mode | Name | Description |
|------|------|-------------|
| 0 | Table Overview | Row count plus per-column type, length, precision, scale, nullability, and collation. *(default)* |
| 1 | Column Detail | Adds number of unique values, unique ratio, NULL count, NULL ratio, and min/max length per column. |
| 2 | Column Statistics | Min, max, mean, median, and standard deviation for numeric and date/time columns. |
| 3 | Candidate Key Check | Given a `@ColumnList`, reports duplicate combinations so you can tell whether the columns form a unique key. |
| 4 | Column Value Distribution | Given a single column, reports each distinct value with its count and percentage of the table. |

You can set `@ShowForeignKeys = 1` and/or `@ShowIndexes = 1` in **any** mode to also return the table's foreign keys and indexes.

## Parameters

| Parameter | Type | Default | Notes |
|-----------|------|---------|-------|
| `@TableName` | `NVARCHAR(500)` | *(required)* | Table to profile. Accepts `schema.table`; defaults to the `dbo` schema if none is given. |
| `@Mode` | `TINYINT` | `0` | One of the modes above (0–4). |
| `@ColumnList` | `NVARCHAR(4000)` | `NULL` | Comma-separated column list. Required for Modes 3 and 4. Mode 4 uses only the first column supplied. |
| `@DatabaseName` | `NVARCHAR(128)` | current DB | Profile a table in another database on the same instance. |
| `@ShowForeignKeys` | `BIT` | `0` | Also return incoming and outgoing foreign keys. |
| `@ShowIndexes` | `BIT` | `0` | Also return indexes, including key/included columns and filter definitions. |
| `@SampleValue` | `INT` | `NULL` | Sample the table instead of scanning it all. Value between 0 and 100. |
| `@SampleType` | `NVARCHAR(50)` | `'PERCENT'` | `'PERCENT'` or `'ROWS'`, applied via `TABLESAMPLE`. |
| `@Verbose` | `BIT` | `0` | Print the generated dynamic SQL and progress messages for debugging. |

> **Note on sampling:** when `@SampleValue` is set, counts and ratios are computed against the sampled rows, not the whole table. `TABLESAMPLE` is page-based, so on small tables it may return all rows or none.

## Examples

```sql
-- Table overview
sp_DataProfile 'Users', 0;

-- Overview with indexes and foreign keys
sp_DataProfile 'Users', 0, @ShowIndexes = 1, @ShowForeignKeys = 1;

-- Column detail (unique counts, nulls, min/max length)
sp_DataProfile 'Users', 1;

-- Column statistics using a 10% sample of the table
sp_DataProfile 'Users', 2, @SampleValue = 10;

-- Candidate key check across several columns
sp_DataProfile 'Users', 3, 'DisplayName, Location, WebsiteUrl, CreationDate';

-- Value distribution for a single column
sp_DataProfile 'Posts', 4, 'PostTypeId';

-- Profile a table in another database
sp_DataProfile 'Users', 1, @DatabaseName = 'StackOverflow';
```

## Notes

- The proc runs under `READ UNCOMMITTED`, so it won't block writers — at the cost of possible dirty reads.
- Modes 1 and 2 scan the table once per metric per column, which can be expensive on wide or large tables. See [docs/analysis.md](docs/analysis.md) for a deeper look at behavior, known gaps, and the performance roadmap.

## License

© 2026 Jorriss LLC. See the [End User Licensing Agreement](http://jorriss.com/eula). Additional documentation lives at <http://www.jorriss.com/spdataprofile>.

Source: <https://github.com/Jorriss/sp_DataProfile>
