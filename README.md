# sp_DataProfile

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
![SQL Server 2012+](https://img.shields.io/badge/SQL%20Server-2012%2B-CC2927?logo=microsoftsqlserver&logoColor=white)
![Language: T-SQL](https://img.shields.io/badge/language-T--SQL-blue.svg)

Point a single stored procedure at a table and get an instant profile of your data — no hand-written queries required.

**What you get:**

- Column metadata: type, length, precision, scale, nullability, collation.
- NULL and uniqueness: distinct/unique counts and ratios, NULL counts and ratios, min/max length.
- Statistics: min, max, mean, median, and standard deviation for numeric and date/time columns.
- Candidate key checks: tell whether a set of columns forms a unique key.
- Value distributions: every distinct value in a column with its count and percentage.
- Optional foreign keys and indexes, in any mode.

## Quickstart

1. **Install** — open [sp_DataProfile.sql](sp_DataProfile.sql) in SQL Server Management Studio (SSMS), connect to your instance, and execute (F5). It creates `dbo.sp_DataProfile` in `master`, so you can call it from any database.
2. **Run it** against any table:
   ```sql
   sp_DataProfile 'Users', 0;
   ```
3. See [what the output looks like](#what-the-output-looks-like) below.

## Contents

- [Requirements](#requirements)
- [Usage](#usage)
- [What the output looks like](#what-the-output-looks-like)
- [Parameters](#parameters)
- [Examples](#examples)
- [Notes](#notes)
- [Contributing](#contributing)
- [License](#license)

## Requirements

- SQL Server 2012 or higher (the proc refuses to run on 2008 R2 and older).
- Median calculations (Mode 2) require compatibility level 110 or higher, since they rely on `PERCENTILE_DISC`. Compatibility level is per-database, so a database set to a lower compat level runs fine but skips the median column.

The examples below target the [StackOverflow sample database](https://www.brentozar.com/archive/2015/10/how-to-download-the-stack-overflow-database-via-bittorrent/) (`Users`, `Posts`), so you can reproduce them as-is.

## Usage

```sql
sp_DataProfile @TableName, @Mode, @ColumnList, ...
```

The behavior is driven by `@Mode`:

| Mode | Name | Description |
|------|------|-------------|
| 0 | Table Overview | Row count plus per-column type, length, precision, scale, nullability, and collation. *(default)* — [example](#examples) |
| 1 | Column Detail | Adds unique values/ratio and a cardinality classification, NULL count/ratio, soft-null counts/ratios (blank, whitespace, zero, negative), min/max length, and min/max string value per column. — [example](#examples) |
| 2 | Column Statistics | Min, max, mean, median, and standard deviation for numeric and date/time columns. — [example](#examples) |
| 3 | Candidate Key Check | Given a `@ColumnList`, reports duplicate combinations so you can tell whether the columns form a unique key. — [example](#examples) |
| 4 | Column Value Distribution | Given a single column, reports each distinct value with its count and percentage of the table. — [example](#examples) |

You can set `@ShowForeignKeys = 1` and/or `@ShowIndexes = 1` in **any** mode to also return the table's foreign keys and indexes.

## What the output looks like

Every call first returns a **table header** result set:

| object_id | schema_name | table_name | row_count | is_sample |
|----------:|-------------|------------|----------:|-----------|
| 901578250 | dbo | Users | 2465713 | False |

Then a per-column result set. The columns shown depend on the mode (see the [mode table](#usage) for which metrics each mode adds); real output has one row per column.

**Mode 0 — Table Overview** (metadata only):

| column_id | name | system_type | length | precision | scale | is_nullable |
|----------:|------|-------------|-------:|----------:|------:|:-----------:|
| 1 | Id | int | 4 | 10 | 0 | 0 |
| 3 | CreationDate | datetime | 8 | 23 | 3 | 0 |
| 4 | DisplayName | nvarchar | 40 | 0 | 0 | 0 |
| 5 | Reputation | int | 4 | 10 | 0 | 0 |
| … | | | | | | |

**Mode 1 — Column Detail** (adds uniqueness, cardinality, NULL and soft-null metrics, and string extremes). The full detail set adds `cardinality`, the soft-null counts and ratios (`num_blank`/`blank_ratio`, `num_whitespace`/`whitespace_ratio`, `num_zero`/`zero_ratio`, `num_negative`/`negative_ratio`), and `min_value`/`max_value`; a representative slice:

| name | num_unique_values | unique_ratio | cardinality | num_nulls | nulls_ratio | num_blank | num_zero | min_length | max_length | min_value | max_value |
|------|------------------:|-------------:|-------------|----------:|------------:|----------:|---------:|-----------:|-----------:|-----------|-----------|
| Id | 2465713 | 1.00000 | Unique | 0 | 0.00000 | | 0 | 4 | 4 | | |
| DisplayName | 2088731 | 0.84709 | High-cardinality | 0 | 0.00000 | 12 | | 1 | 40 | ! | ǆ |
| Age | 78 | 0.00003 | Categorical | 1631503 | 0.66167 | | 0 | 4 | 4 | | |
| WebsiteUrl | 356198 | 0.14446 | High-cardinality | 1900011 | 0.77058 | 40 | | 0 | 200 | | zzz.example |
| … | | | | | | | | | | | |

`cardinality` is one of *Constant* / *Binary* / *Unique* / *Categorical* / *High-cardinality*; the Categorical vs High-cardinality boundary is the distinct-count threshold `@CategoricalMaxDistinct` (default 50). Soft-null counts are populated only for the columns they apply to: blank/whitespace and min/max value on string columns, zero/negative on numeric columns; other cells are `NULL`.

**Mode 2 — Column Statistics** (adds min/max/mean/median/stddev for numeric and date/time columns):

| name | min_value | max_value | mean | median | std_dev |
|------|-----------|-----------|------|--------|---------|
| Reputation | 1 | 1041991 | 137.14 | 1 | 2103.55 |
| Age | 13 | 99 | 34.82 | 32 | 12.91 |
| CreationDate | 2008-07-31 | 2018-12-02 | | | |
| … | | | | | |

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
| `@ExactRowCount` | `BIT` | `0` | Force an exact `COUNT_BIG(*)` row count. Off by default, the row count is read from table metadata (`sys.dm_db_partition_stats`) — near-instant, no scan. Sampling forces this on automatically. |
| `@ApproxDistinct` | `BIT` | `0` | Use `APPROX_COUNT_DISTINCT` (SQL Server 2019+) for distinct/unique counts in Modes 1 and 4; falls back to `COUNT(DISTINCT)` on older versions. |
| `@CategoricalMaxDistinct` | `INT` | `50` | Mode 1 cardinality threshold: an eligible column with distinct count ≤ this value (and not Constant/Binary/Unique) is labeled *Categorical*; above it, *High-cardinality*. |
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

-- Column detail across several columns (unique counts, nulls, min/max length)
sp_DataProfile 'Posts', 1, 'AnswerCount, CreationDate', @ApproxDistinct = 1;

-- Column statistics using a 10% sample of the table
sp_DataProfile 'Users', 2, @SampleValue = 10;

-- Candidate key check across several columns
sp_DataProfile 'Users', 3, 'DisplayName, Location, WebsiteUrl, CreationDate';

-- Value distribution for a single column
sp_DataProfile 'Posts', 4, 'PostTypeId';

-- Profile a table in another database
sp_DataProfile 'Users', 1, @DatabaseName = 'StackOverflow';

-- Force an exact row count instead of the fast metadata read
sp_DataProfile 'Users', 0, @ExactRowCount = 1;

-- Approximate distinct counts (fast on large tables, SQL Server 2019+)
sp_DataProfile 'Posts', 1, @ApproxDistinct = 1;
```

## Notes

- The proc runs under `READ UNCOMMITTED`, so it won't block writers — at the cost of possible dirty reads.
- By default the row count is read from table metadata (`sys.dm_db_partition_stats`), so Mode 0 is near-instant even on huge tables and no scan is needed. Set `@ExactRowCount = 1` (or use sampling) to force a real `COUNT_BIG(*)`.
- Mode 2 computes min/max and mean/standard deviation in a single scan per column, and `COUNT(DISTINCT)` is skipped on `(max)` LOB columns (`nvarchar(max)`, `varchar(max)`, `varbinary(max)`) where it is expensive and rarely meaningful.
- Modes 1 and 2 still scan the table once per metric per column, which can be expensive on wide or large tables. See [docs/analysis.md](docs/analysis.md) for a deeper look at behavior, known gaps, and the performance roadmap.

## Contributing

Development is tracked through [GitHub Issues](https://github.com/Jorriss/sp_DataProfile/issues) — bug reports, feature ideas, and pull requests are all welcome. Commit messages reference the issue they resolve (e.g. `Fixed #15`); please keep that convention in PRs.

## License

Released under the [MIT License](LICENSE). © 2026 Jorriss LLC.

Source: <https://github.com/Jorriss/sp_DataProfile>
