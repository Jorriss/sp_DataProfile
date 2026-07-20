# sp_DataProfile

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
![SQL Server 2012+](https://img.shields.io/badge/SQL%20Server-2012%2B-CC2927?logo=microsoftsqlserver&logoColor=white)
![Language: T-SQL](https://img.shields.io/badge/language-T--SQL-blue.svg)

Point a single stored procedure at a table and get an instant profile of your data — no hand-written queries required.

**What you get:**

- Column metadata: type, length, precision, scale, nullability, collation.
- NULL and uniqueness: distinct/unique counts and ratios, NULL counts and ratios, min/max length.
- Statistics: min, max, mean, median, percentiles (P25/P75/P90/P95/P99), standard deviation, and coefficient of variation for numeric and date/time columns.
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
- Median and percentile calculations (Mode 2) require compatibility level 110 or higher, since they rely on `PERCENTILE_DISC`. Compatibility level is per-database, so a database set to a lower compat level runs fine but skips the median and percentile columns (mean, standard deviation, and coefficient of variation are still reported).

The examples below target the [StackOverflow sample database](https://www.brentozar.com/archive/2015/10/how-to-download-the-stack-overflow-database-via-bittorrent/) (`Users`, `Posts`), so you can reproduce them as-is.

## Usage

```sql
sp_DataProfile @TableName, @Mode, @ColumnList, ...
```

The behavior is driven by `@Mode`:

| Mode | Name | Description |
|------|------|-------------|
| 0 | Table Overview | Storage vitals (size, partitions, compression, last-stats-update) plus per-column type, length, precision, scale, nullability, and collation. *(default)* — [example](#examples) |
| 1 | Column Detail | Adds unique values/ratio and a cardinality classification, NULL count/ratio, soft-null counts/ratios (blank, whitespace, zero, negative), min/max length, and min/max value (alphabetical for string columns, numeric extremes for number columns) per column. — [example](#examples) |
| 2 | Column Statistics | Min, max, mean, median, percentiles (P25/P75/P90/P95/P99), standard deviation, and coefficient of variation for numeric and date/time columns. — [example](#examples) |
| 3 | Candidate Key Check | Given a `@ColumnList`, reports duplicate combinations so you can tell whether the columns form a unique key. — [example](#examples) |
| 4 | Column Value Distribution | Given a single column, reports each distinct value with its count and percentage of the table. — [example](#examples) |

You can set `@ShowForeignKeys = 1`, `@ShowIndexes = 1`, and/or `@ShowConstraints = 1` in **any** mode to also return the table's foreign keys, indexes, and constraints (primary key, default constraints, check constraints, and computed columns as a single result set). When more than one is enabled, the extra result sets come back in that order: foreign keys, then indexes, then constraints.

## What the output looks like

Every call first returns a **table header** result set:

| object_id | schema_name | table_name | row_count | is_sample |
|----------:|-------------|------------|----------:|-----------|
| 901578250 | dbo | Users | 2465713 | False |

In **Mode 0** the header carries four extra storage-vitals columns — `size_mb` (total reserved size, data + all indexes), `partition_count`, `data_compression` (`NONE`/`ROW`/`PAGE`/`COLUMNSTORE`…, or `Mixed` when partitions differ), and `last_stats_update` (most recent statistics update, `NULL` if the table has no statistics):

| object_id | schema_name | table_name | row_count | is_sample | size_mb | partition_count | data_compression | last_stats_update |
|----------:|-------------|------------|----------:|-----------|--------:|----------------:|------------------|-------------------|
| 901578250 | dbo | Users | 2465713 | False | 412.38 | 1 | NONE | 2018-12-02 08:14:11 |

These read from metadata (`sys.dm_db_partition_stats`, `sys.partitions`, `STATS_DATE()`), so they add no base-table scan.

Then a per-column result set. The columns shown depend on the mode (see the [mode table](#usage) for which metrics each mode adds); real output has one row per column.

**Mode 0 — Table Overview** (metadata only):

| column_id | name | system_type | length | precision | scale | is_nullable |
|----------:|------|-------------|-------:|----------:|------:|:-----------:|
| 1 | Id | int | 4 | 10 | 0 | 0 |
| 3 | CreationDate | datetime | 8 | 23 | 3 | 0 |
| 4 | DisplayName | nvarchar | 40 | 0 | 0 | 0 |
| 5 | Reputation | int | 4 | 10 | 0 | 0 |
| … | | | | | | |

**Mode 1 — Column Detail** (adds uniqueness, cardinality, NULL and soft-null metrics, and min/max value extremes). The full detail set adds `cardinality`, the soft-null counts and ratios (`num_blank`/`blank_ratio`, `num_whitespace`/`whitespace_ratio`, `num_zero`/`zero_ratio`, `num_negative`/`negative_ratio`), and `min_value`/`max_value`; a representative slice:

| name | num_unique_values | unique_ratio | cardinality | num_nulls | nulls_ratio | num_blank | num_zero | min_length | max_length | min_value | max_value |
|------|------------------:|-------------:|-------------|----------:|------------:|----------:|---------:|-----------:|-----------:|-----------|-----------|
| Id | 2465713 | 1.00000 | Unique | 0 | 0.00000 | | 0 | 4 | 4 | 1 | 2465713 |
| DisplayName | 2088731 | 0.84709 | High-cardinality | 0 | 0.00000 | 12 | | 1 | 40 | ! | ǆ |
| Age | 78 | 0.00003 | Categorical | 1631503 | 0.66167 | | 0 | 4 | 4 | 1 | 99 |
| WebsiteUrl | 356198 | 0.14446 | High-cardinality | 1900011 | 0.77058 | 40 | | 0 | 200 | | zzz.example |
| … | | | | | | | | | | | |

`cardinality` is one of *Constant* / *Binary* / *Unique* / *Categorical* / *High-cardinality*; the Categorical vs High-cardinality boundary is the distinct-count threshold `@CategoricalMaxDistinct` (default 50). Soft-null counts are populated only for the columns they apply to: blank/whitespace on string columns, zero/negative on numeric columns; other cells are `NULL`. `min_value`/`max_value` carry the alphabetical extremes for string columns and the numeric extremes for number columns.

**Mode 2 — Column Statistics** (adds min/max/mean/median/percentiles/stddev/CV for numeric and date/time columns):

| name | min_value | max_value | mean | median | p25 | p75 | p90 | p95 | p99 | std_dev | coeff_variation |
|------|-----------|-----------|------|--------|-----|-----|-----|-----|-----|---------|-----------------|
| Reputation | 1 | 1041991 | 137.14 | 1 | 1 | 101 | 421 | 1096 | 6874 | 2103.55 | 15.34 |
| Age | 13 | 99 | 34.82 | 32 | 25 | 43 | 54 | 60 | 71 | 12.91 | 0.37 |
| CreationDate | 2008-07-31 | 2018-12-02 | | | | | | | | | |
| … | | | | | | | | | | | |

**Percentiles (`p25` / `p75` / `p90` / `p95` / `p99`)** — a percentile is the value below which that percent of the rows fall. `p90 = 421` means 90% of the values are ≤ 421 and the top 10% are larger. Together with `median` (which is the 50th percentile) they describe the *shape* of the distribution, not just its center:

- `p25` and `p75` are the lower and upper quartiles. The gap between them (the interquartile range) is where the middle half of the data lives — a compact, outlier-resistant measure of spread.
- `p90` / `p95` / `p99` probe the upper tail. When they sit far above the `mean` and `median` — as with `Reputation` above (median 1, mean 137, p99 6,874) — the column is heavily right-skewed: a large mass of small values plus a long tail of big ones. When mean ≈ median and the percentiles rise evenly — as with `Age` — the distribution is roughly symmetric.

*How to use them:* compare `mean` against `median` to detect skew, then read the percentiles to size it. They're the right basis for capacity and SLA thinking ("what does the 95th-percentile order look like?") and for setting outlier thresholds, because unlike `mean`/`std_dev` they aren't dragged around by a handful of extreme values. Percentiles are computed with `PERCENTILE_DISC`, so they return an actual value present in the column (never an interpolated in-between one) and share `median`'s compatibility-level-110 requirement — below compat 110 these six columns are dropped.

**Coefficient of variation (`coeff_variation`)** — the standard deviation expressed as a fraction of the mean (`std_dev / mean`). Because it's unitless, it lets you compare relative variability across columns whose scales are wildly different — you can't tell whether a `std_dev` of 12.91 is "a lot" without knowing the mean, but a CV of 0.37 (Age) versus 15.34 (Reputation) says plainly that reputation is *far* more dispersed relative to its typical value than age is.

*How to use it:* read it as "spread per unit of average." Rules of thumb: below ~0.1 the column is nearly constant; around 1 the spread is comparable to the mean; well above 1 signals a highly volatile or long-tailed column worth a closer look. It's the quickest single number for ranking columns by how noisy they are. Two caveats: it's only meaningful for ratio-scale numerics (not dates), and it's undefined when the mean is 0 — in that case the column reports `NULL` rather than dividing by zero. Unlike the percentiles, CV rides the scalar-aggregate scan, so it's always present, including below compat 110.

**Mode 3 — Candidate Key Check** (given a `@ColumnList`, tests whether those columns form a unique key). The result set has one row per **duplicated** value combination — a combination that appears more than once — ordered by `row_count` descending. Each row carries `row_count` (how many rows share that combination), the profiled columns themselves, and `view_data_sql`, a ready-to-run `SELECT` that pulls the offending rows:

| row_count | DisplayName | Location | WebsiteUrl | CreationDate | view_data_sql |
|----------:|-------------|----------|------------|--------------|---------------|
| 4 | user123 | | | 2011-05-19 06:12:33 | SELECT * FROM [dbo].[Users]... |
| 2 | Alex | London, UK | | 2013-02-08 14:55:01 | SELECT * FROM [dbo].[Users]... |
| … | | | | | |

An **empty result set means the columns form a unique key** — no combination repeats. Any rows returned are the collisions that disqualify the combination as a key; `row_count` tells you how badly each one collides.

**Mode 4 — Column Value Distribution** (given a single column — the first one in `@ColumnList` — tallies each distinct value). The table header adds two columns, `column_name` (the column being distributed) and `distinct_row_count` (how many distinct values it holds):

| object_id | schema_name | table_name | row_count | column_name | distinct_row_count | is_sample |
|----------:|-------------|------------|----------:|-------------|-------------------:|-----------|
| 1330103555 | dbo | Posts | 17142169 | PostTypeId | 8 | False |

Then one row per distinct value, ordered by `Percentage` descending, with its `Count` and its `Percentage` of the table:

| PostTypeId | Count | Percentage |
|-----------:|------:|-----------:|
| 2 | 9760000 | 56.9350 |
| 1 | 6120000 | 35.7016 |
| 3 | 620000 | 3.6167 |
| … | | |

## Parameters

| Parameter | Type | Default | Notes |
|-----------|------|---------|-------|
| `@TableName` | `NVARCHAR(500)` | *(required)* | Table to profile. Accepts `schema.table`; defaults to the `dbo` schema if none is given. |
| `@Mode` | `TINYINT` | `0` | One of the modes above (0–4). |
| `@ColumnList` | `NVARCHAR(4000)` | `NULL` | Comma-separated column list. Required for Modes 3 and 4. Mode 4 uses only the first column supplied. |
| `@DatabaseName` | `NVARCHAR(128)` | current DB | Profile a table in another database on the same instance. |
| `@ShowForeignKeys` | `BIT` | `0` | Also return incoming and outgoing foreign keys. |
| `@ShowIndexes` | `BIT` | `0` | Also return indexes, including key/included columns, filter definitions, and `size_mb` (total reserved size per index). |
| `@ShowConstraints` | `BIT` | `0` | Also return a single result set of the table's constraints: primary key, default constraints, check constraints (with trusted/disabled flags), and computed columns (with their definitions and persisted flag). |
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

-- Overview with indexes, foreign keys, and constraints
sp_DataProfile 'Users', 0, @ShowIndexes = 1, @ShowForeignKeys = 1, @ShowConstraints = 1;

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
