# Changelog

All notable changes to `sp_DataProfile` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.0] - 2026-07-16

First release in over eight years — a major performance overhaul of the profiling
modes plus fast-count/approximate-distinct options, a switch to the MIT License, and
full documentation. Detailed notes: [docs/releases/v0.4.md](docs/releases/v0.4.md).

> ⚠️ **Breaking change:** the minimum supported SQL Server version is now **2012**
> (raised from 2005). The single-pass `CROSS APPLY (VALUES ...)` reshape used by the
> Mode 1 and Mode 2 rewrites requires SQL Server 2012. The proc hard-errors on older
> versions (`@SQLMajorVersion < 11`).

### Added
- `@ExactRowCount BIT = 0` — force an exact `COUNT_BIG(*)` instead of the fast
  metadata-based row count.
- `@ApproxDistinct BIT = 0` — use `APPROX_COUNT_DISTINCT` on SQL Server 2019+ (falls
  back to `COUNT(DISTINCT)` on older versions) for distinct/unique counts in Modes 1 and 4.
- MIT `LICENSE` file.
- In-file header comment block with version, mode reference, and usage examples.
- README overhaul: badges, quickstart, "What the output looks like" sample tables for
  Modes 0–2, table of contents, and a Contributing section, with examples reproducible
  against the StackOverflow sample database.
- `CLAUDE.md`, `docs/` (analysis, fix-plan), and a `tests/` folder.

### Changed
- **Mode 1 (Column Detail): single-pass rewrite.** The three per-metric cursors
  (unique / null / min–max length) are replaced by one wide aggregate scan reshaped via
  `UPDATE ... CROSS APPLY (VALUES ...)`. Turns 100+ scans on a wide table into one.
- **Mode 2 (Column Statistics): single-pass rewrite.** The per-column stats cursor
  (~2 scans per numeric column) is replaced by two dynamic batches — one aggregate scan
  for min/max/mean/std_dev and one `PERCENTILE_DISC` scan for medians. Now 2 scans total
  instead of ~2 per numeric column. Output columns and values are preserved.
- Row count now reads from `sys.dm_db_partition_stats` (near-instant, no scan) by default;
  falls back to `COUNT_BIG(*)` when sampling or when `@ExactRowCount = 1`.
- Mode 1 and the final Mode 0 select now honor `@ColumnList`.
- Min and max string length are computed in a single scan per string column instead of one each.
- `COUNT(DISTINCT)` is skipped on `nvarchar/varchar/varbinary(max)` LOB columns.
- Dynamic SQL variables widened from `NVARCHAR(4000)` to `NVARCHAR(MAX)`.
- Switched from the EULA/jorriss.com links to the MIT License and GitHub source in the
  README and proc header.

### Removed
- Dead `@DatabaseID` declaration and its populating `SELECT`.

### Fixed
- `QUOTENAME` applied to the median `ORDER BY` column and the Mode 3 catalog-view queries.
- Cross-database schema resolution in the column-type lookup.
- Unique-values cursor is now properly closed and deallocated.

## [0.3.0] - 2015-04-20

Last release of the original series (2014–2015). Established the five profiling modes
(Table Overview, Column Detail, Column Statistics, Candidate Key Check, Column Value
Distribution), `@ShowForeignKeys` / `@ShowIndexes`, `TABLESAMPLE` support, median via
`PERCENTILE_DISC` with a compatibility-level check, and case-sensitive-instance fixes.

[0.4.0]: https://github.com/Jorriss/sp_DataProfile/releases/tag/v0.4.0
[0.3.0]: https://github.com/Jorriss/sp_DataProfile/releases/tag/v0.3.0
