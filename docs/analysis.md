# sp_DataProfile — Analysis: Completeness, Performance, and Roadmap

_Analysis of `sp_DataProfile.sql` (v0.3, 1,235 lines)._

## What it does today

Five modes driven by `@Mode`: table overview (0), column detail (1), column statistics (2), candidate-key check (3), value distribution (4), plus optional FK/index output and `TABLESAMPLE` support. It's well-organized and the metadata handling (compat-level detection for `PERCENTILE_DISC`, version gating, cross-DB `QUOTENAME`) is thoughtful. Below is where it falls short on correctness, speed, and coverage.

---

## Correctness / completeness gaps (some are real bugs)

**1. `uniq_cur` is never closed or deallocated.** It's opened at `sp_DataProfile.sql:507` and the loop ends at line 533, but unlike `null_cur`, `len_cur`, and `stats_cur`, there's no `CLOSE uniq_cur; DEALLOCATE uniq_cur;`. It's `LOCAL` so it dies at proc exit, but it's a leak and inconsistent with the other cursors.

**2. `@SQLString` etc. are `NVARCHAR(4000)`, not `NVARCHAR(MAX)`** (lines 61-63). The index query with its two correlated `FOR XML` subqueries (lines 416-452) and the candidate-key `GROUP BY` string on a wide `@ColumnList` can silently truncate past 4000 chars, producing invalid SQL or wrong results on wide tables. `sp_executesql` also *requires* `NVARCHAR(MAX)`/`NVARCHAR(4000)` — the truncation is silent. Make all dynamic-SQL variables `NVARCHAR(MAX)`.

**3. SQL injection / identifier fragility.** `@TableName`, `@Schema`, and especially `@ColumnList` are concatenated raw into dynamic SQL (e.g. line 284, line 1100, line 1103). `QUOTENAME` is used in some spots but not consistently. A column named `[Order Date]` or a reserved word breaks unquoted paths, and this is exploitable if the proc is ever called with untrusted input. Wrap every identifier in `QUOTENAME()` and pass table/schema *names* as parameters to `sp_executesql` rather than string-concatenating literals.

**4. Median column not quoted.** Line 729 uses `ORDER BY ' + @stats_col_name` (raw), while min/max/mean use `QUOTENAME(@stats_col_name)`. Columns with spaces or reserved-word names fail only in the median path.

**5. Mode 3 metadata query isn't cross-database.** Lines 774-781 query bare `sys.tables`/`sys.columns` and `SCHEMA_ID()` — the current DB, not `@DatabaseName`. Every other metadata query qualifies with `QUOTENAME(@DatabaseName)`. Candidate-key check against a table in another DB will silently find no columns.

**6. `@DatabaseID` is dead code** — selected at line 165 and never used.

**7. `TABLESAMPLE` semantics.** With sampling on, `num_rows` comes from the sampled `COUNT_BIG(*)` and `COUNT(DISTINCT)` runs against the sample, so `unique_ratio`/`nulls_ratio` reflect the sample, not the table — worth documenting explicitly since users will read these as table-wide truths. `TABLESAMPLE` is also page-based, so small tables often return 0 or all rows.

**8. Empty string vs NULL.** `num_nulls` counts `IS NULL` only; empty strings and whitespace-only strings are invisible. For real profiling these are usually as important as NULLs.

---

## Performance — the big one

**The dominant cost is one-scan-per-metric-per-column.** In Mode 1, for a table with *C* columns you run: one `COUNT(DISTINCT)` scan per eligible column (lines 513-519), one `COUNT(*) WHERE IS NULL` scan per nullable column (lines 553-561), and two more scans (`MIN(LEN)`, `MAX(LEN)`) per string column (lines 598-625). A 30-column table can trigger **100+ full table scans**. Mode 2 is the same pattern: three separate scans per column for min/max, mean/stddev, and median.

The single highest-impact change: **collapse each mode into one pass over the base table.** Build one dynamic `SELECT` that emits all aggregates for all columns at once:

```sql
SELECT
  COUNT_BIG(*) AS num_rows,
  COUNT(DISTINCT [col1]) AS uniq_col1, SUM(CASE WHEN [col1] IS NULL THEN 1 ELSE 0 END) AS nulls_col1,
  MIN(LEN([col2])) AS minlen_col2, MAX(LEN([col2])) AS maxlen_col2, ...
FROM <table>;
```

One scan instead of ~100. `COUNT(DISTINCT)` still forces a sort/hash per column, but the table is read *once* and the aggregates pipeline. Unpivot the single wide result row into `#table_column_profile`. This alone is typically a 10-50x wall-clock win on wide tables.

Other techniques, in priority order:

- **Metadata row count instead of `COUNT_BIG(*)`.** For the overview, `SELECT SUM(row_count) FROM sys.dm_db_partition_stats WHERE object_id = ... AND index_id IN (0,1)` is instant and needs no scan (lines 311-315 currently scan the whole table just for the count). Offer it as the default with an `@ExactRowCount` flag for the precise version.
- **Batch min/max/avg/stddev/median in Mode 2 into one query per column** (four scans → one). Median (`PERCENTILE_DISC`) can't merge with the plain aggregates cheaply, but the other four can share a scan; and all columns' non-median stats can share the single table pass above.
- **Approximate distinct counts.** `APPROX_COUNT_DISTINCT()` (SQL 2019+, gate on compat level like the median check already does) is dramatically cheaper than `COUNT(DISTINCT)` for cardinality estimates on big tables — perfect for profiling where exactness rarely matters.
- **Skip impossible work.** You already exclude `max` types from length; also skip `COUNT(DISTINCT)` on `nvarchar(max)`/`varbinary(max)`/`xml`/`geography`/`geometry` — distinct on LOB/CLR types is very expensive and often meaningless.
- **`READ UNCOMMITTED` is already set** (line 59) — good for avoiding blocking, worth keeping but documenting the dirty-read tradeoff.

---

## What to add for a genuinely complete profiling tool

Grouped by value:

**Per-column depth (biggest gap):**
- ✅ **Blank/empty/whitespace counts** separate from NULL; zero-count and negative-count for numerics. *(Done — Mode 1 `num_blank`/`num_whitespace`/`num_zero`/`num_negative` + ratios.)*
- **Mode value + its frequency** (most common value), and top-N / bottom-N frequent values per column — right now the frequency distribution (Mode 4) works for only one column at a time.
- **Percentiles** P25/P75/P90/P95/P99 (you already have the `PERCENTILE_DISC` machinery for median — extend it), plus `SUM` and coefficient of variation for numerics.
- ✅ **Min/max *values*** — alphabetical first/last for strings, numeric extremes for number columns, not just min/max length. *(Done — Mode 1 `min_value`/`max_value`.)*
- ✅ **Cardinality classification** — auto-label each column as constant / binary / categorical / high-cardinality / unique-key-candidate from the unique_ratio you already compute. *(Done — Mode 1 `cardinality`, threshold `@CategoricalMaxDistinct`.)*
- **Pattern & format profiling** — detect emails, phone numbers, all-numeric-in-varchar, date-in-varchar, casing patterns, length histogram. This is what separates a "column detail" tool from a real profiler.
- **Data-type-mismatch detection** — count rows where a varchar column isn't `ISNUMERIC`/`TRY_CONVERT`-able to its apparent type.

**Schema / structural:**
- Primary key, default constraints, check constraints, and computed-column definitions in the overview (you already do FKs and indexes).
- Table size (MB), row count, compression, partition count, and last-stats-update — cheap from DMVs.

**Cross-column / relational:**
- **Orphan / referential-integrity check** — for FK columns, count child values absent from the parent.
- **Functional-dependency / composite-key discovery** — generalize the candidate-key check to suggest keys automatically.

**PII / governance:**
- Heuristic **sensitive-data flagging** (column-name + value-pattern based) — increasingly the reason people run a profiler.

**Operational:**
- **Persist results to a history table** (parameterize an output table) so profiles can be trended over time and diffed after ETL loads.
- **Batch mode** — profile every table in a schema/DB, or a wildcard, in one call, writing to that history table.
- The `tests/` folder currently holds only one ad-hoc script (`Checking Dates in Mode 3.sql`). A tSQLt (or even a scripted golden-output) harness covering each mode, cross-DB, sampling, and edge-case column names would make the correctness fixes above safe to land.

---

## Suggested priority order

1. **Correctness fixes** — `NVARCHAR(MAX)` for dynamic SQL, close `uniq_cur`, quote the median column, fix Mode 3 cross-DB. Low risk, prevents silent wrong results.
2. **Single-pass rewrite of Modes 1 and 2** — the 10-50x performance win.
3. **`QUOTENAME`/parameterization hardening** — security + odd-name support.
4. **Feature depth** — blank counts, percentiles, top-N values, cardinality classification.
5. **Operational** — history table + batch mode + a real test harness.
