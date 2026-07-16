# Plan: Correctness & Performance Fixes for sp_DataProfile

## Context

`sp_DataProfile.sql` (v0.3) is a single-file T-SQL profiling proc with five modes. A review surfaced several latent correctness bugs (silent dynamic-SQL truncation, an unclosed cursor, a cross-DB gap, an unquoted identifier) and a dominant performance problem: Modes 1 and 2 scan the base table once **per metric per column**, so a 30-column table can trigger 100+ full scans.

Goal: fix the correctness issues and the safe performance wins now, and stage the larger single-pass rewrite as a verified follow-up. All new SQL must be **version-adaptive** — detect the engine and emit the best statement it supports, reusing the existing `@SQLServerVersion` / `@SQLCompatLevel` detection already in the proc (`sp_DataProfile.sql:86-118`).

Decisions:
- **Phase it** — Phase 1 (correctness) + Phase 2 (safe perf) ship first; Phase 3 (single-pass rewrite) is a separate follow-up done after 1 & 2 are verified.
- **Version-adaptive** — pick the best construct per detected version/compat level. (Update: the Phase 3 Mode 1 rewrite raised the hard floor to **SQL Server 2012** so it can use `CROSS APPLY (VALUES ...)` for the reshape.)

---

## Phase 1 — Correctness fixes (low risk, do first)

All in `sp_DataProfile.sql`.

1. **Dynamic-SQL variables → `NVARCHAR(MAX)`.** Change `@SQLString`, `@SQLStringFK`, `@SQLStringIndexes` (lines 61-63) from `NVARCHAR(4000)` to `NVARCHAR(MAX)`. Also review `@ColumnList`-derived buffers (`@ColumnListClean`, `@ColumnListComma`, `@ColumnListString`, lines 172-196) — widen to `NVARCHAR(MAX)` to prevent truncation on wide tables. Prevents silent truncation of the index `FOR XML` query (lines 416-452) and the candidate-key `GROUP BY` string.

2. **Close/deallocate `uniq_cur`.** Add `CLOSE uniq_cur; DEALLOCATE uniq_cur;` after the unique-values loop ends (after line 533, before the null-cursor block), matching the other three cursors.

3. **Quote the median ORDER BY column.** Line 729 uses raw `@stats_col_name`; change to `QUOTENAME(@stats_col_name)` to match min/max/mean and support odd/reserved names.

4. **Make Mode 3 metadata query cross-database.** Lines 774-781 query bare `sys.tables`/`sys.columns` and `SCHEMA_ID()` (current DB only). Qualify with `QUOTENAME(@DatabaseName)` and resolve the schema id in-context, matching every other metadata query in the proc.

5. **Remove dead code.** Delete the unused `@DatabaseID` declaration and its populating `SELECT` (line 65 decl + lines 165-169), unless we choose to use `database_id` in Phase 2's metadata row count (see below) — in that case keep and reuse it.

6. **Document sampling semantics.** Add a header comment noting that with `@SampleValue` set, `num_rows`, distinct counts, and the ratio computed columns reflect the **sample**, not the full table (`TABLESAMPLE` is page-based). No logic change.

## Phase 2 — Safe performance wins (keeps existing structure)

7. **Metadata-based row count for the overview.** Replace the full `COUNT_BIG(*)` scan (lines 311-315) with `SUM(row_count)` from `sys.dm_db_partition_stats` where `index_id IN (0,1)`, qualified by database. Add an `@ExactRowCount BIT = 0` parameter that falls back to the current exact scan when set, and always use the exact scan when `@IsSample = 1` (sampling needs a real count).

8. **Combine per-column stat scans in Mode 2.** In the stats cursor (lines 668-750), merge the separate min/max query and the mean/stddev query into a single `SELECT` per column (four aggregates, one scan). Median (`PERCENTILE_DISC`) stays separate — it can't cheaply share the scan. Reduces Mode 2 from ~3 scans/column to ~2.

9. **Version-adaptive distinct counts.** Add a parsed major-version integer (from `@SQLServerVersion`, reusing the parse at line 88). Where the proc runs `COUNT(DISTINCT col)` (Mode 1 unique loop lines 513-519; Mode 4 distinct count lines 869-873), emit `APPROX_COUNT_DISTINCT(col)` when major version >= 15 (SQL 2019) **and** exactness isn't required, else keep `COUNT(DISTINCT)`. Gate behind a new `@ApproxDistinct BIT = 0` (opt-in) so default behavior is unchanged.

10. **Skip distinct on LOB/CLR types.** Extend the type filter feeding the unique cursor (line 505) to exclude `nvarchar(max)`/`varchar(max)`/`varbinary(max)`/`xml`/`geography`/`geometry`/`hierarchyid` — `COUNT(DISTINCT)` on these is expensive and rarely meaningful. (`max` detection via `length = -1`.)

## Phase 3 — Single-pass rewrite

**Mode 1 — done.** The three per-metric cursors were replaced with one code-gen cursor that builds a single wide `SELECT ... INTO #agg` (per-column `COUNT(DISTINCT)`/`SUM(CASE WHEN col IS NULL...)`, `MIN/MAX(LEN(col))`) — the only base-table scan — then reshapes that 1-row `#agg` into `#table_column_profile` with one `UPDATE ... CROSS APPLY (VALUES ...)`. Turns 100+ scans into 1. This raised the compatibility floor to SQL Server 2012 (needed for the `VALUES` reshape). An empty-select guard skips the whole block when no column qualifies.

**Mode 2 — still deferred.** Collapse Mode 2's non-median stats into one `SELECT ... INTO #agg` the same way (min/max/mean/stddev, one scan) and combine all medians into a single second scan (`PERCENTILE_DISC ... OVER ()` per column, aggregated to one row — window functions can't share the scalar-aggregate scan, so median stays a separate pass). Mode 2 lands at 2 scans total instead of ~2 per numeric column.

---

## Version-adaptive helper

Add a single derived integer early in the proc (next to the existing version check at line 88):
`@SQLMajorVersion INT = CAST(LEFT(@SQLServerVersion, CHARINDEX('.', @SQLServerVersion) - 1) AS INT);`
Reuse it (and the existing `@SQLCompatLevel`) for all feature gates so version logic lives in one place, mirroring the median gate already present (lines 722, 1044).

## Files to modify

- `sp_DataProfile.sql` — all changes above (single-file proc).
- `docs/analysis.md` — update the "Suggested priority order" section to reflect the phased decision (optional, keeps docs in sync).

## Verification

No automated harness exists yet (`tests/` holds one ad-hoc script). Verify manually against a known DB (e.g. StackOverflow sample, which the header examples use):

1. **Deploy**: run the full `sp_DataProfile.sql` to recreate the proc; confirm no compile errors.
2. **Regression per mode** — run each and confirm result shape/values are unchanged vs. current behavior:
   - `sp_dataprofile 'Users', 0` and with `@ShowIndexes=1, @ShowForeignKeys=1`
   - `sp_dataprofile 'Users', 1` — check `num_unique_values`, `num_nulls`, min/max length populate; run with `@Verbose=1` to confirm no cursor errors and inspect emitted SQL.
   - `sp_dataprofile 'Users', 2` — verify min/max/mean/stddev/median still correct after the combined-scan change; compare to a hand-written query on a small table.
   - `sp_dataprofile 'Posts', 4, 'PostTypeId'` — distinct count + distribution unchanged; test `@ApproxDistinct=1` on a 2019+ instance and confirm it's close to exact.
   - Candidate key: `sp_dataprofile 'Users', 3, 'DisplayName, Location, WebsiteUrl, CreationDate'` — confirm it works, and re-test **cross-database** by passing `@DatabaseName` to a different DB (validates fix #4).
3. **Truncation fix**: run Mode 0 with `@ShowIndexes=1` against a table with many indexes / included columns and a very wide `@ColumnList` in Mode 3; confirm no truncated/invalid SQL (validates fix #1).
4. **Row count**: compare `@ExactRowCount=0` (metadata) vs `=1` (scan) on a large table — counts should match (barring in-flight writes) and metadata path should be near-instant.
5. **Odd names**: create a temp table with a column like `[Order Date]` and a reserved-word column; run Modes 1-3 to confirm quoting fixes (#3) hold.
6. Capture STATISTICS IO / elapsed time before and after on a wide table to quantify the Phase 2 scan reduction.
