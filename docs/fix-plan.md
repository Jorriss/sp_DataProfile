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

1. ✅ **Done.** **Dynamic-SQL variables → `NVARCHAR(MAX)`.** `@SQLString`, `@SQLStringFK`, `@SQLStringIndexes` and the `@ColumnList`-derived buffers (`@ColumnListClean`, `@ColumnListComma`, `@ColumnListString`) are now `NVARCHAR(MAX)`. Prevents silent truncation of the index `FOR XML` query and the candidate-key `GROUP BY` string.

2. ✅ **Done (superseded).** **Close/deallocate `uniq_cur`.** The old unique-values cursor was removed by the Phase 3 Mode 1 single-pass rewrite; its replacement (`m1_cur`) is properly closed and deallocated.

3. ✅ **Done.** **Quote the median ORDER BY column.** The median `ORDER BY` now uses `QUOTENAME(@stats_col_name)`, matching min/max/mean and supporting odd/reserved names.

4. ✅ **Done.** **Make Mode 3 metadata query cross-database.** The Mode 3 metadata query is now qualified with `QUOTENAME(@DatabaseName)` and joins `sys.schemas` in-context, matching every other metadata query in the proc.

5. ✅ **Done.** **Remove dead code.** The unused `@DatabaseID` declaration and its populating `SELECT` were deleted (Phase 2's metadata row count uses `OBJECT_ID(...)`, not `database_id`).

6. ✅ **Done.** **Document sampling semantics.** A header comment now notes that with `@SampleValue` set, `num_rows`, distinct counts, and the ratio computed columns reflect the **sample**, not the full table.

## Phase 2 — Safe performance wins (keeps existing structure)

7. ✅ **Done.** **Metadata-based row count for the overview.** The default path reads `SUM(row_count)` from `sys.dm_db_partition_stats` (`index_id IN (0,1)`), qualified by database. The new `@ExactRowCount BIT = 0` parameter forces the exact `COUNT_BIG(*)` scan, which is also always used when `@IsSample = 1`.

8. ✅ **Done.** **Combine per-column stat scans in Mode 2.** The stats cursor merges min/max/mean/stddev into a single `SELECT` per column (one scan). Median (`PERCENTILE_DISC`) stays separate.

9. ✅ **Done.** **Version-adaptive distinct counts.** `@SQLMajorVersion` is parsed once from `@SQLServerVersion`. Mode 1 (single-pass agg) and Mode 4 emit `APPROX_COUNT_DISTINCT(col)` when major version >= 15 (SQL 2019), gated behind the opt-in `@ApproxDistinct BIT = 0` so default behavior is unchanged.

10. ✅ **Done.** **Skip distinct on LOB/CLR types.** Mode 1's distinct-eligibility check excludes `nvarchar(max)`/`varchar(max)`/`varbinary(max)` (via `length = -1`); `xml`/`geography`/`geometry`/`hierarchyid` are not in the valid-type list at all.

## Phase 3 — Single-pass rewrite

**Mode 1 — done.** The three per-metric cursors were replaced with one code-gen cursor that builds a single wide `SELECT ... INTO #agg` (per-column `COUNT(DISTINCT)`/`SUM(CASE WHEN col IS NULL...)`, `MIN/MAX(LEN(col))`) — the only base-table scan — then reshapes that 1-row `#agg` into `#table_column_profile` with one `UPDATE ... CROSS APPLY (VALUES ...)`. Turns 100+ scans into 1. This raised the compatibility floor to SQL Server 2012 (needed for the `VALUES` reshape). An empty-select guard skips the whole block when no column qualifies.

**Mode 2 — done.** The per-column stats cursor now only accumulates strings, then runs two dynamic batches: Batch A builds one wide `SELECT ... INTO #agg` (min/max for every non-bit type, mean/stddev for numerics) — one scan — and reshapes it into `#table_column_profile` with a single `UPDATE ... CROSS APPLY (VALUES ...)`; Batch B builds one `SELECT DISTINCT ... INTO #median` of every numeric column's `PERCENTILE_DISC(...) OVER ()` — a second scan (window functions can't share the scalar-aggregate scan) — reshaped the same way. Median stays gated on compat level 110+. Mode 2 lands at 2 scans total instead of ~2 per numeric column. Empty-select guards skip either batch when no column qualifies.

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
