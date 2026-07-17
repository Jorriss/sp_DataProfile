# CLAUDE.md

Guidance for working in this repo. For *what the proc does and how to call it*, read [README.md](README.md) — don't duplicate it here.

## What this is

A single T-SQL stored procedure, [sp_DataProfile.sql](sp_DataProfile.sql) (~1,300 lines), that profiles a table in Microsoft SQL Server. The whole product is this one file. There is no build system, package manager, or app to run.

## Build / deploy

- The script `DROP`s and `CREATE`s `dbo.sp_DataProfile` **in `master`** so it resolves like a system proc from any database. Any edit must keep it installable into `master` and keep the leading `sp_` name.
- To install: open the file in SSMS, connect to an instance, execute (F5). There is no other deploy step.
- Keep the DROP-then-CREATE pattern at the top of the file intact.
- There is no linter or formatter configured — match the surrounding style by hand.

## Minimum SQL Server version (the compatibility floor)

- Must run on **SQL Server 2012+**. The proc checks `SERVERPROPERTY('ProductVersion')` and hard-errors (`@SQLMajorVersion < 11`) on older versions. Do **not** introduce syntax that raises this floor (e.g. anything requiring 2014+). Constructs available at the 2012 floor — such as the `VALUES` row constructor and `CROSS APPLY (VALUES ...)` — are fair game and are used by the Mode 1 single-pass reshape.
- Median (Mode 2) uses `PERCENTILE_DISC`, which needs **compatibility level 110+**. Compatibility level is per-database and independent of server version, so a 2012+ instance can still run a DB at a lower compat level — those must still run and just skip the median column with a warning. Preserve this graceful degradation.

## Conventions to match

This proc is heavy on dynamic SQL. When editing:

- Build dynamic SQL into `NVARCHAR(MAX)` variables (`@SQLString`, `@SQLStringFK`, `@SQLStringIndexes`) and execute with `sp_executesql` (use its parameterized/OUTPUT form where values flow back).
- **Always** wrap identifiers with `QUOTENAME` — schema, table, column, and `@DatabaseName`. Inputs like `@TableName` and `@ColumnList` come from the caller and must not be concatenated raw (injection surface).
- Cross-database references are prefixed with `QUOTENAME(@DatabaseName) + '.sys....'`. Follow that pattern for any new catalog-view query.
- Guard every dynamic batch with a null check that `RAISERROR`s `'@SQLString is null'` before executing, as the existing code does.
- Emit errors and progress with `RAISERROR` (`WITH NOWAIT` for progress). When `@Verbose = 1`, print the generated SQL and step messages — keep new steps verbose-aware.
- Work happens through temp tables: `#table_column_profile`, `#table_relationship`, `#table_indexes`.

## Architecture

- Header: parameter declarations and `SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED`.
- Preamble: version/compat checks, `@Mode` validation (`IF @Mode NOT IN (0,1,2,3,4)`), schema/table parsing, and `@SampleValue`/`TABLESAMPLE` setup.
- Shared metadata load into `#table_column_profile`, plus optional FK (`@ShowForeignKeys`) and index (`@ShowIndexes`) blocks that apply in any mode.
- Per-mode logic (0 Overview, 1 Column Detail, 2 Statistics, 3 Candidate Key, 4 Value Distribution) branches on `@Mode`. See the README mode table for behavior.

## Intentional trade-offs — don't "fix" these

- Runs under **READ UNCOMMITTED** on purpose (won't block writers; accepts dirty reads).
- `TABLESAMPLE` is **page-based**, so on small tables it can return all rows or none. This is expected sampling behavior, not a bug.
- Modes 1 and 2 **scan once per metric per column** by design; it's expensive on wide/large tables. Performance rework is tracked, not accidental.

## Testing

- **Automated suite: a [tSQLt](https://tsqlt.org/) harness under [tests/](tests/)** — see [tests/README.md](tests/README.md) for prereqs (CLR + tSQLt; `Ad Hoc Distributed Queries` for the Mode 3 loopback only) and [docs/test-harness-design.md](docs/test-harness-design.md) for the design. Fixtures are committed micro-tables in `DataProfileTest` (+ `DataProfileTest_Compat100`); expected values are hand-computed literals.
- **Run it in SSMS (the user's chosen verification path):** SQLCMD mode → open [tests/run_all.sql](tests/run_all.sql) → F5 (or `sqlcmd -b -S (local) -i tests/run_all.sql`). Run `EXEC tSQLt.Run 'Smoke';` first — it proves the capture plumbing (incl. the Mode 3 loopback) before the rest. After changing the proc, re-run the suite; add/adjust a test for the new behavior.
- Capture uses `tSQLtTest.CaptureProfile` (`tSQLt.ResultSetFilter`; loopback `OPENROWSET` for Mode 3). `@LoopbackServer` defaults to `(local)` + Windows auth — change it in [tests/install/04_capture_helper.sql](tests/install/04_capture_helper.sql) for a named instance / SQL login.
- `tests/legacy/` keeps the original ad-hoc scripts (e.g. `Checking Dates in Mode 3.sql`) for reference. The README examples target the **StackOverflow** sample DB (`Users`, `Posts`) — used by the tagged, skip-if-absent `StackOverflowSmoke` class.

## Related docs & workflow

- [docs/analysis.md](docs/analysis.md) — behavior deep-dive, known gaps, performance roadmap.
- [docs/fix-plan.md](docs/fix-plan.md) — planned fixes.
- Changes are tracked as GitHub issues; commit messages reference them (e.g. `Fixed #15`). Keep that convention.
