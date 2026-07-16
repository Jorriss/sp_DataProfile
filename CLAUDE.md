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

- Must run on **SQL Server 2005+**. The proc checks `SERVERPROPERTY('ProductVersion')` and hard-errors on older versions. Do **not** introduce syntax that raises this floor.
- Median (Mode 2) uses `PERCENTILE_DISC`, which needs **compatibility level 110+** (SQL Server 2012+). Lower compat levels must still run — they just skip the median column with a warning. Preserve this graceful degradation.

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

- `tests/` holds ad-hoc verification scripts (e.g. `Checking Dates in Mode 3.sql`), run manually in SSMS against a real instance.
- The README examples target the **StackOverflow** sample database (`Users`, `Posts`). Use it for manual verification.
- There is no automated test runner. **Ask the user how they want a change verified** rather than assuming — capture the answer here once it's known.

## Related docs & workflow

- [docs/analysis.md](docs/analysis.md) — behavior deep-dive, known gaps, performance roadmap.
- [docs/fix-plan.md](docs/fix-plan.md) — planned fixes.
- Changes are tracked as GitHub issues; commit messages reference them (e.g. `Fixed #15`). Keep that convention.
