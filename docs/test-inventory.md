# Test Inventory (Feature #15)

Concrete, buildable list of the tests the [test-harness-design.md](test-harness-design.md) taxonomy calls for. Each item is named as a tSQLt test so this doubles as a build checklist. Grouped by test class (one file per group under `tests/unit/`).

Roughly **37 tests across 12 files**. Fixtures referenced (`AllTypes`, `Nullable`, `Cardinality`, `Stats`, `Keys`, `Parent`/`Child`, `[Odd Names]`) are the micro-fixtures defined in the design doc's [Fixture strategy](test-harness-design.md#fixture-strategy).

> **Status: implemented.** The suite now lives under [`tests/`](../tests/) (`install/` + `unit/` + `run_all.sql`). The item numbers below are indicative; the exact, current test names are the `CREATE PROCEDURE <Class>.[test ...]` in each `tests/unit/*.sql`. A few items shifted during build — most notably the Guards class (see below) — so treat the test files as the source of truth.

## Smoke.sql
1. `test proc installs into master and is callable by sp_ name`
2. `test Mode 0 default runs without error against AllTypes`
3. `test Mode 0 returns the expected overview column set (shape)`
4. `test CaptureProfile loopback helper returns a rowset` — proves the Mode 3 capture path (loopback OPENROWSET → `tSQLt.ResultSetFilter` → the proc whose own `INSERT...EXEC` would otherwise nest)

## Mode0.sql (Overview)
5. `test Mode 0 overview row shape/values for AllTypes`
6. `test Mode 0 with @ShowForeignKeys=1 emits FK rows for Parent/Child`
7. `test Mode 0 with @ShowIndexes=1 emits index rows`
8. `test Mode 0 with both flags off omits FK and index result sets`

## Mode1.sql (Column Detail)
9. `test Mode 1 num_nulls per column against Nullable`
10. `test Mode 1 distinct count per column against Cardinality`
11. `test Mode 1 min/max length against AllTypes string columns`
12. `test Mode 1 handles each supported type in AllTypes without error`
13. `test Mode 1 @ColumnList restricts output to named columns`

## Mode2.sql (Statistics)
14. `test Mode 2 min/max against Stats`
15. `test Mode 2 mean against Stats`
16. `test Mode 2 stddev against Stats`
17. `test Mode 2 median at compat >= 110 against Stats` — odd row count → unambiguous `PERCENTILE_DISC`
18. `test Mode 2 median gracefully degrades below compat 110` — toggle `COMPATIBILITY_LEVEL=100`, assert warning/skipped column, reset in teardown

## Mode3.sql (Candidate Key)
19. `test Mode 3 flags Keys composite as a candidate key`
20. `test Mode 3 does not flag the non-key combo`
21. `test Mode 3 output captured via loopback` — explicit regression lock for the `INSERT ... EXEC` nesting workaround

## Mode4.sql (Value Distribution)
22. `test Mode 4 distinct values on a Cardinality column`
23. `test Mode 4 distribution counts on a Cardinality column`
24. `test Mode 4 @ApproxDistinct on SQL 2019+ is close to exact` — else skip-with-message; closeness, not equality

## CrossDatabase.sql
25. `test cross-DB call with @DatabaseName='DataProfileTest' equals in-context run` — guards the `QUOTENAME(@DatabaseName)` convention

## Quoting.sql
26. `test Mode 1 against [Odd Names] succeeds with correct per-column rows`
27. `test Mode 2 against [Odd Names] succeeds`
28. `test Mode 3 against [Odd Names] succeeds` — spaced name, reserved word `[Select]`, `[Order Date]`

## Guards.sql
The parameter guards `RAISERROR` at **severity 1** then `RETURN` — informational, *not* catchable — so `ExpectException` doesn't apply. The observable effect is an early return with no result set, asserted via `INSERT ... EXEC` into a dummy table + `AssertEmptyTable`.
29. `test invalid Mode not in 0-4 returns no result set`
30. `test Mode 3 without ColumnList returns no result set`
31. `test invalid SampleType returns no result set` / `test SampleValue out of range returns no result set`
32. `test SQLString is null guard is not reachable via parameters` — skip (internal defensive guard)
33. `test version floor does not fire on a supported host` — host-branch only (can't fake < 2012 down); the throwing branch is unreachable on a modern host

## Sampling.sql
32. `test @SampleValue run completes and returns a row count within [0, total]` — no exact-count assertion (constraint #4)

## VersionMatrix.sql
33. `test compat-gated branch asserted on both sides` — median present ≥110 / skipped <110 (the toggleable one)
34. `test server-version-gated branch matches host` — approx vs exact distinct; other branch skipped-with-log

## StackOverflowSmoke.sql (tagged, skips if DB absent)
35. `test Mode 0 against Posts runs and returns expected shape`
36. `test Mode 1 against Users runs without exception`
37. `test all modes 0-4 against a large table return without error` — shape/no-exception only

## Notes before writing

- **Host-conditional tests** use the shared helpers in `tests/install/05_test_helpers.sql`: `tSQLtTest.HostMajorVersion()`, `tSQLtTest.EffectiveCompatLevel()`, and `tSQLtTest.Skip` (delegates to `tSQLt.SkipTest` when present, else passes with a message). The median degradation side runs against the committed `DataProfileTest_Compat100` DB rather than toggling `COMPATIBILITY_LEVEL` in-test (`ALTER DATABASE` can't run inside tSQLt's transaction).
- **Fixture coverage gap (deliberate):** `AllTypes` `uniqueidentifier`/`bit`/date-time branches are only exercised by "runs without error" (test 12), not asserted by value. If per-type value coverage is wanted, tests 11–12 and 14–16 expand into per-type asserts.
- **Not a gap:** the `Parent`/`Child` orphan row seeds the future #10 orphan check but is only exercised by FK presence (test 6); no orphan assertion until that feature exists.
