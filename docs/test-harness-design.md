# Test Harness Design (Feature #15)

Deep-dive on roadmap item **#15 — Test harness** from [feature-roadmap.md](feature-roadmap.md#L122). That entry grades it **Importance: High · Difficulty: Medium** and notes it "makes every other change in this doc safe to land." This document turns that one paragraph into an actionable design: what to build, how to structure fixtures and assertions, and what to cover.

**Framework: [tSQLt](https://tsqlt.org/).** This is decided — the harness is pure tSQLt, no scripted golden-output fallback tier. The sections below assume that commitment.

For the manual verification steps this would replace, see the [Verification section of fix-plan.md](fix-plan.md#L60). This doc does not change proc behavior — it is purely about *how we prove the proc still works* after a change.

## Why this is the keystone

Every feature in the roadmap adds a new aggregate, mode, or catalog query to a ~1,300-line dynamic-SQL proc that already scans `SERVERPROPERTY`, compat-level, cross-DB, sampling, and odd-identifier paths. Today the only safety net is a human running examples against StackOverflow in SSMS and eyeballing the grid. That does not scale to the ~15 features queued, and it makes every refactor (the Phase 3 single-pass rewrite in [fix-plan.md](fix-plan.md)) a leap of faith.

An automated harness converts "did I break anything?" from a half-hour manual ritual into a single command with a pass/fail answer. That is the whole value: it is invisible to end users but it de-risks everything else.

## What makes this proc hard to test

The harness design has to absorb the same constraints the proc lives under (see [CLAUDE.md](../CLAUDE.md)):

1. **The product is `master.dbo.sp_DataProfile`.** Tests must install it (DROP/CREATE into `master`) and call it by its `sp_` name. The harness setup step is "run the one file." Note this is a **machine-wide side effect that lives outside tSQLt's per-test transaction rollback** — installing into `master` is a bootstrap step, never a test action, and it mutates shared server state (fine for a dedicated test instance/container, worth calling out for a shared dev box).
2. **Output is result sets, not return values.** The proc `SELECT`s from temp tables (`#table_column_profile`, `#table_relationship`, `#table_indexes`). To assert on output the harness must capture those result sets into a table, which in turn pins down an expected column shape per mode. Two wrinkles make this harder than `INSERT ... EXEC sp_DataProfile`:
   - **Every mode emits *two* result sets** — a 5-column metadata set **first** ([sp_DataProfile.sql:962](../sp_DataProfile.sql#L962), [1021](../sp_DataProfile.sql#L1021), [1090](../sp_DataProfile.sql#L1090), [1162](../sp_DataProfile.sql#L1162), [1224](../sp_DataProfile.sql#L1224)), then the detail set the value-assertions actually need. Raw `INSERT ... EXEC` concatenates all sets and fails on the shape mismatch; raw `OPENROWSET(...'EXEC...')` returns only the *first* (metadata) set. So neither reaches the detail set on its own. The harness uses **`tSQLt.ResultSetFilter`** to isolate the requested set (result set 1 or 2) — see [Result-set capture](#result-set-capture).
   - **Mode 3 runs `INSERT ... EXEC` internally** ([sp_DataProfile.sql:856](../sp_DataProfile.sql#L856); Mode 4's insert at [934](../sp_DataProfile.sql#L934) lives inside its dynamic string and does *not* nest). SQL Server forbids nesting `INSERT ... EXEC` (*"An INSERT EXEC statement cannot be nested"*), which breaks the plain `INSERT ... EXEC ResultSetFilter` path — **for Mode 3 only**. That one mode reaches `ResultSetFilter` over a **loopback `OPENROWSET`** so the proc runs top-level on a separate session.
3. **Behavior is version- and compat-adaptive.** Median (Mode 2) needs compat level 110+; `APPROX_COUNT_DISTINCT` needs SQL 2019+. Some assertions are therefore *conditional* on the host instance. Two different levers, with very different testability:
   - **Compat level is per-database and settable** — `ALTER DATABASE DataProfileTest SET COMPATIBILITY_LEVEL = 100` genuinely forces the below-110 branch on any modern host, so the median graceful-degradation path is **fully testable** by toggling compat around the test.
   - **Server major version cannot be faked** — there is no way to make `SERVERPROPERTY('ProductVersion')` report 2012 on a 2019 host. Version-gated branches (`APPROX_COUNT_DISTINCT`, the `< 11` hard-error floor) can only be asserted for the branch that *matches the host*; the other branch is skipped-with-a-logged-message, not exercised. The version matrix is therefore honest about which host it's running on, not a true cross-version matrix (that needs multiple SQL Server containers — a CI concern, noted later).
4. **Sampling is non-deterministic.** `TABLESAMPLE` is page-based and can return all rows or none on small tables. Any fixture used for exact-value assertions must be run **without** sampling; sampling itself is tested only for "runs without error / returns a plausible subset," not for exact counts.
5. **Dirty reads by design.** `READ UNCOMMITTED` is intentional; the harness shouldn't try to assert transactional isolation.

## Framework: tSQLt

[tSQLt](https://tsqlt.org/) is the de-facto T-SQL unit-test framework and the sole harness here. It gives us test classes (schemas), `tSQLt.AssertEquals*`, `tSQLt.AssertEqualsTable`, `tSQLt.ExpectException`, per-test transaction rollback (fixtures auto-clean), and `tSQLt.RunAll`.

- **Why it fits:** `AssertEqualsTable` against a captured result set (constraint #2) is the sweet spot; each test runs inside a transaction that rolls back, so fixtures never leak; output is parseable (JUnit XML via `tSQLt.XmlResultFormatter`) for CI later.
- **Prerequisites to document** (all one-time, in bootstrap): CLR enabled (`sp_configure 'clr enabled', 1`) plus the tSQLt assembly; `ALTER DATABASE ... SET TRUSTWORTHY ON` on the test DB per tSQLt's install; and **`Ad Hoc Distributed Queries` enabled** — needed **only** for the Mode 3 loopback capture below (`sp_configure 'Ad Hoc Distributed Queries', 1`).
- **Conditional skips** (constraint #3) are handled by checking `SERVERPROPERTY`/compat at the top of a test and returning early with a logged message, or by toggling `COMPATIBILITY_LEVEL` around a median test.

We deliberately do **not** keep a scripted golden-output tier. A single assertion style keeps the suite coherent, and golden snapshots re-baseline noisily on every legitimate format change. The cost is a hard CLR prerequisite — acceptable for a dedicated test instance/container.

### Result-set capture

Capture is owned by one helper, `tSQLtTest.CaptureProfile`, so tests pass parameters (table, mode, which result set) and never see the mechanism. It has to solve two problems from constraint #2 — isolating the second (detail) result set, and Mode 3's internal `INSERT ... EXEC`.

**Mechanism: `tSQLt.ResultSetFilter`.** This standard, CLR-based tSQLt proc executes a command that emits multiple result sets and returns just the requested one (`@ResultSetNo`: 1 = metadata, 2 = detail):

```sql
-- Modes 0, 1, 2, 4 — no internal INSERT...EXEC, so this runs on the test session:
INSERT INTO #actual (/* mode's detail-set shape */)
EXEC tSQLt.ResultSetFilter 2,
     N'EXEC master.dbo.sp_DataProfile @TableName = N''Stats'', @DatabaseName = N''DataProfileTest'', @Mode = 2, @SampleValue = 100';
```

**Mode 3 only — loopback.** Mode 3's own `INSERT ... EXEC` ([sp_DataProfile.sql:856](../sp_DataProfile.sql#L856)) would nest under the outer `INSERT ... EXEC ResultSetFilter`. So for that mode the helper reaches `ResultSetFilter` over a **loopback `OPENROWSET`**, running the proc top-level on a separate session:

```sql
INSERT INTO #actual (/* Mode 3 detail-set shape */)
SELECT * FROM OPENROWSET(
    'SQLNCLI', 'Server=(local);Trusted_Connection=yes;',
    'EXEC tSQLt.ResultSetFilter 2, ''EXEC master.dbo.sp_DataProfile @TableName=N''''Keys'''', @Mode=3, @ColumnList=N''''k1,v'''', @DatabaseName=N''''DataProfileTest'''', @SampleValue=100''');
```

Wrapping `ResultSetFilter` (not the proc directly) is what makes `OPENROWSET` surface result set 2 instead of the metadata set. Notes:

- **Rejected: `tSQLt.ResultSetToTable`** (PR #19) would capture a set from an `INSERT ... EXEC` proc without the loopback, but it is **unmerged and absent from released tSQLt**, so the harness does not depend on it.
- **`@SampleValue = 100`** (no `TABLESAMPLE`) is mandatory for every value-assertion capture, so results are deterministic (constraint #4).
- The loopback provider/connection string is environment-specific; it lives in one place — `CaptureProfile`'s `@LoopbackServer` default (`(local)`, Windows auth) — so tests pass parameters, not connection details.
- The Mode 3 loopback (and, defensively, `ResultSetFilter`'s CLR execution) opens outside the test's transaction and cannot see uncommitted fixture rows. Fixtures for value assertions must therefore be **committed** into `DataProfileTest` by the bootstrap, not created inside a rolled-back test — which is why fixtures live in a dedicated seeded DB (below) rather than in per-test temp tables.

## Fixture strategy

The README examples target StackOverflow, but a **multi-GB shared sample DB is the wrong fixture for assertions** — its values drift between snapshots and it's slow. Split the concern:

- **Deterministic micro-fixture DB (`DataProfileTest`)** — a tiny, self-contained database created by a setup script, holding purpose-built tables with *known*, **committed** contents (the loopback capture can't see uncommitted rows — see [Result-set capture](#result-set-capture)). This is what value-level assertions run against. Every expected number (null count, distinct count, min/max, median) is a hand-computed literal in the test, computable by hand from the seed data. Extend the existing [`tests/Checking Dates in Mode 3.sql`](../tests/Checking%20Dates%20in%20Mode%203.sql) fixture idea into a proper seeded schema.
- **StackOverflow (tagged)** — a handful of "runs without error against a real large DB" smoke tests (still tSQLt) that only assert *shape* and *no exception*, gated behind a flag so they're skipped when the sample DB isn't present.

### Micro-fixture contents (design the data to exercise the edges)

One table per concern, small enough to reason about:

| Fixture table | Exercises |
|---|---|
| `AllTypes` | one column per supported data type (int, decimal, bit, all date/time types, varchar, nvarchar, uniqueidentifier) — type-branching in every mode |
| `Nullable` | known mix of NULLs, `''`, whitespace, zeros, negatives — pins `num_nulls` and the future #1 soft-null counts |
| `Cardinality` | a constant column, a binary column, a unique column, a categorical column — pins distinct/unique_ratio and the future #2 classification |
| `Stats` | small numeric column with a hand-computed min/max/mean/stddev/median (odd row count so `PERCENTILE_DISC` is unambiguous) |
| `Keys` | composite candidate key + a non-key combo — Mode 3 |
| `Parent` / `Child` | a real FK + a deliberately orphaned child row — Mode 0 FK output and the future #10 orphan check |
| `[Odd Names]` | column named `[Order Date]`, a reserved word (`[Select]`), and a spaced/quoted name — the QUOTENAME regression |

Seed with `VALUES` row constructors (2012-floor-safe) so the fixture script itself respects the compatibility floor.

## Test taxonomy

Organized as tSQLt test classes (one per area). For the concrete, per-test buildable checklist derived from these areas, see [test-inventory.md](test-inventory.md).

1. **Install / smoke** — proc installs into `master`; `sp_DataProfile 'AllTypes'` (Mode 0 default) runs without error and returns the expected column set. This alone catches most "broke the dynamic SQL" regressions.
2. **Per-mode value assertions** (the core) — for Modes 0–4, capture the result set via the loopback `OPENROWSET` helper and `AssertEqualsTable` against expected rows (hand-computed literals) built from the micro-fixture:
   - Mode 0 overview shape + FK/index rows when `@ShowForeignKeys=1` / `@ShowIndexes=1`, and the unified constraint set (PK/default/check/computed) when `@ShowConstraints=1` — asserted against the `Constrained` fixture, plus the result-set-ordering lock (FK=3, index=4, constraints=5).
   - Mode 1 column detail: `num_nulls`, distinct count, min/max length per `AllTypes`/`Nullable` column.
   - Mode 2 statistics: min/max/mean/stddev against `Stats`; **median as a conditional test** — assert the value at compat ≥110 against `DataProfileTest`, and assert the graceful-degradation (median column dropped) against the committed **`DataProfileTest_Compat100`** fixture DB. The degradation side uses a second committed DB, *not* an in-test `ALTER DATABASE ... SET COMPATIBILITY_LEVEL` — that statement isn't allowed inside tSQLt's per-test transaction, and the proc gates median on `MIN(master compat, target-DB compat)` ([sp_DataProfile.sql:119](../sp_DataProfile.sql#L119)), so pointing `@DatabaseName` at a compat-100 DB forces the branch cleanly.
   - Mode 3 candidate key: `Keys` composite is flagged unique, the non-key combo isn't; cross-DB via `@DatabaseName`.
   - Mode 4 value distribution: distinct + distribution on a `Cardinality` column; `@ApproxDistinct=1` path asserted only on SQL 2019+ (else skip), and only for closeness, not exact equality.
3. **Cross-database** — install the fixture in `DataProfileTest`, call the proc from a *different* current database passing `@DatabaseName='DataProfileTest'`; assert identical output to the in-context run. Guards the cross-DB `QUOTENAME(@DatabaseName)` convention.
4. **Identifier quoting** — run Modes 1–3 against `[Odd Names]`; assert success and correct per-column rows. Regression lock for the QUOTENAME rules in [CLAUDE.md](../CLAUDE.md).
5. **Error / guard paths** — a subtlety learned in build: the parameter guards (`@Mode` not in 0–4, missing `@ColumnList` for Modes 3/4, bad `@SampleType`/`@SampleValue`) `RAISERROR` at **severity 1** then `RETURN` ([sp_DataProfile.sql:139](../sp_DataProfile.sql#L139)–[160](../sp_DataProfile.sql#L160)). Severity ≤10 is informational and **not** catchable, so `tSQLt.ExpectException` never fires. The observable effect is an **early return with no result set**, asserted via `INSERT ... EXEC` into a dummy table + `AssertEmptyTable` (0 result sets → 0 rows; the guards return before any mode body, so no nesting). The genuinely-throwing guards are out of reach here: the `< 11` version floor (severity 16) can't be provoked on a modern host, and `@SQLString is null` is an internal defensive check not reachable through public parameters — both are handled by skip / host-branch assertions rather than `ExpectException`.
6. **Sampling** — `@SampleValue` / `TABLESAMPLE` run completes and returns a row count within `[0, total]`; **no exact-count assertion** (constraint #4). Documents that sampling is exercised but intentionally non-deterministic.
7. **Version-adaptive matrix** — small tests that read `SERVERPROPERTY('ProductMajorVersion')` / compat level and assert the *right branch was taken*. Split by testability (constraint #3): compat-gated behavior (median) is asserted on *both* branches via the committed `DataProfileTest` (≥110) and `DataProfileTest_Compat100` fixtures; server-version-gated behavior (`APPROX_COUNT_DISTINCT`, the `< 11` floor) is asserted only for the host's branch, with the other branch skipped-with-a-message (`tSQLtTest.Skip`, which uses `tSQLt.SkipTest` when the installed tSQLt has it). A true cross-version run needs multiple SQL Server containers (CI concern below).

## Repo layout

```
tests/
  README.md                     # how to run (prereqs: CLR + tSQLt; Ad Hoc Distributed Queries for Mode 3)
  install/
    01_configure_server.sql     # sp_configure: clr enabled + Ad Hoc Distributed Queries — idempotent
    02_install_tsqlt.sql        # documents + verifies the tSQLt install into DataProfileTest
    03_create_fixture_db.sql    # CREATE DATABASE DataProfileTest (+ _Compat100) + seed & COMMIT micro-fixtures
    04_capture_helper.sql       # tSQLtTest.CaptureProfile — ResultSetFilter; loopback OPENROWSET for Mode 3
    05_test_helpers.sql         # tSQLtTest.HostMajorVersion / EffectiveCompatLevel / Skip
  unit/                         # tSQLt test classes, one file per area above
    Smoke.sql
    Mode0.sql ... Mode4.sql
    CrossDatabase.sql
    Quoting.sql
    Guards.sql
    Sampling.sql
    VersionMatrix.sql
    StackOverflowSmoke.sql       # tagged large-DB shape/no-exception tests; skipped if DB absent
  legacy/
    Checking Dates in Mode 3.sql  # the existing ad-hoc script, kept for reference
  run_all.sql                   # rebuilds fixtures+helpers+tests, tSQLt.RunAll, non-zero exit on failure
```

`run_all.sql` is the single entry point, run in **SQLCMD mode** (it uses `:r` includes): `sqlcmd -b -S (local) -i tests/run_all.sql` (or SSMS ▸ SQLCMD Mode ▸ F5) → configure server → build fixture DBs → verify tSQLt → load helpers + test classes → `tSQLt.RunAll` → non-zero exit on failure. Installing tSQLt and the proc are documented one-time prereqs (see [tests/README.md](../tests/README.md)).

## CI considerations (optional, later)

Once the suite exists locally, wiring it into GitHub Actions is a follow-up: spin up a `mcr.microsoft.com/mssql/server` container, `sqlcmd` in the install + `run_all` scripts, emit tSQLt's JUnit XML for the run summary. Deferred — the immediate goal is a *runnable local suite*, not a pipeline. Note it here so the layout (single entry point, machine-parseable output) doesn't have to change when CI lands.

## Build order for the harness itself

1. **Bootstrap + fixtures + smoke** — server config, tSQLt install, `DataProfileTest` micro-fixtures, the `CaptureProfile` loopback helper, and the Mode 0 smoke test. This alone gives a green/red signal and proves the loopback capture pattern end-to-end (including the Mode 3 nesting workaround).
2. **Per-mode value assertions** — Modes 0–4 against the fixtures; get exact-value coverage for today's behavior *before* adding roadmap features.
3. **Cross-DB, quoting, guards** — lock the convention-level invariants.
4. **Version matrix + sampling** — the conditional/non-deterministic tier.
5. **CI** — the GitHub Actions workflow, when automation is wanted.

Steps 1–2 are the ones worth doing *before* the next roadmap feature lands; the rest can trail.

## Decisions (resolved)

- **Framework:** pure tSQLt, no golden-output tier. CLR + tSQLt + `Ad Hoc Distributed Queries` are accepted dev/CI prerequisites.
- **Result-set capture:** `tSQLt.ResultSetFilter` isolates the requested result set (1 = metadata, 2 = detail). Loopback `OPENROWSET` is used **for Mode 3 only**, whose internal `INSERT ... EXEC` breaks the plain `INSERT ... EXEC ResultSetFilter` path. `tSQLt.ResultSetToTable` (PR #19) rejected as unmerged/unreleased.
- **Fixture home:** dedicated, seeded, committed `DataProfileTest` DB.
- **Expected-value source of truth:** hand-computed literals in each test.
- **tSQLt install trust model:** `TRUSTWORTHY ON` on the test DB (simplest path; the bootstrap sets it).
- **StackOverflow smoke tier:** included — a tagged class of "runs without error against a real large DB" tests that assert shape + no exception only, gated behind a flag so they skip cleanly when the DB isn't attached.

## Remaining open questions

- **Loopback connection string (resolved for local):** `CaptureProfile` defaults to `Server=(local);Trusted_Connection=yes;` (Windows auth) via its `@LoopbackServer` parameter — the one place that knows the server name. A named instance or SQL-login environment (e.g. a CI container) overrides `@LoopbackServer` / the connection string there; wiring that to an env var / setup variable is deferred to the CI step below. Note this connection string is *only* exercised by Mode 3 (all other modes capture via `ResultSetFilter` on the test session).
