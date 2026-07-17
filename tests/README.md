# sp_DataProfile test harness

Automated [tSQLt](https://tsqlt.org/) suite for `master.dbo.sp_DataProfile`. Design rationale
lives in [../docs/test-harness-design.md](../docs/test-harness-design.md); the per-test checklist in
[../docs/test-inventory.md](../docs/test-inventory.md).

## Prerequisites (one-time, per test instance)

Use a **dedicated test instance/container** — the harness installs the proc into `master` and creates
databases (machine-wide side effects).

1. **Server config** — `install/01_configure_server.sql` enables:
   - `clr enabled` (tSQLt and `tSQLt.ResultSetFilter` are CLR)
   - `Ad Hoc Distributed Queries` (needed **only** for the Mode 3 loopback capture)
2. **The proc** — open `../sp_DataProfile.sql` in SSMS and run it (F5). Installs into `master`.
3. **Fixtures** — `install/03_create_fixture_db.sql` creates and commits `DataProfileTest` and
   `DataProfileTest_Compat100`.
4. **tSQLt** — download from tsqlt.org and install into `DataProfileTest` (after step 3 created it).
   See `install/02_install_tsqlt.sql` for the exact steps; it also verifies `ResultSetFilter` exists.

## Running

SQLCMD mode is required (the entry point uses `:r` includes, whose paths are
relative to `tests/` — so run from there):

```
cd tests
sqlcmd -b -S (local) -i run_all.sql
```

or in SSMS: **Query ▸ SQLCMD Mode**, open `run_all.sql` (from `tests/`), F5. It rebuilds fixtures + helpers + test
classes, runs `tSQLt.RunAll`, and (with `sqlcmd -b`) exits non-zero if anything failed.

Run one class while iterating: `EXEC tSQLt.Run 'Smoke';`

**Run `Smoke` first.** Its `test CaptureProfile loopback helper returns a rowset` proves the Mode 3
loopback path (server name + `Ad Hoc Distributed Queries`). If it's red, fix the loopback before
trusting the rest — set the server in the `@LoopbackServer` default of
`install/04_capture_helper.sql` if `(local)` isn't right for your instance.

## How capture works (the one non-obvious part)

`sp_DataProfile` returns a metadata result set **then** the detail set. `tSQLtTest.CaptureProfile`
uses `tSQLt.ResultSetFilter` to grab the requested set (`@ResultSetNo`: 1 = metadata, 2 = detail).
Mode 3 runs its own `INSERT...EXEC` internally, which would nest under a plain
`INSERT...EXEC ResultSetFilter`; for that mode only, the helper reaches ResultSetFilter over a
loopback `OPENROWSET` so the proc runs top-level on a separate session. All value captures use
`@SampleValue = 100` (no `TABLESAMPLE`) for determinism.

## Conditional tests

- **Compat-gated (median):** asserted on both sides — `DataProfileTest` (host compat, ≥110) and the
  committed `DataProfileTest_Compat100`. No in-test `ALTER DATABASE` (not allowed inside tSQLt's
  transaction).
- **Server-version-gated (`APPROX_COUNT_DISTINCT`, the <2012 floor):** can't fake the version, so
  only the host's branch is asserted; the other side **skips with a message** (`tSQLtTest.Skip`).
- **StackOverflow smoke:** skips unless a `[StackOverflow]` DB with `Users`/`Posts` is attached.

## Layout

```
install/  01 server config · 02 tSQLt install (doc+verify) · 03 fixtures · 04 CaptureProfile · 05 helpers
unit/     Smoke, Mode0–Mode4, CrossDatabase, Quoting, Guards, Sampling, VersionMatrix, StackOverflowSmoke
legacy/   Checking Dates in Mode 3.sql  (the original ad-hoc script, kept for reference)
run_all.sql
```
