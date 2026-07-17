/*───────────────────────────────────────────────────────────────────────────
  run_all.sql  —  single entry point for the tSQLt harness

  Run in SQLCMD mode (the :r includes need it). The :r paths are relative to the
  tests\ folder, so run FROM tests\ :
      cd tests
      sqlcmd -b -S (local) -i run_all.sql
    or in SSMS: Query ▸ SQLCMD Mode, open this file (from tests\) and F5.

  Prereqs done ONCE by hand before the first run (see tests/README.md):
      • server config              → tests/install/01_configure_server.sql
      • sp_DataProfile installed    → run ..\sp_DataProfile.sql (F5)
      • tSQLt installed in DataProfileTest (03 creates the DB first, then install
        tSQLt per tests/install/02_install_tsqlt.sql)

  This script (idempotent) then: rebuilds fixtures + helpers + tests and runs all.
  Paths are relative to the tests\ folder — run from the repo root or open the
  file from tests\.
───────────────────────────────────────────────────────────────────────────*/
:setvar SQLCMDERRORLEVEL 1
SET NOCOUNT ON;
PRINT '=== sp_DataProfile tSQLt harness ===';

/* 1. Server config (CLR + Ad Hoc Distributed Queries). */
:r .\install\01_configure_server.sql

/* 2. Fixture databases (creates DataProfileTest + _Compat100, seeds & commits). */
:r .\install\03_create_fixture_db.sql

/* 3. Verify tSQLt is installed in DataProfileTest (fails loudly if not). */
:r .\install\02_install_tsqlt.sql

/* 4. Capture helper + shared test helpers. */
:r .\install\04_capture_helper.sql
:r .\install\05_test_helpers.sql

/* 5. Confirm the proc under test is installed. */
IF OBJECT_ID('master.dbo.sp_DataProfile') IS NULL
    RAISERROR('master.dbo.sp_DataProfile is not installed — run sp_DataProfile.sql first.', 16, 1);
GO

/* 6. (Re)create every test class. */
USE [DataProfileTest];
GO
:r .\unit\Smoke.sql
:r .\unit\Mode0.sql
:r .\unit\Mode1.sql
:r .\unit\Mode2.sql
:r .\unit\Mode3.sql
:r .\unit\Mode4.sql
:r .\unit\CrossDatabase.sql
:r .\unit\Quoting.sql
:r .\unit\Guards.sql
:r .\unit\Sampling.sql
:r .\unit\VersionMatrix.sql
:r .\unit\StackOverflowSmoke.sql

/* 7. Run everything. */
USE [DataProfileTest];
GO
EXEC tSQLt.RunAll;
GO

/* 8. Non-zero exit (with sqlcmd -b) if anything failed. */
IF EXISTS (SELECT 1 FROM tSQLt.TestResult WHERE Result NOT IN ('Success', 'Skipped'))
    RAISERROR('tSQLt: one or more tests did not succeed. See the results above.', 16, 1);
GO
