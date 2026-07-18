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

/* 7. Run everything.
      tSQLt.RunAll raises a severity-16 error when any test fails; under sqlcmd -b
      that aborts the whole script right here, skipping the report/exit steps
      below. Swallow it in TRY/CATCH — step 7b lists the failures by name and
      step 8 sets the non-zero exit code. RunAll still prints its own summary. */
USE [DataProfileTest];
GO
BEGIN TRY
    EXEC tSQLt.RunAll;
END TRY
BEGIN CATCH
END CATCH;
GO

/* 7a. Per-test results — every test with a PASS/FAIL/SKIP marker, so the run is
       scannable at a glance. Leading spaces are required: sqlcmd strips a leading
       [bracketed] token from PRINT output, which would eat the marker. */
PRINT '';
PRINT '=== Test results ===';
DECLARE @resultline NVARCHAR(MAX);
DECLARE test_results CURSOR LOCAL FAST_FORWARD FOR
    SELECT '  ['
         + CASE WHEN Result = 'Success' THEN 'PASS'
                WHEN Result = 'Skipped' THEN 'SKIP'
                ELSE 'FAIL' END
         + '] ' + Class + '.' + TestCase
    FROM tSQLt.TestResult
    ORDER BY Class, TestCase;
OPEN test_results;
FETCH NEXT FROM test_results INTO @resultline;
WHILE @@FETCH_STATUS = 0
BEGIN
    PRINT @resultline;
    FETCH NEXT FROM test_results INTO @resultline;
END
CLOSE test_results;
DEALLOCATE test_results;
GO

/* 7b. List anything that didn't pass, by name — RunAll's console summary
       doesn't enumerate the failures. Reads tSQLt.TestResult, the same table
       step 8 uses for the exit code. */
IF EXISTS (SELECT 1 FROM tSQLt.TestResult WHERE Result NOT IN ('Success', 'Skipped'))
BEGIN
    PRINT '';
    PRINT '=== Tests that did not pass ===';
    DECLARE @line NVARCHAR(MAX);
    /* Leading marker (not '[') on purpose: sqlcmd strips a leading [bracketed]
       token from PRINT output, which would eat the class name. */
    DECLARE failed_tests CURSOR LOCAL FAST_FORWARD FOR
        SELECT '  ' + Result + ': ' + Name + CHAR(13) + CHAR(10) + '      ' + ISNULL(Msg, '')
        FROM tSQLt.TestResult
        WHERE Result NOT IN ('Success', 'Skipped')
        ORDER BY Result, Class, TestCase;
    OPEN failed_tests;
    FETCH NEXT FROM failed_tests INTO @line;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        PRINT @line;
        FETCH NEXT FROM failed_tests INTO @line;
    END
    CLOSE failed_tests;
    DEALLOCATE failed_tests;
END
GO

/* 7c. Summary line: total run, succeeded, failed. Skipped tests are counted
       separately so "run" reflects tests that actually executed. */
DECLARE @total    INT ,
        @succeeded INT ,
        @failed    INT ,
        @skipped   INT ,
        @summary   NVARCHAR(MAX);

SELECT @total     = COUNT(*) ,
       @succeeded = SUM(CASE WHEN Result = 'Success' THEN 1 ELSE 0 END) ,
       @skipped   = SUM(CASE WHEN Result = 'Skipped' THEN 1 ELSE 0 END) ,
       @failed    = SUM(CASE WHEN Result NOT IN ('Success', 'Skipped') THEN 1 ELSE 0 END)
FROM   tSQLt.TestResult;

SET @summary = '=== Test summary: '
    + CAST(@total - @skipped AS NVARCHAR(10)) + ' run, '
    + CAST(@succeeded AS NVARCHAR(10)) + ' succeeded, '
    + CAST(@failed AS NVARCHAR(10)) + ' failed'
    + CASE WHEN @skipped > 0
           THEN ' (' + CAST(@skipped AS NVARCHAR(10)) + ' skipped)'
           ELSE '' END
    + ' ===';

PRINT '';
PRINT @summary;
GO

/* 8. Non-zero exit (with sqlcmd -b) if anything failed. */
IF EXISTS (SELECT 1 FROM tSQLt.TestResult WHERE Result NOT IN ('Success', 'Skipped'))
    RAISERROR('tSQLt: one or more tests did not succeed. See the results above.', 16, 1);
GO
