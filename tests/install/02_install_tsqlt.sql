/*───────────────────────────────────────────────────────────────────────────
  02_install_tsqlt.sql  —  install tSQLt into the DataProfileTest database

  tSQLt ships as a binary assembly + T-SQL and is NOT vendored into this repo
  (licence + size). This script does not contain tSQLt; it documents the one-time
  install and verifies the result so run_all.sql fails loudly if it's missing.

  One-time install (do this by hand once, then re-runs of the harness are clean):

    1. Download tSQLt from https://tsqlt.org/downloads/  (unzip).
    2. Ensure 03_create_fixture_db.sql has already created [DataProfileTest]
       (it also sets TRUSTWORTHY ON, which tSQLt's CLR install requires).
    3. In SSMS, connect to the test instance, switch to DataProfileTest, and run
       the tSQLt.class.sql that ships in the download:
           USE [DataProfileTest];
           :r "C:\path\to\tSQLt\tSQLt.class.sql"      -- or open + F5
    4. Re-run this script to confirm.

  tSQLt.ResultSetFilter (used by tests/install/04_capture_helper.sql) is part of
  the standard tSQLt distribution — no extra add-on needed. (We deliberately do
  NOT depend on the unmerged tSQLt.ResultSetToTable from PR #19.)
───────────────────────────────────────────────────────────────────────────*/
SET NOCOUNT ON;

IF DB_ID('DataProfileTest') IS NULL
BEGIN
    RAISERROR('DataProfileTest does not exist yet. Run 03_create_fixture_db.sql first.', 16, 1);
    RETURN;
END

USE [DataProfileTest];
GO

/* Verify tSQLt is present and that ResultSetFilter (our capture dependency) exists. */
IF SCHEMA_ID('tSQLt') IS NULL
    RAISERROR('tSQLt is NOT installed in DataProfileTest. Follow the one-time install steps in this file.', 16, 1);
ELSE IF OBJECT_ID('tSQLt.ResultSetFilter') IS NULL
    RAISERROR('tSQLt is installed but tSQLt.ResultSetFilter is missing. Reinstall a current tSQLt build.', 16, 1);
ELSE
    RAISERROR('tSQLt present in DataProfileTest, ResultSetFilter available. OK.', 0, 1) WITH NOWAIT;

GO
