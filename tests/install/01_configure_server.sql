/*───────────────────────────────────────────────────────────────────────────
  01_configure_server.sql  —  server-level prerequisites for the harness

  Idempotent. Run once per test instance (machine-wide side effect — fine for a
  dedicated test box/container, worth knowing on a shared dev instance).

  What it enables and why:
    - 'clr enabled'                 : tSQLt is a CLR assembly; and tSQLt.ResultSetFilter
                                      (our result-set capture) is a CLR proc.
    - 'Ad Hoc Distributed Queries'  : only needed for the Mode 3 capture path, which
                                      wraps ResultSetFilter in a loopback OPENROWSET to
                                      sidestep the proc's internal INSERT...EXEC
                                      (sp_DataProfile.sql:856). Harmless to leave on a
                                      test box; omit if you never run Mode3 tests.

  See tests/README.md for the full prerequisite list.
───────────────────────────────────────────────────────────────────────────*/
SET NOCOUNT ON;

IF (SELECT CAST(value_in_use AS INT) FROM sys.configurations WHERE name = 'clr enabled') <> 1
BEGIN
    RAISERROR('Enabling ''clr enabled''...', 0, 1) WITH NOWAIT;
    EXEC sp_configure 'show advanced options', 1;  RECONFIGURE;
    EXEC sp_configure 'clr enabled', 1;            RECONFIGURE;
END
ELSE
    RAISERROR('''clr enabled'' already on.', 0, 1) WITH NOWAIT;

IF (SELECT CAST(value_in_use AS INT) FROM sys.configurations WHERE name = 'Ad Hoc Distributed Queries') <> 1
BEGIN
    RAISERROR('Enabling ''Ad Hoc Distributed Queries'' (Mode 3 loopback capture)...', 0, 1) WITH NOWAIT;
    EXEC sp_configure 'show advanced options', 1;         RECONFIGURE;
    EXEC sp_configure 'Ad Hoc Distributed Queries', 1;    RECONFIGURE;
END
ELSE
    RAISERROR('''Ad Hoc Distributed Queries'' already on.', 0, 1) WITH NOWAIT;

GO
