
DROP TABLE dataprofile_test;

CREATE TABLE dataprofile_test (
  dt_date DATE NULL ,
  dt_time TIME NULL ,
  dt_2 DATETIME2 NULL ,
  dt_offset DATETIMEOFFSET  NULL,
  dt_small SMALLDATETIME  NULL,
  dt DATETIME NULL,
  dt_timezero TIME(0) NULL
)

INSERT INTO dataprofile_test
        ( dt_date ,
          dt_time ,
          dt_2 ,
          dt_offset ,
          dt_small ,
          dt,
          dt_timezero
        )
VALUES  ( '2014-12-31', -- dt_col - date
          '09:01:02.123' , -- dt_time - time
          '2015-01-02T00:01:02.123456' , -- dt_2 - datetime2
          '2007-05-08 12:35:29.1234567+12:15' , -- dt_offset - datetimeoffset
          '1955-12-13 12:43:00' , -- dt_small - smalldatetime
          '1988-05-05T03:23:49.123',  -- dt - datetime
          '16:46:37'
        )
GO 3

SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;
GO
sp_dataprofile 'dataprofile_test', 3, 'dt_date, dt_time, dt_2, dt_offset, dt_small, dt, dt_timezero'

SELECT * FROM [dbo].[dataprofile_test] WHERE dt_col = '2014-12-31' AND dt_time = '09:01:02.1230000' AND dt_2 = '2015-01-02 00:01:02.1234560' AND dt_offset = '2007-05-08 12:35:29.1234567 +12:15' AND dt_small = 'Dec 13 1955 12:43PM' AND dt = '1988-05-05T03:23:49.123'

SELECT * FROM [dbo].[dataprofile_test] WHERE dt_col = '2014-12-31' AND dt_time = '09:01:02.1230000' AND dt_2 = '2015-01-02T00:01:02.1234560' AND dt_offset = '2007-05-08T00:20:29.1234567Z' AND dt_small = '1955-12-13T12:43:00' AND dt = '1988-05-05T03:23:49.123'

SELECT * FROM [dbo].[dataprofile_test] WHERE dt_date = '2014-12-31' AND dt_time = '09:01:02.1230000' AND dt_2 = '2015-01-02T00:01:02.1234560' AND dt_offset = '2007-05-08T00:20:29.1234567Z' AND dt_small = '1955-12-13T12:43:00' AND dt = '1988-05-05T03:23:49.123' AND dt_timezero = '00:00:00'

SELECT * FROM dataprofile_test dt

SELECT * FROM [dbo].[dataprofile_test] WHERE dt_date = '2014-12-31' AND dt_time = '09:01:02.1230000' AND dt_2 = '2015-01-02T00:01:02.1234560' AND dt_offset = '2007-05-08T00:20:29.1234567Z' AND dt_small = '1955-12-13T12:43:00' AND dt = '1988-05-05T03:23:49.123' AND dt_timezero = '16:46:37'
