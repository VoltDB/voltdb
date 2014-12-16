-- reset if running again
DROP PROCEDURE CountDevice IF EXISTS;
DROP PROCEDURE GetCardEstForApp IF EXISTS;
DROP TABLE estimates IF EXISTS;

-- contestants table holds the contestants numbers (for voting) and names
CREATE TABLE estimates
(
  appid      bigint          NOT NULL,
  hll        varbinary(8192) NOT NULL,
  CONSTRAINT PK_estimates PRIMARY KEY (appid)
);
PARTITION TABLE estimates ON COLUMN appid;

-- Update classes from jar to that server will know about classes but not procedures yet.
exec @UpdateClasses uniquedevices-procs.jar '';

-- stored procedures
CREATE PROCEDURE FROM CLASS uniquedevices.CountDevice;
PARTITION PROCEDURE CountDevice ON TABLE estimates COLUMN appid;
CREATE PROCEDURE FROM CLASS uniquedevices.GetCardEstForApp;
PARTITION PROCEDURE GetCardEstForApp ON TABLE estimates COLUMN appid;
