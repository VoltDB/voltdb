-- reset if running again
DROP PROCEDURE CountDeviceEstimate IF EXISTS;
DROP PROCEDURE CountDeviceExact IF EXISTS;
DROP PROCEDURE CountDeviceHybrid IF EXISTS;
DROP PROCEDURE GetCardEstForApp IF EXISTS;
DROP PROCEDURE TopApps IF EXISTS;
DROP TABLE estimates IF EXISTS;
DROP TABLE exact IF EXISTS;

-- holds current estimate as integral value and HLL bytes
CREATE TABLE estimates
(
  appid       bigint          NOT NULL,
  devicecount bigint          NOT NULL,
  hll         varbinary(8192) DEFAULT NULL,
  CONSTRAINT  PK_estimates    PRIMARY KEY (appid)
);
PARTITION TABLE estimates ON COLUMN appid;
CREATE INDEX rank ON ESTIMATES (devicecount);

-- used for CountDeviceExact and CountDeviceExact procs
CREATE TABLE exact
(
  appid      bigint          NOT NULL,
  deviceid   bigint          NOT NULL,
  CONSTRAINT PK_exact PRIMARY KEY (appid, deviceid)
);
PARTITION TABLE exact ON COLUMN appid;

-- load classes from jar to that server will know about classes but not procedures yet.
LOAD CLASSES uniquedevices-procs.jar;

-- stored procedures

CREATE PROCEDURE PARTITION ON TABLE estimates COLUMN appid
  FROM CLASS uniquedevices.CountDeviceEstimate;

CREATE PROCEDURE PARTITION ON TABLE estimates COLUMN appid
  FROM CLASS uniquedevices.CountDeviceExact;

CREATE PROCEDURE PARTITION ON TABLE estimates COLUMN appid
  FROM CLASS uniquedevices.CountDeviceHybrid;

CREATE PROCEDURE GetCardEstForApp
    PARTITION ON TABLE estimates COLUMN appid
    AS SELECT devicecount FROM estimates WHERE appid = ?;
CREATE PROCEDURE TopApps
    AS SELECT appid, devicecount FROM estimates ORDER BY devicecount DESC LIMIT 10;
