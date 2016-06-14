file -inlinebatch END_OF_BATCH

DROP PROCEDURE InsertDelete IF EXISTS;
DROP PROCEDURE InsertDeleteWithString IF EXISTS;
DROP TABLE tmp_0 IF EXISTS;
DROP TABLE tmp_1 IF EXISTS;
DROP TABLE tmp_2 IF EXISTS;
DROP TABLE tmp_3 IF EXISTS;
DROP TABLE tmp_4 IF EXISTS;
DROP TABLE tmp_5 IF EXISTS;
DROP TABLE tmp_6 IF EXISTS;
DROP TABLE tmp_7 IF EXISTS;
DROP TABLE tmp_8 IF EXISTS;
DROP TABLE tmp_9 IF EXISTS;
DROP TABLE tmp_s0 IF EXISTS;
DROP TABLE tmp_s1 IF EXISTS;
DROP TABLE tmp_s2 IF EXISTS;
DROP TABLE tmp_s3 IF EXISTS;
DROP TABLE tmp_s4 IF EXISTS;
DROP TABLE tmp_s5 IF EXISTS;
DROP TABLE tmp_s6 IF EXISTS;
DROP TABLE tmp_s7 IF EXISTS;
DROP TABLE tmp_s8 IF EXISTS;
DROP TABLE tmp_s9 IF EXISTS;

END_OF_BATCH

-- Update classes from jar so that server will know about classes but not procedures yet.
LOAD CLASSES procs.jar;

file -inlinebatch END_OF_BATCH

CREATE TABLE tmp_0 (
  appid            INTEGER      NOT NULL,
  deviceid         BIGINT       NOT NULL,
  ts               TIMESTAMP    DEFAULT NOW,
  PRIMARY KEY (deviceid, appid)
);
PARTITION TABLE tmp_0 ON COLUMN deviceid;

CREATE TABLE tmp_1 (
  appid            INTEGER      NOT NULL,
  deviceid         BIGINT       NOT NULL,
  ts               TIMESTAMP    DEFAULT NOW,
  PRIMARY KEY (deviceid, appid)
);
PARTITION TABLE tmp_1 ON COLUMN deviceid;

CREATE TABLE tmp_2 (
  appid            INTEGER      NOT NULL,
  deviceid         BIGINT       NOT NULL,
  ts               TIMESTAMP    DEFAULT NOW,
  PRIMARY KEY (deviceid, appid)
);
PARTITION TABLE tmp_2 ON COLUMN deviceid;

CREATE TABLE tmp_3 (
  appid            INTEGER      NOT NULL,
  deviceid         BIGINT       NOT NULL,
  ts               TIMESTAMP    DEFAULT NOW,
  PRIMARY KEY (deviceid, appid)
);
PARTITION TABLE tmp_3 ON COLUMN deviceid;

CREATE TABLE tmp_4 (
  appid            INTEGER      NOT NULL,
  deviceid         BIGINT       NOT NULL,
  ts               TIMESTAMP    DEFAULT NOW,
  PRIMARY KEY (deviceid, appid)
);
PARTITION TABLE tmp_4 ON COLUMN deviceid;

CREATE TABLE tmp_5 (
  appid            INTEGER      NOT NULL,
  deviceid         BIGINT       NOT NULL,
  ts               TIMESTAMP    DEFAULT NOW,
  PRIMARY KEY (deviceid, appid)
);
PARTITION TABLE tmp_5 ON COLUMN deviceid;

CREATE TABLE tmp_6 (
  appid            INTEGER      NOT NULL,
  deviceid         BIGINT       NOT NULL,
  ts               TIMESTAMP    DEFAULT NOW,
  PRIMARY KEY (deviceid, appid)
);
PARTITION TABLE tmp_6 ON COLUMN deviceid;

CREATE TABLE tmp_7 (
  appid            INTEGER      NOT NULL,
  deviceid         BIGINT       NOT NULL,
  ts               TIMESTAMP    DEFAULT NOW,
  PRIMARY KEY (deviceid, appid)
);
PARTITION TABLE tmp_7 ON COLUMN deviceid;

CREATE TABLE tmp_8 (
  appid            INTEGER      NOT NULL,
  deviceid         BIGINT       NOT NULL,
  ts               TIMESTAMP    DEFAULT NOW,
  PRIMARY KEY (deviceid, appid)
);
PARTITION TABLE tmp_8 ON COLUMN deviceid;

CREATE TABLE tmp_9 (
  appid            INTEGER      NOT NULL,
  deviceid         BIGINT       NOT NULL,
  ts               TIMESTAMP    DEFAULT NOW,
  PRIMARY KEY (deviceid, appid)
);
PARTITION TABLE tmp_9 ON COLUMN deviceid;

-- tables with VARCHAR 16
CREATE TABLE tmp_s0 (
  appid            INTEGER      NOT NULL,
  deviceid         BIGINT       NOT NULL,
  ts               TIMESTAMP    DEFAULT NOW,
  val              VARCHAR(150)  DEFAULT 'FOO',
  PRIMARY KEY (deviceid, appid)
);
PARTITION TABLE tmp_s0 ON COLUMN deviceid;

CREATE TABLE tmp_s1 (
  appid            INTEGER      NOT NULL,
  deviceid         BIGINT       NOT NULL,
  ts               TIMESTAMP    DEFAULT NOW,
  val              VARCHAR(150)  DEFAULT 'FOO',
  PRIMARY KEY (deviceid, appid)
);
PARTITION TABLE tmp_s1 ON COLUMN deviceid;

CREATE TABLE tmp_s2 (
  appid            INTEGER      NOT NULL,
  deviceid         BIGINT       NOT NULL,
  ts               TIMESTAMP    DEFAULT NOW,
  val              VARCHAR(150)  DEFAULT 'FOO',
  PRIMARY KEY (deviceid, appid)
);
PARTITION TABLE tmp_s2 ON COLUMN deviceid;

CREATE TABLE tmp_s3 (
  appid            INTEGER      NOT NULL,
  deviceid         BIGINT       NOT NULL,
  ts               TIMESTAMP    DEFAULT NOW,
  val              VARCHAR(150)  DEFAULT 'FOO',
  PRIMARY KEY (deviceid, appid)
);
PARTITION TABLE tmp_s3 ON COLUMN deviceid;

CREATE TABLE tmp_s4 (
  appid            INTEGER      NOT NULL,
  deviceid         BIGINT       NOT NULL,
  ts               TIMESTAMP    DEFAULT NOW,
  val              VARCHAR(150)  DEFAULT 'FOO',
  PRIMARY KEY (deviceid, appid)
);
PARTITION TABLE tmp_s4 ON COLUMN deviceid;

CREATE TABLE tmp_s5 (
  appid            INTEGER      NOT NULL,
  deviceid         BIGINT       NOT NULL,
  ts               TIMESTAMP    DEFAULT NOW,
  val              VARCHAR(150)  DEFAULT 'FOO',
  PRIMARY KEY (deviceid, appid)
);
PARTITION TABLE tmp_s5 ON COLUMN deviceid;

CREATE TABLE tmp_s6 (
  appid            INTEGER      NOT NULL,
  deviceid         BIGINT       NOT NULL,
  ts               TIMESTAMP    DEFAULT NOW,
  val              VARCHAR(150)  DEFAULT 'FOO',
  PRIMARY KEY (deviceid, appid)
);
PARTITION TABLE tmp_s6 ON COLUMN deviceid;

CREATE TABLE tmp_s7 (
  appid            INTEGER      NOT NULL,
  deviceid         BIGINT       NOT NULL,
  ts               TIMESTAMP    DEFAULT NOW,
  val              VARCHAR(150)  DEFAULT 'FOO',
  PRIMARY KEY (deviceid, appid)
);
PARTITION TABLE tmp_s7 ON COLUMN deviceid;

CREATE TABLE tmp_s8 (
  appid            INTEGER      NOT NULL,
  deviceid         BIGINT       NOT NULL,
  ts               TIMESTAMP    DEFAULT NOW,
  val              VARCHAR(150)  DEFAULT 'FOO',
  PRIMARY KEY (deviceid, appid)
);
PARTITION TABLE tmp_s8 ON COLUMN deviceid;

CREATE TABLE tmp_s9 (
  appid            INTEGER      NOT NULL,
  deviceid         BIGINT       NOT NULL,
  ts               TIMESTAMP    DEFAULT NOW,
  val              VARCHAR(150)  DEFAULT 'FOO',
  PRIMARY KEY (deviceid, appid)
);
PARTITION TABLE tmp_s9 ON COLUMN deviceid;

CREATE PROCEDURE PARTITION ON TABLE tmp_0 COLUMN deviceid PARAMETER 1 FROM CLASS procedures.InsertDelete;
CREATE PROCEDURE PARTITION ON TABLE tmp_s0 COLUMN deviceid PARAMETER 1 FROM CLASS procedures.InsertDeleteWithString;

END_OF_BATCH
