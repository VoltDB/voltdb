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
  ts               TIMESTAMP    DEFAULT NOW
);

CREATE TABLE tmp_1 (
  appid            INTEGER      NOT NULL,
  deviceid         BIGINT       NOT NULL,
  ts               TIMESTAMP    DEFAULT NOW
);

CREATE TABLE tmp_2 (
  appid            INTEGER      NOT NULL,
  deviceid         BIGINT       NOT NULL,
  ts               TIMESTAMP    DEFAULT NOW
);

CREATE TABLE tmp_3 (
  appid            INTEGER      NOT NULL,
  deviceid         BIGINT       NOT NULL,
  ts               TIMESTAMP    DEFAULT NOW
);

CREATE TABLE tmp_4 (
  appid            INTEGER      NOT NULL,
  deviceid         BIGINT       NOT NULL,
  ts               TIMESTAMP    DEFAULT NOW
);

CREATE TABLE tmp_5 (
  appid            INTEGER      NOT NULL,
  deviceid         BIGINT       NOT NULL,
  ts               TIMESTAMP    DEFAULT NOW
);

CREATE TABLE tmp_6 (
  appid            INTEGER      NOT NULL,
  deviceid         BIGINT       NOT NULL,
  ts               TIMESTAMP    DEFAULT NOW
);

CREATE TABLE tmp_7 (
  appid            INTEGER      NOT NULL,
  deviceid         BIGINT       NOT NULL,
  ts               TIMESTAMP    DEFAULT NOW
);

CREATE TABLE tmp_8 (
  appid            INTEGER      NOT NULL,
  deviceid         BIGINT       NOT NULL,
  ts               TIMESTAMP    DEFAULT NOW
);

CREATE TABLE tmp_9 (
  appid            INTEGER      NOT NULL,
  deviceid         BIGINT       NOT NULL,
  ts               TIMESTAMP    DEFAULT NOW
);

-- tables with VARCHAR 16
CREATE TABLE tmp_s0 (
  appid            INTEGER      NOT NULL,
  deviceid         BIGINT       NOT NULL,
  ts               TIMESTAMP    DEFAULT NOW,
  val              VARCHAR(150)  DEFAULT 'FOO'
);

CREATE TABLE tmp_s1 (
  appid            INTEGER      NOT NULL,
  deviceid         BIGINT       NOT NULL,
  ts               TIMESTAMP    DEFAULT NOW,
  val              VARCHAR(150)  DEFAULT 'FOO'
);

CREATE TABLE tmp_s2 (
  appid            INTEGER      NOT NULL,
  deviceid         BIGINT       NOT NULL,
  ts               TIMESTAMP    DEFAULT NOW,
  val              VARCHAR(150)  DEFAULT 'FOO'
);

CREATE TABLE tmp_s3 (
  appid            INTEGER      NOT NULL,
  deviceid         BIGINT       NOT NULL,
  ts               TIMESTAMP    DEFAULT NOW,
  val              VARCHAR(150)  DEFAULT 'FOO'
);

CREATE TABLE tmp_s4 (
  appid            INTEGER      NOT NULL,
  deviceid         BIGINT       NOT NULL,
  ts               TIMESTAMP    DEFAULT NOW,
  val              VARCHAR(150)  DEFAULT 'FOO'
);

CREATE TABLE tmp_s5 (
  appid            INTEGER      NOT NULL,
  deviceid         BIGINT       NOT NULL,
  ts               TIMESTAMP    DEFAULT NOW,
  val              VARCHAR(150)  DEFAULT 'FOO'
);

CREATE TABLE tmp_s6 (
  appid            INTEGER      NOT NULL,
  deviceid         BIGINT       NOT NULL,
  ts               TIMESTAMP    DEFAULT NOW,
  val              VARCHAR(150)  DEFAULT 'FOO'
);

CREATE TABLE tmp_s7 (
  appid            INTEGER      NOT NULL,
  deviceid         BIGINT       NOT NULL,
  ts               TIMESTAMP    DEFAULT NOW,
  val              VARCHAR(150)  DEFAULT 'FOO'
);

CREATE TABLE tmp_s8 (
  appid            INTEGER      NOT NULL,
  deviceid         BIGINT       NOT NULL,
  ts               TIMESTAMP    DEFAULT NOW,
  val              VARCHAR(150)  DEFAULT 'FOO'
);

CREATE TABLE tmp_s9 (
  appid            INTEGER      NOT NULL,
  deviceid         BIGINT       NOT NULL,
  ts               TIMESTAMP    DEFAULT NOW,
  val              VARCHAR(150)  DEFAULT 'FOO'
);

CREATE PROCEDURE FROM CLASS procedures.InsertDelete;
CREATE PROCEDURE FROM CLASS procedures.InsertDeleteWithString;

END_OF_BATCH
