-- DDL for AVRO testing
-- default topic

DROP PROCEDURE test_topic if exists;
DROP STREAM TEST_TOPIC if exists;

CREATE STREAM TEST_TOPIC PARTITION ON COLUMN rowid EXPORT TO TOPIC TEST_TOPIC WITH KEY (rowid) (
  rowid                     BIGINT        NOT NULL
, type_timestamp            TIMESTAMP     DEFAULT NOW
, type_varchar32k           VARCHAR(32768)
);

CREATE PROCEDURE test_topic PARTITION ON TABLE TEST_TOPIC COLUMN rowid AS INSERT INTO TEST_TOPIC (rowid, type_varchar32k) VALUES (?, ?);

-- Additional topics (must have same schema)

DROP PROCEDURE test_topic01 if exists;
DROP STREAM TEST_TOPIC01 if exists;

CREATE STREAM TEST_TOPIC01 PARTITION ON COLUMN rowid EXPORT TO TOPIC TEST_TOPIC01 WITH KEY (rowid) (
  rowid                     BIGINT        NOT NULL
, type_timestamp            TIMESTAMP     DEFAULT NOW
, type_varchar32k           VARCHAR(32768)
);

CREATE PROCEDURE test_topic01  PARTITION ON TABLE TEST_TOPIC01 COLUMN rowid AS INSERT INTO TEST_TOPIC01 (rowid, type_varchar32k) VALUES (?, ?);
