-- default topic

DROP TABLE TEST_TOPIC if exists;
DROP PROCEDURE TEST_TOPIC if exists;
CREATE STREAM TEST_TOPIC PARTITION ON COLUMN rowid AS TOPIC PROFILE topicbenchmark (
  rowid                     BIGINT        NOT NULL
, type_timestamp            TIMESTAMP     DEFAULT NOW
, type_varchar1024          VARCHAR(1024)
);
CREATE PROCEDURE test_topic AS INSERT INTO TEST_TOPIC (rowid, type_varchar1024) VALUES (?, ?);

-- Additional topics (must have same schema)

DROP TABLE TEST_TOPIC01 if exists;
DROP PROCEDURE TEST_TOPIC01 if exists;
CREATE STREAM TEST_TOPIC01 PARTITION ON COLUMN rowid AS TOPIC PROFILE topicbenchmark (
  rowid                     BIGINT        NOT NULL
, type_timestamp            TIMESTAMP     DEFAULT NOW
, type_varchar1024          VARCHAR(1024)
);
CREATE PROCEDURE test_topic01 AS INSERT INTO TEST_TOPIC01 (rowid, type_varchar1024) VALUES (?, ?);
