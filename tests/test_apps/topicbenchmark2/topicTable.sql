DROP TABLE TEST_TOPIC if exists;
DROP PROCEDURE TEST_TOPIC if exists;

CREATE STREAM TEST_TOPIC PARTITION ON COLUMN rowid AS TOPIC PROFILE topicbenchmark (
  rowid                     BIGINT        NOT NULL
, type_timestamp            TIMESTAMP     DEFAULT NOW
, type_varchar1024          VARCHAR(1024)
);

-- use small caps for procedure lookup from kafka consumer
CREATE PROCEDURE test_topic AS INSERT INTO TEST_TOPIC (
  rowid
, type_varchar1024
)
VALUES (
  ?
, ?
);
