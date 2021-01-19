-- default topic

DROP TOPIC TEST_TOPIC if exists;
DROP PROCEDURE test_topic if exists;
DROP STREAM TEST_TOPIC if exists;

CREATE STREAM TEST_TOPIC PARTITION ON COLUMN rowid (
  rowid                     BIGINT        NOT NULL
, type_timestamp            TIMESTAMP     DEFAULT NOW
, type_varchar1024          VARCHAR(1024)
);

CREATE PROCEDURE test_topic AS INSERT INTO TEST_TOPIC (rowid, type_varchar1024) VALUES (?, ?);

CREATE TOPIC USING STREAM TEST_TOPIC EXECUTE PROCEDURE test_topic PROFILE topicbenchmark PROPERTIES(consumer.format.values=csv,producer.format.values=csv,producer.parameters.includeKey=true,consumer.keys=rowid);


-- Additional topics (must have same schema)

DROP TOPIC TEST_TOPIC01 if exists;
DROP PROCEDURE test_topic01 if exists;
DROP STREAM TEST_TOPIC01 if exists;

CREATE STREAM TEST_TOPIC01 PARTITION ON COLUMN rowid (
  rowid                     BIGINT        NOT NULL
, type_timestamp            TIMESTAMP     DEFAULT NOW
, type_varchar1024          VARCHAR(1024)
);

CREATE PROCEDURE test_topic01 AS INSERT INTO TEST_TOPIC01 (rowid, type_varchar1024) VALUES (?, ?);

CREATE TOPIC USING STREAM TEST_TOPIC01 EXECUTE PROCEDURE test_topic01 PROFILE topicbenchmark PROPERTIES(consumer.format.values=csv,producer.format.values=csv,producer.parameters.includeKey=true,consumer.keys=rowid);
