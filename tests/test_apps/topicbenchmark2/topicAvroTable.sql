-- DDL for AVRO testing
-- default topic

DROP TOPIC TEST_TOPIC if exists;
DROP PROCEDURE test_topic if exists;
DROP STREAM TEST_TOPIC if exists;

CREATE STREAM TEST_TOPIC PARTITION ON COLUMN rowid (
  rowid                     BIGINT        NOT NULL
, type_timestamp            TIMESTAMP     DEFAULT NOW
, type_varchar32k           VARCHAR(32768)
);

CREATE PROCEDURE test_topic PARTITION ON TABLE TEST_TOPIC COLUMN rowid AS INSERT INTO TEST_TOPIC (rowid, type_varchar32k) VALUES (?, ?);

-- inline encoding
CREATE TOPIC USING STREAM TEST_TOPIC EXECUTE PROCEDURE test_topic PROFILE topicbenchmark PROPERTIES(topic.store.encoded=true,consumer.format.values=avro,producer.format.values=avro,producer.parameters.includeKey=true,consumer.keys=rowid);


-- Additional topics (must have same schema)

DROP TOPIC TEST_TOPIC01 if exists;
DROP PROCEDURE test_topic01 if exists;
DROP STREAM TEST_TOPIC01 if exists;

CREATE STREAM TEST_TOPIC01 PARTITION ON COLUMN rowid (
  rowid                     BIGINT        NOT NULL
, type_timestamp            TIMESTAMP     DEFAULT NOW
, type_varchar32k           VARCHAR(32768)
);

CREATE PROCEDURE test_topic01  PARTITION ON TABLE TEST_TOPIC01 COLUMN rowid AS INSERT INTO TEST_TOPIC01 (rowid, type_varchar32k) VALUES (?, ?);

-- inline encoding
CREATE TOPIC USING STREAM TEST_TOPIC01 EXECUTE PROCEDURE test_topic01 PROFILE topicbenchmark PROPERTIES(topic.store.encoded=true,consumer.format.values=avro,producer.format.values=avro,producer.parameters.includeKey=true,consumer.keys=rowid);
