-- import other voltdb instance impersonating kafka into connector-less streams

DROP PROCEDURE test_topic if exists;
DROP STREAM TEST_TOPIC if exists;

CREATE STREAM TEST_TOPIC PARTITION ON COLUMN rowid (
  rowid                     BIGINT        NOT NULL
, type_timestamp            TIMESTAMP     DEFAULT NOW
, type_varchar32k           VARCHAR(32768)
);

CREATE PROCEDURE test_topic PARTITION ON TABLE TEST_TOPIC COLUMN rowid AS INSERT INTO TEST_TOPIC (rowid, type_timestamp, type_varchar32k) VALUES (?, ?, ?);

-- Additional topics (must have same schema)

DROP PROCEDURE test_topic01 if exists;
DROP STREAM TEST_TOPIC01 if exists;

CREATE STREAM TEST_TOPIC01 PARTITION ON COLUMN rowid (
  rowid                     BIGINT        NOT NULL
, type_timestamp            TIMESTAMP     DEFAULT NOW
, type_varchar32k           VARCHAR(32768)
);

CREATE PROCEDURE test_topic01  PARTITION ON TABLE TEST_TOPIC01 COLUMN rowid AS INSERT INTO TEST_TOPIC01 (rowid, type_timestamp, type_varchar32k) VALUES (?, ?, ?);

DROP PROCEDURE test_topic02 if exists;
DROP STREAM TEST_TOPIC02 if exists;

CREATE STREAM TEST_TOPIC02 PARTITION ON COLUMN rowid (
  rowid                     BIGINT        NOT NULL
, type_timestamp            TIMESTAMP     DEFAULT NOW
, type_varchar32k           VARCHAR(32768)
);

CREATE PROCEDURE test_topic02  PARTITION ON TABLE TEST_TOPIC02 COLUMN rowid AS INSERT INTO TEST_TOPIC02 (rowid, type_timestamp, type_varchar32k) VALUES (?, ?, ?);

DROP PROCEDURE test_topic03 if exists;
DROP STREAM TEST_TOPIC03 if exists;

CREATE STREAM TEST_TOPIC03 PARTITION ON COLUMN rowid (
  rowid                     BIGINT        NOT NULL
, type_timestamp            TIMESTAMP     DEFAULT NOW
, type_varchar32k           VARCHAR(32768)
);

CREATE PROCEDURE test_topic03  PARTITION ON TABLE TEST_TOPIC03 COLUMN rowid AS INSERT INTO TEST_TOPIC03 (rowid, type_timestamp, type_varchar32k) VALUES (?, ?, ?);

DROP PROCEDURE test_topic04 if exists;
DROP STREAM TEST_TOPIC04 if exists;

CREATE STREAM TEST_TOPIC04 PARTITION ON COLUMN rowid (
  rowid                     BIGINT        NOT NULL
, type_timestamp            TIMESTAMP     DEFAULT NOW
, type_varchar32k           VARCHAR(32768)
);

CREATE PROCEDURE test_topic04  PARTITION ON TABLE TEST_TOPIC04 COLUMN rowid AS INSERT INTO TEST_TOPIC04 (rowid, type_timestamp, type_varchar32k) VALUES (?, ?, ?);

DROP PROCEDURE test_topic05 if exists;
DROP STREAM TEST_TOPIC05 if exists;

CREATE STREAM TEST_TOPIC05 PARTITION ON COLUMN rowid (
  rowid                     BIGINT        NOT NULL
, type_timestamp            TIMESTAMP     DEFAULT NOW
, type_varchar32k           VARCHAR(32768)
);

CREATE PROCEDURE test_topic05  PARTITION ON TABLE TEST_TOPIC05 COLUMN rowid AS INSERT INTO TEST_TOPIC05 (rowid, type_timestamp, type_varchar32k) VALUES (?, ?, ?);
