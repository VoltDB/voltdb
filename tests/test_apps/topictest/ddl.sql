--
-- Stream with partition column in non-default position 2
--
DROP STREAM SOURCE01 if exists;
CREATE STREAM SOURCE01 PARTITION ON COLUMN c EXPORT TO TOPIC source01 (
  a   BIGINT        NOT NULL
, b   VARCHAR(256)  NOT NULL
, c   BIGINT        NOT NULL
, d   VARCHAR(256)  NOT NULL
);
DROP PROCEDURE procedure01 if exists;
CREATE PROCEDURE procedure01 PARTITION ON TABLE SOURCE01 COLUMN c PARAMETER 2 AS INSERT INTO SOURCE01 (a, b, c, d) VALUES (?, ?, ?, ?);

--
-- Stream with partition column in default position 0
--
DROP STREAM SOURCE02 if exists;
CREATE STREAM SOURCE02 PARTITION ON COLUMN a EXPORT TO TOPIC source02 (
  a   BIGINT        NOT NULL
, b   VARCHAR(256)  NOT NULL
, c   BIGINT        NOT NULL
, d   VARCHAR(256)  NOT NULL
);
DROP PROCEDURE procedure02 if exists;
CREATE PROCEDURE procedure02 PARTITION ON TABLE SOURCE02 COLUMN a AS INSERT INTO SOURCE02 (a, b, c, d) VALUES (?, ?, ?, ?);

--
-- Stream with unpartitioned procedure
--
DROP STREAM SOURCE03 if exists;
CREATE STREAM SOURCE03 PARTITION ON COLUMN a EXPORT TO TOPIC source03 (
  a   BIGINT        NOT NULL
, b   VARCHAR(256)  NOT NULL
, c   BIGINT        NOT NULL
, d   VARCHAR(256)  NOT NULL
);
DROP PROCEDURE procedure03 if exists;
CREATE PROCEDURE procedure03 AS INSERT INTO SOURCE03 (a, b, c, d) VALUES (?, ?, ?, ?);

--
-- Stream with incompatible procedure signature
--
DROP STREAM SOURCE04 if exists;
CREATE STREAM SOURCE04 PARTITION ON COLUMN a EXPORT TO TOPIC source04 (
  a   BIGINT        NOT NULL
, b   VARCHAR(256)  NOT NULL
, c   BIGINT        NOT NULL
, d   VARCHAR(256)  NOT NULL
, e   BIGINT        DEFAULT 0
);
DROP PROCEDURE procedure04 if exists;
CREATE PROCEDURE procedure04 AS INSERT INTO SOURCE04 (a, b, c, d, e) VALUES (?, ?, ?, ?, ?);

DROP PROCEDURE BadProcedure if exists;
LOAD CLASSES topictest-server.jar;
CREATE PROCEDURE FROM CLASS topictest.BadProcedure;
