
CREATE STREAM ALL_VALUES PARTITION ON COLUMN rowid EXPORT TO TARGET default (
  txnid                     BIGINT        NOT NULL
, rowid                     BIGINT        NOT NULL
, rowid_group               TINYINT       NOT NULL
, type_null_tinyint         TINYINT
, type_not_null_tinyint     TINYINT       NOT NULL
, type_null_smallint        SMALLINT
, type_not_null_smallint    SMALLINT      NOT NULL
, type_null_integer         INTEGER
, type_not_null_integer     INTEGER       NOT NULL
, type_null_bigint          BIGINT
, type_not_null_bigint      BIGINT        NOT NULL
, type_null_timestamp       TIMESTAMP
, type_not_null_timestamp   TIMESTAMP     NOT NULL
, type_null_float           FLOAT
, type_not_null_float       FLOAT         NOT NULL
, type_null_decimal         DECIMAL
, type_not_null_decimal     DECIMAL       NOT NULL
, type_null_varchar25       VARCHAR(32)
, type_not_null_varchar25   VARCHAR(32)   NOT NULL
, type_null_varchar128      VARCHAR(128)
, type_not_null_varchar128  VARCHAR(128)  NOT NULL
, type_null_varchar1024     VARCHAR(1024)
, type_not_null_varchar1024 VARCHAR(1024) NOT NULL
);

CREATE STREAM ALL_VALUES1 PARTITION ON COLUMN rowid EXPORT TO TARGET default1 (
  txnid                     BIGINT        NOT NULL
, rowid                     BIGINT        NOT NULL
, rowid_group               TINYINT       NOT NULL
, type_null_tinyint         TINYINT
, type_not_null_tinyint     TINYINT       NOT NULL
, type_null_smallint        SMALLINT
, type_not_null_smallint    SMALLINT      NOT NULL
, type_null_integer         INTEGER
, type_not_null_integer     INTEGER       NOT NULL
, type_null_bigint          BIGINT
, type_not_null_bigint      BIGINT        NOT NULL
, type_null_timestamp       TIMESTAMP
, type_not_null_timestamp   TIMESTAMP     NOT NULL
, type_null_float           FLOAT
, type_not_null_float       FLOAT         NOT NULL
, type_null_decimal         DECIMAL
, type_not_null_decimal     DECIMAL       NOT NULL
, type_null_varchar25       VARCHAR(32)
, type_not_null_varchar25   VARCHAR(32)   NOT NULL
, type_null_varchar128      VARCHAR(128)
, type_not_null_varchar128  VARCHAR(128)  NOT NULL
, type_null_varchar1024     VARCHAR(1024)
, type_not_null_varchar1024 VARCHAR(1024) NOT NULL
);

CREATE STREAM ALL_VALUES2 PARTITION ON COLUMN rowid EXPORT TO TARGET default2 (
  txnid                     BIGINT        NOT NULL
, rowid                     BIGINT        NOT NULL
, rowid_group               TINYINT       NOT NULL
, type_null_tinyint         TINYINT
, type_not_null_tinyint     TINYINT       NOT NULL
, type_null_smallint        SMALLINT
, type_not_null_smallint    SMALLINT      NOT NULL
, type_null_integer         INTEGER
, type_not_null_integer     INTEGER       NOT NULL
, type_null_bigint          BIGINT
, type_not_null_bigint      BIGINT        NOT NULL
, type_null_timestamp       TIMESTAMP
, type_not_null_timestamp   TIMESTAMP     NOT NULL
, type_null_float           FLOAT
, type_not_null_float       FLOAT         NOT NULL
, type_null_decimal         DECIMAL
, type_not_null_decimal     DECIMAL       NOT NULL
, type_null_varchar25       VARCHAR(32)
, type_not_null_varchar25   VARCHAR(32)   NOT NULL
, type_null_varchar128      VARCHAR(128)
, type_not_null_varchar128  VARCHAR(128)  NOT NULL
, type_null_varchar1024     VARCHAR(1024)
, type_not_null_varchar1024 VARCHAR(1024) NOT NULL
);

CREATE STREAM ALL_VALUES3 PARTITION ON COLUMN rowid EXPORT TO TARGET default3 (
  txnid                     BIGINT        NOT NULL
, rowid                     BIGINT        NOT NULL
, rowid_group               TINYINT       NOT NULL
, type_null_tinyint         TINYINT
, type_not_null_tinyint     TINYINT       NOT NULL
, type_null_smallint        SMALLINT
, type_not_null_smallint    SMALLINT      NOT NULL
, type_null_integer         INTEGER
, type_not_null_integer     INTEGER       NOT NULL
, type_null_bigint          BIGINT
, type_not_null_bigint      BIGINT        NOT NULL
, type_null_timestamp       TIMESTAMP
, type_not_null_timestamp   TIMESTAMP     NOT NULL
, type_null_float           FLOAT
, type_not_null_float       FLOAT         NOT NULL
, type_null_decimal         DECIMAL
, type_not_null_decimal     DECIMAL       NOT NULL
, type_null_varchar25       VARCHAR(32)
, type_not_null_varchar25   VARCHAR(32)   NOT NULL
, type_null_varchar128      VARCHAR(128)
, type_not_null_varchar128  VARCHAR(128)  NOT NULL
, type_null_varchar1024     VARCHAR(1024)
, type_not_null_varchar1024 VARCHAR(1024) NOT NULL
);

CREATE STREAM ALL_VALUES4 PARTITION ON COLUMN rowid EXPORT TO TARGET default4 (
  txnid                     BIGINT        NOT NULL
, rowid                     BIGINT        NOT NULL
, rowid_group               TINYINT       NOT NULL
, type_null_tinyint         TINYINT
, type_not_null_tinyint     TINYINT       NOT NULL
, type_null_smallint        SMALLINT
, type_not_null_smallint    SMALLINT      NOT NULL
, type_null_integer         INTEGER
, type_not_null_integer     INTEGER       NOT NULL
, type_null_bigint          BIGINT
, type_not_null_bigint      BIGINT        NOT NULL
, type_null_timestamp       TIMESTAMP
, type_not_null_timestamp   TIMESTAMP     NOT NULL
, type_null_float           FLOAT
, type_not_null_float       FLOAT         NOT NULL
, type_null_decimal         DECIMAL
, type_not_null_decimal     DECIMAL       NOT NULL
, type_null_varchar25       VARCHAR(32)
, type_not_null_varchar25   VARCHAR(32)   NOT NULL
, type_null_varchar128      VARCHAR(128)
, type_not_null_varchar128  VARCHAR(128)  NOT NULL
, type_null_varchar1024     VARCHAR(1024)
, type_not_null_varchar1024 VARCHAR(1024) NOT NULL
);

CREATE STREAM ALL_VALUES5 PARTITION ON COLUMN rowid EXPORT TO TARGET default5 (
  txnid                     BIGINT        NOT NULL
, rowid                     BIGINT        NOT NULL
, rowid_group               TINYINT       NOT NULL
, type_null_tinyint         TINYINT
, type_not_null_tinyint     TINYINT       NOT NULL
, type_null_smallint        SMALLINT
, type_not_null_smallint    SMALLINT      NOT NULL
, type_null_integer         INTEGER
, type_not_null_integer     INTEGER       NOT NULL
, type_null_bigint          BIGINT
, type_not_null_bigint      BIGINT        NOT NULL
, type_null_timestamp       TIMESTAMP
, type_not_null_timestamp   TIMESTAMP     NOT NULL
, type_null_float           FLOAT
, type_not_null_float       FLOAT         NOT NULL
, type_null_decimal         DECIMAL
, type_not_null_decimal     DECIMAL       NOT NULL
, type_null_varchar25       VARCHAR(32)
, type_not_null_varchar25   VARCHAR(32)   NOT NULL
, type_null_varchar128      VARCHAR(128)
, type_not_null_varchar128  VARCHAR(128)  NOT NULL
, type_null_varchar1024     VARCHAR(1024)
, type_not_null_varchar1024 VARCHAR(1024) NOT NULL
);

CREATE STREAM ALL_VALUES6 PARTITION ON COLUMN rowid EXPORT TO TARGET default6 (
  txnid                     BIGINT        NOT NULL
, rowid                     BIGINT        NOT NULL
, rowid_group               TINYINT       NOT NULL
, type_null_tinyint         TINYINT
, type_not_null_tinyint     TINYINT       NOT NULL
, type_null_smallint        SMALLINT
, type_not_null_smallint    SMALLINT      NOT NULL
, type_null_integer         INTEGER
, type_not_null_integer     INTEGER       NOT NULL
, type_null_bigint          BIGINT
, type_not_null_bigint      BIGINT        NOT NULL
, type_null_timestamp       TIMESTAMP
, type_not_null_timestamp   TIMESTAMP     NOT NULL
, type_null_float           FLOAT
, type_not_null_float       FLOAT         NOT NULL
, type_null_decimal         DECIMAL
, type_not_null_decimal     DECIMAL       NOT NULL
, type_null_varchar25       VARCHAR(32)
, type_not_null_varchar25   VARCHAR(32)   NOT NULL
, type_null_varchar128      VARCHAR(128)
, type_not_null_varchar128  VARCHAR(128)  NOT NULL
, type_null_varchar1024     VARCHAR(1024)
, type_not_null_varchar1024 VARCHAR(1024) NOT NULL
);

CREATE STREAM ALL_VALUES7 PARTITION ON COLUMN rowid EXPORT TO TARGET default7 (
  txnid                     BIGINT        NOT NULL
, rowid                     BIGINT        NOT NULL
, rowid_group               TINYINT       NOT NULL
, type_null_tinyint         TINYINT
, type_not_null_tinyint     TINYINT       NOT NULL
, type_null_smallint        SMALLINT
, type_not_null_smallint    SMALLINT      NOT NULL
, type_null_integer         INTEGER
, type_not_null_integer     INTEGER       NOT NULL
, type_null_bigint          BIGINT
, type_not_null_bigint      BIGINT        NOT NULL
, type_null_timestamp       TIMESTAMP
, type_not_null_timestamp   TIMESTAMP     NOT NULL
, type_null_float           FLOAT
, type_not_null_float       FLOAT         NOT NULL
, type_null_decimal         DECIMAL
, type_not_null_decimal     DECIMAL       NOT NULL
, type_null_varchar25       VARCHAR(32)
, type_not_null_varchar25   VARCHAR(32)   NOT NULL
, type_null_varchar128      VARCHAR(128)
, type_not_null_varchar128  VARCHAR(128)  NOT NULL
, type_null_varchar1024     VARCHAR(1024)
, type_not_null_varchar1024 VARCHAR(1024) NOT NULL
);

CREATE STREAM ALL_VALUES8 PARTITION ON COLUMN rowid EXPORT TO TARGET default8 (
  txnid                     BIGINT        NOT NULL
, rowid                     BIGINT        NOT NULL
, rowid_group               TINYINT       NOT NULL
, type_null_tinyint         TINYINT
, type_not_null_tinyint     TINYINT       NOT NULL
, type_null_smallint        SMALLINT
, type_not_null_smallint    SMALLINT      NOT NULL
, type_null_integer         INTEGER
, type_not_null_integer     INTEGER       NOT NULL
, type_null_bigint          BIGINT
, type_not_null_bigint      BIGINT        NOT NULL
, type_null_timestamp       TIMESTAMP
, type_not_null_timestamp   TIMESTAMP     NOT NULL
, type_null_float           FLOAT
, type_not_null_float       FLOAT         NOT NULL
, type_null_decimal         DECIMAL
, type_not_null_decimal     DECIMAL       NOT NULL
, type_null_varchar25       VARCHAR(32)
, type_not_null_varchar25   VARCHAR(32)   NOT NULL
, type_null_varchar128      VARCHAR(128)
, type_not_null_varchar128  VARCHAR(128)  NOT NULL
, type_null_varchar1024     VARCHAR(1024)
, type_not_null_varchar1024 VARCHAR(1024) NOT NULL
);

CREATE STREAM ALL_VALUES9 PARTITION ON COLUMN rowid EXPORT TO TARGET default9 (
  txnid                     BIGINT        NOT NULL
, rowid                     BIGINT        NOT NULL
, rowid_group               TINYINT       NOT NULL
, type_null_tinyint         TINYINT
, type_not_null_tinyint     TINYINT       NOT NULL
, type_null_smallint        SMALLINT
, type_not_null_smallint    SMALLINT      NOT NULL
, type_null_integer         INTEGER
, type_not_null_integer     INTEGER       NOT NULL
, type_null_bigint          BIGINT
, type_not_null_bigint      BIGINT        NOT NULL
, type_null_timestamp       TIMESTAMP
, type_not_null_timestamp   TIMESTAMP     NOT NULL
, type_null_float           FLOAT
, type_not_null_float       FLOAT         NOT NULL
, type_null_decimal         DECIMAL
, type_not_null_decimal     DECIMAL       NOT NULL
, type_null_varchar25       VARCHAR(32)
, type_not_null_varchar25   VARCHAR(32)   NOT NULL
, type_null_varchar128      VARCHAR(128)
, type_not_null_varchar128  VARCHAR(128)  NOT NULL
, type_null_varchar1024     VARCHAR(1024)
, type_not_null_varchar1024 VARCHAR(1024) NOT NULL
);

CREATE STREAM ALL_VALUES10 PARTITION ON COLUMN rowid EXPORT TO TARGET default10 (
  txnid                     BIGINT        NOT NULL
, rowid                     BIGINT        NOT NULL
, rowid_group               TINYINT       NOT NULL
, type_null_tinyint         TINYINT
, type_not_null_tinyint     TINYINT       NOT NULL
, type_null_smallint        SMALLINT
, type_not_null_smallint    SMALLINT      NOT NULL
, type_null_integer         INTEGER
, type_not_null_integer     INTEGER       NOT NULL
, type_null_bigint          BIGINT
, type_not_null_bigint      BIGINT        NOT NULL
, type_null_timestamp       TIMESTAMP
, type_not_null_timestamp   TIMESTAMP     NOT NULL
, type_null_float           FLOAT
, type_not_null_float       FLOAT         NOT NULL
, type_null_decimal         DECIMAL
, type_not_null_decimal     DECIMAL       NOT NULL
, type_null_varchar25       VARCHAR(32)
, type_not_null_varchar25   VARCHAR(32)   NOT NULL
, type_null_varchar128      VARCHAR(128)
, type_not_null_varchar128  VARCHAR(128)  NOT NULL
, type_null_varchar1024     VARCHAR(1024)
, type_not_null_varchar1024 VARCHAR(1024) NOT NULL
);





LOAD CLASSES exportbenchmark-procedures.jar;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport;
