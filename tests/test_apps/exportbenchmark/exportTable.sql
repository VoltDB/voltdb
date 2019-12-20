LOAD CLASSES exportbenchmark-procedures.jar;

file -inlinebatch END_OF_BATCH

CREATE STREAM ALL_VALUES PARTITION ON COLUMN rowid EXPORT TO TARGET export_target (
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





CREATE STREAM ALL_VALUES11 PARTITION ON COLUMN rowid EXPORT TO TARGET default11 (
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

CREATE STREAM ALL_VALUES12 PARTITION ON COLUMN rowid EXPORT TO TARGET default12 (
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

CREATE STREAM ALL_VALUES13 PARTITION ON COLUMN rowid EXPORT TO TARGET default13 (
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

CREATE STREAM ALL_VALUES14 PARTITION ON COLUMN rowid EXPORT TO TARGET default14 (
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

CREATE STREAM ALL_VALUES15 PARTITION ON COLUMN rowid EXPORT TO TARGET default15 (
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

CREATE STREAM ALL_VALUES16 PARTITION ON COLUMN rowid EXPORT TO TARGET default16 (
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

CREATE STREAM ALL_VALUES17 PARTITION ON COLUMN rowid EXPORT TO TARGET default17 (
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

CREATE STREAM ALL_VALUES18 PARTITION ON COLUMN rowid EXPORT TO TARGET default18 (
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

CREATE STREAM ALL_VALUES19 PARTITION ON COLUMN rowid EXPORT TO TARGET default19 (
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

CREATE STREAM ALL_VALUES20 PARTITION ON COLUMN rowid EXPORT TO TARGET default20 (
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

CREATE STREAM ALL_VALUES21 PARTITION ON COLUMN rowid EXPORT TO TARGET default21 (
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

CREATE STREAM ALL_VALUES22 PARTITION ON COLUMN rowid EXPORT TO TARGET default22 (
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

CREATE STREAM ALL_VALUES23 PARTITION ON COLUMN rowid EXPORT TO TARGET default23 (
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

CREATE STREAM ALL_VALUES24 PARTITION ON COLUMN rowid EXPORT TO TARGET default24 (
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

CREATE STREAM ALL_VALUES25 PARTITION ON COLUMN rowid EXPORT TO TARGET default25 (
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

CREATE STREAM ALL_VALUES26 PARTITION ON COLUMN rowid EXPORT TO TARGET default26 (
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

CREATE STREAM ALL_VALUES27 PARTITION ON COLUMN rowid EXPORT TO TARGET default27 (
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

CREATE STREAM ALL_VALUES28 PARTITION ON COLUMN rowid EXPORT TO TARGET default28 (
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

CREATE STREAM ALL_VALUES29 PARTITION ON COLUMN rowid EXPORT TO TARGET default29 (
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

CREATE STREAM ALL_VALUES30 PARTITION ON COLUMN rowid EXPORT TO TARGET default30 (
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

CREATE STREAM ALL_VALUES31 PARTITION ON COLUMN rowid EXPORT TO TARGET default31 (
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

CREATE STREAM ALL_VALUES32 PARTITION ON COLUMN rowid EXPORT TO TARGET default32 (
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

CREATE STREAM ALL_VALUES33 PARTITION ON COLUMN rowid EXPORT TO TARGET default33 (
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

CREATE STREAM ALL_VALUES34 PARTITION ON COLUMN rowid EXPORT TO TARGET default34 (
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

CREATE STREAM ALL_VALUES35 PARTITION ON COLUMN rowid EXPORT TO TARGET default35 (
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

CREATE STREAM ALL_VALUES36 PARTITION ON COLUMN rowid EXPORT TO TARGET default36 (
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

CREATE STREAM ALL_VALUES37 PARTITION ON COLUMN rowid EXPORT TO TARGET default37 (
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

CREATE STREAM ALL_VALUES38 PARTITION ON COLUMN rowid EXPORT TO TARGET default38 (
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

CREATE STREAM ALL_VALUES39 PARTITION ON COLUMN rowid EXPORT TO TARGET default39 (
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

CREATE STREAM ALL_VALUES40 PARTITION ON COLUMN rowid EXPORT TO TARGET default40 (
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

CREATE STREAM ALL_VALUES41 PARTITION ON COLUMN rowid EXPORT TO TARGET default41 (
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

CREATE STREAM ALL_VALUES42 PARTITION ON COLUMN rowid EXPORT TO TARGET default42 (
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

CREATE STREAM ALL_VALUES43 PARTITION ON COLUMN rowid EXPORT TO TARGET default43 (
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

CREATE STREAM ALL_VALUES44 PARTITION ON COLUMN rowid EXPORT TO TARGET default44 (
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

CREATE STREAM ALL_VALUES45 PARTITION ON COLUMN rowid EXPORT TO TARGET default45 (
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

CREATE STREAM ALL_VALUES46 PARTITION ON COLUMN rowid EXPORT TO TARGET default46 (
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

CREATE STREAM ALL_VALUES47 PARTITION ON COLUMN rowid EXPORT TO TARGET default47 (
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

CREATE STREAM ALL_VALUES48 PARTITION ON COLUMN rowid EXPORT TO TARGET default48 (
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

CREATE STREAM ALL_VALUES49 PARTITION ON COLUMN rowid EXPORT TO TARGET default49 (
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

CREATE STREAM ALL_VALUES50 PARTITION ON COLUMN rowid EXPORT TO TARGET default50 (
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

CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport;

CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES1 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport1;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES2 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport2;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES3 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport3;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES4 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport4;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES5 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport5;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES6 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport6;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES7 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport7;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES8 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport8;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES9 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport9;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES10 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport10;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES11 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport11;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES12 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport12;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES13 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport13;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES14 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport14;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES15 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport15;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES16 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport16;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES17 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport17;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES18 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport18;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES19 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport19;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES20 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport20;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES21 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport21;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES22 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport22;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES23 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport23;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES24 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport24;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES25 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport25;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES26 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport26;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES27 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport27;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES28 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport28;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES29 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport29;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES30 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport30;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES31 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport31;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES32 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport32;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES33 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport33;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES34 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport34;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES35 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport35;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES36 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport36;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES37 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport37;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES38 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport38;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES39 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport39;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES40 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport40;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES41 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport41;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES42 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport42;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES43 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport43;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES44 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport44;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES45 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport45;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES46 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport46;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES47 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport47;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES48 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport48;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES49 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport49;
CREATE PROCEDURE PARTITION ON TABLE ALL_VALUES50 COLUMN rowid PARAMETER 0 FROM CLASS exportbenchmark.InsertExport50;


END_OF_BATCH
