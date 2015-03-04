IMPORT CLASS genqa2.procedures.SampleRecord;

-- Export Table for Partitioned Data Table deletions
CREATE TABLE export_partitioned_table2
(
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
, type_null_decimal         DECIMAL
, type_not_null_decimal     DECIMAL       NOT NULL
, type_null_float           FLOAT
, type_not_null_float       FLOAT         NOT NULL
, type_null_varchar25       VARCHAR(32)
, type_not_null_varchar25   VARCHAR(32)   NOT NULL
, type_null_varchar128      VARCHAR(128)
, type_not_null_varchar128  VARCHAR(128)  NOT NULL
, type_null_varchar1024     VARCHAR(1024)
, type_not_null_varchar1024 VARCHAR(1024) NOT NULL
);
PARTITION TABLE export_partitioned_table2 ON COLUMN rowid;

CREATE TABLE export_partitioned_table2_foo
(
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
, type_null_decimal         DECIMAL
, type_not_null_decimal     DECIMAL       NOT NULL
, type_null_float           FLOAT
, type_not_null_float       FLOAT         NOT NULL
, type_null_varchar25       VARCHAR(32)
, type_not_null_varchar25   VARCHAR(32)   NOT NULL
, type_null_varchar128      VARCHAR(128)
, type_not_null_varchar128  VARCHAR(128)  NOT NULL
, type_null_varchar1024     VARCHAR(1024)
, type_not_null_varchar1024 VARCHAR(1024) NOT NULL
);
PARTITION TABLE export_partitioned_table2_foo ON COLUMN rowid;

CREATE TABLE export_mirror_partitioned_table2
(
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
, type_null_decimal         DECIMAL
, type_not_null_decimal     DECIMAL       NOT NULL
, type_null_float           FLOAT
, type_not_null_float       FLOAT         NOT NULL
, type_null_varchar25       VARCHAR(32)
, type_not_null_varchar25   VARCHAR(32)   NOT NULL
, type_null_varchar128      VARCHAR(128)
, type_not_null_varchar128  VARCHAR(128)  NOT NULL
, type_null_varchar1024     VARCHAR(1024)
, type_not_null_varchar1024 VARCHAR(1024) NOT NULL
, PRIMARY KEY (rowid)
);
PARTITION TABLE export_mirror_partitioned_table2 ON COLUMN rowid;

CREATE TABLE export_skinny_partitioned_table2
(
  txnid                     BIGINT        NOT NULL
, rowid                     BIGINT        NOT NULL
);
PARTITION TABLE export_skinny_partitioned_table2 ON COLUMN rowid;

CREATE TABLE export_skinny_partitioned_table2_foo
(
  txnid                     BIGINT        NOT NULL
, rowid                     BIGINT        NOT NULL
);
PARTITION TABLE export_skinny_partitioned_table2_foo ON COLUMN rowid;

CREATE TABLE export_done_table
(
  txnid                     BIGINT        NOT NULL
);
PARTITION TABLE export_done_table ON COLUMN txnid;

CREATE TABLE export_done_table_foo
(
  txnid                     BIGINT        NOT NULL
);
PARTITION TABLE export_done_table_foo ON COLUMN txnid;

CREATE PROCEDURE FROM CLASS genqa2.procedures.JiggleSkinnyExportSinglePartition;
CREATE PROCEDURE FROM CLASS genqa2.procedures.JiggleExportSinglePartition;
CREATE PROCEDURE FROM CLASS genqa2.procedures.JiggleExportGroupSinglePartition;
CREATE PROCEDURE FROM CLASS genqa2.procedures.JiggleExportDoneTable;
CREATE PROCEDURE FROM CLASS genqa2.procedures.JiggleExportGroupDoneTable;

PARTITION PROCEDURE JiggleSkinnyExportSinglePartition
  ON TABLE export_skinny_partitioned_table2 COLUMN rowid;

EXPORT TABLE export_skinny_partitioned_table2;
EXPORT TABLE export_partitioned_table2;
EXPORT TABLE export_done_table;

EXPORT TABLE export_skinny_partitioned_table2_foo TO STREAM foo;
EXPORT TABLE export_partitioned_table2_foo TO STREAM foo;
EXPORT TABLE export_done_table_foo TO STREAM foo;
