IMPORT CLASS genqa.procedures.SampleRecord;

-- Partitioned Data Table
CREATE TABLE partitioned_table
(
  rowid                     BIGINT        NOT NULL
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
, PRIMARY KEY (rowid)
);
PARTITION TABLE partitioned_table ON COLUMN rowid;

-- Index over rowid_group on Partitioned Data Table
CREATE INDEX IX_partitioned_table_rowid_group
    ON partitioned_table ( rowid_group );

-- Grouping view over Partitioned Data Table
CREATE VIEW partitioned_table_group
(
  rowid_group
, record_count
)
AS
   SELECT rowid_group
        , COUNT(*)
     FROM partitioned_table
 GROUP BY rowid_group;

-- Export Table for Partitioned Data Table deletions
CREATE STREAM export_partitioned_table PARTITION ON COLUMN rowid
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

CREATE STREAM export_partitioned_table_foo PARTITION ON COLUMN rowid EXPORT TO TARGET foo
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

CREATE TABLE export_mirror_partitioned_table
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
PARTITION TABLE export_mirror_partitioned_table ON COLUMN rowid;

CREATE STREAM export_done_table PARTITION ON COLUMN txnid
(
  txnid                     BIGINT        NOT NULL
);

CREATE STREAM export_done_table_foo PARTITION ON COLUMN txnid EXPORT TO TARGET foo
(
  txnid                     BIGINT        NOT NULL
);

-- Replicated Table
CREATE TABLE replicated_table
(
  rowid                     BIGINT        NOT NULL
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
, PRIMARY KEY (rowid)
);

-- Index over rowid_group on Replicated Data Table
CREATE INDEX IX_replicated_table_rowid_group
    ON replicated_table ( rowid_group );

-- Grouping view over Replicated Data Table
CREATE VIEW replicated_table_group
(
  rowid_group
, record_count
)
AS
   SELECT rowid_group
        , COUNT(*)
     FROM replicated_table
 GROUP BY rowid_group;

-- Export Table for Replicated Data Table deletions
CREATE STREAM export_replicated_table
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

CREATE STREAM export_replicated_table_foo EXPORT TO TARGET foo
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

CREATE STREAM export_skinny_partitioned_table PARTITION ON COLUMN rowid
(
  txnid                     BIGINT        NOT NULL
, rowid                     BIGINT        NOT NULL
);

CREATE STREAM export_skinny_partitioned_table_foo PARTITION ON COLUMN rowid EXPORT TO TARGET foo
(
  txnid                     BIGINT        NOT NULL
, rowid                     BIGINT        NOT NULL
);

CREATE PROCEDURE FROM CLASS genqa.procedures.JiggleSkinnyExportSinglePartition;
CREATE PROCEDURE FROM CLASS genqa.procedures.JiggleSinglePartition;
CREATE PROCEDURE FROM CLASS genqa.procedures.JiggleMultiPartition;
CREATE PROCEDURE FROM CLASS genqa.procedures.JiggleSinglePartitionWithDeletionExport;
CREATE PROCEDURE FROM CLASS genqa.procedures.JiggleMultiPartitionWithDeletionExport;
CREATE PROCEDURE FROM CLASS genqa.procedures.JiggleExportSinglePartition;
CREATE PROCEDURE FROM CLASS genqa.procedures.JiggleExportGroupSinglePartition;
CREATE PROCEDURE FROM CLASS genqa.procedures.JiggleExportMultiPartition;
CREATE PROCEDURE FROM CLASS genqa.procedures.WaitSinglePartition;
CREATE PROCEDURE FROM CLASS genqa.procedures.WaitMultiPartition;
CREATE PROCEDURE FROM CLASS genqa.procedures.JiggleExportDoneTable;
CREATE PROCEDURE FROM CLASS genqa.procedures.JiggleExportGroupDoneTable;
