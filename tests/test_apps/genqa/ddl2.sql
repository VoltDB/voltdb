--IMPORT CLASS genqa2.procedures.SampleRecord;

-- Export Table for Partitioned Data Table deletions
CREATE STREAM export_partitioned_table2 PARTITION ON COLUMN rowid export to target default
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
);
PARTITION TABLE export_mirror_partitioned_table2 ON COLUMN rowid;

CREATE STREAM export_skinny_partitioned_table2 PARTITION ON COLUMN rowid export to target default
(
  txnid                     BIGINT        NOT NULL
, rowid                     BIGINT        NOT NULL
);

CREATE STREAM export_done_table PARTITION ON COLUMN txnid export to target default
(
  txnid                     BIGINT        NOT NULL
);

CREATE PROCEDURE FROM CLASS genqa2.procedures.JiggleSkinnyExportSinglePartition;
CREATE PROCEDURE PARTITION ON TABLE export_partitioned_table COLUMN rowid PARAMETER 0 FROM CLASS genqa2.procedures.JiggleExportSinglePartition;
CREATE PROCEDURE PARTITION ON TABLE export_done_table COLUMN txnid PARAMETER 0 FROM CLASS genqa2.procedures.JiggleExportDoneTable;
