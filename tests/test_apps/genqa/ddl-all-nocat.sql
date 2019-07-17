load classes sp.jar;

file -inlinebatch END_OF_BATCH

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
, type_not_null_timestamp   TIMESTAMP     default now NOT NULL
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
CREATE STREAM export_partitioned_table PARTITION ON COLUMN rowid EXPORT TO TARGET kafka_target
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
, type_not_null_timestamp   TIMESTAMP     default now NOT NULL
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

CREATE STREAM export_partitioned_table_foo PARTITION ON COLUMN rowid EXPORT TO TARGET rabbit_target
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
, type_not_null_timestamp   TIMESTAMP     default now NOT NULL
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

CREATE STREAM export_partitioned_table2 PARTITION ON COLUMN rowid export to target file_target
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
, type_not_null_timestamp   TIMESTAMP     default now NOT NULL
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
, type_not_null_timestamp   TIMESTAMP     default now NOT NULL
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
, type_not_null_timestamp   TIMESTAMP     default now NOT NULL
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

CREATE STREAM export_done_table PARTITION ON COLUMN txnid EXPORT TO TARGET kafka_target
(
  txnid                     BIGINT        NOT NULL
);

CREATE STREAM export_done_table_foo PARTITION ON COLUMN txnid EXPORT TO TARGET rabbit_target
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
, type_not_null_timestamp   TIMESTAMP     default now NOT NULL
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
CREATE STREAM  export_replicated_table EXPORT TO TARGET kafka_target
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
, type_not_null_timestamp   TIMESTAMP     default now NOT NULL
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

CREATE STREAM export_replicated_table_foo EXPORT TO TARGET rabbit_target
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
, type_not_null_timestamp   TIMESTAMP     default now NOT NULL
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


CREATE STREAM export_skinny_partitioned_table  PARTITION ON COLUMN rowid EXPORT TO TARGET kafka_target
(
  txnid                     BIGINT        NOT NULL
, rowid                     BIGINT        NOT NULL
);

CREATE STREAM export_skinny_partitioned_table_foo PARTITION ON COLUMN rowid EXPORT TO TARGET rabbit_target
(
  txnid                     BIGINT        NOT NULL
, rowid                     BIGINT        NOT NULL
);

CREATE STREAM export_skinny_partitioned_table2 PARTITION ON COLUMN rowid export to target file_target
(
  txnid                     BIGINT        NOT NULL
, rowid                     BIGINT        NOT NULL
);


CREATE PROCEDURE FROM CLASS genqa.procedures.JiggleSkinnyExportSinglePartition;
CREATE PROCEDURE PARTITION ON TABLE partitioned_table COLUMN rowid PARAMETER 0 FROM CLASS genqa.procedures.JiggleSinglePartition;
CREATE PROCEDURE FROM CLASS genqa.procedures.JiggleMultiPartition;
CREATE PROCEDURE PARTITION ON TABLE partitioned_table COLUMN rowid PARAMETER 0 FROM CLASS genqa.procedures.JiggleSinglePartitionWithDeletionExport;
CREATE PROCEDURE FROM CLASS genqa.procedures.JiggleMultiPartitionWithDeletionExport;
CREATE PROCEDURE PARTITION ON TABLE export_partitioned_table COLUMN rowid PARAMETER 0 FROM CLASS genqa.procedures.JiggleExportSinglePartition;
CREATE PROCEDURE PARTITION ON TABLE export_partitioned_table COLUMN rowid PARAMETER 0 FROM CLASS genqa.procedures.JiggleExportGroupSinglePartition;
CREATE PROCEDURE FROM CLASS genqa.procedures.JiggleExportMultiPartition;
CREATE PROCEDURE PARTITION ON TABLE partitioned_table COLUMN rowid PARAMETER 0 FROM CLASS genqa.procedures.WaitSinglePartition;
CREATE PROCEDURE FROM CLASS genqa.procedures.WaitMultiPartition;
CREATE PROCEDURE PARTITION ON TABLE export_done_table COLUMN txnid PARAMETER 0 FROM CLASS genqa.procedures.JiggleExportDoneTable;
CREATE PROCEDURE PARTITION ON TABLE export_done_table COLUMN txnid PARAMETER 0 FROM CLASS genqa.procedures.JiggleExportGroupDoneTable;

CREATE PROCEDURE SelectwithLimit as select * from export_mirror_partitioned_table where rowid between ? and ? order by rowid limit ?;

-- CREATE PROCEDURE FROM CLASS genqa2.procedures.JiggleSkinnyExportSinglePartition;
-- CREATE PROCEDURE PARTITION ON TABLE export_partitioned_table2 COLUMN rowid PARAMETER 0 FROM CLASS genqa2.procedures.JiggleExportSinglePartition;
-- CREATE PROCEDURE PARTITION ON TABLE export_done_table COLUMN txnid PARAMETER 0 FROM CLASS genqa2.procedures.JiggleExportDoneTable;

-- Export Stream with extra Geo columns
CREATE STREAM export_geo_partitioned_table PARTITION ON COLUMN rowid EXPORT TO TARGET kafka_target
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
, type_not_null_timestamp   TIMESTAMP     default now NOT NULL
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
, type_null_geography       GEOGRAPHY(1024)
, type_not_null_geography   GEOGRAPHY(1024)   NOT NULL
, type_null_geography_point GEOGRAPHY_POINT
, type_not_null_geography_point GEOGRAPHY_POINT NOT NULL

);

-- should be an exact copy of the stream. Used for verifiing
-- export stream contents.
CREATE TABLE export_geo_mirror_partitioned_table
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
, type_not_null_timestamp   TIMESTAMP     default now NOT NULL
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
, type_null_geography       GEOGRAPHY(1024)
, type_not_null_geography   GEOGRAPHY(1024)   NOT NULL
, type_null_geography_point GEOGRAPHY_POINT
, type_not_null_geography_point GEOGRAPHY_POINT NOT NULL
);
PARTITION TABLE export_geo_mirror_partitioned_table ON COLUMN rowid;

CREATE STREAM export_geo_done_table PARTITION ON COLUMN txnid EXPORT TO TARGET kafka_target
(
  txnid                     BIGINT        NOT NULL
);


CREATE VIEW EXPORT_PARTITIONED_TABLE_VIEW
(
  rowid
, record_count
)
AS
   SELECT rowid
        , COUNT(*)
     FROM EXPORT_PARTITIONED_TABLE
 GROUP BY rowid;


-- this is analogous to JiggleExportSinglePartition to insert tuples, but has the extra 4 geo columns
CREATE PROCEDURE PARTITION ON TABLE export_geo_partitioned_table COLUMN rowid PARAMETER 0 FROM CLASS genqa.procedures.JiggleExportGeoSinglePartition;

-- this is used by the verifier inside JDBCGetData, re-point to the geo tables
-- DROP PROCEDURE SelectwithLimit IF EXISTS;
-- CREATE PROCEDURE SelectwithLimit as select * from export_geo_mirror_partitioned_table where rowid between ? and ? order by rowid limit ?;


END_OF_BATCH
