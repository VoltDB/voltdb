load classes sp.jar;

file -inlinebatch END_OF_BATCH

-- Partitioned Data Table
-- Target for loopback import with metadata, the first 6 columns
CREATE TABLE partitioned_table
(
  VOLT_TRANSACTION_ID       BIGINT
, VOLT_EXPORT_TIMESTAMP     BIGINT
, VOLT_EXPORT_SEQUENCE_NUMBER BIGINT
, VOLT_PARTITION_ID         BIGINT
, VOLT_SITE_ID BIGINT
, VOLT_EXPORT_OPERATION     TINYINT
, txnid                     BIGINT        DEFAULT 0 NOT NULL
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
, type_not_null_timestamp   TIMESTAMP     DEFAULT NOW NOT NULL
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
-- PARTITION TABLE partitioned_table ON COLUMN rowid;

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

-- Grouping view over Partitioned Data Export Op Codes
CREATE VIEW partitioned_table_group_ops
(
  VOLT_EXPORT_OPERATION
, record_count
)
AS
   SELECT VOLT_EXPORT_OPERATION
        , COUNT(*)
     FROM partitioned_table
 GROUP BY VOLT_EXPORT_OPERATION;

-- Export Table for Partitioned Data Table
CREATE TABLE export_partitioned_table EXPORT TO TARGET loopback_target ON insert, update, delete
(
  txnid                     BIGINT        DEFAULT 0 NOT NULL
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
, type_not_null_timestamp   TIMESTAMP     DEFAULT NOW NOT NULL ASSUMEUNIQUE
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
PARTITION TABLE export_partitioned_table ON COLUMN rowid;

CREATE TABLE export_partitioned_table2 EXPORT TO TARGET file_target
(
  txnid                     BIGINT        DEFAULT 0 NOT NULL
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
, type_not_null_timestamp   TIMESTAMP     DEFAULT NOW NOT NULL ASSUMEUNIQUE
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
) USING TTL 5 SECONDS ON COLUMN type_not_null_timestamp;
PARTITION TABLE export_partitioned_table2 ON COLUMN rowid;

CREATE TABLE export_mirror_partitioned_table
(
  txnid                     BIGINT        DEFAULT 0 NOT NULL
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
, type_not_null_timestamp   TIMESTAMP     DEFAULT NOW NOT NULL
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
, type_not_null_timestamp   TIMESTAMP     DEFAULT NOW NOT NULL
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
CREATE TABLE  export_replicated_table EXPORT TO TARGET abc ON insert, delete, update
(
  txnid                     BIGINT        DEFAULT 0 NOT NULL
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
, type_not_null_timestamp   TIMESTAMP     DEFAULT NOW NOT NULL ASSUMEUNIQUE
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
) USING TTL 5 SECONDS ON COLUMN type_not_null_timestamp;

-- CREATE PROCEDURE PARTITION ON TABLE partitioned_table COLUMN rowid PARAMETER 0 FROM CLASS genqa.procedures.JiggleSinglePartition;
CREATE PROCEDURE FROM CLASS genqa.procedures.JiggleSinglePartition;
-- CREATE PROCEDURE PARTITION ON TABLE partitioned_table COLUMN rowid PARAMETER 0 FROM CLASS genqa.procedures.JiggleSinglePartitionWithDeletionExport;
CREATE PROCEDURE FROM CLASS genqa.procedures.JiggleSinglePartitionWithDeletionExport;
CREATE PROCEDURE PARTITION ON TABLE export_partitioned_table COLUMN rowid PARAMETER 0 FROM CLASS genqa.procedures.JiggleExportSinglePartition;
CREATE PROCEDURE PARTITION ON TABLE export_partitioned_table COLUMN rowid PARAMETER 0 FROM CLASS genqa.procedures.JiggleExportGroupSinglePartition;
CREATE PROCEDURE PARTITION ON TABLE export_partitioned_table COLUMN rowid PARAMETER 0 FROM CLASS genqa.procedures.TableExport;
CREATE PROCEDURE FROM CLASS genqa.procedures.JiggleExportMultiPartition;
-- CREATE PROCEDURE PARTITION ON TABLE partitioned_table COLUMN rowid PARAMETER 0 FROM CLASS genqa.procedures.WaitSinglePartition;
CREATE PROCEDURE FROM CLASS genqa.procedures.WaitSinglePartition;
CREATE PROCEDURE FROM CLASS genqa.procedures.WaitMultiPartition;
-- CREATE PROCEDURE FROM CLASS genqa.procedures.MigrateExport;
CREATE PROCEDURE FROM CLASS genqa.procedures.InsertExportDoneDetails;

CREATE PROCEDURE SelectwithLimit as select * from export_mirror_partitioned_table where rowid between ? and ? order by rowid limit ?;

-- Export Table with extra Geo columns
CREATE TABLE export_geo_partitioned_table EXPORT TO TARGET abc ON insert, delete, update
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
, type_not_null_timestamp   TIMESTAMP     DEFAULT NOW NOT NULL
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
PARTITION TABLE export_geo_partitioned_table ON COLUMN rowid;

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
, type_not_null_timestamp   TIMESTAMP     DEFAULT NOW NOT NULL
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

CREATE STREAM export_geo_done_table PARTITION ON COLUMN txnid EXPORT TO TARGET abc
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

-- CREATE PROCEDURE insert_with_metadata PARTITION ON TABLE partitioned_table COLUMN rowid PARAMETER 7 as insert into partitioned_table values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
CREATE PROCEDURE insert_with_metadata as insert into partitioned_table values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);


END_OF_BATCH
