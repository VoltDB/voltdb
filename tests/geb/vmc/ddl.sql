-- DDL SQL file used to initialize the standard VoltDB server, which is used
-- (by default) in the GEB tests of the VMC (VoltDB Management Center)

-- Drop all items first, in case they already exist
DROP PROCEDURE CountPartitionedTable        IF EXISTS;
DROP PROCEDURE CountPartitionedTableByGroup IF EXISTS;
DROP PROCEDURE InsertPartitionedTableZeroes IF EXISTS;
DROP PROCEDURE CountReplicatedTable         IF EXISTS;
DROP PROCEDURE CountReplicatedTableByGroup  IF EXISTS;
DROP PROCEDURE InsertReplicatedTableZeroes  IF EXISTS;

DROP TABLE partitioned_table IF EXISTS CASCADE;
DROP TABLE replicated_table  IF EXISTS CASCADE;

DROP STREAM export_partitioned_table        IF EXISTS;
DROP STREAM export_mirror_partitioned_table IF EXISTS;
DROP STREAM export_done_table               IF EXISTS;
DROP STREAM export_replicated_table         IF EXISTS;
DROP STREAM export_skinny_partitioned_table IF EXISTS;

-- Partitioned Data Table
CREATE TABLE partitioned_table
(
  rowid                     BIGINT          NOT NULL
, rowid_group               TINYINT         NOT NULL
, type_null_tinyint         TINYINT
, type_not_null_tinyint     TINYINT         NOT NULL
, type_null_smallint        SMALLINT
, type_not_null_smallint    SMALLINT        NOT NULL
, type_null_integer         INTEGER
, type_not_null_integer     INTEGER         NOT NULL
, type_null_bigint          BIGINT
, type_not_null_bigint      BIGINT          NOT NULL
, type_null_timestamp       TIMESTAMP
, type_not_null_timestamp   TIMESTAMP       NOT NULL
, type_null_float           FLOAT
, type_not_null_float       FLOAT           NOT NULL
, type_null_decimal         DECIMAL
, type_not_null_decimal     DECIMAL         NOT NULL
, type_null_varchar25       VARCHAR(32)
, type_not_null_varchar25   VARCHAR(32)     NOT NULL
, type_null_varchar128      VARCHAR(128)
, type_not_null_varchar128  VARCHAR(128)    NOT NULL
, type_null_varchar1024     VARCHAR(1024)
, type_not_null_varchar1024 VARCHAR(1024)   NOT NULL
, type_null_point           GEOGRAPHY_POINT
, type_not_null_point       GEOGRAPHY_POINT NOT NULL
, type_null_polygon         GEOGRAPHY
, type_not_null_polygon     GEOGRAPHY       NOT NULL
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

-- Stream Table for Partitioned Data Table deletions
CREATE STREAM export_partitioned_table PARTITION ON COLUMN rowid
(
  txnid                     BIGINT          NOT NULL
, rowid                     BIGINT          NOT NULL
, rowid_group               TINYINT         NOT NULL
, type_null_tinyint         TINYINT
, type_not_null_tinyint     TINYINT         NOT NULL
, type_null_smallint        SMALLINT
, type_not_null_smallint    SMALLINT        NOT NULL
, type_null_integer         INTEGER
, type_not_null_integer     INTEGER         NOT NULL
, type_null_bigint          BIGINT
, type_not_null_bigint      BIGINT          NOT NULL
, type_null_timestamp       TIMESTAMP
, type_not_null_timestamp   TIMESTAMP       NOT NULL
, type_null_decimal         DECIMAL
, type_not_null_decimal     DECIMAL         NOT NULL
, type_null_float           FLOAT
, type_not_null_float       FLOAT           NOT NULL
, type_null_varchar25       VARCHAR(32)
, type_not_null_varchar25   VARCHAR(32)     NOT NULL
, type_null_varchar128      VARCHAR(128)
, type_not_null_varchar128  VARCHAR(128)    NOT NULL
, type_null_varchar1024     VARCHAR(1024)
, type_not_null_varchar1024 VARCHAR(1024)   NOT NULL
--, type_null_point           GEOGRAPHY_POINT
--, type_not_null_point       GEOGRAPHY_POINT NOT NULL
--, type_null_polygon         GEOGRAPHY
--, type_not_null_polygon     GEOGRAPHY       NOT NULL
);

CREATE STREAM export_mirror_partitioned_table PARTITION ON COLUMN rowid
(
  txnid                     BIGINT          NOT NULL
, rowid                     BIGINT          NOT NULL
, rowid_group               TINYINT         NOT NULL
, type_null_tinyint         TINYINT
, type_not_null_tinyint     TINYINT         NOT NULL
, type_null_smallint        SMALLINT
, type_not_null_smallint    SMALLINT        NOT NULL
, type_null_integer         INTEGER
, type_not_null_integer     INTEGER         NOT NULL
, type_null_bigint          BIGINT
, type_not_null_bigint      BIGINT          NOT NULL
, type_null_timestamp       TIMESTAMP
, type_not_null_timestamp   TIMESTAMP       NOT NULL
, type_null_decimal         DECIMAL
, type_not_null_decimal     DECIMAL         NOT NULL
, type_null_float           FLOAT
, type_not_null_float       FLOAT           NOT NULL
, type_null_varchar25       VARCHAR(32)
, type_not_null_varchar25   VARCHAR(32)     NOT NULL
, type_null_varchar128      VARCHAR(128)
, type_not_null_varchar128  VARCHAR(128)    NOT NULL
, type_null_varchar1024     VARCHAR(1024)
, type_not_null_varchar1024 VARCHAR(1024)   NOT NULL
--, type_null_point           GEOGRAPHY_POINT
--, type_not_null_point       GEOGRAPHY_POINT NOT NULL
--, type_null_polygon         GEOGRAPHY
--, type_not_null_polygon     GEOGRAPHY       NOT NULL
);

CREATE STREAM export_done_table PARTITION ON COLUMN txnid
(
  txnid                     BIGINT          NOT NULL
);

-- Replicated Table
CREATE TABLE replicated_table
(
  rowid                     BIGINT          NOT NULL
, rowid_group               TINYINT         NOT NULL
, type_null_tinyint         TINYINT
, type_not_null_tinyint     TINYINT         NOT NULL
, type_null_smallint        SMALLINT
, type_not_null_smallint    SMALLINT        NOT NULL
, type_null_integer         INTEGER
, type_not_null_integer     INTEGER         NOT NULL
, type_null_bigint          BIGINT
, type_not_null_bigint      BIGINT          NOT NULL
, type_null_timestamp       TIMESTAMP
, type_not_null_timestamp   TIMESTAMP       NOT NULL
, type_null_float           FLOAT
, type_not_null_float       FLOAT           NOT NULL
, type_null_decimal         DECIMAL
, type_not_null_decimal     DECIMAL         NOT NULL
, type_null_varchar25       VARCHAR(32)
, type_not_null_varchar25   VARCHAR(32)     NOT NULL
, type_null_varchar128      VARCHAR(128)
, type_not_null_varchar128  VARCHAR(128)    NOT NULL
, type_null_varchar1024     VARCHAR(1024)
, type_not_null_varchar1024 VARCHAR(1024)   NOT NULL
, type_null_point           GEOGRAPHY_POINT
, type_not_null_point       GEOGRAPHY_POINT NOT NULL
, type_null_polygon         GEOGRAPHY
, type_not_null_polygon     GEOGRAPHY       NOT NULL
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

-- Stream Table for Replicated Data Table deletions
CREATE STREAM export_replicated_table
(
  txnid                     BIGINT          NOT NULL
, rowid                     BIGINT          NOT NULL
, rowid_group               TINYINT         NOT NULL
, type_null_tinyint         TINYINT
, type_not_null_tinyint     TINYINT         NOT NULL
, type_null_smallint        SMALLINT
, type_not_null_smallint    SMALLINT        NOT NULL
, type_null_integer         INTEGER
, type_not_null_integer     INTEGER         NOT NULL
, type_null_bigint          BIGINT
, type_not_null_bigint      BIGINT          NOT NULL
, type_null_timestamp       TIMESTAMP
, type_not_null_timestamp   TIMESTAMP       NOT NULL
, type_null_float           FLOAT
, type_not_null_float       FLOAT           NOT NULL
, type_null_decimal         DECIMAL
, type_not_null_decimal     DECIMAL         NOT NULL
, type_null_varchar25       VARCHAR(32)
, type_not_null_varchar25   VARCHAR(32)     NOT NULL
, type_null_varchar128      VARCHAR(128)
, type_not_null_varchar128  VARCHAR(128)    NOT NULL
, type_null_varchar1024     VARCHAR(1024)
, type_not_null_varchar1024 VARCHAR(1024)   NOT NULL
--, type_null_point           GEOGRAPHY_POINT
--, type_not_null_point       GEOGRAPHY_POINT NOT NULL
--, type_null_polygon         GEOGRAPHY
--, type_not_null_polygon     GEOGRAPHY       NOT NULL
);

CREATE STREAM export_skinny_partitioned_table PARTITION ON COLUMN rowid
(
  txnid                     BIGINT          NOT NULL
, rowid                     BIGINT          NOT NULL
);

-- Simple Table used for Kafka Import
CREATE TABLE kafka_import_table
(
  message                   VARCHAR
, PRIMARY KEY (message)
);

-- Simple User-Defined Stored Procedures, to test CREATE PROCEDURE AS ...
-- and the display of User-Defined Stored Procedures in the VMC
CREATE PROCEDURE CountPartitionedTable AS
  SELECT COUNT(*) FROM partitioned_table;
CREATE PROCEDURE CountPartitionedTableByGroup AS
  SELECT COUNT(*) FROM partitioned_table WHERE rowid_group=?;
CREATE PROCEDURE InsertPartitionedTableZeroes AS
  INSERT INTO partitioned_table VALUES
  (?, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, '0', '0', '0', '0', '0', '0', PointFromText('POINT(0 0)'), PointFromText('POINT(0 0)'),
   PolygonFromText('POLYGON((0 0, 1 0, 0 1, 0 0))'), PolygonFromText('POLYGON((0 0, 1 0, 0 1, 0 0))'));

CREATE PROCEDURE CountReplicatedTable AS
  SELECT COUNT(*) FROM replicated_table;
CREATE PROCEDURE CountReplicatedTableByGroup AS
  SELECT COUNT(*) FROM replicated_table WHERE rowid_group=?;
CREATE PROCEDURE InsertReplicatedTableZeroes AS
  INSERT INTO replicated_table VALUES
  (?, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, '0', '0', '0', '0', '0', '0', PointFromText('POINT(0 0)'), PointFromText('POINT(0 0)'),
   PolygonFromText('POLYGON((0 0, 1 0, 0 1, 0 0))'), PolygonFromText('POLYGON((0 0, 1 0, 0 1, 0 0))'));

-- Load the classes used by FullDdlSqlTest, to test CREATE PROCEDURE FROM CLASS
-- (as run in the VMC)
load classes fullddlfeatures.jar;
