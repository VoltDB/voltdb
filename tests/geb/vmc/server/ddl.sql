-- DDL SQL file used to initialize the standard VoltDB server, which is used
-- (by default) in the GEB tests of the VMC (VoltDB Management Center)

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

-- Export Table for Partitioned Data Table deletions
CREATE TABLE export_partitioned_table
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
PARTITION TABLE export_partitioned_table ON COLUMN rowid;

CREATE TABLE export_mirror_partitioned_table
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
, PRIMARY KEY (rowid)
);
PARTITION TABLE export_mirror_partitioned_table ON COLUMN rowid;

CREATE TABLE export_done_table
(
  txnid                     BIGINT          NOT NULL
);
PARTITION TABLE export_done_table ON COLUMN txnid;

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

-- Export Table for Replicated Data Table deletions
CREATE TABLE export_replicated_table
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

CREATE TABLE export_skinny_partitioned_table
(
  txnid                     BIGINT          NOT NULL
, rowid                     BIGINT          NOT NULL
);
PARTITION TABLE export_skinny_partitioned_table ON COLUMN rowid;

EXPORT TABLE export_skinny_partitioned_table;
EXPORT TABLE export_partitioned_table;
EXPORT TABLE export_replicated_table;
EXPORT TABLE export_done_table;

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
