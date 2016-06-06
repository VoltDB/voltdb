
file -inlinebatch END_OF_BATCH

-- Export Table for Partitioned Data Table deletions
CREATE STREAM export_geo_partitioned_table PARTITION ON COLUMN rowid EXPORT TO TARGET abc
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
, type_null_geography       GEOGRAPHY(1024)
, type_not_null_geography   GEOGRAPHY(1024)   NOT NULL
, type_null_geography_point GEOGRAPHY_POINT
, type_not_null_geography_point GEOGRAPHY_POINT NOT NULL

);

CREATE TABLE  export_geo_mirror_partitioned_table
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
, type_null_geography       GEOGRAPHY(1024)
, type_not_null_geography   GEOGRAPHY(1024)   NOT NULL
, type_null_geography_point GEOGRAPHY_POINT
, type_not_null_geography_point GEOGRAPHY_POINT NOT NULL

, PRIMARY KEY (rowid)
);
PARTITION TABLE export_geo_mirror_partitioned_table ON COLUMN rowid;

CREATE STREAM export_geo_done_table PARTITION ON COLUMN txnid EXPORT TO TARGET abc
(
  txnid                     BIGINT        NOT NULL
);


CREATE PROCEDURE FROM CLASS genqa.procedures.JiggleExportGeoSinglePartition;


CREATE PROCEDURE SelectGeowithLimit as select * from export_geo_mirror_partitioned_table where rowid between ? and ? order by rowid limit ?;

END_OF_BATCH
