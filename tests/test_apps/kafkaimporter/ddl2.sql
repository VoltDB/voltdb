-- This is the import table into which a single value will be pushed by kafkaimporter.

-- file -inlinebatch END_OF_BATCH

CREATE TABLE kafkaimporttable2
    (
          key                       BIGINT        NOT NULL
        , value                     BIGINT        NOT NULL
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
PARTITION TABLE kafkaimporttable2 ON COLUMN key;

CREATE TABLE kafkamirrortable2
    (
          key                     BIGINT        NOT NULL
        , value                     BIGINT        NOT NULL
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
        , primary key(key)
            
            --, rowid, rowid_group, type_null_tinyint,type_not_null_tinyint,
            --type_null_smallint,type_not_null_smallint,type_null_integer,type_null_bigint,
            --type_not_null_bigint,type_null_timestamp,type_not_null_timestamp,type_null_float,
            --type_not_null_float,type_null_decimal,type_not_null_decimal,type_null_varchar25,
            --type_not_null_varchar25,type_null_varchar128,type_not_null_varchar128,
            --type_null_varchar1024, type_not_null_varchar1024)
    );
PARTITION TABLE kafkamirrortable2 ON COLUMN key;

CREATE TABLE kafkaexporttable2
    (
          key                       BIGINT        NOT NULL
        , value                     BIGINT        NOT NULL
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
PARTITION TABLE kafkaexporttable2 ON COLUMN key;
EXPORT TABLE kafkaexporttable2;


CREATE TABLE importcounts
    (
                    KEY BIGINT NOT NULL,
                    TOTAL_ROWS_DELETED BIGINT NOT NULL
    );

PARTITION TABLE importcounts on COLUMN KEY;

CREATE TABLE exportcounts
    (
                    KEY BIGINT NOT NULL,
                    TOTAL_ROWS_EXPORTED BIGINT NOT NULL
    );

PARTITION TABLE exportcounts on COLUMN KEY;



-- Stored procedures
LOAD classes sp.jar;

CREATE PROCEDURE PARTITION ON TABLE KafkaImportTable2 COLUMN key FROM class kafkaimporter.db.procedures.InsertImport2;
CREATE PROCEDURE PARTITION ON TABLE Kafkamirrortable2 COLUMN key FROM class kafkaimporter.db.procedures.InsertExport2;

CREATE PROCEDURE CountMirror2 as select count(*) from kafkamirrortable2;
CREATE PROCEDURE CountImport2 as select count(*) from kafkaimporttable2;

-- and now for something completely different

-- CREATE TABLE import_type_null_tinyint
--     (
--           key                       BIGINT        NOT NULL
--         , value                     BIGINT        NOT NULL
--         , type_null_tinyint         TINYINT
--     );

-- CREATE TABLE import_type_null_smallint
--     (
--           key                       BIGINT        NOT NULL
--         , value                     BIGINT        NOT NULL
--         , type_null_smallint        SMALLINT
--     );

-- CREATE TABLE import_type_null_integer
--     (
--           key                       BIGINT        NOT NULL
--         , value                     BIGINT        NOT NULL
--         , type_null_integer         INTEGER
--     );

-- CREATE TABLE import_type_null_bigint
--     (
--           key                       BIGINT        NOT NULL
--         , value                     BIGINT        NOT NULL
--         , type_null_bigint          BIGINT
--     );

-- CREATE TABLE import_type_null_timestamp
--     (
--           key                       BIGINT        NOT NULL
--         , value                     BIGINT        NOT NULL
--         , type_null_timestamp       TIMESTAMP
--     );

-- CREATE TABLE import_type_null_float
--     (
--           key                       BIGINT        NOT NULL
--         , value                     BIGINT        NOT NULL
--         , type_null_float           FLOAT
--     );

-- CREATE TABLE import_type_null_decimal
--     (
--           key                       BIGINT        NOT NULL
--         , value                     BIGINT        NOT NULL
--         , type_null_decimal         DECIMAL
--     );

-- CREATE TABLE import_type_null_varchar25
--     (
--           key                       BIGINT        NOT NULL
--         , value                     BIGINT        NOT NULL
--         , type_null_varchar25       VARCHAR(32)
--     );

-- CREATE TABLE import_type_null_varchar128
--     (
--           key                       BIGINT        NOT NULL
--         , value                     BIGINT        NOT NULL
--         , type_null_varchar128      VARCHAR(128)
--     );

-- CREATE TABLE import_type_null_varchar1024
--     (
--           key                       BIGINT        NOT NULL
--         , value                     BIGINT        NOT NULL
--         , type_null_varchar1024     VARCHAR(1024)
--     );


-- END_OF_BATCH
