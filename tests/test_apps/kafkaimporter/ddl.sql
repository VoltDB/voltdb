-- This is the import table into which a single value will be pushed by kafkaimporter.

LOAD classes sp.jar;

file -inlinebatch END_OF_BATCH

------- Kafka Importer Tables -------
CREATE TABLE kafkaimporttable1
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table1 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable1 ON COLUMN KEY;

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

CREATE TABLE kafkamirrortable1
     (
                  KEY   BIGINT NOT NULL ,
                  value BIGINT NOT NULL ,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafkamirrortable PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkamirrortable1 ON COLUMN KEY;

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
    );
PARTITION TABLE kafkamirrortable2 ON COLUMN key;

-- Export table
CREATE TABLE kafkaexporttable1
     (
                  KEY   BIGINT NOT NULL ,
                  value BIGINT NOT NULL
     );

PARTITION TABLE kafkaexporttable1 ON COLUMN KEY;
EXPORT TABLE kafkaexporttable1;

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
                    KEY BIGINT DEFAULT 0 NOT NULL,
                    TOTAL_ROWS_DELETED BIGINT  DEFAULT 0 NOT NULL,
                    VALUE_MISMATCH BIGINT DEFAULT 0 NOT NULL
    );

PARTITION TABLE importcounts on COLUMN KEY;

CREATE TABLE exportcounts
    (
                    KEY BIGINT NOT NULL,
                    TOTAL_ROWS_EXPORTED BIGINT NOT NULL
    );

PARTITION TABLE exportcounts on COLUMN KEY;

CREATE PROCEDURE PARTITION ON TABLE KafkaImportTable1 COLUMN key FROM class kafkaimporter.db.procedures.InsertImport;
CREATE PROCEDURE PARTITION ON TABLE Kafkamirrortable1 COLUMN key FROM class kafkaimporter.db.procedures.InsertExport;

CREATE PROCEDURE PARTITION ON TABLE KafkaImportTable2 COLUMN key FROM class kafkaimporter.db.procedures.InsertImport2;
CREATE PROCEDURE PARTITION ON TABLE Kafkamirrortable2 COLUMN key FROM class kafkaimporter.db.procedures.InsertExport2;

CREATE PROCEDURE FROM class kafkaimporter.db.procedures.InsertFinal;
CREATE PROCEDURE FROM class kafkaimporter.db.procedures.MatchRows;

CREATE PROCEDURE CountMirror1 as select count(*) from kafkamirrortable1;
CREATE PROCEDURE CountImport1 as select count(*) from kafkaimporttable1;

CREATE PROCEDURE CountMirror2 as select count(*) from kafkamirrortable2;
CREATE PROCEDURE CountImport2 as select count(*) from kafkaimporttable2;
CREATE PROCEDURE ImportCountMinMax as select count(key), min(key), max(key) from kafkaimporttable1;
CREATE PROCEDURE InsertOnly as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly ON TABLE Kafkaimporttable1 COLUMN key;

END_OF_BATCH
