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

CREATE PROCEDURE InsertOnly0 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly0 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly1 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly1 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly2 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly2 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly3 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly3 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly4 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly4 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly5 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly5 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly6 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly6 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly7 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly7 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly8 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly8 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly9 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly9 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly10 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly10 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly11 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly11 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly12 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly12 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly13 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly13 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly14 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly14 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly15 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly15 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly16 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly16 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly17 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly17 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly18 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly18 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly19 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly19 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly20 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly20 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly21 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly21 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly22 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly22 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly23 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly23 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly24 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly24 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly25 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly25 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly26 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly26 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly27 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly27 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly28 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly28 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly29 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly29 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly30 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly30 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly31 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly31 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly32 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly32 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly33 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly33 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly34 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly34 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly35 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly35 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly36 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly36 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly37 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly37 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly38 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly38 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly39 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly39 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly40 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly40 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly41 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly41 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly42 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly42 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly43 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly43 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly44 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly44 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly45 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly45 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly46 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly46 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly47 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly47 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly48 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly48 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly49 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly49 ON TABLE Kafkaimporttable1 COLUMN key;

END_OF_BATCH
