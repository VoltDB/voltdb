-- This is the import table into which a single value will be pushed by kafkaimporter.

LOAD classes sp.jar;

file -inlinebatch END_OF_BATCH

------- Kafka Importer Tables -------

CREATE TABLE kafkaimporttable0
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table0 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable0 ON COLUMN KEY;

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
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table2 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable2 ON COLUMN KEY;

CREATE TABLE kafkaimporttable3
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table3 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable3 ON COLUMN KEY;

CREATE TABLE kafkaimporttable4
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table4 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable4 ON COLUMN KEY;

CREATE TABLE kafkaimporttable5
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table5 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable5 ON COLUMN KEY;

CREATE TABLE kafkaimporttable6
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table6 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable6 ON COLUMN KEY;

CREATE TABLE kafkaimporttable7
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table7 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable7 ON COLUMN KEY;

CREATE TABLE kafkaimporttable8
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table8 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable8 ON COLUMN KEY;

CREATE TABLE kafkaimporttable9
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table9 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable9 ON COLUMN KEY;

CREATE TABLE kafkaimporttable10
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table10 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable10 ON COLUMN KEY;

CREATE TABLE kafkaimporttable11
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table11 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable11 ON COLUMN KEY;

CREATE TABLE kafkaimporttable12
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table12 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable12 ON COLUMN KEY;

CREATE TABLE kafkaimporttable13
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table13 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable13 ON COLUMN KEY;

CREATE TABLE kafkaimporttable14
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table14 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable14 ON COLUMN KEY;

CREATE TABLE kafkaimporttable15
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table15 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable15 ON COLUMN KEY;

CREATE TABLE kafkaimporttable16
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table16 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable16 ON COLUMN KEY;

CREATE TABLE kafkaimporttable17
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table17 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable17 ON COLUMN KEY;

CREATE TABLE kafkaimporttable18
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table18 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable18 ON COLUMN KEY;

CREATE TABLE kafkaimporttable19
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table19 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable19 ON COLUMN KEY;

CREATE TABLE kafkaimporttable20
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table20 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable20 ON COLUMN KEY;

CREATE TABLE kafkaimporttable21
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table21 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable21 ON COLUMN KEY;

CREATE TABLE kafkaimporttable22
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table22 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable22 ON COLUMN KEY;

CREATE TABLE kafkaimporttable23
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table23 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable23 ON COLUMN KEY;

CREATE TABLE kafkaimporttable24
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table24 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable24 ON COLUMN KEY;

CREATE TABLE kafkaimporttable25
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table25 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable25 ON COLUMN KEY;

CREATE TABLE kafkaimporttable26
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table26 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable26 ON COLUMN KEY;

CREATE TABLE kafkaimporttable27
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table27 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable27 ON COLUMN KEY;

CREATE TABLE kafkaimporttable28
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table28 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable28 ON COLUMN KEY;

CREATE TABLE kafkaimporttable29
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table29 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable29 ON COLUMN KEY;

CREATE TABLE kafkaimporttable30
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table30 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable30 ON COLUMN KEY;

CREATE TABLE kafkaimporttable31
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table31 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable31 ON COLUMN KEY;

CREATE TABLE kafkaimporttable32
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table32 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable32 ON COLUMN KEY;

CREATE TABLE kafkaimporttable33
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table33 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable33 ON COLUMN KEY;

CREATE TABLE kafkaimporttable34
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table34 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable34 ON COLUMN KEY;

CREATE TABLE kafkaimporttable35
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table35 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable35 ON COLUMN KEY;

CREATE TABLE kafkaimporttable36
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table36 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable36 ON COLUMN KEY;

CREATE TABLE kafkaimporttable37
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table37 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable37 ON COLUMN KEY;

CREATE TABLE kafkaimporttable38
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table38 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable38 ON COLUMN KEY;

CREATE TABLE kafkaimporttable39
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table39 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable39 ON COLUMN KEY;

CREATE TABLE kafkaimporttable40
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table40 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable40 ON COLUMN KEY;

CREATE TABLE kafkaimporttable41
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table41 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable41 ON COLUMN KEY;

CREATE TABLE kafkaimporttable42
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table42 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable42 ON COLUMN KEY;

CREATE TABLE kafkaimporttable43
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table43 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable43 ON COLUMN KEY;

CREATE TABLE kafkaimporttable44
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table44 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable44 ON COLUMN KEY;

CREATE TABLE kafkaimporttable45
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table45 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable45 ON COLUMN KEY;

CREATE TABLE kafkaimporttable46
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table46 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable46 ON COLUMN KEY;

CREATE TABLE kafkaimporttable47
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table47 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable47 ON COLUMN KEY;

CREATE TABLE kafkaimporttable48
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table48 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable48 ON COLUMN KEY;

CREATE TABLE kafkaimporttable49
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table49 PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkaimporttable49 ON COLUMN KEY;

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

CREATE PROCEDURE PARTITION ON TABLE Kafkamirrortable2 COLUMN key FROM class kafkaimporter.db.procedures.InsertExport2;

CREATE PROCEDURE FROM class kafkaimporter.db.procedures.InsertFinal;
CREATE PROCEDURE FROM class kafkaimporter.db.procedures.MatchRows;

CREATE PROCEDURE CountMirror1 as select count(*) from kafkamirrortable1;
CREATE PROCEDURE CountImport1 as select count(*) from kafkaimporttable1;

CREATE PROCEDURE CountMirror2 as select count(*) from kafkamirrortable2;
CREATE PROCEDURE CountImport2 as select count(*) from kafkaimporttable2;
CREATE PROCEDURE ImportCountMinMax as select count(key), min(key), max(key) from kafkaimporttable1;

CREATE PROCEDURE InsertOnly0 as upsert into KAFKAIMPORTTABLE0(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly0 ON TABLE Kafkaimporttable0 COLUMN key;

CREATE PROCEDURE InsertOnly1 as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly1 ON TABLE Kafkaimporttable1 COLUMN key;

CREATE PROCEDURE InsertOnly2 as upsert into KAFKAIMPORTTABLE2(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly2 ON TABLE Kafkaimporttable2 COLUMN key;

CREATE PROCEDURE InsertOnly3 as upsert into KAFKAIMPORTTABLE3(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly3 ON TABLE Kafkaimporttable3 COLUMN key;

CREATE PROCEDURE InsertOnly4 as upsert into KAFKAIMPORTTABLE4(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly4 ON TABLE Kafkaimporttable4 COLUMN key;

CREATE PROCEDURE InsertOnly5 as upsert into KAFKAIMPORTTABLE5(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly5 ON TABLE Kafkaimporttable5 COLUMN key;

CREATE PROCEDURE InsertOnly6 as upsert into KAFKAIMPORTTABLE6(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly6 ON TABLE Kafkaimporttable6 COLUMN key;

CREATE PROCEDURE InsertOnly7 as upsert into KAFKAIMPORTTABLE7(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly7 ON TABLE Kafkaimporttable7 COLUMN key;

CREATE PROCEDURE InsertOnly8 as upsert into KAFKAIMPORTTABLE8(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly8 ON TABLE Kafkaimporttable8 COLUMN key;

CREATE PROCEDURE InsertOnly9 as upsert into KAFKAIMPORTTABLE9(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly9 ON TABLE Kafkaimporttable9 COLUMN key;

CREATE PROCEDURE InsertOnly10 as upsert into KAFKAIMPORTTABLE10(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly10 ON TABLE Kafkaimporttable10 COLUMN key;

CREATE PROCEDURE InsertOnly11 as upsert into KAFKAIMPORTTABLE11(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly11 ON TABLE Kafkaimporttable11 COLUMN key;

CREATE PROCEDURE InsertOnly12 as upsert into KAFKAIMPORTTABLE12(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly12 ON TABLE Kafkaimporttable12 COLUMN key;

CREATE PROCEDURE InsertOnly13 as upsert into KAFKAIMPORTTABLE13(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly13 ON TABLE Kafkaimporttable13 COLUMN key;

CREATE PROCEDURE InsertOnly14 as upsert into KAFKAIMPORTTABLE14(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly14 ON TABLE Kafkaimporttable14 COLUMN key;

CREATE PROCEDURE InsertOnly15 as upsert into KAFKAIMPORTTABLE15(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly15 ON TABLE Kafkaimporttable15 COLUMN key;

CREATE PROCEDURE InsertOnly16 as upsert into KAFKAIMPORTTABLE16(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly16 ON TABLE Kafkaimporttable16 COLUMN key;

CREATE PROCEDURE InsertOnly17 as upsert into KAFKAIMPORTTABLE17(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly17 ON TABLE Kafkaimporttable17 COLUMN key;

CREATE PROCEDURE InsertOnly18 as upsert into KAFKAIMPORTTABLE18(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly18 ON TABLE Kafkaimporttable18 COLUMN key;

CREATE PROCEDURE InsertOnly19 as upsert into KAFKAIMPORTTABLE19(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly19 ON TABLE Kafkaimporttable19 COLUMN key;

CREATE PROCEDURE InsertOnly20 as upsert into KAFKAIMPORTTABLE20(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly20 ON TABLE Kafkaimporttable20 COLUMN key;

CREATE PROCEDURE InsertOnly21 as upsert into KAFKAIMPORTTABLE21(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly21 ON TABLE Kafkaimporttable21 COLUMN key;

CREATE PROCEDURE InsertOnly22 as upsert into KAFKAIMPORTTABLE22(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly22 ON TABLE Kafkaimporttable22 COLUMN key;

CREATE PROCEDURE InsertOnly23 as upsert into KAFKAIMPORTTABLE23(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly23 ON TABLE Kafkaimporttable23 COLUMN key;

CREATE PROCEDURE InsertOnly24 as upsert into KAFKAIMPORTTABLE24(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly24 ON TABLE Kafkaimporttable24 COLUMN key;

CREATE PROCEDURE InsertOnly25 as upsert into KAFKAIMPORTTABLE25(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly25 ON TABLE Kafkaimporttable25 COLUMN key;

CREATE PROCEDURE InsertOnly26 as upsert into KAFKAIMPORTTABLE26(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly26 ON TABLE Kafkaimporttable26 COLUMN key;

CREATE PROCEDURE InsertOnly27 as upsert into KAFKAIMPORTTABLE27(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly27 ON TABLE Kafkaimporttable27 COLUMN key;

CREATE PROCEDURE InsertOnly28 as upsert into KAFKAIMPORTTABLE28(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly28 ON TABLE Kafkaimporttable28 COLUMN key;

CREATE PROCEDURE InsertOnly29 as upsert into KAFKAIMPORTTABLE29(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly29 ON TABLE Kafkaimporttable29 COLUMN key;

CREATE PROCEDURE InsertOnly30 as upsert into KAFKAIMPORTTABLE30(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly30 ON TABLE Kafkaimporttable30 COLUMN key;

CREATE PROCEDURE InsertOnly31 as upsert into KAFKAIMPORTTABLE31(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly31 ON TABLE Kafkaimporttable31 COLUMN key;

CREATE PROCEDURE InsertOnly32 as upsert into KAFKAIMPORTTABLE32(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly32 ON TABLE Kafkaimporttable32 COLUMN key;

CREATE PROCEDURE InsertOnly33 as upsert into KAFKAIMPORTTABLE33(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly33 ON TABLE Kafkaimporttable33 COLUMN key;

CREATE PROCEDURE InsertOnly34 as upsert into KAFKAIMPORTTABLE34(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly34 ON TABLE Kafkaimporttable34 COLUMN key;

CREATE PROCEDURE InsertOnly35 as upsert into KAFKAIMPORTTABLE35(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly35 ON TABLE Kafkaimporttable35 COLUMN key;

CREATE PROCEDURE InsertOnly36 as upsert into KAFKAIMPORTTABLE36(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly36 ON TABLE Kafkaimporttable36 COLUMN key;

CREATE PROCEDURE InsertOnly37 as upsert into KAFKAIMPORTTABLE37(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly37 ON TABLE Kafkaimporttable37 COLUMN key;

CREATE PROCEDURE InsertOnly38 as upsert into KAFKAIMPORTTABLE38(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly38 ON TABLE Kafkaimporttable38 COLUMN key;

CREATE PROCEDURE InsertOnly39 as upsert into KAFKAIMPORTTABLE39(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly39 ON TABLE Kafkaimporttable39 COLUMN key;

CREATE PROCEDURE InsertOnly40 as upsert into KAFKAIMPORTTABLE40(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly40 ON TABLE Kafkaimporttable40 COLUMN key;

CREATE PROCEDURE InsertOnly41 as upsert into KAFKAIMPORTTABLE41(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly41 ON TABLE Kafkaimporttable41 COLUMN key;

CREATE PROCEDURE InsertOnly42 as upsert into KAFKAIMPORTTABLE42(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly42 ON TABLE Kafkaimporttable42 COLUMN key;

CREATE PROCEDURE InsertOnly43 as upsert into KAFKAIMPORTTABLE43(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly43 ON TABLE Kafkaimporttable43 COLUMN key;

CREATE PROCEDURE InsertOnly44 as upsert into KAFKAIMPORTTABLE44(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly44 ON TABLE Kafkaimporttable44 COLUMN key;

CREATE PROCEDURE InsertOnly45 as upsert into KAFKAIMPORTTABLE45(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly45 ON TABLE Kafkaimporttable45 COLUMN key;

CREATE PROCEDURE InsertOnly46 as upsert into KAFKAIMPORTTABLE46(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly46 ON TABLE Kafkaimporttable46 COLUMN key;

CREATE PROCEDURE InsertOnly47 as upsert into KAFKAIMPORTTABLE47(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly47 ON TABLE Kafkaimporttable47 COLUMN key;

CREATE PROCEDURE InsertOnly48 as upsert into KAFKAIMPORTTABLE48(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly48 ON TABLE Kafkaimporttable48 COLUMN key;

CREATE PROCEDURE InsertOnly49 as upsert into KAFKAIMPORTTABLE49(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly49 ON TABLE Kafkaimporttable49 COLUMN key;

END_OF_BATCH
