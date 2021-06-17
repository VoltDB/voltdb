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

CREATE TABLE kafkamirrortable1
     (
                  KEY   BIGINT NOT NULL ,
                  value BIGINT NOT NULL ,
                  import_count BIGINT DEFAULT 0,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafkamirrortable PRIMARY KEY ( KEY )
     );

PARTITION TABLE kafkamirrortable1 ON COLUMN KEY;

CREATE TABLE importNoMatch
     (
                  KEY   BIGINT NOT NULL ,
                  value BIGINT NOT NULL ,
                  streamNumber  BIGINT DEFAULT 0,
                  CONSTRAINT pk_importNoMatch PRIMARY KEY ( KEY )
     );

PARTITION TABLE importNoMatch ON COLUMN KEY;

-- Export table
CREATE STREAM KAFKAEXPORTTABLE1 PARTITION ON COLUMN KEY EXPORT TO TOPIC KAFKAEXPORTTABLE1 WITH KEY (KEY) (
                  KEY   BIGINT NOT NULL ,
                  value BIGINT NOT NULL
     );

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

CREATE PROCEDURE PARTITION ON TABLE Kafkamirrortable1 COLUMN key FROM class kafkaimporter.db.procedures.InsertExport;

CREATE PROCEDURE FROM class kafkaimporter.db.procedures.InsertFinal;
CREATE PROCEDURE FROM class kafkaimporter.db.procedures.MatchRows;

CREATE PROCEDURE PARTITION ON TABLE KAFKAIMPORTTABLE1 COLUMN key FROM class kafkaimporter.db.procedures.InsertImportWithCount1;
CREATE PROCEDURE PARTITION ON TABLE KAFKAIMPORTTABLE2 COLUMN key FROM class kafkaimporter.db.procedures.InsertImportWithCount2;
CREATE PROCEDURE PARTITION ON TABLE KAFKAIMPORTTABLE3 COLUMN key FROM class kafkaimporter.db.procedures.InsertImportWithCount3;
CREATE PROCEDURE PARTITION ON TABLE KAFKAIMPORTTABLE4 COLUMN key FROM class kafkaimporter.db.procedures.InsertImportWithCount4;

CREATE PROCEDURE CountMirror1 as select count(*) from kafkamirrortable1;
CREATE PROCEDURE CountImport1 as select count(*) from kafkaimporttable1;

CREATE PROCEDURE ImportCountMinMax as select count(key), min(key), max(key) from kafkaimporttable1;
CREATE PROCEDURE InsertOnly1 PARTITION ON TABLE KAFKAIMPORTTABLE1 COLUMN key as upsert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
CREATE PROCEDURE InsertOnly2 PARTITION ON TABLE KAFKAIMPORTTABLE2 COLUMN key as upsert into KAFKAIMPORTTABLE2(key, value) VALUES(?, ?);
CREATE PROCEDURE InsertOnly3 PARTITION ON TABLE KAFKAIMPORTTABLE3 COLUMN key as upsert into KAFKAIMPORTTABLE3(key, value) VALUES(?, ?);
CREATE PROCEDURE InsertOnly4 PARTITION ON TABLE KAFKAIMPORTTABLE4 COLUMN key as upsert into KAFKAIMPORTTABLE4(key, value) VALUES(?, ?);

END_OF_BATCH
