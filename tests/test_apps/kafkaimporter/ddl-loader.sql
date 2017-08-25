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

CREATE PROCEDURE CountImport2 as select count(*) from kafkaimporttable2;
CREATE PROCEDURE ImportCountMinMax as select count(key), min(key), max(key) from kafkaimporttable0;

CREATE PROCEDURE InsertOnly0 PARTITION ON TABLE KAFKAIMPORTTABLE0 COLUMN key as insert into KAFKAIMPORTTABLE0(key, value) VALUES(?, ?);
CREATE PROCEDURE InsertOnly1 PARTITION ON TABLE KAFKAIMPORTTABLE1 COLUMN key as insert into KAFKAIMPORTTABLE1(key, value) VALUES(?, ?);
CREATE PROCEDURE InsertOnly2 PARTITION ON TABLE KAFKAIMPORTTABLE2 COLUMN key as insert into KAFKAIMPORTTABLE2(key, value) VALUES(?, ?);
CREATE PROCEDURE InsertOnly3 PARTITION ON TABLE KAFKAIMPORTTABLE3 COLUMN key as insert into KAFKAIMPORTTABLE3(key, value) VALUES(?, ?);
CREATE PROCEDURE InsertOnly4 PARTITION ON TABLE KAFKAIMPORTTABLE4 COLUMN key as insert into KAFKAIMPORTTABLE4(key, value) VALUES(?, ?);

END_OF_BATCH
