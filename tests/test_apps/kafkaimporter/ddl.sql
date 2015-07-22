-- This is the import table into which a single value will be pushed by kafkaimporter.

-- file -inlinebatch END_OF_BATCH

------- kafka Importer Table -------
CREATE TABLE importtable
     (
                  KEY   BIGINT NOT NULL ,
                  value BIGINT NOT NULL ,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_importtable PRIMARY KEY ( KEY )
     );
-- Partition on id
PARTITION TABLE importtable ON COLUMN KEY;

------- Kafka Importer Tables -------
CREATE TABLE kafkaimporttable1
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_import_table1 PRIMARY KEY ( KEY )
     );
-- Partition on id
PARTITION TABLE kafkaimporttable1 ON COLUMN KEY;

CREATE TABLE kafkamirrortable1
     (
                  KEY   BIGINT NOT NULL ,
                  value BIGINT NOT NULL ,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafkamirrortable PRIMARY KEY ( KEY )
     );

-- Partition on id
PARTITION TABLE kafkamirrortable1 ON COLUMN KEY;

-- Export table
CREATE TABLE kafkaexporttable1
     (
                  KEY   BIGINT NOT NULL ,
                  value BIGINT NOT NULL
     );

PARTITION TABLE kafkaexporttable1 ON COLUMN KEY;
EXPORT TABLE kafkaexporttable1;

-- Stored procedures
LOAD classes sp.jar;

CREATE PROCEDURE PARTITION ON TABLE KafkaImportTable1 COLUMN key FROM class kafkaimporter.db.procedures.InsertImport;
CREATE PROCEDURE PARTITION ON TABLE Kafkamirrortable1 COLUMN key FROM class kafkaimporter.db.procedures.InsertExport;
CREATE PROCEDURE PARTITION ON TABLE KafkaMirrorTable1 COLUMN key FROM class kafkaimporter.db.procedures.DeleteRows;

CREATE PROCEDURE FROM class kafkaimporter.db.procedures.InsertFinal;
CREATE PROCEDURE FROM class kafkaimporter.db.procedures.MatchRows;

CREATE PROCEDURE CountMirror as select count(*) from kafkamirrortable1;
CREATE PROCEDURE CountImport as select count(*) from kafkaimporttable1;

-- END_OF_BATCH
