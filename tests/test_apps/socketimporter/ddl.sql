-- This is the import table into which a single value will be pushed by socketimporter.

-- file -inlinebatch END_OF_BATCH

------- Socket Importer Table -------
CREATE TABLE importTable (
key BIGINT NOT NULL
, value BIGINT NOT NULL
, CONSTRAINT PK_importTable PRIMARY KEY
  (
    key
  )
);
-- Partition on id
PARTITION table importTable ON COLUMN key;


------- Kafka Importer Tables -------
create table KafkaImportTable1 (
key BIGINT NOT NULL
, value BIGINT NOT NULL
, CONSTRAINT PK_kafka_import_table1 PRIMARY KEY
  (
    key
  )
);
-- Partition on id
PARTITION table KafkaImportTable1 ON COLUMN key;


create table kafkaMirrorTable1 (
key BIGINT NOT NULL
, value BIGINT NOT NULL
, CONSTRAINT PK_kafkaMirrorTable PRIMARY KEY
  (
    key
  )
);
-- Partition on id
PARTITION table kafkaMirrorTable1 ON COLUMN key;


-- Export table

create table kafkaExportTable1 (
key BIGINT NOT NULL
, value BIGINT NOT NULL
);

export table kafkaExportTable1;

-- Stored procedures
load classes sp.jar;
create procedure from class socketimporter.db.procedures.InsertExport;
create procedure from class socketimporter.db.procedures.MatchRows;

-- END_OF_BATCH
