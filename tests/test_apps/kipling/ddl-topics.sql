-- This is the import table into which a single value will be pushed by kafkaimporter.

LOAD classes sp.jar;

file -inlinebatch END_OF_BATCH


CREATE STREAM  T8_KAFKAEXPORTTABLE1 partition on column key as topic
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_export_table1 PRIMARY KEY ( KEY )
     );


END_OF_BATCH
