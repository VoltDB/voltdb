CREATE STREAM  T8_KAFKAEXPORTTABLENUM partition on column key as topic
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kafka_export_tableNUM PRIMARY KEY ( KEY )
     );

