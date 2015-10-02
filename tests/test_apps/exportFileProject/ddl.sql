
LOAD classes sp.jar;

file -inlinebatch END_OF_BATCH

------- 
CREATE TABLE kvtable
     (
                  KEY   BIGINT NOT NULL,
                  value BIGINT NOT NULL,
                  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                  CONSTRAINT pk_kvtable PRIMARY KEY ( KEY )
     );

PARTITION TABLE kvtable ON COLUMN KEY;

-- Export table
CREATE TABLE kvexporttable
     (
                  KEY   BIGINT NOT NULL ,
                  value BIGINT NOT NULL
     );

PARTITION TABLE kvexporttable ON COLUMN KEY;
EXPORT TABLE kvexporttable TO STREAM stuff;

CREATE PROCEDURE PARTITION ON TABLE kvtable COLUMN key FROM class exportFileProject.db.procedures.InsertExport;

END_OF_BATCH
