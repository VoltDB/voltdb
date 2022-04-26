-- DDL for the delete-update-snapshot-benchmark.  Note that 'DUSB'
-- is short for 'delete-update-snapshot-benchmark'.

-- Load all the classes from the jar, including the procedures that will
-- also need to be 'CREATE-ed' below
LOAD CLASSES dusbench.jar;

file -inlinebatch END_OF_BATCH1

-- A table partitioned on the BLOCK_ID column
CREATE TABLE PARTITIONED (
  ID                 BIGINT NOT NULL,
  BLOCK_ID           BIGINT NOT NULL,
  MOD_ID             BIGINT,
  TINY               TINYINT,
  SMALL              SMALLINT,
  INTEG              INTEGER,
  BIG                BIGINT,
  FLOT               FLOAT,
  DECML              DECIMAL,
  TIMESTMP           TIMESTAMP,
  VCHAR_INLINE       VARCHAR(14),
  VCHAR_INLINE_MAX   VARCHAR(63 BYTES),
  VCHAR_OUTLINE_MIN  VARCHAR(64 BYTES),
  VCHAR_OUTLINE      VARCHAR(20),
  VCHAR_DEFAULT      VARCHAR,
  VARBIN_INLINE      VARBINARY(32),
  VARBIN_INLINE_MAX  VARBINARY(63),
  VARBIN_OUTLINE_MIN VARBINARY(64),
  VARBIN_OUTLINE     VARBINARY(128),
  VARBIN_DEFAULT     VARBINARY,
  POINT              GEOGRAPHY_POINT,
  POLYGON            GEOGRAPHY,  -- TODO: GEOGRAPHY(size),
  PRIMARY KEY (ID, BLOCK_ID)
);
PARTITION TABLE PARTITIONED ON COLUMN BLOCK_ID;
CREATE INDEX IDX_PARTITIONED_ID      ON PARTITIONED (ID);
CREATE INDEX IDX_PARTITIONED_BLOCKID ON PARTITIONED (BLOCK_ID);

CREATE PROCEDURE PARTITION ON TABLE PARTITIONED COLUMN BLOCK_ID
                 FROM CLASS procedures.InsertByBlock;
CREATE PROCEDURE PARTITION ON TABLE PARTITIONED COLUMN BLOCK_ID
                 FROM CLASS procedures.DeleteByBlock;
CREATE PROCEDURE PARTITION ON TABLE PARTITIONED COLUMN BLOCK_ID
                 FROM CLASS procedures.UpdateByBlock;

END_OF_BATCH1
