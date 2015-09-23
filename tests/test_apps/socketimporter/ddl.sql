-- This is the import table into which a single value will be pushed by socketimporter.

file -inlinebatch END_OF_BATCH

-- schema is the style of kvbenchmark, varbin to be done when
-- importer supports it
CREATE TABLE importtable1
(
  key      varchar(250) not null
, value    varchar(1048576 BYTES) not null
, PRIMARY KEY (key)
);
-- Partition on key
PARTITION table importTable1 ON COLUMN key;

-- socketimporter classic schema
CREATE TABLE importTable2
(
key BIGINT NOT NULL
, value BIGINT NOT NULL
, CONSTRAINT PK_importTable2 PRIMARY KEY
  (
    key
  )
);
-- Partition on key
PARTITION table importTable2 ON COLUMN key;

END_OF_BATCH
