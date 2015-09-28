-- This is the import table into which a single value will be pushed by socketimporter.
file -inlinebatch END_OF_BATCH

-- schema is the style of kvbenchmark, varbin to be done when
-- importer supports it
CREATE TABLE importtable (
  key      varchar(250) not null
, value    varchar(1048576 BYTES) not null
, insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
, PRIMARY KEY (key)
);
-- Partition on key
PARTITION table importtable  ON COLUMN key;

CREATE PROCEDURE InsertOnly as insert into importtable(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertOnly ON TABLE importtable COLUMN key;

CREATE PROCEDURE SelectMaxTime as select since_epoch(millis, max(insert_time)) from importtable;

END_OF_BATCH
