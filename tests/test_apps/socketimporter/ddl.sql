-- This is the import table into which a single value will be pushed by socketimporter.

file -inlinebatch END_OF_BATCH

-- schema is the style of kvbenchmark, varbin to be done when
-- importer supports it
CREATE TABLE partitioned 
(
  key      varchar(250) not null
, value    varchar(1048576 BYTES) not null
, insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
, PRIMARY KEY (key)
);
-- Partition on key
PARTITION table partitioned  ON COLUMN key;

CREATE TABLE replicated 
(
  key      varchar(250) not null
, value    varchar(1048576 BYTES) not null
, insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
, PRIMARY KEY (key)
);

CREATE PROCEDURE InsertReplicated as insert into REPLICATED(key, value) VALUES(?, ?);

CREATE PROCEDURE InsertPartitioned as insert into Partitioned(key, value) VALUES(?, ?);
PARTITION PROCEDURE InsertPartitioned ON TABLE Partitioned COLUMN key;

CREATE PROCEDURE SelectMaxTimeReplicated as select since_epoch(millis, max(insert_time)) from REPLICATED;

CREATE PROCEDURE SelectMaxTimePartitioned as select since_epoch(millis, max(insert_time)) from PARTITIONED;
PARTITION PROCEDURE SelectMaxTimePartitioned ON TABLE Partitioned COLUMN key;
END_OF_BATCH
