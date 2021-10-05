LOAD CLASSES kvbenchmark.jar;

file -inlinebatch END_OF_BATCH
CREATE TABLE store
(
  key      varchar(250) not null
, value    varbinary(1048576) not null
, PRIMARY KEY (key)
);

PARTITION TABLE store ON COLUMN key;

CREATE PROCEDURE FROM CLASS kvbench.procedures.Initialize;
CREATE PROCEDURE FROM CLASS kvbench.procedures.Get;
PARTITION PROCEDURE Get ON TABLE store COLUMN key;
CREATE PROCEDURE FROM CLASS kvbench.procedures.Put;
PARTITION PROCEDURE Put ON TABLE store COLUMN key;
CREATE PROCEDURE FROM CLASS kvbench.procedures.Remove;
PARTITION PROCEDURE Remove ON TABLE store COLUMN key;

-- A table the workload ignores that tests can use as they'd like
CREATE TABLE extras
(
  key     VARCHAR(250) UNIQUE NOT NULL
, value   INTEGER
, tcreate TIMESTAMP DEFAULT NOW NOT NULL
, tupdate TIMESTAMP DEFAULT NOW NOT NULL
, PRIMARY KEY (key)
);
PARTITION TABLE extras ON COLUMN key;

END_OF_BATCH
