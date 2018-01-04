LOAD CLASSES kvbenchmark.jar;

CREATE TABLE store
(
  key      varchar(250) not null
, value    varbinary(1048576) not null
, PRIMARY KEY (key)
);

PARTITION TABLE store ON COLUMN key;

CREATE PROCEDURE FROM CLASS kvbench.procedures.Initialize;
CREATE PROCEDURE PARTITION ON TABLE store COLUMN key FROM CLASS kvbench.procedures.Get;
CREATE PROCEDURE PARTITION ON TABLE store COLUMN key FROM CLASS kvbench.procedures.Put;
CREATE PROCEDURE PARTITION ON TABLE store COLUMN key FROM CLASS kvbench.procedures.Remove;
