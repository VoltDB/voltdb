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
