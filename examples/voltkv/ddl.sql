CREATE TABLE store
(
  key      varchar(250) not null
, value    varbinary(1048576) not null
, PRIMARY KEY (key)
);

PARTITION TABLE store ON COLUMN key;

CREATE PROCEDURE FROM CLASS voltkv.procedures.Initialize;
CREATE PROCEDURE FROM CLASS voltkv.procedures.Get;
PARTITION PROCEDURE Get ON TABLE store COLUMN key;
CREATE PROCEDURE FROM CLASS voltkv.procedures.Put;
PARTITION PROCEDURE Put ON TABLE store COLUMN key;
CREATE PROCEDURE FROM CLASS voltkv.procedures.Remove;
PARTITION PROCEDURE Remove ON TABLE store COLUMN key;
