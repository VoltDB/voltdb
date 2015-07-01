CREATE TABLE store
(
  key      varchar(250) not null
, value    varbinary(1048576) not null
, PRIMARY KEY (key)
);

PARTITION TABLE store ON COLUMN key;

CREATE PROCEDURE FROM CLASS oneshotkv.procedures.Initialize;
CREATE PROCEDURE FROM CLASS oneshotkv.procedures.Get;
CREATE PROCEDURE FROM CLASS oneshotkv.procedures.GetMP;
CREATE PROCEDURE FROM CLASS oneshotkv.procedures.Put;
CREATE PROCEDURE FROM CLASS oneshotkv.procedures.PutsMP;
CREATE PROCEDURE FROM CLASS oneshotkv.procedures.OptimisticPutsMP;
CREATE PROCEDURE FROM CLASS oneshotkv.procedures.Remove;
