CREATE TABLE store
(
  key      varchar(250) not null
, value    varbinary(1048576) not null
, PRIMARY KEY (key)
);
PARTITION TABLE store ON COLUMN key;

-- make sure to load the Java code for the procedures, before creating them
LOAD CLASSES oneshotkv.jar;

CREATE PROCEDURE FROM CLASS oneshotkv.procedures.Initialize;
CREATE PROCEDURE PARTITION ON TABLE STORE COLUMN KEY FROM CLASS oneshotkv.procedures.Get;
CREATE PROCEDURE FROM CLASS oneshotkv.procedures.GetMP;
CREATE PROCEDURE PARTITION ON TABLE STORE COLUMN KEY FROM CLASS oneshotkv.procedures.Put;
CREATE PROCEDURE FROM CLASS oneshotkv.procedures.PutsMP;
CREATE PROCEDURE FROM CLASS oneshotkv.procedures.OptimisticPutsMP;
CREATE PROCEDURE PARTITION ON TABLE STORE COLUMN KEY FROM CLASS oneshotkv.procedures.Remove;
