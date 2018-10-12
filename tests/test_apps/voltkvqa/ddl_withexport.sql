LOAD CLASSES voltkv.jar;

CREATE TABLE store
(
  key      varchar(250) not null
, value    varbinary(1048576) not null
, PRIMARY KEY (key)
);
PARTITION TABLE store ON COLUMN key;

CREATE STREAM store_export PARTITION ON COLUMN key EXPORT TO TARGET tsvout (
  key      varchar(250) not null
, value    varbinary(1040000) not null
, stamp timestamp not null
, txnid bigint not null
, random float not null
);

CREATE PROCEDURE FROM class voltkvqa.procedures.Initialize;
CREATE PROCEDURE PARTITION ON TABLE store COLUMN key FROM class voltkvqa.procedures.Get;
CREATE PROCEDURE PARTITION ON TABLE store COLUMN key FROM class voltkvqa.procedures_withexport.Put;
CREATE PROCEDURE PARTITION ON TABLE store COLUMN key FROM class voltkvqa.procedures.Remove;
CREATE PROCEDURE FROM class voltkvqa.procedures.GetMp;
CREATE PROCEDURE FROM class voltkvqa.procedures.PutMp;
