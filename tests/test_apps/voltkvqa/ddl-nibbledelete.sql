LOAD CLASSES voltkv.jar;

CREATE TABLE store
(
  key      varchar(250) not null
, value    varbinary(1048576) not null
, inserttime     timestamp DEFAULT NOW
, PRIMARY KEY (key)
,
);
PARTITION TABLE store ON COLUMN key;

CREATE INDEX inserttimeidx ON store ( inserttime );
CREATE PROCEDURE FROM class voltkvqa.procedures.Initialize;
CREATE PROCEDURE PARTITION ON TABLE store COLUMN key FROM class voltkvqa.procedures.Get;
CREATE PROCEDURE PARTITION ON TABLE store COLUMN key FROM class voltkvqa.procedures.Put;
CREATE PROCEDURE PARTITION ON TABLE store COLUMN key FROM class voltkvqa.procedures.Remove;
CREATE PROCEDURE FROM class voltkvqa.procedures.GetMp;
CREATE PROCEDURE FROM class voltkvqa.procedures.PutMp;

