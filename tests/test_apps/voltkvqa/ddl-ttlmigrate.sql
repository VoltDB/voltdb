LOAD CLASSES voltkv.jar;

CREATE TABLE store MIGRATE TO TARGET ABC1
(
  key      varchar(250) not null
, value    varbinary(1048576) not null
, inserttime     timestamp DEFAULT NOW not null
, PRIMARY KEY (key)
,
) USING TTL 5 MINUTES ON COLUMN inserttime;
PARTITION TABLE store ON COLUMN key;
CREATE INDEX inserttimeidx ON store (inserttime) WHERE NOT MIGRATING;

CREATE PROCEDURE FROM class voltkvqa.procedures.Initialize;
CREATE PROCEDURE PARTITION ON TABLE store COLUMN key FROM class voltkvqa.procedures.Get;
CREATE PROCEDURE PARTITION ON TABLE store COLUMN key FROM class voltkvqa.procedures.Put;
CREATE PROCEDURE PARTITION ON TABLE store COLUMN key FROM class voltkvqa.procedures.Remove;
CREATE PROCEDURE FROM class voltkvqa.procedures.GetMp;
CREATE PROCEDURE FROM class voltkvqa.procedures.PutMp;

