CREATE TABLE store
(
  key      varchar(250) not null
, value    varbinary(1048576) not null
, PRIMARY KEY (key)
);

PARTITION TABLE store ON COLUMN key;

-- Update classes from jar so that the server will know about classes
-- but not procedures yet.
-- This command cannot be part of a DDL batch.
LOAD CLASSES voltkv-procs.jar;

CREATE PROCEDURE FROM CLASS voltkv.MPGet;
CREATE PROCEDURE FROM CLASS voltkv.MPPut;
