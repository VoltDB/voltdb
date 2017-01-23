CREATE TABLE store
(
  key      bigint not null
, value    varbinary(1048576) not null
, PRIMARY KEY (key)
);

PARTITION TABLE store ON COLUMN key;

-- LOAD CLASSES overhead.jar;

CREATE PROCEDURE FROM CLASS overhead.procedures.NoArgs;
CREATE PROCEDURE FROM CLASS overhead.procedures.BinaryPayload;
CREATE PROCEDURE FROM CLASS overhead.procedures.NoArgsRW;
CREATE PROCEDURE FROM CLASS overhead.procedures.BinaryPayloadRW;


