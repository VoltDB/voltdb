CREATE TABLE store
(
  key      bigint not null
, value    varbinary(1048576) not null
, PRIMARY KEY (key)
);
PARTITION TABLE store ON COLUMN key;

-- make sure to load the Java code for the procedures, before creating them
LOAD CLASSES overhead.jar;

CREATE PROCEDURE PARTITION ON TABLE STORE COLUMN KEY FROM CLASS overhead.procedures.NoArgs;
CREATE PROCEDURE PARTITION ON TABLE STORE COLUMN KEY FROM CLASS overhead.procedures.BinaryPayload;
CREATE PROCEDURE PARTITION ON TABLE STORE COLUMN KEY FROM CLASS overhead.procedures.NoArgsRW;
CREATE PROCEDURE PARTITION ON TABLE STORE COLUMN KEY FROM CLASS overhead.procedures.BinaryPayloadRW;
