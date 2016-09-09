CREATE TABLE Store
(
    keyspace VARBINARY(128)  NOT NULL
,   key      VARCHAR(128)    NOT NULL
,   value    VARBINARY(2056) NOT NULL
,   PRIMARY KEY (keyspace, key)
);
PARTITION TABLE Store ON COLUMN key;

-- Update classes from jar so that the server will know about classes, but not procedures yet.
LOAD CLASSES ycsb-procs.jar;

-- Batch load CREATE PROCEDURE statements
file -inlinebatch END_OF_BATCH

CREATE PROCEDURE FROM CLASS com.procedures.Put;
PARTITION PROCEDURE Put ON TABLE Store COLUMN key PARAMETER 1;

CREATE PROCEDURE FROM CLASS com.procedures.Scan;
PARTITION PROCEDURE Scan ON TABLE Store COLUMN key PARAMETER 1;

CREATE PROCEDURE Get AS
    SELECT value FROM Store WHERE keyspace = ? AND key = ?
;
PARTITION PROCEDURE Get ON TABLE Store COLUMN key PARAMETER 1;

END_OF_BATCH
