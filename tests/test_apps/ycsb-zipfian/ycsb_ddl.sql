CREATE TABLE Store
(
    keyspace VARBINARY(128)  NOT NULL
,   key      VARCHAR(128)    NOT NULL
,   value    VARBINARY(2056) NOT NULL
,   PRIMARY KEY (keyspace, key)
);
-- 10m row loaded, 100m operations
-- assuming 3 hosts, 8 partitions, k-factor 0,
-- 0.4m rows per partition, with 40k in memory (10% data), 2k (5% in memory data) per eviction
PARTITION TABLE Store ON COLUMN key;

CREATE PROCEDURE PARTITION ON TABLE Store COLUMN key PARAMETER 1 FROM CLASS com.procedures.Put;

CREATE PROCEDURE PARTITION ON TABLE Store COLUMN key PARAMETER 1 FROM CLASS com.procedures.Scan;

CREATE PROCEDURE Get PARTITION ON TABLE Store COLUMN key PARAMETER 1 AS
    SELECT value FROM Store WHERE keyspace = ? AND key = ?;
