-- partitioned table
CREATE TABLE partitioned
(
  txnid bigint             NOT NULL ASSUMEUNIQUE
, ts    bigint             NOT NULL
, cid   tinyint            NOT NULL
, rid   bigint             NOT NULL
, value varbinary(1048576) NOT NULL
, CONSTRAINT PK_txnid PRIMARY KEY
  (
    cid, txnid
  )
, UNIQUE ( cid, rid )
);
PARTITION TABLE partitioned ON COLUMN cid;

-- replicated table
CREATE TABLE replicated
(
  txnid  bigint NOT NULL
, ts     bigint NOT NULL
, rid    bigint NOT NULL
, cnt    bigint NOT NULL
, CONSTRAINT PK_id PRIMARY KEY
  (
    txnid
  )
, UNIQUE ( rid )
, UNIQUE ( cnt )
);

-- procedures
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.Initialize;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.doTxn;
PARTITION PROCEDURE doTxn ON TABLE partitioned COLUMN cid;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.updateReplicated;
CREATE PROCEDURE txnIdSelfCheck.procedures.getLastRow AS
       SELECT cid, MAX(-1 * rid) AS last_rid FROM partitioned GROUP BY cid;
CREATE PROCEDURE txnIdSelfCheck.procedures.getLastReplicatedRow AS
       SELECT rid FROM replicated;
