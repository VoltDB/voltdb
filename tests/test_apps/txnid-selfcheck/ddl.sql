-- Tell sqlcmd to batch the following commands together,
-- so that the schema loads quickly.
file -inlinebatch END_OF_BATCH

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

END_OF_BATCH

LOAD CLASSES txnid-procs.jar;

-- The following CREATE PROCEDURE statements can all be batched.
file -inlinebatch END_OF_2ND_BATCH

-- procedures
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.Initialize;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.doTxn;
PARTITION PROCEDURE doTxn ON TABLE partitioned COLUMN cid;
CREATE PROCEDURE FROM CLASS txnIdSelfCheck.procedures.updateReplicated;
CREATE PROCEDURE getLastRow AS
       SELECT cid, MAX(-1 * rid) AS last_rid FROM partitioned GROUP BY cid;
CREATE PROCEDURE getLastReplicatedRow AS
       SELECT rid FROM replicated;

END_OF_2ND_BATCH
