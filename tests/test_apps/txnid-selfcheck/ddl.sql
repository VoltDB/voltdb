-- partitioned table
CREATE TABLE transactions
(
  txnid bigint     NOT NULL
, pid   tinyint NOT NULL
, rid   bigint NOT NULL
, value varbinary(1048576) NOT NULL
, CONSTRAINT PK_txnid PRIMARY KEY
  (
    txnid
  )
, UNIQUE ( rid )
);

-- replicated table
CREATE TABLE replicated
(
  id bigint NOT NULL
, CONSTRAINT PK_id PRIMARY KEY
  (
    id
  )
);