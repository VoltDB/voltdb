CREATE TABLE transactions
(
  txnid bigint     NOT NULL
, pid   tinyint NOT NULL
, CONSTRAINT PK_contestants PRIMARY KEY
  (
    txnid
  )
);
