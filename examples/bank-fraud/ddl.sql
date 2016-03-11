-------------------- EXAMPLE DDL SQL -------------------------------------------
-- CREATE TABLE example_of_types (
--   id              INTEGER NOT NULL, -- java int, 4-byte signed integer, -2,147,483,647 to 2,147,483,647
--   name            VARCHAR(40),      -- java String
--   data            VARBINARY(256),   -- java byte array
--   status          TINYINT,          -- java byte, 1-byte signed integer, -127 to 127
--   type            SMALLINT,         -- java short, 2-byte signed integer, -32,767 to 32,767
--   pan             BIGINT,           -- java long, 8-byte signed integer, -9,223,372,036,854,775,807 to 9,223,372,036,854,775,807
--   balance_open    FLOAT,            -- java double, 8-byte numeric
--   balance         DECIMAL,          -- java BigDecimal, 16-byte fixed scale of 12 and precision of 38
--   last_updated    TIMESTAMP,        -- java long, org.voltdb.types.TimestampType, 8-byte signed integer (milliseconds since epoch)
--   CONSTRAINT pk_example_of_types PRIMARY KEY (id)
-- );
-- PARTITION TABLE example_of_types ON COLUMN id;
-- CREATE INDEX idx_example_pan ON example_of_types (pan);
--
-- CREATE VIEW view_example AS
--  SELECT type, COUNT(*) AS records, SUM(balance)
--  FROM example_of_types
--  GROUP BY type;
--
-- CREATE PROCEDURE foo AS SELECT * FROM foo;
-- CREATE PROCEDURE FROM CLASS procedures.UpsertSymbol;
-- PARTITION PROCEDURE UpsertSymbol ON TABLE symbols COLUMN symbol PARAMETER 0;
---------------------------------------------------------------------------------

----- REPLICATED TABLES -----------------

CREATE TABLE customer(
  cust_id BIGINT NOT NULL,
  cust_first_name VARCHAR(50) NOT NULL,
  cust_last_name VARCHAR(50) NOT NULL,
  cust_city VARCHAR(50) NOT NULL,
  cust_state VARCHAR(2) NOT NULL,
  cust_phn_no VARCHAR(20) NOT NULL,
  cust_dob TIMESTAMP NOT NULL,
  cust_gender VARCHAR(1) NOT NULL,
  PRIMARY KEY (cust_id)
);

CREATE TABLE rules(
  rule_id INTEGER NOT NULL,
  rule_desc VARCHAR(200) NOT NULL,
  PRIMARY KEY (rule_id)
);

----- PARTITIONED TABLES -----------------


CREATE TABLE account(
  acc_no BIGINT NOT NULL,
  cust_id BIGINT NOT NULL,
  acc_balance FLOAT NOT NULL,
  acc_credit_line FLOAT NOT NULL,
  acc_open_date TIMESTAMP NOT NULL,
  acc_fraud_privilege VARCHAR(1) NOT NULL,
  PRIMARY KEY (acc_no)
);
PARTITION TABLE account ON COLUMN acc_no;

CREATE TABLE transaction(
  txn_id BIGINT NOT NULL,
  acc_no BIGINT  NOT NULL,
  txn_amt FLOAT NOT NULL,
  txn_state VARCHAR(5) NOT NULL,
  txn_city VARCHAR(50) NOT NULL,
  txn_ts TIMESTAMP  NOT NULL,
  --txn_dt VARCHAR(10) NOT NULL,
  txn_status VARCHAR(20) NOT NULL,
  PRIMARY KEY (acc_no, txn_ts, txn_id)
);
PARTITION TABLE transaction ON COLUMN acc_no;

--CREATE INDEX idx_transaction ON transaction (acc_no, txn_dt);

CREATE TABLE account_fraud(
  acc_no BIGINT NOT NULL,
  rule_id INTEGER NOT NULL,
  fraud_status_online VARCHAR(1) DEFAULT 'N',
  fraud_status_offline VARCHAR(1) DEFAULT 'N',
  fraud_ts TIMESTAMP NOT NULL,
  PRIMARY KEY (acc_no)
);
PARTITION TABLE account_fraud ON COLUMN acc_no;

--CREATE INDEX idx_account_fraud_ts ON account_fraud (fraud_ts);

--IMPORT CLASS procedures.PerformanceTimer;


--------- VIEWS ---------------------------

CREATE VIEW transaction_vw (
  acc_no,
  txn_dt,
  threshold,
  total_txn_amt
) AS
SELECT
  acc_no,
  TRUNCATE(DAY,txn_ts),
  COUNT(*),
  SUM(txn_amt)
FROM transaction
GROUP BY acc_no,TRUNCATE(DAY,txn_ts);


--------- PROCEDURES ----------------------

-- Update classes from jar to that server will know about classes but not procedures yet.
LOAD CLASSES bankfraud-procs.jar;

CREATE PROCEDURE FROM CLASS bankfraud.DetectFraud;
PARTITION PROCEDURE DetectFraud ON TABLE transaction COLUMN acc_no PARAMETER 1;

CREATE PROCEDURE FraudFirst50 AS
SELECT f.acc_no, f.fraud_ts, f.rule_id
FROM account_fraud f
ORDER BY f.FRAUD_TS LIMIT 50;

CREATE PROCEDURE GetAcct AS
SELECT c.cust_id, a.acc_no, c.cust_first_name, c.cust_last_name, c.cust_city, c.cust_state, a.acc_balance, a.acc_open_date
FROM account a
INNER JOIN customer c ON a.cust_id = c.cust_id
WHERE a.acc_no = ?;
PARTITION PROCEDURE GetAcct ON TABLE account COLUMN acc_no PARAMETER 0;

CREATE PROCEDURE GetTransactions AS
SELECT *
FROM transaction
WHERE acc_no = ?
ORDER BY txn_ts;
PARTITION PROCEDURE GetTransactions ON TABLE transaction COLUMN acc_no PARAMETER 0;

