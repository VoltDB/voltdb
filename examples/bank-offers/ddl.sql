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
-- CREATE PROCEDURE PARTITION ON TABLE symbols COLUMN symbol [PARAMETER 0]
--  FROM CLASS procedures.UpsertSymbol;
---------------------------------------------------------------------------------

-- Update classes from jar to that server will know about classes but not procedures yet.
LOAD CLASSES bankoffers-procs.jar;

file -inlinebatch END_OF_BATCH

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

CREATE TABLE vendor_offers(
  vendor_id INTEGER NOT NULL,
  offer_priority SMALLINT NOT NULL,
  out_of_state TINYINT,
  min_visits INTEGER,
  min_spend FLOAT,
  avg_spend FLOAT,
  offer_text VARCHAR(200)
);
CREATE INDEX idx_vendor_offers ON vendor_offers (vendor_id,offer_priority);

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

CREATE TABLE activity(
  txn_id BIGINT NOT NULL,
  acc_no BIGINT  NOT NULL,
  txn_amt FLOAT NOT NULL,
  txn_state VARCHAR(5) NOT NULL,
  txn_city VARCHAR(50) NOT NULL,
  txn_ts TIMESTAMP  NOT NULL,
  vendor_id INTEGER,
  PRIMARY KEY (acc_no, txn_ts, txn_id)
);
PARTITION TABLE activity ON COLUMN acc_no;

CREATE TABLE offers_given(
  acc_no BIGINT NOT NULL,
  vendor_id INTEGER,
  offer_ts TIMESTAMP NOT NULL,
  offer_text VARCHAR(200)
);
PARTITION TABLE offers_given ON COLUMN acc_no;
CREATE INDEX idx_offers_given ON offers_given (offer_ts);

CREATE STREAM offers_given_export PARTITION ON COLUMN acc_no EXPORT TO TARGET table_test (
  acc_no BIGINT NOT NULL,
  vendor_id INTEGER,
  offer_ts TIMESTAMP NOT NULL,
  offer_text VARCHAR(200)
);

--------- VIEWS ---------------------------

CREATE VIEW acct_vendor_totals AS
SELECT
  acc_no,
  vendor_id,
  COUNT(*) as total_visits,
  SUM(txn_amt) as total_spend
FROM activity
GROUP BY acc_no, vendor_id;


CREATE VIEW total_offers AS
SELECT TRUNCATE(SECOND,offer_ts) AS offer_ts, acc_no, COUNT(*) as total_offers
FROM offers_given_export
GROUP BY TRUNCATE(SECOND,offer_ts), acc_no;

--------- PROCEDURES ----------------------

CREATE PROCEDURE recent_offer_totals AS
SELECT * FROM total_offers ORDER BY offer_ts DESC LIMIT 60;

CREATE PROCEDURE PARTITION ON TABLE activity COLUMN acc_no PARAMETER 1
  FROM CLASS bankoffers.CheckForOffers;

CREATE PROCEDURE RecentOffersList AS
SELECT og.offer_ts, c.cust_first_name, c.cust_last_name, og.offer_text, avt.acc_no, avt.vendor_id, avt.total_visits, avt.total_spend
FROM offers_given og
INNER JOIN acct_vendor_totals avt on avt.acc_no = og.acc_no AND avt.vendor_id = og.vendor_id
INNER JOIN account a on avt.acc_no = a.acc_no
INNER JOIN customer c on a.cust_id = c.cust_id
ORDER BY og.offer_ts desc
LIMIT 10;

END_OF_BATCH
