file -inlinebatch END_OF_BATCH

-- card account table
CREATE TABLE card_account(
  pan   		VARCHAR(16)    NOT NULL,
  card_available	INTEGER        NOT NULL, -- 1=ACTIVE, 0=INACTIVE
  card_status		VARCHAR(20)    NOT NULL, -- CREATED, PRINTED, WAREHOUSED, SHIPPED, ACTIVATED, REDEEMED, ZEROED, ARCHIVED
  balance               FLOAT          NOT NULL, -- ledger balance
  available_balance     FLOAT          NOT NULL, -- ledger balance - pre-authorized amount(s)
  currency              VARCHAR(3)     NOT NULL, -- ISO 4217 currency codes
  last_activity         TIMESTAMP      NOT NULL,
  CONSTRAINT PK_card_acct PRIMARY KEY ( pan )
);
PARTITION TABLE card_account ON COLUMN pan;

-- card activity table
CREATE TABLE card_activity(
  pan   		VARCHAR(16)    NOT NULL,
  date_time		TIMESTAMP      NOT NULL,
  activity_type         VARCHAR(8)     NOT NULL,
  cr_dr                 VARCHAR(1)     NOT NULL,
  amount                FLOAT          NOT NULL
);
PARTITION TABLE card_activity ON COLUMN pan;

CREATE INDEX IDX_card_activity_pan_date ON card_activity (pan, date_time);

END_OF_BATCH

-- Update classes from jar
LOAD CLASSES np-procs.jar;

-- stored procedures
CREATE PROCEDURE FROM CLASS np.Authorize;
PARTITION PROCEDURE Authorize ON TABLE card_activity COLUMN pan PARAMETER 0;

CREATE PROCEDURE FROM CLASS np.Redeem;
PARTITION PROCEDURE Redeem ON TABLE card_activity COLUMN pan PARAMETER 0;

CREATE PROCEDURE PARTITION ON TABLE card_activity COLUMN pan AND ON TABLE card_activity COLUMN pan FROM CLASS np.Transfer;

CREATE PROCEDURE FROM CLASS np.Select;
