-------------------- EXAMPLE SQL -----------------------------------------------
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
-- CREATE INDEX pan_example ON example_of_types (pan);
--
-- CREATE VIEW view_example AS
--  SELECT type, COUNT(*) AS records, SUM(balance)
--  FROM example_of_types
--  GROUP BY type;
--
-- CREATE PROCEDURE foo AS SELECT * FROM foo;
-- CREATE PROCEDURE PARTITION ON TABLE symbols COLUMN symbol [PARAMETER 0]
--  FROM CLASS procedures.UpsertSymbol;
--
---------------------------------------------------------------------------------

------------- REPLICATED TABLES --------------
-- traders
CREATE TABLE cnt (
  codcnt            INTEGER NOT NULL, -- trader ID
  CONSTRAINT pk_cnt PRIMARY KEY (codcnt)
);

-- exchanges
CREATE TABLE exc (
  codexc            INTEGER NOT NULL, -- exchange ID
  CONSTRAINT pk_exc PRIMARY KEY (codexc)
);

------------- PARTITIONED TABLES --------------

-- securities
CREATE TABLE sec (
  codsec            INTEGER NOT NULL, -- security ID
  codexc            INTEGER NOT NULL, -- exchange ID
  CONSTRAINT pk_sec PRIMARY KEY (codsec)
);
PARTITION TABLE sec ON COLUMN codsec;

-- position of a trader wrt a security (no expiration date)
CREATE TABLE pos (
  codcnt            INTEGER NOT NULL, -- trader ID
  codsec            INTEGER NOT NULL, -- security ID
  pos_cum_qty_ord   INTEGER NOT NULL, -- cumulative orders quantity
  pos_cum_qty_exe   INTEGER,          -- cumulative executed quantity
  pos_prc           FLOAT   NOT NULL, -- price to be used to calculate the value of a position
  pos_cum_val_ord   FLOAT   NOT NULL, -- cumulative value of orders
  pos_cum_val_exe   FLOAT,            -- cumulative value of executed quantities
  CONSTRAINT pk_pos PRIMARY KEY (codsec,codcnt)
);
PARTITION TABLE pos ON COLUMN codsec;

-- order in input to the system (erased at EOD)
CREATE TABLE ord (
  codord           INTEGER NOT NULL, -- order ID
  ord_cnt          INTEGER NOT NULL, -- trader ID
  ord_sec          INTEGER NOT NULL, -- security ID
  ord_qty          INTEGER NOT NULL, -- quantity of the order
  ord_prc          FLOAT   NOT NULL, -- price of the order
);
PARTITION TABLE ord ON COLUMN ord_sec;

-- trades in input to the system (erased at EOD)
CREATE TABLE trd (
  codtrd           INTEGER NOT NULL, -- trade ID
  trd_cnt          INTEGER NOT NULL, -- customer (trader?) ID
  trd_sec          INTEGER NOT NULL, -- security ID
  trd_qty          INTEGER NOT NULL, -- quantity executed
  trd_prc          FLOAT   NOT NULL, -- price of the trade
);
PARTITION TABLE trd ON COLUMN trd_sec;

-- prices in input to the system (erased at EOD)
CREATE TABLE prc (
  codprc           INTEGER   NOT NULL, -- ID of price
  prc_sec          INTEGER   NOT NULL, -- security ID
  prc_price        FLOAT     NOT NULL, -- price distributed by a market
  prc_ts           TIMESTAMP NOT NULL, -- timestamp of the price
);
PARTITION TABLE prc ON COLUMN prc_sec;

-------------- STORED PROCEDURES ------------------------

-- Update classes from jar to that server will know about classes but not procedures yet.
LOAD CLASSES positionkeeper-procs.jar;

-- CREATE PROCEDURE foo AS SELECT * FROM foo;
-- CREATE PROCEDURE PARTITION ON TABLE symbols COLUMN symbol
--   FROM CLASS procedures.UpsertSymbol;

CREATE PROCEDURE PARTITION ON TABLE ord COLUMN ord_sec PARAMETER 2
  FROM CLASS positionkeeper.OrderInsert;

CREATE PROCEDURE PARTITION ON TABLE trd COLUMN trd_sec PARAMETER 2
  FROM CLASS positionkeeper.TradeInsert;

CREATE PROCEDURE PARTITION ON TABLE prc COLUMN prc_sec PARAMETER 1
  FROM CLASS positionkeeper.PriceInsert;

CREATE PROCEDURE get_all_pos_for_sec
  PARTITION ON TABLE pos COLUMN codsec
  AS SELECT * FROM pos WHERE codsec = ?;

CREATE PROCEDURE get_all_pos_for_cnt
  AS SELECT * FROM pos WHERE codcnt = ?;

CREATE PROCEDURE get_all_ord_for_sec_cnt
  PARTITION ON TABLE ord COLUMN ord_sec
  AS SELECT * FROM ord WHERE ord_sec = ? AND ord_cnt = ?;

CREATE PROCEDURE get_all_trd_for_sec_cnt
  PARTITION ON TABLE trd COLUMN trd_sec
  AS SELECT * FROM trd WHERE trd_sec = ? AND trd_cnt = ?;

CREATE PROCEDURE get_pos_exe_val_for_sec_cnt
  PARTITION ON TABLE pos COLUMN codsec
  AS SELECT pos_cum_val_exe FROM pos WHERE codsec = ? AND codcnt = ?;

CREATE PROCEDURE get_pos_ord_val_for_sec_cnt
  PARTITION ON TABLE pos COLUMN codsec
  AS SELECT pos_cum_val_ord FROM pos WHERE codsec = ? AND codcnt = ?;
