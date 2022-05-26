-- This file is part of VoltDB.
-- Copyright (C) 2008-2022 Volt Active Data Inc.
--
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
-- CREATE INDEX idx_example ON example_of_types (type,balance);
--
-- CREATE VIEW view_example AS
--  SELECT type, COUNT(*) AS records, SUM(balance)
--  FROM example_of_types
--  GROUP BY type;
--
-- CREATE PROCEDURE select_example
--  PARTITION ON TABLE example_of_types COLUMN id [PARAMETER 0]
--  AS SELECT name, status FROM example_of_types WHERE id = ?;
--
-- CREATE PROCEDURE PARTITION ON TABLE symbols COLUMN symbol PARAMETER 0
--  FROM CLASS procedures.UpsertSymbol;
---------------------------------------------------------------------------------
file -inlinebatch END_BATCH

CREATE STREAM ticks PARTITION ON COLUMN symbol (
  symbol                    VARCHAR(16) NOT NULL,
  time                      TIMESTAMP NOT NULL,
  seq                       BIGINT NOT NULL,
  exch                      VARCHAR(2) NOT NULL,
  bid                       INTEGER,
  bid_size                  INTEGER,
  ask                       INTEGER,
  ask_size                  INTEGER
);

CREATE TABLE last_ticks (
  symbol                    VARCHAR(16) NOT NULL,
  time                      TIMESTAMP NOT NULL,
  seq                       BIGINT NOT NULL,
  exch                      VARCHAR(2) NOT NULL,
  bid                       INTEGER,
  bid_size                  INTEGER,
  ask                       INTEGER,
  ask_size                  INTEGER,
  CONSTRAINT pk_last_ticks PRIMARY KEY (symbol, exch)
);
PARTITION TABLE last_ticks ON COLUMN symbol;
CREATE INDEX idx_last_ticks_bid ON last_ticks (symbol,bid,seq);
CREATE INDEX idx_last_ticks_ask ON last_ticks (symbol,ask,seq);

CREATE TABLE nbbos (
  symbol                    VARCHAR(16) NOT NULL,
  time                      TIMESTAMP NOT NULL,
  seq                       BIGINT,
  bid                       INTEGER,
  bsize                     INTEGER,
  bid_exch                  VARCHAR(2),
  ask                       INTEGER,
  asize                     INTEGER,
  ask_exch                  VARCHAR(2),
  CONSTRAINT pk_nbbos PRIMARY KEY (symbol, time, seq)
);
PARTITION TABLE nbbos ON COLUMN symbol;

CREATE PROCEDURE nbbo_last_symbol
  PARTITION ON TABLE nbbos COLUMN symbol
  AS SELECT * FROM nbbos WHERE symbol = ? ORDER BY time desc LIMIT 1;

CREATE PROCEDURE nbbo_last_bid_symbol
  PARTITION ON TABLE nbbos COLUMN symbol
  AS SELECT bid, bsize, bid_exch, time FROM nbbos WHERE symbol = ? ORDER BY time desc LIMIT 1;

CREATE PROCEDURE nbbo_last_ask_symbol
  PARTITION ON TABLE nbbos COLUMN symbol
  AS SELECT ask, asize, ask_exch, time FROM nbbos WHERE symbol = ? ORDER BY time desc LIMIT 1;

CREATE PROCEDURE nbbo_hist_symbol
  PARTITION ON TABLE nbbos COLUMN symbol
  AS SELECT * FROM nbbos WHERE symbol = ? and time > TO_TIMESTAMP(Second,SINCE_EPOCH(Second,NOW)-60*5) ORDER BY time desc;

CREATE PROCEDURE last_ticks_symbol
  PARTITION ON TABLE last_ticks COLUMN symbol
  AS SELECT * FROM last_ticks WHERE symbol = ? ORDER BY exch;

CREATE PROCEDURE last_bids_symbol
  PARTITION ON TABLE last_ticks COLUMN symbol
  AS SELECT time, exch, bid_size, bid FROM last_ticks WHERE symbol = ? ORDER BY bid desc;

CREATE PROCEDURE last_asks_symbol
  PARTITION ON TABLE last_ticks COLUMN symbol
  AS SELECT time, exch, ask_size, ask FROM last_ticks WHERE symbol = ? ORDER BY ask asc;

END_BATCH

-- Update classes from jar to that server will know about classes but not procedures yet.
LOAD CLASSES nbbo-procs.jar;

CREATE PROCEDURE PARTITION ON TABLE ticks COLUMN symbol
  FROM CLASS nbbo.ProcessTick;

