 -- This file is part of VoltDB.
 -- Copyright (C) 2008-2015 VoltDB Inc.

 -- This program is free software: you can redistribute it and/or modify
 -- it under the terms of the GNU Affero General Public License as
 -- published by the Free Software Foundation, either version 3 of the
 -- License, or (at your option) any later version.

 -- This program is distributed in the hope that it will be useful,
 -- but WITHOUT ANY WARRANTY; without even the implied warranty of
 -- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 -- GNU Affero General Public License for more details.

 -- You should have received a copy of the GNU Affero General Public License
 -- along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.


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
--
-- CREATE VIEW view_example AS
--  SELECT type, COUNT(*) AS records, SUM(balance)
--  FROM example_of_types
--  GROUP BY type;
--
-- CREATE PROCEDURE PARTITION ON TABLE symbols COLUMN symbol [PARAMETER 0]
--  FROM CLASS procedures.UpsertSymbol;
---------------------------------------------------------------------------------

-- Update classes from jar to that server will know about classes but not procedures yet.
LOAD CLASSES metrocard-procs.jar;

file -inlinebatch END_OF_BATCH

-------------- REPLICATED TABLES ------------------------------------------------
CREATE TABLE stations (
  station_id            SMALLINT          NOT NULL,
  name                  VARCHAR(25 BYTES) NOT NULL,
  fare                  SMALLINT          DEFAULT 250 NOT NULL,
  weight                INTEGER           NOT NULL,
  CONSTRAINT PK_stations PRIMARY KEY (station_id)
);

-------------- PARTITIONED TABLES -----------------------------------------------
CREATE TABLE cards(
  card_id               INTEGER        NOT NULL,
  enabled               TINYINT        DEFAULT 1 NOT NULL, -- 1=enabled, 0=disabled
  card_type             TINYINT        DEFAULT 0 NOT NULL, -- 0=pay per ride, 1=unlimited
  balance               INTEGER        DEFAULT 0, -- implicitly divide by 100 to get currency value
  expires               TIMESTAMP,
  name                  VARCHAR(50)    NOT NULL,
  phone                 VARCHAR(10)    NOT NULL, -- phone number, assumes North America
  email                 VARCHAR(50)    NOT NULL,
  notify                TINYINT           DEFAULT 0, -- 0=don't contact, 1=email, 2=text
  CONSTRAINT PK_metrocards_card_id PRIMARY KEY ( card_id )
);
PARTITION TABLE cards ON COLUMN card_id;

CREATE TABLE activity(
  card_id               INTEGER        NOT NULL,
  date_time             TIMESTAMP      NOT NULL,
  station_id            SMALLINT       NOT NULL,
  activity_code         TINYINT        NOT NULL, -- 1=entry, 2=purchase
  amount                INTEGER        NOT NULL
);
PARTITION TABLE activity ON COLUMN card_id;

CREATE STREAM card_alert_export PARTITION ON COLUMN card_id EXPORT TO TARGET alertstream (
  card_id               INTEGER        NOT NULL,
  export_time           BIGINT         NOT NULL,
  station_name          VARCHAR(25)    NOT NULL,
  name                  VARCHAR(50)    NOT NULL,
  phone                 VARCHAR(10)    NOT NULL, -- phone number, assumes North America
  email                 VARCHAR(50)    NOT NULL,
  notify                TINYINT           DEFAULT 0, -- 0=don't contact, 1=email, 2=text
  alert_message         VARCHAR(64)    NOT NULL
);

-------------- VIEWS ------------------------------------------------------------
CREATE VIEW secondly_entries_by_station
AS
SELECT
  TRUNCATE(SECOND,date_time) AS second,
  station_id,
  COUNT(*) AS activities,
  COUNT(DECODE(activity_code,1,1)) AS entries,
  SUM(DECODE(activity_code,1,amount)) AS entry_total,
  COUNT(DECODE(activity_code,2,1)) AS purchases,
  SUM(DECODE(activity_code,2,amount)) AS purchase_total
FROM activity
GROUP BY
  TRUNCATE(SECOND,date_time),
  station_id;


CREATE VIEW secondly_stats
AS
SELECT
  TRUNCATE(SECOND,date_time) AS second,
  COUNT(*) AS activities,
  COUNT(DECODE(activity_code,1,1)) AS entries,
  SUM(DECODE(activity_code,1,amount)) AS entry_total,
  COUNT(DECODE(activity_code,2,1)) AS purchases,
  SUM(DECODE(activity_code,2,amount)) AS purchase_total
FROM activity
GROUP BY
  TRUNCATE(SECOND,date_time);


-------------- PROCEDURES -------------------------------------------------------

CREATE PROCEDURE PARTITION ON TABLE cards COLUMN card_id PARAMETER 0 FROM CLASS metrocard.CardSwipe;
CREATE PROCEDURE FROM CLASS metrocard.GetBusiestStationInLastMinute;
CREATE PROCEDURE FROM CLASS metrocard.GetSwipesPerSecond;


CREATE PROCEDURE ReplenishCard PARTITION ON TABLE cards COLUMN card_id PARAMETER 1 AS
UPDATE cards SET balance = balance + ?
WHERE card_id = ? AND card_type = 0;

END_OF_BATCH
