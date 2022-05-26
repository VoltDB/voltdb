 -- This file is part of VoltDB.
 -- Copyright (C) 2008-2022 Volt Active Data Inc.

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

-------------- REPLICATED TABLES ------------------------------------------------

CREATE TABLE inventory (
  inventory_id           INTEGER       NOT NULL,
  site_id                INTEGER       NOT NULL,
  page_id                INTEGER       NOT NULL,
  site_directory         VARCHAR(256)  NOT NULL,
  page_url               VARCHAR(2000 BYTES)     NOT NULL,
  CONSTRAINT pk_inventory PRIMARY KEY (inventory_id)
);

CREATE TABLE creatives (
  creative_id            INTEGER       NOT NULL,
  campaign_id            INTEGER       NOT NULL,
  advertiser_id          INTEGER       NOT NULL,
  creative_image_url     VARCHAR(2000 BYTES)     NOT NULL,
  creative_name          VARCHAR(256)  NOT NULL,
  creative_description   VARCHAR(256)  NOT NULL,
  CONSTRAINT pk_creatives PRIMARY KEY (creative_id)
);

-------------- PARTITIONED TABLES ----------------------------------------------

CREATE TABLE event_data (
  utc_time               TIMESTAMP     NOT NULL,
  ip_address             BIGINT        NOT NULL,
  cookie_uid             BIGINT,
  creative_id            INTEGER       NOT NULL,
  inventory_id           INTEGER       NOT NULL,
  type_id                INTEGER       NOT NULL,
  cost                   DECIMAL,
  -- derived from creative_id:
  campaign_id            INTEGER       NOT NULL,
  advertiser_id          INTEGER       NOT NULL,
  -- derived from inventory_id:
  site_id                INTEGER       NOT NULL,
  page_id                INTEGER       NOT NULL,
  -- derived from type_id:
  is_impression          INTEGER       NOT NULL,
  is_clickthrough        INTEGER       NOT NULL,
  is_conversion          INTEGER       NOT NULL
);
CREATE INDEX ttl ON event_data (utc_time);
ALTER TABLE event_data ALTER USING TTL 1 MINUTES ON COLUMN utc_time;
PARTITION TABLE event_data ON COLUMN creative_id;

-------------- VIEWS ----------------------------------------------

CREATE VIEW campaign_rates AS
SELECT
  advertiser_id,
  campaign_id,
  COUNT(*) AS records,
  SUM(is_impression) AS impressions,
  SUM(is_clickthrough) AS clicks,
  SUM(is_conversion) as conversions,
  SUM(cost) as cost
FROM event_data
GROUP BY advertiser_id, campaign_id;

CREATE PROCEDURE advertiser_summary AS
SELECT
  campaign_id,
  cost as spent,
  impressions,
  1000*cost/impressions as cpm,
  clicks,
  CAST(clicks AS DECIMAL)/impressions AS ctr,
  cost/DECODE(clicks,0,null,clicks) as cpc,
  conversions,
  CAST(conversions AS DECIMAL)/DECODE(clicks,0,null,clicks) as convr,
  cost/DECODE(conversions,0,null,conversions) as cpconv
FROM campaign_rates
WHERE advertiser_id = ?
ORDER BY campaign_id;


CREATE VIEW creative_rates AS
SELECT
  advertiser_id,
  campaign_id,
  creative_id,
  COUNT(*) AS records,
  SUM(is_impression) AS impressions,
  SUM(is_clickthrough) AS clicks,
  SUM(is_conversion) as conversions,
  SUM(cost) as cost
FROM event_data
GROUP BY advertiser_id, campaign_id, creative_id;

CREATE PROCEDURE campaign_summary AS
SELECT
  creative_id,
  cost as spent,
  impressions,
  1000*cost/impressions as cpm,
  clicks,
  CAST(clicks AS DECIMAL)/impressions AS ctr,
  cost/DECODE(clicks,0,null,clicks) as cpc,
  conversions,
  CAST(conversions AS DECIMAL)/DECODE(clicks,0,null,clicks) as convr,
  cost/DECODE(conversions,0,null,conversions) as cpconv
FROM creative_rates
WHERE advertiser_id = ? AND campaign_id = ?
ORDER BY creative_id;



-- CREATE VIEW ad_campaign_rates_daily AS
-- SELECT advertiser_id, campaign_id, TRUNCATE(DAY,utc_time) as utc_dt, COUNT(*) AS records, SUM(is_impression) AS impressions, SUM(is_clickthrough) AS clicks, SUM(is_conversion) as conversions, SUM(cost) as cost
-- FROM event_data
-- GROUP BY advertiser_id, campaign_id, TRUNCATE(DAY,utc_time);

CREATE VIEW advertiser_rates_minutely AS
SELECT
  advertiser_id,
  TRUNCATE(MINUTE,utc_time) as utc_min,
  COUNT(*) AS records,
  SUM(is_impression) AS impressions,
  SUM(is_clickthrough) AS clicks,
  SUM(is_conversion) as conversions,
  SUM(cost) AS spent
FROM event_data
GROUP BY advertiser_id, TRUNCATE(MINUTE,utc_time);

-- Update classes from jar to that server will know about classes but not procedures yet.
LOAD CLASSES adperformance-procs.jar;

CREATE PROCEDURE advertiser_minutely_clicks AS
SELECT utc_min, clicks, conversions
FROM advertiser_rates_minutely
WHERE advertiser_id = ?
ORDER BY utc_min DESC OFFSET 1 LIMIT 30;

-- CREATE VIEW ad_campaign_creative_rates_minutely AS
-- SELECT advertiser_id, campaign_id, creative_id, TRUNCATE(MINUTE,utc_time) as utc_min, COUNT(*) AS records, SUM(is_impression) AS impressions, SUM(is_clickthrough) AS clicks, SUM(is_conversion) as conversions
-- FROM event_data
-- GROUP BY advertiser_id, campaign_id, creative_id, TRUNCATE(MINUTE,utc_time);

CREATE PROCEDURE FROM CLASS adperformance.InitializeCreatives;

CREATE PROCEDURE PARTITION ON TABLE event_data COLUMN creative_id PARAMETER 3
  FROM CLASS adperformance.TrackEvent;

-- CREATE PROCEDURE ad_campaign_minutely_rates AS
-- SELECT campaign_id, utc_min, clicks/impressions as ctr, conversions/clicks as cr
-- FROM ad_campaign_rates_minutely
-- WHERE advertiser_id = ? AND utc_min > ?
-- ORDER BY campaign_id, utc_min;

-- CREATE PROCEDURE ad_campaign_creative_minutely_rates AS
-- SELECT creative_id, utc_min, clicks/impressions as ctr, conversions/clicks as cr
-- FROM ad_campaign_creative_rates_minutely
-- WHERE advertiser_id = ? AND campaign_id = ? AND utc_min > ?
-- ORDER BY utc_min ASC, ctr DESC;
