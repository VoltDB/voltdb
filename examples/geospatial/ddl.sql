-- Tell sqlcmd to batch the following commands together,
-- so that the schema loads quickly.
file -inlinebatch END_OF_BATCH

-- This table contains one row for each advertiser who can bid on ad
-- impressions (that is, an ad being shown to a user on their mobile
-- device).
CREATE TABLE advertisers
(
  id bigint not null
, name varchar(256) not null
, primary key (id)
);

-- This table has one row for each bid made by an advertiser.  A bid
-- is valid for the duration of time between ts_start and ts_end and
-- the region indictated.
--
-- When a user logs in from a place and time where more than one bid
-- applies, an ad from the highest bidder (largest bid_amount) will be
-- served.
--
-- Expired bids (for which ts_end is older than current_timestamp)
-- should be purged periodically.
CREATE TABLE bids
(
  id bigint not null
, advertiser_id bigint not null
, region geography not null
, ts_start timestamp not null
, ts_end timestamp not null
, bid_amount float not null
, primary key (id)
);

-- This table has one row for each device that is being served ads,
-- including the location from where it last appeared.
CREATE TABLE devices
(
  id bigint not null
, location point not null
, primary key (id)
);

partition table devices on column id;

-- This table contains one row for each ad served to a device
CREATE TABLE impressions
(
  bid_id bigint not null
, advertiser_id bigint not null
, bid_amount float not null
, device_id bigint not null
, ts timestamp not null
);

partition table impressions on column device_id;

-- This materiazlied view aggregates the number of ads served from
-- each advertiser per minute, and the amount their account will be
-- charged.
CREATE VIEW impressions_by_minute_by_advertiser
(
  ts_minute
, advertiser_id
, impression_count
, sum_of_impression_revenue
)
AS
  select
    truncate(minute, ts) as ts_minute
  , advertiser_id
  , count(*)
  , sum(bid_amount)
  from impressions
  group by ts_minute, advertiser_id;

END_OF_BATCH
