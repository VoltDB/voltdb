
load classes geospatial-procs.jar;

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
-- the region indictated.  The region is represented in the GEOGRAPHY
-- field which will contain polygon values.  Each polygon can take
-- up to 16K of storage.
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
, region geography(16384) not null
, ts_start timestamp not null
, ts_end timestamp not null
, bid_amount float not null
, primary key (id)
);

CREATE INDEX bids_end ON bids(ts_end);

-- Create a geospatial index on the polygons that model bid regions.
-- This will acccelerate the key query in GetHighestBidForLocation.
CREATE INDEX bid_area on bids(region);

-- This table contains one row for each occurence of a
-- device requesting an ad.  If an ad was served, it
-- includes the winning bid info, and nulls otherwise.
CREATE TABLE ad_requests
(
  device_id bigint not null
, ts timestamp not null
, bid_id bigint
, advertiser_id bigint
, bid_amount float
);

partition table ad_requests on column device_id;

CREATE INDEX requests_ts ON ad_requests(ts);

-- This materiazlied view aggregates the number of ads served from
-- each advertiser per second, and the amount their account will be
-- charged.
CREATE VIEW requests_by_second_by_advertiser
(
  ts_second
, advertiser_id
, request_count
, sum_of_ad_revenue
)
AS
  select
    truncate(second, ts) as ts_second
  , advertiser_id
  , count(*)
  , sum(bid_amount)
  from ad_requests
  group by ts_second, advertiser_id;

-- A stored procedure that gets the bid id for the highest bid given a
-- device's location and the current time.
CREATE PROCEDURE
       partition on table ad_requests column device_id
       FROM CLASS geospatial.GetHighestBidForLocation;

-- A stored procedure that reports statistics on winning bids for the
-- most recent time period.
CREATE PROCEDURE FROM CLASS geospatial.GetStats;

-- Delete expired bids from the bids table.
CREATE PROCEDURE DeleteExpiredBids AS
       DELETE FROM bids WHERE current_timestamp > ts_end;

-- Delete rows from ad_requests that are older than the time period
-- specified by the caller.
CREATE PROCEDURE
       partition on table ad_requests column device_id
       FROM CLASS geospatial.DeleteOldAdRequests;

END_OF_BATCH
