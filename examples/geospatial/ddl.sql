-- Tell sqlcmd to batch the following commands together,
-- so that the schema loads quickly.
file -inlinebatch END_OF_BATCH

CREATE TABLE advertisers
(
  id bigint not null
, name varchar(256) not null
, primary key (id)
);

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

CREATE TABLE devices
(
  id bigint not null
, location point not null
, primary key (id)
);



CREATE TABLE impressions
(
  bid_id bigint not null
, advertiser_id bigint not null
, bid_amount float not null
, device_id bigint not null
, ts timestamp not null
);

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
