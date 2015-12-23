Geospatial Ad Brokering Example Application
===========================================

This example demonstrates geospatial functionality that was added to
VoltDB in version 6.0.  The problem space for this demo is serving ads
to many mobile device users based on their location in real time.
This demo app does several things at once, described below.

To run the demo, use the script in the `examples/geospatial/` directory.

```shell
    ./run.sh demo
```

## The Bid Generator
One thread simulates advertisers making bids on how much they will pay
to show ads.  Bids are valid for a particular duration and within a
particular region.  Regions are stored as polygons in the GEOGRAPHY
column of the `bids` table.  In this example, polygons are chosen
randomly, but in the real world a business might want to
show an ad when a mobile device is near their storefront (or perhaps
that of a competitor).  GEOGRAPHY is a new data type supported in
VoltDB 6.0.

## Mobile Device Simulation
On another thread, we simulate users accessing a browser or
social media on their device.  When this happens, the device makes a
request to the database for an ad.  The stored procedure invoked here
is `GetHighestBidForLocation`, which invokes this SQL statment:

```SQL
    select id, advertiser_id, bid_amount
    from bids
    where current_timestamp between ts_start and ts_end
      and contains(region, ?)
    order by bid_amount desc, id
    limit 1;
```

The parameter (`?`) in this query is the location of the device as a
point in terms of longitude and latitude.  This query makes use of
the CONTAINS function, also new in version 6.0.  The net effect of
this query is to find the bids where the mobile device is inside the
bid's polygon, and return the bid with the highest dollar amount.

In addition to using a geospatial query to find matching bids, `GetHighestBidForLocation`
also stores info about winning bids (and unmet ad requests) in the table
`ad_requests`.

## Periodic Reporting of Statistics
Every five seconds, statistics for the most recent five-second period are displayed
on the console:
- Transactional throughput and latency statistics
- The number of ad requests made by mobile devices, and the percentage of those that had winning bids.
- The top 5 customers ordered by the sum of the dollar amounts of the bids they won.  This is achieved using a materialized view `requests_by_second_by_advertiser` on the table `ad_requests`.

## Nibble Deletion
As time passes, old bids will no longer be active, because their end timestamp is in the past.  Rows in the `bids` table
should therefore be purged to make room for new bids.

Likewise, the oldest rows in the `ad_requests` table should be periodically aged out, once historical data has completed its useful lifetime.  We arbitrarily choose this time to be 6 seconds, to allow time for statistics to be displayed.  In a real application, this data might be written to an export table before being deleted.

To achieve the deletion of unneeded data, we define a class called `NibbleDeleter` that gets rid of unneeded rows once every second.  Deleting large numbers of rows can impact performance, so we chose to do the delete small numbers of rows relatively frequently to minimize this impact.  This is sometimes called the "nibble" pattern and is common in VoltDB applications.  For more info on aging out data in VoltDB, see this blog post:

https://voltdb.com/blog/aging-out-data-voltdb

## Performance
We've rate-limited the number of transactions in this application to 10,000/s.  At any given time there are 100 active bids, so we're doing about one million CONTAINS operations per second, with each ad request being completed with sub-millisecond latency on average.  Naturally, these figures are for the hardware upon which we developed this app, and your mileage may vary.

Currently, each ad request must scan many rows of the bids table to find matching bids.  In future versions of VoltDB, geospatial indexes will provide a many-fold increase to throughput, allowing us to handle many more active bids, and more frequent ad requests with ease.
