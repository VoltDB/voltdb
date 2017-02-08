# Geospatial Ad Brokering Example Application

This example demonstrates geospatial functionality that was added to VoltDB in version 6.0.  The problem space for this demo is serving ads to many mobile device users based on their location in real time. This demo app does several things at once, described below.

More on this application can be found later in this readme file.

Quickstart
---------------------------
Make sure "bin" inside the VoltDB kit is in your PATH.  Then open a shell and go to the examples/geospatial directory, then execute the following commands to start the database:

    voltdb init
    voltdb start

Wait until you see "Server completed initialization."
Open a new shell in the same directory and run the following to load the schema:

    sqlcmd < ddl.sql

In the same shell, run the following script to preload some data:

    csvloader -f advertisers.csv advertisers

Then run the demo client application:

    ./run.sh client

You can stop the server or running client at any time with `Ctrl-c` or `SIGINT`.  Of course VoltDB can also run in the background using the -B option, in which case you can stop it with the `voltadmin shutdown` command.

Note that the downloaded VoltDB kits include pre-compiled stored procedures and client code as jarfiles. To run the example from a source build, it may be necessary to compile the Java source code by typing "run.sh jars" before step 3 above. Note that this step requires a full Java JDK.

Using the run.sh script
---------------------------
VoltDB examples come with a run.sh shell script that simplifies compiling and running the example client application and other parts of the examples.
- *run.sh* : start the server
- *run.sh server* : start the server
- *run.sh init* : compile stored procedures and load the schema and stored procedures
- *run.sh jars* : compile all Java clients and stored procedures into two Java jarfiles
- *run.sh client* : start the client, more than 1 client is permitted
- *run.sh clean* : remove compilation and runtime artifacts
- *run.sh cleanall* : remove compilation and runtime artifacts *and* the two included jarfiles

If you change the client or procedure Java code, you must recompile the jars by deleting them in the shell or using `./run.sh jars`.

Client Behavior Options
---------------------------
You can control various characteristics of the demo by modifying the parameters passed into the java application in the "client" function of the run.sh script.

**Speed & Duration:**

    --displayinterval=5           (seconds between status reports)
    --warmup=5                    (how long to warm up before measuring
                                   benchmark performance.)
    --duration=120                (benchmark duration in seconds)
    --ratelimit=20000             (run up to this rate of requests/second)

**Cluster Info:**

    --servers=$SERVERS            (host(s) client connect to, e.g.
                                   =localhost
                                   =localhost:21212
                                   =volt9a,volt9b,volt9c
                                   =foo.example.com:21212,bar.example.com:21212)

Customizing this Example
---------------------------
See the "deployment-examples" directory within the "examples" directory for ways to alter the default single-node, no authorization deployment style of the examples. There are readme files and example deployment XML files for different clustering, authorization, export, logging and persistence settings.

The Bid Generator
---------------------------
One thread simulates advertisers making bids on how much they will pay to show ads.  Bids are valid for a particular duration and within a particular region.  Regions are stored as polygons in the GEOGRAPHY column of the `bids` table.  In this example, polygons are chosen randomly, but in the real world a business might want to show an ad when a mobile device is near their storefront (or perhaps that of a competitor).  GEOGRAPHY is a new data type supported in VoltDB 6.0.

Mobile Device Simulation
---------------------------
On another thread, we simulate users accessing a browser or social media on their device.  When this happens, the device makes a request to the database for an ad.  The stored procedure invoked here is `GetHighestBidForLocation`, which invokes this SQL statement:

```SQL
    select id, advertiser_id, bid_amount
    from bids
    where current_timestamp between ts_start and ts_end
      and contains(region, ?)
    order by bid_amount desc, id
    limit 1;
```

The parameter (`?`) in this query is the location of the device as a point in terms of longitude and latitude.  This query makes use of the CONTAINS function, also new in version 6.0.  In order to avoid having to examine each row in the bids table to determine which regions contain the device, a geospatial index is used to find relevant regions quickly.  The net effect of this query is to find the bids where the mobile device is inside the bid's polygon, and return the bid with the highest dollar amount.

In addition to using a geospatial query to find matching bids, `GetHighestBidForLocation` also stores info about winning bids (and unmet ad requests) in the table `ad_requests`.

Periodic Reporting of Statistics
---------------------------
Every five seconds, statistics for the most recent five-second period are displayed
on the console:
- Transactional throughput and latency statistics
- The number of ad requests made by mobile devices, and the percentage of those that had winning bids.
- The top 5 customers ordered by the sum of the dollar amounts of the bids they won.  This is achieved using a materialized view `requests_by_second_by_advertiser` on the table `ad_requests`.

Nibble Deletion
---------------------------
As time passes, old bids will no longer be active, because their end timestamp is in the past. Rows in the `bids` table should therefore be purged to make room for new bids.

Likewise, the oldest rows in the `ad_requests` table should be periodically aged out, once historical data has completed its useful lifetime.  We arbitrarily choose this time to be 6 seconds, to allow time for statistics to be displayed.  In a real application, this data might be written to a stream table before being deleted.

To achieve the deletion of unneeded data, we define a class called `NibbleDeleter` that gets rid of unneeded rows once every second.  Deleting large numbers of rows can impact performance, so we chose to do the delete small numbers of rows relatively frequently to minimize this impact.  This is sometimes called the "nibble" pattern and is common in VoltDB applications.  For more info on aging out data in VoltDB, see this blog post:

https://voltdb.com/blog/aging-out-data-voltdb

Performance
---------------------------
The DDL for this example creates a geospatial index called `bid_area` which allows for fast evaluation of the CONTAINS predicate in the key query of the demo.  In addition, the stored procedure executing this query, `GetHighestBidForLocation`, can be run on just a single partition of the database, allowing execution on multiple threads at the same time.  On the desktop machine used to develop this example, it was easy to achieve up to 150,000 transactions per second where the bulk of the workload is doing point-in-polygon evaluation.
