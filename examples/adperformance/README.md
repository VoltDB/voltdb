# Ad Performance Application

Use Case
---------------------------
This application simulates a high velocity stream of events (impressions, clickthroughs, conversions) that are enriched and ingested.  These events are randomly generated in the client, but represent a stream of events that would be received from web traffic.

The "TrackEvent" stored procedure processes these events.  It looks up the corresponding advertiser and campaign based on the creative ID which represents which ad was shown.  It also retrieves the corresponding web site and page based on the inventory ID from the event.  The timestamp and event type fields are converted to aid in aggregation, and all of this data is then inserted into the impression_data table.

Several views maintain real-time aggregations on this table to provide a minutely summary for each advertiser, plus drill-down reports grouped by campaign and creative to show detail-level metrics, costs and rates with real-time accuracy.

Several SQL features in VoltDB are demonstrated in this application, including:
  - VIEW group by expressions
  - [TRUNCATE](http://voltdb.com/docs/UsingVoltDB/sqlfunctruncate.php) Timestamp function
  - [DECODE](http://voltdb.com/docs/UsingVoltDB/sqlfuncdecode.php) function
  - TTL processing for data expiration

Time to Live (TTL) helps the application operate on a constant stream of incoming events by setting an expiration timestamp to automatically delete old data. Call [@Statistics TABLE](https://docs.voltdb.com/v7docs/UsingVoltDB/sysprocstatistics.php#sysprocstattable) for aggregation statistics to watch the deletion of data on the "event_data" table beginning after the default TTL = 1 minute.

Quickstart
---------------------------
Make sure "bin" inside the VoltDB kit is in your PATH.  Then open a shell and go to the examples/adperformance directory, then execute the following commands to start the database:

    voltdb init
    voltdb start

Wait until you see "Server completed initialization."
Open a new shell in the same directory and run the following to load the schema:

    sqlcmd < ddl.sql

In the same shell, run the following script to preload some data and run the demo client application:

    ./run.sh client

Open up the index.html file in the "web" directory to view the status dashboard.

If you're running the example on a VoltDB cluster, rather than your local desktop or laptop, run `./run.sh webserver` in a new shell on one of the machines in the cluster, then connect to your dashboard from your browser at [http://servername:8081](http://servername:8081).

You can stop the server, running client, or webserver at any time with `Ctrl-c` or `SIGINT`.  Of course VoltDB can also run in the background using the -B option, in which case you can stop it with the `voltadmin shutdown` command.

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
- *run.sh webserver* : serve the web directory over http on port 8081

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

**Parameters Affecting Simulation:**

    --sites=200                   (number of web sites where ad events may occur)
    --pagespersite=20             (number of pages per web site)
    --advertisers=40              (number of advertisers)
    --campaignsperadvertiser=10   (number of campaigns per advertiser)
    --creativespercampaign=10     (number of creatives or banners per campaign)

Customizing this Example
---------------------------
See the "HOWTOs" directory within the "examples" directory for ways to alter the default single-node, no authorization deployment style of the examples. There are readme files and example deployment XML files for different clustering, authorization, export, logging and persistence settings.
