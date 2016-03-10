# VoltDB Example App: Ad Performance#

Use Case
--------
This application simulates a high velocity stream of events (impressions, clickthroughs, conversions) that are enriched and ingested.  These events are randomly generated in the client, but represent a stream of events that would be received from web traffic.

The "TrackEvent" stored procedure processes these events.  It looks up the corresponding advertiser and campaign based on the creative ID which represents which ad was shown.  It also retrieves the corresponding web site and page based on the inventory ID from the event.  The timestamp and event type fields are converted to aid in aggregation, and all of this data is then inserted into the impression_data table.

Several views maintain real-time aggregations on this table to provide a minutely summary for each advertiser, plus drill-down reports grouped by campaign and creative to show detail-level metrics, costs and rates with real-time accuracy.

Several new SQL features in VoltDB are demonstrated in this application, including:
  - VIEW group by expressions
  - [TRUNCATE](http://voltdb.com/docs/UsingVoltDB/sqlfunctruncate.php) Timestamp function
  - [DECODE](http://voltdb.com/docs/UsingVoltDB/sqlfuncdecode.php) function

Code organization
-----------------
The code is divided into projects:

- "db": the database project, which contains the schema, stored procedures and other configurations that are compiled into a catalog and run in a VoltDB database.  
- "client": a java client that loads a set of cards and then generates random card transactions a high velocity to simulate card activity.
- "web": a web dashboard client (static html page with dynamic content)

See below for instructions on running these applications.  For any questions, 
please contact fieldengineering@voltdb.com.

Pre-requisites
--------------
Before running these scripts you need to have VoltDB 4.0 (Enterprise or Community) or later installed, and you should add the voltdb-$(VERSION)/bin directory to your PATH environment variable, for example:

    export PATH="$PATH:$HOME/voltdb-ent-4.0.2/bin"


Demo Instructions
-----------------

1. Start the web server

    ./run.sh start_web
   
2. Start the database and client 

    ./run.sh demo

3. Open a web browser to http://hostname:8081

4. To stop the demo:

Stop the client (if it hasn't already completed)

    Ctrl-C
    
Stop the database

    voltadmin shutdown
   
Stop the web server

    ./run.sh stop_web

Options
-------
You can control various characteristics of the demo by modifying the parameters passed into the InvestmentBenchmark java application in the "client" function of the run.sh script.

Speed & Duration:

    --duration=120                (benchmark duration in seconds)
    --autotune=true               (true = ignore rate limit, run at max throughput until latency is impacted)
                                  (false = run at the specified rate limit)
    --ratelimit=20000             (when autotune=false, run up to this rate of requests/second)

Metadata volumes and ratios:

    --sites=100                   (number of web sites where ad events may occur)
    --pagespersite=10             (number of pages per web site)
    --advertisers=100             (number of advertisers)
    --campaignsperadvertiser=10   (number of campaigns per advertiser)
    --creativespercampaign=5      (number of creatives or banners per campaign)


Instructions for running on a cluster
-------------------------------------

Before running this demo on a cluster, make the following changes:

1. On each server, edit the run.sh file to set the HOST variable to the name of the **first** server in the cluster:
    
    HOST=voltserver01
    
2. On each server, edit db/deployment.xml to change hostcount from 1 to the actual number of servers:

    <cluster hostcount="1" sitesperhost="3" kfactor="0" />

4. On each server, start the database

	./run.sh server
    
5. On one server, Edit the run.sh script to set the SERVERS variable to a comma-separated list of the servers in the cluster

    SERVERS=voltserver01,voltserver02,voltserver03
    
6. Run the client script:

	./run.sh client
