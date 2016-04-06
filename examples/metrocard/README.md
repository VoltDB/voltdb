# VoltDB Example App: MetroCard

Use Case
--------
This application performs high velocity transaction processing for metro cards.  These transactions include:

- Card generation (during the initialization)
- Card Swipes (during the benchmark)

Optionally, the project can export data using the HTTP connector. There's a simple webserver included that acts as destination for the exported rows of data.

Code organization
-----------------
The code is divided into projects:

- "db": the database project, which contains the schema, stored procedures and other configurations that are compiled into a catalog and run in a VoltDB database.
- "client": a java client that loads a set of cards and then generates random card transactions a high velocity to simulate card activity.
- "web": a web dashboard client (static html page with dynamic content)
- "exportWebServer": a Python-based webserver configured to receive exported rows and provide, as an additional endpoint, a rolling display of the last 10 exported rows

See below for instructions on running these applications.  For any questions,
please contact fieldengineering@voltdb.com.

Pre-requisites
--------------
Before running these scripts you need to have VoltDB 5.0 (Enterprise or Community) or later installed, and you should add the voltdb-$(VERSION)/bin directory to your PATH environment variable, for example:

    export PATH="$PATH:$HOME/voltdb-ent-5.2.1/bin"


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

    --cardcount=500000            (number of metro cards created)


Instructions for running on a cluster
-------------------------------------

Before running this demo on a cluster, make the following changes:

1. On each server, edit the run.sh file to set the HOST variable to the name of the **first** server in the cluster:

    HOST=voltserver01

2. On each server, start the database after starting the web server as in step 1 above:

	./run.sh cluster-server

3. On one server, Edit the run.sh script to set the SERVERS variable to a comma-separated list of the servers in the cluster

    SERVERS=voltserver01,voltserver02,voltserver03

4. Run the client script:

	./run.sh client

Instructions for running with HTTP export
-----------------------------------------

Here we run the VoltDB database configured to export rows to an HTTP destination, based on conditions set in the stored procedure, CardSwipe. See CardSwipe.java for more details.

1. Start the app-metro dashboard, and browse to it on http://localhost:8081, or some other URL depending on your configuration:
    ./run.sh start_web <port number>

2. Start the export web server:
    ./run.sh start_export_web

   Exported rows can be viewed in the command line output from the web service.

3. Start the VoltDB server:
    ./run.sh export-server

4. Start the client script:
    ./run.sh client

Browse to http://localhost:8081 to see the app-metro dashboard.

Browse to http://localhost:8083/htmlRows to view a continuously refreshing view of the last 10 exported rows.
