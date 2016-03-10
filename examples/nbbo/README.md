# VoltDB NBBO Example App

Use Case
--------
NBBO is the National Best Bid and Offer, defined as the lowest available ask price and highest available bid price across the participating markets for a given security.  Brokers should route trade orders to the market with the best price, and by law must guarantee customers the best available price.

This example app includes a VoltDB database schema that stores each market data tick and automatically inserts a new NBBO record whenever there is a change to the best available bid or ask price.  This can be used to serve the current NBBO or the history of NBBO changes on demand to consumers such as the dashboard or other applications.

The example includes a web dashboard that shows the real-time NBBO for a security and the latest avaialble prices from each exchange.  It also includes a client benchmark application that generates synthetic market data ticks for all of the listed stocks from NYSE, AMEX, and NASDAQ.  The prices are simulated using a random walk algorithm that starts from the end of day closing price that is initially read from a CSV file.  It is not intended to be a realistic simulation of market data, but simply to generate simulated data for demonstration purposes.

Code organization
-----------------
The code is divided into projects:

- "db": the database project, which contains the schema, stored procedures and other configurations that are compiled into a catalog and run in a VoltDB database.  
- "client": a java client that generates tick events and records performance metrics.
- "web": a simple web server that provides the demo dashboard.

See below for instructions on running these applications.  For any questions, 
please contact fieldengineering@voltdb.com.

Pre-requisites
--------------

Before running these scripts you need to have VoltDB 4.0 or later installed.  If you choose the .tar.gz file distribution, simply untar it to a directory such as your $HOME directory, then add the bin subdirectory to your PATH environment variable.  For example:

    export PATH="$PATH:$HOME/voltdb-ent-4.7/bin"

You may choose to add this to your .bashrc file.

If you installed the .deb or .rpm distribution, the binaries should already be in your PATH.  To verify this, the following command should return a version number:

    voltdb --version

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
You can control various characteristics of the demo by modifying the parameters passed into the NbboBenchmark java application in the "client" function within the run.sh script.

Speed & Duration:

    --duration=120                (benchmark duration in seconds)
    --autotune=true               (true = ignore rate limit, run at max throughput until latency is impacted)
                                  (false = run at the specified rate limit)
    --ratelimit=20000             (when autotune=false, run up to this rate of requests/second)


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



