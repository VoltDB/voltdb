# VoltDB Example App: Position Keeper

Use Case
--------
This application simulates a simple position keeper applicaton that maintains the positions of portfolios that are updated frequently as trades and price changes occur.

Code organization
-----------------
The code is divided into two projects:

- "db": the database project, which contains the schema, stored procedures and other configurations that are compiled into a catalog and run in a VoltDB database.  
- "client": a java client that loads a set of cards and then generates random card transactions a high velocity to simulate card activity.

See below for instructions on running these applications.  For any questions, 
please contact fieldengineering@voltdb.com.

Pre-requisites
--------------
Before running these scripts you need to have VoltDB 4.0 (Enterprise or Community) or later installed, and you should add the voltdb-$(VERSION)/bin directory to your PATH environment variable, for example:

    export PATH="$PATH:$HOME/voltdb-ent-4.0.2/bin"


Instructions
------------

1. Start the database in the background

    ./start_db.sh
     
2. Run the client application

    ./run_client.sh
	
Note: this client can only be run once, not repeatedly, or it will stop with unique constraint violations.  To run it again, the database needs to be stopped and restarted, or all of the tables need to be truncated.

3. To stop the database and clean up temp files

    voltadmin shutdown
    ./clean.sh



Options
-------
You can control various characteristics of the demo by modifying the parameters passed into the InvestmentBenchmark java application in the run_client.sh script.

Speed & Duration:

    --duration=120                (benchmark duration in seconds)
    --autotune=true               (true = ignore rate limit, run at max throughput until latency is impacted)
                                  (false = run at the specified rate limit)
    --ratelimit=20000             (when autotune=false, run up to this rate of requests/second)

Use case parameters:

	--traders=1000                 (number of traders)
	--secpercnt=10                 (number of securities each trader has in their portfolio)


Instructions for running on a cluster
-------------------------------------

Before running this demo on a cluster, make the following changes:

1. On each server, edit the start_db_cluster.sh file to set the HOST variable to the name of the **first** server in the cluster:
    
    HOST=voltserver01
    
2. On each server, edit db/deployment-cluster.xml to set hostcount to the correct number of servers:

    <cluster hostcount="3" sitesperhost="8" kfactor="0" />
	
Also, in the same file, set the voltdbroot path to an appropriate path for your servers.

4. On each server, run the start script:

    ./start_db_cluster.sh
    
5. On one server, Edit the run_client.sh script to set the SERVERS variable to a comma-separated list of the servers in the cluster

    SERVERS=voltserver01,voltserver02,voltserver03
    
6. Run the client script:

    ./run_client.sh




