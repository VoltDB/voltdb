# VoltDB Bank Offers Example App

Use Case
--------
Ingest generated consumer purchase transaction data and find the best matching offer to present to the consumer at the point of sale.

Matching offers are found using a query that joins a summary view of the account activity with this vendor (merchant) against the available offers from that vendor.  The best match is determined by the priority for the offers that was set by the vendor.

The result is inserted into a persistent table, where it could drive a live web interaction, such as serving an online ad that describes the offer, retrieved from the inputs of the account and vendor_id.  It is also shown in the web dashboard as recent examples of offers.  This result is also inserted into an export table so that it can be pushed to Hadoop using the HTTP export connector.

The web dashboard shows recent offers as well as a moving chart of offers/sec and overall transactions/sec.

Quickstart
---------------------------
VoltDB examples come with a run.sh script that sets up some environment and saves some of the typing needed to work with Java clients. It is very readable and when executed, shows what is precisely being run to accomplish a given task.

1. Make sure "bin" inside the VoltDB kit is in your path.
2. Type "voltdb create -f" to start an empty, single-node VoltDB server.
3. Open a new shell in the same directory and type "sqlcmd < ddl.sql" to load the schema and the jarfile of procedures into VoltDB.
4. Type "./run.sh client" to run the client code.
5. Open up the index.html the "web" directory to view the status dashboard.

If you're running the example on a different machine than your web browser is running on, you can run `./run.sh webserver` in a new shell and then connect to your dashboard from a browser at [http://servername:8081](http://servername:8081).

You can stop the server, running client, or webserver at any time with `ctrl-c` or `SIGINT`.

Note that the downloaded VoltDB kits include pre-compiled stored procedures and client code as jarfiles. To run the example from a source build, it may be necessary to compile the Java source code by typing "run.sh jars" before step 3 above. Note that this step requires a full Java JDK.

Other run.sh Actions
---------------------------
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

    --custcount=100000            (number of customers to pre-populate)
    --vendorcount=5000            (number of vendors to pre-populate)

Customizing this Example
---------------------------
See the "deployment-examples" directory within the "examples" directory for ways to alter the default single-node, no authorization deployment style of the examples. There are readme files and example deployment XML files for different clustering, authorization, export, logging and persistence settings.



