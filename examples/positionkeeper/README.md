# VoltDB Example App: Position Keeper

Use Case
--------
This application simulates a simple position keeper application that maintains portfolio positions. Portfolios are updated frequently as trades and price changes occur.

Quickstart
---------------------------
VoltDB examples come with a run.sh script that sets up some environment and saves some of the typing needed to build and run the Java clients. The script is readable and when run, shows what commands are being executed to accomplish a given task.

1. Make sure "bin" inside the VoltDB kit is in your path.
2. Type "voltdb create -f" to start an empty, single-node VoltDB server.
3. Open a new shell in the same directory and type "sqlcmd < ddl.sql" to load the schema and the jarfile of procedures into VoltDB.
4. Type "./run.sh client" to run the client code.

You can stop the server or running client at any time with `ctrl-c` or `SIGINT`.

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

    --traders=1000                (number of traders)
	--secpercnt=10                (number of securities each trader has in their portfolio)

Customizing this Example
---------------------------
See the "deployment-examples" directory within the "examples/HOWTOs" directory for ways to alter the default single-node, no authorization deployment style of the examples. There are readme files and example deployment XML files for different clustering, authorization, export, logging and persistence settings.
