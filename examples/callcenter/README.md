# Call Center Example Application


Use Case
---------------------------
Process begin and end call events from a call center. Pair/join events in VoltDB to create a definitive record of completed calls.

Use VoltDB's strong consistency and stored procedure logic to compute a running standard deviation on call length by agent. This is not a trivial thing to compute without strong consistency. The provided HTML dashboard shows a top-N list of agents by standard deviation. It can be found in the "web" folder.

Note that in order for the simulation to be interesting, this app uses unrealistic call data. The average call time is 5s by default to make the stats interesting in a two-minute example run.

This app doesn't really show off the extraordinary throughput of VoltDB, though it will get a lot faster if you set the average call time lower and/or the number of agents higher.


VoltDB Features and Patterns
---------------------------

- **Streaming Joins**: Joining begin and end events for the same call using a table to hold unpaired state. Note the example supports out-of-order pairing.
- **Idempotent Processing**: Some of the input messages are duplicated, simulating real-life *at-least-once* delivery guarantees. This app uses VoltDB strong consistency to ignore redundant processing.
- **Complex Calculations**: Computing standard deviation is reasonably complex math, and is enabled using strong consistency in VoltDB.
- **Procedure Class Hierarchies**: The begin and end procedures inherit from a common base class that computes standard deviation for completed calls.

Quickstart
---------------------------
Make sure "bin" inside the VoltDB kit is in your PATH.  Then open a shell and go to the examples/callcenter directory, then execute the following commands to start the database:

    voltdb init
    voltdb start

Wait until you see "Server completed initialization."
Open a new shell in the same directory and run the following to load the schema:

    sqlcmd < ddl.sql

In the same shell, run the following script to preload some data and run the demo client application:

    ./run.sh client

Open up the index.html file the "web" directory to view the status dashboard.

If you're running the example on a VoltDB cluster, rather than your local desktop or laptop, run `./run.sh webserver` in a new shell on one of the machines in the cluster, then connect to your dashboard from your browser at [http://servername:8081](http://servername:8081).

You can stop the server, running client, or webserver at any time with `Ctrl-c` or `SIGINT`.  Of course VoltDB can also run in the background using the -B option, in which case you can stop it with the `voltadmin shutdown` command.

Note that the downloaded VoltDB kits include pre-compiled stored procedures and client code as jarfiles. To run the example from a source build, it may be necessary to compile the Java source code by typing "run.sh jars" before step 3 above. Note that this step requires a full Java JDK.


Using the run.sh script
---------------------------
VoltDB examples come with a run.sh shell script that simplifies compiling and running the example client application and other parts of the examples.
- *run.sh* : start the server
- *run.sh server* : start the server
- *run.sh init* : load the schema and stored procedures
- *run.sh jars* : compile all Java clients and stored procedures into two Java jarfiles
- *run.sh client* : start the client
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

    --agents=3000                 (number of call center agents)
    --numbers=1500000             (number of phone numbers in total)
    --meancalldurationseconds=5   (mean call duration in seconds)
    --maxcalldurationseconds=60   (max call duration)

Customizing this Example
---------------------------
See the "deployment-examples" directory within the "examples" directory for ways to alter the default single-node, no authorization deployment style of the examples. There are readme files and example deployment XML files for different clustering, authorization, export, logging and persistence settings.
