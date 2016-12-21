Unique Devices application
===========================
The Unique Devices appllication demonstrates real-time analytics on fast moving data.  It also can be considered a representative implementation of the speed layer in the Lambda Architecture.

This example application solves a specific problem. Assume you offer a service to mobile app developers. Every time someone starts a mobile app, a message is sent to your service containing the application identifier and a unique id representing the device. Your service gives developers a bounded estimate of how many unique devices have used their app on any given day.

This example app was developed in response to a presentation that a Twitter (formerly Crashlytics) engineer has given several times, including a [20 minute presentation](http://youtu.be/56wy_mGEnzQ) from October 2014 at the Boston Facebook @Scale Conference. The presentation describes their use of the [Lambda Architecture](http://en.wikipedia.org/wiki/Lambda_architecture) for their service. The engineer describes this problem and how they solved it at a scale of 800,000 messages per second.

A key point of this presentation and this sample app is that the volume of data is so large that counting unique devices is often impractical and a realistic approach is to use a cardinality estimation algorithm. In this case [HyperLogLog](http://en.wikipedia.org/wiki/HyperLogLog) is used. This example app shows that it's possible, and in fact, easy, to leverage a third party software library in VoltDB stored procedure code. In this example application, an open source HLL library was sourced [here](https://github.com/addthis/stream-lib) and modified slightly, then directly used in VoltDB to processes binary blobs and estimate cardinality at a high rate.

Alternate Versions
----------
The example can be run in three different modes by changing the name of the procedure called by the client to one of three provided choices:

* **CountDeviceEstimate** uses HyperLogLog to provide estimate values for the number of unique devices per app.
* **CountDeviceExact** uses traditional indexes to exactly count unique devices per app. It is slower and requires much more space when the number of unique devices is large.
* **CountDeviceHybrid** uses exact counting for values up to 1000 while providing HLL-based estimates for values larger than 1000.

To switch between versions, change the source of AsyncBenchmark.java around line 360 to call a different procedure.

Benefits of ACID Consistency
----------
In the default mode, where the app is using HyperLogLog to estimate counts, the system uses VoltDB's strong consistency to transactionally store the integer estimate value in the table, along with the blob representing the HLL data structure. This model of transactionally reading, processing and updating is something VoltDB excels at. Because the estimate value is always 100% current and easily accessible via SQL queries, using the data is easier, and the complexity of the HLL algorithm is limited to a single piece of stored logic. In fact, whether the processing is using HLL, exact counts or a hybrid mode can be abstracted away from any clients consuming the data.

ACID consistency is also key to the simplicity of the hybrid estimate code. Without a transactional handoff between the exact count and the estimated values, it's much harder to claim the exact values are actually exact under the conditions promised.

Finally, it is not a difficult exercise to add a history table to this example and keep daily history for each app in VoltDB. One would need to add some logic to the core processing to check for date rollover since the last call, then to store the current estimates in the history table, then reset the new day's data to zero. With ACID consistency, the code to do this is a handful of if-statements, a huge win over less consistent systems. This could replace the batch layer of a basic Lambda Architecture implementation.

Quickstart
---------------------------
Make sure "bin" inside the VoltDB kit is in your PATH.  Then open a shell and go to the examples/uniquedevices directory, then execute the following commands to start the database:

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

    --appcount=100                (number of distinct applications the data generator
                                   should generate. Valid values are between 1 and lots.)

Customizing this Example
---------------------------
See the "deployment-examples" directory within the "examples" directory for ways to alter the default single-node, no authorization deployment style of the examples. There are readme files and example deployment XML files for different clustering, authorization, export, logging and persistence settings.
