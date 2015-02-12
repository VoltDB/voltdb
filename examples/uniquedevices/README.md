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
In the default mode, where the app is using HyperLogLog to estimate counts, the system uses VoltDB's strong consistency to transacitonally store the integer estimate value in the table, along with the blob representing the HLL data structure. This model of transactionally reading, processing and updating is something VoltDB excels at. Because the estimate value is always 100% current and easily accessible via SQL queries, using the data is easier, and the complexity of the HLL algorithm is limited to a single piece of stored logic. In fact, whether the processing is using HLL, exact counts or a hybrid mode can be abstracted away from any clients consuming the data.

ACID consistency is also key to the simplicity of the hybrid estimate code. Without a transactional handoff between the exact count and the estimated values, it's much harder to claim the exact values are actually exact under the conditions promised.

Finally, it is not a difficult exercise to add a history table to this example and keep daily history for each app in VoltDB. One would need to add some logic to the core processing to check for date rollover since the last call, then to store the current estimates in the history table, then reset the new day's data to zero. With ACID consistency, the code to do this is a handful of if-statements, a huge win over less consistent systems. This could replace the batch layer of a basic Lambda Architecture implementation.

Quickstart
-----------
VoltDB Examples come with a run.sh script that sets up the environment and saves some of the typing needed to work with Java clients. It should be fairly readable to show what, precisely, is being run to accomplish a given task.

1. Make sure "bin" inside the VoltDB kit is in your path.
2. Type "voltdb create" to start an empty, single-node VoltDB server.
3. Type "sqlcmd < ddl.sql" to load the schema and the jarfile of procedures into VoltDB.
4. Type "./run.sh client" to run the client code.

Note that the downloaded VoltDB kits include pre-compiled stored procedures and client code as jarfiles. To run the example from a source build, it may be necessary to compile the Java source code by typing "run.sh jars" before step 3 above. Note that this step requires a full Java JDK.

Other run.sh actions
-----------
- *run.sh* : start the server
- *run.sh server* : start the server
- *run.sh init* : compile stored procedures and load the schema and stored procedures
- *run.sh jars* : compile all Java clients and stored procedures into two Java jarfiles
- *run.sh client* : start the client, more than 1 client is permitted
- *run.sh clean* : remove compilation and runtime artifacts
- *run.sh cleanall* : remove compilation and runtime artifacts *and* the two included jarfiles

run.sh Client Options
--------------
Near the bottom of the run.sh bash script is the section run when you type `run.sh client`. In that section is the actual shell command to run the client code, reproduced below:

    java -classpath $CLIENTCLASSPATH -Dlog4j.configuration=file://$LOG4J \
        uniquedevices.UniqueDevicesClient \
        --displayinterval=5 \
        --duration=120 \
        --servers=localhost:21212 \
        --appcount=100

Changing these settings changes the behavior of the app.

* **displayinterval** controls the time in seconds between console performance updates.
*  **duration** controls the length of the benchmark in seconds.
* **servers** is a comma-separated list of servers to connect the client to, with optional ports.
* **appcount** is the number of distinct applications the data generator should generate. Valid values are between 1 and lots.
