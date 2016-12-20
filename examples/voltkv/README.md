Key-Value application
===========================
This example application demonstrates how to create a "Key-Value" store using VoltDB and the automatic stored procedures created for each table.

Many attributes of the application are customizable through arguments passed to the client, including:

  - The maximum number of transactions the client will attempt per second
  - How long the sample client runs
  - When to start recording performance statistics
  - How frequently to report those statistics
  - "Key" size
  - Minimum and Maximum "Value" size (Value sizes will be randomly distributed between min and max)
  - Percentage of transactions that are "Gets" (out of 100%, the rest are "Puts")
  - Store the "Value" as is, or compress the "Value" (compressible binary)

These attributes can be adjusted by modifying the arguments to the "async-benchmark" and "sync-benchmark" targets in run.sh.

Interpreting the Results
------------------------
The default client configuration will allow the system to automatically tune itself for optimal performance, regardless of your underlying hardware and cluster deployment.

The client starts "fire-hosing" the VoltDB server by attempting to submit transactions faster than the server can possibly process them (1 billion transactions per second, or 1B TPS).  Within 5 seconds, the automated tuning should be able to figure out an optimized throughput (TPS) that maintains transaction latency within 6 ms.

You can also turn auto-tuning off to experiment with different loads or to better understand why proper tuning is key to getting the most of your specific VoltDB deployment.

Rate-limiting your clients (or adding cluster nodes) is essential to preventing "fire-hosing" your server (cluster) and will ensure you get proper application responsiveness (latency) while maximizing througput (TPS) for your hardware configuration.

While the "Key-Value" application is designed with benchmarking in mind, it is essentially a network-bound application: you will generally find that you max-out your network bandwidth long before you exhaust VoltDB's throughput processing capability.

For a better feel for the type of performance VoltDB is capable of on your hardware, check out the "Voter" application sample.

For more on benchmarking and tips on application tuning, make sure you visit the VoltDB blog:
 - http://voltdb.com/search/node/benchmark
 - http://voltdb.com/search/node/tuning

Calling stored procedures
-------------------------
This example makes use of the built-in stored procedures that are automatically created for each table. Client applications use stored procedures to make query calls to the database. However, they can use the builtin procedures to make basic query calls to the server instead of writing custom stored procedures. For example, a Get query uses the builtin STORE.select procedure call and a Put query (insert or update) uses the builtin STORE.upsert.

Quickstart
-----------
Make sure "bin" inside the VoltDB kit is in your PATH.  Then open a shell and go to the examples/voltkv directory, then execute the following commands to start the database:

    voltdb init
    voltdb start

Wait until you see "Server completed initialization."
Open a new shell in the same directory and run the following to load the schema:

    sqlcmd < ddl.sql

In the same shell, run the following script to preload some data and run the demo client application:

    ./run.sh client

You can stop the server or running client at any time with `Ctrl-c` or `SIGINT`.  Of course VoltDB can also run in the background using the -B option, in which case you can stop it with the `voltadmin shutdown` command.

Using the run.sh script
---------------------------
VoltDB examples come with a run.sh shell script that simplifies compiling and running the example client application and other parts of the examples.
- *run.sh* : start the server
- *run.sh server* : start the server
- *run.sh init* : compile stored procedures and load the schema and stored procedures
- *run.sh jars* : compile all Java clients into a Java jarfile
- *run.sh client* : start the async client benchmark, initialize the given number of key-value pairs (puts) if needed, and begin normal client processing (gets and puts)
- *run.sh async-benchmark* : same as run.sh client
- *run.sh sync-benchmark* : start the multi-threaded sync client,  initialize the given number of key-value pairs (puts) if needed, and begin normal client processing (gets and puts)
- *run.sh jdbc-benchmark* : start the JDBC client benchmark
- *run.sh clean* : remove compiled and other runtime artifacts
- *run.sh cleanall* : remove compilation and runtime artifacts *and* the included client jarfile
