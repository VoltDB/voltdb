ContentionMark Example and Benchmark
===========================

ContentionMark is a small VoltDB application that measures VoltDB throughput under extreme contention.

How extreme? By default, ContentionMark creates one row, then updates it as fast as VoltDB will let it, one transaction per update.

We believe that no other system on the planet performs as well under contention as VoltDB, due to its unique architecture. We designed this app/benchmark to allow people to test us.

Note that the update is a read, increment, write operation, where the increment is between 0 and 4 (inclusive). This is slightly more difficult than a increment-by-one operation common to many counters.

It's also the smallest example we have, for what it's worth.


Variations to Try
-----------

- Change the disk persistence settings using a deployment.xml file. Try synchronous and asynchronous persistence. Try the app without persistence at all.
- Try with different replication settings.
- Change the number of contested keys by editing the run.sh file.
- Change the operation to be more complex than increment.
- Alter the default number (currently 8) of partition replicas per host using a deployment.xml file.

A more complex version of this app might populate the base table with billions of rows, but send 99% of updates to a single row. That might be a fun experiment for someone looking to learn VoltDB.

Quickstart
-----------

VoltDB Examples come with a run.sh script that sets up some environment and saves some of the typing needed to work with Java clients. It should be fairly readable to show what is precisely being run to accomplish a given task.

1. Make sure "bin" inside the VoltDB kit is in your path.
2. Type "voltdb create --force" to start an empty, single-node VoltDB server.
3. Type "sqlcmd < ddl.sql" to load the schema and the jarfile of procedures into VoltDB.
4. Type "./run.sh client" to run the client code.

run.sh actions described
---------------------

- *run.sh* : start the server
- *run.sh server* : start the server
- *run.sh init* : load the schema and any stored procedures
- *run.sh jars* : compile the Java client code into a Java jarfile
- *run.sh client* : start the client benchmark
- *run.sh clean* : remove compiled and other runtime artifacts
- *run.sh cleanall* : remove compilation and runtime artifacts *and* the included client jarfile

