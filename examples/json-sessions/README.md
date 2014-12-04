JSON application
===========================

This sample demonstrates how to use VoltDB JSON functions to implement a flexible schema-type of application.  The application demonstrated here is a single sign-on session tracking application.  Logins from multiple sites (URLs) are tracked in a user session table in VoltDB.  Each login has common fields such as username and global session id.  Further, each login has site-specific data stored in a varchar column as JSON data.

The main goal of the JSON application is to demonstrate flexible schema and JSON support in VoltDB:

* This sample application uses the VoltDB synchronous API to load the database.
* The sample first creates 10 threads and loads up as many random logins as possible in 10 seconds.  The insertion rate (tx/second) and latency is calculated and displayed.
* Once the data is loaded, the sample application executes a series of AdHoc SQL queries, demonstrating various SQL queries on the JSON data.


Quickstart
-----------

VoltDB Examples come with a run.sh script that sets up some environment and saves some of the typing needed to work with Java clients. It should be fairly readable to show what is precisely being run to accomplish a given task.

1. Make sure "bin" inside the VoltDB kit is in your path.
2. Type "voltdb create" to start an empty, single-node VoltDB server.
3. Type "sqlcmd < ddl.sql" to load the schema and the jarfile of procedures into VoltDB.
4. Type "./run.sh client" to run the client code.

Note that the downloaded VoltDB kits include pre-compiled stored procedures and client code as jarfiles. To run the example from a source build, it may be necessary to compile the Java source code by typing "run.sh jars" before step 3 above. Note that this step requires a full Java JDK.


run.sh actions
-----------

* *run.sh* : start the server
* *run.sh server* : start the server
* *run.sh init* : load the schema and stored procedures
* *run.sh srccompile* : compile all Java clients and stored procedures
* *run.sh client* : start the client, more than 1 client is permitted
- *run.sh clean* : remove compilation and runtime artifacts
- *run.sh cleanall* : remove compilation and runtime artifacts *and* the two included jarfiles

