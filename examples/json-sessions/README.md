# JSON application

This sample demonstrates how to use VoltDB JSON functions to implement a flexible schema-type of application.  The application demonstrated here is a single sign-on session tracking application.  Logins from multiple sites (URLs) are tracked in a user session table in VoltDB.  Each login has common fields such as username and global session id.  Further, each login has site-specific data stored in a varchar column as JSON data.

The main goal of the JSON application is to demonstrate flexible schema and JSON support in VoltDB:

* This sample application uses the VoltDB synchronous API to load the database.
* The sample first creates 10 threads and loads up as many random logins as possible in 10 seconds.  The insertion rate (tx/second) and latency is calculated and displayed.
* Once the data is loaded, the sample application executes a series of AdHoc SQL queries, demonstrating various SQL queries on the JSON data.


Quickstart
---------------------------
Make sure "bin" inside the VoltDB kit is in your PATH.  Then open a shell and go to the examples/json-sessions directory, then execute the following commands to start the database:

    voltdb init
    voltdb start

Wait until you see "Server completed initialization."
Open a new shell in the same directory and run the following to load the schema:

    sqlcmd < ddl.sql

In the same shell, run the following script to preload some data and run the demo client application:

    ./run.sh client

You can stop the server or running client at any time with `ctrl-c` or `SIGINT`.  Of course VoltDB can also run in the background using the -B option, in which case you can stop it with the `voltadmin shutdown` command.

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

If you change the client or procedure Java code, you must recompile the jars by deleting them in the shell or using `./run.sh jars`.


Customizing this Example
---------------------------
See the "deployment-examples" directory within the "examples" directory for ways to alter the default single-node, no authorization deployment style of the examples. There are readme files and example deployment XML files for different clustering, authorization, export, logging and persistence settings.
