# VoltDB Example App: Fraud Detection (Realtime and In Transaction fraud detection and Prevention)

Use Case
--------
This application demonstrates how stream of data can be ingested into VoltDB and while ingesting real time decisions such as fraud prevention can be made by simply using power of SQL.
This application performs ingestion of metro card swipes and train activity from 2 different kafka topics. The ingestion from kafka is tied to java stored procedures which detect anomalies and
computes various VIEWS on data such as

1. Busiest station
2. Average Wait time for passengers per station
3. Acceptance rate (Fraud Prevention)

A simple javascript driven dashboard then displays this data.

Quickstart
---------------------------
This example application uses VoltDB, node.js, and Kafka to emulate a distributed environment. To run the example, be sure you have node.js and Kafka (version 8) installed and running. Node.js must be installed on the local machine while Kakfa can be running either locally or on a separate server. (If Kafka is running remotely, edit the deployment*.xml file to point to the server  and edit the run.sh file to modify the train() function.)

Once node.js and Kafka (kafka 8) is ready, you can start the example application. Make sure "bin" inside the VoltDB kit is in your PATH. Then open a shell, go to the examples/fraud-detection directory, and execute the following commands to set up the database:

./run.sh server
./run.sh init (Before initializing make sure your kafka is configured card_swipes and train_activity topics)

Then run the demo application:

./run.sh train (This produces simulated data of card swipes and train activity)
./run.sh npminstall (To setup node plugins)
./run.sh nodeserver (To start the Fraud Detection Dashboard)

If you're running the example on a VoltDB cluster, rather than your local desktop or laptop, run `./run.sh nodeserver` in a new shell on one of the machines in the cluster, then connect to your dashboard from your browser at [http://servername:3000](http://servername:3000).

You can stop the server, running client, or webserver at any time with `Ctrl-c` or `SIGINT`.  Of course VoltDB can also run in the background using the -B option, in which case you can stop it with the `voltadmin shutdown` command.

Note: When you run.sh train the data events to kafka are removed by your kafka retention policy, running this over and over can use disk space and make your machine/VoltDB run slow.

Note: that the downloaded VoltDB kits include pre-compiled stored procedures and client code as jarfiles. To run the example from a source build, it may be necessary to compile the Java source code by typing "run.sh jars" before. Note that this step requires a full Java JDK.

Using the run.sh script
---------------------------
VoltDB examples come with a run.sh shell script that simplifies compiling and running the example client application and other parts of the examples.
- *run.sh* : start the server
- *run.sh server* : start the server
- *run.sh init* : compile stored procedures and load the schema, stored procedures and sample data.
- *run.sh train* : Run the simulator application which runs train and card swipe activities
- *run.sh jars* : compile all Java clients and stored procedures into two Java jarfiles
- *run.sh clean* : remove compilation and runtime artifacts, compiled jarfiles *and* sampledata
- *run.sh npminstall* : install the necessary npm packages
- *run.sh nodeserver* : run the node server over http on port 3000

If you change the client or procedure Java code, you must recompile the jars by deleting them in the shell or using `./run.sh jars`.
