# VoltDB Example App: Fraud Detection (Realtime and In Transaction)

Use Case
--------
This application performs high velocity transaction processing for metro cards. 

- Run a simulated train and swipes of cards detecting fraud (during ./run.sh train)

Quickstart
---------------------------
Make sure "bin" inside the VoltDB kit is in your PATH.  Then open a shell and go to the examples/fraud-detection directory, then execute the following commands to start the database:

Make sure you have kafka (version 8) running locally with `card_swipes` and `train_activity` topics. If you have kafka running somewhere elase, modify deployment*.xml to point to connect location and run.sh train() function.

    ./run.sh server
    ./run.sh init

Then run the demo application:

    ./run.sh train
    ./run.sh npminstall (To setup node plugins)
    ./run.sh nodeserver (This will show Fraud Detection Dashboard)

If you're running the example on a VoltDB cluster, rather than your local desktop or laptop, run `./run.sh nodeserver` in a new shell on one of the machines in the cluster, then connect to your dashboard from your browser at [http://servername:3000](http://servername:3000).

You can stop the server, running client, or webserver at any time with `Ctrl-c` or `SIGINT`.  Of course VoltDB can also run in the background using the -B option, in which case you can stop it with the `voltadmin shutdown` command.

Note that the downloaded VoltDB kits include pre-compiled stored procedures and client code as jarfiles. To run the example from a source build, it may be necessary to compile the Java source code by typing "run.sh jars" before. Note that this step requires a full Java JDK.

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
