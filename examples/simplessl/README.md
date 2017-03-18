# SimpleSSL Application

This example application demonstrates how to configure and run VoltDB with SSL and basic authentication.

VoltDB servers using SSL must be provided with a private key and a certificate which establishes trust for that key. Clients are provided a one or more certificates that establishes that the server can be trusted. 'sqlcmd' must also be provided with certificates in order to load the DDL.

Clients represent ...



The "SimpleSSL" application is not designed for performance assessments. SSL support is present in the "Voter" and "VoltKV" examples, both of which provide benchmarking applications.


Preparation
---------------------------

The first time you run this example, you will need to configure your key and certificate stores. Both the key store and trust store must be in Java's 'Java Keystore' format for VoltDB and its clients to use them. Generating these stores requires access to a JDK (Java Development Kit), though a JRE is sufficient for running VoltDB. The paths to these files are kept in a Java 'properties' file. 

Verify that both your JVM and your client support the encryption protocol you are looking to use. This is essential to ensure that VoltDB clients connect to the server in a fast and secure manner, since there are known limitations with Java and other key tools. Oracle Java (as of Java 8) requires the "Unlimited Strength Java(TM) Cryptography Extension Policy" package to use more secure encryption schemes, or a comparable substitute such as Bouncy Castle. Python 2 is also known not to support TLS 1.2, which has many advantages over prior versions. It is your responsibility to respect your government's regulations on the use and import of cryptography. 

The Internet has many more thorough guides, but here's a quick way to generate a self-signed certificate for testing using Linux. This is not recommended for production use.





Many attributes of the application are customizable through arguments passed to the client. These attributes can be adjusted by modifying the arguments to the "client" target in run.sh.




Quickstart
---------------------------
Make sure "bin" inside the VoltDB kit is in your PATH.  Then open a shell and go to the examples/voter directory, then execute the following commands to start the database:

    voltdb init
    voltdb start

Wait until you see "Server completed initialization."
Open a new shell in the same directory and run the following to load the schema:

    ./run.sh init

In the same shell, run the following script several times. This will run the demo client application:

    ./run.sh client


You can stop the server, running client, or webserver at any time with `Ctrl-c` or `SIGINT`. When running VoltDB in the background using the -B option, you can stop it with the `voltadmin shutdown` command.

Note that the downloaded VoltDB kits include pre-compiled client code as jarfiles. To run the example from a source build, it may be necessary to compile the Java source code by typing "run.sh jars" before step 3 above. This requires a full Java JDK.


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
To further customize the cluster, see the "deployment-examples" directory within the "examples" directory. There are README files and example deployment XML files for different clustering, authorization, export, logging and persistence settings.
