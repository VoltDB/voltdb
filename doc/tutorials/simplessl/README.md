# SimpleSSL Application

This example application demonstrates how to configure and run VoltDB while using SSL to secure connections to the database.

VoltDB servers using SSL must be provided with a private key and a certificate which establishes trust for that key. Clients, including 'sqlcmd', are provided a one or more certificates that establishes that the server can be trusted.

The "SimpleSSL" application is not designed for performance assessments. SSL support is present in the "Voter" and "VoltKV" examples, both of which provide benchmarking applications.


Preparation
---------------------------

Before running this example for the first time, you will need to configure your key and certificate stores. Both the key store and trust store must be in Java's 'Java Keystore' format for VoltDB and its clients to use them. Generating these stores requires access to a JDK (Java Development Kit). 
- The server finds its key and certificate stores in the deployment.xml configuration file.
- Clients use a plain-text Java 'properties' file, such as the supplied 'SSL.properties'.
- The file names aren't important. Just modify run.sh if you prefer different file names.

Verify that both your JVM and your client support the encryption protocol you are looking to use. You may need to install additional packages to meet your security and performance goals. Oracle Java (as of Java 8) requires the "Unlimited Strength Java(TM) Cryptography Extension Policy" package to use more secure encryption schemes, or a comparable substitute such as Bouncy Castle. The supplied client will warn you if this is not installed. If you're using Python 2, be aware that it does not support TLS 1.2 - the minimum version recommended by VoltDB. Keep in mind that some governments restrict the export and use of cryptography, and it is your responsibility to abide by any laws that apply.

Here's a quick way to generate a self-signed certificate for testing this example. Using a self-signed certificate is not recommended for production or sensitive test data.

For a more detailed description of the process, see the following:
https://docs.oracle.com/cd/E19509-01/820-3503/6nf1il6er/index.html

The supplied SSL.properties and deployment.xml assume all files are in the 'simplessl' directory and all passwords are 'example'. Modify them if this is not true.

> keytool -genkey  -keystore example.keystore -storepass example -alias example -keyalg rsa -validity 365 -keysize 2048

> keytool -certreq -keystore example.keystore -storepass example -alias example -keyalg rsa -file example.csr

> keytool -gencert -keystore example.keystore -storepass example -alias example -infile example.csr -outfile example.cert -validity 365

> keytool -import  -keystore example.keystore -storepass example -alias example -file example.cert

> keytool -import -file example.cert -keystore example.truststore -storepass example -alias example 

To run the example from a source build, it may be necessary to compile the client source code by typing "run.sh jars". This requires a full Java JDK. Note that the downloaded VoltDB kits include pre-compiled client code.


Quickstart
---------------------------
Execute the following command to start the database:

    ./run.sh server

Wait until you see "Server completed initialization."
Open a new shell in the same directory and run the following to load the schema:

    ./run.sh init

In the same shell, run the following script several times. This will run the demo client application:

    ./run.sh client

You can stop the server or running client at any time with `Ctrl-c` or `SIGINT`. When running VoltDB in the background using the -B option, you can stop it with the `voltadmin shutdown` command.


Using the run.sh script
---------------------------
The run.sh shell script simplifies compiling and running the server and client applications.
- *run.sh* : start the server
- *run.sh server* : start the server
- *run.sh init* : compile stored procedures and load the schema and stored procedures
- *run.sh jars* : compile all Java clients into a jar file
- *run.sh client* : start the client, more than 1 client is permitted
- *run.sh clean* : remove compilation and runtime artifacts
- *run.sh cleanall* : remove compilation and runtime artifacts *and* the included client jar file

If you change the client Java code, you must recompile the jar by using `./run.sh jars`.


Customizing this Example
---------------------------
To further customize the cluster, see the "deployment-examples" directory within the "examples" directory. There are README files and example deployment XML files for different clustering, authorization, export, logging and persistence settings.
