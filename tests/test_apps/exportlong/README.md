# ExportLong: export longevity test

## Overview
ExportLong is a client test program intended to exercise the export subsystem over long periods of time.
It was originally created to investigate ENG-21601, ENG-21637, ENG-21670. The test creates data streams exporting to the export targets and creates 1 thread for each stream to invoke the standard insert procedure to send data to the stream for the duration of the test, which may be infinite.

The 'sources' parameter defines the number of STREAM created by the test: the streams are named SOURCE000, SOURCE001, SOURCE002, etc... All the source streams have the same structure. Up to 1,000 streams are allowed.

The 'targets' parameter defines the number of export targets these sources export to (EXPORT TO TARGET....): the targets are named TARGET00, TARGET01, TARGET02, etc...  Up to 100 targets are allowed. The source streams are assigned to the target in an round-robin fashion in order to spread the sources across the targets.

The 'duration' parameter defines the number of seconds the test should run, with 0 being 'infinite' i.e. the test will run until interrupted by the user.  

The 'rate' parameter defines the number of invocations per second each thread will execute to insert rows in the stream.  

The actual export targets to be used in the test must be configured in the server prior to starting the test client. If left undefined the test client will insert rows that will not be exported and will simply increase the TUPLE_PENDING counts and increase disk usage. This will result in  unexported tuples preventing a proper 'shutdown --save'.

A 'run.sh' script facilitates running 1-node standalone tests on localhost, and provides examples for various test cases.

## 1-node standalone tests

Use 'run.sh' to run a 1-node standalone test on localhost:

    # manually setup and configure the export targets TARGET00, TARGET01 etc, in the deployment.xml  
    # then start the server as follows:

    ./run.sh server &

    # then start one of the test cases, e.g. for a 120s test with 10 sources, 2 targets, 1000 invocations/second:

    ./run.sh client

Once the test finishes the VoltDB instance can be shut down or the process killed.  

## Multinode or remote tests

If testing against a multinode K > 1 configuration, or against a remote cluster, the VoltDB cluster must be configured and started separately and the ExportLong client must be invoked with the 'servers' parameter set to the correct host/ports

# Tests exporting to external databases via JDBC (e.g. Oracle)

If testing against an external database, the tables must be pre-created in the target database, the 'createtable' parameter must be set to false, and the 'ignoregenerations' parameter must be set to true. Below is an example target configuration for Oracle:


    <configuration target="TARGET00" enabled="true" type="jdbc" exportconnectorclass="">  
      <property name="jdbcdriver">oracle.jdbc.driver.OracleDriver</property>  
      <property name="jdbcurl">jdbc:oracle:thin:@//10.10.182.248:1539/XE</property>  
      <property name="jdbcuser">SYSTEM</property>  
      <property name="jdbcpassword">oracle</property>  
      <property name="createtable">false</property>  
      <property name="ignoregenerations">true</property>  
      <property name="skipinternals">true</property>  
    </configuration>  

A model for creating the tables in Oracle:

    CREATE TABLE "SOURCE000" ("ID" NUMBER(19), "TYPE_TINYINT" NUMBER(3), "TYPE_SMALLINT" NUMBER(5),
    "TYPE_INTEGER" NUMBER(10), "TYPE_BIGINT" NUMBER(19), "TYPE_TIMESTAMP" TIMESTAMP WITH TIME ZONE,
    "TYPE_FLOAT" DOUBLE PRECISION, "TYPE_DECIMAL" DECIMAL(38,12), "TYPE_VARCHAR1024"
     VARCHAR2(1024));

## Test parameters

The ExportLong test client accepts the following parameters:

    @Option(desc = "Comma separated list of the form server[:port] to connect to.")
    String servers = "localhost";

    @Option(desc = "Test duration, in seconds, 0 = infinite, default = 120s)")
    long duration = 120;

    @Option(desc = "Logging interval, in seconds, 0 = no logging, default = 60s)")
    long loginterval = 60;

    @Option(desc = "Number of source streams, default 10, up to 1000, will be named SOURCE000, SOURCE001, etc..")
    int sources = 10;

    @Option(desc = "Number of export targets, default 2, up to , will be named TARGET00, TARGET01, etc..")
    int targets = 2;

    @Option(desc = "If true, create the source streams, default true")
    boolean create = true;

    @Option(desc = "Number of invocations per second, per source, default = 1000")
    int rate = 1000;

    @Option(desc = "Number of row identifiers, per source, default = 1000")
    int idcount = 1000;
