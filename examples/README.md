Examples Guide
================

Example Projects
--------------------------

The following projects are included in the examples directory:

### contentionmark ###
  
ContentionMark is a small VoltDB application that measures VoltDB throughput under extreme contention. By default, ContentionMark creates one row, thenupdates it as fast as VoltDB will let it, one transaction per update.

### geospatial ###

This example demonstrates geospatial functionality that was added to VoltDB in version 6.0.  The problem space for this demo is serving ads to many mobile device users based on their location in real time.

### json-sessions ####

This example shows how to use flexible schema and JSON within a VoltDB application.

### uniquedevices ###

This example counts unique device ids as they stream into VoltDB. It demonstrates VoltDB's stream processing power and the use of external libraries in Java stored procedures. It is loosely based on a real problem previously solved using the Lambda architecture.

### voltkv ###

This directory contains the sources for a "Key-Value" implementation in VoltDB along with a simple benchmark.

VoltKV includes synchronous, asynchronous and JDBC versions of the client code.

### voter ###
This example is a simulation of a telephone based voting process where callers are allowed a limited number of votes.
  
Voter includes synchronous, asynchronous and JDBC versions of the client code.

### windowing ###

This example shows an application that takes in time-series data, runs analytic queries over several time-windows, and deletes older data as it either ages out or passes a rowcount threshold.
  
### windowing-with-ddl ###

A modified version of the windowing example that demonstrates the V5 feature that lets users define how to age out old tuples using a special constraint on the table definition.  This example has no Java stored procedures.
  
Getting Started
--------------------------

Each example contains a README.md file with full instructions for running, building, etc.

The examples all contain a bash script file named "run.sh" that contains functions for doing most things. This is generally only strictly needed for running the client apps, but can be a useful reference for other operations.

The following steps will run an example:

1. Make sure "bin" inside the VoltDB kit is in your path.
2. Type "voltdb create" to start an empty, single-node VoltDB server.
3. Open a second terminal window in the same working directory.
4. Type "sqlcmd < ddl.sql" to load the schema and the jarfile of procedures into VoltDB.
5. Type "./run.sh client" to run the client code.

Typing "./run.sh help" will also show the available targets for each example.

When finished, hit Ctrl C in the server terminal window to stop the VoltDB database.
