Examples Guide
================

Example Projects
--------------------------

The following projects are included in the examples directory:

### json-sessions ####
  This example shows how to use flexible schema and JSON within
  a VoltDB application.

### voltkv ###
  This directory contains the sources for the "Key-Value" implementation
  in VoltDB.

### voter ###
  This example is a simulation of a telephone based voting process
  where callers are allowed a limited number of votes.

### windowing ###

This example shows an application that takes in time-series data, runs analytic queries over several time-windows, and deletes older data as it either ages out or passes a rowcount threshold.

Getting Started
--------------------------

Each example contains a README.md file with full instrcutions for running, building, etc.

The examples all contain a bash script file named "run.sh" that contains functions for doing most things. This is generally only strictly needed for running the client apps, but can be a useful reference for other operations.

Usually the follow steps will run an example:

1. Make sure "bin" inside the VoltDB kit is in your path.
2. Type "voltdb create" to start an empty, single-node VoltDB server.
3. Type "sqlcmd < ddl.sql" to load the schema and the jarfile of procedures into VoltDB.
4. Type "./run.sh client" to run the client code.

Typing "./run.sh help" will also show the available targets for each example.

When finished, hit Ctrl C in the server terminal window to stop the VoltDB database.
