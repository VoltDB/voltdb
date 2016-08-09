# Stored Value Card Application

This application performs high velocity transaction processing for stored value cards.  These transactions include:

- Pre-Authorizations, where the available balance is checked if sufficient and then modified, but the purchase is not completed.
- Purchase, where the previously-authorized purchase is completed
- Transfer, where a balance transfer is made between two cards.


See below for instructions on running these applications. 


Quickstart
------------------
VoltDB examples come with a run.sh script that sets up some environment and saves some of the typing needed to work with Java clients. It is very readable and when executed, shows what is precisely being run to accomplish a given task.

1. Make sure "bin" inside the VoltDB kit is in your path.
2. Type "voltdb create -f" to start an empty, single-node VoltDB server.
3. Open a new shell in the same directory and type "sqlcmd < ddl.sql" to load the schema and the jarfile of procedures into VoltDB.
4. Type "./run.sh client" to run the client code.

