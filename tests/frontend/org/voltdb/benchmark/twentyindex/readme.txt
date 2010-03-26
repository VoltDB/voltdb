20 Index Application
====================

Application consists of 3 tables:
  table1 : maintains a row for every event
  table2 : maintains a row for every mainId/eventId combination
  table3 : maintains a row for every mainId (20 columns, each column is individually indexed) 

Purpose:
  This application shows how VoltDB handles a table with frequent insert/update activity and 20 indexes.


Ant targets:
  ant benchmarklocal
    Run the server and client on the local machine.

  ant benchmarkcluster
    Run the server(s) and client(s) on the cluster.

build.xml
  Set a max transaction rate by setting <param name="txnrate" value="3000"/> in the ant build target.

