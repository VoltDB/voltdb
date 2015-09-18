#!/usr/bin/env bash
# Starts up a VoltDB server, to be used to test the VMC (VoltDB Management
# Center), by running the GEB tests of it (with a separate script)

# Build the jar file used by FullDdlSqlTest, to test CREATE PROCEDURE FROM CLASS
jar cf fullddlfeatures.jar -C ../../../../obj/release/testprocs org/voltdb_testprocs/fullddlfeatures/testCreateProcFromClassProc.class

# Start the VoltDB server
voltdb create -d deployment.xml &

# Run sqlcmd, and use it to load DDL and (stored procedure) classes;
# make multiple attempts, until the server has successfully started up
for i in {1..300}; do
  sleep 1
  SQLCMD_RESPONSE=$(sqlcmd < ddl.sql 2>&1)
  if [ "$SQLCMD_RESPONSE" != "Connection refused" ]; then
    echo "Loaded ddl.sql (server started; sqlcmd ran after $i attempts)"
    break
  fi
done
