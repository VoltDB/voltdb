#!/usr/bin/env bash
# Starts up a VoltDB server, to be used to test the VMC (VoltDB Management
# Center), by running the GEB tests of it (with a separate script)

# Build the jar file used by FullDdlSqlTest, to test CREATE PROCEDURE FROM CLASS
jar cf fullddlfeatures.jar -C ../../../../obj/release/testprocs org/voltdb_testprocs/fullddlfeatures/testCreateProcFromClassProc.class

# Add (pro or community) bin directory to PATH
if [[ "$@" == *"-p"* ]]; then  # -p arg means to use "pro"
    CURRENT_DIR=`pwd`
    cd ../../../../../pro/obj/pro/
    SUBDIR=`ls -d1 volt* | grep -v tar.gz`
    cd $CURRENT_DIR
    PATH=$PATH:../../../../../pro/obj/pro/$SUBDIR/bin
else  # default is to use "community"
    PATH=$PATH:../../../../bin
fi
echo "which voltdb:" `which voltdb`

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
