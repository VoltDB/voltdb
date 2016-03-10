#!/usr/bin/env bash

. ./compile.sh

HOST=voltserver01
LICENSE=$VOLTDB_HOME/voltdb/license.xml
DEPLOY=deployment-cluster.xml

mkdir -p /tmp/voltdb

# assume 4.0 syntax
COMMAND="voltdb create -H $HOST -d $DEPLOY -l $LICENSE ${CATALOG_NAME}.jar"

# start database in background
nohup $COMMAND > log/nohup.log 2>&1 &

echo    
echo "-----------------------------------------------------------------"
echo "Starting VoltDB using commands:"
echo "    cd db"
echo "    nohup $COMMAND > log/nohup.log 2>&1 &"
echo "    tail -f log/nohup.log"
echo 
echo "The database will be available when you see:"
echo "Server completed initialization."
echo
echo "Use Ctrl-C to stop tailing the log file.  VoltDB will keep running."
echo 
echo "To stop the database, Ctrl-C and then use the command:"
echo "    voltadmin shutdown"
echo "-----------------------------------------------------------------"
echo

# tail the console output, so you can see when the server starts
tail -f log/nohup.log
