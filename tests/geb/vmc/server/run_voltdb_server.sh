#!/usr/bin/env bash
# Starts up a VoltDB server, to be used to test the VMC (VoltDB Management
# Center), by running the GEB tests of it (with a separate script). The
# following options are possible:
#   -p :  run the "pro" version of VoltDB (default is "community" version)
#   -g :  run the "Genqa" test app (with the "pro" version of VoltDB)
#   -v :  run the "Voter" example app (with either version of VoltDB)

# Build the jar file used by FullDdlSqlTest, to test CREATE PROCEDURE FROM CLASS
jar cf fullddlfeatures.jar -C ../../../../obj/release/testprocs org/voltdb_testprocs/fullddlfeatures/testCreateProcFromClassProc.class

# Set default values - the default is to use the "community" version of VoltDB,
# using the deployment.xml and ddl.sql files in this directory
CURRENT_DIR=`pwd`
EXTRA_PATH="$CURRENT_DIR/../../../../bin"
DEPLOY="deployment.xml"
DDL_DIR="."

# Using a -p arg means to use the "pro" version of VoltDB
if [[ "$@" == *"-p"* ]]; then
    SUBDIR=$(basename `ls -d1 ../../../../../pro/obj/pro/volt* | grep -v tar.gz`)
    EXTRA_PATH="$CURRENT_DIR/../../../../../pro/obj/pro/$SUBDIR/bin"
    DEPLOY="deploy_pro.xml"
fi

# Using a -g arg means to run the "Genqa" test app (& the "pro" version of VoltDB)
if [[ "$@" == *"-g"* ]]; then
    SUBDIR=$(basename `ls -d1 ../../../../../pro/obj/pro/volt* | grep -v tar.gz`)
    EXTRA_PATH="$CURRENT_DIR/../../../../../pro/obj/pro/$SUBDIR/bin"
    DEPLOY="../../../test_apps/genqa/deployment.xml ../../../test_apps/genqa/genqa.jar"
    DDL_DIR="../../../test_apps/genqa/"
# Using a -v arg means to run the "Voter" example app
elif [[ "$@" == *"-v"* ]]; then
    DEPLOY="../../../../examples/voter/deployment.xml"
    DDL_DIR="../../../../examples/voter/"
fi

# Add the (community or pro) bin directory to the PATH
PATH=$PATH:$EXTRA_PATH

# Start the VoltDB server
echo "which voltdb:" `which voltdb`
echo "Executing   : voltdb create -d $DEPLOY &"
voltdb create -d $DEPLOY &

# Run sqlcmd, from the specified directory, and use it to load DDL and (stored
# procedure) classes; make multiple attempts, until the server has successfully
# started up
cd $DDL_DIR
echo "pwd         :" `pwd`
echo "which sqlcmd:" `which sqlcmd`
echo "Executing   : sqlcmd < ddl.sql 2>&1"
for i in {1..120}; do
    sleep 1
    SQLCMD_RESPONSE=$(sqlcmd < ddl.sql 2>&1)
    if [[ "$SQLCMD_RESPONSE" == *"command not found"* ]]; then
        echo "sqlcmd response:" $SQLCMD_RESPONSE
        break
    elif [ "$SQLCMD_RESPONSE" != "Connection refused" ]; then
        echo "Loaded ddl.sql (server started; sqlcmd ran after $i attempts)"
        cd $CURRENT_DIR
        exit 0
    fi
done
cd $CURRENT_DIR
echo "Failed to load ddl.sql file via sqlcmd, after $i attempt(s)"
exit 1
