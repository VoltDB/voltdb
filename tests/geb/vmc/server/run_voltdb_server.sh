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
DDL_FILE="ddl.sql"
GENQA=
VOTER=

# Using a -p arg means to use the "pro" version of VoltDB
if [[ "$@" == *"-p"* ]]; then
    SUBDIR=$(basename `ls -d1 ../../../../../pro/obj/pro/volt* | grep -v tar.gz`)
    EXTRA_PATH="$CURRENT_DIR/../../../../../pro/obj/pro/$SUBDIR/bin"
    DEPLOY="deploy_pro.xml"
fi

# Using a -g arg means to run the "Genqa" test app (& the "pro" version of VoltDB)
if [[ "$@" == *"-g"* ]]; then
    GENQA=true
    SUBDIR=$(basename `ls -d1 ../../../../../pro/obj/pro/volt* | grep -v tar.gz`)
    EXTRA_PATH="$CURRENT_DIR/../../../../../pro/obj/pro/$SUBDIR/bin"
    DEPLOY="../../../test_apps/genqa/deployment.xml ../../../test_apps/genqa/genqa.jar"
    DDL_DIR="../../../test_apps/genqa/"
# Using a -v arg means to run the "Voter" example app
elif [[ "$@" == *"-v"* ]]; then
    VOTER=true
    DEPLOY="../../../../examples/voter/deployment.xml"
    DDL_DIR="../../../../examples/voter/"
fi

# Add the (community or pro) bin directory to the PATH
PATH=$PATH:$EXTRA_PATH

# Start the VoltDB server
VOLTDB_COMMAND="voltdb create --force -d $DEPLOY &"
echo "which voltdb  :" `which voltdb`
echo "voltdb version:" `voltdb --version`
echo "Executing     : $VOLTDB_COMMAND"
eval $VOLTDB_COMMAND

# Run sqlcmd, from the specified directory, and use it to load DDL and (stored
# procedure) classes; make multiple attempts, until the server has successfully
# started up
cd $DDL_DIR
SQLCMD_COMMAND="sqlcmd < $DDL_FILE 2>&1"
echo "pwd           :" `pwd`
echo "which sqlcmd  :" `which sqlcmd`
echo "Executing     : $SQLCMD_COMMAND"

for i in {1..120}; do
    SQLCMD_RESPONSE=$(eval $SQLCMD_COMMAND)

    # If the VoltDB server has not yet completed initialization, keep waiting
    if [[ "$SQLCMD_RESPONSE" == *"Unable to connect"* || "$SQLCMD_RESPONSE" == *"Connection refused"* ]]; then
        #echo "debug: sqlcmd response: $SQLCMD_RESPONSE"
        sleep 1

    # If the VoltDB server has processed the DDL file, we're done
    elif [[ "$SQLCMD_RESPONSE" == *"CREATE TABLE"* && "$SQLCMD_RESPONSE" == *"CREATE VIEW"* && \
            ($VOTER || "$SQLCMD_RESPONSE" == *"CREATE INDEX"*) && \
            ($GENQA || "$SQLCMD_RESPONSE" == *"CREATE PROCEDURE"*) ]]; then
        #echo -e "debug: sqlcmd response:\n$SQLCMD_RESPONSE"
        echo -e "\nLoaded $DDL_FILE (server started; sqlcmd ran after $i attempts)\n"
        cd $CURRENT_DIR
        exit 0

    # Otherwise, print an error message and exit
    else
        echo -e "\nsqlcmd response had error(s):\n$SQLCMD_RESPONSE\n"
        break
    fi
done
cd $CURRENT_DIR
echo -e "\n***** Failed to load $DDL_FILE file via sqlcmd, after $i attempt(s)! *****\n"
exit 1
