#!/usr/bin/env bash

initialize_configuration()
{
    if [ ! -f $CONFIG_FILE ]; then
        echo "::: One-Time Initialization :::"
        echo "Looking for development root..."
        if [ ! -d $WORK_ROOT ]; then
            echo "Creating \"$WORK_ROOT\" directory..."
            mkdir $WORK_ROOT
        fi
        if [ -f $DEVELOPMENT_ROOT/voltdb/log4j.xml ]; then
            echo "Copying log4j.xml to \"work\"..."
            \cp $DEVELOPMENT_ROOT/voltdb/log4j.xml $WORK_ROOT/
        fi
        echo "Generating $WORK_ROOT/$CONFIG_NAME..."
        cat <<EOF > $CONFIG_FILE || exit 1
### General configuration (use bash syntax and absolute paths)
# Supports only one host for now.
RUNTIME_ROOT=$WORK_ROOT/runtime
DEVELOPMENT_ROOT=$DEVELOPMENT_ROOT
SEED_ROOT=$WORK_ROOT/seed/$APPLICATION_NAME
SNAPSHOT_ROOT=$WORK_ROOT/snapshots
LOG4J_CONFIG=$WORK_ROOT/log4j.xml
APPLICATION_NAME=voter
APPLICATION_CLASS=SyncBenchmark
APPLICATION_OPTIONS="--warmup=6 --contestants=6 --maxvotes=2 --threads=40"
COMPARISON_RELEASE=3.0
SERVER_HOST=localhost
SERVER_PORT=21212
CLIENT_HOST=localhost
HOST_COUNT=1
SITES_PER_HOST=6
K_FACTOR=0
DURATION=300
DISPLAY_INTERVAL=10
SEED_NONCE=SEED
SEED_DURATION=600
JAVA_HEAP_MAX=1024m
EOF
        echo ""
        echo "Please edit $CONFIG_FILE before re-running."
        exit 1
    fi
}

kill_running_server()
{
    local SERVER_PID
    for SERVER_PID in $(jps | grep VoltDB | awk '{print $1}'); do
        echo "Killing old server ($SERVER_PID)..."
        kill $SERVER_PID
    done
}

prepare_files()
{
    mkdir -p $RUN_OUTPUT_ROOT
    test -d $APPLICATION_NAME && \rm -rf $APPLICATION_NAME/
    test -d $ARCHIVE_ROOT || mkdir $ARCHIVE_ROOT
    test -d $RUNTIME_ROOT && \rm -rf $RUNTIME_ROOT
    mkdir -p $RUNTIME_ROOT || exit 1
    test -d $SNAPSHOT_ROOT && \rm -rf $SNAPSHOT_ROOT
    mkdir -p $SNAPSHOT_ROOT || exit 1
    cat <<EOF > $DEPLOYMENT_NOSNAP || exit 1
<?xml version="1.0"?>
<deployment>
    <cluster hostcount="$HOST_COUNT" sitesperhost="$SITES_PER_HOST" kfactor="$K_FACTOR" />
    <httpd enabled="true">
        <jsonapi enabled="true" />
    </httpd>
    <paths>
        <voltdbroot path="$RUNTIME_ROOT" />
    </paths>
</deployment>
EOF
    cat <<EOF > $DEPLOYMENT_SNAP || exit 1
<?xml version="1.0"?>
<deployment>
    <cluster hostcount="1" sitesperhost="6" kfactor="0" />
    <httpd enabled="true">
        <jsonapi enabled="true" />
    </httpd>
    <snapshot prefix="auto" frequency="10s" retain="1" />
    <paths>
        <voltdbroot path="$RUNTIME_ROOT" />
        <snapshots path="$SNAPSHOT_ROOT/" />
    </paths>
</deployment>
EOF
    pushd $WORK_ROOT > /dev/null
    COMPARISON_TARBALL=LINUX-voltdb-$COMPARISON_RELEASE.tar.gz
    if [ ! -e $COMPARISON_TARBALL ]; then
        URL=http://volt0/kits/released/$COMPARISON_RELEASE-release/$COMPARISON_TARBALL
        echo "Downloading $URL..."
        if ! wget -q $URL; then
            echo "ERROR: Failed to download $URL"
            exit 1
        fi
    fi
    \cp -a $DEVELOPMENT_ROOT/examples/$APPLICATION_NAME/ . || exit 1
    tar xfz $COMPARISON_TARBALL || exit 1
}

run()
{
    local TEST_NAME=$1
    local DISTRIBUTION_ROOT=$2
    local DEPLOYMENT_FILE=$3
    local APPLICATION_CLASSPATH=$({ \
        \ls -1 "$DISTRIBUTION_ROOT/voltdb"/voltdb-*.jar; \
        \ls -1 "$DISTRIBUTION_ROOT/lib"/*.jar; \
        \ls -1 "$DISTRIBUTION_ROOT/lib"/extension/*.jar; \
    } 2> /dev/null | paste -sd ':' - )
    local CLIENT_SCRIPT=$PWD/client.sh
    echo "
======================================================================
 Run $TEST_NAME ($DISTRIBUTION_ROOT)
======================================================================
"
    prepare_run $TEST_NAME $CLIENT_SCRIPT $APPLICATION_CLASSPATH
    compile_application $TEST_NAME $APPLICATION_CLASSPATH
    create_catalog $TEST_NAME $APPLICATION_CLASSPATH
    start_server $TEST_NAME $DISTRIBUTION_ROOT $APPLICATION_CLASSPATH $DEPLOYMENT_FILE
    local SERVER_PID=$!
    load_data $TEST_NAME $DISTRIBUTION_ROOT $APPLICATION_CLASSPATH
    start_client $TEST_NAME $CLIENT_SCRIPT
    kill_server $TEST_NAME $SERVER_PID
    finalize_run $TEST_NAME
}

prepare_run()
{
    local TEST_NAME=$1
    local CLIENT_SCRIPT=$2
    local APPLICATION_CLASSPATH=$3
    local OUTPUT_DIRECTORY=$RUN_OUTPUT_ROOT/$TEST_NAME
    local BASE_CLASSPATH=".:$DEVELOPMENT_ROOT/lib/*:$DEVELOPMENT_ROOT/voltdb/*"
    mkdir -p $OUTPUT_DIRECTORY
    rm -rf obj debugoutput $APPLICATION_NAME.jar voltdbroot
    mkdir -p obj
    rm -f test.txt log/*
    cat <<-EOF > $CLIENT_SCRIPT || exit 1
#!/usr/bin/env bash
cd $WORK_ROOT/$APPLICATION_NAME
java -classpath obj:$BASE_CLASSPATH:$APPLICATION_CLASSPATH:obj \
    -Dlog4j.configuration=file://$LOG4J_CONFIG \
    $APPLICATION_NAME.$APPLICATION_CLASS \
    --displayinterval=$DISPLAY_INTERVAL \
    --duration=$DURATION \
    --servers=$SERVER_HOST:$SERVER_PORT \
    $APPLICATION_OPTIONS
EOF
    chmod +x $CLIENT_SCRIPT || exit 1
}

compile_application()
{
    local TEST_NAME=$1
    local APPLICATION_CLASSPATH=$2
    local OUTPUT_DIRECTORY=$RUN_OUTPUT_ROOT/$TEST_NAME
    local BASE_CLASSPATH=".:$DEVELOPMENT_ROOT/lib/*:$DEVELOPMENT_ROOT/voltdb/*"
    echo "Compiling source files..."
    javac -target 1.6 -source 1.6 -classpath $BASE_CLASSPATH:$APPLICATION_CLASSPATH -d obj \
        src/$APPLICATION_NAME/*.java src/$APPLICATION_NAME/procedures/*.java \
            > $OUTPUT_DIRECTORY/srccompile.txt || exit 1
}

create_catalog()
{
    local TEST_NAME=$1
    local APPLICATION_CLASSPATH=$2
    local OUTPUT_DIRECTORY=$RUN_OUTPUT_ROOT/$TEST_NAME
    echo "Compiling the catalog..."
    java -Xmx512m -classpath $APPLICATION_CLASSPATH:obj -Dlog4j.configuration=file://$LOG4J_CONFIG \
        org.voltdb.compiler.VoltCompiler $APPLICATION_NAME.jar ddl.sql \
            > $OUTPUT_DIRECTORY/catalog.txt
    if [ $? -ne 0 ]; then
        cat $OUTPUT_DIRECTORY/catalog.txt
        exit 1
    fi
}

start_server()
{
    local TEST_NAME=$1
    local DISTRIBUTION_ROOT=$2
    local APPLICATION_CLASSPATH=$3
    local DEPLOYMENT_FILE=$4
    local OUTPUT_DIRECTORY=$RUN_OUTPUT_ROOT/$TEST_NAME
    echo "Starting $TEST_NAME server..."
    java -server -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp \
        -XX:-ReduceInitialCardMarks -Xmx$JAVA_HEAP_MAX \
        -Dlog4j.configuration=file://$LOG4J_CONFIG \
        -Djava.library.path=$DISTRIBUTION_ROOT/voltdb \
        -classpath $APPLICATION_CLASSPATH \
        org.voltdb.VoltDB create \
        catalog $APPLICATION_NAME.jar \
        deployment $DEPLOYMENT_FILE \
        host $SERVER_HOST \
            > $OUTPUT_DIRECTORY/server.txt &
    local SERVER_PID=$!
    echo -n "Waiting for $TEST_NAME server ($SERVER_PID)"
    while ! ( grep -q "Server completed initialization" $OUTPUT_DIRECTORY/server.txt 2> /dev/null ); do
        echo -n "."
        PS_LINE_COUNT=$(ps -p $SERVER_PID | wc -l)
        if [ $PS_LINE_COUNT -ne 2 ]; then
            echo "* Server process not found *"
            exit 1
        fi
        sleep 1
    done
    echo ""
}

load_data()
{
    local TEST_NAME=$1
    local DISTRIBUTION_ROOT=$2
    local APPLICATION_CLASSPATH=$3
    local BASE_CLASSPATH=".:$DEVELOPMENT_ROOT/lib/*:$DEVELOPMENT_ROOT/voltdb/*"
    if [ -n "$SEED_NONCE" ]; then
        if [ $(\ls -1 $SEED_ROOT/$SEED_NONCE-*.vpt 2> /dev/null | wc -l) -gt 0 ]; then
            echo "Restoring snapshot..."
            # Invoke through python in case execute permission is turned off, e.g. for a Parallels share.
            python $DISTRIBUTION_ROOT/bin/voltadmin restore $SEED_ROOT $SEED_NONCE || exit 1
        else
            echo "Generating seed data..."
            LOG4JOPT=-Dlog4j.configuration=file://$LOG4J_CONFIG
            java -classpath obj:$BASE_CLASSPATH:$APPLICATION_CLASSPATH:obj \
                $LOG4JOPT \
                $APPLICATION_NAME.$APPLICATION_CLASS \
                --displayinterval=60 \
                --duration=$SEED_DURATION \
                --servers=$SERVER_HOST:$SERVER_PORT \
                $APPLICATION_OPTIONS || exit 1
            echo "Saving snapshot..."
            test -d $SEED_ROOT || mkdir -p $SEED_ROOT
            # Invoke through python in case execute permission is turned off, e.g. for a Parallels share.
            python $DISTRIBUTION_ROOT/bin/voltadmin save $SEED_ROOT $SEED_NONCE || exit 1
        fi
    fi
}

start_client()
{
    local TEST_NAME=$1
    local CLIENT_SCRIPT=$2
    local OUTPUT_DIRECTORY=$RUN_OUTPUT_ROOT/$TEST_NAME
    echo "Starting $TEST_NAME client on $CLIENT_HOST..."
    ssh -t $CLIENT_HOST sh -c $CLIENT_SCRIPT | tee $OUTPUT_DIRECTORY/run.txt || exit 1
}

kill_server()
{
    local TEST_NAME=$1
    local SERVER_PID=$2
    echo "Killing $TEST_NAME server ($SERVER_PID)"
    PS_LINE_COUNT=0
    while [ $PS_LINE_COUNT -ne 2 ]; do
        echo -n "."
        PS_LINE_COUNT=$(ps -p $SERVER_PID | wc -l)
        sleep 1
    done
    kill $SERVER_PID
}

finalize_run()
{
    local TEST_NAME=$1
    local OUTPUT_DIRECTORY=$RUN_OUTPUT_ROOT/$TEST_NAME
    cp log/volt.log $OUTPUT_DIRECTORY/volt.log
}

### Main program

if [ -z "$1" ]; then
    echo "Usage: $(basename $0) WORK_DIRECTORY"
    exit 1
fi

# Globals
WORK_ROOT=$1
CONFIG_FILE=$WORK_ROOT/test.cfg
DEVELOPMENT_ROOT=$PWD
while [ "$DEVELOPMENT_ROOT" != "/" -a ! -e "$DEVELOPMENT_ROOT/build.xml" ]; do
    DEVELOPMENT_ROOT=$(dirname $DEVELOPMENT_ROOT)
done
TIMESTAMP=$(date "+%y%m%d%H%M%S")

initialize_configuration
source $CONFIG_FILE

# Derived globals
OUTPUT_ROOT=$WORK_ROOT/output
ARCHIVE_ROOT=$WORK_ROOT/archive
RUN_OUTPUT_ROOT="$OUTPUT_ROOT/$TIMESTAMP"
DEPLOYMENT_NOSNAP=$WORK_ROOT/deployment_nosnap.xml
DEPLOYMENT_SNAP=$WORK_ROOT/deployment_snap.xml

kill_running_server

prepare_files

COMPARISON_DISTRIBUTION_ROOT="$PWD/voltdb-$COMPARISON_RELEASE"
pushd $WORK_ROOT/$APPLICATION_NAME > /dev/null

run old-nosnap $COMPARISON_DISTRIBUTION_ROOT $DEPLOYMENT_NOSNAP
run old-snap   $COMPARISON_DISTRIBUTION_ROOT $DEPLOYMENT_SNAP
run new-nosnap $DEVELOPMENT_ROOT             $DEPLOYMENT_NOSNAP
run new-snap   $DEVELOPMENT_ROOT             $DEPLOYMENT_SNAP

popd > /dev/null
popd > /dev/null

python analyze.py $RUN_OUTPUT_ROOT | tee $RUN_OUTPUT_ROOT/results.txt

echo "Runtime output: $RUN_OUTPUT_ROOT"
