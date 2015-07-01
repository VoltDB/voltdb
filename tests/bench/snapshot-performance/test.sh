#!/usr/bin/env bash

initialize_configuration()
{
    if [ ! -f $CONFIG_FILE ]; then
        echo "::: One-Time Initialization :::"
        echo "Looking for development root..."
        if [ ! -d $WORK_ROOT ]; then
            echo "Creating \"$WORK_ROOT\" directory..."
            mkdir -p $WORK_ROOT
        fi
        if [ -f $DEVELOPMENT_ROOT/voltdb/log4j.xml ]; then
            echo "Copying log4j.xml to \"work\"..."
            \cp $DEVELOPMENT_ROOT/voltdb/log4j.xml $WORK_ROOT/
        fi
        echo "Generating $WORK_ROOT/$CONFIG_NAME..."
        cat <<EOF > $CONFIG_FILE || exit 1
### General configuration (use bash syntax and absolute paths)
# Current source root.
DEVELOPMENT_ROOT=$DEVELOPMENT_ROOT
# Comparison distribution version #, source root path, or distribution root path.
COMPARISON_DISTRIBUTION=???
# Where runtime files are saved.
RUNTIME_ROOT=$WORK_ROOT/runtime
# Where output files, including logs, profiles, etc. are saved.
OUTPUT_ROOT=$WORK_ROOT/output
SEED_ROOT=$WORK_ROOT/seed/$APPLICATION_NAME
SNAPSHOT_ROOT=$WORK_ROOT/snapshots
LOG4J_CONFIG=$WORK_ROOT/log4j.xml
APPLICATION_NAME=voter
APPLICATION_CLASS=SyncBenchmark
APPLICATION_OPTIONS="--warmup=6 --contestants=6 --maxvotes=2 --threads=40"
SERVER_HOST=localhost
SERVER_PORT=21212
CLIENT_HOST=localhost
# Supports only one host for now.
HOST_COUNT=1
SITES_PER_HOST=6
K_FACTOR=0
DURATION=240
DISPLAY_INTERVAL=60
SEED_NONCE=SEED
SEED_DURATION=360
JAVA_HEAP_MAX=1024m
SNAPSHOT_FREQUENCY=5s
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
    <cluster hostcount="$HOST_COUNT" sitesperhost="$SITES_PER_HOST" kfactor="$K_FACTOR" />
    <httpd enabled="true">
        <jsonapi enabled="true" />
    </httpd>
    <snapshot prefix="auto" frequency="$SNAPSHOT_FREQUENCY" retain="1" />
    <paths>
        <voltdbroot path="$RUNTIME_ROOT" />
        <snapshots path="$SNAPSHOT_ROOT/" />
    </paths>
</deployment>
EOF
    \cp -a $DEVELOPMENT_ROOT/examples/$APPLICATION_NAME/ $WORK_ROOT/ || exit 1
}

prepare_comparison_distribution()
{
    if [ "$COMPARISON_DISTRIBUTION" = "???" ]; then
        echo "ERROR: Please set COMPARISON_DISTRIBUTION in $CONFIG_FILE."
        exit 1
    fi
    if [[ $COMPARISON_DISTRIBUTION =~ ^[0-9]+[.][0-9]+$ ]]; then
        pushd $WORK_ROOT > /dev/null
        # Download (as needed) and extract distribution tarball based on a version number.
        COMPARISON_TARBALL=LINUX-voltdb-$COMPARISON_DISTRIBUTION
        if [ ! -e $COMPARISON_TARBALL ]; then
            URL=http://volt0/kits/released/$COMPARISON_DISTRIBUTION-release/$COMPARISON_TARBALL
            echo "Downloading $URL..."
            if ! wget -q $URL; then
                echo "ERROR: Failed to download $URL"
                exit 1
            fi
        fi
        echo "Extracting $COMPARISON_TARBALL..."
        tar xfz $COMPARISON_TARBALL || exit 1
        COMPARISON_DISTRIBUTION_ROOT=$PWD/voltdb-$COMPARISON_DISTRIBUTION
        popd > /dev/null
    else
        # Otherwise assume it is a distribution root path.
        COMPARISON_DISTRIBUTION_ROOT=$COMPARISON_DISTRIBUTION
    fi
    if [ ! -d $COMPARISON_DISTRIBUTION_ROOT/voltdb -o ! -d $COMPARISON_DISTRIBUTION_ROOT/lib ]; then
        echo "ERROR: $COMPARISON_DISTRIBUTION_ROOT is not a proper distribution root."
        exit 1
    fi
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
    javac -target 1.7 -source 1.7 -classpath $BASE_CLASSPATH:$APPLICATION_CLASSPATH -d obj \
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
            echo "Producing seed data..."
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

capture_stats()
{
    local TEST_NAME=$1
    local DISTRIBUTION_ROOT=$2
    local OUTPUT_DIRECTORY=$RUN_OUTPUT_ROOT/$TEST_NAME
    (python $DEVELOPMENT_ROOT/tools/volt sql "exec @Statistics PROCEDURE 0" | tee -a $OUTPUT_DIRECTORY/statistics.txt) || exit 1
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

profiler()
{
    local TEST_NAME=$1
    local ACTION=$2
    local OUTPUT_DIRECTORY=$RUN_OUTPUT_ROOT/$TEST_NAME
    if [ -n "$ZOOMSCRIPT" ]; then
        echo "Profiler: $ACTION"
        if [ $ACTION = start -o $ACTION = stop ]; then
            $ZOOMSCRIPT $ACTION || exit 1
        elif [ $ACTION = quit -o $ACTION = run ]; then
            for PID in $(pgrep zoom); do
                echo "Killing zoom ($PID)..."
                kill -s INT $PID
            done
            if [ $ACTION = run ]; then
                pushd $OUTPUT_DIRECTORY > /dev/null
                zoom run --allow_zoomscript --basename $TEST_NAME --output_text &
                test $? -ne 0 && exit 1
                ZOOM_PID=$!
                popd > /dev/null
            fi
        fi
    fi
}

run_test()
{
    local TEST_NAME=$1
    local DISTRIBUTION_ROOT=$2
    local DEPLOYMENT_FILE=$3
    local APPLICATION_CLASSPATH=$({ \
        \ls -1 "$DISTRIBUTION_ROOT/voltdb"/voltdb-*.jar; \
        \ls -1 "$DISTRIBUTION_ROOT/lib"/*.jar; \
        \ls -1 "$DISTRIBUTION_ROOT/lib"/extension/*.jar; \
    } 2> /dev/null | paste -sd ':' - )
    pushd $WORK_ROOT/$APPLICATION_NAME > /dev/null
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
    profiler $TEST_NAME run
    load_data $TEST_NAME $DISTRIBUTION_ROOT $APPLICATION_CLASSPATH
    profiler $TEST_NAME start
    start_client $TEST_NAME $CLIENT_SCRIPT
    profiler $TEST_NAME stop
    capture_stats $TEST_NAME $DISTRIBUTION_ROOT
    kill_server $TEST_NAME $SERVER_PID
    profiler $TEST_NAME quit
    finalize_run $TEST_NAME
    popd > /dev/null
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
ZOOMSCRIPT=$(which zoomscript)

initialize_configuration
source $CONFIG_FILE

# Derived globals
RUN_OUTPUT_ROOT="$OUTPUT_ROOT/$TIMESTAMP"
DEPLOYMENT_NOSNAP=$WORK_ROOT/deployment_nosnap.xml
DEPLOYMENT_SNAP=$WORK_ROOT/deployment_snap.xml

# Additional command line arguments for special behavior.
if [ "$2" = "clean" ]; then
    echo "Cleaning old output and runtime directories..."
    test -d $OUTPUT_ROOT  && \rm -rf $OUTPUT_ROOT
    test -d $RUNTIME_ROOT && \rm -rf $RUNTIME_ROOT
fi
if [ "$2" = "seed" -o "$2" = "clean" ]; then
    echo "Clearing old seed data..."
    test -d $SEED_ROOT && \rm -rf $SEED_ROOT
fi

kill_running_server

prepare_files

prepare_comparison_distribution

run_test old-nosnap $COMPARISON_DISTRIBUTION_ROOT $DEPLOYMENT_NOSNAP
run_test old-snap   $COMPARISON_DISTRIBUTION_ROOT $DEPLOYMENT_SNAP
run_test new-nosnap $DEVELOPMENT_ROOT             $DEPLOYMENT_NOSNAP
run_test new-snap   $DEVELOPMENT_ROOT             $DEPLOYMENT_SNAP

python analyze.py $RUN_OUTPUT_ROOT | tee -a $RUN_OUTPUT_ROOT/results.txt

echo "Runtime output: $RUN_OUTPUT_ROOT"
