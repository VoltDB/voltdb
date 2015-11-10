################################################################################
#
# Source this script from tests to initialize environment variables.
#
# It provides daemon start/stop functions with automatic killing at exit.
#
# Environment variables, e.g. DEPLOYMENT, can be tweaked by the caller.
#
# APPNAME must be set before invoking voltdb_daemon_start.
#
################################################################################

# find voltdb binaries in either installation or distribution directory.

if [ -n "$(which voltdb 2> /dev/null)" ]; then
    VOLTDB_BIN=$(dirname "$(which voltdb)")
else
    VOLTDB_BIN="$(pwd)/../../../bin"
fi
# installation layout has all libraries in $VOLTDB_ROOT/lib/voltdb
if [ -d "$VOLTDB_BIN/../lib/voltdb" ]; then
    VOLTDB_BASE=$(dirname "$VOLTDB_BIN")
    VOLTDB_LIB="$VOLTDB_BASE/lib/voltdb"
    VOLTDB_VOLTDB="$VOLTDB_LIB"
# distribution layout has libraries in separate lib and voltdb directories
elif [ -d "$VOLTDB_BIN/../voltdb" ]; then
    VOLTDB_BASE=$(dirname "$VOLTDB_BIN")
    VOLTDB_LIB="$VOLTDB_BASE/lib"
    VOLTDB_VOLTDB="$VOLTDB_BASE/voltdb"
else 
    VOLTDB_LIB="`pwd`/../../../lib"
    VOLTDB_VOLTDB="`pwd`/../../../voltdb"
fi

CLASSPATH=$(ls -x "$VOLTDB_VOLTDB"/voltdb-*.jar | tr '[:space:]' ':')$(ls -x "$VOLTDB_LIB"/*.jar | egrep -v 'voltdb[a-z0-9.-]+\.jar' | tr '[:space:]' ':')
VOLTDB="$VOLTDB_BIN/voltdb"
LOG4J="$VOLTDB_VOLTDB/log4j.xml"
CLIENTLOG4J="$VOLTDB_VOLTDB/../tests/log4j-allconsole.xml"
LICENSE="$VOLTDB_VOLTDB/license.xml"
HOST="localhost"
DEPLOYMENT=""
SERVER_CREATE_RETRIES=3
SERVER_CREATE_SLEEP=5
SERVER_INIT_GREP="Server completed initialization."
SERVER_INIT_RETRIES=5
SERVER_INIT_SLEEP=5

function _ds_die() {
    local HOST=$1
    local OUTPUT_FILE=~/.voltdb_server/$HOST.out
    local ERROR_FILE=~/.voltdb_server/$HOST.err
    echo "FAILED: $@"
    if [ -e $OUTPUT_FILE ]; then
        echo "
>>> Output Log <<<
"
        cat $OUTPUT_FILE
    fi
    if [ -e $ERROR_FILE ]; then
        echo "
>>> Error Log <<<
"
        cat $ERROR_FILE
    fi
    exit 1
}

function voltdb_daemon_start() {
    # voltdb CLI options
    local DEPLOYMENT_OPTION=""
    test -n "$DEPLOYMENT" && DEPLOYMENT_OPTION="-d $DEPLOYMENT"

    local PID_FILE=~/.voltdb_server/$HOST.pid
    local OUTPUT_FILE=~/.voltdb_server/$HOST.out
    local ERROR_FILE=~/.voltdb_server/$HOST.err

    test -f $PID_FILE && rm -v $PID_FILE
    test -f $OUTPUT_FILE && rm -v $OUTPUT_FILE
    test -f $ERROR_FILE && rm -v $ERROR_FILE

    echo "Starting server..."
    $VOLTDB create -B $DEPLOYMENT_OPTION -l $LICENSE -H $HOST $APPNAME.jar || exit 1

    # Make sure the server gets stopped before exiting
    trap "$VOLTDB stop -H $HOST" EXIT

    # Wait for the server process to start.
    local I=0
    while [ ! -e $PID_FILE ]; do
        test $I -eq $SERVER_CREATE_RETRIES && _ds_die $HOST "Gave up on server process creation."
        echo "Waiting for server process to get created..."
        sleep $SERVER_CREATE_SLEEP
        let I=I+1
    done
    local SERVER_PID=$(cat $PID_FILE)

    # Wait for the server to initialize.
    let I=0
    while ! grep -q "$SERVER_INIT_GREP" $OUTPUT_FILE; do
        test $I -eq $SERVER_INIT_RETRIES && _ds_die $HOST "Gave up on server initialization."
        if ! kill -0 $SERVER_PID; then
            _ds_die $HOST "Server process (PID=$SERVER_PID) died."
        fi
        echo "Waiting for server (PID=$SERVER_PID) to initialize..."
        sleep $SERVER_INIT_SLEEP
        let I=I+1
    done
}

function voltdb_daemon_stop() {
    $VOLTDB stop -H $HOST
}
