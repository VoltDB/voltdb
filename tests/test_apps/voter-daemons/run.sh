#!/usr/bin/env bash

APPNAME="voter"

# find voltdb binaries in either installation or distribution directory.
if [ -n "$(which voltdb 2> /dev/null)" ]; then
    VOLTDB_BIN=$(dirname "$(which voltdb)")
else
    VOLTDB_BIN="$(dirname $(dirname $(pwd)))/bin"
    echo "The VoltDB scripts are not in your PATH."
    echo "For ease of use, add the VoltDB bin directory: "
    echo
    echo $VOLTDB_BIN
    echo
    echo "to your PATH."
    echo
fi
# installation layout has all libraries in $VOLTDB_ROOT/lib/voltdb
if [ -d "$VOLTDB_BIN/../lib/voltdb" ]; then
    VOLTDB_BASE=$(dirname "$VOLTDB_BIN")
    VOLTDB_LIB="$VOLTDB_BASE/lib/voltdb"
    VOLTDB_VOLTDB="$VOLTDB_LIB"
# distribution layout has libraries in separate lib and voltdb directories
else
    VOLTDB_BASE=$(dirname "$VOLTDB_BIN")
    VOLTDB_LIB="$VOLTDB_BASE/lib"
    VOLTDB_VOLTDB="$VOLTDB_BASE/voltdb"
fi

APPCLASSPATH=$CLASSPATH:$({ \
    \ls -1 "$VOLTDB_VOLTDB"/voltdb-*.jar; \
    \ls -1 "$VOLTDB_LIB"/*.jar; \
    \ls -1 "$VOLTDB_LIB"/extension/*.jar; \
} 2> /dev/null | paste -sd ':' - )
CLIENTCLASSPATH=$CLASSPATH:$({ \
    \ls -1 "$VOLTDB_VOLTDB"/voltdbclient-*.jar; \
    \ls -1 "$VOLTDB_LIB"/commons-cli-1.2.jar; \
} 2> /dev/null | paste -sd ':' - )
VOLTDB="$VOLTDB_BIN/voltdb"
LOG4J="$VOLTDB_VOLTDB/log4j.xml"
LICENSE="$VOLTDB_VOLTDB/license.xml"
HOST="localhost"

function _copy() {
    # Prefer to use rsync so that modifications are not overridden.
    if command -v rsync > /dev/null 2>&1; then
        rsync -ur ../../../examples/voter/ddl.sql .
        rsync -ur ../../../examples/voter/src .
    else
        cp -afv ../../../examples/voter/ddl.sql .
        cp -afv ../../../examples/voter/src .
    fi
}

# remove build artifacts
function clean() {
    rm -rf obj debugoutput $APPNAME.jar s[12]_[12] d[12]_[12].xml statement-plans catalog-report.html log
}

# remove build artifacts and copied source
function clean-all() {
    clean
    rm -rf src ddl.sql
}

# compile the source code for procedures and the client
function srccompile() {
    _copy
    mkdir -p obj
    javac -target 1.7 -source 1.7 -classpath $APPCLASSPATH -d obj \
        src/voter/*.java \
        src/voter/procedures/*.java
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
}

# build an application catalog
function catalog() {
    srccompile
    echo "Compiling the voter application catalog."
    echo "To perform this action manually, use the command line: "
    echo
    echo "voltdb compile --classpath obj -o $APPNAME.jar ddl.sql"
    echo
    $VOLTDB compile --classpath obj -o $APPNAME.jar ddl.sql
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
}

# Usate: _deployment INSTANCE HOST_COUNT
function _deployment() {
    echo "Generating deployment d$1_$2.xml..."
    echo "\
<?xml version=\"1.0\"?>
<deployment>
    <cluster hostcount=\"$2\" kfactor=\"0\" />
    <httpd enabled=\"true\">
        <jsonapi enabled=\"true\" />
    </httpd>
    <paths>
        <voltdbroot path=\"s$1_$2/voltdbroot\" />
        <snapshots path=\"snapshots\" />
        <commandlog path=\"commandlog\" />
        <commandlogsnapshot path=\"commandlog/snapshots\" />
    </paths>
</deployment>
" > d$1_$2.xml
}

# Port #:  instance 1: 20000+offset  instance 2: 20010+offset
function _port() {
    echo $(($1*10+19990+$2))
}

# Usage: _server INSTANCE HOST_COUNT
function _server() {
    test -f $APPNAME.jar || catalog
    test -d s$1_$2/voltdbroot/snapshots || mkdir -p s$1_$2/voltdbroot/snapshots
    local CMD
    if [ $2 -eq 1 ]; then
        CMD="$VOLTDB create -B"
    else
        CMD="$VOLTDB create -B -I $1"
    fi
    # Replication port needs to be the last assigned port #, including
    # the RMI port, because it consumes more than one port number.
    CMD="$CMD \
-d d$1_$2.xml \
-l $LICENSE \
-H localhost:$(_port 1 1) \
--internal=$(_port $1 2) \
--zookeeper=$(_port $1 3) \
--http=$(_port $1 4) \
--admin=$(_port $1 5) \
--client=$(_port $1 6) \
--replication=$(_port $1 8) \
$APPNAME.jar"
    local VOLTDB_OPTS=-Dvolt.rmi.agent.port=$(_port $1 7)
    echo "Starting VoltDB daemon $1 (host count: $2)..."
    echo "Command: VOLTDB_OPTS=$VOLTDB_OPTS $CMD"
    VOLTDB_OPTS=$VOLTDB_OPTS $CMD
}

# Single server
function create1() {
    _deployment 1 1
    _server 1 1
    echo ">>>>> Tailing log (Ctrl-C stops tail, not server)..."
    watch1
}

# Dual server
function create2() {
    _deployment 1 2
    _deployment 2 2
    _server 1 2
    sleep 5
    _server 2 2
    echo ">>>>> Tailing logs (Ctrl-C stops tail, not server)..."
    watch2
}

# Single server
function watch1() {
    echo "Command: tail -F ~/.voltdb_server/localhost_$(_port 1 1).out"
    tail -F ~/.voltdb_server/localhost_$(_port 1 1).out
}

# Dual server
function watch2() {
    echo "Command: tail -F ~/.voltdb_server/localhost_$(_port 1 1)_1.out ~/.voltdb_server/localhost_$(_port 1 1)_2.out"
    tail -F ~/.voltdb_server/localhost_$(_port 1 1)_1.out ~/.voltdb_server/localhost_$(_port 1 1)_2.out
}

function _client() {
    test -d obj || srccompile
    java -classpath obj:$CLIENTCLASSPATH:obj -Dlog4j.configuration=file://$LOG4J \
        voter.AsyncBenchmark \
        --displayinterval=5 \
        --warmup=5 \
        --duration=10 \
        --servers=localhost:$(_port $1 6) \
        --contestants=6 \
        --maxvotes=2
}

# Single server
function client1() {
    _client 1 1
}

# Dual server
function client2() {
    _client 1 2
    _client 2 2
}

function _stop() {
    if [ $2 -eq 1 ]; then
        echo "Command: $VOLTDB stop -H localhost:$(_port 1 1)"
        $VOLTDB stop -H localhost:$(_port 1 1)
    else
        echo "Command: $VOLTDB stop -H localhost:$(_port 1 1) -I $1"
        $VOLTDB stop -H localhost:$(_port 1 1) -I $1
    fi
}

# Single server
function stop1() {
    _stop 1 1
}

# Dual server
function stop2() {
    _stop 1 2
    _stop 2 2
}

function help() {
    echo "Usage: ./run.sh {clean|clean-all|catalog|create1|create2|client1|client2|watch1|watch2|stop1|stop2}"
}

# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ $# -eq 0 ]; then help; exit; fi
"$@"
