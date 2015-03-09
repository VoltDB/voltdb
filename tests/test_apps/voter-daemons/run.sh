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

function _run() {
    if [ "$DRYRUN" = "true" ]; then
        echo "$@"
    else
        echo "Command: $@"
        "$@"
    fi
}

function populate() {
    # Prefer to use rsync so that modifications are not overridden.
    if command -v rsync > /dev/null 2>&1; then
        rsync -ur ../../../examples/voter/ddl.sql .
        rsync -ur ../../../examples/voter/src .
    else
        cp -afv ../../../examples/voter/ddl.sql .
        cp -afv ../../../examples/voter/src .
    fi
}

function _info() {
    if [ "$DRYRUN" != "true" ]; then
        echo ">>> $@"
    fi
}

# Port #:  instance 1: 20000+offset  instance 2: 20010+offset
function _port() {
    echo $(($1*10+19990+$2))
}

function _checkarg() {
    if [ "$2" != "1" -a "$2" != "2" ]; then
        echo "Usage: $@ N  (N=1|2)"
        exit 1
    fi
}

# remove build artifacts
function clean() {
    rm -rf obj debugoutput $APPNAME.jar s[12] d[12].xml statement-plans catalog-report.html log voltdb_crash*.txt
    rm -f ~/.voltdb_server/*
}

# remove build artifacts and copied source
function clean-all() {
    clean
    rm -rf src ddl.sql
}

# compile the source code for procedures and the client
function srccompile() {
    populate
    mkdir -p obj
    javac -target 1.7 -source 1.7 -classpath $APPCLASSPATH -d obj \
        src/voter/*.java \
        src/voter/procedures/*.java || exit 1
}

# build an application catalog
function catalog() {
    srccompile
    if ! ($VOLTDB compile --classpath obj -o $APPNAME.jar ddl.sql > /dev/null); then
        echo "Catalog compilation failed"
        exit 1
    fi
}

# Usage: deployment INSTANCE
function deployment() {
    _info "Generating deployment d$1.xml..."
    echo "\
<?xml version=\"1.0\"?>
<deployment>
    <cluster hostcount=\"1\" kfactor=\"0\" />
    <httpd enabled=\"true\">
        <jsonapi enabled=\"true\" />
    </httpd>
    <paths>
        <voltdbroot path=\"s$1/voltdbroot\" />
        <snapshots path=\"snapshots\" />
        <commandlog path=\"commandlog\" />
        <commandlogsnapshot path=\"commandlog/snapshots\" />
    </paths>
</deployment>
" > d$1.xml
}

# Usage: server INSTANCE
function server() {
    _checkarg server "$@"
    deployment $1
    test -f $APPNAME.jar || catalog
    test -d s$1/voltdbroot || mkdir -p s$1/voltdbroot
    # Replication port needs to be the last assigned port #, including
    # the RMI port, because it consumes more than one port number.
    local CMD="$VOLTDB create -B \
-d d$1.xml \
-l $LICENSE \
-H localhost:$(_port $1 1) \
--internal=$(_port $1 2) \
--zookeeper=$(_port $1 3) \
--http=$(_port $1 4) \
--admin=$(_port $1 5) \
--client=$(_port $1 6) \
--replication=$(_port $1 8) \
$APPNAME.jar"
    local VOLTDB_OPTS=-Dvolt.rmi.agent.port=$(_port $1 7)
    _info "Starting VoltDB daemon $1 (VOLTDB_OPTS=$VOLTDB_OPTS)..."
    VOLTDB_OPTS=$VOLTDB_OPTS _run $CMD
}

function watch() {
    _run tail -F ~/.voltdb_server/localhost_200[01]1*.out
}

# Usage: client INSTANCE
function client() {
    _checkarg client "$@"
    test -d obj || srccompile
    _run java -classpath obj:$CLIENTCLASSPATH:obj -Dlog4j.configuration=file://$LOG4J \
        voter.AsyncBenchmark \
        --displayinterval=5 \
        --warmup=5 \
        --duration=10 \
        --servers=localhost:$(_port $1 6) \
        --contestants=6 \
        --maxvotes=2
}

# Usage: stop INSTANCE
function stop() {
    _checkarg stop "$@"
    _run $VOLTDB stop -H localhost:$(_port $1 1)
}

function help() {
    echo "Usage: ./run.sh {clean|clean-all|catalog|server N|client N|stop N|watch}"
}

if [ "$1" = "-n" -o "$1" = "--dry-run" ]; then
    export DRYRUN=true
    shift
else
    export DRYRUN=false
fi

# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ $# -eq 0 ]; then help; exit; fi
"$@"
