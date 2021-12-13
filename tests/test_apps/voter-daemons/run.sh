#!/usr/bin/env bash

###
# Uses the version of 'voter' from test_apps/voter
###

# find voltdb binaries
if [ -e ../../../bin/voltdb ]; then
    # assume this is the tests/test_apps/voter-daemons directory
    VOLTDB_BIN="$(dirname $(dirname $(dirname $(pwd))))/bin"
elif [ -n "$(which voltdb 2> /dev/null)" ]; then
    # assume we're using voltdb from the path
    VOLTDB_BIN=$(dirname "$(which voltdb)")
else
    echo "Unable to find VoltDB installation."
    echo "Please add VoltDB's bin directory to your path."
    exit -1
fi

# call script to set up paths, including
# java classpaths and binary paths
source $VOLTDB_BIN/voltenv

LOG4J="$VOLTDB_VOLTDB/log4j.xml"
VOTER_BASE=../voter

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
        rsync -ur $VOTER_BASE/ddl.sql .
        rsync -ur $VOTER_BASE/client .
        rsync -ur $VOTER_BASE/procedures .
        rsync -u  $VOTER_BASE/run.sh runvoter.tmp
    else
        cp -afv $VOTER_BASE/ddl.sql .
        cp -afv $VOTER_BASE/client .
        cp -afv $VOTER_BASE/procedures .
        cp -afv $VOTER_BASE/run.sh runvoter.tmp
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
    rm -rf voter-client.jar voter-procs.jar s[12] d[12].xml voltdb_crash*.txt
    rm -f ~/.voltdb_server/*
}

# remove build artifacts and copied source
function clean-all() {
    clean
    rm -rf ddl.sql client procedures runvoter.tmp
}

# compile the source code for procedures and the client
function srccompile() {
    populate
    ./runvoter.tmp jars
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
        <commandlog path=\"command_log\" />
        <commandlogsnapshot path=\"command_log_snapshots\" />
    </paths>
</deployment>
" > d$1.xml
}

# Usage: server INSTANCE
function server() {
    _checkarg server "$@"
    deployment $1
    if [ ! -e voter-procs.jar ] || [ ! -e voter-client.jar ]; then
        srccompile;
    fi
    test -d s$1 || mkdir s$1

    local CMD1="voltdb init --force --dir=s$1 --config=d$1.xml"
    _info "Initializing VoltDB daemon $1 ..."
    _run $CMD1

    # Replication port needs to be the last assigned port #, including
    # the RMI port, because it consumes more than one port number.
    local CMD2="voltdb start --background --instance=$1 \
    --dir=s$1 \
    --host=localhost:$(_port 1 2),localhost:$(_port 2 2) \
    --count=2 \
    --internal=$(_port $1 2) \
    --zookeeper=$(_port $1 3) \
    --http=$(_port $1 4) \
    --admin=$(_port $1 5) \
    --client=$(_port $1 6) \
    --replication=$(_port $1 8)"
    local VOLTDB_OPTS=-Dvolt.rmi.agent.port=$(_port $1 7)
    _info "Starting VoltDB daemon $1 ..."
    VOLTDB_OPTS=$VOLTDB_OPTS _run $CMD2
}

function watch() {
    _run tail -F ~/.voltdb_server/server_[12].out
}

# Usage: client INSTANCE
function client() {
    _checkarg client "$@"
    if [ ! -e voter-procs.jar ] || [ ! -e voter-client.jar ]; then
        srccompile;
    fi
    sqlcmd --port=$(_port $1 6) <ddl.sql
    _run java -classpath voter-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file://$LOG4J \
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
    # argument is internal port, --host is admin port
    _run voltadmin stop --host=localhost:$(_port $1 5) localhost:$(_port $1 2)
}

function help() {
    echo "
  Usage: ./run.sh OPTION

  Options:
      clean | clean-all | srccompile |
      server N | client N | stop N |
      watch
"
}

if [ "$1" = "-n" -o "$1" = "--dry-run" ]; then
    export DRYRUN=true
    shift
else
    export DRYRUN=false
fi

# Run the target passed as the first arg on the command line
# If no first arg, run help
if [ $# -eq 0 ]; then help; exit; fi
"$@"
