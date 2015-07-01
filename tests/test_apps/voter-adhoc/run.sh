#!/usr/bin/env bash
VOLTDB_BIN="$(pwd)/../../../bin"
VOLTDB_BASE=$(dirname "$VOLTDB_BIN")
VOLTDB_LIB="$VOLTDB_BASE/lib"
VOLTDB_VOLTDB="$VOLTDB_BASE/voltdb"
LOG4J="$VOLTDB_VOLTDB/log4j.xml"

CLASSPATH=$({ \
    \ls -1 "$VOLTDB_VOLTDB"/voltdb-*.jar; \
    \ls -1 "$VOLTDB_LIB"/*.jar; \
    \ls -1 "$VOLTDB_LIB"/extension/*.jar; \
} 2> /dev/null | paste -sd ':' - )

VOTER_BASE=$VOLTDB_BASE/examples/voter
FILES="\
src
ddl.sql
deployment.xml"

function clean() {
    rm -Rf $FILES
}

function copyvoter() {
    #Get all the files from real voter
    for F in $FILES
    do
        cp -pR $VOTER_BASE/$F .
    done
}

function copyrunsh(){
    #Get the run.sh and copy to another name
    cp -p $VOTER_BASE/run.sh runtest.sh
    #Change the paths to work from tests/test_apps
    sed  's#\.\./#../../#' runtest.sh > runexample.sh
}

#Copy the run.sh from voter and modify to be called
copyrunsh

# If too many args show help, show help and exit
if [ $# -gt 1 ]; then bash runexample.sh help; exit; fi


# If clean then clean the run.sh stuff, then call clean
if [ "$1" = "clean" ]
then
    clean
    bash runexample.sh clean
    rm runexample.sh runtest.sh
    exit
fi

#Otherwise - copy the rest of voter
copyvoter
#Copy the AdHocBenchmark.java from her to src
cp -p AdHocBenchmark.java src/voter/

#If adhoc, run it otherwise call into the original run.sh
if [ "$1" = "adhoc" ]
then
    (cat adhoc-quiet.input | sqlcmd > /dev/null) || exit 1
    bash runexample.sh srccompile
    java -classpath obj:$CLASSPATH:obj -Dlog4j.configuration=file://$LOG4J \
        voter.AdHocBenchmark \
        --displayinterval=5 \
        --warmup=5 \
        --duration=12 \
        --servers=localhost:21212 \
        --contestants=6 \
        --maxvotes=2
elif [ "$1" = "adhoc-logged" ]
then
    (cat adhoc-noisy.input | sqlcmd > /dev/null) || exit 1
    bash runexample.sh srccompile
    java -classpath obj:$CLASSPATH:obj -Dlog4j.configuration=file://$LOG4J \
        voter.AdHocBenchmark \
        --displayinterval=5 \
        --warmup=5 \
        --duration=12 \
        --servers=localhost:21212 \
        --contestants=6 \
        --maxvotes=2
else
    bash runexample.sh $1
fi


