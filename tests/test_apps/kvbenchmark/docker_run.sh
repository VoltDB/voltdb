#/bin/sh
# this file is used as an entry point inside a docker container
# execute the application
# example usage:
# create a network for the containers to talk over
# > docker network create --driver bridge voltdb_network
# start volt
# > docker run -it -v $(pwd):/build --network=voltdb_network -p 21212:21212 -p 21211:21211 -p 8081:8080 -p 3021:3021 -p 5555:5555 -p 7181:7181 -w /build --name="voltdb" voltdb/voltdb-enterprise:latest bash -c "cd /opt/voltdb/bin && ./voltdb init && ./voltdb start"
# build the docker image with necessary volt files.
# > build_docker.sh voltdb/kvbenchmark
# this file will get run by default with no command options.
# > docker run --rm --network=voltdb_network --name kvbenchmark -w /kvbenchmark -e SERVERS=voltdb voltdb/kvbenchmark
# to debug the contents of the kvbenchmark data
# > docker run --rm --network=voltdb_network --name kvbenchmark -w /kvbenchmark -e SERVERS=voltdb voltdb/kvbenchmark  voltdb/bin/sqlcmd --servers=voltdb --query='select key from store limit 1'


set -x
[ -z "$LOG4J" ] && LOG4J=voltdb/log4j.xml
[ -z "$DISPLAYINTERVAL" ] && DISPLAYINTERVAL=5
[ -z "$SERVERS" ] && SERVERS=localhost && echo "SERVERS:$SERVERS"
[ -z "$DURATION" ] && DURATION=120
[ -z "$POOLSIZE" ] && POOLSIZE=100000
[ -z "$PRELOAD" ] && PRELOAD=true
[ -z "$GETPUTRATIO" ] && GETPUTRATIO=0.90
[ -z "$KEYSIZE" ] && KEYSIZE=32
[ -z "$MINVALUESIZE" ] && MINVALUESIZE=1024
[ -z "$MAXVALUESIZE" ] && MAXVALUESIZE=1024
[ -z "$USECOMPRESSION" ] && USECOMPRESSION=false
[ -z "$THREADS" ] && THREADS=40
[ -z "$CSVFILE" ] && CSVFILE=periodic.csv.gz
[ -z "$TOPOLOGYAWARE" ] && TOPOLOGYAWARE=true
[ -z "$LOG4J" ] && LOG4J=voltdb/log4j.xml
[ -z "$VJAR" ] && VJAR=`ls -1t voltdb/voltdb/voltdbclient-*.jar | head -1`
[ -z "$USERNAME" ] && USERNAME=""
[ -z "$PASSWORD" ] && PASSWORD=""
[ -z "$SSLFILE" ] && SSLFILE=""
[ -z "$STATSFILE" ] && STATSFILE=""
[ -z "$DR" ] && DR=""
[ -z "$SQLCMDPORT" ] && SQLCMDPORT=""
# if set only load the DDL
[ -z "$DDLONLY" ] && DDLONLY=""

SSLOPT=""
if [ "$SSLFILE" != "" ]; then
    SSLOPT="--ssl=$SSLFILE"
fi

SPORT=""
if [ "$SQLCMDPORT" != "" ]; then
    SPORT="--port=$SQLCMDPORT"
fi

voltdb/bin/sqlcmd --servers=$SERVERS $SPORT $SSLOPT --query="select key from store limit 1"

if [ $? != 0 ]; then
    echo "loading ddl"
    voltdb/bin/sqlcmd --servers=$SERVERS $SPORT $SSLOPT < ddl.sql
    if [ "$DR" != "" ]; then
        voltdb/bin/sqlcmd --servers=$SERVERS $SPORT $SSLOPT --query="DR TABLE STORE; DR TABLE EXTRAS"
    fi


else
    voltdb/bin/sqlcmd --servers=$SERVERS $SSLOPT --query="truncate table store"
fi
if [ "$DDLONLY" != "" ]; then
    exit 0
fi

JAVA=`which java`
CLASSPATH="kvbenchmark.jar"
APPCLASSPATH=$({ \
    \ls -1 voltdb/lib/*.jar; \
    \ls -1 voltdb/lib/extension/*.jar; \
} 2> /dev/null | paste -sd ':' - )
$JAVA -classpath kvbenchmark.jar:${VJAR}:${APPCLASSPATH} -Dlog4j.configuration=$LOG4J \
        kvbench.SyncBenchmark \
        --displayinterval=$DISPLAYINTERVAL \
        --duration=$DURATION \
        --servers=$SERVERS \
        --poolsize=$POOLSIZE \
        --preload=$PRELOAD \
        --getputratio=$GETPUTRATIO \
        --keysize=$KEYSIZE \
        --minvaluesize=$MINVALUESIZE \
        --maxvaluesize=$MAXVALUESIZE \
        --usecompression=$USECOMPRESSION \
        --threads=$THREADS \
        --csvfile=periodic.csv.gz \
        --sslfile=$SSLFILE \
        --username=$USERNAME \
        --password=$PASSWORD \
        --statsfile=$STATSFILE \
        --topologyaware=$TOPOLOGYAWARE
