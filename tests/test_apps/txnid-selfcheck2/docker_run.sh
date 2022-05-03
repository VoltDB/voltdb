#/bin/sh
# this file is used as an entry point inside a docker container
# execute the application
# example usage:
# create a network for the containers to talk over
# > docker network create --driver bridge voltdb_network
# start volt
# > docker run -it -v $(pwd):/build --network=voltdb_network -p 21212:21212 -p 21211:21211 -p 8081:8080 -p 3021:3021 -p 5555:5555 -p 7181:7181 -w /build --name="voltdb" voltdb/voltdb-enterprise:latest bash -c "cd /opt/voltdb/bin && ./voltdb init && ./voltdb start"
# build the docker image with necessary volt files.
# > build_docker.sh voltdb/txnid2
# this file will get run by default with no command options.
# > docker run --rm --network=voltdb_network --name txnid2 -w /txnid2 -e SERVERS=voltdb voltdb/txnid2
# to debug the contents of the kvbenchmark data
# > docker run --rm --network=voltdb_network --name txnid2 -w /txnid2 -e SERVERS=voltdb voltdb/txnid2  voltdb/bin/sqlcmd --servers=voltdb --query='select txnid from partitioned limit 1'


set -x
[ -z "$DISPLAYINTERVAL" ] && DISPLAYINTERVAL=5
[ -z "$SERVERS" ] && SERVERS=localhost && echo "SERVERS:$SERVERS"
[ -z "$DURATION" ] && DURATION=120
[ -z "$THREADS" ] && THREADS=40
[ -z "$THREADOFFSET" ] && THREADOFFSET=0
[ -z "$USECLIENTV2" ] && USECLIENTV2="false"
[ -z "$USEPRIORITIES" ] && USEPRIORITIES="false"
[ -z "$MINVALUESIZE" ] && MINVALUESIZE=1024
[ -z "$MAXVALUESIZE" ] && MAXVALUESIZE=1024
[ -z "$ENTROPY" ] && ENTROPY=127
[ -z "$FILLERROWSIZE" ] && FILLERROWSIZE=10240
[ -z "$REPLFILLERROWMB" ] && REPLFILLERROWMB=32
[ -z "$PARTFILLERROWMB" ] && PARTFILLERROWMB=128
[ -z "$PROGRESSTIMEOUT" ] && PROGRESSTIMEOUT=20
[ -z "$USECOMPRESSION" ] && USECOMPRESSION=false
[ -z "$ALLOWINPROCADHOC" ] && ALLOWINPROCADHOC=false
[ -z "$DISABLEDTHREADS" ] && DISABLEDTHREADS="Cappedlt"
[ -z "$RATELIMIT" ] && RATELIMIT=100000000
[ -z "$MPRATIO" ] && MPRATIO=0.20
[ -z "$SWAPRATIO" ] && SWAPRATIO=0.50
[ -z "$UPSERTRATIO" ] && UPSERTRATIO=0.50
[ -z "$UPSERTHITRATIO" ] && UPSERTHITRATIO=0.20
[ -z "$ENABLEHASHMISMATCHGEN" ] && ENABLEHASHMISMATCHGEN=false
[ -z "$CSVFILE" ] && CSVFILE=periodic.csv.gz
[ -z "$TOPOLOGYAWARE" ] && TOPOLOGYAWARE=true
[ -z "$DROPTASKS" ] && DROPTASKS=true
[ -z "$LOG4J" ] && LOG4J=""
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

drtables="partitioned \
dimension \
replicated \
adhocr \
adhocp \
bigr \
bigp \
nibdr \
nibdp \
forDroppedProcedure \
ex_partview_shadow \
T_PAYMENT50 \
loadp \
cploadp \
loadmp \
cploadmp \
trur \
trup \
swapr \
swapp \
tempr \
tempp \
capr \
capp \
importp \
importr \
importbp \
importbr \
ttlmigratep \
ttlmigrater  \
taskp \
taskr"

# check if we have loaded tables
voltdb/bin/sqlcmd --servers=$SERVERS $SPORT $SSLOPT --query="select txnid from partitioned limit 1"

if [ $? != 0 ]; then
    echo "loading ddl"
    voltdb/bin/sqlcmd --servers=$SERVERS $SPORT $SSLOPT < src/txnIdSelfCheck/ddl.sql
    if [ "$DR" != "" ]; then
        for t in $tables; do
        voltdb/bin/sqlcmd --servers=$SERVERS $SPORT $SSLOPT --query="DR TABLE $t"
        done
    fi


else
    for t in $tables; do
        voltdb/bin/sqlcmd --servers=$SERVERS $SSLOPT --query="TRUNCATE TABLE $t"
    done
fi
if [ "$DDLONLY" != "" ]; then
    exit 0
fi

JAVA=`which java`
CLASSPATH=$(ls -x voltdb/voltdb/voltdb-*.jar | tr '[:space:]' ':')$(ls -x voltdb/lib/*.jar | egrep -v 'voltdb[a-z0-9.-]+\.jar' | tr '[:space:]' ':')
#CLASSPATH="txnid.jar"
#APPCLASSPATH=$({ \
#    \ls -1 voltdb/lib/*.jar; \
#    \ls -1 voltdb/lib/extension/*.jar; \
#} 2> /dev/null | paste -sd ':' - )

ARGS=""

# CLIENTV2 and PRIORITIES ARE NOT SUPPORTED IN ALL RELEASES
#if [ "$USECLIENTV2" == "true" ]; then
#    ARGS="$ARGS --useclientv2=$USECLIENTV2"
#fi
#if [ "$USEPRIORITIES" == "true" ]; then
#    ARGS="$ARGS --usepriorities=$USEPRIORITIES"
#fi

CMD="$JAVA -ea -classpath txnid.jar:${CLASSPATH}"
if [ "" != "$LOG4J" ]; then
    CMD="$CMD -Dlog4j.configuration=file:./$LOG4J"
fi

CMD="$CMD txnIdSelfCheck.Benchmark $ARGS \
        --displayinterval=$DISPLAYINTERVAL \
        --duration=$DURATION \
        --servers=$SERVERS \
        --threads=$THREADS \
        --threadoffset=$THREADOFFSET \
        --minvaluesize=$MINVALUESIZE \
        --maxvaluesize=$MAXVALUESIZE \
        --entropy=$ENTROPY \
        --fillerrowsize=$FILLERROWSIZE \
        --replfillerrowmb=$REPLFILLERROWMB \
        --partfillerrowmb=$PARTFILLERROWMB \
        --progresstimeout=$PROGRESSTIMEOUT \
        --usecompression=$USECOMPRESSION \
        --allowinprocadhoc=$ALLOWINPROCADHOC \
        --ratelimit=$RATELIMIT \
        --statsfile=$STATSFILE \
        --mpratio=$MPRATIO \
        --swapratio=$SWAPRATIO \
        --upsertratio=$UPSERTRATIO \
        --upserthitratio=$UPSERTHITRATIO \
        --disabledthreads=$DISABLEDTHREADS \
        --sslfile=$SSLFILE \
        --username=$USERNAME \
        --password=$PASSWORD \
        --droptasks=$DROPTASKS \
        --enablehashmismatchgen=$ENABLEHASHMISMATCHGEN"

$CMD
