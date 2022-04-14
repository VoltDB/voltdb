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
[ -z "$SERVERS" ] && SERVERS=localhost && echo "SERVERS:$SERVERS"
[ -z "$WARMUP" ] && WARMUP=5
[ -z "$DURATION" ] && DURATION=120
[ -z "$CHECKPOINT" ] && CHECKPOINT=50
[ -z "$DELAY" ] && DELAY=1
[ -z "$CLIENTVERSION" ] && CLIENTVERSION=2
[ -z "$PRIORITIZE" ] && PRIORITIZE=true
[ -z "$SINGLECLIENT" ] && SINGLECLIENT=true
[ -z "$USESPS" ] && USESPS=true
[ -z "$USEMPS" ] && USEMPS=true
[ -z "$ASYNC" ] && ASYNC=true
[ -z "$VERIFY" ] && VERIFY=true
[ -z "$VARATION" ] && VARIATION=5
[ -z "$PRINTSTATS" ] && PRINTSTATS=true
[ -z "$SPRATES" ] && SPRATES="800,800,800,800,800,800,800,800"
[ -z "$MPRATES" ] && MPRATES="20,20,20,20,20,20,20,20"

#[ -z "$STATSFILE" ] && STATSFILE=""
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

drtables=" \
MP_TABLE \
TABLE01 \
TABLE02 \
TABLE03 \
TABLE04 \
TABLE05 \
TABLE06 \
TABLE07 \
TABLE08"

# check if we have loaded tables
voltdb/bin/sqlcmd --servers=$SERVERS $SPORT $SSLOPT --query="select rowid from TABLE01 limit 1"

if [ $? != 0 ]; then
    echo "loading ddl"
    voltdb/bin/sqlcmd --servers=$SERVERS $SPORT $SSLOPT < table.sql
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

CLIENTCLASSPATH=$CLIENTLIBS:$CLIENTCLASSPATH

CLASSPATH=$(ls -x voltdb/voltdb/voltdb-*.jar | tr '[:space:]' ':')$(ls -x voltdb/lib/*.jar | egrep -v 'voltdb[a-z0-9.-]+\.jar' | tr '[:space:]' ':')

ARGS=""

CMD="$JAVA -ea -classpath priorityclient-client.jar:${CLASSPATH}"
if [ "" != "$LOG4J" ]; then
    CMD="$CMD -Dlog4j.configuration=file:./$LOG4J"
fi

CMD="$CMD client.PriorityClient $ARGS\
      --servers=$SERVERS \
      --warmup=$WARMUP \
      --checkpoint=$CHECKPOINT \
      --duration=$DURATION \
      --delay=$DELAY \
      --clientversion=$CLIENTVERSION \
      --prioritize=$PRIORITIZE \
      --singleclient=$SINGLECLIENT \
      --usesps=$USESPS \
      --usemps=$USEMPS \
      --async=$ASYNC \
      --verify=$VERIFY \
      --variation=$VARIATION \
      --printstats=$PRINTSTATS \
      --sprates=$SPRATES \
      --mprates=$MPRATES \
      --sslfile=$SSLFILE \
      --username=$USERNAME \
      --password=$PASSWORD"
$CMD
