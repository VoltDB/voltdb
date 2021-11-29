#/bin/sh
# this file is used as an entry point inside a docker container

set -x
[ -z "$LOG4J" ] && LOG4J=voltdb/log4j.xml
[ -z "$VJAR" ] && VJAR=`ls -1t voltdb/voltdb/voltdbclient-*.jar | head -1`

SSLOPT=""
SPORT=""

echo "loading ddl"
voltdb/bin/sqlcmd --servers=$SERVERS $SPORT $SSLOPT < topicTable.sql

JAVA=`which java`
CLASSPATH="topicbenchmark2-client.jar"
APPCLASSPATH=$({ \
    \ls -1 voltdb/lib/*.jar; \
    \ls -1 voltdb/lib/extension/*.jar; \
} 2> /dev/null | paste -sd ':' - )
$JAVA -classpath ${CLASSPATH}:${VJAR}:${APPCLASSPATH} -Dlog4j.configuration=$LOG4J \
        topicbenchmark2.TopicBenchmark2 \
        --topic=$TOPIC \
        --subscribe=$SUBSCRIBE \
        --usegroupids=$USEGROUPIDS \
        --servers=$SERVERS \
        --topicPort=$TOPICPORT \
        --varcharsize=$VARCHARSIZE \
        --asciionly=$ASCIIONLY \
        --useavro=$USEAVRO \
        --usekafka=$USEKAFKA \
        --schemaregistry=$SCHEMAREGISTRY \
        --count=$COUNT \
        --producers=$PRODUCERS \
        --insertrate=$INSERTRATE \
        --insertwarnthreshold=$INSERTWARNTHRESHOLD \
        --groups=$GROUPS \
        --groupmembers=$GROUPMEMBERS \
        --staticmembers=$STATICMEMBERS \
        --transientmembers=$TRANSIENTMEMBERS \
        --transientmaxduration=$TRANSIENTMAXDURATION \
        --groupprefix=$GROUPPREFIX \
        --sessiontimeout=$SESSIONTIMEOUT \
        --maxpollsilence=$MAXPOLLSILENCE \
        --pollprogress=$POLLPROGRESS \
        --verification=$VERIFICATION \
        --maxfailedinserts=$MAXFAILEDINSERTS \
        --logsuppression=$LOGSUPPRESSION
