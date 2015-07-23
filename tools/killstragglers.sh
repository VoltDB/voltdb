#!/bin/bash

# Kill any/all java processes that are listening on any port(s)
# sudo kill if USER is 'test' and we're in the evening test hours and/or on specific volt hosts...
# If you need to run overnight, schedule with qa.

HOUR=`date +%H`
DAY=`date +%u`
IGNORE="JENKINS_HOME|zookeeper.server.quorum.QuorumPeerMain|kafka.Kafka"

if [ $USER = "test" ]; then
    SUDO=sudo
fi
for P in `$SUDO pgrep -f org.voltdb.VoltDB | xargs`
do
    logger -sp user.notice -t TESTKILL "User $USER $BUILD_TAG Killing `$SUDO ps --no-headers -p $P -o pid,user,command`"
    $SUDO kill -9 $P
done
exit 0
