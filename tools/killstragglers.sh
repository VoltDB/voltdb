#!/bin/bash

# Kill (VoltDB server) processes that are listening on any of the usual ports

HOUR=`date +%H`
DAY=`date +%u`
IGNORE="JENKINS_HOME|zookeeper.server.quorum.QuorumPeerMain|kafka.Kafka"

# Use sudo if USER is 'test'
if [ $USER = "test" ]; then
    SUDO=sudo
fi

# Make an attempt to kill any VoltDB server processes; however,
# this might not work when the process command is extremely long
for P in `$SUDO pgrep -f org.voltdb.VoltDB | xargs`
do
    logger -sp user.notice -t TESTKILL "User $USER $BUILD_TAG Killing `$SUDO ps --no-headers -p $P -o pid,user,command`"
    $SUDO kill -9 $P
done

# If the fuser -k or lsof command exists on this system, use it to kill any
# processes that are using any of the standard VoltDB ports (first, list the
# processes about to be killed)
kill_port_processes_using_fuser() {
    $SUDO fuser    ${PORT}/tcp
    $SUDO fuser -k ${PORT}/tcp
}
kill_port_processes_using_lsof() {
    $SUDO lsof -i tcp:${PORT}
    $SUDO lsof -i tcp:${PORT} | grep -v PID | awk '{print $2}' | xargs kill
}

KILL_PORT_PROCESSES_COMMAND=
# Mac OS (OSTYPE darwin*) supports fuser, but not fuser -k
if [[ "$OSTYPE" != darwin* ]] && type fuser > /dev/null 2>&1; then
    KILL_PORT_PROCESSES_COMMAND=kill_port_processes_using_fuser
elif type lsof > /dev/null 2>&1; then
    KILL_PORT_PROCESSES_COMMAND=kill_port_processes_using_lsof
fi

for PORT in 3021 7181 21211 21212 8080 5555
do
    $KILL_PORT_PROCESSES_COMMAND
done

exit 0
