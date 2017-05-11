#!/bin/bash

# Kill (VoltDB server) processes that are listening on any of the usual ports
STANDARD_VOLTDB_PORTS="3021 7181 21211 21212 5555"

HOUR=`date +%H`
DAY=`date +%u`
IGNORE="JENKINS_HOME|zookeeper.server.quorum.QuorumPeerMain|kafka.Kafka"

# If USER is 'test', use sudo and include 8080 in the ports to be killed
# (but if not running as 'test', I don't want to kill my browser)
if [ $USER = "test" ]; then
    SUDO=sudo
    STANDARD_VOLTDB_PORTS="$STANDARD_VOLTDB_PORTS 8080"
fi

# Make an attempt to kill any VoltDB server processes; however,
# this might not work when the process command is extremely long
for PROC in `$SUDO pgrep -f org.voltdb.VoltDB | xargs`
do
    logger -sp user.notice -t TESTKILL "User $USER $BUILD_TAG Killing `$SUDO ps -p $PROC -o pid= -o user= -o command=`"
    $SUDO kill -9 $PROC
done

# If the fuser -k or lsof command exists on this system, use it to kill any
# processes that are using any of the standard VoltDB ports; but first, list
# and log (in the system log) the process about to be killed
get_port_process_using_fuser() {
    $SUDO fuser ${PORT}/tcp
    PROC=`$SUDO fuser ${PORT}/tcp`
}
kill_port_process_using_fuser() {
    $SUDO fuser -k ${PORT}/tcp
}
get_port_process_using_lsof() {
    $SUDO lsof -i tcp:${PORT}
    PROC=`$SUDO lsof -i tcp:$PORT | grep -v PID | awk '{print $2}'`
}
kill_port_process_using_lsof() {
    $SUDO kill -9 $PROC
}

GET_PORT_PROCESS_COMMAND=
KILL_PORT_PROCESS_COMMAND=
# Mac OS (OSTYPE darwin*) supports fuser, but not fuser -k {port}/tcp
if [[ "$OSTYPE" != darwin* ]] && type fuser > /dev/null 2>&1; then
    GET_PORT_PROCESS_COMMAND=get_port_process_using_fuser
    KILL_PORT_PROCESS_COMMAND=kill_port_process_using_fuser
elif type lsof > /dev/null 2>&1; then
    GET_PORT_PROCESS_COMMAND=get_port_process_using_lsof
    KILL_PORT_PROCESS_COMMAND=kill_port_process_using_lsof
else
    echo "Unable to kill processes using VoltDB ports: neither fuser (-k) nor lsof is installed on this system."
fi

PROC=
for PORT in $STANDARD_VOLTDB_PORTS
do
    $GET_PORT_PROCESS_COMMAND
    if [ -n "$PROC" ]; then
        logger -sp user.notice -t TESTKILL "User $USER Port $PORT $BUILD_TAG Killing ($KILL_PORT_PROCESS_COMMAND): `$SUDO ps -p $PROC -o pid= -o user= -o command=`"
        $KILL_PORT_PROCESS_COMMAND
    fi
done

exit 0
