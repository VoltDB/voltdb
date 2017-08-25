#!/bin/bash

# Kill (VoltDB server) processes that are listening on any of the usual ports
STANDARD_VOLTDB_PORTS="3021 7181 21211 21212 5555"

while getopts "hp:" opt; do
  case $opt in
    h)
      echo 'Usage: attempts to kill any VoltDB processes, and any processes that are'
      echo 'using any of the usual VoltDB ports (3021, 7181, 21211, 21212, 5555);'
      echo 'also 8080, but only when running as USER "test", since that is liable'
      echo 'to kill your browser. You may specify additional port(s) to be killed'
      echo 'using the -p option, e.g., to kill a commonly used PostgreSQL port:'
      echo '    killstragglers -p 5432'
      echo 'or, when using ant (see build.xml):'
      echo '    ant killstragglers -Dport=5432'
      echo 'It is also possible to specify 2 or more additional ports, e.g.:'
      echo '    killstragglers -p "5432 8080"'
      echo 'or, when using ant:'
      echo '    ant killstragglers -Dport="5432 8080"'
      exit;;
    p)
      STANDARD_VOLTDB_PORTS="$STANDARD_VOLTDB_PORTS $OPTARG"
      ;;
  esac
done

HOUR=`date +%H`
DAY=`date +%u`
IGNORE="JENKINS_HOME|zookeeper.server.quorum.QuorumPeerMain|kafka.Kafka"

# If USER is 'test', use sudo and include 8080 in the ports to be killed
# (but if not running as 'test', I don't want to kill my browser)
if [ $USER = "test" ]; then
    SUDO=sudo
    STANDARD_VOLTDB_PORTS="$STANDARD_VOLTDB_PORTS 8080"
fi

# Uncomment these to get debug print:
#echo "Debug: STANDARD_VOLTDB_PORTS: $STANDARD_VOLTDB_PORTS"
#echo "Debug: SUDO: $SUDO"
#echo "Debug: USER: $USER"
#echo "Debug: BUILD_TAG: $BUILD_TAG"

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
    PROC=`$SUDO lsof -i tcp:$PORT | grep -v PID | tail -1 | awk '{print $2}'`
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

# Uncomment these to get debug print:
#echo "Debug: GET_PORT_PROCESS_COMMAND : $GET_PORT_PROCESS_COMMAND"
#echo "Debug: KILL_PORT_PROCESS_COMMAND: $KILL_PORT_PROCESS_COMMAND"

PROC=
for PORT in $STANDARD_VOLTDB_PORTS
do
    $GET_PORT_PROCESS_COMMAND
    # Uncomment these to get debug print:
    #echo "Debug: PORT: $PORT"
    #echo "Debug: PROC: $PROC"
    if [ -n "$PROC" ]; then
        logger -sp user.notice -t TESTKILL "User $USER Port $PORT $BUILD_TAG Killing ($KILL_PORT_PROCESS_COMMAND): `$SUDO ps -p $PROC -o pid= -o user= -o command=`"
        $KILL_PORT_PROCESS_COMMAND
    fi
done

exit 0
