#!/bin/bash
usage="Usage: daemon.sh (start|stop) [rejoin-hostname]"
if [ $# = 0 ]; then
 echo $usage
 exit 1
fi

if [ "$VOLTDB_PID_DIR" = "" ]; then
  VOLTDB_PID_DIR=/tmp
fi

if [ "$VOLTDB_IDENT_STRING" = "" ]; then
  export VOLTDB_IDENT_STRING="$USER"
fi

# Set default scheduling priority
if [ "$VOLTDB_NICENESS" = "" ]; then
    export VOLTDB_NICENESS=5
fi

startStop=$1
pid=$VOLTDB_PID_DIR/voltdb-$VOLTDB_IDENT_STRING.pid

log=/home/sebc/svndev/eng/tests/test_apps/genqa/daemon.log
command="java -server -Xmn1536m -Xmx2048m -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp -XX:-ReduceInitialCardMarks -Dvolt.rmi.agent.port=9090 -Dvolt.rmi.server.hostname=$HOSTNAME -Djava.library.path=../../../obj/release/dist/voltdb -Dlog4j.configuration=/home/sebc/svndev/eng/obj/release/dist/voltdb/log4j.properties -cp .:../../../obj/release/dist/voltdb/* org.voltdb.VoltDB port 21212 internalport 3021 catalog genqa.jar deployment /home/sebc/svndev/eng/tests/test_apps/genqa/deployment.xml"
case $startStop in

  (start)

    mkdir -p "/tmp/genqa"

    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo Already running as process `cat $pid`.  Stop it first.
        exit 1
      fi
    fi
    
    if [ "$2" == "" ]; then
        nohup nice -n $VOLTDB_NICENESS $command &> "$log" < /dev/null &
    else
        nohup nice -n $VOLTDB_NICENESS $command rejoinhost $2 &> "$log" < /dev/null &
    fi
    echo $! > $pid
    sleep 1; head -n 100 "$log"
    ;;

  (stop)
    
    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo stopping
        kill `cat $pid`
      else
        echo no process to stop
      fi
    else
      echo no process to stop
    fi
    ;;

  (*)
    echo $usage
    exit 1
    ;;

esac
