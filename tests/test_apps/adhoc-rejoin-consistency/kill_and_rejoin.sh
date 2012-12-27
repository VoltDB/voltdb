#!/bin/bash

VOLTDBROOT=/localhome/prosegay/rejoincrash
LEADERHOST=volt3e
REJOINHOST=volt3f

# kills and rejoins a node
# rc 0 if normal exit, otherwise rc 1

mkdir -p $VOLTDBROOT

    # get server pid
    while true
    do
        PID=`pgrep -f ".* host $LEADERHOST"`
        [ $PID ] && break
    done

    # check that other node is still alive
    #./check_tcp -H $LEADERHOST -p 8080 2>/dev/null
    ping -c 1 $LEADERHOST 2>/dev/null
    if [ $? -gt 0 ]; then
        echo "other node not responding"
        exit 1
    fi

    #Z=$(( ($RANDOM%50)+3 ))
    Z=$1
    echo "short delay before kill $Z..."
    sleep $Z

    echo `date +%Y-%m-%d\ %H:%M:%S` killing $PID
    kill $PID

    # wait for it to come down
    echo "wait for pid to die..."
    while ps -p $PID >/dev/null 2>&1
    do
        sleep 1
    done

    unset PID
    # capture and reset the log
    cat $VOLTDBROOT/log/volt.log >> $VOLTDBROOT/log/rejoin.log
    echo "" > $VOLTDBROOT/log/volt.log

    ./run.sh rejoin &
    echo `date +%Y-%m-%d\ %H:%M:%S` "rejoin started"

    t=0
    echo "wait for rejoin completed or timeout..."
    while ! grep -q 'CONSOLE: Node rejoin completed' $VOLTDBROOT/log/volt.log 
    do
        #[ $t -gt 70 ] && exit 1
        sleep 1
        #let t+=1
    done
    echo "all clear"
exit 0
