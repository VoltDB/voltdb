#!/bin/bash 

./run.sh srccompile || exit 99

#SERVERS_CHECK=volt10a,volt10b
#SERVERS=(volt10a volt10b)
SERVERS_CHECK=volt3e,volt3f
SERVERS=(volt3e volt3f)

NSERVERS=${#SERVERS[@]}
echo $NSERVERS

declare -a A

set -o pipefail
NPASS=${1:-0}
PASS=1
while true;
do

    KILLIT=$(( ($RANDOM % 30)+1 ))
    RATE=$(( ($RANDOM % 2)+1 ))
RATE=1
    DURATION=300

    pkill -9 -f 'AdHocRejoinConsistency.AsyncBenchmark' # ensure no stragglers

    echo "Starting Async client Duration: $DURATION Rate: $RATE Killing: $KILLIT"
    DURATION=$DURATION RATE=$RATE ./run.sh client &
    CPID=$!

    if true; then
        ./kill_and_rejoin.sh $KILLIT &
        KPID=$!
        # wait for both these jobs to finish then check the state
        wait $KPID
    else
        sleep 15
    fi

    echo "killing client... $CPID"
    kill $CPID
    pkill -f 'AdHocRejoinConsistency.AsyncBenchmark'
    while pgrep -f 'AdHocRejoinConsistency.AsyncBenchmark' &>/dev/null 
    do
        sleep 1
    done

    # check the data integrity
    # non-zero return code indicates error
    if ! ./run.sh verify; then
        echo "Error data miscompare"
        exit 1
    fi
    #exit

done

