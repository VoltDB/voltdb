#!/bin/bash
set -e 

function init() {
    bin/voltdb init -C ${DEPLOYMENT} -D ${DIRECTORY_SPEC}
}

function execVoltdbStart() {
    if [ -z "$HOST_COUNT" ] && [ -z "$HOSTS" ]; then
        echo "To start a Volt cluster, atleast need to provide HOST_COUNT OR list of HOSTS"
        exit
    fi
    
    if [ -n "${HOST_COUNT}" ]; then
        OPTIONS=" -c $HOST_COUNT"
    fi
    
    if [ -n "${HOSTS}" ]; then
        OPTIONS="$OPTIONS -H $HOSTS"
    fi

    if [ -n "${DIRECTORY_SPEC}" ]; then
        OPTIONS="$OPTIONS -D ${DIRECTORY_SPEC}"
        echo $(ls )
    fi 

    #if [ -n "${VOLUMES}" ]; then
    #OPTIONS="$OPTIONS -v $VOLUMES"
    #fi

    echo "options to run with $OPTIONS"

    #echo "print environment variables"
    #printenv

    #for word in "$@"; do echo "$word"; done
    #for word in "$*"; do echo "$word"; done

    OPTIONS="$OPTIONS --ignore=thp"

    exec bin/voltdb start $OPTIONS
}

if [ ! -f ${DIRECTORY_SPEC}/voltdbroot/.initialized ]; then
    init
fi

execVoltdbStart

#exec "$@ $OPTIONS"
