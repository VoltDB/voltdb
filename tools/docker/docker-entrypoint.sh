#!/bin/bash
set -e

function init() {
    if [ -f  ${CUSTOM_CONFIG} ]; then
        DEPLOYMENT=${CUSTOM_CONFIG}
        #echo "using custom deployment ${DEPLOYMENT} (${CUSTOM_CONFIG})"
    else
        DEPLOYMENT=${DEFAULT_DEPLOYMENT}
        #echo "using default deployment ${DEPLOYMENT} (${DEFAULT_DEPLOYMENT})"

    fi
    #cat ${DEPLOYMENT}
    OPTIONS="-C ${DEPLOYMENT} -D ${DIRECTORY_SPEC}"
    echo "Run voltdb init $OPTIONS"
    bin/voltdb init ${OPTIONS}
}

function execVoltdbStart() {
    if [ -z "${HOST_COUNT}" ] && [ -z "${HOSTS}" ]; then
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
    fi

    if [ -f ${LICENSE_FILE} ]; then
        OPTIONS="$OPTIONS -l ${LICENSE_FILE}"
    fi

    OPTIONS="$OPTIONS --ignore=thp"

    echo "Run voltdb start $OPTIONS"
    exec bin/voltdb start $OPTIONS
}

if [ ! -f ${DIRECTORY_SPEC}/voltdbroot/.initialized ]; then
    init
fi

execVoltdbStart

#exec "$@ $OPTIONS"