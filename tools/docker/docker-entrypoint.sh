#!/bin/bash

############################################################################################
# This file is part of VoltDB.
# Copyright (C) 2008-2016 VoltDB Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
############################################################################################

set -e

function init() {
    if [ -f  ${CUSTOM_CONFIG} ]; then
        DEPLOYMENT=${CUSTOM_CONFIG}
    else
        DEPLOYMENT=${DEFAULT_DEPLOYMENT}
    fi

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