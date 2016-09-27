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


# Helper script for creating VOLTDB cluster using voltdb's docker community image
#
# Environment variables
# HOSTCOUNT     Required        Specifies number of the node to initialize the cluster with
# CONFIGFILE    Optional        Specifies the deployment file to be used instead of default
#                               deployment packaged with voltdb community the image. Path to
#                               deployment should be absolute
# LICENSE       Optional        Specifies the license file. Path to the file should be absolute
# STORAGE       Optional        Specifies the host location to use as the location for voltdbroot
#                               for the cluster nodes. Path to the file should be absolute
############################################################################################

# script assumes there is docker image called voltdb. If image name is different, update DOCKER_IMAGE

usage() {

echo "Helper script for creating VOLTDB cluster using voltdb's docker community image

Usage:
    HOSTCOUNT=<number of nodes> CONFIGFILE=<config file absolute path> LICENSE=<license file absolute path> STORAGE=<config file absolute path> ./docker_run.sh

Details:
    Environment variables
    HOSTCOUNT     Required        Specifies number of the node to initialize the cluster with
    CONFIGFILE    Optional        Specifies the deployment file to be used instead of default
                                  deployment packaged with voltdb community the image. Path to
                                  deployment should be absolute
    LICENSE       Optional        Specifies the license file. Path to the file should be absolute
    STORAGE       Optional        Specifies the host location to use as the location for voltdbroot
                                  for the cluster nodes. Path to the file should be absolute

Usage examples:
    HOSTCOUNT=3 CONFIGFILE=/home/foo/deployment_t.xml LICENSE=/home/foo/license.xml STORAGE=/home/foo/voltdbCluster/ ./docker_run.sh
    HOSTCOUNT=3 ./docker_run.sh"
exit 0;
}

function start() {

    case ${HOSTCOUNT} in
        (*[!0-9]*|'') usage;
    esac

    # form the host list
    NODE="node"
    for (( i=1; i<=${HOSTCOUNT}; i++ ))
    do
        if [ -n "${NODE_LIST}" ]; then
            NODE_LIST="${NODE_LIST},${NODE}${i}"
        else
            NODE_LIST="${NODE}${i}"
        fi
    done

    echo "Host list: " ${NODE_LIST}

    # image to run, if not defined use the latest community image
    DOCKER_IMAGE=${DOCKER_IMAGE:-"voltdb/voltdb-community"}

    # fetch paths used for voltdbroot, custom config and license file in container based on docker image
    DOCKER_DATA_STORE=$(echo $(docker inspect --format '{{range  .Config.Env}}{{println .}}{{end}}' $DOCKER_IMAGE) |  tr ' ' '\n' | grep DIRECTORY_SPEC | sed 's/^.*=//')
    DOCKER_CUSTOM_CONFIG=$(echo $(docker inspect --format '{{range  .Config.Env}}{{println .}}{{end}}' $DOCKER_IMAGE) |  tr ' ' '\n' | grep CUSTOM_CONFIG | sed 's/^.*=//')
    DOCKER_LICENSE=$(echo $(docker inspect --format '{{range  .Config.Env}}{{println .}}{{end}}' $DOCKER_IMAGE) |  tr ' ' '\n' | grep LICENSE_FILE | sed 's/^.*=//')

    # set the docker subnet to voltLocalCluster
    if [ -z ${NETWORK} ]; then
        NETWORK=voltLocalCluster
        #TODO: check if network called voltLocalCluster exists, if doesn't create bridged network called voltLocalCluster
    fi

    echo "DOCKER file paths - voltdbroot: ${DOCKER_DATA_STORE};  custom config: ${DOCKER_CUSTOM_CONFIG}; licensefile: ${DOCKER_LICENSE}"

    CUSTOM_CONFIG_MOUNT=""
    # check for deployment file
    if [[ ! -z ${CONFIGFILE// } ]]; then
        if [ ! -f ${CONFIGFILE} ]; then
            echo "Supplied config file: ${CONFIGFILE} is not a regular file"
        fi
        CUSTOM_CONFIG_MOUNT="-v ${CONFIGFILE}:${DOCKER_CUSTOM_CONFIG}"
        echo "Use config file: ${CONFIGFILE}"
    fi

    LICENSE_MOUNT=""
    # check for license file
    if [ ! -z ${LICENSE// } ]; then
        if [ ! -f ${LICENSE} ]; then
            echo "Supplied license file: ${LICENSE} is not a regular file"
        fi
        LICENSE_MOUNT="-v ${LICENSE}:${DOCKER_LICENSE}"
        echo "Use license file: ${LICENSE}"
    fi

    for (( i=1; i<=${HOSTCOUNT}; i++ ))
    do
        echo ""
        MOUNT_POINTS="${CUSTOM_CONFIG_MOUNT} ${LICENSE_MOUNT}"

        # check if data needs to be persisted
        if [ ! -z ${STORAGE// } ]; then
            # provision directory for storing the data
            HOST_LOCATION=${STORAGE}${NODE}${i}
            if [ -a  ${HOST_LOCATION} ]; then
                # check if the provided storage location is directory
                if [ ! -d  ${HOST_LOCATION} ]; then
                    echo "Storage location on host, ${HOST_LOCATION}, is not a directory"
                    exit
                fi
            fi
            echo "Node ${NODE}${i}: mounted host directory ${HOST_LOCATION} to the container's voltdbroot at ${DOCKER_DATA_STORE}"
            MOUNT_POINTS="${MOUNT_POINTS} -v ${HOST_LOCATION}:${DOCKER_DATA_STORE}"
        fi

        OPTIONS="-d -P -e HOST_COUNT="${HOSTCOUNT}" -e HOSTS="${NODE_LIST}" ${MOUNT_POINTS} --name=${NODE}${i} --network=${NETWORK} ${DOCKER_IMAGE}"
        echo "---- docker run ${OPTIONS} ----"
        docker run ${OPTIONS}
    done
}

start