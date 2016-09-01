#!/bin/bash

# script assumes there is docker image called voltdb. If image name is different, update DOCKER_IMAGE
function start() {
    : ${HOSTCOUNT?"Need to set HOSTCOUNT"}
    #: ${STORAGE?"Need to specify storage location"}

    # form the host list
    NODE="node"
    for (( i=1; i<=$HOSTCOUNT; i++ ))
    do
        if [ -n "${NODE_LIST}" ]; then
            NODE_LIST="$NODE_LIST,$NODE$i"
        else
            NODE_LIST="$NODE$i"
        fi
    done

    echo "server list: " $NODE_LIST

    # image to run
    DOCKER_IMAGE="voltdb"

    # fetch paths used for voltdbroot, custom config and license file in container based on docker image
    DOCKER_DATA_STORE=$(echo $(docker inspect --format '{{range  .Config.Env}}{{println .}}{{end}}' $DOCKER_IMAGE) |  tr ' ' '\n' | grep DIRECTORY_SPEC | sed 's/^.*=//')
    DOCKER_CUSTOM_CONFIG=$(echo $(docker inspect --format '{{range  .Config.Env}}{{println .}}{{end}}' $DOCKER_IMAGE) |  tr ' ' '\n' | grep CUSTOM_CONFIG | sed 's/^.*=//')
    DOCKER_LICENSE=$(echo $(docker inspect --format '{{range  .Config.Env}}{{println .}}{{end}}' $DOCKER_IMAGE) |  tr ' ' '\n' | grep LICENSE_FILE | sed 's/^.*=//')

    # set the docker subnet to voltLocalCluster
    if [ -z ${NETWORK} ]; then
        NETWORK=voltLocalCluster
        #TODO: check if netowkr called voltLocalCluster exists, if doesn't create bridged network called voltLocalCluster
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

    for (( i=1; i<=$HOSTCOUNT; i++ ))
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
        OPTIONS="-d -P -e HOST_COUNT="$HOSTCOUNT" -e HOSTS="$NODE_LIST" ${MOUNT_POINTS} --name=${NODE}${i} --network=${NETWORK} ${DOCKER_IMAGE}"
        echo "---- docker run ${OPTIONS} ----"
        #docker run -d -P -e HOST_COUNT="$HOSTCOUNT" -e HOSTS="$NODE_LIST"  ${MOUNT_POINTS} --name=$NODE$i --network=voltLocalCluster $DOCKER_IMAGE
        docker run ${OPTIONS}
    done
}

start