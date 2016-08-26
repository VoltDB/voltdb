#!/bin/bash

# script assumes there is docker image called voltdb. If image name is different, update DOCKER_IMAGE

function start() {
    : ${HOSTCOUNT?"Need to set HOSTCOUNT"}
    #: ${STORAGE?"Need to specify storage location"}

    # form the host list
    NODE="node"
    for ((i=1; i<=$HOSTCOUNT; i++))
    do
        if [ -n "${NODE_LIST}" ]; then
            NODE_LIST="$NODE_LIST,$NODE$i"
        else
            NODE_LIST="$NODE$i"
        fi
    done
    
    echo "server list" $NODE_LIST
    
    # image to run
    DOCKER_IMAGE="voltdb"

    if [ -n $STORAGE ]; then
    # get the voltdb root directory location from the image - defined by DIRECTORY_SPEC
    DOCKER_DATA_STORE=$(echo $(docker inspect --format '{{range  .Config.Env}}{{println .}}{{end}}' $DOCKER_IMAGE) |  tr ' ' '\n' | grep DIRECTORY_SPEC | sed 's/^.*=//')
    #echo "docker directory specification for voltdb is: $DOCKER_DATA_STORE"
    fi

    for ((i=1; i<=$HOSTCOUNT; i++))
    do
        # if storage location 
        if [[ $STORAGE && ${STORAGE-x} ]]; then
            # echo "Data persistence to host specified: $STORAGE"
            # provision directory for storing the data
            HOST_LOCATION=$STORAGE$NODE$i
            if [ -a  $HOST_LOCATION ]; then
                #check if the provided storage location is directory
                if [ ! -d  $HOST_LOCATION ]; then
                    echo "Storage location on host, $HOST_LOCATION, is not a directory"
                    exit
                fi            
            fi
        fi 

        # instantiate docker containers
        if [ -z $STORAGE ]; then
            docker run -d -P -e HOST_COUNT="$HOSTCOUNT" -e HOSTS="$NODE_LIST" --name=$NODE$i --network=voltLocalCluster $DOCKER_IMAGE            
        else
            echo "Node: $NODE$i Mounted host directory, $HOST_LOCATION, into the container's voltdbroot at $DOCKER_DATA_STORE"             
            docker run -d -P -e HOST_COUNT="$HOSTCOUNT" -e HOSTS="$NODE_LIST"  -v $HOST_LOCATION:$DOCKER_DATA_STORE  --name=$NODE$i --network=voltLocalCluster $DOCKER_IMAGE
        fi
    done
}

start