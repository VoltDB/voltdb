#!/bin/bash

# This file is part of VoltDB.
# Copyright (C) 2008-2019 VoltDB Inc.

# Author: Phil Rosegay

# Functions to deploy VoltDB in Kubernetes, see the associated README


make_statefulset() {
    # customize the k8s voltdb-statefulset.yaml
    MANIFEST=${CLUSTER_NAME}.yaml
    cp voltdb-statefulset.yaml                        $MANIFEST
    SED="sed -i"
    [[ "$OSTYPE" =~ "darwin" ]] && SED="sed -i.bak"
    $SED "s:--clusterName--:$CLUSTER_NAME:g"          $MANIFEST
    $SED "s+--containerImage---+$REP/$IMAGE_TAG+g"    $MANIFEST
    $SED "s:--replicaCount--:$NODECOUNT:g"            $MANIFEST
    $SED "s:--pvolumeSize--:${PVOLUME_SIZE:-1Gi}:g"   $MANIFEST
    $SED "s:--memorySize--:${MEMORY_SIZE:-4Gi}:g"     $MANIFEST
    $SED "s:--cpuCount--:${CPU_COUNT:-1}:g"           $MANIFEST
    rm -f *.bak
}


find_files() {
    # takes a comma-separated list of file paths, which may include Unix-style wildcards (*?[ch-ranges])
    # returns a comma separated list of all matching files, following links
    files=`find -H ${@//,/\  } -maxdepth 1 -type f -print`
    echo $files
}

build_configmap_cmd() {
    I=0
    for f in $@
    do
        echo -n "--from-file=${f} "
        let I+=1
    done
    if [[ $I -gt 1 ]]; then
        FNS=""
        for f in $@
        do
            FNS+="`basename ${f}`,"
        done
        ORDERFILE=`mktemp`
        echo "$FNS" | sed -e 's/,$//' > $ORDERFILE
        echo -n "--from-file=.loadorder=$ORDERFILE"
    fi
    echo -n ""
}


delete_configmap_ifexists() {
    set +e  # don't check errors, may not exist
    kubectl delete configmap $1
    set -e
}


make_configmap() {

    MAPNAME=${CLUSTER_NAME}-init-classes
    delete_configmap_ifexists $MAPNAME
    CONFIG_MAP_ARGS=""
    TYPE=classes
    if [ ! -z "${CLASSES_FILES}" ]; then
        files=`find_files "${CLASSES_FILES}"`
        CONFIG_MAP_ARGS=`build_configmap_cmd "$files"`
    fi
    # create classes configmap, even if it is empty
    kubectl create configmap $MAPNAME $CONFIG_MAP_ARGS

    MAPNAME=${CLUSTER_NAME}-init-schema
    delete_configmap_ifexists $MAPNAME
    CONFIG_MAP_ARGS=""
    TYPE=schema
    if [ ! -z "${SCHEMA_FILES}" ]; then
        files=`find_files "${SCHEMA_FILES}"`
        CONFIG_MAP_ARGS=`build_configmap_cmd "$files"`
    fi
    # create schema configmap, even if it is empty
    kubectl create configmap $MAPNAME $CONFIG_MAP_ARGS

    MAPNAME=${CLUSTER_NAME}-init-configmap
    delete_configmap_ifexists $MAPNAME
    CONFIG_MAP_ARGS=""
    [ ! -z "${DEPLOYMENT_FILE}" ] && CONFIG_MAP_ARGS+=" --from-file=deployment=${DEPLOYMENT_FILE}"
    [ ! -z "${LICENSE_FILE}" ] && CONFIG_MAP_ARGS+=" --from-file=license=${LICENSE_FILE}"
    [ ! -z "${LOG4J_CONFIG_PATH}" ] && CONFIG_MAP_ARGS+=" --from-file=log4jcfg=${LOG4J_CONFIG_PATH}"

    kubectl create configmap $MAPNAME $CONFIG_MAP_ARGS

    kubectl delete configmap ${CLUSTER_NAME}-run-env

    # build the runtime environment settings file
    PROP_FILE="$(mktemp)"
    CONFIG_MAP_ARGS="--from-env-file=${PROP_FILE}"
    [ ! -z "${VOLTDB_INIT_ARGS}" ] && echo "VOLTDB_INIT_ARGS=${VOLTDB_INIT_ARGS}"     >> ${PROP_FILE}
    grep VOLTDB_START_ARGS      ${CFG_FILE} >> ${PROP_FILE}
    egrep NODECOUNT              ${CFG_FILE} >> ${PROP_FILE}
    [ ! -z "${LOG4J_CONFIG_PATH}"  ] && echo "LOG4J_CONFIG_PATH=\"/etc/voltdb/log4jcfg\"" >> ${PROP_FILE}
    [ ! -z "${VOLTDB_OPTS}" ] && echo "VOLTDB_OPTS=${VOLTDB_OPTS}"                    >> ${PROP_FILE}
    grep VOLTDB_HEAPMAX         ${CFG_FILE} >> ${PROP_FILE}
    #cat ${PROP_FILE}

    kubectl create configmap ${CLUSTER_NAME}-run-env --from-env-file=${PROP_FILE}

    rm -f ${PROP_FILE}
}


build_image() {

    # Requires a voltdb kit be installed (and we should be running from it)

    # For voltdb bundles and extensions, manually copy your assets to the corresponding directories
    # prior to creating the image

    if [ ! -d ../../bin ]; then
        echo "ERROR VoltDB tree structure error, VoltDB binaries not found"
        exit -1
    fi

    # Build Tag Deploy the image
    # nb. the docker build environment will encompass the VoltDB kit
    #     Some content that is not normally required for production --doc and examples-- are removed, see Dockerfile

    pushd ../.. > /dev/null

    docker image build -t ${IMAGE_TAG:-$CLUSTER_NAME} \
                       --build-arg VOLTDB_DIST_NAME=$(basename `pwd`) \
                       -f tools/kubernetes/docker/Dockerfile \
                       "$PWD"

    # tag and push as appropriate
    [ -n "${IMAGE_TAG}" ] && docker tag ${IMAGE_TAG} ${REP}/${IMAGE_TAG}
    [ -n "${REP}" ] && docker push ${REP}/${IMAGE_TAG}

    popd > /dev/null

    }


start_voltdb() {
    kubectl scale statefulset ${CLUSTER_NAME} --replicas=$NODECOUNT || kubectl create -f ${CLUSTER_NAME}.yaml
    }


stop_voltdb() {
    # Quiesce the cluster, put it into admin mode, then scale it down
    kubectl exec ${CLUSTER_NAME}-0 -- voltadmin pause --wait
    kubectl scale statefulset ${CLUSTER_NAME} --replicas=0
    }


force_voltdb() {

    # !!! Your voltdb statefulset will be forced to terminate ungracefully

    for P in `kubectl get pods | egrep "^$CLUSTER_NAME" | cut -d\  -f1`
    do
        kubectl delete pods $P --grace-period=0 --force
    done
    kubectl delete statefulset $CLUSTER_NAME --grace-period=0 --force
    kubectl delete service $CLUSTER_NAME
}


purge_persistent_claims() {

    # !!! You will loose your related persistent volumes (all your data), with no possibility of recovery

    for P in `kubectl get pvc | egrep "$CLUSTER_NAME" | cut -d\  -f1`
    do
        kubectl delete pvc $P
    done
}

usage() {
    echo "Usage: $0 parameter-file options ..."
    echo "      -B --build-voltdb-image         Builds a docker image of voltdb and pushes it to your repo"
    echo "      -M --install-configmap          Run kubectl create configmap for your config"
    echo "      -C --configure-voltdb           Customize the voltdb-manifest with provided parameters"
    echo "      -S --start-voltdb               Run kubectl create <voltdb-manifest> or kubectl .. --replicas="
    echo "      -P --stop-voltdb                Run voltadmin pause --wait; kubectl scale <voltdb-manifest> replicas=0"
    echo "      -P --restart-voltdb             Shorthand for -P -S"
    echo "      -D --purge-persistent-volumes   Run kubectl delete on clusters persistent volumes"
    echo "      -F --force-voltdb-down          Run kubectl delete cluster statefulset and running pods"
    exit 1
}


# MAIN

# source the template settings
if [ ! -e "$1" ]; then
    echo "ERROR parameter file not specified, customize the template with your settings and database assets"
    usage
fi

# parse out the parameter file filename parts
#_EXT=$([[ "$1 = *.* ]] && echo ".${1##*.}" || echo '')
CLUSTER_NAME="${1%.*}"

# source our config file parameters
CFG_FILE=$1
source ${CFG_FILE}

shift 1

# use Cluster name as default image name
: ${IMAGE_TAG:=${CLUSTER_NAME}}

if [ $# -eq 0 ]; then
    echo "ERROR option(s) missing"
    usage
fi

for cmd in "$@"
do
    case $cmd in

        -B|--build-voltdb-image)
                                    build_image
        ;;
        -M|--install-configmap)
                                    make_configmap
        ;;
        -C|--configure-voltdb)
                                    make_statefulset
        ;;
        -S|--start-voltdb)
                                    start_voltdb
        ;;
        -P|--stop-voltdb)
                                    stop_voltdb
        ;;
        -D|--purge-persistent-claims)
                                    purge_persistent_claims
        ;;
        -F|--force-voltdb-set)
                                    force_voltdb
        ;;
        -R|--restart_voltdb)
                                    stop_voltdb
                                    start_voltdb
        ;;
        -h|--help)
                                    usage
        ;;
        *)
                                    echo "ERROR unrecognized option"
                                    usage
                                    exit 1
        ;;
    esac
done
