#!/bin/bash -E

# This file is part of VoltDB.
# Copyright (C) 2008-2019 VoltDB Inc.

# Author: Phil Rosegay

# Functions to deploy VoltDB in Kubernetes, see the associated README
# !!!! bash only !!!!


err_report() {
    echo "Exit on line $(caller)" >&2
    exit 111
}

trap err_report ERR

make_statefulset() {
    # customize the k8s voltdb-statefulset.yaml
    MANIFEST=${CLUSTER_NAME}.yaml
    cp voltdb-statefulset.yaml                        $MANIFEST
    SED="sed -i"
    [[ $OSTYPE =~ darwin ]] && SED="sed -i.bak"
    $SED "s:--clusterName--:$CLUSTER_NAME:g"          $MANIFEST
    $SED "s+--containerImage---+$REP/$IMAGE_TAG+g"    $MANIFEST
    $SED "s:--replicaCount--:$NODECOUNT:g"            $MANIFEST
    $SED "s:--pvolumeSize--:${PVOLUME_SIZE:-1Gi}:g"   $MANIFEST
    $SED "s:--memorySize--:${MEMORY_SIZE:-4Gi}:g"     $MANIFEST
    $SED "s:--cpuCount--:${CPU_COUNT:-1}:g"           $MANIFEST
    echo "Deployment manifest '$MANIFEST' created, ready to deploy"
    rm -f *.bak
}


build_configmap_from_files() {
    local MAPNAME=$1
    shift 1
    # Takes a comma-separated list of file paths, which may include Unix-style wildcards (*?[ch-ranges])
    # returns a comma separated list of all matching files, following links
    # nb. find may fail, inform user
    if [[ -n $@ ]]; then
        local FILES=$(find ${@//,/ } -type f -print)
        local RC=$?
        if [[ $RC -ne 0 ]]; then
            echo "ERROR: create configmap '$MAPNAME' failed due to error(s)" >&2
            exit 111
        fi
    fi
    local ORDERFILE=$(mktemp)
    local CONFIG_MAP_ARGS=""
    if [[ -n $FILES ]]; then
        [[ -n $FILES ]] && echo "Files found for configmap/$MAPNAME:"
        local FNS=""
        for f in $FILES
        do
            echo -e "\t$f"
            CONFIG_MAP_ARGS+="--from-file=${f} "
            FNS+="$(basename ${f}),"
        done
        echo "$FNS" | sed -e 's/,$//' > $ORDERFILE
        CONFIG_MAP_ARGS+="--from-file=.loadorder=$ORDERFILE"
    fi
    echo "Creating configmap '$MAPNAME'.."
    # if there are no files, we create an empty configmap
    kubectl_ifexists delete configmap "$MAPNAME"
    kubectl create configmap "$MAPNAME" $CONFIG_MAP_ARGS
    rm -f $ORDERFILE
}


kubectl_ifexists() {
    RC=0
    if kubectl get "$2"/"$3" &>/dev/null; then
        kubectl $@
        RC=$?
    fi
    return $RC
}


make_configmaps() {

    # make classes configmap
    # packages voltdb procedures/udf classes jar files
    MAPNAME=${CLUSTER_NAME}-init-classes
    build_configmap_from_files $MAPNAME "${CLASSES_FILES}"

    # make schema configmap
    # packages voltdb ddl files
    MAPNAME=${CLUSTER_NAME}-init-schema
    kubectl_ifexists delete configmap "$MAPNAME"
    build_configmap_from_files $MAPNAME "${SCHEMA_FILES}"

    # make init configmap
    # packages: deployment.xml, license.xml, log4j properties
    MAPNAME=${CLUSTER_NAME}-init-configmap
    kubectl_ifexists delete configmap "$MAPNAME"
    CONFIG_MAP_ARGS=""
    [[ -n ${DEPLOYMENT_FILE} ]] && CONFIG_MAP_ARGS+=" --from-file=deployment=${DEPLOYMENT_FILE}"
    [[ -n ${LICENSE_FILE} ]] && CONFIG_MAP_ARGS+=" --from-file=license=${LICENSE_FILE}"
    [[ -n ${LOG4J_CONFIG_PATH} ]] && CONFIG_MAP_ARGS+=" --from-file=log4jcfg=${LOG4J_CONFIG_PATH}"

    kubectl create configmap $MAPNAME $CONFIG_MAP_ARGS

    # make runtime environment settings configmap
    # packages voltdb runtime settings NODECOUNT, VOLTDB_OPTS, etc.
    MAPNAME=${CLUSTER_NAME}-run-env
    kubectl_ifexists delete configmap "$MAPNAME"

    PROP_FILE="$(mktemp)"
    CONFIG_MAP_ARGS="--from-env-file=${PROP_FILE}"
    [[ -n ${VOLTDB_INIT_ARGS} ]] && echo "VOLTDB_INIT_ARGS=${VOLTDB_INIT_ARGS}"     >> ${PROP_FILE}
    grep VOLTDB_START_ARGS      ${CFG_FILE} >> ${PROP_FILE}
    egrep NODECOUNT             ${CFG_FILE} >> ${PROP_FILE}
    [[ -n ${LOG4J_CONFIG_PATH}  ]] && echo "LOG4J_CONFIG_PATH=\"/etc/voltdb/log4jcfg\"" >> ${PROP_FILE}
    [[ -n ${VOLTDB_OPTS} ]] && echo "VOLTDB_OPTS=${VOLTDB_OPTS}"                    >> ${PROP_FILE}
    grep VOLTDB_HEAPMAX         ${CFG_FILE} >> ${PROP_FILE}

    kubectl create configmap $MAPNAME --from-env-file=${PROP_FILE}

    rm -f ${PROP_FILE}
}


build_image() {

    # Requires a voltdb kit be installed (and we should be running from it)

    # For voltdb bundles and extensions, manually copy your assets to the corresponding directories
    # prior to creating the image

    if [[ ! -d ../../bin ]]; then
        echo "ERROR VoltDB tree structure error, VoltDB binaries not found"
        exit -1
    fi

    # Build Tag Deploy the image
    # nb. the docker build environment will encompass the VoltDB kit
    #     Some content that is not normally required for production --doc and examples-- are removed, see Dockerfile

    pushd ../.. > /dev/null

    docker image build -t ${IMAGE_TAG:-$CLUSTER_NAME} \
                       --build-arg VOLTDB_DIST_NAME=$(basename $(pwd)) \
                       -f tools/kubernetes/docker/Dockerfile \
                       "$PWD"

    # tag and push as appropriate
    [[ -n ${IMAGE_TAG} ]] && docker tag ${IMAGE_TAG} ${REP}/${IMAGE_TAG}
    [[ -n ${REP} ]] && docker push ${REP}/${IMAGE_TAG}

    popd > /dev/null

    }


start_voltdb() {
    if kubectl_ifexists scale statefulset ${CLUSTER_NAME} --replicas=$NODECOUNT; then
        kubectl create -f ${CLUSTER_NAME}.yaml
    fi
    }


stop_voltdb() {
    # Quiesce the cluster, put it into admin mode, then scale it down to zero replicas (nodes)
    local PODS=$(kubectl get pods --no-headers=true 2>/dev/null | grep "$CLUSTER_NAME" )
    RUNNING_POD=$(echo "$PODS" | grep Running | head -1 | cut -d\  -f1)
    if [[ -z $RUNNING_POD ]]; then
        echo "ERROR: no running pods for cluster '$CLUSTER_NAME' were found"
        echo "$PODS"
        exit 1
    fi
    if ! kubectl exec ${RUNNING_POD} -- voltadmin pause --wait; then
        echo "ERROR: unable to pause/drain cluster '$CLUSTER_NAME', is the cluster operational?"
        exit 1
    fi
    kubectl scale statefulset ${CLUSTER_NAME} --replicas=0
    }


force_voltdb() {

    # !!! Your voltdb statefulset and its pod(s) will be forced to terminate ungracefully

    for P in $(kubectl get pods --no-headers=true 2>/dev/null| egrep "^$CLUSTER_NAME" | cut -d\  -f1)
    do
        kubectl delete pods "$P" --grace-period=0 --force
    done

    kubectl_ifexists delete statefulset $CLUSTER_NAME --grace-period=0 --force
    kubectl_ifexists delete service $CLUSTER_NAME
}


purge_persistent_claims() {

    # !!! You will loose your related persistent volumes (all your data), with no possibility of recovery

    for P in $(kubectl get pvc 2>/dev/null | egrep "$CLUSTER_NAME" | cut -d\  -f1)
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
if [[ ! -e "$1" ]]; then
    echo "ERROR parameter file not specified, customize the template with your settings and database assets"
    usage
fi

# source our config file parameters
CFG_FILE=$1
source ${CFG_FILE}

if [[ -z $CLUSTER_NAME ]]; then
    echo "ERROR: CLUSTER_NAME unspecified"
    usage
fi

shift 1

# use Cluster name as default image name
: ${IMAGE_TAG:=${CLUSTER_NAME}}

if [[ $# -eq 0 ]]; then
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
                                    make_configmaps
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
