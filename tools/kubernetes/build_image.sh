#!/bin/bash -e

# This file is part of VoltDB.
# Copyright (C) 2008-2018 VoltDB Inc.

# Author: Phil Rosegay

# Build a VoltDB docker image for VoltDB on Kubernetes

# source the template settings
if [ ! -e "$1" ]; then
    echo "ERROR parameter file not specified, customize the template with your setttings and database assets"
    echo "Usage: $0 CLUSTER_CONFIG_FILE"
    exit 1
elif [[ "$1" =~ ".cfg$" ]]; then
    echo "ERROR config file extension should be '.cfg'"
    exit 1
fi

CLUSTER_NAME=`basename $1 .cfg`
source $1

# use Cluster name as default image name
: ${IMAGE_TAG:=${CLUSTER_NAME}}

# customize the k8s statefulset
MANIFEST=`basename $1 .cfg`.yaml
cp voltdb-statefulset.yaml                        $MANIFEST
SED="sed -i"
[[ "$OSTYPE" =~ "darwin" ]] && SED="sed -i ''"
$SED "s:--clusterName--:$CLUSTER_NAME:g"          $MANIFEST
$SED "s+--containerImage---+$REP/$IMAGE_TAG+g"    $MANIFEST
$SED "s:--replicaCount--:$NODECOUNT:g"            $MANIFEST
$SED "s:--pvolumeSize--:${PVOLUME_SIZE:-1Gi}:g"   $MANIFEST
$SED "s:--memorySize--:${MEMORY_SIZE:-4Gi}:g"     $MANIFEST
$SED "s:--cpuCount--:${CPU_COUNT:-2}:g"           $MANIFEST

TMP_DIR=.assets/$CLUSTER_NAME
(rm -rf $TMP_DIR >/dev/null)
mkdir -p $TMP_DIR

# copy VOLTDB Deployment file - this must exist
if [ ! -f "${DEPLOYMENT_FILE}" ]; then
    echo "ERROR deployment file not specified or missing"
    exit 1
fi
cp ${DEPLOYMENT_FILE} $TMP_DIR/.deployment

# COPY customer supplied assets to the Dockerfile directory
# make empty files if these don't exist
cp ${SCHEMA_FILE:=/dev/null} $TMP_DIR/.schema
cp ${CLASSES_JAR:=/dev/null} $TMP_DIR/.classes
mkdir -p $TMP_DIR/.bundles
[ -d "${BUNDLES_DIR}" ] && cp -a ${BUNDLES_DIR}/ $TMP_DIR/.bundles/
mkdir -p $TMP_DIR/.extension
[ -d "${EXTENSION_DIR}" ] && cp -a ${EXTENSION_DIR}/ $TMP_DIR/.extension/
[ -f "${LOG4J_FILE}" ] && cp ${LOG4J_FILE} $TMP_DIR/.log4j
[ -f "${LICENSE_FILE}" ] && cp ${LICENSE_FILE} $TMP_DIR/.license

OWD=`pwd`
pushd ../.. > /dev/null
VOLTDB_DIST=$(basename `pwd`)

# Build Tag Deploy the image
# nb. the docker build environment will encompass the voltdb kit tree
docker image build -t ${IMAGE_TAG:-$CLUSTER_NAME} \
            --build-arg IP_DIR=${OWD#$PWD/}/$TMP_DIR \
            --build-arg VOLTDB_DIST_NAME=$VOLTDB_DIST \
            --build-arg NODECOUNT=$NODECOUNT \
        -f tools/kubernetes/docker/Dockerfile \
        "$PWD"
docker tag ${IMAGE_TAG} ${REP}/${IMAGE_TAG}
docker push ${REP}/${IMAGE_TAG}

rm -rf $TMP_DIR
