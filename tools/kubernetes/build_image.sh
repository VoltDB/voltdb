#!/bin/bash -ex

# This file is part of VoltDB.
# Copyright (C) 2008-2018 VoltDB Inc.

# Author: Phil Rosegay

# Build a VoltDB docker image for VoltDB on Kubernetes

# source the template settings
if [ ! -e "$1" ]; then
    echo "ERROR parameter file not specified, customize the template with your setttings and database assets"
    echo "Usage: $0 parameter_file"
    exit 1
fi
source $1

TMP_DIR=.IP_$$
mkdir -p $TMP_DIR

# copy VOLTDB Deployment file - this must exist
cp ${DEPLOYMENT_FILE} $TMP_DIR/.deployment

# get the hostcount from the deployment file
# set env HOSTCOUNT
eval xmllint --xpath "/deployment/cluster/@hostcount" $TMP_DIR/.deployment | awk '{print toupper($1)}' | xargs

# COPY customer supplied assets to the Dockerfile directory
# make empty files if these don't exist
cp ${SCHEMA_FILE:=/dev/null} $TMP_DIR/.schema
cp ${CLASSES_JAR:=/dev/null} $TMP_DIR/.classes
[ -n ${BUNDLES_DIR} ] && mkdir -p $TMP_DIR/.bundles && cp ${BUNDLES_DIR}/* $TMP_DIR/.bundles/
[ -n ${EXTENSION_DIR} ] && mkdir $TMP_DIR/.extension && cp ${EXTENSION_DIR}/* $TMP_DIR/.extension/
[ -n ${LOG4J_CUSTOM_FILE} ] && cp ${LOG4J_CUSTOM_FILE} $TMP_DIR/.log4j
[ -n ${LICENSE_FILE} ] && cp ${LICENSE_FILE} $TMP_DIR/.license

# nb. the docker build environemnt will encompass the voltdb kit tree
OWD=`pwd`
pushd ../.. > /dev/null
VOLTDB_DIST=$(basename `pwd`)

# Build Tag Deploy the image
docker image build -t ${IMAGE_TAG} \
            --build-arg IP_DIR=${OWD#$PWD/}/$TMP_DIR \
            --build-arg VOLTDB_DIST_NAME=$VOLTDB_DIST \
            --build-arg HOSTCOUNT=2 \
        -f tools/kubernetes/docker/Dockerfile \
        "$PWD"
docker tag ${IMAGE_TAG} ${REP}/${IMAGE_TAG}
docker push ${REP}/${IMAGE_TAG}

rm -rf $TMP_DIR
