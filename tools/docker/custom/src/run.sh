#!/usr/bin/env bash

###############################################################################
#
# Part of the helper scripts for creating a custom runnable VoltDB Docker image.
#
# This script executes a custom VoltDB Docker image.
#
#
# Environment variables:
#
#   OS_DIST     Optional    Name of OS distribution to target, defaults to ubuntu-14.04
#
#
# Usage examples:
#
#   OS_DIST=ubuntu-16.04 ./run.sh
#   ./run.sh
#
#
###############################################################################

# Init
set -e

# Get the script path
pushd `dirname $0` > /dev/null
SCRIPT_PATH=`pwd`
popd > /dev/null

# Project directories
ROOT_PROJECT_DIR=${SCRIPT_PATH}/../../../..
PROJECT_DIR=${ROOT_PROJECT_DIR}/tools/docker/custom
BUILD_DIR=${PROJECT_DIR}/build

# Get VoltDB version
VOLTDB_VERSION=${VOLTDB_VERSION:-`cat ${ROOT_PROJECT_DIR}/version.txt`}
VOLTDB_SPEC=voltdb-${VOLTDB_VERSION}

# Get OS dist
OS_DIST=${OS_DIST:-ubuntu-14.04}

# Docker image
DOCKER_IMAGE_NAME=voltdb-runner
DOCKER_IMAGE_VERSION=1.0
DOCKER_TAG=${DOCKER_IMAGE_NAME}_${VOLTDB_SPEC}_${OS_DIST}:${DOCKER_IMAGE_VERSION}

# Create Docker bridge network if network does not already exist
CURRENT_NETWORK=`sudo docker network ls -q --filter 'name=voltLocalCluster'`
if [ -z ${CURRENT_NETWORK} ]; then
	sudo docker network create -d bridge voltLocalCluster
fi

# Run Docker container
HOSTCOUNT=3 DOCKER_IMAGE=${DOCKER_TAG} ${ROOT_PROJECT_DIR}/tools/docker/local-host-cluster.sh
