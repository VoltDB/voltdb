#!/usr/bin/env bash

###############################################################################
#
# Part of the helper scripts for creating a custom runnable VoltDB Docker image.
#
# This script creates an image suitable for running VoltDB on a particular OS
# distribution.
#
#
# Environment variables:
#
#   OS_DIST     Optional    Name of OS distribution to target, defaults to ubuntu-14.04
#   PROXY       Optional    Proxy address to pass to Docker, useful if behind a
#                           firewall or to speed up apt
#
#
# Usage examples:
#
#   OS_DIST=ubuntu-16.04 PROXY=10.0.0.3:3142 ./make-run-image.sh
#   OS_DIST=ubuntu-16.04 ./make-run-image.sh
#   PROXY=10.0.0.3:3142 ./make-run-image.sh
#   ./make-run-image.sh
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
ROOT_PROJECT_DIR=${SCRIPT_PATH}/../../../../..
PROJECT_DIR=${ROOT_PROJECT_DIR}/tools/docker/custom
BUILD_DIR=${PROJECT_DIR}/build

# Get VoltDB version
VOLTDB_VERSION=`cat ${ROOT_PROJECT_DIR}/version.txt`
VOLTDB_SPEC=voltdb-${VOLTDB_VERSION}

# Get OS dist
OS_DIST=${OS_DIST:-ubuntu-14.04}

# VoltDB distribution
VOLTDB_DIST_NAME=${VOLTDB_SPEC}_${OS_DIST}.tar.gz
VOLTDB_DIST_DIR=${BUILD_DIR}/dist

# Layout Docker image source
mkdir -p ${BUILD_DIR}
cp ${VOLTDB_DIST_DIR}/${VOLTDB_DIST_NAME} -T ${BUILD_DIR}/${VOLTDB_SPEC}.tar.gz
m4 -I ${PROJECT_DIR}/src/run/include -I ${PROJECT_DIR}/src/common/include ${PROJECT_DIR}/src/run/${OS_DIST}/Dockerfile.m4 > ${BUILD_DIR}/Dockerfile
cp ${ROOT_PROJECT_DIR}/tools/docker/deployment.xml ${BUILD_DIR}
cp ${ROOT_PROJECT_DIR}/tools/docker/docker-entrypoint.sh ${BUILD_DIR}

# Docker image
DOCKER_IMAGE_NAME=voltdb-runner
DOCKER_IMAGE_VERSION=1.0
DOCKER_TAG=${DOCKER_IMAGE_NAME}_${VOLTDB_SPEC}_${OS_DIST}:${DOCKER_IMAGE_VERSION}

# Docker image build context path
DOCKER_BUILD_CONTEXT=${BUILD_DIR}

# Proxy
if [ -n "${PROXY}" ]; then
  BUILD_ARGS="--build-arg http_proxy=http://${PROXY} --build-arg https_proxy=https://${PROXY}"
fi

# Volt Dockerfile expects VOLT_KIT_VERSION otherwise it simply uses the community VoltDB tarfile
BUILD_ARGS="${BUILD_ARGS} --build-arg VOLT_KIT_VERSION=${VOLTDB_VERSION}"

# Build a Docker image for running
sudo docker build \
	${BUILD_ARGS} \
	--tag ${DOCKER_TAG} \
	${DOCKER_BUILD_CONTEXT}
