#!/usr/bin/env bash

###############################################################################
#
# Helper script for creating a custom runnable VoltDB Docker image.
#
# Useful in that it allows specific versions of Volt to be run on specific OS
# distributions (other than the community Volt image).
#
#
# Environment variables:
#
#   OS_DIST     Optional    Name of OS distribution to target, defaults to ubuntu-14.04
#   PROXY       Optional    Proxy address to pass to Docker, useful if behind a
#                           firewall or to speed up apt.
#
#
# Usage examples:
#
#   OS_DIST=ubuntu-16.04 PROXY=10.0.0.3:3142 ./build.sh
#   OS_DIST=ubuntu-16.04 ./build.sh
#   PROXY=10.0.0.3:3142 ./build.sh
#   ./build.sh
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
ROOT_PROJECT_DIR=${SCRIPT_PATH}/../../../../
PROJECT_DIR=${ROOT_PROJECT_DIR}/tools/docker/custom
BUILD_DIR=${PROJECT_DIR}/build

# Get VoltDB version
VOLTDB_VERSION=`cat ${ROOT_PROJECT_DIR}/version.txt`

# Get OS dist
OS_DIST=${OS_DIST:-ubuntu-14.04}

# Make VoltDB builder docker image
echo "builder | Making VoltDB build environment image (VoltDB: ${VOLTDB_VERSION}, OS: ${OS_DIST})"
${PROJECT_DIR}/src/build/make-build-image.sh

# Run VoltDB build in builder container
echo "builder | Building VoltDB (VoltDB: ${VOLTDB_VERSION}, OS: ${OS_DIST})"
${PROJECT_DIR}/src/build/build.sh

# Make runnable VoltDB docker image
echo "builder | Making VoltDB run image (VoltDB: ${VOLTDB_VERSION}, OS: ${OS_DIST})"
${PROJECT_DIR}/src/run/make-run-image.sh
